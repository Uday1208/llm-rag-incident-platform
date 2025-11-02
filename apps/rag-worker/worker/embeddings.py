"""
File: embeddings.py
Purpose: Embed text batches with SentenceTransformers + optional Redis cache.
Notes:
- Public API (unchanged): init_embedder(app), close_embedder(app), embed_texts(List[str]) -> List[List[float]]
- Resolves model/redis from module globals OR app.state; lazily initializes as a safety net.
- Offloads encode to a worker thread; Redis cache is best-effort (never raises to caller).
"""

from __future__ import annotations
import os
import logging
from typing import List, Optional

import anyio
import numpy as np
from redis.asyncio import Redis
from redis.exceptions import (
    ConnectionError as RedisConnError,
    TimeoutError as RedisTimeout,
    AuthenticationError,
    RedisError,
)

log = logging.getLogger("rag-worker.embeddings")

# ----------------- Configuration -----------------
EMBED_MODEL_NAME = os.getenv("EMBED_MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2").strip()
EMBED_DIM = int(os.getenv("EMBED_DIM", "384"))          # keep in-sync with pgvector column dim
EMBED_BATCH = int(os.getenv("EMBED_BATCH", "32"))
CACHE_TTL_SEC = int(os.getenv("CACHE_TTL_SEC", "3600"))

REDIS_HOST = os.getenv("REDIS_HOST") or os.getenv("REDIS_HOSTNAME")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or os.getenv("REDIS_KEY")
REDIS_SSL = os.getenv("REDIS_SSL", "false").lower() in ("1", "true", "yes")

# ----------------- State -----------------
# Support both module-level state and app.state for maximal compatibility with your existing code
_model = None             # type: ignore
_redis: Optional[Redis] = None
_APP = None               # late-bound FastAPI app (if caller passes it)

# ----------------- Helpers -----------------
def _resolve_model():
    """Return a SentenceTransformer model from globals/app.state, lazily loading if needed."""
    global _model, _APP
    if _model is not None:
        return _model
    # try app.state if available
    app = _APP
    if app is not None:
        m = getattr(app.state, "embed_model", None)
        if m is not None:
            _model = m
            return _model
    # last resort: lazy-load (safety net so requests don't crash if init path diverged)
    from sentence_transformers import SentenceTransformer
    log.warning("embedder lazy-load: init_embedder() may not have run; loading %s now", EMBED_MODEL_NAME)
    _model = SentenceTransformer(EMBED_MODEL_NAME)
    if app is not None:
        app.state.embed_model = _model
    return _model

def _resolve_redis() -> Optional[Redis]:
    """Return Redis client from globals/app.state; create lazily if not present."""
    global _redis, _APP
    if _redis is not None:
        return _redis
    app = _APP
    r = getattr(app.state, "embed_redis", None) if app is not None else None
    if r is not None:
        _redis = r
        return _redis
    if not REDIS_HOST:
        return None
    r = Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        db=0,
        ssl=REDIS_SSL,
        # hardened socket options
        decode_responses=False,
        socket_keepalive=True,
        health_check_interval=30,
        socket_connect_timeout=5,
        socket_timeout=5,
    )
    _redis = r
    if app is not None:
        app.state.embed_redis = r
    return _redis

# ----------------- Public API -----------------
async def init_embedder(app) -> None:
    """Initialize global/app.state model and Redis client (idempotent)."""
    global _APP, _model, _redis
    _APP = app

    # Model
    if getattr(app.state, "embed_model", None) is None:
        from sentence_transformers import SentenceTransformer
        app.state.embed_model = SentenceTransformer(EMBED_MODEL_NAME)
        log.info("embedder initialized: %s", EMBED_MODEL_NAME)
    _model = app.state.embed_model

    # Redis (optional)
    if REDIS_HOST and getattr(app.state, "embed_redis", None) is None:
        app.state.embed_redis = Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            db=0,
            ssl=REDIS_SSL,
            decode_responses=False,
            socket_keepalive=True,
            health_check_interval=30,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        try:
            # best-effort sanity check; non-fatal
            await app.state.embed_redis.ping()
            log.info("redis cache connected: host=%s port=%s ssl=%s", REDIS_HOST, REDIS_PORT, REDIS_SSL)
        except Exception as e:
            log.warning("redis ping failed (cache disabled this run): %s", e)
    _redis = getattr(app.state, "embed_redis", None)

async def close_embedder(app) -> None:
    """Close Redis connection (model does not need explicit close)."""
    r: Optional[Redis] = getattr(app.state, "embed_redis", None)
    if r is not None:
        try:
            await r.aclose()
        except Exception:
            pass
        finally:
            app.state.embed_redis = None
    # Keep _model resident; no explicit close for SentenceTransformer

# ----------------- Embedding -----------------
async def embed_texts(texts: List[str]) -> List[List[float]]:
    """
    Compute embeddings with caching:
    - signature preserved: List[str] -> List[List[float]]
    - encode is offloaded to thread; Redis cache is best-effort
    - vectors are float32 and forced to EMBED_DIM (pad/trim) for pgvector
    """
    # normalize inputs
    items: List[str] = []
    for t in texts or []:
        s = t if isinstance(t, str) else str(t)
        s = s.strip()
        if s:
            items.append(s)
    if not items:
        return []

    model = _resolve_model()
    r = _resolve_redis()

    # derive simple keys (stable enough for our use)
    try:
        keys = [f"emb:{len(s)}:{hash(s)}" for s in items]
    except Exception:
        keys = [f"emb:{i}" for i in range(len(items))]

    # 1) read-through cache
    cached: List[Optional[List[float]]] = [None] * len(items)
    if r is not None:
        for i, k in enumerate(keys):
            try:
                buf = await r.get(k)
            except (RedisConnError, RedisTimeout, AuthenticationError, RedisError):
                buf = None
            if buf:
                try:
                    arr = np.frombuffer(buf, dtype=np.float32)
                    if EMBED_DIM and arr.size == EMBED_DIM:
                        cached[i] = arr.astype(np.float32).tolist()
                except Exception:
                    pass

    # 2) encode missing
    to_idx = [i for i, v in enumerate(cached) if v is None]
    if not to_idx:
        return cached  # type: ignore

    to_encode = [items[i] for i in to_idx]
    batch_size = EMBED_BATCH

    def _encode_block(block: List[str]) -> np.ndarray:
        arr = model.encode(
            block,
            batch_size=batch_size,
            convert_to_numpy=True,
            normalize_embeddings=True,
        )
        return np.asarray(arr, dtype=np.float32)

    parts: List[np.ndarray] = []
    for i in range(0, len(to_encode), batch_size):
        block = to_encode[i : i + batch_size]
        arr = await anyio.to_thread.run_sync(_encode_block, block)
        parts.append(arr)

    enc = np.concatenate(parts, axis=0) if len(parts) > 1 else parts[0]

    # 3) enforce dimension
    if EMBED_DIM:
        d = EMBED_DIM
        if enc.shape[1] != d:
            if enc.shape[1] > d:
                enc = enc[:, :d]
            else:
                pad = np.zeros((enc.shape[0], d - enc.shape[1]), dtype=np.float32)
                enc = np.concatenate([enc, pad], axis=1)

    # 4) fill results + write-through cache
    ttl = CACHE_TTL_SEC
    for j, idx in enumerate(to_idx):
        vec = enc[j]
        cached[idx] = vec.astype(np.float32).tolist()
        if r is not None:
            try:
                await r.setex(keys[idx], ttl, vec.tobytes())
            except (RedisConnError, RedisTimeout, AuthenticationError, RedisError):
                pass

    return cached  # type: ignore

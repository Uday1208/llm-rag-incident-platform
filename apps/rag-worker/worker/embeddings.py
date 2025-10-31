"""
File: embeddings.py
Purpose: Compute sentence embeddings with optional Redis caching (resilient).
Notes:
- Reads configuration from .config.settings (no os.getenv here).
- Uses Redis with keepalive/retry; cache failures do NOT fail requests.
- Each function includes a one-liner explaining its role.
"""

from __future__ import annotations

import asyncio
from typing import List, Optional

import numpy as np
from sentence_transformers import SentenceTransformer

# Redis (async) with resilience
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import (
    RedisError,
    ConnectionError,
    TimeoutError,
    AuthenticationError,
)

# Project imports
from .config import settings
from .instrumentation import CACHE_HITS, CACHE_MISSES, CACHE_ERRORS, EMBED_TIME


# -----------------------
# Settings (from .config)
# -----------------------
def _s(name: str, default: str) -> str:
    """Return string config value from settings with a default."""
    return str(getattr(settings, name, default))

def _i(name: str, default: int) -> int:
    """Return int config value from settings with a default."""
    try:
        return int(getattr(settings, name, default))
    except Exception:
        return int(default)

def _b(name: str, default: bool) -> bool:
    """Return bool config value from settings with a default."""
    val = getattr(settings, name, default)
    if isinstance(val, bool):
        return val
    sval = str(val).strip().lower()
    return sval in ("1", "true", "yes", "y", "on")

_EMBED_MODEL = _s("EMBED_MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2")
_EMBED_DIM   = _i("EMBED_DIM", 384)

_REDIS_HOST     = _s("REDIS_HOST", "")
_REDIS_PORT     = _i("REDIS_PORT", 6380)     # Azure Redis often TLS on 6380
_REDIS_PASSWORD = getattr(settings, "REDIS_PASSWORD", None) or None
_REDIS_DB       = _i("REDIS_DB", 0)
_REDIS_SSL      = _b("REDIS_SSL", True)
_REDIS_TTL      = _i("REDIS_TTL_SEC", 86400) # 1 day


# -----------------------
# Singletons
# -----------------------
_model: Optional[SentenceTransformer] = None
_redis: Optional[Redis] = None


def _get_model() -> SentenceTransformer:
    """Return a memoized sentence-transformer model."""
    global _model
    if _model is None:
        _model = SentenceTransformer(_EMBED_MODEL)
    return _model


def _get_redis() -> Optional[Redis]:
    """Return a memoized Redis client or None if not configured."""
    global _redis
    if _redis is not None:
        return _redis
    if not _REDIS_HOST:
        return None

    # ---- Client options (resilient & production-friendly) ----
    _redis = Redis(
        host=_REDIS_HOST,
        port=_REDIS_PORT,
        password=_REDIS_PASSWORD,
        db=_REDIS_DB,
        ssl=_REDIS_SSL,
        decode_responses=False,                # store packed float32 bytes
        socket_keepalive=True,
        health_check_interval=30,
        socket_connect_timeout=5,
        socket_timeout=5,
        retry=Retry(ExponentialBackoff(cap=3, base=0.2), retries=3),
        retry_on_error=[ConnectionError, TimeoutError],
    )
    return _redis


def _cache_key(text: str) -> bytes:
    """Return a stable cache key for a given text."""
    return ("emb:" + str(hash(text))).encode("utf-8")


def _pack(vec: np.ndarray) -> bytes:
    """Return a float32 numpy vector packed as bytes."""
    return vec.astype(np.float32).tobytes(order="C")


def _unpack(buf: bytes) -> np.ndarray:
    """Return a float32 numpy vector unpacked from bytes."""
    return np.frombuffer(buf, dtype=np.float32)


async def _cache_get(r: Redis, key: bytes) -> Optional[bytes]:
    """Return cached bytes or None; swallow transient Redis errors."""
    try:
        # (requested) wrap GET in try/except and count errors
        return await r.get(key)
    except (ConnectionError, TimeoutError, AuthenticationError, RedisError):
        CACHE_ERRORS.labels(kind="get").inc()
        return None


async def _cache_setex(r: Redis, key: bytes, ttl: int, val: bytes) -> None:
    """Set cached bytes with TTL; ignore transient Redis errors."""
    try:
        # (requested) wrap SETEX in try/except and count errors
        await r.setex(key, ttl, val)
    except (ConnectionError, TimeoutError, AuthenticationError, RedisError):
        CACHE_ERRORS.labels(kind="set").inc()
        # best-effort cache; do not re-raise


async def embed_texts(texts: List[str]) -> List[List[float]]:
    """Return embeddings for input texts, using cache when available and never failing on cache blips."""
    model = _get_model()
    r = _get_redis()

    keys = [_cache_key(t) for t in texts]
    cached: List[Optional[np.ndarray]] = [None] * len(texts)

    # 1) Parallel cache lookups (if Redis configured)
    if r is not None:
        async def _g(i: int, k: bytes):
            buf = await _cache_get(r, k)
            if not buf:
                return
            try:
                v = _unpack(buf)
                if v.shape == (_EMBED_DIM,):
                    cached[i] = v
            except Exception:
                cached[i] = None

        await asyncio.gather(*(_g(i, k) for i, k in enumerate(keys)))

    # 2) Compute missing embeddings
    to_compute = [i for i, v in enumerate(cached) if v is None]
    if to_compute:
        # metrics
        try:
            CACHE_HITS.inc(len(texts) - len(to_compute))
            CACHE_MISSES.inc(len(to_compute))
        except Exception:
            pass

        inputs = [texts[i] for i in to_compute]
        with EMBED_TIME.labels(phase="encode").time():
            vecs = await asyncio.to_thread(
                model.encode, inputs, normalize_embeddings=True
            )  # np.ndarray (N, D)

        for off, idx in enumerate(to_compute):
            cached[idx] = np.asarray(vecs[off], dtype=np.float32)

        # 3) Fire-and-forget cache writes
        if r is not None:
            async def _s(i: int):
                v = cached[i]
                if v is not None:
                    await _cache_setex(r, keys[i], _REDIS_TTL, _pack(v))

            asyncio.create_task(asyncio.gather(*(_s(i) for i in to_compute)))
    else:
        # all hits
        try:
            CACHE_HITS.inc(len(texts))
        except Exception:
            pass

    # 4) Return Python lists
    return [c.astype(np.float32).tolist() for c in cached]

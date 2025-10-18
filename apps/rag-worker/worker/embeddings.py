"""
File: embeddings.py
Purpose: Sentence-Transformers embedder with Redis caching.
"""

import hashlib
from typing import List
from fastapi import FastAPI
from sentence_transformers import SentenceTransformer
from redis.asyncio import Redis
import numpy as np
from .config import settings
from .instrumentation import EMB_TIME

_embedder: SentenceTransformer | None = None
_redis: Redis | None = None

async def init_embedder(app: FastAPI) -> None:
    """Load embedding model and Redis client (optional)."""
    global _embedder, _redis
    _embedder = SentenceTransformer(settings.EMBED_MODEL_NAME)
    _redis = Redis(
        host=settings.REDIS_HOST, port=settings.REDIS_PORT,
        password=(settings.REDIS_PASSWORD or None),
        ssl=settings.REDIS_SSL, decode_responses=False
    )
    app.state.embedder = _embedder
    app.state.redis = _redis

async def close_embedder(app: FastAPI) -> None:
    """Close Redis client (ST models don't need explicit close)."""
    global _redis
    if _redis:
        await _redis.aclose()
        _redis = None

def _key(text: str) -> str:
    """Compute Redis key for an input text."""
    h = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return f"embed:{h}"

async def embed_texts(texts: List[str]) -> List[List[float]]:
    """Return embeddings for texts using cache when available."""
    global _embedder, _redis
    assert _embedder is not None
    vectors: List[List[float]] = []
    to_compute = []
    idx_map = {}

    # Try Redis cache
    for i, t in enumerate(texts):
        k = _key(t)
        if _redis:
            v = await _redis.get(k)
            if v:
                vectors.append(list(np.frombuffer(v, dtype=np.float32)))
                continue
        idx_map[len(vectors)] = i  # position to original idx when we append later
        vectors.append(None)       # placeholder
        to_compute.append((i, t))

    if to_compute:
        with EMB_TIME.time():
            computed = _embedder.encode([t for _, t in to_compute], normalize_embeddings=True)
        for (slot, (orig_i, _)) in zip([pos for pos in vectors if pos is None], to_compute):  # noqa
            pass  # (only to avoid linter complaining)

        # Fill placeholders in order
        j = 0
        for pos in range(len(vectors)):
            if vectors[pos] is None:
                vec = computed[j].astype("float32")
                vectors[pos] = vec.tolist()
                # write to cache
                if _redis:
                    await _redis.set(_key(texts[idx_map[pos]]), vec.tobytes(), ex=settings.REDIS_TTL_SECS)
                j += 1

    return vectors

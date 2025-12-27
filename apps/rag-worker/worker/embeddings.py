"""
Multi-provider embedding service.

Supports:
- Local SentenceTransformers (default: all-MiniLM-L6-v2, 384-dim)
- Azure OpenAI (text-embedding-ada-002, 1536-dim)
- OpenAI API

Configuration via environment variables (see config.py).
"""

from __future__ import annotations
import hashlib
import logging
from typing import List, Optional, Protocol, Any
from abc import ABC, abstractmethod

import anyio
import numpy as np
import httpx

from .config import EmbeddingConfig, EmbeddingProvider, RedisConfig

log = logging.getLogger("rag-worker.embeddings")


# =============================================================================
# Embedding Provider Interface
# =============================================================================

class EmbeddingProviderProtocol(Protocol):
    """Protocol for embedding providers."""
    
    async def embed(self, texts: List[str]) -> List[List[float]]:
        """Embed a list of texts and return vectors."""
        ...
    
    @property
    def dimension(self) -> int:
        """Return the embedding dimension."""
        ...


# =============================================================================
# Local SentenceTransformers Provider
# =============================================================================

class LocalEmbeddingProvider:
    """SentenceTransformers-based local embedding."""
    
    def __init__(self, config: EmbeddingConfig):
        self.config = config
        self._model = None
    
    @property
    def dimension(self) -> int:
        return self.config.dimension
    
    def _get_model(self):
        """Lazy load the model."""
        if self._model is None:
            from sentence_transformers import SentenceTransformer
            # Handle both short names and full HuggingFace paths
            model_name = self.config.model_name
            if "/" not in model_name:
                model_name = f"sentence-transformers/{model_name}"
            log.info(f"Loading local embedding model: {model_name}")
            self._model = SentenceTransformer(model_name)
        return self._model
    
    async def embed(self, texts: List[str]) -> List[List[float]]:
        """Embed texts using SentenceTransformers (offloaded to thread)."""
        if not texts:
            return []
        
        model = self._get_model()
        batch_size = self.config.batch_size
        
        def _encode_sync(batch: List[str]) -> np.ndarray:
            arr = model.encode(
                batch,
                batch_size=batch_size,
                convert_to_numpy=True,
                normalize_embeddings=True,
                show_progress_bar=False,
            )
            return np.asarray(arr, dtype=np.float32)
        
        # Process in batches
        all_embeddings = []
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            embeddings = await anyio.to_thread.run_sync(_encode_sync, batch)
            all_embeddings.append(embeddings)
        
        # Concatenate and ensure correct dimension
        result = np.concatenate(all_embeddings, axis=0) if len(all_embeddings) > 1 else all_embeddings[0]
        result = self._ensure_dimension(result)
        
        return result.tolist()
    
    def _ensure_dimension(self, arr: np.ndarray) -> np.ndarray:
        """Pad or truncate to match configured dimension."""
        if arr.shape[1] == self.config.dimension:
            return arr
        elif arr.shape[1] > self.config.dimension:
            return arr[:, :self.config.dimension]
        else:
            pad = np.zeros((arr.shape[0], self.config.dimension - arr.shape[1]), dtype=np.float32)
            return np.concatenate([arr, pad], axis=1)


# =============================================================================
# Azure OpenAI Provider
# =============================================================================

class AzureOpenAIEmbeddingProvider:
    """Azure OpenAI embedding provider."""
    
    def __init__(self, config: EmbeddingConfig):
        self.config = config
        if not config.azure_endpoint or not config.azure_api_key:
            raise ValueError("Azure OpenAI requires AOAI_ENDPOINT and AOAI_KEY")
    
    @property
    def dimension(self) -> int:
        return self.config.dimension
    
    async def embed(self, texts: List[str]) -> List[List[float]]:
        """Embed texts using Azure OpenAI API."""
        if not texts:
            return []
        
        deployment = self.config.azure_deployment or self.config.model_name
        url = f"{self.config.azure_endpoint}/openai/deployments/{deployment}/embeddings?api-version={self.config.azure_api_version}"
        
        headers = {
            "Content-Type": "application/json",
            "api-key": self.config.azure_api_key,
        }
        
        all_embeddings = []
        batch_size = self.config.batch_size
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            for i in range(0, len(texts), batch_size):
                batch = texts[i:i + batch_size]
                payload = {"input": batch}
                
                try:
                    response = await client.post(url, headers=headers, json=payload)
                    response.raise_for_status()
                    data = response.json()
                    
                    # Extract embeddings from response
                    for item in sorted(data["data"], key=lambda x: x["index"]):
                        all_embeddings.append(item["embedding"])
                        
                except httpx.HTTPStatusError as e:
                    log.error(f"Azure OpenAI embedding failed: {e.response.status_code} - {e.response.text[:200]}")
                    raise
                except Exception as e:
                    log.error(f"Azure OpenAI embedding error: {e}")
                    raise
        
        return all_embeddings


# =============================================================================
# OpenAI API Provider
# =============================================================================

class OpenAIEmbeddingProvider:
    """OpenAI API embedding provider."""
    
    def __init__(self, config: EmbeddingConfig):
        self.config = config
        if not config.openai_api_key:
            raise ValueError("OpenAI requires OPENAI_API_KEY")
    
    @property
    def dimension(self) -> int:
        return self.config.dimension
    
    async def embed(self, texts: List[str]) -> List[List[float]]:
        """Embed texts using OpenAI API."""
        if not texts:
            return []
        
        url = "https://api.openai.com/v1/embeddings"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.config.openai_api_key}",
        }
        
        all_embeddings = []
        batch_size = self.config.batch_size
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            for i in range(0, len(texts), batch_size):
                batch = texts[i:i + batch_size]
                payload = {
                    "input": batch,
                    "model": self.config.model_name,
                }
                
                response = await client.post(url, headers=headers, json=payload)
                response.raise_for_status()
                data = response.json()
                
                for item in sorted(data["data"], key=lambda x: x["index"]):
                    all_embeddings.append(item["embedding"])
        
        return all_embeddings


# =============================================================================
# Caching Layer
# =============================================================================

class EmbeddingCache:
    """Redis-based embedding cache."""
    
    def __init__(self, redis_config: RedisConfig, ttl_seconds: int = 3600):
        self.config = redis_config
        self.ttl = ttl_seconds
        self._client = None
    
    async def _get_client(self):
        if self._client is None and self.config.enabled:
            from redis.asyncio import Redis
            self._client = Redis(
                host=self.config.host,
                port=self.config.port,
                password=self.config.password,
                db=self.config.db,
                ssl=self.config.ssl,
                decode_responses=False,
                socket_keepalive=True,
                socket_connect_timeout=5,
                socket_timeout=5,
            )
        return self._client
    
    def _make_key(self, text: str, model: str) -> str:
        """Generate cache key from text and model."""
        h = hashlib.sha256(f"{model}:{text}".encode()).hexdigest()[:32]
        return f"emb:{h}"
    
    async def get(self, text: str, model: str) -> Optional[List[float]]:
        """Get cached embedding."""
        client = await self._get_client()
        if not client:
            return None
        
        try:
            key = self._make_key(text, model)
            data = await client.get(key)
            if data:
                arr = np.frombuffer(data, dtype=np.float32)
                return arr.tolist()
        except Exception as e:
            log.debug(f"Cache get error: {e}")
        return None
    
    async def set(self, text: str, model: str, embedding: List[float]) -> None:
        """Cache an embedding."""
        client = await self._get_client()
        if not client:
            return
        
        try:
            key = self._make_key(text, model)
            arr = np.array(embedding, dtype=np.float32)
            await client.setex(key, self.ttl, arr.tobytes())
        except Exception as e:
            log.debug(f"Cache set error: {e}")
    
    async def close(self) -> None:
        """Close Redis connection."""
        if self._client:
            await self._client.aclose()
            self._client = None


# =============================================================================
# Main Embedding Service
# =============================================================================

class EmbeddingService:
    """
    High-level embedding service with provider abstraction and caching.
    
    Usage:
        service = EmbeddingService.from_config()
        vectors = await service.embed(["text1", "text2"])
    """
    
    def __init__(
        self,
        provider: EmbeddingProviderProtocol,
        cache: Optional[EmbeddingCache] = None,
        model_name: str = "unknown"
    ):
        self.provider = provider
        self.cache = cache
        self.model_name = model_name
    
    @property
    def dimension(self) -> int:
        return self.provider.dimension
    
    @classmethod
    def from_config(cls, embed_config: Optional[EmbeddingConfig] = None, redis_config: Optional[RedisConfig] = None) -> "EmbeddingService":
        """Create service from configuration."""
        if embed_config is None:
            embed_config = EmbeddingConfig.from_env()
        if redis_config is None:
            redis_config = RedisConfig.from_env()
        
        # Select provider
        if embed_config.provider == EmbeddingProvider.AZURE_OPENAI:
            provider = AzureOpenAIEmbeddingProvider(embed_config)
        elif embed_config.provider == EmbeddingProvider.OPENAI:
            provider = OpenAIEmbeddingProvider(embed_config)
        else:
            provider = LocalEmbeddingProvider(embed_config)
        
        # Setup cache
        cache = EmbeddingCache(redis_config, embed_config.cache_ttl_seconds) if redis_config.enabled else None
        
        log.info(f"Initialized embedding service: provider={embed_config.provider.value}, model={embed_config.model_name}, dim={embed_config.dimension}")
        
        return cls(provider, cache, embed_config.model_name)
    
    async def embed(self, texts: List[str]) -> List[List[float]]:
        """
        Embed texts with caching.
        
        Args:
            texts: List of strings to embed
            
        Returns:
            List of embedding vectors
        """
        if not texts:
            return []
        
        # Normalize inputs
        texts = [t.strip() if isinstance(t, str) else str(t).strip() for t in texts]
        texts = [t for t in texts if t]  # Remove empty
        
        if not texts:
            return []
        
        # Check cache
        results: List[Optional[List[float]]] = [None] * len(texts)
        uncached_indices: List[int] = []
        
        if self.cache:
            for i, text in enumerate(texts):
                cached = await self.cache.get(text, self.model_name)
                if cached:
                    results[i] = cached
                else:
                    uncached_indices.append(i)
        else:
            uncached_indices = list(range(len(texts)))
        
        # Embed uncached texts
        if uncached_indices:
            uncached_texts = [texts[i] for i in uncached_indices]
            embeddings = await self.provider.embed(uncached_texts)
            
            for j, i in enumerate(uncached_indices):
                results[i] = embeddings[j]
                if self.cache:
                    await self.cache.set(texts[i], self.model_name, embeddings[j])
        
        return results  # type: ignore
    
    async def close(self) -> None:
        """Cleanup resources."""
        if self.cache:
            await self.cache.close()


# =============================================================================
# Backward Compatibility (Legacy API)
# =============================================================================

# Module-level state for backward compatibility
_service: Optional[EmbeddingService] = None
_APP = None


async def init_embedder(app) -> None:
    """Initialize embedder (backward-compatible)."""
    global _service, _APP
    _APP = app
    
    if _service is None:
        _service = EmbeddingService.from_config()
    
    app.state.embed_service = _service
    app.state.embed_model = getattr(_service.provider, "_model", None)  # For legacy access


async def close_embedder(app) -> None:
    """Close embedder (backward-compatible)."""
    global _service
    if _service:
        await _service.close()
        _service = None


async def embed_texts(texts: List[str]) -> List[List[float]]:
    """Embed texts (backward-compatible)."""
    global _service
    if _service is None:
        _service = EmbeddingService.from_config()
    return await _service.embed(texts)


def get_embedding_dimension() -> int:
    """Get current embedding dimension."""
    config = EmbeddingConfig.from_env()
    return config.dimension

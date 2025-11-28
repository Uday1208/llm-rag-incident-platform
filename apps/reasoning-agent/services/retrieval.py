# reasoning-agent/services/retrieval.py
"""Thin retrieval service: get query embedding from rag-worker and search PG via pgvector."""

import os
import httpx
from typing import List, Dict, Any

RAG_WORKER_URL = (os.getenv("RAG_WORKER_URL", "") or "").rstrip("/")
EMBED_URL = f"{RAG_WORKER_URL}/internal/embed" if RAG_WORKER_URL else None
SEARCH_URL = f"{RAG_WORKER_URL}/internal/search" if RAG_WORKER_URL else None

class RetrievalError(Exception): ...
class ConfigError(Exception): ...

async def embed_query(text: str, timeout: float = 8.0) -> List[float]:
    if not EMBED_URL:
        raise ConfigError("RAG_WORKER_URL not configured")
    async with httpx.AsyncClient() as http:
        payload = {"texts": [text]}
        #r = await http.post(EMBED_URL, json={"text": text}, timeout=timeout)
        r = await http.post(EMBED_URL, json=payload, timeout=timeout)
        if r.status_code == 422:
            raise ValueError("worker/embed schema mismatch: expects {'texts':[str]} -> {'vectors':[[...]]}")
        r.raise_for_status()
        vec = r.json().get("embedding", [])
        if not isinstance(vec, list) or not vec:
            raise RetrievalError("Invalid embedding from rag-worker")
        return vec

async def search_by_embedding(vec: List[float], top_k: int = 5, timeout: float = 8.0) -> List[Dict[str, Any]]:
    if not SEARCH_URL:
        raise ConfigError("RAG_WORKER_URL not configured")
    async with httpx.AsyncClient() as http:
        r = await http.post(SEARCH_URL, json={"embedding": vec, "top_k": top_k}, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        return data.get("results", [])

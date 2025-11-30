# reasoning-agent/services/retrieval.py
"""Thin retrieval service: get query embedding from rag-worker and search PG via pgvector."""

import os
import httpx
from typing import List, Dict, Any, Optional
import math

RAG_WORKER_URL = (os.getenv("RAG_WORKER_URL", "") or "").rstrip("/")
EMBED_URL = f"{RAG_WORKER_URL}/internal/embed" if RAG_WORKER_URL else None
SEARCH_URL = f"{RAG_WORKER_URL}/internal/search" if RAG_WORKER_URL else None

class RetrievalError(Exception): ...
class ConfigError(Exception): ...

async def embed_query(text: str, timeout: float = 8.0) -> List[float]:
    """Call rag-worker /internal/embed and return a cleaned float vector."""
    if not EMBED_URL:
        raise ConfigError("RAG_WORKER_URL not configured")
    async with httpx.AsyncClient() as http:
        payload = {"texts": [text]}
        #r = await http.post(EMBED_URL, json={"text": text}, timeout=timeout)
        r = await http.post(EMBED_URL, json=payload, timeout=timeout)
        if r.status_code == 422:
            raise ValueError("worker/embed schema mismatch: expects {'texts':[str]} -> {'vectors':[[...]]}")
        try:
            r.raise_for_status()
        except httpx.HTTPStatusError as e:
            # bubble worker details to client
            raise HTTPException(status_code=e.response.status_code,
                                detail=f"search failed: {e.response.text[:200]}")
        data = r.json()
        vec = None
    
        # Accept several response shapes defensively:
        # 1) {"vectors":[[...]]}  (preferred)
        # 2) {"vectors":[{"embedding":[...]}]}
        # 3) {"embedding":[...]}
        if isinstance(data, dict):
            if isinstance(data.get("vectors"), list) and data["vectors"]:
                first = data["vectors"][0]
                vec = first.get("embedding") if isinstance(first, dict) else first
            elif isinstance(data.get("embedding"), list):
                vec = data["embedding"]
    
        if not isinstance(vec, list) or not vec:
            raise HTTPException(status_code=422, detail="search failed: Invalid embedding from rag-worker")
    
        # Coerce to finite floats
        clean: list[float] = []
        for x in vec:
            try:
                f = float(x)
                if math.isfinite(f):
                    clean.append(f)
            except Exception:
                continue
    
        if not clean:
            raise HTTPException(status_code=422, detail="search failed: Invalid embedding from rag-worker")
    
        return clean
        '''r.raise_for_status()
        vec = r.json().get("embedding", [])
        if not isinstance(vec, list) or not vec:
            raise RetrievalError("Invalid embedding from rag-worker")
        return vec'''

async def search_by_embedding(vec: List[float], top_k: int = 5, timeout: float = 8.0) -> List[Dict[str, Any]]:
    if not SEARCH_URL:
        raise ConfigError("RAG_WORKER_URL not configured")
    async with httpx.AsyncClient() as http:
        r = await http.post(SEARCH_URL, json={"embedding": vec, "top_k": top_k}, timeout=timeout)
        r.raise_for_status()
        data = r.json()
        return data.get("results", [])

async def search_by_text(query: str, top_k: int = 3) -> List[Dict[str, Any]]:
    """One-shot text → embedding → vector search via rag-worker."""
    vec = await embed_query(query)                  # uses existing embed_query()
    hits = await search_by_embedding(vec, top_k)    # uses existing search_by_embedding()
    return hits or []

def _format_hit(hit: Dict[str, Any], max_lines: int = 3) -> str:
    """Compact, readable line(s) for a single hit (title + 2–3 key lines)."""
    title = (hit.get("title") or hit.get("source") or "doc").strip()
    snippet = (hit.get("snippet") or hit.get("content") or "").strip()
    if max_lines > 0:
        lines = [ln.strip() for ln in snippet.splitlines() if ln.strip()]
        snippet = " ".join(lines[:max_lines])
    return f"{title}: {snippet}" if snippet else title

async def top_contexts_for_query(
    query: str,
    *,
    top_k: int = 3,
    max_lines: int = 3,
    min_score: float = 0.0
) -> List[str]:
    """Return formatted context strings for a query using rag-worker search."""
    hits = await search_by_text(query, top_k=top_k)
    out: List[str] = []
    for h in hits:
        score = float(h.get("score") or h.get("similarity") or 0.0)
        if min_score and score < min_score:
            continue
        out.append(_format_hit(h, max_lines=max_lines))
    return out

"""
File: routers/query.py
Purpose: Public query endpoint that orchestrates RAG and optional anomaly scoring.
"""

from typing import Any, Optional
from fastapi import APIRouter, Depends, Request, HTTPException, status
from redis.asyncio import Redis
from ..schemas.query import QueryRequest, QueryResponse
from ..deps import require_api_key
from ..config import settings
from ..clients import call_rag_worker, call_ts_score, DownstreamError
from ..limiter import allow_request, limiter_key

router = APIRouter()

def _client_ip(req: Request) -> str:
    """Extract best-effort client IP from headers."""
    # Try standard forward headers first
    for hdr in ("x-forwarded-for", "x-original-forwarded-for", "x-client-ip"):
        v = req.headers.get(hdr)
        if v:
            return v.split(",")[0].strip()
    return req.client.host if req.client else "unknown"

@router.post("/query", response_model=QueryResponse, status_code=status.HTTP_200_OK,
             dependencies=[Depends(require_api_key)])
async def query(req: Request, body: QueryRequest) -> Any:
    """Accept query, enforce rate-limit, call RAG worker, optionally enrich with anomaly score, return result."""
    redis: Optional[Redis] = req.app.state.redis if hasattr(req.app.state, "redis") else None
    http = req.app.state.http

    # Rate limit
    if redis:
        key = limiter_key(req.headers.get("x-api-key"), _client_ip(req))
        allowed = await allow_request(redis, key)
        if not allowed:
            raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Rate limit exceeded")

    # Call rag-worker for end-to-end RAG
    try:
        rag_payload = {"query": body.query, "top_k": body.top_k, "with_scores": body.with_scores}
        rag = await call_rag_worker(http, rag_payload)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"RAG worker error: {str(e)}")

    # Optionally call TorchServe for anomaly signal (e.g., on retrieved contexts or query)
    anomaly_signal = None
    if settings.TS_MODEL_URL:
        try:
            '''# pick a few retrieved context snippets if present
            lines = rag.get("contexts", [])[:10] if isinstance(rag.get("contexts"), list) else []
            ts = await call_ts_score(http, lines)
            # expect {"scores":[...]} → reduce to a single signal (mean)
            scores = ts.get("scores")
            if isinstance(scores, list) and scores:
                anomaly_signal = float(sum(scores) / len(scores))'''
            from services.ts_client import score_lines
            raw_ctx = rag.get("contexts") or []
            if not isinstance(raw_ctx, list):
                raw_ctx = []
            lines = [
                (ctx.get("content", "") if isinstance(ctx, dict) else str(ctx))
                for ctx in raw_ctx
            ][:50]  # keep payload small
            anomaly_signal = await score_lines(lines)
        except Exception:
            # Soft-fail: keep main answer even if TS is unavailable
            anomaly_signal = None

    # Normalize to gateway response schema
    answer = rag.get("answer", "")
    context_ids = rag.get("context_ids", []) or []
    scores = rag.get("scores") if body.with_scores else None

    return QueryResponse(
        answer=answer,
        context_ids=context_ids,
        scores=scores,
        anomaly_signal=anomaly_signal
    )

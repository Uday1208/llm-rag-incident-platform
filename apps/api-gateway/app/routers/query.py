"""
File: routers/query.py
Purpose: Public query endpoint (skeleton). Delegates to RAG worker and TorchServe later.
"""

from fastapi import APIRouter, Depends, status
from typing import Any
from ..schemas.query import QueryRequest, QueryResponse
from ..deps import require_api_key

router = APIRouter()

@router.post("/query", response_model=QueryResponse, status_code=status.HTTP_200_OK,
             dependencies=[Depends(require_api_key)])
def query(req: QueryRequest) -> Any:
    """Accept query, route to downstream services, and return aggregated result (stub)."""
    # TODO: call RAG worker for embed+retrieve
    # TODO: call TorchServe for anomaly score fusion
    # TODO: log, trace, and emit metrics
    return QueryResponse(answer="[stub] gateway up", context_ids=[], scores=None)

"""Search endpoint that embeds the query and returns nearest documents."""

from typing import Optional, List
from pydantic import BaseModel, Field
from fastapi import APIRouter, HTTPException
#from ..services.retrieval import embed_query, search_by_embedding
from services.retrieval import embed_query, search_by_embedding
import os

router = APIRouter(prefix="/v1", tags=["search"])

class SearchReq(BaseModel):
    """Request body for semantic search over incident documents."""
    query: str = Field(..., min_length=1)
    top_k: Optional[int] = Field(default=None, ge=1, le=50)

class Doc(BaseModel):
    """Minimal document view returned by search."""
    id: str
    source: Optional[str] = None
    ts: Optional[str] = None
    severity: Optional[str] = None
    content: str

class SearchResp(BaseModel):
    """Search response including matched documents."""
    query: str
    top_k: int
    results: List[Doc]

@router.post("/search", response_model=SearchResp)
async def search(req: SearchReq) -> SearchResp:
    """Embed the query, retrieve nearest documents, and return results."""
    try:
        top_k = req.top_k or int(os.getenv("RETR_TOP_K", "5"))
        qvec = await embed_query(req.query)
        hits = search_by_embedding(qvec, top_k=top_k)
        return SearchResp(query=req.query, top_k=top_k, results=[Doc(**h) for h in hits])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"search failed: {e}")

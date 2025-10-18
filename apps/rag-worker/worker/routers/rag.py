"""
File: routers/rag.py
Purpose: RAG endpoint: embed query, search pgvector, compose answer.
"""

from fastapi import APIRouter, HTTPException
from typing import List
from ..schemas.rag import RAGRequest, RAGResponse
from ..embeddings import embed_texts
from ..repository import search_by_embedding
from ..rag import compose_answer

router = APIRouter()

@router.post("/rag", response_model=RAGResponse)
async def rag(req: RAGRequest) -> RAGResponse:
    """Perform retrieval (pgvector) and compose an answer (optional generation)."""
    if not req.query.strip():
        raise HTTPException(status_code=400, detail="Empty query")
    qvec = (await embed_texts([req.query]))[0]
    rows = search_by_embedding(qvec, req.top_k)
    if not rows:
        return RAGResponse(answer="I don't know.", context_ids=[], scores=[], contexts=[])

    ids = [r[0] for r in rows]
    ctx = [r[1] for r in rows]
    scores = [r[2] for r in rows] if req.with_scores else None
    answer = compose_answer(req.query, ctx)
    return RAGResponse(answer=answer, context_ids=ids, scores=scores, contexts=ctx)

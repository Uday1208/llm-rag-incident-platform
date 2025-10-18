"""
File: routers/rag.py
Purpose: RAG endpoint with LC or raw pgvector retrieval based on USE_LANGCHAIN_STORE.
"""

from fastapi import APIRouter, HTTPException
'''from ..schemas.rag import RAGRequest, RAGResponse
from ..embeddings import embed_texts
from ..repository import search_by_embedding
from ..repository_lc import search_by_query as lc_search_by_query
from ..rag import compose_answer
from ..config import settings'''
from worker.schemas.rag import RAGRequest, RAGResponse
from worker.embeddings import embed_texts
from worker.repository import search_by_embedding
from worker.repository_lc import search_by_query as lc_search_by_query
from worker.rag import compose_answer
from worker.config import settings

router = APIRouter()

@router.post("/rag", response_model=RAGResponse)
async def rag(req: RAGRequest) -> RAGResponse:
    """Perform retrieval and compose an answer (LC or raw path)."""
    if not req.query.strip():
        raise HTTPException(status_code=400, detail="Empty query")

    if settings.USE_LANGCHAIN_STORE:
        rows = lc_search_by_query(req.query, req.top_k)  # [(id, content, sim)]
        if not rows:
            return RAGResponse(answer="I don't know.", context_ids=[], scores=[], contexts=[])
        ids = [r[0] for r in rows]
        ctx = [r[1] for r in rows]
        scores = [r[2] for r in rows] if req.with_scores else None
        answer = compose_answer(req.query, ctx)
        return RAGResponse(answer=answer, context_ids=ids, scores=scores, contexts=ctx)

    # Raw pgvector: we embed query ourselves then run SQL <=> search
    qvec = (await embed_texts([req.query]))[0]
    rows = search_by_embedding(qvec, req.top_k)
    if not rows:
        return RAGResponse(answer="I don't know.", context_ids=[], scores=[], contexts=[])
    ids = [r[0] for r in rows]
    ctx = [r[1] for r in rows]
    scores = [r[2] for r in rows] if req.with_scores else None
    answer = compose_answer(req.query, ctx)
    return RAGResponse(answer=answer, context_ids=ids, scores=scores, contexts=ctx)

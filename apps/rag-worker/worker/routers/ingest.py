"""
File: routers/ingest.py
Purpose: Batch ingest/upsert documents into vector store; toggles LC vs raw repository.
"""

from fastapi import APIRouter, HTTPException
from typing import List, Tuple
from ..schemas.ingest import IngestRequest, IngestResponse
from ..embeddings import embed_texts
from ..repository import upsert_documents
from ..repository_lc import upsert_texts as upsert_texts_lc
from ..config import settings

router = APIRouter()

@router.post("/ingest", response_model=IngestResponse)
async def ingest(req: IngestRequest) -> IngestResponse:
    """Embed & upsert using raw pgvector or LangChain PGVector depending on flag."""
    if not req.documents:
        raise HTTPException(status_code=400, detail="No documents provided")

    if settings.USE_LANGCHAIN_STORE:
        ids = [d.id for d in req.documents]
        sources = [d.source for d in req.documents]
        contents = [d.content for d in req.documents]
        ts_iso = [d.ts.isoformat() if d.ts else None for d in req.documents]
        n = upsert_texts_lc(ids, sources, contents, ts_iso)
        return IngestResponse(upserted=n)

    # Raw pgvector path (fast)
    contents = [d.content for d in req.documents]
    vecs = await embed_texts(contents)
    rows: List[Tuple[str, str, str, str, list]] = []
    for d, v in zip(req.documents, vecs):
        ts_iso = d.ts.isoformat() if d.ts else None
        rows.append((d.id, d.source, ts_iso, d.content, v))
    upsert_documents(rows)
    return IngestResponse(upserted=len(rows))

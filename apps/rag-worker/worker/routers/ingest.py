"""
File: routers/ingest.py
Purpose: Batch ingest/upsert documents with embeddings into pgvector.
"""

from fastapi import APIRouter, HTTPException
from typing import List, Tuple
from ..schemas.ingest import IngestRequest, IngestResponse
from ..embeddings import embed_texts
from ..repository import upsert_documents

router = APIRouter()

@router.post("/ingest", response_model=IngestResponse)
async def ingest(req: IngestRequest) -> IngestResponse:
    """Embed and upsert documents into the vector store."""
    if not req.documents:
        raise HTTPException(status_code=400, detail="No documents provided")
    contents = [d.content for d in req.documents]
    vecs = await embed_texts(contents)
    rows: List[Tuple[str, str, str, str, list]] = []
    for d, v in zip(req.documents, vecs):
        ts_iso = d.ts.isoformat() if d.ts else None
        rows.append((d.id, d.source, ts_iso, d.content, v))
    upsert_documents(rows)
    return IngestResponse(upserted=len(rows))

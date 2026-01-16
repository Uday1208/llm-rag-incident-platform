"""
File: routers/ingest.py
Purpose: Ingest endpoint (embed + upsert).
Notes:
- Keeps existing route (/v1/ingest) and async signature.
- Uses absolute imports (worker.*) to avoid package path issues.
- Sanitizes inputs so embedder never receives None/empty strings.
"""

from typing import List, Tuple
from fastapi import APIRouter, HTTPException

from worker.schemas.ingest import IngestRequest
from worker.embeddings import embed_texts
from worker.repository import upsert_documents, upsert_incidents

def _coerce_severity(doc) -> int | None:
    """
    Map incoming fields to severity int 0..5.
    Accepts: severity (int/str), or level/severity_level strings.
    Returns None if unknown.
    """
    try:
        if 'severity' in doc and doc['severity'] is not None:
            s = int(doc['severity'])
            return max(0, min(5, s))
    except Exception:
        pass

    lvl = str(doc.get('level') or doc.get('severity_level') or "").strip().lower()
    if lvl:
        table = {
            "trace": 0, "debug": 0,
            "info": 1,
            "warn": 2, "warning": 2,
            "error": 3,
            "critical": 4, "fatal": 5,
        }
        return table.get(lvl, None)
    return None

router = APIRouter()

@router.post("/v1/ingest")
async def ingest(req: IngestRequest):
    """
    Accepts a list of documents, computes embeddings, and upserts into Postgres.
    Populates 'documents' (for RAG) and 'incidents' (if metadata present).
    """
    raw_docs = req.documents if isinstance(req.documents, list) else []
    if not raw_docs:
        return {"upserted": 0}

    # Prepare for embeddings
    contents = [str(d.content).strip() for d in raw_docs if d.id and d.content]
    if not contents:
        return {"upserted": 0}

    try:
        vecs: List[List[float]] = await embed_texts(contents)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"embedding failed: {e}")

    # 1. Build rows for 'documents' table
    doc_rows = []
    
    # 2. Build rows for 'incidents' table
    inc_rows = []
    
    for d, v in zip(raw_docs, vecs):
        # Severity coercion
        sev_int = _coerce_severity(d.dict())
        doc_rows.append((d.id, d.source, d.ts, d.content, sev_int, v))
        
        # Check for incident metadata
        if d.metadata:
            m = d.metadata
            # (incident_id, title, status, severity, started_at, resolved_at, owner, tags)
            inc_rows.append((
                d.id,
                m.get("title") or m.get("error_signature") or d.source,
                m.get("status", "OPEN"),
                d.severity or "INFO",
                d.ts,
                m.get("resolved_at"),
                m.get("owner") or d.source,
                m.get("tags", [])
            ))

    doc_count = upsert_documents(doc_rows)
    inc_count = upsert_incidents(inc_rows)
    
    return {
        "upserted": doc_count,
        "incidents_created": inc_count
    }


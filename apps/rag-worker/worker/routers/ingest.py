"""
File: routers/ingest.py
Purpose: Ingest endpoint (embed + upsert).
Notes:
- Keeps existing route (/v1/ingest) and async signature.
- Uses absolute imports (worker.*) to avoid package path issues.
- Sanitizes inputs so embedder never receives None/empty strings.
"""

import json
from typing import List, Tuple
from fastapi import APIRouter, HTTPException

from worker.schemas.ingest import IngestRequest
from worker.embeddings import embed_texts
from worker.repository import upsert_documents, upsert_incidents

def _map_to_prod_severity(level_or_val: str | int | None) -> str:
    """Map internal severity to production labels SEV1..SEV4."""
    if level_or_val is None:
        return "SEV3"
    
    val = str(level_or_val).strip().upper()
    # Map descriptive names
    mapping = {
        "CRITICAL": "SEV1",
        "FATAL": "SEV1",
        "ERROR": "SEV2",
        "WARNING": "SEV3",
        "WARN": "SEV3",
        "INFO": "SEV4",
        "DEBUG": "SEV4",
        "TRACE": "SEV4",
        "5": "SEV1",
        "4": "SEV2",
        "3": "SEV2",
        "2": "SEV3",
        "1": "SEV4",
        "0": "SEV4",
    }
    return mapping.get(val, "SEV3")

def _coerce_severity(doc) -> str:
    """Return mapped production severity (SEV1-4)."""
    val = doc.get("severity") or doc.get("level") or doc.get("severity_level") or "INFO"
    return _map_to_prod_severity(val)

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
        # Severity coercion to SEV labels
        sev_label = _coerce_severity(d.dict())
        doc_rows.append((d.id, d.source, d.ts, d.content, sev_label, v))
        
        # Check for incident metadata
        if d.metadata:
            m = d.metadata
            status = str(m.get("status") or "open").lower()
            if status not in ["open", "mitigated", "resolved", "closed"]:
                status = "open"
                
            # (incident_id, title, status, severity, started_at, resolved_at, owner, tags)
            inc_rows.append((
                d.id,
                m.get("title") or m.get("error_signature") or d.source,
                status,
                sev_label,
                d.ts,
                m.get("resolved_at"),
                m.get("owner") or d.source,
                m.get("tags", []),
                json.dumps(m.get("propagation", []))
            ))

    doc_count = upsert_documents(doc_rows)
    inc_count = upsert_incidents(inc_rows)
    
    return {
        "upserted": doc_count,
        "incidents_created": inc_count
    }


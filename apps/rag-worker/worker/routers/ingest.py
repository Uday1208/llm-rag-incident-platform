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
from worker.repository import upsert_documents

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
    Each document requires at least id and content.
    """

    raw_docs = req.documents if isinstance(req.documents, list) else []

    ids: List[str] = []
    sources: List[str] = []
    ts_list: List[str] = []
    contents: List[str] = []

    # Be tolerant to either Pydantic models or plain dicts
    for d in raw_docs:
        _id = getattr(d, "id", None) or (d.get("id") if isinstance(d, dict) else None)
        _src = getattr(d, "source", None) or (d.get("source") if isinstance(d, dict) else "")
        _ts  = getattr(d, "ts", None) or (d.get("ts") if isinstance(d, dict) else "")
        _ct  = getattr(d, "content", None) or (d.get("content") if isinstance(d, dict) else "")

        # Require id + content; silently skip partials (or raise 400 if you prefer strictness)
        if not _id or not _ct:
            continue

        # Normalize to strings and strip content
        ids.append(str(_id))
        sources.append(str(_src))
        ts_list.append(str(_ts))
        contents.append(str(_ct).strip())

    if not contents:
        return {"upserted": 0}

    # Embeddings (bubble up errors so ops sees real failures)
    try:
        vecs: List[List[float]] = await embed_texts(contents)
    except Exception as e:
        # Keep simple: no starlette JSONResponse import; use HTTPException
        raise HTTPException(status_code=500, detail=f"embedding failed: {e}")

    if len(vecs) != len(contents):
        # Prevent mismatched inserts (dimension drift or partial encode)
        raise HTTPException(status_code=500, detail="embedding output size mismatch")

    '''# Build rows for repository (id, source, ts, content, embedding)
    rows: List[Tuple[str, str, str, str, list]] = []
    for i in range(len(contents)):
        rows.append((ids[i], sources[i], ts_list[i], contents[i], vecs[i]))

    n = upsert_documents(rows)'''
    
    # --- BEGIN minimal add for severity (keeps your order & types) ---
    rows = []
    for d, v in zip(req.documents, vecs):
        # _coerce_severity was added earlier; works with Pydantic model or dict
        sev = _coerce_severity(d.dict() if hasattr(d, "dict") else d)
        rows.append((
            d.id,
            d.source,
            d.ts,        # already ISO per your schema
            d.content,
            sev,         # <--- NEW: severity (int 0..5) or None
            v,           # embedding (list[float])
        ))
    count = upsert_documents(rows)
    # --- END minimal add for severity ---
    return {"upserted": count}

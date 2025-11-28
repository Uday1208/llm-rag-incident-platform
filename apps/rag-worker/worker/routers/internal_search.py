# worker/routers/internal_search.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, conlist
import os
import psycopg2
from typing import List, Dict, Any
from ..db import get_conn

router = APIRouter(tags=["internal"])

#PG_CONN = os.getenv("PG_CONN", "")
EMBED_DIM = int(os.getenv("EMBED_DIM", "384"))

class SearchIn(BaseModel):
    embedding: conlist(float, min_items=1)
    top_k: int = 5

@router.post("/internal/search")
def internal_search(inp: SearchIn) -> Dict[str, Any]:
    '''if not PG_CONN:
        raise HTTPException(status_code=500, detail="PG_CONN not configured")'''
    if len(inp.embedding) != EMBED_DIM:
        raise HTTPException(status_code=400, detail=f"embedding length {len(inp.embedding)} != EMBED_DIM {EMBED_DIM}")

    sql = """
    SELECT id, source, ts, content, severity,
           1 - (embedding <=> %s::vector) AS score
    FROM documents
    WHERE embedding IS NOT NULL
    ORDER BY embedding <=> %s::vector
    LIMIT %s
    """
    try:        
        #with psycopg2.connect(PG_CONN) as conn, conn.cursor() as cur:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (inp.embedding, inp.embedding, inp.top_k))
            rows = cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    results: List[Dict[str, Any]] = []
    for (id_, source, ts, content, severity, score) in rows:
        results.append({
            "id": id_,
            "source": source,
            "ts": ts.isoformat() if ts else None,
            "content": content,
            "severity": severity,
            "score": float(score) if score is not None else None,
        })
    return {"results": results}

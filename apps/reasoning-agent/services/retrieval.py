"""Thin retrieval service: get query embedding from rag-worker and search PG via pgvector."""

import os
from typing import List
import httpx
import psycopg2
import psycopg2.extras

PG_CONN = os.getenv("PG_CONN", "")
RAG_EMBED_URL = os.getenv("RAG_EMBED_URL", "")
RETR_TIMEOUT = float(os.getenv("RETR_TIMEOUT", "8"))

def _pg() -> psycopg2.extensions.connection:
    """Open a Postgres connection using PG_CONN."""
    if not PG_CONN:
        raise RuntimeError("PG_CONN not set")
    return psycopg2.connect(PG_CONN)

async def embed_query(text: str) -> List[float]:
    """Fetch an embedding for a single query string from rag-worker."""
    if not RAG_EMBED_URL:
        raise RuntimeError("RAG_EMBED_URL not set")
    async with httpx.AsyncClient(timeout=RETR_TIMEOUT) as http:
        r = await http.post(RAG_EMBED_URL, json={"texts": [text]})
        r.raise_for_status()
        data = r.json()
        vecs = data.get("vectors") or []
        if not vecs or not vecs[0]:
            raise RuntimeError("empty embedding from rag-worker")
        return [float(x) for x in vecs[0]]

def search_by_embedding(qvec: List[float], top_k: int = 5) -> list[dict]:
    """Return top_k documents ordered by vector distance to the query embedding."""
    sql = """
      SELECT id, source, ts, severity, content
      FROM documents
      WHERE embedding IS NOT NULL
      ORDER BY embedding <-> %s
      LIMIT %s
    """
    with _pg() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, (qvec, top_k))
        return [dict(r) for r in cur.fetchall()]

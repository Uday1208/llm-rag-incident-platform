"""
File: repository.py
Purpose: Data access layer for documents (upsert, search).
"""

from typing import Iterable, List, Tuple
from .db import get_conn
from .instrumentation import DB_TIME
from .config import settings

def _py_floats(vec: Iterable[float]) -> List[float]:
    """Convert any numpy scalars to plain Python floats (psycopg2-friendly)."""
    return [float(x) for x in vec]

def upsert_documents(rows: Iterable[Tuple[str, str, str, str, list]]) -> int:
    """Upsert documents: (id, source, ts_iso, content, embedding)."""
    sql = f"""
    INSERT INTO documents (id, source, ts, content, embedding)
    VALUES (%s, %s, %s, %s, CAST(%s AS {settings.VECTOR_SQLTYPE}))
    ON CONFLICT (id) DO UPDATE
      SET source    = EXCLUDED.source,
          ts        = EXCLUDED.ts,
          content   = EXCLUDED.content,
          embedding = EXCLUDED.embedding;
    """
    with DB_TIME.labels(route="upsert").time():
        with get_conn() as conn:
            with conn.cursor() as cur:
                # Coerce embeddings to plain Python floats before binding
                to_exec = [
                    (id_, src, ts, txt, _py_floats(emb))
                    for (id_, src, ts, txt, emb) in rows
                ]
                cur.executemany(sql, to_exec)
            conn.commit()
    return len(to_exec)

def search_by_embedding(query_vec: List[float], top_k: int) -> List[Tuple[str, str, float]]:
    """Return (id, content, similarity) top_k by cosine distance."""
    sql = f"""
    SELECT id, content, (1 - (embedding <=> CAST(%s AS {settings.VECTOR_SQLTYPE}))) AS sim
    FROM documents
    ORDER BY embedding <=> CAST(%s AS {settings.VECTOR_SQLTYPE})
    LIMIT %s;
    """
    with DB_TIME.labels(route="search").time():
        with get_conn() as conn:
            with conn.cursor() as cur:
                v = _py_floats(query_vec)  # ensure psycopg2 gets plain floats
                cur.execute(sql, (v, v, top_k))
                rows = cur.fetchall()
    return [(r[0], r[1], float(r[2])) for r in rows]

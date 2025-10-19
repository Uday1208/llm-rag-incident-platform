"""
File: repository.py
Purpose: Data access layer for documents (upsert, search).
"""

from typing import Iterable, List, Tuple
from .db import get_conn
from .instrumentation import DB_TIME

def upsert_documents(rows: Iterable[Tuple[str, str, str, str, list]]) -> int:
    """Upsert documents: (id, source, ts_iso, content, embedding)."""
    sql = """
    INSERT INTO documents (id, source, ts, content, embedding)
    VALUES (%s, %s, %s, %s, CAST(%s AS vector(384)))
    ON CONFLICT (id) DO UPDATE
      SET source    = EXCLUDED.source,
          ts        = EXCLUDED.ts,
          content   = EXCLUDED.content,
          embedding = EXCLUDED.embedding;
    """
    with DB_TIME.labels(route="upsert").time():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()
    return len(list(rows))

def search_by_embedding(query_vec: list[float], top_k: int) -> List[Tuple[str, str, float]]:
    """Return (id, content, similarity) top_k by cosine distance."""
    sql = """
    SELECT id, content, (1 - (embedding <=> CAST(%s AS vector(384)))) AS sim
    FROM documents
    ORDER BY embedding <=> CAST(%s AS vector(384))
    LIMIT %s;
    """
    with DB_TIME.labels(route="search").time():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (query_vec, query_vec, top_k))
                rows = cur.fetchall()
    return [(r[0], r[1], float(r[2])) for r in rows]

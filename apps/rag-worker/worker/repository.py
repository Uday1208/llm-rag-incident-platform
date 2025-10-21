"""
File: repository.py
Purpose: Data access layer for documents (upsert, search).
"""

from typing import Iterable, List, Tuple
from .db import get_conn, has_cosine_operator
from .instrumentation import DB_TIME
from .config import settings  # ensures settings.VECTOR_SQLTYPE like "vector(384)"

def _to_pyfloats(vec: Iterable) -> list[float]:
    """Convert any numeric iterable (e.g., numpy) into plain Python floats."""
    return [float(x) for x in vec]


def upsert_documents(rows: Iterable[Tuple[str, str, str, str, list]]) -> int:
    """Upsert documents: (id, source, ts_iso, content, embedding)."""
    sql = """
    INSERT INTO documents (id, source, ts, content, embedding)
    VALUES (%s, %s, %s, %s, CAST(%s AS {settings.VECTOR_SQLTYPE}))
    ON CONFLICT (id) DO UPDATE
      SET source    = EXCLUDED.source,
          ts        = EXCLUDED.ts,
          content   = EXCLUDED.content,
          embedding = EXCLUDED.embedding;
    """
    materialized = [(i, s, ts, c, _to_pyfloats(e)) for (i, s, ts, c, e) in rows]
    with DB_TIME.labels(route="upsert").time():
        with get_conn() as conn, conn.cursor() as cur:
            cur.executemany(sql, materialized)
            conn.commit()
    return len(materialized)


def search_by_embedding(query_vec: list[float], top_k: int) -> List[Tuple[str, str, float]]:
    """Return (id, content, similarity) top_k using pgvector NN search."""
    qv = _to_pyfloats(query_vec)
    if has_cosine_operator():
        sql = """
        SELECT id, content, (1 - (embedding <=> CAST(%s AS {settings.VECTOR_SQLTYPE}))) AS sim
        FROM documents
        ORDER BY embedding <=> %s
        LIMIT %s;
        """
        params = (qv, qv, int(top_k))
    else:
        # Fallback to Euclidean distance if cosine op not present
        sql = """
        SELECT id, content, NULL::float AS sim
        FROM documents
        ORDER BY embedding <-> %s
        LIMIT %s;
        """
        params = (qv, int(top_k))

    with DB_TIME.labels(route="search").time():
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    return [(r[0], r[1], float(r[2]) if r[2] is not None else 0.0) for r in rows]

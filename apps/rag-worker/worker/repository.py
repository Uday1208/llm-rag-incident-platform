"""
File: repository.py
Purpose: Data access layer for documents (upsert, search).
"""

from typing import Iterable, List, Tuple, Optional

from .db import get_conn, has_cosine_operator
from .instrumentation import DB_TIME
from .config import settings  # ensures settings.VECTOR_SQLTYPE like "vector(384)"

VEC_TYPE = settings.VECTOR_SQLTYPE

def _to_pyfloats(vec: Iterable) -> list[float]:
    """Convert any numeric iterable (e.g., numpy) into plain Python floats."""
    return [float(x) for x in vec]

# Row shape expected by upsert_documents:
# (id, source, ts_iso, content, severity, embedding)
# - severity is Optional[int] (0..5). Pass None if unknown.
def upsert_documents(rows: Iterable[Tuple[str, str, str, str, list]]) -> int:
    """Upsert documents: (id, source, ts_iso, content, severity, embedding)."""
    sql = f"""
    INSERT INTO documents (id, source, ts, content, severity, embedding)
    VALUES (%s, %s, %s, %s, %s, CAST(%s AS {VEC_TYPE}))
    ON CONFLICT (id) DO UPDATE
      SET source    = EXCLUDED.source,
          ts        = EXCLUDED.ts,
          content   = EXCLUDED.content,
          -- only overwrite severity if a new non-null value is provided
          severity  = COALESCE(EXCLUDED.severity, documents.severity),
          embedding = EXCLUDED.embedding;
    """
    materialized = [(i, s, ts, c, sev, _to_pyfloats(e)) for (i, s, ts, c, sev, e) in rows]

    if not materialized:
        return 0
        
    with DB_TIME.labels(route="upsert").time():
        with get_conn() as conn, conn.cursor() as cur:
            cur.executemany(sql, materialized)
            conn.commit()
    return len(materialized)


def search_by_embedding(query_vec: list[float], top_k: int) -> List[Tuple[str, str, float]]:
    """Return (id, content, similarity) top_k using pgvector NN search."""
    qv = _to_pyfloats(query_vec)
    if has_cosine_operator():
        sql = f"""
        SELECT id, content, (1 - (embedding <=> CAST(%s AS {VEC_TYPE}))) AS sim
        FROM documents
        ORDER BY embedding <=> CAST(%s AS {VEC_TYPE})
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


def get_recent_bundles(limit: int = 50) -> List[Tuple]:
    """Fetch recent incident bundles for the dashboard."""
    sql = """
    SELECT id, trace_id, service, severity, symptoms, error_signature, first_ts, content
    FROM incident_bundles
    ORDER BY first_ts DESC NULLS LAST
    LIMIT %s;
    """
    try:
        with DB_TIME.labels(route="list_incidents").time():
            with get_conn() as conn, conn.cursor() as cur:
                cur.execute(sql, (limit,))
                rows = cur.fetchall()
        return rows
    except Exception:
        # If table doesn't exist or other error, return empty
        return []


def get_bundle(bundle_id: str) -> Optional[Tuple]:
    """Fetch a single incident bundle by ID."""
    sql = """
    SELECT id, trace_id, service, severity, symptoms, error_signature, first_ts, content
    FROM incident_bundles
    WHERE id = %s;
    """
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (bundle_id,))
            return cur.fetchone()
    except Exception:
        return None



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
def upsert_incidents(rows: Iterable[Tuple]) -> int:
    """
    Upsert incidents: (incident_id, title, status, severity, started_at, resolved_at, owner, tags)
    """
    sql = """
    INSERT INTO incidents (incident_id, title, status, severity, started_at, resolved_at, owner, tags, propagation)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (incident_id) DO UPDATE
      SET title      = EXCLUDED.title,
          status     = EXCLUDED.status,
          severity   = EXCLUDED.severity,
          started_at = EXCLUDED.started_at,
          resolved_at = EXCLUDED.resolved_at,
          owner      = EXCLUDED.owner,
          tags       = EXCLUDED.tags,
          propagation = EXCLUDED.propagation;
    """
    materialized = list(rows)
    if not materialized:
        return 0
        
    with DB_TIME.labels(route="upsert_incidents").time():
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


def get_recent_incidents(limit: int = 50) -> List[Tuple]:
    """Fetch recent incidents for the dashboard from the 'incidents' table."""
    # Mapping columns: incident_id as id, title as symptoms/summary, started_at as first_ts
    sql = """
    SELECT incident_id, incident_id, 'N/A', severity, title, '', started_at, title, propagation
    FROM incidents
    ORDER BY started_at DESC NULLS LAST
    LIMIT %s;
    """
    try:
        with DB_TIME.labels(route="list_incidents").time():
            with get_conn() as conn, conn.cursor() as cur:
                cur.execute(sql, (limit,))
                rows = cur.fetchall()
        return rows
    except Exception:
        return []


def get_incident(incident_id: str) -> Optional[Tuple]:
    """Fetch a single incident by ID from the 'incidents' table."""
    sql = """
    SELECT incident_id, incident_id, 'N/A', severity, title, '', started_at, title, propagation
    FROM incidents
    WHERE incident_id = %s;
    """
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(sql, (incident_id,))
            return cur.fetchone()
    except Exception:
        return None




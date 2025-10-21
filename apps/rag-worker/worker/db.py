"""
File: db.py
Purpose: DB connection utilities + pgvector schema bootstrap and capability checks.
"""

import os
import psycopg2
from contextlib import contextmanager
from .instrumentation import DB_TIME  # metrics histogram/timer

# Build DSN from env (PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, PG_SSLMODE)
PG_DSN = (
    f"host={os.getenv('PGHOST')} "
    f"port={os.getenv('PGPORT', '5432')} "
    f"dbname={os.getenv('PGDATABASE')} "
    f"user={os.getenv('PGUSER')} "
    f"password={os.getenv('PGPASSWORD')} "
    f"sslmode={os.getenv('PG_SSLMODE', 'require')}"
)

# Vector dimension (must match embeddings you store)
VECTOR_DIM = int(os.getenv("VECTOR_DIM", "384"))

# Cached capability probe
_HAS_COSINE = None


@contextmanager
def get_conn():
    """Yield a psycopg2 connection with pgvector adapter registered."""
    with DB_TIME.labels(route="connect").time():
        conn = psycopg2.connect(PG_DSN)
    try:
        # Ensure Python list[float] binds as pgvector (not numeric[])
        try:
            from pgvector.psycopg2 import register_vector
            register_vector(conn)
        except Exception:
            pass
        yield conn
    finally:
        conn.close()


def ensure_schema():
    """Create pgvector extension, documents table and IVFFlat index (idempotent)."""
    sqls = [
        "CREATE EXTENSION IF NOT EXISTS vector;",
        f"""
        CREATE TABLE IF NOT EXISTS documents (
          id        TEXT PRIMARY KEY,
          source    TEXT,
          ts        TIMESTAMPTZ,
          content   TEXT NOT NULL,
          embedding VECTOR({VECTOR_DIM}) NOT NULL
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_documents_embedding "
        "ON documents USING ivfflat (embedding vector_cosine_ops);",
    ]
    with DB_TIME.labels(route="schema").time():
        with get_conn() as conn, conn.cursor() as cur:
            for s in sqls:
                cur.execute(s)
            conn.commit()


def has_cosine_operator() -> bool:
    """Return True if '<=> (vector,vector)' exists on this server (pgvector >=0.5)."""
    global _HAS_COSINE
    if _HAS_COSINE is not None:
        return _HAS_COSINE
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT 1
            FROM pg_operator
            WHERE oprname = '<=>'
              AND oprleft = 'vector'::regtype
              AND oprright = 'vector'::regtype
            LIMIT 1;
        """)
        _HAS_COSINE = bool(cur.fetchone())
    return _HAS_COSINE

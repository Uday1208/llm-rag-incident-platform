"""
File: db.py
Purpose: DB connection utilities with optional psycopg2 pool, pgvector schema bootstrap,
         and capability checks (cosine operator). Each connection registers pgvector adapter.
"""

import os
import psycopg2
from contextlib import contextmanager
from psycopg2.pool import SimpleConnectionPool
from .instrumentation import DB_TIME  # histogram/timer

# Build DSN from env
PG_DSN = (
    f"host={os.getenv('PGHOST')} "
    f"port={os.getenv('PGPORT', '5432')} "
    f"dbname={os.getenv('PGDATABASE')} "
    f"user={os.getenv('PGUSER')} "
    f"password={os.getenv('PGPASSWORD')} "
    f"sslmode={os.getenv('PG_SSLMODE', 'require')}"
)

# Vector dim must match embeddings you store
VECTOR_DIM = int(os.getenv("VECTOR_DIM", "384"))

# Pool settings (optional)
POOL_MIN = int(os.getenv("DB_POOL_MIN", "1"))
POOL_MAX = int(os.getenv("DB_POOL_MAX", "10"))

# Globals
_POOL: SimpleConnectionPool | None = None
_HAS_COSINE: bool | None = None


def _register_vector_adapter(conn: psycopg2.extensions.connection) -> None:
    """Register pgvector adapter so Python list[float] binds as 'vector' (not numeric[])."""
    try:
        from pgvector.psycopg2 import register_vector
        register_vector(conn)
    except Exception:
        # If the extension/package isn't present yet, queries that rely on it will fail fast anyway.
        pass


def _new_connection() -> psycopg2.extensions.connection:
    """Create a new raw connection and register pgvector adapter."""
    conn = psycopg2.connect(PG_DSN)
    _register_vector_adapter(conn)
    return conn


def init_db_pool(app: object = None) -> None:
    """Initialize a global connection pool (idempotent). Accepts optional app for compatibility."""
    global _POOL
    if _POOL is None:
        with DB_TIME.labels(route="pool_init").time():
            _POOL = SimpleConnectionPool(POOL_MIN, POOL_MAX, dsn=PG_DSN)
    # Warm one connection to ensure adapter registration and extension availability
    with get_conn():
        pass


def close_db_pool(app: object = None) -> None:
    """Close and clear the global pool. Accepts optional app for compatibility."""
    global _POOL
    if _POOL is not None:
        _POOL.closeall()
        _POOL = None


@contextmanager
def get_conn():
    """Yield a connection (from pool if available), with pgvector adapter registered."""
    if _POOL is not None:
        with DB_TIME.labels(route="connect_pool").time():
            conn = _POOL.getconn()
        try:
            _register_vector_adapter(conn)
            yield conn
        finally:
            _POOL.putconn(conn)
    else:
        with DB_TIME.labels(route="connect").time():
            conn = _new_connection()
        try:
            yield conn
        finally:
            conn.close()


def ensure_schema(app: object = None) -> None:
    """Create pgvector extension, documents table, and IVFFlat index (idempotent)."""
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
    """Return True if '<=> (vector,vector)' exists on this server (pgvector >= 0.5)."""
    global _HAS_COSINE
    if _HAS_COSINE is not None:
        return _HAS_COSINE
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM pg_operator
            WHERE oprname = '<=>'
              AND oprleft = 'vector'::regtype
              AND oprright = 'vector'::regtype
            LIMIT 1;
            """
        )
        _HAS_COSINE = bool(cur.fetchone())
    return _HAS_COSINE

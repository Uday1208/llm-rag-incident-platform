"""
File: db.py
Purpose: Connection management for Postgres with pgvector support (async-safe via asyncio.to_thread).
"""

import os
import psycopg2
import asyncio
from contextlib import contextmanager
from psycopg2.pool import SimpleConnectionPool
from .instrumentation import DB_TIME

# ========= Your existing env var scheme =========
# Uses PG, PG_USER, PG_PASS, PG_DB_NAME (like your Azure setup)
PGHOST = os.getenv("PG")  # host name
PGUSER = os.getenv("PG_USER")
PGPASSWORD = os.getenv("PG_PASS")
PGDB = os.getenv("PG_DB")
PGPORT = os.getenv("PG_PORT", "5432")
PG_SSLMODE = os.getenv("PG_SSLMODE", "require")

POOL_MIN = int(os.getenv("DB_POOL_MIN", "1"))
POOL_MAX = int(os.getenv("DB_POOL_MAX", "5"))
VECTOR_DIM = int(os.getenv("VECTOR_DIM", "384"))

_POOL: SimpleConnectionPool | None = None
_HAS_COSINE: bool | None = None


def _register_vector_adapter(conn):
    """Register pgvector adapter so Python list[float] binds as 'vector'."""
    try:
        from pgvector.psycopg2 import register_vector
        register_vector(conn)
    except Exception:
        pass


def _make_dsn() -> str:
    """Compose DSN using Azure-style env vars."""
    if not all([PGHOST, PGUSER, PGPASSWORD, PGDB]):
        missing = [k for k, v in {
            "PG": PGHOST, "PG_USER": PGUSER, "PG_PASS": PGPASSWORD, "PG_DB_NAME": PGDB
        }.items() if not v or v == "None"]
        raise RuntimeError(f"Missing Postgres env vars: {', '.join(missing)}")

    return f"host={PGHOST} port={PGPORT} dbname={PGDB} user={PGUSER} password={PGPASSWORD} sslmode={PG_SSLMODE}"


@contextmanager
def get_conn():
    """Yield a DB connection, pooled if available."""
    if _POOL:
        with DB_TIME.labels(route="connect_pool").time():
            conn = _POOL.getconn()
        try:
            _register_vector_adapter(conn)
            yield conn
        finally:
            _POOL.putconn(conn)
    else:
        with DB_TIME.labels(route="connect").time():
            conn = psycopg2.connect(_make_dsn())
            _register_vector_adapter(conn)
            yield conn
            conn.close()


# ========= SYNC IMPLEMENTATIONS =========

def _init_db_pool_sync():
    """Initialize the global connection pool."""
    global _POOL
    if _POOL is None:
        with DB_TIME.labels(route="pool_init").time():
            dsn = _make_dsn()
            _POOL = SimpleConnectionPool(POOL_MIN, POOL_MAX, dsn=dsn)
    # Warm one connection
    with get_conn():
        pass


def _close_db_pool_sync():
    """Close the global pool."""
    global _POOL
    if _POOL:
        _POOL.closeall()
        _POOL = None


def _ensure_schema_sync():
    """Ensure pgvector extension and documents schema exist."""
    sqls = [
        "CREATE EXTENSION IF NOT EXISTS vector;",
        f"""
        CREATE TABLE IF NOT EXISTS documents (
            id TEXT PRIMARY KEY,
            source TEXT,
            ts TIMESTAMPTZ,
            content TEXT NOT NULL,
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
    """Return True if <=> operator is available for vector type."""
    global _HAS_COSINE
    if _HAS_COSINE is not None:
        return _HAS_COSINE
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM pg_operator
            WHERE oprname = '<=>'
              AND oprleft = 'vector'::regtype
              AND oprright = 'vector'::regtype
            LIMIT 1;
            """
        )
        _HAS_COSINE = bool(cur.fetchone())
    return _HAS_COSINE


# ========= ASYNC WRAPPERS =========
# These make main.py `await init_db_pool(app)` etc. work.

async def init_db_pool(app=None):
    """Async wrapper for pool init."""
    await asyncio.to_thread(_init_db_pool_sync)

async def close_db_pool(app=None):
    """Async wrapper for pool close."""
    await asyncio.to_thread(_close_db_pool_sync)

async def ensure_schema(app=None):
    """Async wrapper for schema ensure."""
    await asyncio.to_thread(_ensure_schema_sync)

"""
File: db.py
Purpose: Connection management for Postgres (Azure env vars) with pgvector support,
         optional pooling, schema bootstrap, cosine-operator check, and async wrappers.

Env (Azure-style):
  PG=<hostname>            # e.g. pg-xxxxx.postgres.database.azure.com
  PG_USER=<username>
  PG_PASS=<password>
  PG_DB=ragincdb
  PG_PORT=5432             # optional (default 5432)
  PG_SSLMODE=require       # optional (default require)
  DB_POOL_MIN=1            # optional
  DB_POOL_MAX=5            # optional
  VECTOR_DIM=384           # must match the embedding model
"""

from __future__ import annotations

import asyncio
import os
import psycopg2
from contextlib import contextmanager
from psycopg2.pool import SimpleConnectionPool
from .instrumentation import DB_TIME  # Prometheus histogram/timer

# -------- Azure env-style variables (kept exactly as in your repo) --------
PGHOST = os.getenv("PG")                   # Postgres host
PGUSER = os.getenv("PG_USER")              # Postgres user
PGPASSWORD = os.getenv("PG_PASS")          # Postgres password
PGDB = os.getenv("PG_DB")             # Postgres database name
PGPORT = os.getenv("PG_PORT", "5432")      # Postgres port
PG_SSLMODE = os.getenv("PG_SSLMODE", "require")

POOL_MIN = int(os.getenv("DB_POOL_MIN", "1"))
POOL_MAX = int(os.getenv("DB_POOL_MAX", "5"))
VECTOR_DIM = int(os.getenv("VECTOR_DIM", "384"))

# -------- Module globals --------
_POOL: SimpleConnectionPool | None = None
_HAS_COSINE: bool | None = None


def _register_vector_adapter(conn: psycopg2.extensions.connection) -> None:
    """Register pgvector adapter so Python list[float] binds as 'vector'."""
    try:
        from pgvector.psycopg2 import register_vector
        register_vector(conn)
    except Exception:
        # If pgvector not installed yet, extension creation will fail fast elsewhere.
        pass


def _make_dsn() -> str:
    """Build psycopg2 DSN from Azure-style env vars; raise if anything essential is missing."""
    missing = [k for k, v in {
        "PG": PGHOST, "PG_USER": PGUSER, "PG_PASS": PGPASSWORD, "PG_DB_NAME": PGDB
    }.items() if not v or v == "None"]
    if missing:
        raise RuntimeError(f"Missing Postgres env vars: {', '.join(missing)}")

    return (
        f"host={PGHOST} port={PGPORT} dbname={PGDB} "
        f"user={PGUSER} password={PGPASSWORD} sslmode={PG_SSLMODE}"
    )


@contextmanager
def get_conn():
    """Yield a DB connection (from pool if available); ensures pgvector adapter is registered."""
    global _POOL
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
            conn = psycopg2.connect(_make_dsn())
        try:
            _register_vector_adapter(conn)
            yield conn
        finally:
            conn.close()


# ----------------------- SYNC implementations -----------------------

def _init_db_pool_sync() -> None:
    """Create the global connection pool (idempotent) and warm a connection."""
    global _POOL
    if _POOL is None:
        with DB_TIME.labels(route="pool_init").time():
            _POOL = SimpleConnectionPool(POOL_MIN, POOL_MAX, dsn=_make_dsn())
    # Warm one connection (validates DSN & registers pgvector)
    with get_conn():
        pass


def _close_db_pool_sync() -> None:
    """Close the global connection pool and clear the handle."""
    global _POOL
    if _POOL is not None:
        _POOL.closeall()
        _POOL = None


def _ensure_schema_sync() -> None:
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
    """Return True if '<=> (vector,vector)' operator exists (pgvector >= 0.5)."""
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


# ----------------------- ASYNC wrappers (awaitable in lifespan) -----------------------

async def init_db_pool(app=None) -> None:
    """Async wrapper for pool init; safe to `await` in FastAPI lifespan."""
    await asyncio.to_thread(_init_db_pool_sync)


async def close_db_pool(app=None) -> None:
    """Async wrapper for pool close; safe to `await` in FastAPI shutdown."""
    await asyncio.to_thread(_close_db_pool_sync)


async def ensure_schema(app=None) -> None:
    """Async wrapper for schema ensure; safe to `await` in FastAPI startup."""
    await asyncio.to_thread(_ensure_schema_sync)

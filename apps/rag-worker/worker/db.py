"""
File: db.py
Purpose: psycopg2 connection pool with pgvector registration; schema management helpers.
"""

import psycopg2
from psycopg2 import pool
from pgvector.psycopg2 import register_vector
from fastapi import FastAPI
from contextlib import contextmanager
from time import time
from .config import settings
from .instrumentation import DB_TIME

_pool: pool.SimpleConnectionPool | None = None

async def init_db_pool(app: FastAPI) -> None:
    """Initialize global psycopg2 connection pool."""
    global _pool
    _pool = psycopg2.pool.SimpleConnectionPool(
        minconn=settings.PG_POOL_MIN,
        maxconn=settings.PG_POOL_MAX,
        host=settings.PG_HOST,
        dbname=settings.PG_DB,
        user=settings.PG_USER,
        password=settings.PG_PASS,
        sslmode=settings.PG_SSLMODE,
    )
    # register pgvector type on a test connection
    conn = _pool.getconn()
    try:
        register_vector(conn)
    finally:
        _pool.putconn(conn)
    app.state.db_pool = _pool

async def close_db_pool(app: FastAPI) -> None:
    """Close global psycopg2 connection pool."""
    global _pool
    if _pool:
        _pool.closeall()
        _pool = None

@contextmanager
def get_conn():
    """Yield a pooled psycopg2 connection with pgvector registered."""
    assert _pool is not None, "DB pool not initialized"
    conn = _pool.getconn()
    try:
        register_vector(conn)
        yield conn
    finally:
        _pool.putconn(conn)

async def ensure_schema(app: FastAPI) -> None:
    """Create documents table and ivfflat index if not present."""
    ddl = """
    CREATE TABLE IF NOT EXISTS documents (
        id TEXT PRIMARY KEY,
        source TEXT,
        ts TIMESTAMPTZ,
        content TEXT NOT NULL,
        embedding VECTOR(384) NOT NULL
    );
    CREATE INDEX IF NOT EXISTS documents_embedding_idx
      ON documents USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
    """
    with DB_TIME.labels(route="schema").time():
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()

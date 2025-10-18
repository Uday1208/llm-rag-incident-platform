"""
File: main.py
Purpose: Application entrypoint for the RAG Worker. Wires routers, logging, telemetry, pools.
"""

from fastapi import FastAPI
from contextlib import asynccontextmanager
from .logging_setup import configure_logging
from .instrumentation import setup_metrics, REQUESTS, LATENCY
from .routers import health, metrics, ingest, rag
from .db import init_db_pool, close_db_pool, ensure_schema
from .embeddings import init_embedder, close_embedder
from .config import settings

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage app startup/shutdown lifecycle."""
    configure_logging()
    setup_metrics(app)
    await init_embedder(app)
    await init_db_pool(app)
    await ensure_schema(app)  # create table/index if missing
    yield
    await close_db_pool(app)
    await close_embedder(app)

app = FastAPI(
    title="LLM RAG Incident Platform - RAG Worker",
    version="1.0.0",
    lifespan=lifespan
)

# Simple request timing middleware for metrics
@app.middleware("http")
async def prometheus_mw(request, call_next):
    """Track request metrics and latency histograms."""
    route = request.url.path
    with LATENCY.labels(route=route).time():
        resp = await call_next(request)
    REQUESTS.labels(route=route, method=request.method, status=str(resp.status_code)).inc()
    return resp

# Routers
app.include_router(health.router, prefix="", tags=["system"])
app.include_router(metrics.router, prefix="", tags=["system"])
app.include_router(ingest.router, prefix="/v1", tags=["ingest"])
app.include_router(rag.router, prefix="/v1", tags=["rag"])

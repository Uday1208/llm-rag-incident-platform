"""
File: main.py
Purpose: Application entrypoint for the RAG Worker. Wires routers, logging, telemetry, pools.
Notes: Non-blocking startup (init runs in background) so startup probes don't fail.
"""

import os
import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from fastapi import FastAPI

# OpenTelemetry instrumentation
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

# ---- keep your existing local imports exactly as-is ----
from .logging_setup import configure_logging                      # sets JSON logging/levels/handlers
from .instrumentation import setup_metrics, REQUESTS, LATENCY     # registers Prometheus metrics in app.state
from .routers import health, metrics, ingest, rag                 # system/ingest/RAG routers
from .db import init_db_pool, close_db_pool, ensure_schema        # PG pool init/close; create schema if missing
from .embeddings import init_embedder, close_embedder             # load ST model; open/close Redis client
from .config import settings                                      # typed settings from env

from .routers import embed_api
from .routers import internal_search
from .routers import export

log = logging.getLogger("rag-worker")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

# Initialize OpenTelemetry tracer
trace.set_tracer_provider(TracerProvider())
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

# ---------- helpers to run init tasks without blocking startup ----------
async def _guarded(name: str, coro, app: FastAPI, timeout_sec: float = None):
    """Run an async init task with timeout; never raise during startup."""
    t = timeout_sec or float(os.getenv("INIT_TIMEOUT_SEC", "30"))
    try:
        await asyncio.wait_for(coro, timeout=t)
        app.state.init[name] = True
        log.info("init-%s: OK", name)
    except Exception as e:
        app.state.init["errors"][name] = str(e)
        log.exception("init-%s: FAILED: %s", name, e)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # logging & metrics first so subsequent failures are visible
    configure_logging()
    setup_metrics(app)

    # flags visible to your existing health router (if it wants to read them)
    app.state.init = {"embedder": False, "db_pool": False, "schema": False, "errors": {}}

    # kick off heavy init in background (do NOT await here)
    async def _init_all():
        # order: embedder/redis -> db pool -> schema
        await _guarded("embedder", init_embedder(app), app)
        await _guarded("db_pool", init_db_pool(app), app)
        await _guarded("schema", ensure_schema(app), app)

    bg_task = asyncio.create_task(_init_all())

    try:
        # do not block startup; app starts serving immediately
        yield
    finally:
        # graceful shutdown; don't let exceptions kill shutdown
        if not bg_task.done():
            bg_task.cancel()
            with suppress(asyncio.CancelledError):
                await bg_task
        with suppress(Exception):
            await close_db_pool(app)
        with suppress(Exception):
            await close_embedder(app)

app = FastAPI(
    title="LLM RAG Incident Platform - RAG Worker",
    version="1.0.0",
    lifespan=lifespan,
)

# Instrument FastAPI for distributed tracing
FastAPIInstrumentor.instrument_app(app)

@app.middleware("http")
async def prometheus_mw(request, call_next):
    """Track request counts and latency per route."""
    route = request.url.path
    with LATENCY.labels(route=route).time():
        resp = await call_next(request)
    REQUESTS.labels(route=route, method=request.method, status=str(resp.status_code)).inc()
    return resp

# ---- routers (unchanged) ----
app.include_router(health.router,   prefix="",    tags=["system"])
app.include_router(metrics.router,  prefix="",    tags=["system"])
app.include_router(ingest.router,   prefix="", tags=["ingest"])
app.include_router(rag.router,      prefix="/v1", tags=["rag"])
app.include_router(embed_api.router, prefix="")
app.include_router(internal_search.router, prefix="")
app.include_router(export.router, prefix="")

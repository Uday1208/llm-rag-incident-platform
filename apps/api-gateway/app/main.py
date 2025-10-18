"""
File: main.py
Purpose: Application entrypoint for the API Gateway service. Wires routers, logging, telemetry, and clients.
"""

from fastapi import FastAPI, Request
from contextlib import asynccontextmanager
from .logging_setup import configure_logging
from .instrumentation import setup_metrics, REQUESTS, LATENCY
from .routers import health, metrics, query
from .clients import init_clients, close_clients

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage app startup/shutdown lifecycle (connect pools, warm caches)."""
    configure_logging()
    setup_metrics(app)
    await init_clients(app)
    yield
    await close_clients(app)

app = FastAPI(
    title="LLM RAG Incident Platform - API Gateway",
    version="1.0.0",
    lifespan=lifespan
)

# Simple request timing middleware for metrics
@app.middleware("http")
async def prometheus_middleware(request: Request, call_next):
    """Track request metrics and latency histograms."""
    route = request.url.path
    with LATENCY.labels(route=route).time():
        response = await call_next(request)
    REQUESTS.labels(route=route, method=request.method, status=str(response.status_code)).inc()
    return response

# Routers
app.include_router(health.router, prefix="", tags=["system"])
app.include_router(metrics.router, prefix="", tags=["system"])
app.include_router(query.router, prefix="/v1", tags=["query"])

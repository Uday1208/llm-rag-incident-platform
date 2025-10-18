"""
File: main.py
Purpose: Application entrypoint for the API Gateway service. Wires routers, logging, and telemetry.
"""

from fastapi import FastAPI
from contextlib import asynccontextmanager
from .logging_setup import configure_logging
from .instrumentation import setup_metrics
from .routers import health, metrics, query

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage app startup/shutdown lifecycle (connect pools, warm caches, etc.)."""
    configure_logging()
    setup_metrics(app)
    # TODO: initialize outbound clients (RAG worker, TorchServe), connection pools
    yield
    # TODO: graceful cleanup (close pools, flush metrics)

app = FastAPI(title="LLM RAG Incident Platform - API Gateway", lifespan=lifespan)

# Routers
app.include_router(health.router, prefix="", tags=["system"])
app.include_router(metrics.router, prefix="", tags=["system"])
app.include_router(query.router, prefix="/v1", tags=["query"])

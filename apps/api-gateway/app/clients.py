"""
File: clients.py
Purpose: Initialize and manage shared async clients (HTTP and Redis).
"""

import ssl
import httpx
from fastapi import FastAPI
from redis.asyncio import Redis
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from .config import settings

class DownstreamError(Exception):
    """Raised for downstream service errors."""

async def init_clients(app: FastAPI) -> None:
    """Create shared async clients and attach to app.state."""
    limits = httpx.Limits(max_keepalive_connections=settings.HTTP_MAX_KEEPALIVE,
                          max_connections=settings.HTTP_MAX_CONNECTIONS)
    timeout = httpx.Timeout(settings.HTTP_TIMEOUT_SECS)
    app.state.http = httpx.AsyncClient(limits=limits, timeout=timeout)
    
    # Instrument HTTPX to propagate trace context to downstream services
    HTTPXClientInstrumentor.instrument_client(app.state.http)

    # Redis TLS if configured
    ssl_ctx = None
    if settings.REDIS_SSL:
        ssl_ctx = ssl.create_default_context()
    app.state.redis = Redis(
        host=settings.REDIS_HOST,
        port=settings.REDIS_PORT,
        password=settings.REDIS_PASSWORD or None,
        ssl=settings.REDIS_SSL,
        ssl_cert_reqs=None if settings.REDIS_SSL else None,  # ACA managed TLS
        ssl_ca_certs=None if settings.REDIS_SSL else None,
        socket_timeout=5,
        decode_responses=True
    )

async def close_clients(app: FastAPI) -> None:
    """Close shared async clients on shutdown."""
    if hasattr(app.state, "http"):
        await app.state.http.aclose()
    if hasattr(app.state, "redis"):
        await app.state.redis.aclose()

@retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential(multiplier=0.3, min=0.3, max=2),
    retry=retry_if_exception_type(httpx.HTTPError),
    reraise=True,
)
async def call_rag_worker(http: httpx.AsyncClient, payload: dict) -> dict:
    """Call rag-worker /v1/rag to perform end-to-end RAG (embed+retrieve+generate)."""
    url = f"{settings.RAG_WORKER_URL}/v1/rag"
    resp = await http.post(url, json=payload, headers={"Content-Type": "application/json"})
    resp.raise_for_status()
    return resp.json()

@retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential(multiplier=0.3, min=0.3, max=2),
    retry=retry_if_exception_type(httpx.HTTPError),
    reraise=True,
)
async def call_ts_score(http: httpx.AsyncClient, lines: list[str]) -> dict:
    """Optionally call TorchServe to score log lines for anomaly signals."""
    if not settings.TS_MODEL_URL:
        return {"scores": None}
    url = f"{settings.TS_MODEL_URL}/predictions/log_anom"
    resp = await http.post(url, json={"lines": lines}, headers={"Content-Type": "application/json"})
    resp.raise_for_status()
    return resp.json()

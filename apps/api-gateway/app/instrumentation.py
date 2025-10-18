"""
File: instrumentation.py
Purpose: Expose Prometheus metrics and (optionally) OTEL tracing.
"""

from fastapi import FastAPI
from prometheus_client import Counter, Histogram

REQUESTS = Counter("api_requests_total", "Total API requests", ["route", "method", "status"])
LATENCY = Histogram("api_latency_seconds", "API latency in seconds", ["route"])

def setup_metrics(app: FastAPI) -> None:
    """Attach instrumentation objects to app state (optional)."""
    app.state.metrics = {"requests": REQUESTS, "latency": LATENCY}

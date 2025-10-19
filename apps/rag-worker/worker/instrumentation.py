"""
File: instrumentation.py
Purpose: Prometheus metrics for the worker.
"""

from fastapi import FastAPI
from prometheus_client import Counter, Histogram

REQUESTS = Counter("rag_requests_total", "Total RAG worker requests", ["route", "method", "status"])
LATENCY  = Histogram("rag_latency_seconds", "RAG worker latency", ["route"])

# Keep EMB_TIME label-free (we use EMB_TIME.time() directly with no labels)
EMB_TIME = Histogram("embed_seconds", "Time to embed text")

# ✅ Add a label dimension here because we call DB_TIME.labels(route="...").time()
DB_TIME  = Histogram("db_seconds", "Time for DB operations", ["route"])

def setup_metrics(app: FastAPI) -> None:
    """Attach instrumentation objects if needed."""
    app.state.metrics = {"requests": REQUESTS, "latency": LATENCY, "embed": EMB_TIME, "db": DB_TIME}

"""
File: instrumentation.py
Purpose: Central Prometheus metrics for rag-worker.

Exports:
  - REQ_COUNTER(route): count HTTP requests per logical route
  - DB_TIME(route): DB timing histogram per logical route
  - EMBED_TIME(phase): embedding timing histogram (phase="encode")
  - CACHE_HITS / CACHE_MISSES: counters for embedding cache efficiency
  - CACHE_ERRORS(kind): counter with kind in {"get","set"} for cache failures

Also:
  - setup_metrics(app): attaches metric objects to app.state.metrics for introspection
"""

#from __future__ import annotations

from fastapi import FastAPI
from prometheus_client import Counter, Histogram

# -------------------------
# Metric definitions
# -------------------------

# Count requests by logical route (example routes: "ingest", "rag", "health")
REQ_COUNTER = Counter(
    "requests_total",
    "HTTP requests by logical route",
    ["route"],
)

# DB latency per route (wrap DB calls with: with DB_TIME.labels(route="search").time(): ...)
DB_TIME = Histogram(
    "db_seconds",
    "DB time per route",
    ["route"],
)

# --- Back-compat aliases so existing imports keep working ---
REQUESTS = REQ_COUNTER  # legacy name used by main.py
LATENCY  = DB_TIME      # legacy name used by main.py

# Embedding timing (wrap model.encode with: with EMBED_TIME.labels(phase="encode").time(): ...)
EMBED_TIME = Histogram(
    "embed_seconds",
    "Embedding stages",
    ["phase"],  # e.g., "encode"
)

# Cache effectiveness
CACHE_HITS = Counter(
    "embed_cache_hits_total",
    "Embedding cache hits",
)

CACHE_MISSES = Counter(
    "embed_cache_misses_total",
    "Embedding cache misses",
)

# Cache errors (label `kind` in {"get","set"})
CACHE_ERRORS = Counter(
    "embed_cache_errors_total",
    "Embedding cache errors",
    ["kind"],
)

# -------------------------
# App wiring helper
# -------------------------

def setup_metrics(app: FastAPI) -> None:
    """
    Attach metric objects to app.state.metrics so other modules can introspect
    without importing symbols directly. Safe to call multiple times.
    NOTE: All metrics are already registered on the default Prometheus registry.
    """
    # Ensure the attribute exists
    if not hasattr(app.state, "metrics") or not isinstance(getattr(app.state, "metrics"), dict):
        app.state.metrics = {}

    # Populate/refresh the dictionary
    app.state.metrics.update({
        "requests_total": REQ_COUNTER,
        "db_seconds": DB_TIME,
        "embed_seconds": EMBED_TIME,
        "embed_cache_hits_total": CACHE_HITS,
        "embed_cache_misses_total": CACHE_MISSES,
        "embed_cache_errors_total": CACHE_ERRORS,
    })

"""
File: instrumentation.py
Purpose: Prometheus metrics collectors used across the worker

Exports:
  - REQ_COUNTER(route): count HTTP requests per logical route
  - DB_TIME(route): DB timing histogram per logical route
  - EMBED_TIME(phase): embedding timing histogram (phase="encode")
  - CACHE_HITS / CACHE_MISSES: counters for embedding cache efficiency
  - CACHE_ERRORS(kind): counter with kind in {"get","set"} for cache failures

"""

from prometheus_client import Counter, Histogram, CollectorRegistry, CONTENT_TYPE_LATEST, generate_latest

# Single-process default registry is fine for ACA
REGISTRY = CollectorRegistry(auto_describe=True)

# Matches middleware usage: REQUESTS.labels(route=..., method=..., status=...)
REQUESTS = Counter(
    "http_requests_total",
    "Total HTTP requests",
    labelnames=["route", "method", "status"],
    registry=REGISTRY,
)

# Matches middleware usage: LATENCY.labels(route=..., method=...).observe(...)
LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    labelnames=["route", "method"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
    registry=REGISTRY,
)

# Matches repository/db usage in your code: DB_TIME.labels(route="schema").time(), etc.
DB_TIME = Histogram(
    "db_seconds",
    "DB operation durations in seconds",
    labelnames=["route"],   # IMPORTANT: use 'route' because your code calls labels(route="...")
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2),
    registry=REGISTRY,
)

# Optional cache error counter (only if you reference it; otherwise harmless)
CACHE_ERRORS = Counter(
    "cache_errors_total",
    "Redis cache errors (get/set)",
    labelnames=["kind"],  # 'get' or 'set'
    registry=REGISTRY,
)

def setup_metrics(app):
    """Attach registry to app.state for /metrics endpoint to read."""
    app.state.prom_registry = REGISTRY

def render_metrics():
    """Return (content_type, payload) for a Starlette/FastAPI Response."""
    return CONTENT_TYPE_LATEST, generate_latest(REGISTRY)

"""
File: instrumentation.py
Purpose: Prometheus metrics used across worker.

Exports:
  - REQ_COUNTER(route): count HTTP requests per logical route
  - DB_TIME(route): DB timing histogram per logical route
  - EMBED_TIME(phase): embedding timing histogram (phase="encode")
  - CACHE_HITS / CACHE_MISSES: counters for embedding cache efficiency
  - CACHE_ERRORS(kind): counter with kind in {"get","set"} for cache failures

NOTE:
- Label names MUST match how they're used in middleware/repository code:
  * REQUESTS.labels(route=..., method=..., status=...)
  * LATENCY.labels(route=..., method=...)
  * DB_TIME.labels(route=...)
"""

from prometheus_client import Counter, Histogram, REGISTRY, CONTENT_TYPE_LATEST, generate_latest

# Matches: REQUESTS.labels(route=..., method=..., status=...).inc()
REQUESTS = Counter(
    "http_requests_total",
    "Total HTTP requests",
    labelnames=["route", "method", "status"],
)

# Matches: LATENCY.labels(route=..., method=...).observe(duration)
LATENCY = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    labelnames=["route", "method"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
)

# Matches: DB_TIME.labels(route="...").time() blocks in db/repository code
DB_TIME = Histogram(
    "db_seconds",
    "DB operation durations in seconds",
    labelnames=["route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2),
)

# Optional: only if your code references it. Safe to keep even if unused.
CACHE_ERRORS = Counter(
    "cache_errors_total",
    "Redis cache errors (get/set)",
    labelnames=["kind"],  # 'get' or 'set'
)

def setup_metrics(app):
    """Optional: expose metrics handles on app.state for debugging/ops."""
    app.state.metrics = {
        "REQUESTS": REQUESTS,
        "LATENCY": LATENCY,
        "DB_TIME": DB_TIME,
        "CACHE_ERRORS": CACHE_ERRORS,
        "registry": REGISTRY,
        "render": lambda: (CONTENT_TYPE_LATEST, generate_latest(REGISTRY)),
    }

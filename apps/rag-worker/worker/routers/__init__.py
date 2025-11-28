"""Router registry for rag-worker (health, ingest, and internal-embed)."""

from .health import router as health_router
from .ingest import router as ingest_router
from .embed_api import router as embed_router  # internal embed endpoint
from .internal_search import router as internal_search_router

routers = [
    health_router,
    ingest_router,
    embed_router,
    internal_search_router
]

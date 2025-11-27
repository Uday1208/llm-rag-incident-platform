"""Router registry for reasoning-agent (health, reason, search)."""

#from .health import router as health_router
from .reason import router as reason_router
from .search import router as search_router

routers = [
    #health_router,
    reason_router,
    search_router,
]

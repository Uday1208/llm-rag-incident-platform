"""Router registry for reasoning-agent (health, reason, search)."""

#from .health import router as health_router
#from .reason import router as reason_router
#from .search import router as search_router
from routers.reason import router as reason_router
from routers.search import router as search_router
from routers.llm import router as llm_router

routers = [
    reason_router,
    search_router,
    llm_router,
]

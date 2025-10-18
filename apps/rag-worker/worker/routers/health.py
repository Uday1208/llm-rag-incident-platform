"""
File: routers/health.py
Purpose: Liveness and readiness probes for RAG Worker.
"""

from fastapi import APIRouter

router = APIRouter()

@router.get("/health")
def health() -> dict:
    """Return service health status for liveness probes."""
    return {"status": "ok"}

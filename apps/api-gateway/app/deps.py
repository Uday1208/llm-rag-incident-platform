"""
File: deps.py
Purpose: Dependency injection helpers (e.g., Redis client, auth check).
"""

import os
from fastapi import Header, HTTPException, status
from typing import Optional
from .config import settings

def require_api_key(x_api_key: Optional[str] = Header(None)) -> None:
    """Validate inbound API key for protected endpoints."""
    if settings.API_KEY and x_api_key != settings.API_KEY:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")

# (Later) Redis connection pool, outbound clients, etc.
# def get_redis() -> redis.Redis: ...
# def get_http_client() -> httpx.AsyncClient: ...

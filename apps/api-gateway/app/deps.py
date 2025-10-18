"""
File: deps.py
Purpose: Dependency helpers (auth guard).
"""

from typing import Optional
from fastapi import Header, HTTPException, status
from .config import settings

def require_api_key(x_api_key: Optional[str] = Header(None)) -> None:
    """Validate inbound API key for protected endpoints."""
    if settings.API_KEY and x_api_key != settings.API_KEY:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")

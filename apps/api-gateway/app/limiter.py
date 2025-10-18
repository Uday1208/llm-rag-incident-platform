"""
File: limiter.py
Purpose: Redis-backed, fixed-window rate limiter for API requests.
"""

import time
from typing import Optional
from redis.asyncio import Redis
from .config import settings

async def allow_request(r: Redis, key: str) -> bool:
    """Return True if the request is allowed under rate limit, else False."""
    if not settings.RL_ENABLED:
        return True
    window = settings.RL_WINDOW_SECS
    max_req = settings.RL_REQUESTS
    now_window = int(time.time() // window)
    redis_key = f"rl:{key}:{now_window}"
    # INCR with expiry creates the fixed window
    count = await r.incr(redis_key)
    if count == 1:
        await r.expire(redis_key, window)
    return count <= max_req

def limiter_key(api_key: Optional[str], ip: str) -> str:
    """Compute limiter key based on API key or IP address."""
    if settings.RL_KEY_BY_IP or not api_key:
        return f"ip:{ip}"
    return f"key:{api_key}"

"""
File: services/ts_client.py
Purpose: Thin client to call TorchServe anomaly endpoint with timeouts & logs.
"""

import os
import json
import httpx
import logging
from typing import List, Optional

_logger = logging.getLogger(__name__)
_TS_URL = os.getenv("TS_MODEL_URL", "").rstrip("/")
_TIMEOUT = float(os.getenv("TS_HTTP_TIMEOUT", "5"))  # seconds


async def score_lines(lines: List[str]) -> Optional[float]:
    """Return mean anomaly score for lines, or None if unavailable/fails."""
    if not _TS_URL:
        _logger.info("TS_MODEL_URL not set; skipping ts-model call")
        return None
    if not lines:
        return None

    url = f"{_TS_URL}/predictions/log_anom"
    payload = {"lines": [str(x) for x in lines]}
    try:
        # Send raw JSON bytes; follow redirects to survive http->https
        content = json.dumps(payload).encode("utf-8")
        async with httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=True) as client:
            r = await client.post(url, content=content, headers={"Content-Type": "application/json"})
        r.raise_for_status()
        data = r.json()
        scores = data.get("scores") or []
        if not scores:
            _logger.debug("ts-model returned empty scores: %s", data)
            return None
        mean = float(sum(scores) / len(scores))
        _logger.debug("ts-model mean anomaly score=%.4f", mean)
        return mean
    except Exception as e:
        _logger.warning("ts-model call failed: %s", e)
        return None

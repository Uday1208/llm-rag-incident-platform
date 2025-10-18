"""
File: tests/test_smoke.py
Purpose: Minimal smoke test to ensure the app starts and /health works.
"""

from fastapi.testclient import TestClient
from worker.main import app

def test_health_ok():
    """Verify health endpoint returns ok."""
    c = TestClient(app)
    r = c.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"

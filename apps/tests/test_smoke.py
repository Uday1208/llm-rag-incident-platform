"""
File: tests/test_smoke.py
Purpose: Minimal smoke test to ensure the app starts and /health works.
"""

from fastapi.testclient import TestClient
from app.main import app

def test_health_ok():
    """Verify health endpoint returns ok."""
    client = TestClient(app)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"

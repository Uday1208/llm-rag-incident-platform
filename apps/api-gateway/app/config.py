"""
File: config.py
Purpose: Centralized configuration using environment variables (12-factor).
"""

from pydantic import BaseSettings, AnyUrl

class Settings(BaseSettings):
    """Load service configuration from environment variables."""
    ENV: str = "prod"
    SERVICE_NAME: str = "api-gateway"
    LOG_LEVEL: str = "INFO"

    # Downstream services
    RAG_WORKER_URL: AnyUrl = "http://rag-worker:8081"
    TS_MODEL_URL: str = ""  # optional; when set, gateway will enrich with anomaly scoring

    # Redis (rate limit / cache)
    REDIS_HOST: str = ""
    REDIS_PORT: int = 6380
    REDIS_SSL: bool = True
    REDIS_PASSWORD: str = ""

    # Security
    API_KEY: str = ""  # simple shared secret for gateway ingress (optional for POC)

    # HTTP client tuning
    HTTP_TIMEOUT_SECS: float = 10.0
    HTTP_MAX_KEEPALIVE: int = 100
    HTTP_MAX_CONNECTIONS: int = 100

    # Rate limiting (fixed window)
    RL_ENABLED: bool = True
    RL_REQUESTS: int = 60         # max requests
    RL_WINDOW_SECS: int = 60      # per window
    RL_KEY_BY_IP: bool = False    # if False, rate-limit by API key

    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()

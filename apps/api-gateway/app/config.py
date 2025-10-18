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

    # Downstream endpoints (to be set after containers are up)
    RAG_WORKER_URL: AnyUrl = "http://rag-worker:8081"
    TS_MODEL_URL: AnyUrl = "http://ts-model:8082"

    # Redis (rate limit / cache) – host may be private; use ACA secrets + env binding
    REDIS_HOST: str = ""
    REDIS_PORT: int = 6380
    REDIS_SSL: bool = True
    REDIS_PASSWORD: str = ""

    # Security
    API_KEY: str = ""  # simple shared secret for gateway ingress (optional for POC)

    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()

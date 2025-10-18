"""
File: config.py
Purpose: Centralized configuration using environment variables (12-factor).
"""

from pydantic import BaseSettings

class Settings(BaseSettings):
    """Load service configuration from environment variables."""
    ENV: str = "prod"
    SERVICE_NAME: str = "rag-worker"
    LOG_LEVEL: str = "INFO"

    # Postgres (Azure Flexible Server; pgvector enabled)
    PG_HOST: str
    PG_DB: str = "ragincdb"
    PG_USER: str
    PG_PASS: str
    PG_SSLMODE: str = "require"
    PG_POOL_MIN: int = 1
    PG_POOL_MAX: int = 10

    # Redis cache for embeddings
    REDIS_HOST: str = ""
    REDIS_PORT: int = 6380
    REDIS_SSL: bool = True
    REDIS_PASSWORD: str = ""
    REDIS_TTL_SECS: int = 86400

    # Embeddings / LLM
    EMBED_MODEL_NAME: str = "sentence-transformers/all-MiniLM-L6-v2"  # 384-dim
    MODEL_ID: str = ""  # optional transformers generation model; if empty => no generation
    MAX_CONTEXT_CHARS: int = 3000
    TOP_K_DEFAULT: int = 5

    # ...existing...
    USE_LANGCHAIN_STORE: bool = False  # toggle LC vector store
    LC_COLLECTION: str = "documents_lc"  # table/collection name for LC store

    # Optional: buildable SQLAlchemy URL (LangChain needs this)
    @property
    def PG_SQLALCHEMY_URL(self) -> str:
        """Return SQLAlchemy URL for pgvector (psycopg2)."""
        return f"postgresql+psycopg2://{self.PG_USER}:{self.PG_PASS}@{self.PG_HOST}/{self.PG_DB}"

    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()

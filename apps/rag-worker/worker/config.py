"""
Centralized configuration for the RAG worker.

All settings are loaded from environment variables with sensible defaults.
"""

import os
from typing import Optional
from dataclasses import dataclass
from enum import Enum


class EmbeddingProvider(str, Enum):
    """Supported embedding providers."""
    LOCAL = "local"           # SentenceTransformers (default)
    AZURE_OPENAI = "azure"    # Azure OpenAI
    OPENAI = "openai"         # OpenAI API


class LLMProvider(str, Enum):
    """Supported LLM providers."""
    OLLAMA = "ollama"         # Ollama (default)
    AZURE_OPENAI = "azure"    # Azure OpenAI
    OPENAI = "openai"         # OpenAI API


# =============================================================================
# Embedding Models Configuration
# =============================================================================

EMBEDDING_MODELS = {
    # Local SentenceTransformers models
    "all-MiniLM-L6-v2": {"dim": 384, "provider": "local"},
    "all-mpnet-base-v2": {"dim": 768, "provider": "local"},
    "paraphrase-multilingual-MiniLM-L12-v2": {"dim": 384, "provider": "local"},
    
    # Azure OpenAI models
    "text-embedding-ada-002": {"dim": 1536, "provider": "azure"},
    "text-embedding-3-small": {"dim": 1536, "provider": "azure"},
    "text-embedding-3-large": {"dim": 3072, "provider": "azure"},
}


@dataclass
class EmbeddingConfig:
    """Configuration for embedding generation."""
    provider: EmbeddingProvider
    model_name: str
    dimension: int
    batch_size: int
    
    # Azure OpenAI specific
    azure_endpoint: Optional[str] = None
    azure_api_key: Optional[str] = None
    azure_api_version: str = "2024-02-01"
    azure_deployment: Optional[str] = None
    
    # OpenAI specific
    openai_api_key: Optional[str] = None
    
    # Caching
    cache_ttl_seconds: int = 3600
    
    @classmethod
    def from_env(cls) -> "EmbeddingConfig":
        """Load configuration from environment variables."""
        provider_str = os.getenv("EMBED_PROVIDER", "local").lower()
        provider = EmbeddingProvider(provider_str) if provider_str in [e.value for e in EmbeddingProvider] else EmbeddingProvider.LOCAL
        
        # Model name (with defaults per provider)
        if provider == EmbeddingProvider.LOCAL:
            model_name = os.getenv("EMBED_MODEL_NAME", "all-MiniLM-L6-v2")
        elif provider == EmbeddingProvider.AZURE_OPENAI:
            model_name = os.getenv("EMBED_MODEL_NAME", "text-embedding-ada-002")
        else:
            model_name = os.getenv("EMBED_MODEL_NAME", "text-embedding-ada-002")
        
        # Get dimension from model config or env override
        model_info = EMBEDDING_MODELS.get(model_name, {})
        dimension = int(os.getenv("EMBED_DIM", str(model_info.get("dim", 384))))
        
        return cls(
            provider=provider,
            model_name=model_name,
            dimension=dimension,
            batch_size=int(os.getenv("EMBED_BATCH_SIZE", "32")),
            
            # Azure OpenAI
            azure_endpoint=os.getenv("AOAI_ENDPOINT"),
            azure_api_key=os.getenv("AOAI_KEY"),
            azure_api_version=os.getenv("AOAI_API_VERSION", "2024-02-01"),
            azure_deployment=os.getenv("AOAI_EMBED_DEPLOYMENT"),
            
            # OpenAI
            openai_api_key=os.getenv("OPENAI_API_KEY"),
            
            # Caching
            cache_ttl_seconds=int(os.getenv("EMBED_CACHE_TTL", "3600")),
        )


@dataclass
class LLMConfig:
    """Configuration for LLM chat/completion."""
    provider: LLMProvider
    model_name: str
    temperature: float
    max_tokens: int
    timeout_seconds: float
    
    # Ollama specific
    ollama_base_url: Optional[str] = None
    
    # Azure OpenAI specific
    azure_endpoint: Optional[str] = None
    azure_api_key: Optional[str] = None
    azure_api_version: str = "2024-02-01"
    azure_deployment: Optional[str] = None
    
    # OpenAI specific
    openai_api_key: Optional[str] = None
    
    @classmethod
    def from_env(cls) -> "LLMConfig":
        """Load configuration from environment variables."""
        provider_str = os.getenv("LLM_PROVIDER", "ollama").lower()
        provider = LLMProvider(provider_str) if provider_str in [e.value for e in LLMProvider] else LLMProvider.OLLAMA
        
        # Model name (with defaults per provider)
        if provider == LLMProvider.OLLAMA:
            model_name = os.getenv("LLM_MODEL", "qwen2.5:7b")
        elif provider == LLMProvider.AZURE_OPENAI:
            model_name = os.getenv("LLM_MODEL", "gpt-4o-mini")
        else:
            model_name = os.getenv("LLM_MODEL", "gpt-4o-mini")
        
        return cls(
            provider=provider,
            model_name=model_name,
            temperature=float(os.getenv("LLM_TEMPERATURE", "0.3")),
            max_tokens=int(os.getenv("LLM_MAX_TOKENS", "1024")),
            timeout_seconds=float(os.getenv("LLM_TIMEOUT", "60")),
            
            # Ollama
            ollama_base_url=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
            
            # Azure OpenAI
            azure_endpoint=os.getenv("AOAI_ENDPOINT"),
            azure_api_key=os.getenv("AOAI_KEY"),
            azure_api_version=os.getenv("AOAI_API_VERSION", "2024-02-01"),
            azure_deployment=os.getenv("AOAI_CHAT_DEPLOYMENT"),
            
            # OpenAI
            openai_api_key=os.getenv("OPENAI_API_KEY"),
        )


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    host: str
    port: int
    database: str
    user: str
    password: str
    sslmode: str
    pool_min: int
    pool_max: int
    vector_dim: int  # Must match embedding dimension
    
    @classmethod
    def from_env(cls) -> "DatabaseConfig":
        """Load configuration from environment variables."""
        # Get embedding config to sync vector dimension
        embed_config = EmbeddingConfig.from_env()
        
        return cls(
            host=os.getenv("PG_HOST", "localhost"),
            port=int(os.getenv("PG_PORT", "5432")),
            database=os.getenv("PG_DB", "ragincdb"),
            user=os.getenv("PG_USER", "postgres"),
            password=os.getenv("PG_PASS", ""),
            sslmode=os.getenv("PG_SSLMODE", "require"),
            pool_min=int(os.getenv("DB_POOL_MIN", "1")),
            pool_max=int(os.getenv("DB_POOL_MAX", "5")),
            vector_dim=embed_config.dimension,
        )
    
    @property
    def dsn(self) -> str:
        """Build psycopg2 DSN string."""
        return (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password} sslmode={self.sslmode}"
        )
    
    @property
    def sqlalchemy_url(self) -> str:
        """Build SQLAlchemy URL."""
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}?sslmode={self.sslmode}"


# =============================================================================
# Redis Configuration
# =============================================================================

@dataclass
class RedisConfig:
    """Redis cache configuration."""
    host: Optional[str]
    port: int
    password: Optional[str]
    ssl: bool
    db: int
    
    @classmethod
    def from_env(cls) -> "RedisConfig":
        return cls(
            host=os.getenv("REDIS_HOST") or os.getenv("REDIS_HOSTNAME"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            password=os.getenv("REDIS_PASSWORD") or os.getenv("REDIS_KEY"),
            ssl=os.getenv("REDIS_SSL", "false").lower() in ("1", "true", "yes"),
            db=int(os.getenv("REDIS_DB", "0")),
        )
    
    @property
    def enabled(self) -> bool:
        return bool(self.host)


# =============================================================================
# Global Config Instance
# =============================================================================

@dataclass
class GlobalConfig:
    """Aggregated configuration."""
    embedding: EmbeddingConfig
    llm: LLMConfig
    database: DatabaseConfig
    redis: RedisConfig
    USE_LANGCHAIN_STORE: bool
    LOG_LEVEL: str

    @classmethod
    def load(cls) -> "GlobalConfig":
        return cls(
            embedding=EmbeddingConfig.from_env(),
            llm=LLMConfig.from_env(),
            database=DatabaseConfig.from_env(),
            redis=RedisConfig.from_env(),
            USE_LANGCHAIN_STORE=os.getenv("USE_LANGCHAIN_STORE", "false").lower() == "true",
            LOG_LEVEL=os.getenv("LOG_LEVEL", "INFO").upper(),
        )

    @property
    def VECTOR_SQLTYPE(self) -> str:
        """Get pgvector SQL type definition (e.g., 'vector(384)')."""
        return f"vector({self.embedding.dimension})"

# Export global settings instance
settings = GlobalConfig.load()

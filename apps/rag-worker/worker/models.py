import os
from datetime import datetime
from typing import Optional, List, Dict, Any
from sqlalchemy import (
    String, Text, Integer, Float, Boolean,
    DateTime, ForeignKey, Index, CheckConstraint,
    JSON, Enum as SQLEnum
)

# Configurable vector dimension (must match embedding model)
VECTOR_DIM = int(os.getenv("EMBED_DIM", "384"))
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import JSONB
from pgvector.sqlalchemy import Vector


class Base(DeclarativeBase):
    """Base class for all models."""
    pass


# =============================================================================
# Enums
# =============================================================================

class SeverityLevel:
    """Severity levels matching Production PG constraints."""
    SEV1 = "SEV1" # Critical
    SEV2 = "SEV2" # Error
    SEV3 = "SEV3" # Warning
    SEV4 = "SEV4" # Info


class AgentSessionStatus:
    """Status of an agent resolution session."""
    ACTIVE = "active"
    RESOLVED = "resolved"
    TIMEOUT = "timeout"
    FAILED = "failed"


# =============================================================================
# Core Tables
# =============================================================================


class Resolution(Base):
    """
    Resolution record linked to an incident.
    Stores runbook steps and effectiveness feedback.
    """
    __tablename__ = "resolutions"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # Link to incident (actual table uses incident_id)
    incident_id: Mapped[Optional[str]] = mapped_column(
        String(64), 
        index=True
    )
    
    # Resolution content
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    steps: Mapped[Optional[Dict]] = mapped_column(JSONB)  # Structured runbook
    root_cause: Mapped[Optional[str]] = mapped_column(Text)
    preventive_action: Mapped[Optional[str]] = mapped_column(Text)
    
    # Vector embedding for resolution search
    embedding: Mapped[List[float]] = mapped_column(Vector(VECTOR_DIM), nullable=True)
    
    # Metadata
    created_by: Mapped[str] = mapped_column(String(128), default="system")
    effectiveness: Mapped[Optional[int]] = mapped_column(
        Integer,
        CheckConstraint("effectiveness BETWEEN 1 AND 5")
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        default=datetime.utcnow
    )
    
    # Tags for categorization
    tags: Mapped[Optional[List[str]]] = mapped_column(JSONB)



class AgentSession(Base):
    """
    Stores agent conversation history for the resolution agent.
    Enables session continuity and debugging.
    """
    __tablename__ = "agent_sessions"
    
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    
    # Query and status
    user_query: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(
        String(16), 
        default=AgentSessionStatus.ACTIVE,
        index=True
    )
    
    # Execution details
    iterations: Mapped[int] = mapped_column(Integer, default=0)
    memory: Mapped[Optional[Dict]] = mapped_column(JSONB)  # Full ReAct trace
    
    # Results
    result: Mapped[Optional[str]] = mapped_column(Text)
    cited_bundles: Mapped[Optional[List[str]]] = mapped_column(JSONB)
    cited_resolutions: Mapped[Optional[List[int]]] = mapped_column(JSONB)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )
    
    # User feedback
    feedback_helpful: Mapped[Optional[bool]] = mapped_column(Boolean)
    feedback_comment: Mapped[Optional[str]] = mapped_column(Text)


# =============================================================================
# Legacy Table (Backward Compatibility)
# =============================================================================

class Document(Base):
    """
    Legacy documents table for backward compatibility.
    Use IncidentBundle for new implementations.
    """
    __tablename__ = "documents"
    
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    source: Mapped[Optional[str]] = mapped_column(String(256))
    ts: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    content: Mapped[str] = mapped_column(Text, nullable=False)
    severity: Mapped[Optional[str]] = mapped_column(String(16))
    embedding: Mapped[Optional[List[float]]] = mapped_column(Vector(VECTOR_DIM))
    
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        default=datetime.utcnow
    )


# =============================================================================
# Utility Tables
# =============================================================================

class EmbeddingCache(Base):
    """
    Cache for computed embeddings to avoid recomputation.
    """
    __tablename__ = "embedding_cache"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    text_hash: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    model: Mapped[str] = mapped_column(String(64))  # e.g., text-embedding-ada-002
    embedding: Mapped[List[float]] = mapped_column(Vector(VECTOR_DIM), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        default=datetime.utcnow
    )


class ServiceHealth(Base):
    """
    Track service health status for the preprocessing pipeline.
    """
    __tablename__ = "service_health"
    
    service_name: Mapped[str] = mapped_column(String(64), primary_key=True)
    last_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    incident_count_24h: Mapped[int] = mapped_column(Integer, default=0)
    avg_severity: Mapped[Optional[float]] = mapped_column(Float)
    status: Mapped[str] = mapped_column(String(16), default="healthy")  # healthy, degraded, down

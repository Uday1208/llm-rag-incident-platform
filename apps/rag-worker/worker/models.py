"""
SQLAlchemy models for the LLM-RAG Incident Platform.

Tables:
- incident_bundles: TraceID-correlated incident groups with embeddings
- resolutions: Past resolutions linked to incidents
- agent_sessions: Agentic conversation history
- documents: Legacy table (backward compatibility)

Vector dimension is configurable via EMBED_DIM env var (default: 384 for MiniLM).
"""

import os
from datetime import datetime
from typing import Optional, List, Dict, Any
from sqlalchemy import (
    String, Text, Integer, Float, Boolean,
    DateTime, ForeignKey, Index, CheckConstraint,
    JSON, Enum as SQLEnum
)

# Configurable vector dimension (must match embedding model)
# Default: 384 for all-MiniLM-L6-v2
# Set to 1536 for text-embedding-ada-002
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
    """Severity levels matching Application Insights."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class AgentSessionStatus:
    """Status of an agent resolution session."""
    ACTIVE = "active"
    RESOLVED = "resolved"
    TIMEOUT = "timeout"
    FAILED = "failed"


# =============================================================================
# Core Tables
# =============================================================================

class IncidentBundle(Base):
    """
    TraceID-correlated incident bundle.
    Groups related log entries from a distributed trace.
    """
    __tablename__ = "incident_bundles"
    
    # Primary key
    id: Mapped[str] = mapped_column(String(64), primary_key=True)
    
    # Trace context (from Application Insights)
    trace_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    span_id: Mapped[Optional[str]] = mapped_column(String(64))
    
    # Service identification
    service: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    operation: Mapped[Optional[str]] = mapped_column(String(256))
    environment: Mapped[Optional[str]] = mapped_column(String(32))
    
    # Severity
    severity: Mapped[str] = mapped_column(
        String(16), 
        nullable=False, 
        index=True,
        default=SeverityLevel.INFO
    )
    
    # Structured summary (GPT-4o-mini generated)
    symptoms: Mapped[Optional[str]] = mapped_column(Text)
    failing_dependency: Mapped[Optional[str]] = mapped_column(String(256))
    error_signature: Mapped[Optional[str]] = mapped_column(String(512))
    
    # Full content for search
    content: Mapped[str] = mapped_column(Text, nullable=False)
    
    # Vector embedding (dimension from EMBED_DIM env var)
    # Default: 384 for MiniLM, 1536 for Azure OpenAI
    embedding: Mapped[Optional[List[float]]] = mapped_column(Vector(VECTOR_DIM))
    
    # Metadata
    log_count: Mapped[int] = mapped_column(Integer, default=1)
    first_ts: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    last_ts: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        default=datetime.utcnow
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    
    # Source reference
    raw_blob_path: Mapped[Optional[str]] = mapped_column(String(512))
    
    # Additional metadata (flexible schema)
    metadata_: Mapped[Optional[Dict]] = mapped_column("metadata", JSONB)
    
    # Relationships
    resolutions: Mapped[List["Resolution"]] = relationship(
        back_populates="bundle",
        cascade="all, delete-orphan"
    )
    
    __table_args__ = (
        Index("idx_bundles_first_ts", "first_ts"),
        Index("idx_bundles_service_severity", "service", "severity"),
    )


class Resolution(Base):
    """
    Resolution record linked to an incident bundle.
    Stores runbook steps and effectiveness feedback.
    """
    __tablename__ = "resolutions"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    
    # Link to incident
    bundle_id: Mapped[Optional[str]] = mapped_column(
        String(64), 
        ForeignKey("incident_bundles.id", ondelete="SET NULL"),
        index=True
    )
    
    # Resolution content
    summary: Mapped[str] = mapped_column(Text, nullable=False)
    steps: Mapped[Optional[Dict]] = mapped_column(JSONB)  # Structured runbook
    root_cause: Mapped[Optional[str]] = mapped_column(Text)
    preventive_action: Mapped[Optional[str]] = mapped_column(Text)
    
    # Vector embedding for resolution search
    embedding: Mapped[Optional[List[float]]] = mapped_column(Vector(VECTOR_DIM))
    
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
    
    # Relationship
    bundle: Mapped[Optional["IncidentBundle"]] = relationship(
        back_populates="resolutions"
    )


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

"""Initial schema with incident bundles and resolutions

Revision ID: 001
Revises: None
Create Date: 2025-12-27

Vector dimension is configurable via EMBED_DIM environment variable.
Default: 384 (for all-MiniLM-L6-v2)
Set to 1536 for Azure OpenAI text-embedding-ada-002
"""

import os
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from pgvector.sqlalchemy import Vector


# revision identifiers
revision: str = "001_initial"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Get vector dimension from environment (default: 384 for MiniLM)
VECTOR_DIM = int(os.getenv("EMBED_DIM", "384"))


def upgrade() -> None:
    # Enable pgvector extension
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    
    # ==========================================================================
    # incident_bundles - TraceID-correlated incident groups
    # ==========================================================================
    op.create_table(
        "incident_bundles",
        sa.Column("id", sa.String(64), primary_key=True),
        sa.Column("trace_id", sa.String(64), nullable=False, index=True),
        sa.Column("span_id", sa.String(64), nullable=True),
        sa.Column("service", sa.String(128), nullable=False, index=True),
        sa.Column("operation", sa.String(256), nullable=True),
        sa.Column("environment", sa.String(32), nullable=True),
        sa.Column("severity", sa.String(16), nullable=False, index=True, default="INFO"),
        sa.Column("symptoms", sa.Text(), nullable=True),
        sa.Column("failing_dependency", sa.String(256), nullable=True),
        sa.Column("error_signature", sa.String(512), nullable=True),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("embedding", Vector(VECTOR_DIM), nullable=True),
        sa.Column("log_count", sa.Integer(), default=1),
        sa.Column("first_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("raw_blob_path", sa.String(512), nullable=True),
        sa.Column("metadata", postgresql.JSONB(), nullable=True),
    )
    
    # Additional indexes for incident_bundles
    op.create_index("idx_bundles_first_ts", "incident_bundles", ["first_ts"])
    op.create_index("idx_bundles_service_severity", "incident_bundles", ["service", "severity"])
    
    # Vector similarity index (IVFFlat)
    op.execute(f"""
        CREATE INDEX idx_bundles_embedding 
        ON incident_bundles USING ivfflat (embedding vector_cosine_ops) 
        WITH (lists = 10)
    """)
    
    # ==========================================================================
    # resolutions - Past resolutions linked to incidents
    # ==========================================================================
    op.create_table(
        "resolutions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("bundle_id", sa.String(64), sa.ForeignKey("incident_bundles.id", ondelete="SET NULL"), index=True),
        sa.Column("summary", sa.Text(), nullable=False),
        sa.Column("steps", postgresql.JSONB(), nullable=True),
        sa.Column("root_cause", sa.Text(), nullable=True),
        sa.Column("preventive_action", sa.Text(), nullable=True),
        sa.Column("embedding", Vector(VECTOR_DIM), nullable=True),
        sa.Column("created_by", sa.String(128), default="system"),
        sa.Column("effectiveness", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("tags", postgresql.JSONB(), nullable=True),
        sa.CheckConstraint("effectiveness BETWEEN 1 AND 5", name="check_effectiveness_range"),
    )
    
    # ==========================================================================
    # agent_sessions - Agentic conversation history
    # ==========================================================================
    op.create_table(
        "agent_sessions",
        sa.Column("id", sa.String(64), primary_key=True),
        sa.Column("user_query", sa.Text(), nullable=False),
        sa.Column("status", sa.String(16), default="active", index=True),
        sa.Column("iterations", sa.Integer(), default=0),
        sa.Column("memory", postgresql.JSONB(), nullable=True),
        sa.Column("result", sa.Text(), nullable=True),
        sa.Column("cited_bundles", postgresql.JSONB(), nullable=True),
        sa.Column("cited_resolutions", postgresql.JSONB(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("feedback_helpful", sa.Boolean(), nullable=True),
        sa.Column("feedback_comment", sa.Text(), nullable=True),
    )
    
    # ==========================================================================
    # embedding_cache - Cache for computed embeddings
    # ==========================================================================
    op.create_table(
        "embedding_cache",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("text_hash", sa.String(64), unique=True, index=True),
        sa.Column("model", sa.String(64), nullable=False),
        sa.Column("embedding", Vector(VECTOR_DIM), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    
    # ==========================================================================
    # service_health - Track service health status
    # ==========================================================================
    op.create_table(
        "service_health",
        sa.Column("service_name", sa.String(64), primary_key=True),
        sa.Column("last_seen", sa.DateTime(timezone=True), nullable=True),
        sa.Column("incident_count_24h", sa.Integer(), default=0),
        sa.Column("avg_severity", sa.Float(), nullable=True),
        sa.Column("status", sa.String(16), default="healthy"),
    )
    
    # ==========================================================================
    # documents - Legacy table for backward compatibility
    # ==========================================================================
    op.create_table(
        "documents",
        sa.Column("id", sa.String(64), primary_key=True),
        sa.Column("source", sa.String(256), nullable=True),
        sa.Column("ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("severity", sa.String(16), nullable=True),
        sa.Column("embedding", Vector(VECTOR_DIM), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    
    # Vector index for documents
    op.execute(f"""
        CREATE INDEX idx_documents_embedding 
        ON documents USING ivfflat (embedding vector_cosine_ops) 
        WITH (lists = 10)
    """)
    
    # ==========================================================================
    # v_incidents_display - View for clean display
    # ==========================================================================
    op.execute("""
        CREATE OR REPLACE VIEW v_incidents_display AS
        SELECT 
            ib.id,
            ib.trace_id,
            ib.service,
            ib.severity,
            ib.symptoms,
            ib.failing_dependency,
            ib.error_signature,
            ib.log_count,
            ib.first_ts,
            r.summary as resolution_summary,
            r.steps as resolution_steps
        FROM incident_bundles ib
        LEFT JOIN resolutions r ON r.bundle_id = ib.id
        ORDER BY ib.first_ts DESC
    """)


def downgrade() -> None:
    # Drop view first
    op.execute("DROP VIEW IF EXISTS v_incidents_display")
    
    # Drop tables in reverse order (respecting foreign keys)
    op.drop_table("documents")
    op.drop_table("service_health")
    op.drop_table("embedding_cache")
    op.drop_table("agent_sessions")
    op.drop_table("resolutions")
    op.drop_table("incident_bundles")

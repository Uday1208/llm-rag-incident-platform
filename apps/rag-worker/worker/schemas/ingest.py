"""
File: schemas/ingest.py
Purpose: Pydantic models for ingest API.
"""

from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime

class IngestDoc(BaseModel):
    """Single document with metadata and content."""
    id: str = Field(..., description="Unique ID for document")
    source: str = Field("", description="Source label")
    ts: Optional[datetime] = Field(None, description="Event timestamp")
    content: str = Field(..., description="Raw text content to index")

class IngestRequest(BaseModel):
    """Ingest request holding a list of documents."""
    documents: List[IngestDoc]

class IngestResponse(BaseModel):
    """Ingest response summarizing results."""
    upserted: int

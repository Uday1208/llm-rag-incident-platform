"""
File: rag.py
Purpose: Pydantic request/response models for /v1/rag.
"""

from pydantic import BaseModel, Field
from typing import List, Optional

class RAGRequest(BaseModel):
    """Query for retrieval and answer generation."""
    query: str = Field(..., description="User question or log snippet")
    top_k: int = Field(5, ge=1, le=50, description="Neighbors to retrieve")
    with_scores: bool = Field(True, description="Return similarity scores")

class RAGResponse(BaseModel):
    """RAG result containing answer, context ids, scores, and raw contexts."""
    answer: str
    context_ids: List[str]
    scores: Optional[List[float]] = None
    contexts: Optional[List[str]] = None

"""
File: schemas/query.py
Purpose: Pydantic request/response models for query flow.
"""

from pydantic import BaseModel, Field
from typing import List, Optional

class QueryRequest(BaseModel):
    """User query payload schema."""
    query: str = Field(..., description="Incident question or log snippet")
    top_k: int = Field(5, ge=1, le=50, description="Top-k retrieval size")
    with_scores: bool = Field(True, description="Return retrieval scores")

class QueryResponse(BaseModel):
    """Aggregated response including provenance and optional anomaly signal."""
    answer: str
    context_ids: List[str]
    scores: Optional[List[float]] = None
    anomaly_signal: Optional[float] = None

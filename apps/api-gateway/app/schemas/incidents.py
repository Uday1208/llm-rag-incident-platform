from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel

class IncidentSummary(BaseModel):
    id: str
    trace_id: str
    service: str
    severity: str
    symptoms: Optional[str] = None
    error_signature: Optional[str] = None
    first_ts: Optional[datetime] = None
    summary: Optional[str] = None
    title: Optional[str] = None
    status: Optional[str] = None
    owner: Optional[str] = None
    tags: Optional[List[str]] = None
    resolved_at: Optional[datetime] = None


class IncidentListResponse(BaseModel):
    incidents: List[IncidentSummary]

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
    summary: Optional[str] = None  # content or symptoms

class IncidentListResponse(BaseModel):
    incidents: List[IncidentSummary]

from fastapi import APIRouter
from worker.schemas.incidents import IncidentListResponse, IncidentSummary
from worker.repository import get_recent_bundles

router = APIRouter()

@router.get("/incidents", response_model=IncidentListResponse)
def list_incidents(limit: int = 50):
    """List recent incident bundles."""
    rows = get_recent_bundles(limit)
    incidents = []
    for r in rows:
        # id, trace_id, service, severity, symptoms, error_signature, first_ts, content
        incidents.append(IncidentSummary(
            id=r[0],
            trace_id=r[1],
            service=r[2],
            severity=r[3],
            symptoms=r[4],
            error_signature=r[5],
            first_ts=r[6],
            summary=r[7][:200] if r[7] else None # Truncate content for summary if needed
        ))
    return IncidentListResponse(incidents=incidents)

@router.get("/incidents/{incident_id}", response_model=IncidentSummary)
def get_incident(incident_id: str):
    """Get full details for a single incident bundle."""
    from worker.repository import get_bundle
    from fastapi import HTTPException
    
    r = get_bundle(incident_id)
    if not r:
        raise HTTPException(status_code=404, detail="Incident not found")
        
    return IncidentSummary(
        id=r[0],
        trace_id=r[1],
        service=r[2],
        severity=r[3],
        symptoms=r[4],
        error_signature=r[5],
        first_ts=r[6],
        summary=r[7]  # Return full content
    )


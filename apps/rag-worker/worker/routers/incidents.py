from fastapi import APIRouter
from worker.schemas.incidents import IncidentListResponse, IncidentSummary
from worker.repository import get_recent_bundles

router = APIRouter()

@router.get("/incidents", response_model=IncidentListResponse)
def list_incidents(limit: int = 50):
    """List recent incidents from the incidents table."""
    from worker.repository import get_recent_incidents
    # Row query: incident_id, title, status, severity, started_at, resolved_at, owner, tags
    sql = """
    SELECT incident_id, title, status, severity, started_at, resolved_at, owner, tags
    FROM incidents
    ORDER BY started_at DESC NULLS LAST
    LIMIT %s;
    """
    rows = []
    from worker.db import get_conn
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (limit,))
            rows = cur.fetchall()
            
    incidents = []
    for r in rows:
        incidents.append(IncidentSummary(
            id=r[0],
            trace_id=r[0], # fallback
            service='N/A', # fallback
            severity=r[3],
            symptoms=r[1], # fallback to title
            error_signature='',
            first_ts=r[4],
            summary=r[1],  # fallback to title
            title=r[1],
            status=r[2],
            resolved_at=r[5],
            owner=r[6],
            tags=r[7]
        ))
    return IncidentListResponse(incidents=incidents)

@router.get("/incidents/{incident_id}", response_model=IncidentSummary)
def get_incident(incident_id: str):
    """Get full details for a single incident."""
    from worker.db import get_conn
    from fastapi import HTTPException
    
    sql = """
    SELECT incident_id, title, status, severity, started_at, resolved_at, owner, tags
    FROM incidents
    WHERE incident_id = %s;
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (incident_id,))
            r = cur.fetchone()
            
    if not r:
        raise HTTPException(status_code=404, detail="Incident not found")
        
    return IncidentSummary(
        id=r[0],
        trace_id=r[0],
        service='N/A',
        severity=r[3],
        symptoms=r[1],
        error_signature='',
        first_ts=r[4],
        summary=r[1],
        title=r[1],
        status=r[2],
        resolved_at=r[5],
        owner=r[6],
        tags=r[7]
    )




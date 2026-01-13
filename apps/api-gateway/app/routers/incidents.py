from typing import Any
from fastapi import APIRouter, Depends, Request, HTTPException, status
from ..schemas.incidents import IncidentListResponse
from ..deps import require_api_key
from ..clients import list_incidents_from_worker

router = APIRouter()

@router.get("/incidents", response_model=IncidentListResponse, status_code=status.HTTP_200_OK,
             dependencies=[Depends(require_api_key)])
async def list_incidents(req: Request, limit: int = 50) -> Any:
    """Proxy request to rag-worker to list recent incidents."""
    http = req.app.state.http
    try:
        data = await list_incidents_from_worker(http, limit)
        return data  # Already fits the schema
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Worker error: {str(e)}")

@router.get("/incidents/{incident_id}", response_model=Any, status_code=status.HTTP_200_OK,
             dependencies=[Depends(require_api_key)])
async def get_incident(req: Request, incident_id: str) -> Any:
    """Proxy request to rag-worker to get incident details."""
    http = req.app.state.http
    try:
        from ..clients import get_incident_from_worker
        data = await get_incident_from_worker(http, incident_id)
        return data
    except Exception as e:
        raise HTTPException(status_code=404, detail="Incident not found or worker error")


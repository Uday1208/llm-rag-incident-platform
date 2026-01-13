from typing import Any
from fastapi import APIRouter, Depends, Request, HTTPException, status
from pydantic import BaseModel
from ..deps import require_api_key
from ..clients import get_incident_from_worker, call_reasoning_agent

router = APIRouter()

class ResolutionResponse(BaseModel):
    analysis: str

@router.post("/resolutions/generate/{incident_id}", response_model=ResolutionResponse, status_code=status.HTTP_200_OK,
             dependencies=[Depends(require_api_key)])
async def generate_resolution(req: Request, incident_id: str) -> Any:
    """Generate resolution analysis for an incident using the reasoning agent."""
    http = req.app.state.http
    
    # 1. Fetch incident details
    try:
        incident = await get_incident_from_worker(http, incident_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Incident not found")

    # 2. Construct prompt from symptoms or symptoms + content
    symptoms = incident.get("symptoms") or incident.get("summary") or "No details available."
    query = f"""
    Analyze the following incident and propose a resolution strategy.
    
    Service: {incident.get('service')}
    Severity: {incident.get('severity')}
    Error: {incident.get('error_signature')}
    
    Details:
    {symptoms}
    """

    # 3. Call reasoning agent
    try:
        resp = await call_reasoning_agent(http, query)
        answer = resp.get("answer", "No analysis generated.")
        return ResolutionResponse(analysis=answer)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Reasoning agent failed: {str(e)}")

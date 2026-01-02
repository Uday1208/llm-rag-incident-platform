"""
Router for agentic resolution endpoints.

Provides the ReAct-based incident resolution API with
support for switching between Custom and LangChain agents.
"""

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any

from agent import resolve_incident, get_agent, compare_agents, AgentType


router = APIRouter(prefix="/v1/agent", tags=["agent"])


# =============================================================================
# Request/Response Models
# =============================================================================

class ResolveRequest(BaseModel):
    """Request to resolve an incident."""
    query: str = Field(..., description="Incident description or question")
    session_id: Optional[str] = Field(None, description="Optional session ID for continuity")
    max_iterations: Optional[int] = Field(5, description="Maximum ReAct iterations", ge=1, le=10)
    agent_type: Optional[str] = Field(None, description="Agent type: 'custom' or 'langchain'")


class ResolveResponse(BaseModel):
    """Resolution response."""
    session_id: str
    status: str  # active, resolved, timeout, failed
    iterations: int
    result: Optional[str]
    cited_incidents: List[str]
    cited_resolutions: List[str]
    agent_type: Optional[str] = None
    duration_seconds: Optional[float] = None
    

class ToolCallRequest(BaseModel):
    """Request to execute a single tool (for debugging/testing)."""
    tool_name: str
    arguments: Dict[str, Any]


class CompareRequest(BaseModel):
    """Request to compare both agents."""
    query: str = Field(..., description="Incident description to test")


# =============================================================================
# Endpoints
# =============================================================================

@router.post("/resolve", response_model=ResolveResponse)
async def resolve(request: ResolveRequest) -> ResolveResponse:
    """
    Resolve an incident using the ReAct agent.
    
    Set `agent_type` to switch between implementations:
    - "custom": Our custom ReAct implementation (default)
    - "langchain": LangChain AgentExecutor
    
    Example queries:
    - "Payment API returning 504 errors since 09:30"
    - "Database connection pool exhausted on order-service"
    """
    try:
        result = await resolve_incident(
            query=request.query,
            session_id=request.session_id,
            agent_type=request.agent_type,
        )
        
        return ResolveResponse(
            session_id=result.get("id", ""),
            status=result.get("status", "unknown"),
            iterations=result.get("iterations", 0),
            result=result.get("result"),
            cited_incidents=result.get("cited_incidents", []),
            cited_resolutions=result.get("cited_resolutions", []),
            agent_type=result.get("agent_type"),
            duration_seconds=result.get("duration_seconds"),
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/compare")
async def compare(request: CompareRequest) -> Dict[str, Any]:
    """
    Run the same query through both agents and compare results.
    
    Returns timing and output from both Custom and LangChain agents.
    Useful for benchmarking and understanding differences.
    """
    try:
        return await compare_agents(request.query)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tool")
async def execute_tool(request: ToolCallRequest) -> Dict[str, Any]:
    """
    Execute a single tool directly (for debugging/testing).
    
    Available tools:
    - search_incidents
    - search_resolutions
    - analyze_trace
    - suggest_resolution
    - get_service_health
    """
    agent = get_agent()
    # Access executor from the agent (works for both types)
    executor = getattr(agent, 'executor', None)
    if not executor:
        from agent import ToolExecutor
        executor = ToolExecutor()
    result = await executor.execute(request.tool_name, request.arguments)
    return {"tool": request.tool_name, "result": result}


@router.get("/tools")
async def list_tools() -> Dict[str, Any]:
    """List available agent tools and their schemas."""
    from agent import AGENT_TOOLS
    return {"tools": AGENT_TOOLS}


@router.get("/types")
async def list_agent_types() -> Dict[str, Any]:
    """List available agent implementations."""
    from agent import is_langchain_available
    return {
        "available_types": [
            {"type": "custom", "description": "Custom ReAct implementation", "available": True},
            {"type": "langchain", "description": "LangChain AgentExecutor", "available": is_langchain_available()},
        ],
        "current_default": "custom",
        "note": "Set AGENT_TYPE env var or pass agent_type in request to switch"
    }


@router.get("/health")
async def agent_health() -> Dict[str, str]:
    """Agent health check."""
    return {"status": "healthy", "agent": "resolution-agent"}


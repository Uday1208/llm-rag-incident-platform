"""
Resolution Agent using ReAct pattern.

Implements the Thought → Action → Observation loop for
intelligent incident resolution with tool calling.
"""

import uuid
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass, field

from .tools import AGENT_TOOLS
from .executor import ToolExecutor
from ..services.llm_client import LLMClient, chat_with_tools, RESOLUTION_AGENT_PROMPT

log = logging.getLogger("reasoning-agent.resolver")


# =============================================================================
# Configuration
# =============================================================================

MAX_ITERATIONS = 5  # Maximum ReAct loop iterations
DEFAULT_TEMPERATURE = 0.3


# =============================================================================
# Data Classes
# =============================================================================

@dataclass
class AgentMessage:
    """A message in the agent conversation."""
    role: str  # system, user, assistant, tool
    content: str
    tool_calls: Optional[List[Dict]] = None
    tool_call_id: Optional[str] = None
    name: Optional[str] = None  # For tool responses
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dict for LLM API."""
        d = {"role": self.role, "content": self.content}
        if self.tool_calls:
            d["tool_calls"] = self.tool_calls
        if self.tool_call_id:
            d["tool_call_id"] = self.tool_call_id
        if self.name:
            d["name"] = self.name
        return d


@dataclass
class AgentSession:
    """A resolution agent session."""
    id: str
    user_query: str
    status: str = "active"  # active, resolved, timeout, failed
    iterations: int = 0
    memory: List[AgentMessage] = field(default_factory=list)
    result: Optional[str] = None
    cited_incidents: List[str] = field(default_factory=list)
    cited_resolutions: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize session for storage/response."""
        return {
            "id": self.id,
            "user_query": self.user_query,
            "status": self.status,
            "iterations": self.iterations,
            "result": self.result,
            "cited_incidents": self.cited_incidents,
            "cited_resolutions": self.cited_resolutions,
            "memory": [m.to_dict() for m in self.memory],
            "created_at": self.created_at.isoformat(),
        }


# =============================================================================
# Resolution Agent
# =============================================================================

class ResolutionAgent:
    """
    ReAct-based resolution agent.
    
    Follows the pattern:
    1. THINK: Analyze the query and decide what to do
    2. ACT: Call a tool to gather information
    3. OBSERVE: Process the tool result
    4. REPEAT until ready to respond
    5. RESPOND: Provide final resolution
    
    Usage:
        agent = ResolutionAgent()
        result = await agent.resolve("Payment API returning 504 errors since 09:30")
    """
    
    def __init__(
        self,
        llm_client: Optional[LLMClient] = None,
        tool_executor: Optional[ToolExecutor] = None,
        max_iterations: int = MAX_ITERATIONS,
    ):
        self.llm = llm_client or LLMClient.from_config()
        self.executor = tool_executor or ToolExecutor()
        self.max_iterations = max_iterations
    
    async def resolve(self, query: str, session_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Main entry point. Runs the ReAct loop until resolution.
        
        Args:
            query: User's incident description or question
            session_id: Optional session ID for continuity
            
        Returns:
            Resolution result with answer, citations, and trace
        """
        session = AgentSession(
            id=session_id or str(uuid.uuid4())[:8],
            user_query=query,
        )
        
        # Initialize conversation
        session.memory.append(AgentMessage(role="user", content=query))
        
        log.info(f"[{session.id}] Starting resolution for: {query[:100]}...")
        
        try:
            for i in range(self.max_iterations):
                session.iterations = i + 1
                
                # Get LLM response (may include tool calls)
                response = await self._get_llm_response(session)
                
                # Record assistant response
                session.memory.append(AgentMessage(
                    role="assistant",
                    content=response.get("content", ""),
                    tool_calls=response.get("tool_calls"),
                ))
                
                # Check if LLM wants to call tools
                tool_calls = response.get("tool_calls", [])
                
                if not tool_calls:
                    # No more tools - LLM has final answer
                    session.status = "resolved"
                    session.result = response.get("content", "")
                    log.info(f"[{session.id}] Resolved in {i + 1} iterations")
                    break
                
                # Execute tools and add results
                for tool_call in tool_calls:
                    tool_result = await self._execute_tool(tool_call, session)
                    session.memory.append(AgentMessage(
                        role="tool",
                        content=json.dumps(tool_result),
                        tool_call_id=tool_call.get("id"),
                        name=tool_call.get("name"),
                    ))
            else:
                # Max iterations reached
                session.status = "timeout"
                session.result = self._build_timeout_response(session)
                log.warning(f"[{session.id}] Timed out after {self.max_iterations} iterations")
        
        except Exception as e:
            session.status = "failed"
            session.result = f"Resolution failed: {str(e)}"
            log.error(f"[{session.id}] Failed: {e}")
        
        return session.to_dict()
    
    async def _get_llm_response(self, session: AgentSession) -> Dict[str, Any]:
        """Get LLM response with tool calling."""
        messages = [m.to_dict() for m in session.memory]
        
        response = await chat_with_tools(
            messages=messages,
            tools=AGENT_TOOLS,
            system_prompt=RESOLUTION_AGENT_PROMPT,
        )
        
        return response
    
    async def _execute_tool(self, tool_call: Dict[str, Any], session: AgentSession) -> Dict[str, Any]:
        """Execute a tool and track citations."""
        tool_name = tool_call.get("name", "")
        arguments = tool_call.get("arguments", {})
        
        log.debug(f"[{session.id}] Executing tool: {tool_name}")
        
        result = await self.executor.execute(tool_name, arguments)
        
        # Track citations
        if tool_name == "search_incidents":
            for incident in result.get("results", []):
                if incident.get("id") and incident["id"] not in session.cited_incidents:
                    session.cited_incidents.append(incident["id"])
        
        elif tool_name == "search_resolutions":
            for resolution in result.get("resolutions", []):
                res_id = resolution.get("id") if isinstance(resolution, dict) else str(resolution)
                if res_id and res_id not in session.cited_resolutions:
                    session.cited_resolutions.append(res_id)
        
        return result
    
    def _build_timeout_response(self, session: AgentSession) -> str:
        """Build response when max iterations reached."""
        lines = [
            "I gathered some information but couldn't complete the analysis:",
            "",
        ]
        
        if session.cited_incidents:
            lines.append(f"**Found {len(session.cited_incidents)} related incidents:**")
            for inc_id in session.cited_incidents[:5]:
                lines.append(f"  - {inc_id}")
            lines.append("")
        
        if session.cited_resolutions:
            lines.append(f"**Found {len(session.cited_resolutions)} related resolutions:**")
            for res_id in session.cited_resolutions[:3]:
                lines.append(f"  - {res_id}")
            lines.append("")
        
        lines.append("Please review these references and run a more specific query if needed.")
        
        return "\n".join(lines)


# =============================================================================
# Convenience Functions
# =============================================================================

_agent: Optional[ResolutionAgent] = None


def get_agent() -> ResolutionAgent:
    """Get or create the global agent instance."""
    global _agent
    if _agent is None:
        _agent = ResolutionAgent()
    return _agent


async def resolve_incident(query: str, session_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Convenience function for incident resolution.
    
    Args:
        query: User's incident description
        session_id: Optional session ID
        
    Returns:
        Resolution result
    """
    agent = get_agent()
    return await agent.resolve(query, session_id)

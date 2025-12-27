"""
Agent Factory with runtime selection.

Provides a unified interface to switch between:
- Custom ReAct agent (default)
- LangChain agent (optional)

Configuration via AGENT_TYPE environment variable.
"""

import os
import logging
from typing import Dict, Any, Optional, Protocol
from enum import Enum

log = logging.getLogger("reasoning-agent.factory")


class AgentType(str, Enum):
    """Supported agent implementations."""
    CUSTOM = "custom"      # Our custom ReAct implementation
    LANGCHAIN = "langchain"  # LangChain AgentExecutor


class AgentProtocol(Protocol):
    """Protocol for resolution agents."""
    
    async def resolve(self, query: str, session_id: Optional[str] = None) -> Dict[str, Any]:
        """Resolve an incident and return result."""
        ...


# Configuration
AGENT_TYPE = os.getenv("AGENT_TYPE", "custom").lower()


def get_agent_type() -> AgentType:
    """Get configured agent type."""
    try:
        return AgentType(AGENT_TYPE)
    except ValueError:
        log.warning(f"Unknown AGENT_TYPE '{AGENT_TYPE}', defaulting to 'custom'")
        return AgentType.CUSTOM


def create_agent(agent_type: Optional[AgentType] = None) -> AgentProtocol:
    """
    Factory function to create an agent instance.
    
    Args:
        agent_type: Override the configured agent type
        
    Returns:
        Agent instance (custom or LangChain)
    """
    selected_type = agent_type or get_agent_type()
    
    if selected_type == AgentType.LANGCHAIN:
        try:
            from .langchain_agent import LangChainResolutionAgent, is_langchain_available
            
            if not is_langchain_available():
                log.warning("LangChain not available, falling back to custom agent")
                from .resolver import ResolutionAgent
                return ResolutionAgent()
            
            log.info("Using LangChain agent")
            return LangChainResolutionAgent()
            
        except ImportError as e:
            log.warning(f"Failed to import LangChain agent: {e}, using custom")
            from .resolver import ResolutionAgent
            return ResolutionAgent()
    
    else:
        log.info("Using custom ReAct agent")
        from .resolver import ResolutionAgent
        return ResolutionAgent()


# Singleton with lazy initialization
_agent: Optional[AgentProtocol] = None


def get_agent() -> AgentProtocol:
    """Get or create the global agent instance."""
    global _agent
    if _agent is None:
        _agent = create_agent()
    return _agent


def reset_agent() -> None:
    """Reset the global agent (for testing/switching)."""
    global _agent
    _agent = None


async def resolve_incident(
    query: str,
    session_id: Optional[str] = None,
    agent_type: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Resolve an incident using the configured agent.
    
    Args:
        query: Incident description
        session_id: Optional session ID
        agent_type: Override agent type for this call ("custom" or "langchain")
        
    Returns:
        Resolution result with metadata
    """
    if agent_type:
        # Create temporary agent for comparison
        try:
            selected_type = AgentType(agent_type.lower())
        except ValueError:
            selected_type = get_agent_type()
        agent = create_agent(selected_type)
    else:
        agent = get_agent()
    
    result = await agent.resolve(query, session_id)
    
    # Add agent type to result for comparison
    if "agent_type" not in result:
        result["agent_type"] = AGENT_TYPE
    
    return result


async def compare_agents(query: str) -> Dict[str, Any]:
    """
    Run the same query through both agents for comparison.
    
    Args:
        query: Incident description
        
    Returns:
        Comparison results
    """
    from datetime import datetime
    import asyncio
    
    results = {"query": query, "agents": {}}
    
    # Run custom agent
    custom_start = datetime.utcnow()
    try:
        custom_result = await resolve_incident(query, agent_type="custom")
        custom_result["duration_seconds"] = (datetime.utcnow() - custom_start).total_seconds()
        results["agents"]["custom"] = custom_result
    except Exception as e:
        results["agents"]["custom"] = {"status": "error", "error": str(e)}
    
    # Run LangChain agent
    langchain_start = datetime.utcnow()
    try:
        langchain_result = await resolve_incident(query, agent_type="langchain")
        langchain_result["duration_seconds"] = (datetime.utcnow() - langchain_start).total_seconds()
        results["agents"]["langchain"] = langchain_result
    except Exception as e:
        results["agents"]["langchain"] = {"status": "error", "error": str(e)}
    
    # Add comparison summary
    custom_time = results["agents"].get("custom", {}).get("duration_seconds", 0)
    langchain_time = results["agents"].get("langchain", {}).get("duration_seconds", 0)
    
    results["comparison"] = {
        "custom_duration": custom_time,
        "langchain_duration": langchain_time,
        "faster_agent": "custom" if custom_time < langchain_time else "langchain",
        "time_difference_seconds": abs(custom_time - langchain_time),
    }
    
    return results

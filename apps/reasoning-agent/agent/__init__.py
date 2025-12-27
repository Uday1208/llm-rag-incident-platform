# Resolution Agent package

# Factory (preferred interface)
from .factory import (
    get_agent,
    create_agent,
    resolve_incident,
    compare_agents,
    reset_agent,
    AgentType,
)

# Custom agent (direct access)
from .resolver import ResolutionAgent

# Tools
from .tools import AGENT_TOOLS, get_tool_names
from .executor import ToolExecutor

# LangChain agent (optional)
try:
    from .langchain_agent import LangChainResolutionAgent, is_langchain_available
except ImportError:
    LangChainResolutionAgent = None
    is_langchain_available = lambda: False

__all__ = [
    # Factory
    "get_agent",
    "create_agent", 
    "resolve_incident",
    "compare_agents",
    "reset_agent",
    "AgentType",
    # Agents
    "ResolutionAgent",
    "LangChainResolutionAgent",
    "is_langchain_available",
    # Tools
    "AGENT_TOOLS",
    "get_tool_names",
    "ToolExecutor",
]


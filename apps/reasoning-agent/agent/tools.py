"""
Tool definitions for the Resolution Agent.

Defines the tools available to the ReAct agent for incident resolution:
- search_incidents: Semantic search over incident bundles
- search_resolutions: Find past resolutions
- analyze_trace: Get detailed trace analysis
- suggest_resolution: Generate resolution suggestions
"""

from typing import List, Dict, Any, Optional


# =============================================================================
# Tool Schemas (OpenAI Function Calling format)
# =============================================================================

SEARCH_INCIDENTS_TOOL = {
    "name": "search_incidents",
    "description": "Search past incidents by semantic similarity. Use when you need to find similar issues that happened before.",
    "parameters": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Natural language description of the incident or error pattern to search for"
            },
            "top_k": {
                "type": "integer",
                "default": 5,
                "description": "Number of results to return (default: 5)"
            },
            "service_filter": {
                "type": "string",
                "description": "Optional: filter results to a specific service name"
            },
            "severity_filter": {
                "type": "string",
                "enum": ["WARNING", "ERROR", "CRITICAL"],
                "description": "Optional: filter by minimum severity"
            }
        },
        "required": ["query"]
    }
}

SEARCH_RESOLUTIONS_TOOL = {
    "name": "search_resolutions",
    "description": "Find past resolutions for incidents. Use to find how similar issues were resolved before.",
    "parameters": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "Description of the issue to find resolutions for"
            },
            "incident_ids": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Optional: specific incident IDs to find resolutions for"
            },
            "top_k": {
                "type": "integer",
                "default": 3,
                "description": "Number of resolutions to return"
            }
        }
    }
}

ANALYZE_TRACE_TOOL = {
    "name": "analyze_trace",
    "description": "Get detailed analysis of a specific incident trace. Use when you need more context about a specific incident.",
    "parameters": {
        "type": "object",
        "properties": {
            "incident_id": {
                "type": "string",
                "description": "The incident bundle ID to analyze"
            },
            "trace_id": {
                "type": "string",
                "description": "Alternative: the trace ID (operation_Id) to analyze"
            }
        }
    }
}

SUGGEST_RESOLUTION_TOOL = {
    "name": "suggest_resolution",
    "description": "Generate resolution suggestions based on gathered context. Call this when you have enough information to suggest a resolution.",
    "parameters": {
        "type": "object",
        "properties": {
            "incident_summary": {
                "type": "string",
                "description": "Summary of the current incident"
            },
            "similar_incidents": {
                "type": "array",
                "items": {"type": "object"},
                "description": "List of similar incidents found"
            },
            "past_resolutions": {
                "type": "array",
                "items": {"type": "string"},
                "description": "List of past resolutions that might apply"
            },
            "confidence": {
                "type": "string",
                "enum": ["high", "medium", "low"],
                "description": "Your confidence in the suggestion based on available evidence"
            }
        },
        "required": ["incident_summary"]
    }
}

GET_SERVICE_HEALTH_TOOL = {
    "name": "get_service_health",
    "description": "Get current health status and recent incident count for a service.",
    "parameters": {
        "type": "object",
        "properties": {
            "service_name": {
                "type": "string",
                "description": "Name of the service to check"
            }
        },
        "required": ["service_name"]
    }
}


# =============================================================================
# All Tools
# =============================================================================

AGENT_TOOLS = [
    SEARCH_INCIDENTS_TOOL,
    SEARCH_RESOLUTIONS_TOOL,
    ANALYZE_TRACE_TOOL,
    SUGGEST_RESOLUTION_TOOL,
    GET_SERVICE_HEALTH_TOOL,
]


def get_tool_by_name(name: str) -> Optional[Dict[str, Any]]:
    """Get tool definition by name."""
    for tool in AGENT_TOOLS:
        if tool["name"] == name:
            return tool
    return None


def get_tool_names() -> List[str]:
    """Get list of all tool names."""
    return [tool["name"] for tool in AGENT_TOOLS]

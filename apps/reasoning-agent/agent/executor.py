"""
Tool executor for the Resolution Agent.

Implements the actual logic for each tool defined in tools.py.
Connects to rag-worker for search operations.
"""

import os
import logging
from typing import Dict, Any, List, Optional

import httpx

log = logging.getLogger("reasoning-agent.executor")

# =============================================================================
# Configuration
# =============================================================================

RAG_WORKER_URL = os.getenv("RAG_WORKER_URL", "http://rag-worker:8000").rstrip("/")
RAG_WORKER_TOKEN = os.getenv("RAG_WORKER_TOKEN", "")
TIMEOUT = float(os.getenv("TOOL_TIMEOUT", "30"))


# =============================================================================
# Tool Executor
# =============================================================================

class ToolExecutor:
    """Executes tools for the resolution agent."""
    
    def __init__(self, rag_worker_url: str = RAG_WORKER_URL, token: str = RAG_WORKER_TOKEN):
        self.rag_worker_url = rag_worker_url
        self.token = token
    
    async def execute(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a tool by name with given arguments.
        
        Args:
            tool_name: Name of the tool to execute
            arguments: Tool-specific arguments
            
        Returns:
            Tool result as dictionary
        """
        executor_map = {
            "search_incidents": self._search_incidents,
            "search_resolutions": self._search_resolutions,
            "analyze_trace": self._analyze_trace,
            "suggest_resolution": self._suggest_resolution,
            "get_service_health": self._get_service_health,
        }
        
        executor = executor_map.get(tool_name)
        if not executor:
            return {"error": f"Unknown tool: {tool_name}"}
        
        try:
            return await executor(arguments)
        except Exception as e:
            log.error(f"Tool {tool_name} failed: {e}")
            return {"error": str(e)}
    
    def _get_headers(self) -> Dict[str, str]:
        """Get HTTP headers for rag-worker requests."""
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers
    
    # -------------------------------------------------------------------------
    # Tool Implementations
    # -------------------------------------------------------------------------
    
    async def _search_incidents(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Search incidents by semantic similarity.
        
        Uses rag-worker's embed + search endpoints.
        """
        query = args.get("query", "")
        top_k = args.get("top_k", 5)
        service_filter = args.get("service_filter")
        severity_filter = args.get("severity_filter")
        
        if not query:
            return {"error": "query is required", "results": []}
        
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            # 1. Get embedding for query
            embed_response = await client.post(
                f"{self.rag_worker_url}/internal/embed",
                headers=self._get_headers(),
                json={"texts": [query]}
            )
            embed_response.raise_for_status()
            embed_data = embed_response.json()
            
            # Extract embedding vector
            vectors = embed_data.get("vectors", [])
            if not vectors:
                return {"error": "embedding failed", "results": []}
            embedding = vectors[0] if isinstance(vectors[0], list) else vectors[0].get("embedding", [])
            
            # 2. Search by embedding
            search_payload = {
                "embedding": embedding,
                "top_k": top_k * 2,  # Fetch more for filtering
            }
            search_response = await client.post(
                f"{self.rag_worker_url}/internal/search",
                headers=self._get_headers(),
                json=search_payload
            )
            search_response.raise_for_status()
            search_data = search_response.json()
            
            results = search_data.get("results", [])
            
            # 3. Apply filters
            filtered = []
            for r in results:
                # Service filter
                if service_filter:
                    source = r.get("source", "") or r.get("service", "")
                    if service_filter.lower() not in source.lower():
                        continue
                
                # Severity filter
                if severity_filter:
                    severity_order = {"WARNING": 1, "ERROR": 2, "CRITICAL": 3}
                    result_severity = r.get("severity", "INFO")
                    if severity_order.get(result_severity, 0) < severity_order.get(severity_filter, 0):
                        continue
                
                filtered.append({
                    "id": r.get("id"),
                    "service": r.get("source") or r.get("service"),
                    "severity": r.get("severity"),
                    "content": r.get("content", "")[:500],  # Truncate for context
                    "score": r.get("score") or r.get("similarity"),
                    "timestamp": r.get("ts") or r.get("timestamp"),
                })
                
                if len(filtered) >= top_k:
                    break
            
            return {
                "query": query,
                "count": len(filtered),
                "results": filtered
            }
    
    async def _search_resolutions(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Search for past resolutions.
        
        Can search by query text or specific incident IDs.
        """
        query = args.get("query", "")
        incident_ids = args.get("incident_ids", [])
        top_k = args.get("top_k", 3)
        
        # For now, search resolutions by text similarity
        # In production, this would query the resolutions table directly
        
        if not query and not incident_ids:
            return {"error": "query or incident_ids required", "resolutions": []}
        
        search_query = query or f"resolution for incidents: {', '.join(incident_ids[:5])}"
        
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            # Use the agent's search endpoint which can search resolutions
            try:
                response = await client.post(
                    f"{self.rag_worker_url}/v1/search",
                    headers=self._get_headers(),
                    json={"query": f"resolution: {search_query}", "top_k": top_k}
                )
                if response.status_code == 200:
                    data = response.json()
                    return {
                        "query": search_query,
                        "resolutions": data.get("results", [])
                    }
            except Exception as e:
                log.warning(f"Resolution search failed: {e}")
        
        # Fallback: return placeholder indicating no resolutions found
        return {
            "query": search_query,
            "resolutions": [],
            "note": "No matching resolutions found. Consider creating a new resolution after resolving this incident."
        }
    
    async def _analyze_trace(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get detailed analysis of a specific incident trace.
        """
        incident_id = args.get("incident_id")
        trace_id = args.get("trace_id")
        
        if not incident_id and not trace_id:
            return {"error": "incident_id or trace_id required"}
        
        # Search for the specific incident
        search_query = incident_id or trace_id
        
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            try:
                response = await client.post(
                    f"{self.rag_worker_url}/v1/search",
                    headers=self._get_headers(),
                    json={"query": search_query, "top_k": 1}
                )
                response.raise_for_status()
                data = response.json()
                
                results = data.get("results", [])
                if not results:
                    return {"error": "Incident not found", "incident_id": incident_id, "trace_id": trace_id}
                
                incident = results[0]
                
                # Build analysis
                return {
                    "incident_id": incident.get("id"),
                    "trace_id": trace_id or incident.get("id"),
                    "service": incident.get("source") or incident.get("service"),
                    "severity": incident.get("severity"),
                    "timestamp": incident.get("ts") or incident.get("timestamp"),
                    "content": incident.get("content"),
                    "analysis": {
                        "error_pattern": self._extract_error_pattern(incident.get("content", "")),
                        "affected_components": self._extract_components(incident.get("content", "")),
                    }
                }
            except Exception as e:
                return {"error": f"Analysis failed: {e}"}
    
    async def _suggest_resolution(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate resolution suggestions based on context.
        
        This synthesizes the gathered information into actionable steps.
        """
        incident_summary = args.get("incident_summary", "")
        similar_incidents = args.get("similar_incidents", [])
        past_resolutions = args.get("past_resolutions", [])
        confidence = args.get("confidence", "medium")
        
        # Build suggestion based on available context
        steps = []
        
        # If we have past resolutions, use them as primary guidance
        if past_resolutions:
            steps.append("Based on past resolutions:")
            for i, res in enumerate(past_resolutions[:3], 1):
                if isinstance(res, dict):
                    steps.append(f"  {i}. {res.get('summary', res)}")
                else:
                    steps.append(f"  {i}. {res}")
        
        # Add generic triage steps based on incident type
        if "timeout" in incident_summary.lower():
            steps.extend([
                "Check service dependencies and their response times",
                "Review connection pool settings and limits",
                "Check for network issues or DNS resolution problems",
            ])
        elif "connection" in incident_summary.lower():
            steps.extend([
                "Verify database/cache connection strings",
                "Check firewall rules and network security groups",
                "Review connection limits and pool exhaustion",
            ])
        elif "500" in incident_summary or "internal server error" in incident_summary.lower():
            steps.extend([
                "Check application logs for stack trace",
                "Review recent deployments or config changes",
                "Verify external service availability",
            ])
        
        # Default steps if nothing specific
        if not steps:
            steps = [
                "Review the full incident trace for error details",
                "Check service metrics around the incident time",
                "Verify configuration and environment variables",
                "Check for any recent changes or deployments",
            ]
        
        return {
            "summary": incident_summary,
            "confidence": confidence,
            "suggested_steps": steps,
            "similar_incident_count": len(similar_incidents),
            "resolution_sources": len(past_resolutions),
            "recommendation": f"Confidence: {confidence}. " + (
                "High confidence based on similar past incidents." if confidence == "high"
                else "Review steps and adapt based on specific context." if confidence == "medium"
                else "Limited historical data. Proceed with caution and document findings."
            )
        }
    
    async def _get_service_health(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get service health status.
        """
        service_name = args.get("service_name", "")
        
        if not service_name:
            return {"error": "service_name required"}
        
        # In production, this would query the service_health table
        # For now, search recent incidents for the service
        incidents = await self._search_incidents({
            "query": f"errors in {service_name}",
            "service_filter": service_name,
            "top_k": 10,
        })
        
        incident_count = incidents.get("count", 0)
        
        # Determine health status based on recent incidents
        if incident_count == 0:
            status = "healthy"
        elif incident_count <= 3:
            status = "degraded"
        else:
            status = "unhealthy"
        
        return {
            "service_name": service_name,
            "status": status,
            "incident_count_recent": incident_count,
            "recent_incidents": incidents.get("results", [])[:3],
        }
    
    # -------------------------------------------------------------------------
    # Helper Methods
    # -------------------------------------------------------------------------
    
    def _extract_error_pattern(self, content: str) -> Optional[str]:
        """Extract the main error pattern from content."""
        import re
        
        # Look for exception types
        exc_match = re.search(r'([A-Z][a-zA-Z]+(?:Exception|Error|Timeout|Failure))', content)
        if exc_match:
            return exc_match.group(1)
        
        # Look for HTTP error codes
        http_match = re.search(r'(HTTP\s*)?([45]\d{2})\s*([A-Za-z\s]+)?', content)
        if http_match:
            return f"HTTP {http_match.group(2)}"
        
        return None
    
    def _extract_components(self, content: str) -> List[str]:
        """Extract affected components from content."""
        components = []
        
        # Look for file paths
        import re
        paths = re.findall(r'/app/[^\s:]+', content)
        for path in paths[:3]:
            components.append(path.split("/")[-1])
        
        # Look for service names
        services = re.findall(r'(?:service|api|handler)[\s:=]+([a-zA-Z0-9_-]+)', content, re.I)
        components.extend(services[:3])
        
        return list(set(components))

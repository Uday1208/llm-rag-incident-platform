"""
LLM-based summarization for incident bundles.

Generates structured summaries (symptoms, failing_dependency, error_signature)
using Azure OpenAI or local LLM.
"""

import os
import json
import logging
from typing import Dict, Any, Optional, List

import httpx

log = logging.getLogger("preprocessor.summarizer")


# =============================================================================
# Configuration
# =============================================================================

# Default reasoning agent URL (internal service name)
REASONING_AGENT_URL = os.getenv("REASONING_AGENT_URL", "http://reasoning-agent:8082")
LLM_TIMEOUT = float(os.getenv("LLM_TIMEOUT", "60"))


# =============================================================================
# Prompts
# =============================================================================
# (System prompts are now managed by reasoning-agent, but we keep templates for local reference if needed)

SUMMARIZE_USER_TEMPLATE = """Analyze this incident bundle and extract a structured summary:

Service: {service}
Operation: {operation}
Severity: {severity}
Log Count: {log_count}

Logs:
{content}

Return only valid JSON with symptoms, failing_dependency, and error_signature."""


# =============================================================================
# Summarizer
# =============================================================================

class IncidentSummarizer:
    """Summarizes incident bundles by delegating to reasoning-agent."""
    
    def __init__(
        self,
        reasoning_agent_url: str = REASONING_AGENT_URL,
        timeout: float = LLM_TIMEOUT,
    ):
        self.url = f"{reasoning_agent_url.rstrip('/')}/v1/llm/summarize"
        self.timeout = timeout
    
    async def summarize(self, bundle: Dict[str, Any]) -> Dict[str, Optional[str]]:
        """
        Generate structured summary for an incident bundle via reasoning-agent.
        """
        payload = {
            "service": bundle.get("service", "unknown"),
            "operation": bundle.get("operation", "unknown"),
            "severity": bundle.get("severity", "ERROR"),
            "log_count": bundle.get("log_count", 1),
            "content": bundle.get("content", "")[:5000],  # Give agent more context
        }
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(self.url, json=payload)
                response.raise_for_status()
                data = response.json()
                
                return {
                    "symptoms": data.get("symptoms"),
                    "failing_dependency": data.get("failing_dependency"),
                    "error_signature": data.get("error_signature"),
                }
            
        except Exception as e:
            # Soft fail: If LLM summary fails, log and return empty summary
            log.warning(f"LLM summarization (via agent) failed: {repr(e)}")
            return {
                "symptoms": None,
                "failing_dependency": None,
                "error_signature": None
            }
    
    def _fallback_summary(self, bundle: Dict[str, Any]) -> Dict[str, Optional[str]]:
        """Generate fallback summary from bundle content."""
        content = bundle.get("content", "")
        
        # Extract first error line as symptoms
        symptoms = None
        for line in content.split("\n"):
            if "[ERROR]" in line or "[CRITICAL]" in line:
                symptoms = line.replace("[ERROR]", "").replace("[CRITICAL]", "").strip()[:200]
                break
        
        # Extract exception type as error signature
        error_signature = None
        for word in ["Exception", "Error", "Timeout", "Failure"]:
            if word in content:
                # Find the exception class
                import re
                match = re.search(r'([A-Z][a-zA-Z]+(?:Exception|Error|Timeout|Failure))', content)
                if match:
                    error_signature = match.group(1)
                    break
        
        return {
            "symptoms": symptoms,
            "failing_dependency": None,
            "error_signature": error_signature,
        }


async def summarize_bundles(
    bundles: List[Dict[str, Any]],
    summarizer: Optional[IncidentSummarizer] = None,
) -> List[Dict[str, Any]]:
    """
    Summarize multiple bundles.
    
    Args:
        bundles: List of incident bundles
        summarizer: Optional summarizer instance
        
    Returns:
        Bundles with added summary fields
    """
    if summarizer is None:
        summarizer = IncidentSummarizer()
    
    results = []
    for bundle in bundles:
        summary = await summarizer.summarize(bundle)
        bundle.update(summary)
        results.append(bundle)
    
    return results

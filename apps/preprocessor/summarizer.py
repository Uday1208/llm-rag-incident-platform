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

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "ollama")  # ollama | azure | openai
LLM_MODEL = os.getenv("LLM_MODEL", "qwen2.5:7b")
LLM_TIMEOUT = float(os.getenv("LLM_TIMEOUT", "60"))

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
AOAI_ENDPOINT = os.getenv("AOAI_ENDPOINT", "")
AOAI_KEY = os.getenv("AOAI_KEY", "")
AOAI_DEPLOYMENT = os.getenv("AOAI_CHAT_DEPLOYMENT", "")
AOAI_API_VERSION = os.getenv("AOAI_API_VERSION", "2024-02-01")


# =============================================================================
# Prompts
# =============================================================================

SUMMARIZE_SYSTEM_PROMPT = """You are an expert SRE analyzing incident logs. 
Given a bundle of related log entries, extract a structured summary.

Return JSON with exactly these fields:
{
  "symptoms": "Observable behavior (1-2 sentences)",
  "failing_dependency": "Service or component that failed",
  "error_signature": "Exception type or error code with location"
}

Be concise and specific. If information is not available, use null."""

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
    """Summarizes incident bundles using LLM."""
    
    def __init__(
        self,
        provider: str = LLM_PROVIDER,
        model: str = LLM_MODEL,
        timeout: float = LLM_TIMEOUT,
    ):
        self.provider = provider.lower()
        self.model = model
        self.timeout = timeout
    
    async def summarize(self, bundle: Dict[str, Any]) -> Dict[str, Optional[str]]:
        """
        Generate structured summary for an incident bundle.
        
        Args:
            bundle: Incident bundle dict with service, operation, severity, content
            
        Returns:
            Dict with symptoms, failing_dependency, error_signature
        """
        # Format the prompt
        user_message = SUMMARIZE_USER_TEMPLATE.format(
            service=bundle.get("service", "unknown"),
            operation=bundle.get("operation", "unknown"),
            severity=bundle.get("severity", "ERROR"),
            log_count=bundle.get("log_count", 1),
            content=bundle.get("content", "")[:3000],  # Limit content length
        )
        
        try:
            if self.provider == "azure":
                response = await self._call_azure_openai(user_message)
            elif self.provider == "openai":
                response = await self._call_openai(user_message)
            else:
                response = await self._call_ollama(user_message)
            
            # Parse JSON response
            return self._parse_summary(response)
            
        except Exception as e:
            # Soft fail: If LLM summary fails, just return bundle as-is or with empty summary
            log.warning(f"LLM summarization failed: {e}")
            return {
                "symptoms": None,
                "failing_dependency": None,
                "error_signature": None
            }
    
    async def _call_ollama(self, user_message: str) -> str:
        """Call Ollama API."""
        url = f"{OLLAMA_BASE_URL}/api/chat"
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": SUMMARIZE_SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
            "stream": False,
            "format": "json",
        }
        
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(url, json=payload)
            response.raise_for_status()
            data = response.json()
            return data.get("message", {}).get("content", "")
    
    async def _call_azure_openai(self, user_message: str) -> str:
        """Call Azure OpenAI API."""
        if not AOAI_ENDPOINT or not AOAI_KEY:
            raise ValueError("Azure OpenAI not configured")
        
        deployment = AOAI_DEPLOYMENT or self.model
        url = f"{AOAI_ENDPOINT}/openai/deployments/{deployment}/chat/completions?api-version={AOAI_API_VERSION}"
        
        headers = {
            "Content-Type": "application/json",
            "api-key": AOAI_KEY,
        }
        
        payload = {
            "messages": [
                {"role": "system", "content": SUMMARIZE_SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
            "temperature": 0.3,
            "max_tokens": 256,
            "response_format": {"type": "json_object"},
        }
        
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
            return data["choices"][0]["message"]["content"]
    
    async def _call_openai(self, user_message: str) -> str:
        """Call OpenAI API."""
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OpenAI not configured")
        
        url = "https://api.openai.com/v1/chat/completions"
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        }
        
        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": SUMMARIZE_SYSTEM_PROMPT},
                {"role": "user", "content": user_message},
            ],
            "temperature": 0.3,
            "max_tokens": 256,
            "response_format": {"type": "json_object"},
        }
        
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
            return data["choices"][0]["message"]["content"]
    
    def _parse_summary(self, response: str) -> Dict[str, Optional[str]]:
        """Parse LLM response into structured summary."""
        try:
            data = json.loads(response)
            return {
                "symptoms": data.get("symptoms"),
                "failing_dependency": data.get("failing_dependency"),
                "error_signature": data.get("error_signature"),
            }
        except json.JSONDecodeError:
            log.warning(f"Failed to parse LLM response as JSON: {response[:100]}")
            return {"symptoms": None, "failing_dependency": None, "error_signature": None}
    
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

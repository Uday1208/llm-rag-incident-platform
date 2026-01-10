from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any, Optional

import logging
from services.llm_client import LLMClient, LLMError

log = logging.getLogger("reasoning-agent.llm_router")

router = APIRouter(prefix="/v1/llm", tags=["llm"])

class SummarizeRequest(BaseModel):
    service: str
    operation: str
    severity: str
    log_count: int
    content: str

class SummarizeResponse(BaseModel):
    symptoms: Optional[str]
    failing_dependency: Optional[str]
    error_signature: Optional[str]

@router.post("/summarize", response_model=SummarizeResponse)
async def summarize(req: SummarizeRequest):
    """
    Summarize an incident bundle into structured fields.
    This replaces direct Ollama calls from the preprocessor.
    """
    client = LLMClient.from_config()
    
    system_prompt = """You are an expert SRE analyzing incident logs. 
Return JSON with exactly these fields:
{
  "symptoms": "Observable behavior",
  "failing_dependency": "Service or component that failed",
  "error_signature": "Exception type or error code"
}
Return only valid JSON."""

    user_message = f"""Analyze this incident bundle:
Service: {req.service}
Operation: {req.operation}
Severity: {req.severity}
Log Count: {req.log_count}

Logs:
{req.content[:3000]}
"""
    
    try:
        # Use simple chat for now, expecting JSON in response
        response_text = await client.chat(
            messages=[{"role": "user", "content": user_message}],
            system_prompt=system_prompt
        )
        
        # Basic JSON extraction if LLM is chatty
        import json
        import re
        
        try:
            # Try parsing directly
            data = json.loads(response_text)
        except json.JSONDecodeError:
            # Try extracting from code blocks
            match = re.search(r'\{.*\}', response_text, re.DOTALL)
            if match:
                data = json.loads(match.group(0))
            else:
                raise ValueError("No JSON found in response")
                
        return SummarizeResponse(
            symptoms=data.get("symptoms"),
            failing_dependency=data.get("failing_dependency"),
            error_signature=data.get("error_signature")
        )
        
    except Exception as e:
        log.error(f"Summarization failed: {e}")
        # Always return structured empty response rather than 500 to keep pipeline moving
        return SummarizeResponse(symptoms=None, failing_dependency=None, error_signature=None)

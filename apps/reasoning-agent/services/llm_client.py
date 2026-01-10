"""
Multi-provider LLM client for the reasoning agent.

Supports:
- Ollama (default: qwen2.5:7b)
- Azure OpenAI (gpt-4o-mini)
- OpenAI API

Configuration via environment variables:
- LLM_PROVIDER: ollama | azure | openai (default: ollama)
- LLM_MODEL: model name (defaults per provider)
- LLM_TEMPERATURE, LLM_MAX_TOKENS, LLM_TIMEOUT

For Azure: AOAI_ENDPOINT, AOAI_KEY, AOAI_CHAT_DEPLOYMENT
For OpenAI: OPENAI_API_KEY
For Ollama: OLLAMA_BASE_URL
"""

import os
import json
import asyncio
import logging
from typing import List, Dict, Any, Optional
from enum import Enum
from dataclasses import dataclass

import httpx

log = logging.getLogger("reasoning-agent.llm")


# =============================================================================
# Configuration
# =============================================================================

class LLMProvider(str, Enum):
    OLLAMA = "ollama"
    AZURE_OPENAI = "azure"
    OPENAI = "openai"


@dataclass
class LLMConfig:
    """LLM configuration loaded from environment."""
    provider: LLMProvider
    model: str
    temperature: float
    max_tokens: int
    timeout: float
    retries: int
    
    # Provider-specific
    ollama_base_url: str
    azure_endpoint: Optional[str]
    azure_api_key: Optional[str]
    azure_deployment: Optional[str]
    azure_api_version: str
    openai_api_key: Optional[str]
    
    # Fallback behavior
    fallback_to_azure: bool
    
    @classmethod
    def from_env(cls) -> "LLMConfig":
        provider_str = os.getenv("LLM_PROVIDER", "ollama").lower()
        try:
            provider = LLMProvider(provider_str)
        except ValueError:
            provider = LLMProvider.OLLAMA
        
        # Default model per provider
        default_models = {
            LLMProvider.OLLAMA: "qwen2.5:7b",
            LLMProvider.AZURE_OPENAI: "gpt-4o-mini",
            LLMProvider.OPENAI: "gpt-4o-mini",
        }
        
        return cls(
            provider=provider,
            model=os.getenv("LLM_MODEL", default_models[provider]),
            temperature=float(os.getenv("LLM_TEMPERATURE", "0.3")),
            max_tokens=int(os.getenv("LLM_MAX_TOKENS", "1024")),
            timeout=float(os.getenv("LLM_TIMEOUT", "60")),
            retries=int(os.getenv("LLM_RETRIES", "2")),
            ollama_base_url=os.getenv("OLLAMA_BASE_URL", "http://local-llm:11434"),
            azure_endpoint=os.getenv("AOAI_ENDPOINT"),
            azure_api_key=os.getenv("AOAI_KEY"),
            azure_deployment=os.getenv("AOAI_CHAT_DEPLOYMENT"),
            azure_api_version=os.getenv("AOAI_API_VERSION", "2024-02-01"),
            openai_api_key=os.getenv("OPENAI_API_KEY"),
            fallback_to_azure=os.getenv("FALLBACK_TO_AZURE", "false").lower() == "true",
        )


# =============================================================================
# Exceptions
# =============================================================================

class LLMError(Exception):
    """Base exception for LLM errors."""
    pass


class LLMTimeout(LLMError):
    """Raised when LLM calls time out after retries."""
    pass


class LLMUnavailable(LLMError):
    """Raised when the LLM endpoint returns 4xx/5xx or can't be reached."""
    pass


# =============================================================================
# System Prompts
# =============================================================================

DEFAULT_SYSTEM_PROMPT = """You are a senior SRE assistant. Given a user question and related log contexts, triage the issue, infer likely root cause patterns, and return concise next steps. If evidence is weak, say that clearly."""

RESOLUTION_AGENT_PROMPT = """You are an expert SRE resolution agent. Your job is to help operators resolve incidents quickly.

PROCESS:
1. THINK: Analyze what you know and what you need
2. ACT: Use available tools to gather information
3. OBSERVE: Process tool results
4. REPEAT until you have enough context
5. RESPOND: Provide resolution with specific steps

Always cite your sources (incident IDs, resolution IDs).
If evidence is weak, say so clearly.
Format your response in markdown for readability."""


# =============================================================================
# Provider Implementations
# =============================================================================

async def _ollama_chat(
    config: LLMConfig,
    messages: List[Dict[str, str]],
    tools: Optional[List[Dict]] = None,
) -> Dict[str, Any]:
    """Call Ollama /api/chat endpoint."""
    url = f"{config.ollama_base_url.rstrip('/')}/api/chat"
    
    payload = {
        "model": config.model,
        "messages": messages,
        "options": {
            "temperature": config.temperature,
            "num_predict": config.max_tokens,
        },
        "stream": False,
    }
    
    # Ollama supports tools in newer versions
    if tools:
        payload["tools"] = tools
    
    last_err = None
    for attempt in range(config.retries + 1):
        try:
            async with httpx.AsyncClient(timeout=config.timeout) as client:
                response = await client.post(url, json=payload)
                
                if response.status_code >= 500:
                    raise LLMUnavailable(f"Ollama 5xx: {response.status_code}")
                if response.status_code >= 400:
                    raise LLMUnavailable(f"Ollama 4xx: {response.status_code} - {response.text[:200]}")
                
                data = response.json()
                
                # Extract response
                msg = data.get("message", {})
                content = msg.get("content", "") or data.get("response", "")
                tool_calls = msg.get("tool_calls", [])
                
                return {
                    "content": content,
                    "tool_calls": tool_calls,
                    "model": config.model,
                    "provider": "ollama",
                }
                
        except (httpx.ReadTimeout, httpx.ConnectTimeout) as e:
            last_err = e
            if attempt < config.retries:
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            raise LLMTimeout(f"Ollama timed out after {config.retries + 1} attempts") from e
        except httpx.HTTPError as e:
            last_err = e
            if attempt < config.retries:
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            raise LLMUnavailable(f"Ollama HTTP error: {e}") from e
    
    raise LLMUnavailable(str(last_err) if last_err else "Unknown Ollama error")


async def _azure_openai_chat(
    config: LLMConfig,
    messages: List[Dict[str, str]],
    tools: Optional[List[Dict]] = None,
) -> Dict[str, Any]:
    """Call Azure OpenAI chat completions endpoint."""
    if not config.azure_endpoint or not config.azure_api_key:
        raise LLMUnavailable("Azure OpenAI not configured (missing AOAI_ENDPOINT or AOAI_KEY)")
    
    deployment = config.azure_deployment or config.model
    url = f"{config.azure_endpoint.rstrip('/')}/openai/deployments/{deployment}/chat/completions?api-version={config.azure_api_version}"
    
    headers = {
        "Content-Type": "application/json",
        "api-key": config.azure_api_key,
    }
    
    payload = {
        "messages": messages,
        "temperature": config.temperature,
        "max_tokens": config.max_tokens,
    }
    
    if tools:
        payload["tools"] = [{"type": "function", "function": t} for t in tools]
        payload["tool_choice"] = "auto"
    
    async with httpx.AsyncClient(timeout=config.timeout) as client:
        response = await client.post(url, headers=headers, json=payload)
        
        if response.status_code >= 400:
            raise LLMUnavailable(f"Azure OpenAI {response.status_code}: {response.text[:200]}")
        
        data = response.json()
        choice = data.get("choices", [{}])[0]
        msg = choice.get("message", {})
        
        # Parse tool calls
        tool_calls = []
        if msg.get("tool_calls"):
            for tc in msg["tool_calls"]:
                tool_calls.append({
                    "id": tc.get("id"),
                    "name": tc.get("function", {}).get("name"),
                    "arguments": json.loads(tc.get("function", {}).get("arguments", "{}")),
                })
        
        return {
            "content": msg.get("content", ""),
            "tool_calls": tool_calls,
            "model": config.model,
            "provider": "azure",
        }


async def _openai_chat(
    config: LLMConfig,
    messages: List[Dict[str, str]],
    tools: Optional[List[Dict]] = None,
) -> Dict[str, Any]:
    """Call OpenAI API chat completions endpoint."""
    if not config.openai_api_key:
        raise LLMUnavailable("OpenAI not configured (missing OPENAI_API_KEY)")
    
    url = "https://api.openai.com/v1/chat/completions"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {config.openai_api_key}",
    }
    
    payload = {
        "model": config.model,
        "messages": messages,
        "temperature": config.temperature,
        "max_tokens": config.max_tokens,
    }
    
    if tools:
        payload["tools"] = [{"type": "function", "function": t} for t in tools]
        payload["tool_choice"] = "auto"
    
    async with httpx.AsyncClient(timeout=config.timeout) as client:
        response = await client.post(url, headers=headers, json=payload)
        
        if response.status_code >= 400:
            raise LLMUnavailable(f"OpenAI {response.status_code}: {response.text[:200]}")
        
        data = response.json()
        choice = data.get("choices", [{}])[0]
        msg = choice.get("message", {})
        
        # Parse tool calls
        tool_calls = []
        if msg.get("tool_calls"):
            for tc in msg["tool_calls"]:
                tool_calls.append({
                    "id": tc.get("id"),
                    "name": tc.get("function", {}).get("name"),
                    "arguments": json.loads(tc.get("function", {}).get("arguments", "{}")),
                })
        
        return {
            "content": msg.get("content", ""),
            "tool_calls": tool_calls,
            "model": config.model,
            "provider": "openai",
        }


# =============================================================================
# Main LLM Client
# =============================================================================

class LLMClient:
    """
    High-level LLM client with provider abstraction.
    
    Usage:
        client = LLMClient.from_config()
        response = await client.chat(messages)
        response = await client.chat_with_tools(messages, tools)
    """
    
    def __init__(self, config: LLMConfig):
        self.config = config
    
    @classmethod
    def from_config(cls, config: Optional[LLMConfig] = None) -> "LLMClient":
        if config is None:
            config = LLMConfig.from_env()
        return cls(config)
    
    async def chat(
        self,
        messages: List[Dict[str, str]],
        system_prompt: Optional[str] = None,
    ) -> str:
        """Simple chat completion, returns content string."""
        if system_prompt:
            messages = [{"role": "system", "content": system_prompt}] + messages
        
        response = await self._dispatch(messages)
        return response.get("content", "")
    
    async def chat_with_tools(
        self,
        messages: List[Dict[str, str]],
        tools: List[Dict[str, Any]],
        system_prompt: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Chat with function calling support.
        
        Returns:
            {
                "content": str,
                "tool_calls": [{"id": str, "name": str, "arguments": dict}, ...]
            }
        """
        if system_prompt:
            messages = [{"role": "system", "content": system_prompt}] + messages
        
        return await self._dispatch(messages, tools)
    
    async def _dispatch(
        self,
        messages: List[Dict[str, str]],
        tools: Optional[List[Dict]] = None,
    ) -> Dict[str, Any]:
        """Dispatch to appropriate provider."""
        try:
            if self.config.provider == LLMProvider.AZURE_OPENAI:
                return await _azure_openai_chat(self.config, messages, tools)
            elif self.config.provider == LLMProvider.OPENAI:
                return await _openai_chat(self.config, messages, tools)
            else:  # Default to Ollama
                return await _ollama_chat(self.config, messages, tools)
                
        except LLMError as e:
            # Try fallback to Azure if configured
            if self.config.fallback_to_azure and self.config.provider != LLMProvider.AZURE_OPENAI:
                log.warning(f"Primary LLM failed ({e}), falling back to Azure OpenAI")
                try:
                    return await _azure_openai_chat(self.config, messages, tools)
                except Exception:
                    raise e  # Raise original error if fallback also fails
            raise


# =============================================================================
# Backward-Compatible Functions
# =============================================================================

# Module-level client for backward compatibility
_client: Optional[LLMClient] = None


def _get_client() -> LLMClient:
    global _client
    if _client is None:
        _client = LLMClient.from_config()
    return _client


async def chat_reasoning(
    question: str,
    contexts: List[str],
    temperature: float = 0.3,
    max_tokens: int = 1024,
    system_prompt: Optional[str] = None,
) -> str:
    """
    Backward-compatible reasoning function.
    
    Args:
        question: User's question
        contexts: Retrieved context strings
        temperature: Sampling temperature
        max_tokens: Max response tokens
        system_prompt: Optional custom system prompt
        
    Returns:
        LLM response string
    """
    client = _get_client()
    
    # Build context block
    ctx_block = "\n\n".join(f"- {c}" for c in contexts[:20]) if contexts else "(no context available)"
    
    messages = [
        {"role": "system", "content": system_prompt or DEFAULT_SYSTEM_PROMPT},
        {"role": "user", "content": f"Question:\n{question}\n\nRelevant logs/contexts:\n{ctx_block}"},
    ]
    
    response = await client._dispatch(messages)
    return response.get("content", "")


async def chat_with_tools(
    messages: List[Dict[str, str]],
    tools: List[Dict[str, Any]],
    system_prompt: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Chat with function calling support.
    Used by the resolution agent.
    """
    client = _get_client()
    return await client.chat_with_tools(messages, tools, system_prompt)

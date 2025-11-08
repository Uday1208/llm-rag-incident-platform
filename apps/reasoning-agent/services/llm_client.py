# apps/reasoning-agent/services/llm_client.py
# Purpose: Unified LLM client for local-llm (OpenAI-compatible or Ollama) + optional AOAI fallback.

import os
import json
from typing import List, Dict, Any, Optional
import httpx
import anyio

# --- Env (names kept simple & explicit) ---
LLM_KIND = (os.getenv("LOCAL_LLM_KIND") or "").strip().lower()       # "openai_compat" | "ollama"
LLM_BASE = (os.getenv("LOCAL_LLM_BASE_URL") or "").rstrip("/")       # e.g. http://local-llm:8000  or http://ollama:11434
LLM_MODEL = os.getenv("LOCAL_LLM_MODEL", "qwen2.5:7b")
LLM_KEY   = os.getenv("LOCAL_LLM_API_KEY", "")                       # optional for openai_compat
LLM_TIMEOUT = float(os.getenv("LOCAL_LLM_TIMEOUT", "15"))

# Optional fallback to Azure OpenAI if local fails
AOAI_ENDPOINT = (os.getenv("AOAI_ENDPOINT") or "").rstrip("/")
AOAI_KEY      = os.getenv("AOAI_KEY", "")
AOAI_DEPLOY   = os.getenv("AOAI_DEPLOYMENT", "")
AOAI_API_VER  = os.getenv("AOAI_API_VERSION", "2024-08-01-preview")
FALLBACK_AOAI = (os.getenv("FALLBACK_TO_AOAI", "false").lower() == "true")


def _sys_prompt() -> str:
    return (
        "You are a senior SRE assistant. Given a user question and related log contexts, "
        "triage the issue, infer likely root cause patterns, and return concise next steps. "
        "If evidence is weak, say that clearly."
    )


async def _openai_compat_chat(messages: List[Dict[str, str]], temperature: float, max_tokens: int) -> str:
    """
    Calls {LLM_BASE}/v1/chat/completions (OpenAI-compatible).
    """
    url = f"{LLM_BASE}/v1/chat/completions"
    headers = {"content-type": "application/json"}
    if LLM_KEY:
        headers["authorization"] = f"Bearer {LLM_KEY}"

    payload = {
        "model": LLM_MODEL,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as http:
        r = await http.post(url, headers=headers, json=payload)
        r.raise_for_status()
        data = r.json()
        return (data["choices"][0]["message"]["content"] or "").strip()


async def _ollama_chat(messages: List[Dict[str, str]], temperature: float, max_tokens: int) -> str:
    """
    Calls Ollama /api/chat.
    """
    url = f"{LLM_BASE}/api/chat"
    payload = {
        "model": LLM_MODEL,
        "messages": messages,
        "options": {
            "temperature": temperature,
            "num_predict": max_tokens
        },
        "stream": False
    }
    async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as http:
        r = await http.post(url, json=payload)
        r.raise_for_status()
        data = r.json()
        # Ollama can return either "message":{"content":...} or "response"
        if "message" in data and isinstance(data["message"], dict):
            return (data["message"].get("content") or "").strip()
        return (data.get("response") or "").strip()


async def _aoai_chat(messages: List[Dict[str, str]], temperature: float, max_tokens: int) -> str:
    """
    Simple AOAI chat calls to /openai/deployments/{dep}/chat/completions?api-version=...
    """
    if not (AOAI_ENDPOINT and AOAI_KEY and AOAI_DEPLOY):
        raise RuntimeError("AOAI not configured")
    url = f"{AOAI_ENDPOINT}/openai/deployments/{AOAI_DEPLOY}/chat/completions?api-version={AOAI_API_VER}"
    headers = {
        "content-type": "application/json",
        "api-key": AOAI_KEY,
    }
    payload = {
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as http:
        r = await http.post(url, headers=headers, json=payload)
        r.raise_for_status()
        data = r.json()
        return (data["choices"][0]["message"]["content"] or "").strip()


async def chat_reasoning(
    question: str,
    contexts: List[str],
    temperature: float = 0.2,
    max_tokens: int = 512,
    system_prompt: Optional[str] = None,
) -> str:
    """
    Unifies calling local-llm or AOAI. Returns assistant text.
    """
    sys = system_prompt or _sys_prompt()
    ctx_block = "\n\n".join(f"- {c}" for c in contexts[:20])

    messages = [
        {"role": "system", "content": sys},
        {"role": "user", "content": f"Question:\n{question}\n\nRelevant logs/contexts:\n{ctx_block}"},
    ]

    try:
        if LLM_KIND == "openai_compat":
            return await _openai_compat_chat(messages, temperature, max_tokens)
        elif LLM_KIND == "ollama":
            return await _ollama_chat(messages, temperature, max_tokens)
        else:
            # If not specified, try openai_compat first
            return await _openai_compat_chat(messages, temperature, max_tokens)
    except Exception as e:
        if FALLBACK_AOAI:
            try:
                return await _aoai_chat(messages, temperature, max_tokens)
            except Exception:
                # bubble original error if fallback also fails
                raise e
        raise

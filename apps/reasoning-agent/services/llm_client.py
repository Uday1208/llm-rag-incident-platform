# apps/reasoning-agent/services/llm_client.py
# Purpose: Unified LLM client for local-llm (OpenAI-compatible or Ollama) + optional AOAI fallback.

import os
import json
from typing import List, Dict, Any, Optional
import httpx
import asyncio, anyio

# --- Env (names kept simple & explicit) ---
LLM_KIND = (os.getenv("LOCAL_LLM_KIND") or "").strip().lower()       # "openai_compat" | "ollama"
LLM_BASE = (os.getenv("LOCAL_LLM_BASE_URL") or "").rstrip("/")       # e.g. http://local-llm:8000  or http://ollama:11434
LLM_BASE_URL = (os.getenv("LLM_BASE_URL") or "http://local-llm:11434").rstrip("/")
LLM_MODEL = os.getenv("LOCAL_LLM_MODEL", "qwen2.5:7b")
LLM_KEY   = os.getenv("LOCAL_LLM_API_KEY", "")                       # optional for openai_compat
LLM_TIMEOUT = float(os.getenv("LOCAL_LLM_TIMEOUT", "25"))            # bump from default
LLM_RETRIES  = int(os.getenv("LLM_RETRIES", "2")) 

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

class LLMTimeout(Exception):
    """Raised when LLM calls time out after retries."""
    pass

class LLMUnavailable(Exception):
    """Raised when the LLM endpoint returns 4xx/5xx or can’t be reached."""
    pass

async def _ollama_chat(messages: List[Dict[str, str]], temperature: float = 0.3, max_tokens: int = 512) -> str:
    """
    Talks to an Ollama-compatible endpoint: POST {LLM_BASE_URL}/api/chat
    Body: { model, messages, options{temperature,num_predict} }
    """
    url = f"{LLM_BASE_URL}/api/chat"
    payload = {
        "model": LLM_MODEL,
        "messages": messages,
        "options": {
            "temperature": temperature,
            "num_predict": max_tokens
        },
        "stream": False
    }

    last_err = None
    for attempt in range(LLM_RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=LLM_TIMEOUT) as http:
                r = await http.post(url, json=payload)
                if r.status_code >= 500:
                    raise LLMUnavailable(f"LLM 5xx: {r.status_code}")
                if r.status_code >= 400:
                    # pass back text to the caller; often model not loaded etc.
                    raise LLMUnavailable(f"LLM 4xx: {r.status_code} {r.text[:200]}")
                data = r.json()
                # Ollama chat returns { message: { role, content }, ... }
                msg = (data.get("message") or {}).get("content") or data.get("response")
                return msg or ""
        except (httpx.ReadTimeout, httpx.ConnectTimeout) as e:
            last_err = e
            if attempt < LLM_RETRIES:
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            raise LLMTimeout(f"LLM request timed out after {LLM_RETRIES+1} attempts") from e
        except httpx.HTTPError as e:
            last_err = e
            if attempt < LLM_RETRIES:
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            raise LLMUnavailable(f"LLM HTTP error: {e}") from e
    # Fallback (shouldn’t hit)
    raise LLMUnavailable(str(last_err) if last_err else "unknown LLM error")



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
    temperature: float = 0.3,
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

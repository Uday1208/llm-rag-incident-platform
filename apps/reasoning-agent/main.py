"""
File: main.py
Service: reasoning-agent (FastAPI)
Purpose: Compose an RCA + recommended fix from RAG contexts (and optional anomaly score)
         using Azure OpenAI (default) with a local Transformers fallback for offline tests.

Endpoints:
- GET  /health   : liveness check
- GET  /metrics  : Prometheus metrics
- POST /v1/reason: generate cause/recommendation from query + contexts

Env:
- USE_AOAI=true|false
- AOAI_ENDPOINT=https://<your-aoai>.openai.azure.com
- AOAI_API_KEY=********
- AOAI_DEPLOYMENT=<chat-model-deployment>  # e.g., gpt-4o-mini
- LLM_TEMPERATURE=0.3
- LOCAL_MODEL=google/flan-t5-base           # only used if USE_AOAI=false
"""

import os
import json
from typing import List, Optional

import httpx
from fastapi import FastAPI
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram, generate_latest

# --- Config ---
USE_AOAI = os.getenv("USE_AOAI", "true").lower() == "true"
AOAI_ENDPOINT = os.getenv("AOAI_ENDPOINT", "").rstrip("/")
AOAI_DEPLOYMENT = os.getenv("AOAI_DEPLOYMENT", "")
AOAI_API_KEY = os.getenv("AOAI_API_KEY", "")
TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0.3"))

# --- Metrics ---
REQS = Counter("reason_requests_total", "Total /v1/reason requests")
ERRS = Counter("reason_errors_total", "Total /v1/reason errors")
LAT  = Histogram("reason_seconds", "Latency of /v1/reason")

# --- Schemas ---
class ContextItem(BaseModel):
    """One retrieved context chunk used for grounding."""
    id: str = Field(..., description="Document or chunk id")
    content: str = Field(..., description="Grounding text")
    score: Optional[float] = Field(None, description="Similarity score or rank")

class ReasonIn(BaseModel):
    """Input schema for reasoning endpoint."""
    query: str
    contexts: List[ContextItem] = []
    anomaly_signal: Optional[dict] = None

class ReasonOut(BaseModel):
    """Normalized output from the reasoning step."""
    cause: str
    recommendation: str
    confidence: float = 0.5
    used_context_ids: List[str] = []

# --- App ---
app = FastAPI(title="reasoning-agent", version="1.0.0")

@app.get("/health")
def health() -> dict:
    """Return basic liveness info."""
    return {"ok": True, "use_aoai": USE_AOAI}

@app.get("/metrics")
def metrics():
    """Expose Prometheus metrics."""
    return generate_latest()

@app.post("/v1/reason", response_model=ReasonOut)
@LAT.time()
async def reason(payload: ReasonIn) -> ReasonOut:
    """Compose an RCA and fix from query + contexts (and optional anomaly signal)."""
    REQS.inc()
    prompt = _build_prompt(payload)
    try:
        if USE_AOAI:
            text = await _call_aoai_chat(prompt, TEMPERATURE)
        else:
            text = _run_local_flan(prompt, TEMPERATURE)
        return _coerce_json(text, payload.contexts)
    except Exception:
        ERRS.inc()
        # Safe fallback response
        return ReasonOut(
            cause="Unknown",
            recommendation="Insufficient context to determine a root cause. Collect additional logs and retry.",
            confidence=0.3,
            used_context_ids=[c.id for c in payload.contexts[:5]]
        )

# --- Helpers ---
def _build_prompt(p: ReasonIn) -> str:
    """Construct an instruction-following prompt with grounding context."""
    ctx_lines = "\n".join(
        f"- [{c.id}] {c.content}" for c in p.contexts[:8] if c.content
    )
    anom = f"\nAnomaly: {json.dumps(p.anomaly_signal)}" if p.anomaly_signal else ""
    return (
        "You are a senior SRE assistant.\n"
        "Given the historical incident logs and resolutions, identify the most probable cause and a safe, actionable fix.\n"
        "Respond ONLY as strict JSON with keys: cause, recommendation, confidence (0..1).\n"
        f"User Query: {p.query}\n"
        f"Context:\n{ctx_lines}{anom}\n"
        "JSON:"
    )

async def _call_aoai_chat(prompt: str, temperature: float) -> str:
    """Call Azure OpenAI Chat Completions endpoint; return message content."""
    headers = {"api-key": AOAI_API_KEY, "Content-Type": "application/json"}
    body = {
        "messages": [{"role": "user", "content": prompt}],
        "temperature": temperature,
        "max_tokens": 512
    }
    url = f"{AOAI_ENDPOINT}/openai/deployments/{AOAI_DEPLOYMENT}/chat/completions?api-version=2024-02-15-preview"
    async with httpx.AsyncClient(timeout=20) as cx:
        r = await cx.post(url, headers=headers, json=body)
        r.raise_for_status()
        j = r.json()
        return j["choices"][0]["message"]["content"]

def _run_local_flan(prompt: str, temperature: float) -> str:
    """Use a local Transformers pipeline as a fallback for offline testing."""
    from transformers import pipeline
    model = os.getenv("LOCAL_MODEL", "google/flan-t5-base")
    pipe = pipeline(
        "text2text-generation",
        model=model,
        max_new_tokens=256,
        do_sample=(temperature > 0.01),
        temperature=temperature,
    )
    out = pipe(prompt)[0]["generated_text"]
    return out

def _coerce_json(text: str, ctxs: List[ContextItem]) -> ReasonOut:
    """Parse LLM output into ReasonOut; fall back if not valid JSON."""
    try:
        j = json.loads(text)
        return ReasonOut(
            cause=str(j.get("cause", "Unknown")).strip(),
            recommendation=str(j.get("recommendation", "Collect more context and retry.")).strip(),
            confidence=float(j.get("confidence", 0.5)),
            used_context_ids=[c.id for c in ctxs[:5]],
        )
    except Exception:
        return ReasonOut(
            cause="Unknown",
            recommendation="Insufficient context to determine a root cause.",
            confidence=0.3,
            used_context_ids=[c.id for c in ctxs[:5]],
        )

if __name__ == "__main__":
    # Local dev: uvicorn apps.reasoning-agent.main:app --reload
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8080")))

"""
File: main.py
Service: reasoning-agent (FastAPI)
Purpose: Compose an RCA + recommended fix from RAG contexts (and optional anomaly score)
         using Azure OpenAI (default) with a local Transformers fallback for offline tests.

Endpoints:
- GET  /health       : liveness check
- GET  /metrics      : Prometheus metrics
- POST /v1/reason    : generate cause/recommendation from query + contexts
- POST /v1/agent/*   : ReAct-based agentic resolution

Env:
- LLM_PROVIDER=ollama|azure|openai
- LLM_MODEL=qwen2.5:7b
- AOAI_ENDPOINT=https://<your-aoai>.openai.azure.com
- AOAI_KEY=********
- AOAI_CHAT_DEPLOYMENT=<chat-model-deployment>
"""

from fastapi import FastAPI
from routers import reason_router, search_router
from routers.agent import router as agent_router

app = FastAPI(
    title="LLM-RAG Reasoning Agent",
    description="Incident reasoning and resolution using RAG + LLM",
    version="2.0.0",
)

# Include routers
app.include_router(reason_router)
app.include_router(search_router)
app.include_router(agent_router)  # Agentic resolution


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "reasoning-agent"}


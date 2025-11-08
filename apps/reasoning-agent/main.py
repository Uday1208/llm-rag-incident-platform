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

# apps/reasoning-agent/main.py
from fastapi import FastAPI
from routers.reason import router as reason_router

app = FastAPI()
app.include_router(reason_router)

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

import os
from contextlib import asynccontextmanager
from fastapi import FastAPI

# OpenTelemetry
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from routers import reason_router, search_router, llm
from routers.agent import router as agent_router
from logging_setup import configure_logging

# Initialize OpenTelemetry tracer
trace.set_tracer_provider(TracerProvider())
# trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    configure_logging(os.getenv("LOG_LEVEL", "INFO"))
    yield

app = FastAPI(
    title="LLM-RAG Reasoning Agent",
    description="Incident reasoning and resolution using RAG + LLM",
    version="2.0.0",
    lifespan=lifespan,
)

# Instrument FastAPI for distributed tracing
FastAPIInstrumentor.instrument_app(app)

# Include routers
app.include_router(reason_router)
app.include_router(search_router)
app.include_router(agent_router)  # Agentic resolution
app.include_router(llm.router)


@app.get("/health")
async def health():
    return {"status": "healthy", "service": "reasoning-agent"}



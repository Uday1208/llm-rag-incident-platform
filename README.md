# LLM-RAG Incident Intelligence Platform

An AI-powered incident resolution platform that combines **Retrieval-Augmented Generation (RAG)** with **agentic workflows** to help SREs quickly diagnose and resolve production incidents.

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        Event-Driven Ingestion                            │
│  Application Insights → Blob Storage → Preprocessor → rag-worker (DB)   │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
┌─────────────────────────────────▼───────────────────────────────────────┐
│                      RAG + Agentic Resolution                            │
│  User Query → reasoning-agent (ReAct) → vector search → LLM → Answer    │
└─────────────────────────────────────────────────────────────────────────┘
```

### Architecture Patterns

| Pattern | Implementation | Purpose |
|---------|---------------|---------|
| **Event-Driven** | Ingestor + Preprocessor | Async log ingestion from Event Hub/Blob |
| **Microservices** | 4 deployable services | Independent scaling and deployment |
| **RAG** | pgvector + embeddings | Ground LLM answers in real incident data |
| **ReAct Agent** | Custom + LangChain | Iterative reasoning with tool calling |

## 📦 Services

| Service | Port | Description |
|---------|------|-------------|
| `ingestor` | 8001 | Event Hub consumer, log normalization |
| `preprocessor` | 8002 | Batch processing, TraceID bundling, LLM summarization |
| `rag-worker` | 8000 | Embeddings, vector search, database owner |
| `reasoning-agent` | 8003 | ReAct agent for incident resolution |

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- PostgreSQL with pgvector extension
- Redis (optional, for caching)
- Ollama (for local LLM) or Azure OpenAI

### Local Development

```bash
# Clone the repository
git clone https://github.com/Uday1208/llm-rag-incident-platform.git
cd llm-rag-incident-platform

# Start rag-worker
cd apps/rag-worker
pip install -r requirements.txt
export PG_HOST=localhost PG_USER=postgres PG_PASS=secret PG_DB=ragincdb
uvicorn worker.main:app --port 8000

# Start reasoning-agent
cd apps/reasoning-agent
pip install -r requirements.txt
export RAG_WORKER_URL=http://localhost:8000
uvicorn main:app --port 8003
```

### Environment Variables

```bash
# Database
PG_HOST=your-postgres-host
PG_USER=postgres
PG_PASS=secret
PG_DB=ragincdb

# Embeddings (default: local MiniLM)
EMBED_PROVIDER=local          # local | azure | openai
EMBED_MODEL_NAME=all-MiniLM-L6-v2
EMBED_DIM=384

# LLM (default: Ollama)
LLM_PROVIDER=ollama           # ollama | azure | openai
LLM_MODEL=qwen2.5:7b
OLLAMA_BASE_URL=http://localhost:11434

# Azure OpenAI (optional)
AOAI_ENDPOINT=https://your-instance.openai.azure.com
AOAI_KEY=your-key
AOAI_CHAT_DEPLOYMENT=gpt-4o-mini
AOAI_EMBED_DEPLOYMENT=text-embedding-ada-002

# Agent type
AGENT_TYPE=custom             # custom | langchain
```

## 🤖 Agentic Resolution API

### Resolve an Incident

```bash
POST /v1/agent/resolve
{
  "query": "Payment API returning 504 errors since 09:30",
  "agent_type": "custom"  # or "langchain"
}
```

### Compare Agent Implementations

```bash
POST /v1/agent/compare
{
  "query": "Database connection timeout on order-service"
}
# Returns timing and results from both custom and LangChain agents
```

### Available Tools

| Tool | Description |
|------|-------------|
| `search_incidents` | Semantic search over past incidents |
| `search_resolutions` | Find how similar issues were resolved |
| `analyze_trace` | Get detailed trace analysis |
| `suggest_resolution` | Generate actionable resolution steps |
| `get_service_health` | Check service health status |

## 🗄️ Database Schema

Managed with **Alembic** migrations:

```bash
cd apps/rag-worker
alembic upgrade head
```

### Tables

| Table | Purpose |
|-------|---------|
| `incident_bundles` | TraceID-correlated incident groups |
| `resolutions` | Past resolutions with runbook steps |
| `agent_sessions` | ReAct agent conversation history |
| `embedding_cache` | Cached embeddings for performance |
| `service_health` | Service health tracking |

## 📊 Key Features

- **TraceID Correlation**: Groups related logs using Application Insights `operation_Id`
- **Multi-Provider Embeddings**: Local (MiniLM), Azure OpenAI, or OpenAI
- **Dual Agent Support**: Custom ReAct or LangChain implementations
- **LLM Summarization**: Structured summaries (symptoms, failing_dependency, error_signature)
- **Configurable Triggers**: Schedule, event, or on-demand preprocessing

## 📚 Interview Notes

See [INTERVIEW_NOTES.md](./INTERVIEW_NOTES.md) for detailed architecture explanations suitable for technical interviews.

## 🛠️ Development

### Project Structure

```
apps/
├── ingestor/           # Event Hub consumer
├── preprocessor/       # Batch processing pipeline
├── rag-worker/         # Embeddings + vector search
│   ├── worker/
│   │   ├── config.py       # Centralized configuration
│   │   ├── embeddings.py   # Multi-provider embeddings
│   │   ├── models.py       # SQLAlchemy models
│   │   └── db.py           # Database connection
│   └── alembic/            # Database migrations
└── reasoning-agent/    # ReAct agent
    ├── agent/
    │   ├── resolver.py     # Custom ReAct implementation
    │   ├── langchain_agent.py  # LangChain implementation
    │   ├── factory.py      # Agent switcher
    │   ├── tools.py        # Tool definitions
    │   └── executor.py     # Tool implementations
    └── routers/
```

### Running Tests

```bash
# Unit tests
pytest apps/rag-worker/tests/
pytest apps/reasoning-agent/tests/

# Integration tests (requires running services)
pytest tests/integration/
```

## 📝 License

MIT License - see [LICENSE](./LICENSE) for details.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

---

Built with ❤️ for SREs who want smarter incident resolution.
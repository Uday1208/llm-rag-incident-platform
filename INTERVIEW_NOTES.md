# LLM-RAG Incident Platform — Interview Notes

## Architecture Overview

### Pattern: Event-Driven Microservices + RAG

```
┌─────────────────────────────────────────────────────────────────┐
│                     Event-Driven Ingestion                       │
│  App Insights → Blob Storage → Preprocessor → rag-worker (DB)   │
└─────────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────┐
│              RAG-based Reasoning Layer                           │
│  User Query → reasoning-agent → (embed + search) → LLM → Answer │
└─────────────────────────────────────────────────────────────────┘
```

**Architecture styles used:**
| Style | Where Used | Why |
|-------|-----------|-----|
| **Event-Driven** | Ingestor consuming from Event Hub/Blob | Decouples log producers from consumers |
| **Microservices** | Separate deployable services | Independent scaling and deployment |
| **RAG Pattern** | reasoning-agent + rag-worker | Grounds LLM answers in real incident data |
| **API Gateway** | api-gateway service | Single entry point, routing, auth |

---

## Q&A: Database per Microservice

**Q: Should each microservice have its own database?**

**A: Not necessarily. "Database per microservice" is a guideline, not a rule.**

**Our approach: Database per Bounded Context**
- `rag-worker` owns the database, exposes data via APIs
- Other services (ingestor, reasoning-agent) call rag-worker APIs
- **No direct DB access from multiple services** = loose coupling maintained

**When to split databases:**
- Different data domains (user auth vs. incidents)
- Different scaling needs (write-heavy vs. read-heavy)
- Compliance requirements (PII isolation)
- Team ownership boundaries

**Our design is valid because:**
- All data is same bounded context (incidents/resolutions)
- pgvector needs all embeddings in one place for similarity search
- Simpler operations (single backup, connection pool)

---

## Service Responsibilities

| Service | Responsibility | Stateful? |
|---------|---------------|-----------|
| **ingestor** | Consume logs, normalize, forward to rag-worker | No |
| **rag-worker** | Store documents, embeddings, vector search | Yes (owns Postgres) |
| **reasoning-agent** | Agentic resolution flow, LLM orchestration | No (stateless) |
| **local-llm** | Ollama model serving | No |
| **api-gateway** | Routing, auth | No |

---

## Key Patterns for Interview

### 1. RAG (Retrieval-Augmented Generation)
- **Problem:** LLMs hallucinate and lack domain knowledge
- **Solution:** Retrieve relevant context → inject into prompt → generate
- **Our stack:** pgvector for retrieval, Azure OpenAI for generation

### 2. ReAct Agent Pattern (Our Implementation)

**What is it?**
- ReAct = **Re**asoning + **Act**ing
- LLM decides **which tools to call** based on reasoning
- Iterative loop: Think → Act → Observe → Repeat

**Our ReAct Loop:**
```
User Query: "Payment API returning 504 errors"
     ↓
THINK: "I should search for similar incidents"
     ↓
ACT: call search_incidents(query="Payment API 504")
     ↓
OBSERVE: Found 3 similar incidents
     ↓
THINK: "Let me check if there are past resolutions"
     ↓
ACT: call search_resolutions(...)
     ↓
OBSERVE: Found resolution about connection pool
     ↓
RESPOND: "Based on past incidents, check connection pool..."
```

**Tools Available:**
| Tool | Purpose |
|------|---------|
| `search_incidents` | Semantic search over past incidents |
| `search_resolutions` | Find how similar issues were resolved |
| `analyze_trace` | Get detailed trace analysis |
| `suggest_resolution` | Generate actionable steps |
| `get_service_health` | Check service status |

**Why ReAct over Chain-of-Thought?**
- CoT just generates text; ReAct can **execute actions**
- ReAct grounds reasoning in **real data** (RAG)
- Fallback handling when evidence is weak

### 3. TraceID Correlation
- **Problem:** Logs are individual lines, incidents span multiple services
- **Solution:** Group by `operation_Id` (distributed trace context)
- Creates "incident bundles" for semantic search

### 4. Vector Similarity Search
- Embeddings: text → 1536-dim vector (Azure OpenAI) or 384-dim (local)
- Search: cosine similarity via pgvector
- Index: IVFFlat for approximate nearest neighbors

---

## Data Flow

```
1. Microservices → Application Insights (logs with TraceID)
2. Diagnostic Settings → Blob Storage (JSON export)
3. Preprocessor (scheduled/event) → TraceID grouping with Pandas
4. GPT-4o-mini summarization → structured summary
5. Embedding → Postgres/pgvector
6. User query → reasoning-agent → vector search → LLM answer
```

---

## Why These Technology Choices?

| Choice | Reason |
|--------|--------|
| **Postgres + pgvector** | Single DB for relational + vector; operational simplicity |
| **Azure OpenAI** | Enterprise compliance, SLA, no GPU management |
| **FastAPI** | Async, auto OpenAPI docs, Python ML ecosystem |
| **Alembic** | DB migrations with version control, works across DB backends |
| **Event Hub/Blob** | Native Azure integration, replay capability |

---

## Cost Optimization

- Filter logs to ERROR/WARN only before processing
- Batch embedding calls (reduce API calls)
- Cache embeddings in Redis (avoid recomputation)
- Use `text-embedding-3-small` over `ada-002` (cheaper, comparable quality)

---

## Cleanup TODO

- [ ] Remove `ts-model` (TorchServe, not needed with Azure OpenAI)
- [ ] Remove `local-llm` if not using Ollama fallback
- [ ] Consolidate to 4 services: ingestor, preprocessor, rag-worker, reasoning-agent

---

## Custom vs LangChain Agent (Interview Gold!)

**Q: Why implement your own ReAct agent instead of using LangChain?**

| Aspect | Custom Implementation | LangChain |
|--------|----------------------|-----------|
| **Lines of Code** | ~300 lines | 1000s (with deps) |
| **Dependencies** | Just `httpx` | Heavy dependency tree |
| **Transparency** | Full control, easy to debug | Abstraction layers |
| **Flexibility** | Tailored to exact needs | Generic patterns |
| **Learning** | Understand every line | Black box for interviews |

**Our Hybrid Approach:**
```python
# Switch via environment or per-request
AGENT_TYPE=custom  # or langchain

# Compare performance
POST /v1/agent/compare
{"query": "Payment API 504 errors"}
```

**When to use each:**
- **Custom**: Production, debugging, interviews
- **LangChain**: Prototyping, complex chains, ecosystem integrations

**Interview Answer:**
> "We implemented both to understand tradeoffs. Custom gives us full control
> and debuggability. LangChain is great for rapid prototyping but adds
> dependency complexity. For production, we prefer custom because we can
> explain and debug every line."


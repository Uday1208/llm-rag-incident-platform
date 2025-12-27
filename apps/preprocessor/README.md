# Preprocessor

Batch preprocessing pipeline for Application Insights logs.

## Features

- **TraceID correlation**: Groups related logs by `operation_Id`
- **LLM summarization**: Generates structured summaries (symptoms, failing_dependency, error_signature)
- **Configurable triggers**: schedule, event, on-demand

## Trigger Modes

| Mode | Description | Configuration |
|------|-------------|---------------|
| `schedule` | Timer-based (cron) | `PREPROCESS_CRON="*/15 * * * *"` |
| `event` | Blob trigger | Use with Azure Functions |
| `on-demand` | HTTP API | Call `POST /process` |

## Environment Variables

```bash
# Trigger
PREPROCESS_TRIGGER=schedule|event|on-demand
PREPROCESS_CRON="*/15 * * * *"  # For schedule mode

# Blob Storage
BLOB_CONN=<connection-string>
BLOB_CONTAINER=raw-logs
BLOB_PREFIX=appinsights/
PROCESSED_PREFIX=processed/

# Processing
PREPROCESS_BATCH_SIZE=1000
PREPROCESS_MIN_SEVERITY=WARNING
TRACE_WINDOW_SECONDS=60

# Output
RAG_WORKER_URL=http://rag-worker:8000

# LLM (optional)
ENABLE_LLM_SUMMARY=true
LLM_PROVIDER=ollama|azure|openai
```

## API Endpoints

### Health Check
```bash
GET /health
```

### Process Logs (On-demand)
```bash
# Process unprocessed blobs
POST /process
{"limit": 100}

# Process specific date
POST /process
{"date": "2025-12-27"}

# Process async (returns immediately)
POST /process/async
{"limit": 100}
```

## Local Development

```bash
cd apps/preprocessor
pip install -r requirements.txt
python -m main
```

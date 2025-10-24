"""
File: main.py
Service: ingestor
Purpose: Consume logs from Azure Event Hubs, archive raw to Blob Storage,
         normalize to a canonical schema, and forward normalized docs to rag-worker /v1/ingest.

Endpoints:
- GET /health   : liveness check
- GET /metrics  : Prometheus metrics

Env Vars:
- EVENTHUB_CONN           : Event Hub connection string
- EVENTHUB_NAME           : Event Hub name
- EVENTHUB_CONSUMER       : Consumer group (default: $Default)
- BLOB_CONN               : Azure Blob Storage connection string
- BLOB_CONTAINER          : Container name for raw archives (e.g., raw-logs)
- RAW_PREFIX              : Optional prefix in container (default: "eh/")
- RAG_WORKER_URL          : http://rag-worker.internal.<env-fqdn>/v1/ingest
- RAG_WORKER_TOKEN        : Optional bearer token for rag-worker (if you enforce auth)
- NORMALIZE_SOURCE_FIELD  : Which EH json field maps to 'source' (default: "resourceId")
- NORMALIZE_MESSAGE_FIELD : Which EH json field maps to 'message' (default: "message")
- NORMALIZE_TS_FIELD      : Which EH json field maps to timestamp (default: "time")
- SERVICE_NAME            : for logs/metrics tagging (default: "ingestor")
"""

import asyncio
import hashlib
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, generate_latest
from python_json_logger import jsonlogger

from azure.eventhub.aio import EventHubConsumerClient
from azure.storage.blob import BlobServiceClient

# ----------------- Config -----------------
EVENTHUB_CONN  = os.getenv("EVENTHUB_CONN", "")
EVENTHUB_NAME  = os.getenv("EVENTHUB_NAME", "")
CONSUMER_GROUP = os.getenv("EVENTHUB_CONSUMER", "$Default")

BLOB_CONN      = os.getenv("BLOB_CONN", "")
BLOB_CONTAINER = os.getenv("BLOB_CONTAINER", "raw-logs")
RAW_PREFIX     = os.getenv("RAW_PREFIX", "eh/")

RAG_WORKER_URL = os.getenv("RAG_WORKER_URL", "")
RAG_WORKER_TOKEN = os.getenv("RAG_WORKER_TOKEN", "")

SRC_FIELD = os.getenv("NORMALIZE_SOURCE_FIELD", "resourceId")
MSG_FIELD = os.getenv("NORMALIZE_MESSAGE_FIELD", "message")
TS_FIELD  = os.getenv("NORMALIZE_TS_FIELD", "time")

SERVICE_NAME = os.getenv("SERVICE_NAME", "ingestor")

BATCH_MAX = int(os.getenv("BATCH_MAX", "50"))      # max docs per ingest POST
BATCH_FLUSH_SECS = float(os.getenv("BATCH_FLUSH_SECS", "2.0"))  # flush window

# ----------------- Metrics -----------------
INGEST_EH_EVENTS   = Counter("ingestor_eh_events_total", "Total events consumed from Event Hubs")
INGEST_RAW_BYTES   = Counter("ingestor_raw_bytes_total", "Total bytes archived to Blob")
INGEST_DOCS_NORM   = Counter("ingestor_docs_normalized_total", "Total normalized docs emitted")
INGEST_POST_OK     = Counter("ingestor_post_ok_total", "Total successful /v1/ingest posts")
INGEST_POST_ERR    = Counter("ingestor_post_err_total", "Total failed /v1/ingest posts")
LOOP_LATENCY       = Histogram("ingestor_loop_seconds", "Ingest loop iteration latency seconds")

# ----------------- Logging -----------------
import logging
logger = logging.getLogger(SERVICE_NAME)
logger.setLevel(logging.INFO)
_handler = logging.StreamHandler()
_handler.setFormatter(jsonlogger.JsonFormatter())
logger.addHandler(_handler)

# ----------------- FastAPI -----------------
app = FastAPI(title="ingestor", version="1.0.0")

@app.get("/health")
def health():
    """Return liveness and config sanity (redacted)."""
    return {
        "ok": True,
        "eventhub": bool(EVENTHUB_NAME),
        "blob_container": BLOB_CONTAINER,
        "rag_worker_url": bool(RAG_WORKER_URL),
    }

@app.get("/metrics")
def metrics():
    """Expose Prometheus metrics."""
    return generate_latest()

# ----------------- Helpers -----------------
def _utc_iso(dt: Optional[str]) -> str:
    """Normalize timestamp to UTC ISO-8601; if none, now()."""
    if not dt:
        return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
    try:
        # try native parse of common formats
        return datetime.fromisoformat(dt.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
    except Exception:
        return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

def _sha1_id(source: str, ts: str, content: str) -> str:
    """Deterministic id (sha1) from source+ts+content."""
    h = hashlib.sha1()
    h.update(source.encode("utf-8", "ignore"))
    h.update(ts.encode("utf-8", "ignore"))
    h.update(content.encode("utf-8", "ignore"))
    return h.hexdigest()

def _archive_blob_name(partition_id: str) -> str:
    """Build a unique blob path for raw event payload."""
    now = datetime.utcnow()
    return f"{RAW_PREFIX}y={now:%Y}/m={now:%m}/d={now:%d}/h={now:%H}/p={partition_id}/{int(time.time()*1000)}.json"

def _normalize_one(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Normalize one EH payload into canonical doc for embeddings."""
    # Many Azure logs are JSON with keys like time, resourceId, category, level, message
    # Fallbacks if fields are absent.
    source = str(payload.get(SRC_FIELD) or payload.get("category") or payload.get("source") or "unknown").strip()
    content = payload.get(MSG_FIELD) or payload.get("msg") or payload.get("content")
    if not content:
        # some services place log in 'properties' or 'log'
        props = payload.get("properties") or payload.get("log")
        if isinstance(props, dict):
            content = json.dumps(props)
        elif isinstance(props, str):
            content = props
    if not content:
        return None
    ts_iso = _utc_iso(str(payload.get(TS_FIELD) or payload.get("timestamp") or payload.get("timeGenerated") or ""))
    doc_id = _sha1_id(source, ts_iso, content)
    return {
        "id": doc_id,
        "source": source[:128],
        "ts": ts_iso,
        "content": str(content)[:5000],  # safety bound
    }

async def _post_to_rag_worker(docs: List[Dict[str, Any]]):
    """POST documents to rag-worker /v1/ingest in batches."""
    if not docs:
        return
    headers = {"Content-Type": "application/json"}
    if RAG_WORKER_TOKEN:
        headers["Authorization"] = f"Bearer {RAG_WORKER_TOKEN}"

    # rag-worker expects {"documents":[{id,source,ts,content}]}
    for i in range(0, len(docs), BATCH_MAX):
        batch = docs[i:i+BATCH_MAX]
        body = {"documents": batch}
        try:
            async with httpx.AsyncClient(timeout=20) as cx:
                r = await cx.post(RAG_WORKER_URL, headers=headers, json=body)
                r.raise_for_status()
                INGEST_POST_OK.inc()
        except Exception as e:
            INGEST_POST_ERR.inc()
            logger.error({"msg": "rag-worker ingest failed", "err": str(e), "batch_size": len(batch)})

# ----------------- EH / Blob clients (initialized at startup) -----------------
eh_client: Optional[EventHubConsumerClient] = None
blob_client: Optional[BlobServiceClient] = None

# ----------------- EH Callbacks -----------------
async def on_event(partition_context, event):
    """Event Hubs callback per event: archive raw, push normalized."""
    data = event.body_as_str(encoding="UTF-8")
    partition_id = partition_context.partition_id
    INGEST_EH_EVENTS.inc()

    # 1) archive raw to Blob
    try:
        blob_name = _archive_blob_name(partition_id)
        b = blob_client.get_blob_client(container=BLOB_CONTAINER, blob=blob_name)
        payload_raw = {
            "enqueued_time_utc": event.enqueued_time.astimezone(timezone.utc).isoformat() if event.enqueued_time else None,
            "system_properties": {k.decode() if isinstance(k, bytes) else k: (v.decode() if isinstance(v, bytes) else v)
                                  for k, v in (event.system_properties or {}).items()},
            "body": data,
        }
        content_bytes = json.dumps(payload_raw, ensure_ascii=False).encode("utf-8")
        b.upload_blob(content_bytes, overwrite=False)
        INGEST_RAW_BYTES.inc(len(content_bytes))
    except Exception as e:
        logger.error({"msg":"blob archive failed","err":str(e)})

    # 2) normalize + buffer for ingest
    docs: List[Dict[str, Any]] = []
    try:
        # EH body may be a JSON string or raw text
        try:
            j = json.loads(data)
            if isinstance(j, dict):
                doc = _normalize_one(j)
                if doc: docs.append(doc)
            elif isinstance(j, list):
                for it in j:
                    if isinstance(it, dict):
                        d = _normalize_one(it)
                        if d: docs.append(d)
        except json.JSONDecodeError:
            # treat as plain line
            doc = _normalize_one({SRC_FIELD: "eventhub", TS_FIELD: None, MSG_FIELD: data})
            if doc: docs.append(doc)
    except Exception as e:
        logger.error({"msg":"normalize failed","err":str(e)})

    if docs:
        INGEST_DOCS_NORM.inc(len(docs))
        await _post_to_rag_worker(docs)

async def on_error(partition_context, error):
    """EH callback on errors."""
    logger.error({"msg": "eventhub error", "partition": getattr(partition_context, "partition_id", None), "err": str(error)})

async def on_partition_initialize(partition_context):
    """EH callback when a partition is initialized."""
    logger.info({"msg":"partition init","partition":partition_context.partition_id})

async def on_partition_close(partition_context, reason):
    """EH callback when a partition is closed."""
    logger.info({"msg":"partition close","partition":partition_context.partition_id,"reason":reason})

# ----------------- Lifespan -----------------
@app.on_event("startup")
async def startup():
    """Initialize EH consumer and Blob client, start receive loop in background."""
    global eh_client, blob_client
    assert EVENTHUB_CONN and EVENTHUB_NAME and BLOB_CONN and BLOB_CONTAINER and RAG_WORKER_URL, "Missing required env vars"

    blob_client = BlobServiceClient.from_connection_string(BLOB_CONN)
    # ensure container exists
    try:
        blob_client.create_container(BLOB_CONTAINER)
    except Exception:
        pass

    eh_client = EventHubConsumerClient.from_connection_string(
        conn_str=EVENTHUB_CONN, consumer_group=CONSUMER_GROUP, eventhub_name=EVENTHUB_NAME
    )

    async def _run():
        while True:
            with LOOP_LATENCY.time():
                try:
                    await eh_client.receive(
                        on_event=on_event,
                        on_error=on_error,
                        on_partition_initialize=on_partition_initialize,
                        on_partition_close=on_partition_close,
                        starting_position="-1",  # from beginning; change to "@latest" for only-new
                    )
                except Exception as e:
                    logger.error({"msg":"receive loop failed; retrying","err":str(e)})
                    await asyncio.sleep(2)

    app.state._task = asyncio.create_task(_run())
    logger.info({"msg": "ingestor started"})

@app.on_event("shutdown")
async def shutdown():
    """Graceful shutdown of EH client."""
    task = getattr(app.state, "_task", None)
    if task:
        task.cancel()
    if eh_client:
        await eh_client.close()
    logger.info({"msg": "ingestor stopped"})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8080")))

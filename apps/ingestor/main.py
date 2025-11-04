# apps/ingestor/main.py
# Purpose: Consume Event Hub app logs, normalize, archive to Blob, and forward to rag-worker.
# Notes:
#  - Uses your original env var names (BLOB_CONN, BLOB_CONTAINER, etc.)
#  - starting_position set to "@latest" to only process new events
#  - Filters out Azure Monitor "metrics" payloads, keeps Console/System logs

"""
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

import os, json, asyncio, hashlib, logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI
from starlette.responses import PlainTextResponse

from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub import EventData
from azure.storage.blob.aio import BlobServiceClient

# -----------------------
# Environment (UNCHANGED names)
# -----------------------
EVENTHUB_CONN = os.getenv("EVENTHUB_CONN", "")          # NAMESPACE-level listen rule
EVENTHUB_NAME = os.getenv("EVENTHUB_NAME", "")          # e.g., eh-rag-logs
EVENTHUB_CONSUMER = os.getenv("EVENTHUB_CONSUMER", "$Default")

BLOB_CONN = os.getenv("BLOB_CONN", "")                  # Storage connection string (raw archive)
BLOB_CONTAINER = os.getenv("BLOB_CONTAINER", "raw-logs")

RAG_WORKER_URL = (os.getenv("RAG_WORKER_URL", "") or "").rstrip("/")
RAG_INGEST_URL = f"{RAG_WORKER_URL}/v1/ingest" if RAG_WORKER_URL else ""

POST_TIMEOUT = float(os.getenv("POST_TIMEOUT", "8"))    # HTTP POST timeout to rag-worker
BATCH_MAX = int(os.getenv("BATCH_MAX", "128"))          # docs per flush
BATCH_WINDOW = float(os.getenv("BATCH_WINDOW", "1.0"))  # seconds between flushes

# Limit to "app logs" categories. You can change by env: ALLOW_CATEGORIES="ContainerAppConsoleLogs,ContainerAppSystemLogs"
ALLOW_CATEGORIES = set(
    (os.getenv("ALLOW_CATEGORIES") or "ContainerAppConsoleLogs,ContainerAppSystemLogs").split(",")
)

# -----------------------
# Logging (JSON-ish)
# -----------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='{"asctime":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
)
log = logging.getLogger("ingestor")

# --- SDK log tuning (NEW) ---
SDK_LOG_LEVEL = (os.getenv("SDK_LOG_LEVEL") or "WARNING").upper()
for name in (
    "uamqp",
    "azure",                     # umbrella
    "azure.eventhub",
    "azure.storage.blob",
    "azure.core.pipeline.policies.http_logging_policy",
):
    lg = logging.getLogger(name)
    lg.setLevel(getattr(logging, SDK_LOG_LEVEL, logging.WARNING))
    lg.propagate = False

# -----------------------
# Helpers
# -----------------------
def utc_iso(ts: Optional[str] = None) -> str:
    """Return ISO8601 UTC string. If ts is already ISO, pass through."""
    if ts:
        try:
            datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return ts
        except Exception:
            pass
    return datetime.now(timezone.utc).isoformat()

def sha1_id(*parts: str) -> str:
    """Stable ID for dedupe/debug."""
    h = hashlib.sha1()
    for p in parts:
        h.update(p.encode("utf-8", errors="ignore"))
    return h.hexdigest()

def is_metric_payload(obj: Dict[str, Any]) -> bool:
    """Azure Monitor metrics to EH look like {"records":[{..., 'metricName': 'IngressUsageBytes', ...}]}."""
    if "records" in obj and isinstance(obj["records"], list):
        rec0 = obj["records"][0] if obj["records"] else {}
        return isinstance(rec0, dict) and "metricName" in rec0
    return "metricName" in obj  # rare top-level case

def is_allowed_log(obj: Dict[str, Any]) -> bool:
    """Only forward Console/System app logs; drop others unless they look like free-form 'message' records."""
    cat = (obj.get("category") or obj.get("Category") or "").strip()
    return (cat in ALLOW_CATEGORIES) or (not cat and ("message" in obj or "msg" in obj))

def normalize_one(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Map Event Hub log shape → our canonical doc (id, source, ts, content)."""
    if is_metric_payload(payload):
        return None  # <-- drop metrics
    if not is_allowed_log(payload):
        return None  # <-- drop other noise

    source = str(payload.get("category") or payload.get("source") or "unknown").strip()

    # Try common fields for the log content
    content = payload.get("message") or payload.get("msg") or payload.get("content")
    if not content:
        props = payload.get("properties") or payload.get("log")
        if isinstance(props, dict):
            content = json.dumps(props)
        elif isinstance(props, str):
            content = props

    if not content:
        return None

    ts = payload.get("timeGenerated") or payload.get("timestamp") or payload.get("ts") or utc_iso()
    ts_iso = utc_iso(str(ts))

    return {
        "id": sha1_id(source, ts_iso, str(content)),
        "source": source[:128],
        "ts": ts_iso,
        "content": str(content)[:5000],
    }

async def archive_raw(blob_svc: BlobServiceClient, data: bytes, partition_id: str):
    """Write raw JSONL to blob for later replay/debug."""
    now = datetime.now(timezone.utc)
    path = f"eh/{partition_id}/{now:%Y/%m/%d/%H/%M}/{int(now.timestamp()*1_000_0000)}.jsonl"
    try:
        cont = blob_svc.get_container_client(BLOB_CONTAINER)
        await cont.upload_blob(name=path, data=data, overwrite=False)
        log.info(json.dumps({"msg":"raw archived","partition":partition_id,"bytes":len(data),"blob":path}))
    except Exception as e:
        log.warning(json.dumps({"msg":"raw archive failed","err":str(e),"partition":partition_id}))

async def post_docs(session: httpx.AsyncClient, docs: List[Dict[str, Any]]) -> bool:
    """POST normalized docs to rag-worker /v1/ingest."""
    if not RAG_INGEST_URL:
        log.error(json.dumps({"msg":"rag-worker URL not configured"}))
        return False
    try:
        resp = await session.post(RAG_INGEST_URL, json={"documents": docs}, timeout=POST_TIMEOUT)
        #ok = 200 <= resp.status_code < 300
        if 200 <= resp.status_code < 300:
            log.info(json.dumps({
                "msg":"rag-worker ingest result",
                "status": resp.status_code,
                "count": len(docs)#,
                #"body": (await resp.aread())[:256].decode("utf-8","ignore") if not ok else ""
            }))
            return True
        else:
            body = (await resp.aread())[:256].decode("utf-8","ignore")
            log.info(json.dumps({"msg":"rag-worker ingest result","status": resp.status_code,"count": len(docs),"body": body}))    
            return False
    except Exception as e:
        log.error(json.dumps({"msg":"rag-worker ingest failed","err":str(e),"count":len(docs)}))
        return False

# --- add module globals (NEW) ---
EVENTS_TOTAL = 0
NORMALIZED_TOTAL = 0
FORWARDED_TOTAL = 0
SKIPPED_METRICS_TOTAL = 0
SUMMARY_INTERVAL = int(os.getenv("SUMMARY_INTERVAL", "60"))  # seconds

async def _summary_loop():
    global EVENTS_TOTAL, NORMALIZED_TOTAL, FORWARDED_TOTAL, SKIPPED_METRICS_TOTAL
    while True:
        await asyncio.sleep(SUMMARY_INTERVAL)
        log.info(json.dumps({
            "msg":"ingestor summary",
            "events": EVENTS_TOTAL,
            "normalized": NORMALIZED_TOTAL,
            "forwarded": FORWARDED_TOTAL,
            "skipped_metrics": SKIPPED_METRICS_TOTAL
        }))

# -----------------------
# Event Hub consumer task
# -----------------------
async def run_consumer():
    log.info(json.dumps({
        "msg":"ingestor starting",
        "hub":EVENTHUB_NAME,"cg":EVENTHUB_CONSUMER,
        "rag_ingest":RAG_INGEST_URL,
        "allow_categories":sorted(list(ALLOW_CATEGORIES))
    }))

    client = EventHubConsumerClient.from_connection_string(
        conn_str=EVENTHUB_CONN,
        consumer_group=EVENTHUB_CONSUMER,
        eventhub_name=EVENTHUB_NAME,
        logging_enable=False,               # NEW: no HTTP policy spam
    )

    blob_svc = BlobServiceClient.from_connection_string(
        BLOB_CONN, logging_enable=False     # NEW
    ) if BLOB_CONN else None
    http = httpx.AsyncClient(headers={"content-type":"application/json"})

    batch: List[Dict[str, Any]] = []
    batch_deadline = asyncio.get_event_loop().time() + BATCH_WINDOW

    async def on_event(partition_context, event: EventData):
        global EVENTS_TOTAL, NORMALIZED_TOTAL, SKIPPED_METRICS_TOTAL
        nonlocal batch, batch_deadline
        
        #body = event.body_as_str(encoding="utf-8", errors="ignore")
        # --- BEGIN robust body decode (replace your body_as_str(...) line) ---
        # event.body can be bytes or an iterable of bytes depending on azure-eventhub version
        body = getattr(event, "body", None)

        if isinstance(body, (bytes, bytearray)):
            raw = bytes(body)
        else:
            # body is a generator/iterable of bytes (most common)
            raw = b"".join(part for part in body)
        
        # decode in a version-safe way; ignore broken utf-8 sequences
        body = raw.decode("utf-8", errors="ignore")
        # --- END robust body decode ---
        
        size = len(body.encode("utf-8", "ignore"))
        part = partition_context.partition_id

        # Try to parse JSON or JSON-lines
        items: List[Dict[str, Any]] = []
        try:
            if "\n" in body:
                for line in body.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        items.append(json.loads(line))
                    except Exception:
                        items.append({"message": line})
            else:
                obj = json.loads(body)
                # Diagnostics often send {"records":[...]}
                if isinstance(obj, dict) and "records" in obj and isinstance(obj["records"], list):
                    items.extend([r for r in obj["records"] if isinstance(r, dict)])
                else:
                    items.append(obj if isinstance(obj, dict) else {"message": str(obj)})
        except Exception as e:
            log.warning(json.dumps({"msg":"json parse failed","err":str(e),"partition":part,"sample":body[:200]}))
            items = [{"message": body}]

        # Normalize and filter
        norm: List[Dict[str, Any]] = []
        skipped_metrics = 0
        for it in items:
            if is_metric_payload(it):
                skipped_metrics += 1
                continue
            doc = normalize_one(it)
            if doc:
                norm.append(doc)


        # after building 'items' and 'norm'
        EVENTS_TOTAL += len(items)
        NORMALIZED_TOTAL += len(norm)
        SKIPPED_METRICS_TOTAL += skipped_metrics
        
        '''log.info(json.dumps({
            "msg":"event received",
            "partition":part,
            "size_bytes":size,
            "items":len(items),
            "normalized":len(norm),
            "skipped_metrics":skipped_metrics
        }))'''

        # Was: log.info(...) every event
        # Now: only DEBUG per-event (so it’s muted at default WARNING/INFO)
        log.debug(json.dumps({
            "msg":"event received",
            "partition": part,
            "items": len(items),
            "normalized": len(norm),
            "skipped_metrics": skipped_metrics
        }))

        # Archive raw for audit/replay
        if blob_svc:
            try:
                await archive_raw(blob_svc, (body+"\n").encode("utf-8"), part)
            except Exception as e:
                log.warning(json.dumps({"msg":"archive error","err":str(e),"partition":part}))

        if norm:
            batch.extend(norm)

        # Time/size-based flush
        now = asyncio.get_event_loop().time()
        if len(batch) >= BATCH_MAX or now >= batch_deadline:
            if batch:
                docs = batch
                batch = []
                batch_deadline = now + BATCH_WINDOW
                ok = await post_docs(http, docs)
                if ok:
                    global FORWARDED_TOTAL
                    FORWARDED_TOTAL += len(docs)

        await partition_context.update_checkpoint(event)

    async with client:
        try:
            log.info(json.dumps({"msg":"consumer starting receive","start":"@latest"}))
            # *** CHANGED: start from newest events only ***
            await client.receive(on_event=on_event, starting_position="@latest")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            log.error(json.dumps({"msg":"consumer error","err":str(e)}))

    await http.aclose()
    log.info(json.dumps({"msg":"ingestor stopped"}))

# -----------------------
# FastAPI app
# -----------------------
app = FastAPI()

@app.on_event("startup")
async def _startup():
    # Sanity log to confirm which resources are used
    log.info(json.dumps({
        "msg":"startup",
        "hub":EVENTHUB_NAME,
        "cg":EVENTHUB_CONSUMER,
        "rag_ingest":RAG_INGEST_URL,
        "blob_container":BLOB_CONTAINER if BLOB_CONN else None
    }))
    asyncio.create_task(run_consumer())  # background consumer
    asyncio.create_task(_summary_loop())   # NEW

@app.get("/health", response_class=PlainTextResponse)
async def health():
    return "ok"

@app.get("/")
async def root():
    log.info(json.dumps({"msg":"ingestor root hit"}))
    return {"ok": True}

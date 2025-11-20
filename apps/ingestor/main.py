# apps/ingestor/main.py
# Purpose: Consume Event Hub logs, archive raw to Blob, forward only WARN/ERROR/CRITICAL to rag-worker.
# Notes:
# - Keeps your env var names as-is.
# - Starts from @latest.
# - Uses modules in apps/ingestor/modules/*

import os, json, asyncio, logging, hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import FastAPI
from starlette.responses import PlainTextResponse
import re

# Safe imports from our helper modules (absolute first, then relative)
try:
    from modules.archive import get_blob_client, archive_raw
    from modules.normalize import (
        is_metric_payload, normalize_payload, extract_severity,
        LEVEL_NUM, LEVEL_NAME
    )
    from modules.eh_consumer import get_consumer, decode_event_items
    from modules.forwarder import get_http_client, post_docs
    from modules.incidents import summarize_from_lines
except ImportError:
    from .modules.archive import get_blob_client, archive_raw
    from .modules.normalize import (
        is_metric_payload, normalize_payload, extract_severity,
        LEVEL_NUM, LEVEL_NAME
    )
    from .modules.eh_consumer import get_consumer, decode_event_items
    from .modules.forwarder import get_http_client, post_docs
    from .modules.incidents import summarize_from_lines

# -----------------------
# Environment (UNCHANGED NAMES)
# -----------------------
EVENTHUB_CONN      = os.getenv("EVENTHUB_CONN", "")
EVENTHUB_NAME      = os.getenv("EVENTHUB_NAME", "")
EVENTHUB_CONSUMER  = os.getenv("EVENTHUB_CONSUMER", "$Default")

BLOB_CONN          = os.getenv("BLOB_CONN", "")
BLOB_CONTAINER     = os.getenv("BLOB_CONTAINER", "raw-logs")
RAW_PREFIX         = (os.getenv("RAW_PREFIX") or "eh/").strip("/")

RAG_WORKER_URL     = (os.getenv("RAG_WORKER_URL", "") or "").rstrip("/")
RAG_INGEST_URL     = f"{RAG_WORKER_URL}/v1/ingest" if RAG_WORKER_URL else ""
RAG_WORKER_TOKEN   = os.getenv("RAG_WORKER_TOKEN", "")

POST_TIMEOUT       = float(os.getenv("POST_TIMEOUT", "8"))
BATCH_MAX          = int(os.getenv("BATCH_MAX", "128"))
BATCH_WINDOW       = float(os.getenv("BATCH_WINDOW", "1.0"))

# Only forward logs with level >= FORWARD_MIN_LEVEL
# Allowed: DEBUG, INFO, WARN, WARNING, ERROR, CRITICAL, FATAL
FORWARD_MIN_LEVEL  = (os.getenv("FORWARD_MIN_LEVEL") or "WARNING").upper()
FORWARD_MIN_NUM    = LEVEL_NUM.get(FORWARD_MIN_LEVEL, 30)  # default WARNING→30

# Optional category filter (kept for compatibility, but severity gate is the main control)
ALLOW_CATEGORIES = set(
    (os.getenv("ALLOW_CATEGORIES") or "ContainerAppConsoleLogs,ContainerAppSystemLogs").split(",")
)

INCIDENT_SUMMARY = (os.getenv("INCIDENT_SUMMARY","1") == "1")  # enable one-incident-per-event
MIN_LEVEL = (os.getenv("MIN_LEVEL") or "WARNING").upper()      # already used in your tool; optional here
# -----------------------
# Logging (JSON-ish)
# -----------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='{"asctime":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
)
log = logging.getLogger("ingestor")

# Quiet noisy SDK logs unless overridden
SDK_LOG_LEVEL = (os.getenv("SDK_LOG_LEVEL") or "WARNING").upper()
for name in ("uamqp", "azure", "azure.eventhub", "azure.storage.blob",
             "azure.core.pipeline.policies.http_logging_policy"):
    lg = logging.getLogger(name)
    lg.setLevel(getattr(logging, SDK_LOG_LEVEL, logging.WARNING))
    lg.propagate = False

# -----------------------
# Helpers
# -----------------------

# --- add near other helpers ---

_SEV_ORDER = {"DEBUG":0, "INFO":1, "WARNING":2, "ERROR":3, "CRITICAL":4}
FORWARD_MIN_LEVEL = (os.getenv("FORWARD_MIN_LEVEL") or "INFO").upper()

def _level_ok(level: str) -> bool:
    return _SEV_ORDER.get(level.upper(), 1) >= _SEV_ORDER.get(FORWARD_MIN_LEVEL, 1)

_HTTP_ERR_RE = re.compile(r'\b(5\d{2})\b.*\b(Internal Server Error|Gateway|Timeout|Error)\b', re.I)
_HTTP_WARN_RE = re.compile(r'\b(4\d{2})\b', re.I)

def classify_severity(msg: str) -> str:
    t = msg.strip()
    if _HTTP_ERR_RE.search(t):
        return "ERROR"
    if _HTTP_WARN_RE.search(t):
        return "WARNING"
    # common app prefixes
    if t.startswith(("ERROR", "Exception", "Traceback")):
        return "ERROR"
    if t.startswith(("WARN", "WARNING")):
        return "WARNING"
    return "INFO"

def utc_iso(ts: Optional[str] = None) -> str:
    if ts:
        try:
            datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            return str(ts)
        except Exception:
            pass
    return datetime.now(timezone.utc).isoformat()

def sha1_id(*parts: str) -> str:
    h = hashlib.sha1()
    for p in parts:
        h.update(p.encode("utf-8", errors="ignore"))
    return h.hexdigest()

# -----------------------
# Periodic summary counters
# -----------------------
EVENTS_TOTAL = 0
NORMALIZED_TOTAL = 0
FORWARDED_TOTAL = 0
SKIPPED_METRICS_TOTAL = 0
DROPPED_BY_LEVEL_TOTAL = 0
SUMMARY_INTERVAL = int(os.getenv("SUMMARY_INTERVAL", "60"))  # seconds

async def _summary_loop():
    while True:
        await asyncio.sleep(SUMMARY_INTERVAL)
        log.info(json.dumps({
            "msg":"ingestor summary",
            "events": EVENTS_TOTAL,
            "normalized": NORMALIZED_TOTAL,
            "forwarded": FORWARDED_TOTAL,
            "skipped_metrics": SKIPPED_METRICS_TOTAL,
            "dropped_by_level": DROPPED_BY_LEVEL_TOTAL,
            "forward_min_level": FORWARD_MIN_LEVEL
        }))

# -----------------------
# Core consumer task
# -----------------------
async def run_consumer():
    log.info(json.dumps({
        "msg":"ingestor starting",
        "hub":EVENTHUB_NAME, "cg":EVENTHUB_CONSUMER,
        "rag_ingest":RAG_INGEST_URL,
        "allow_categories":sorted(list(ALLOW_CATEGORIES)),
        "forward_min_level": FORWARD_MIN_LEVEL
    }))

    eh_client = get_consumer(
        conn_str=EVENTHUB_CONN,
        hub=EVENTHUB_NAME,
        group=EVENTHUB_CONSUMER,
        logging_enable=False,
    )

    blob_svc = get_blob_client(BLOB_CONN) if BLOB_CONN else None
    http: httpx.AsyncClient = get_http_client(RAG_WORKER_TOKEN)

    batch: List[Dict[str, Any]] = []
    batch_deadline = asyncio.get_event_loop().time() + BATCH_WINDOW

    async def on_event(partition_context, event) -> None:
        nonlocal batch, batch_deadline
        global EVENTS_TOTAL, NORMALIZED_TOTAL, SKIPPED_METRICS_TOTAL, FORWARDED_TOTAL, DROPPED_BY_LEVEL_TOTAL
    
        # Decode EH payload -> (list[dict], raw_text)
        items, raw_text = decode_event_items(event)
        part = getattr(partition_context, "partition_id", "0") or "0"
        EVENTS_TOTAL += len(items)
    
        # ---------- INCIDENT SUMMARY FAST PATH ----------
        # Build console "lines" from the raw records and summarize to ONE incident.
        # If enabled, we post that single doc and skip per-line forwarding for this event.
        if INCIDENT_SUMMARY:
            # turn records into lines (prefer properties.Log / message fields)
            lines: List[str] = []
            for it in items:
                msg = it.get("message") or it.get("msg")
                if not msg:
                    props = it.get("properties") or it.get("log")
                    if isinstance(props, dict):
                        if isinstance(props.get("Log"), str):
                            msg = props["Log"]
                        else:
                            # prefer some common keys before falling back to compact JSON
                            msg = (
                                props.get("Details")
                                or props.get("detail")
                                or props.get("error")
                                or props.get("ExceptionMessage")
                                or props.get("Message")
                                or props.get("msg")
                            )
                            if not msg:
                                try:
                                    msg = json.dumps(props, ensure_ascii=False)
                                except Exception:
                                    msg = str(props)
                    elif isinstance(props, str):
                        msg = props
                if msg:
                    if isinstance(msg, str):
                        lines.extend(msg.splitlines())
                    else:
                        lines.append(str(msg))
    
            # Always archive raw for replay/debug even if no incident is produced
            if blob_svc and raw_text:
                try:
                    await archive_raw(
                        blob_svc=blob_svc,
                        container=BLOB_CONTAINER,
                        prefix=RAW_PREFIX,
                        partition_id=part,
                        lines=[raw_text],
                    )
                except Exception as e:
                    log.warning(json.dumps({"msg": "archive error", "err": str(e), "partition": part}))
    
            # Summarize → ONE incident doc
            inc = summarize_from_lines(lines, min_level=MIN_LEVEL)
            if inc and RAG_INGEST_URL:
                ok = await post_docs(http=http, url=RAG_INGEST_URL, docs=[inc], timeout=POST_TIMEOUT)
                if ok:
                    FORWARDED_TOTAL += 1
    
            # We handled this event (summarized or not). Checkpoint and return.
            await partition_context.update_checkpoint(event)
            return
        # ---------- END INCIDENT SUMMARY FAST PATH ----------
    
        # -------- Existing per-record normalization path (unchanged) --------
        norm: List[Dict[str, Any]] = []
        dropped_by_level = 0
        skipped_metrics = 0
    
        for it in items:
            if is_metric_payload(it):
                skipped_metrics += 1
                continue
            doc = normalize_payload(it)
            if not doc:
                continue
            if doc.pop("_dropped_by_level", False):
                dropped_by_level += 1
                continue
            norm.append(doc)
    
        NORMALIZED_TOTAL += len(norm)
        SKIPPED_METRICS_TOTAL += skipped_metrics
        DROPPED_BY_LEVEL_TOTAL += dropped_by_level
    
        # Archive raw JSONL for replay/debug
        if blob_svc and raw_text:
            try:
                await archive_raw(
                    blob_svc=blob_svc,
                    container=BLOB_CONTAINER,
                    prefix=RAW_PREFIX,
                    partition_id=part,
                    lines=[raw_text],
                )
            except Exception as e:
                log.warning(json.dumps({"msg": "archive error", "err": str(e), "partition": part}))
    
        # Batch forwarding
        if norm:
            batch.extend(norm)
    
        now = asyncio.get_event_loop().time()
        if len(batch) >= BATCH_MAX or now >= batch_deadline:
            if batch and RAG_INGEST_URL:
                docs = batch
                batch = []
                batch_deadline = now + BATCH_WINDOW
                ok = await post_docs(http=http, url=RAG_INGEST_URL, docs=docs, timeout=POST_TIMEOUT)
                if ok:
                    FORWARDED_TOTAL += len(docs)
    
        await partition_context.update_checkpoint(event)

    '''async def on_event(partition_context, event) -> None:
        nonlocal batch, batch_deadline
        global EVENTS_TOTAL, NORMALIZED_TOTAL, SKIPPED_METRICS_TOTAL, FORWARDED_TOTAL, DROPPED_BY_LEVEL_TOTAL

        items, raw_text = decode_event_items(event)
        part = getattr(partition_context, "partition_id", "0") or "0"
        EVENTS_TOTAL += len(items)
        
        # Normalize and filter
        norm: List[Dict[str, Any]] = []
        dropped_by_level = 0
        skipped_metrics = 0
        
        for it in items:
            if is_metric_payload(it):
                skipped_metrics += 1
                continue
            #doc = normalize_one(it)
            doc = normalize_payload(it)
            if not doc:
                continue
            if doc.pop("_dropped_by_level", False):
                dropped_by_level += 1
                continue
            norm.append(doc)
        
        EVENTS_TOTAL += len(items)
        NORMALIZED_TOTAL += len(norm)
        SKIPPED_METRICS_TOTAL += skipped_metrics
        DROPPED_BY_LEVEL_TOTAL += dropped_by_level

        # Archive raw JSONL for replay/debug
        if blob_svc and raw_text:
            try:
                await archive_raw(
                    blob_svc=blob_svc,
                    container=BLOB_CONTAINER,
                    prefix=RAW_PREFIX,
                    partition_id=part,
                    lines=[raw_text],
                )
            except Exception as e:
                log.warning(json.dumps({"msg":"archive error","err":str(e),"partition":part}))

        # Batch forwarding
        if norm:
            batch.extend(norm)

        now = asyncio.get_event_loop().time()
        if len(batch) >= BATCH_MAX or now >= batch_deadline:
            if batch and RAG_INGEST_URL:
                docs = batch
                batch = []
                batch_deadline = now + BATCH_WINDOW
                ok = await post_docs(http=http, url=RAG_INGEST_URL, docs=docs, timeout=POST_TIMEOUT)
                if ok:
                    FORWARDED_TOTAL += len(docs)

        await partition_context.update_checkpoint(event)'''
    

    async with eh_client:
        try:
            log.info(json.dumps({"msg":"consumer starting receive","start":"@latest"}))
            await eh_client.receive(on_event=on_event, starting_position="@latest")
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
    log.info(json.dumps({
        "msg":"startup",
        "hub":EVENTHUB_NAME,
        "cg":EVENTHUB_CONSUMER,
        "rag_ingest":RAG_INGEST_URL,
        "blob_container":BLOB_CONTAINER if BLOB_CONN else None,
        "forward_min_level": FORWARD_MIN_LEVEL
    }))
    asyncio.create_task(run_consumer())
    asyncio.create_task(_summary_loop())

@app.get("/health", response_class=PlainTextResponse)
async def health():
    return "ok"

@app.get("/")
async def root():
    log.info(json.dumps({"msg":"ingestor root hit"}))
    return {"ok": True}

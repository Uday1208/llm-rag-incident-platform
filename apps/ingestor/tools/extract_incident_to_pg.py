# apps/ingestor/tools/extract_incident_to_pg.py
"""
Purpose:
  Read one or more raw JSONL blobs produced by Event Hubs diagnostics, extract compact
  “incident episodes” (headline + exception + last /app/... frame), and insert/upsert
  into Postgres 'documents' table. Designed to be safe to run repeatedly.

Env vars:
  BLOB_CONN         : Azure Blob Storage connection string
  BLOB_CONTAINER    : Container name for raw archives (e.g., "raw-logs")
  PG_DSN            : (preferred) PostgreSQL DSN, e.g. "host=... dbname=... user=... password=... port=..."
    – OR –
  PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASS

  MIN_LEVEL         : Minimum severity to persist (DEBUG|INFO|WARNING|ERROR|CRITICAL). Default: WARNING
  KEEP_INTERNAL_FRAMES: "false" to drop site-packages/usr/local frames. Default: "false"
  MAX_STACK_LINES   : Max app-stack lines to keep. Default: 12

Usage examples (in container shell):
  python -m apps.ingestor.tools.extract_incident_to_pg --blob "eh/2/2025/11/07/09/12/1762573368610904.jsonl"
  python -m apps.ingestor.tools.extract_incident_to_pg --prefix "eh/2/2025/11/07" --limit 5 --dry-run
  python -m apps.ingestor.tools.extract_incident_to_pg --list-file /tmp/blobs.txt

Notes:
  * We dedupe inside each blob by a signature built from {exception, generalized message, last /app frame, source}
  * We keep raw blobs as audit—this only writes condensed rows to Postgres.
"""

import os
import re
import sys
import json
import argparse
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, Iterable, List, Tuple, Optional, DefaultDict
from collections import defaultdict

# Sync SDKs to keep it simple for a CLI tool
from azure.storage.blob import BlobServiceClient
import psycopg2
import psycopg2.extras

# -------------------------
# Env + constants
# -------------------------
BLOB_CONN      = os.getenv("BLOB_CONN", "")
BLOB_CONTAINER = os.getenv("BLOB_CONTAINER", "raw-logs")

MIN_LEVEL = (os.getenv("MIN_LEVEL") or "WARNING").upper().strip()
KEEP_INTERNAL = (os.getenv("KEEP_INTERNAL_FRAMES") or "false").lower().startswith("t")
MAX_STACK_LINES = int(os.getenv("MAX_STACK_LINES", "12"))

# --------------- severity helpers ---------------
LEVEL_ORDER = {"DEBUG":10, "INFO":20, "WARNING":30, "ERROR":40, "CRITICAL":50}
def coerce_severity(val: Any) -> str:
    """Map incoming level/number/string to standard severity."""
    if val is None:
        return "INFO"
    if isinstance(val, (int, float)):
        n = int(val)
        if   n >= 50: return "CRITICAL"
        elif n >= 40: return "ERROR"
        elif n >= 30: return "WARNING"
        elif n >= 20: return "INFO"
        else:         return "DEBUG"
    s = str(val).upper()
    if s in LEVEL_ORDER:
        return s
    # crude keyword fallbacks
    if "CRITICAL" in s: return "CRITICAL"
    if "ERROR"    in s: return "ERROR"
    if "WARN"     in s: return "WARNING"
    if "INFO"     in s: return "INFO"
    return "DEBUG"

def meets_min_level(sev: str) -> bool:
    return LEVEL_ORDER.get(sev, 10) >= LEVEL_ORDER.get(MIN_LEVEL, 30)

# --------------- normalization helpers ---------------
UUID_RE = re.compile(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b")
IP_RE   = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b")
ISO_RE  = re.compile(r"\b\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?Z\b")
NUM_RE  = re.compile(r"\b\d+\b")
APP_FRAME_RE = re.compile(r'File "(/app/[^"]+)", line (\d+), in ([\w<>]+)')

TB_MARK = "Traceback (most recent call last):"
EXC_TAIL_RE = re.compile(r"^\s*([\w\.]+(?:Error|Exception|Failure|Exit))\s*:(.*)$")

def is_tb_start(line: str) -> bool:
    return TB_MARK in (line or "")

def is_tb_frame_or_cont(line: str) -> bool:
    """Heuristic: part of traceback block (indented lines, File \"...\", or 'During handling...' etc.)"""
    s = (line or "")
    ls = s.lstrip()
    return (
        ls.startswith('File "') or
        ls.startswith("File '") or
        ls.startswith("During handling of the above exception") or
        s.startswith("  ") or
        s.startswith("\t")
    )

def is_exception_tail(line: str) -> bool:
    """Looks like 'ValueError: message' etc."""
    return bool(EXC_TAIL_RE.match(line or ""))

def parse_exception_tail(line: str) -> Tuple[Optional[str], Optional[str]]:
    m = EXC_TAIL_RE.match(line or "")
    if not m:
        return None, None
    return m.group(1).strip(), m.group(2).strip()


def generalize_message(s: str) -> str:
    """Replace dynamic tokens to stabilize dedup signature."""
    s = UUID_RE.sub("#UUID#", s)
    s = IP_RE.sub("#IP#", s)
    s = ISO_RE.sub("#TS#", s)
    s = NUM_RE.sub("#", s)
    return s

def utc_iso(ts: Optional[str]) -> str:
    if not ts:
        return datetime.now(timezone.utc).isoformat()
    try:
        # handle "...Z"
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
    except Exception:
        return datetime.now(timezone.utc).isoformat()

# --------------- payload extraction ---------------
def read_content_and_meta(obj: Dict[str, Any]) -> Tuple[str, str, str]:
    """
    Heuristically extract (content, source, ts) from Azure Monitor/Event Hub shapes.
    - content: prefer 'message'/'msg' or properties.Log/Log
    - source : category/resource/app composite
    - ts     : timeGenerated/timestamp/ts
    """
    content = (
        obj.get("message")
        or obj.get("msg")
        or (obj.get("properties") or {}).get("Log")
        or (obj.get("properties") or {}).get("Logs")
        or (obj.get("Properties") or {}).get("Log")
        or (obj.get("log"))
        or ""
    )
    # collapse dict content if needed
    if isinstance(content, dict):
        try:
            content = json.dumps(content, ensure_ascii=False)
        except Exception:
            content = str(content)

    cat = (obj.get("category") or obj.get("Category") or "").strip()
    src = obj.get("source") or cat or obj.get("resourceId") or "unknown"
    if cat and obj.get("app"):
        src = f"{obj.get('app')}/{cat}"

    ts = obj.get("timeGenerated") or obj.get("timestamp") or obj.get("ts")
    return str(content), str(src), utc_iso(str(ts) if ts else None)

def find_traceback_bits(text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (headline, traceback_text) if a traceback is present, else (None, None).
    Heuristic: 'Traceback (most recent call last):' marker, headline is the line
    just before it (or up to 2 lines above if preceding is blank).
    """
    if not text:
        return None, None
    lines = text.splitlines()
    tb_idx = None
    marker = "Traceback (most recent call last):"
    for i, line in enumerate(lines):
        if marker in line:
            tb_idx = i
            break
    if tb_idx is None:
        return None, None

    # headline: prefer line right before traceback (skip empty)
    hl_idx = tb_idx - 1
    while hl_idx >= 0 and not lines[hl_idx].strip():
        hl_idx -= 1
    headline = lines[hl_idx].strip() if hl_idx >= 0 else ""

    traceback_text = "\n".join(lines[tb_idx:])
    return (headline or None), traceback_text

def extract_app_frames(traceback_text: str) -> List[str]:
    """Keep only /app/... frames (+ code lines), optionally drop internal frames."""
    if not traceback_text:
        return []
    out: List[str] = []
    for line in traceback_text.splitlines():
        if '/app/' in line:
            out.append(line)
        elif KEEP_INTERNAL:
            if ('site-packages' in line) or ('/usr/local/' in line):
                out.append(line)
    # keep only last MAX_STACK_LINES
    if len(out) > MAX_STACK_LINES:
        out = out[-MAX_STACK_LINES:]
    return out

def extract_exception_summary(traceback_text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    From the last non-empty traceback line ('ExceptionType: message'), return (ExceptionType, message).
    """
    if not traceback_text:
        return None, None
    tail = [ln.strip() for ln in traceback_text.splitlines() if ln.strip()]
    if not tail:
        return None, None
    last = tail[-1]
    # Common "Type: message"
    if ":" in last:
        etype, msg = last.split(":", 1)
        return etype.strip(), msg.strip()
    return last.strip(), None

def build_signature(source: str, exception: Optional[str], headline: Optional[str], app_frame: Optional[str]) -> str:
    sig_parts = [
        exception or "NOEXC",
        generalize_message(headline or ""),
        generalize_message(app_frame or ""),
        source or "unknown",
    ]
    h = hashlib.sha1()
    h.update("||".join(sig_parts).encode("utf-8", errors="ignore"))
    return h.hexdigest()

# --------------- Postgres ---------------
def pg_connect():
    dsn = os.getenv("PG_DSN")
    if not dsn:
        host = os.getenv("PG_HOST", "localhost")
        port = os.getenv("PG_PORT", "5432")
        db   = os.getenv("PG_DB", "")
        user = os.getenv("PG_USER", "")
        pw   = os.getenv("PG_PASS", "")
        dsn  = f"host={host} port={port} dbname={db} user={user} password={pw}"
    return psycopg2.connect(dsn)

UPSERT_SQL = """
INSERT INTO documents (id, source, ts, content, severity)
VALUES (%(id)s, %(source)s, %(ts)s, %(content)s, %(severity)s)
ON CONFLICT (id) DO UPDATE
  SET ts = EXCLUDED.ts,
      content = EXCLUDED.content,
      severity = COALESCE(EXCLUDED.severity, documents.severity),
      source = EXCLUDED.source;
"""

# --------------- blob reading ---------------
def list_blobs_by_prefix(bsc: BlobServiceClient, container: str, prefix: str) -> List[str]:
    cc = bsc.get_container_client(container)
    return [b.name for b in cc.list_blobs(name_starts_with=prefix)]

def download_blob_text(bsc: BlobServiceClient, container: str, name: str) -> str:
    bc = bsc.get_blob_client(container=container, blob=name)
    return bc.download_blob().readall().decode("utf-8", errors="ignore")

def parse_jsonl_or_records(raw: str) -> Iterable[Dict[str, Any]]:
    # Accept raw JSONL or a single JSON with {"records":[...]}
    if "\n" in raw:
        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict) and "records" in obj and isinstance(obj["records"], list):
                    for r in obj["records"]:
                        if isinstance(r, dict):
                            yield r
                elif isinstance(obj, dict):
                    yield obj
                else:
                    yield {"message": str(obj)}
            except Exception:
                yield {"message": line}
    else:
        try:
            obj = json.loads(raw)
        except Exception:
            yield {"message": raw}
            return
        if isinstance(obj, dict) and "records" in obj and isinstance(obj["records"], list):
            for r in obj["records"]:
                if isinstance(r, dict):
                    yield r
        elif isinstance(obj, dict):
            yield obj
        else:
            yield {"message": str(obj)}

# --------------- condenser ---------------
def process_blob_text(raw: str, blob_name: str) -> List[Dict[str, Any]]:
    """
    Build compact 'incident episode' docs by stitching traceback lines that
    are split across multiple records in the blob.
    """
    # Episode state
    episodes: Dict[str, Dict[str, Any]] = {}
    current = None  # type: Optional[Dict[str, Any]]
    prev_nonempty_message = ""  # candidate headline
    last_source_for_prev = "unknown"
    last_ts_for_prev = None

    def flush_current():
        nonlocal current, episodes
        if not current:
            return
        # Build summary content
        lines = current["lines"]  # type: List[str]
        tb_text = "\n".join(lines)
        app_frames = extract_app_frames(tb_text)
        last_app_frame = app_frames[-1] if app_frames else None

        # Exception summary
        exc_type, exc_msg = extract_exception_summary(tb_text)
        if not exc_type:
            # try last line style once more
            for ln in reversed([ln for ln in lines if ln.strip()]):
                if is_exception_tail(ln):
                    exc_type, exc_msg = parse_exception_tail(ln)
                    break

        # Headline: prefer captured, else previous nonempty message
        headline = (current.get("headline") or "").strip()
        if not headline:
            headline = (current.get("fallback_headline") or "").strip()

        # Signature + doc
        signature = build_signature(current.get("source","unknown"), exc_type, headline, last_app_frame)
        if signature not in episodes:
            summary = []
            if headline:              summary.append(f"Headline: {headline}")
            if exc_type:              summary.append(f"Exception: {exc_type}" + (f": {exc_msg}" if exc_msg else ""))
            if last_app_frame:        summary.append(f"AppFrame: {last_app_frame}")
            if app_frames:            summary.append("Stack:\n" + "\n".join(app_frames[-MAX_STACK_LINES:]))
            summary.append(f"Count: {current['count']}  Blobs: {blob_name}")

            episodes[signature] = {
                "id": hashlib.sha1(signature.encode("utf-8")).hexdigest(),
                "source": current.get("source","unknown")[:128],
                "ts": current.get("ts_last") or current.get("ts_first") or utc_iso(None),
                "content": "\n".join(summary)[:5000],
                "severity": current.get("severity") or None,
            }
        else:
            # bump last-seen ts if we see another episode with same signature
            if current.get("ts_last"):
                episodes[signature]["ts"] = current["ts_last"]

        current = None

    # Walk records in order
    for obj in parse_jsonl_or_records(raw):
        content, source, ts = read_content_and_meta(obj)
        sev = coerce_severity(obj.get("level") or obj.get("Level") or obj.get("severity"))
        ts = utc_iso(ts)

        text = (content or "").rstrip("\n")
        if not text.strip():
            continue

        # If not within an episode, see if this starts one
        if current is None:
            # maintain previous non-empty line as a possible headline
            if is_tb_start(text):
                current = {
                    "lines": [text],
                    "headline": prev_nonempty_message,   # may be empty
                    "fallback_headline": prev_nonempty_message,
                    "source": source,
                    "ts_first": ts,
                    "ts_last": ts,
                    "severity": sev,
                    "count": 1,
                    "seen_exc_tail": is_exception_tail(text),
                }
            else:
                prev_nonempty_message = text
                last_source_for_prev = source
                last_ts_for_prev = ts
            continue

        # We are in an episode
        current["ts_last"] = ts
        if LEVEL_ORDER.get(sev,10) > LEVEL_ORDER.get(current.get("severity","INFO"),10):
            current["severity"] = sev

        if is_tb_start(text):
            # A new traceback starts; flush the current, then start new
            flush_current()
            current = {
                "lines": [text],
                "headline": prev_nonempty_message,
                "fallback_headline": prev_nonempty_message,
                "source": source,
                "ts_first": ts,
                "ts_last": ts,
                "severity": sev,
                "count": 1,
                "seen_exc_tail": is_exception_tail(text),
            }
            continue

        # Still same episode; append traceback parts
        current["lines"].append(text)
        current["count"] += 1
        if is_exception_tail(text):
            current["seen_exc_tail"] = True
        else:
            # Heuristic: if line is NOT a traceback continuation and we already saw an exception tail,
            # the episode likely ended.
            if (not is_tb_frame_or_cont(text)) and current.get("seen_exc_tail"):
                flush_current()
                # this line is a normal log; keep as next headline candidate
                prev_nonempty_message = text
                last_source_for_prev = source
                last_ts_for_prev = ts

    # End of blob: flush if open
    flush_current()

    # Convert episodes to docs
    docs: List[Dict[str, Any]] = []
    for ep in episodes.values():
        # apply severity gate (optional)
        if not meets_min_level(ep.get("severity") or "INFO"):
            continue
        docs.append({
            "id": ep["id"],
            "source": ep["source"],
            "ts": ep["ts"],
            "content": ep["content"],
            "severity": ep.get("severity")
        })
    return docs


# --------------- main ---------------
def main():
    ap = argparse.ArgumentParser(description="Extract incidents from raw-logs blobs and write to Postgres")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--blob", action="append", help="Exact blob path (repeatable)")
    g.add_argument("--prefix", help="List all blobs under this prefix")
    g.add_argument("--list-file", help="File with one blob path per line")
    ap.add_argument("--limit", type=int, default=0, help="Optional limit on number of blobs to process")
    ap.add_argument("--dry-run", action="store_true", help="Print what would be written, do not write")
    args = ap.parse_args()

    if not BLOB_CONN or not BLOB_CONTAINER:
        print("BLOB_CONN and BLOB_CONTAINER must be set", file=sys.stderr)
        sys.exit(2)

    bsc = BlobServiceClient.from_connection_string(BLOB_CONN)
    if args.prefix:
        blobs = list_blobs_by_prefix(bsc, BLOB_CONTAINER, args.prefix)
    elif args.list_file:
        with open(args.list_file, "r", encoding="utf-8") as fh:
            blobs = [ln.strip() for ln in fh if ln.strip()]
    else:
        blobs = args.blob or []

    if args.limit and len(blobs) > args.limit:
        blobs = blobs[:args.limit]

    print(f"[extract] container={BLOB_CONTAINER} blobs={len(blobs)} min_level={MIN_LEVEL} keep_internal={KEEP_INTERNAL}")

    conn = None
    cur  = None
    if not args.dry_run:
        conn = pg_connect()
        cur = conn.cursor()

    total_docs = 0
    for i, name in enumerate(blobs, 1):
        try:
            raw = download_blob_text(bsc, BLOB_CONTAINER, name)
        except Exception as e:
            print(f"[{i}/{len(blobs)}] download failed: {name} err={e}", file=sys.stderr)
            continue

        docs = process_blob_text(raw, name)
        if args.dry_run:
            print(f"[{i}/{len(blobs)}] {name} -> {len(docs)} incidents")
            for d in docs:
                print(json.dumps({k: d[k] for k in ("id","source","ts","severity")}, ensure_ascii=False))
        else:
            for d in docs:
                cur.execute(UPSERT_SQL, d)
            conn.commit()
            print(f"[{i}/{len(blobs)}] {name} -> upserted {len(docs)} incident rows")
        total_docs += len(docs)

    if cur:  cur.close()
    if conn: conn.close()
    print(f"[done] total incident rows: {total_docs}")

if __name__ == "__main__":
    main()

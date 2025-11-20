# tools/blob_scan.py
# Purpose: Scan archived Event Hub blobs (JSON/JSONL) for error-like records,
#          summarize metrics, and optionally print samples for investigation.
#
# Env Vars (used if CLI args not provided):
#   - BLOB_CONN        : Azure Blob Storage connection string
#   - BLOB_CONTAINER   : Container name (e.g., raw-logs)
#   - SCAN_PREFIX      : Optional path prefix (e.g., "eh/")
#
# Usage examples (inside the ingestor container):
#   python tools/blob_scan.py --prefix eh/ --since 2025-11-04T00:00:00Z --print-samples 5
#   python tools/blob_scan.py --prefix eh/ --limit 200 --out metrics.json --exit-on-error
#
# Notes:
# - Designed to be conservative: only flags entries that look like ERROR/EXCEPTION/TIMEOUT
#   or have severity fields like "Error", "Critical", etc.
# - Works on both top-level {"records":[...]} and JSONL blobs, and plain JSON blobs.

import os
import re
import sys
import json
import argparse
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from azure.storage.blob import BlobServiceClient

# --- NEW: imports (top of file) ---
import json
from pathlib import Path

# -------------- Patterns & severities --------------

# Common message substrings/patterns that indicate failure
_ERR_PATTERNS = [
    r"\bERROR\b",
    r"\bError\b",
    r"\bException\b",
    r"Traceback",
    r"\bfailed\b",
    r"\bfailure\b",
    r"\btimeout\b",
    r"\bunavailable\b",
    r"\b5\d{2}\b",                  # HTTP 5xx
]
_ERR_RE = re.compile("|".join(_ERR_PATTERNS))

# Map common severity labels to a normalized level
_SEV_MAP = {
    "CRITICAL": "CRITICAL",
    "FATAL": "CRITICAL",
    "ERROR": "ERROR",
    "ERR": "ERROR",
    "WARN": "WARNING",
    "WARNING": "WARNING",
    "INFO": "INFO",
    "DEBUG": "DEBUG",
    "TRACE": "TRACE",
}

# -------------- Helpers --------------

def _iso_to_dt(s: str) -> Optional[datetime]:
    """Parse ISO8601 string to datetime."""
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def _coerce_str(x: Any) -> str:
    """Coerce any value to a safe display string."""
    if isinstance(x, (bytes, bytearray)):
        try:
            return x.decode("utf-8", "ignore")
        except Exception:
            return str(x)
    return str(x)

def _get_severity(obj: Dict[str, Any]) -> Optional[str]:
    """Extract normalized severity from a record dict."""
    cand = (
        obj.get("level")
        or obj.get("Level")
        or obj.get("severity")
        or obj.get("severityLevel")
        or obj.get("Severity")
        or obj.get("logLevel")
    )
    if not cand:
        return None
    s = _coerce_str(cand).strip().upper()
    return _SEV_MAP.get(s, s)

def _get_message(obj: Dict[str, Any]) -> str:
    """Extract a human message string from a record dict."""
    # common fields we saw in ACA -> EH dumps
    for k in ("message", "msg", "content", "body", "exceptionMessage"):
        v = obj.get(k)
        if isinstance(v, (str, bytes, bytearray)):
            return _coerce_str(v)
        if isinstance(v, dict):
            # nested structure -> keep compact
            try:
                return json.dumps(v, ensure_ascii=False)[:2000]
            except Exception:
                return str(v)
    # fall back to a compact JSON line
    try:
        return json.dumps(obj, ensure_ascii=False)[:2000]
    except Exception:
        return str(obj)[:2000]

def _looks_like_error(obj: Dict[str, Any]) -> Tuple[bool, str]:
    """Heuristically decide if record is error-like; return (is_error_like, reason)."""
    sev = (_get_severity(obj) or "").upper()
    if sev in ("CRITICAL", "ERROR"):
        return True, f"severity={sev}"
    msg = _get_message(obj)
    if _ERR_RE.search(msg):
        return True, "pattern_match"
    # HTTP status-based cue
    code = obj.get("statusCode") or obj.get("status") or obj.get("httpStatus")
    try:
        if code and int(code) >= 500:
            return True, f"http_{code}"
    except Exception:
        pass
    return False, ""

def _iter_records_from_blob_bytes(data: bytes) -> Iterable[Dict[str, Any]]:
    """Yield dict records from a blob payload that may be JSONL or JSON."""
    text = data.decode("utf-8", "ignore")
    # JSONL fast path
    if "\n" in text:
        for ln in text.splitlines():
            ln = ln.strip()
            if not ln:
                continue
            try:
                obj = json.loads(ln)
                if isinstance(obj, dict):
                    yield obj
                elif isinstance(obj, list):
                    for it in obj:
                        if isinstance(it, dict):
                            yield it
            except Exception:
                # treat as free-form message
                yield {"message": ln}
        return

    # Single JSON object/array
    try:
        obj = json.loads(text)
        if isinstance(obj, dict) and isinstance(obj.get("records"), list):
            for it in obj["records"]:
                if isinstance(it, dict):
                    yield it
        elif isinstance(obj, dict):
            yield obj
        elif isinstance(obj, list):
            for it in obj:
                if isinstance(it, dict):
                    yield it
    except Exception:
        # Raw text fallback
        yield {"message": text}

# -------------- Scanner --------------

def scan_container(
    conn_str: str,
    container: str,
    prefix: str,
    since: Optional[datetime],
    limit: Optional[int],
    print_samples: int,
) -> Dict[str, Any]:
    """Scan blobs and return metrics dict."""
    svc = BlobServiceClient.from_connection_string(conn_str)
    cont = svc.get_container_client(container)

    metrics = {
        "scanned_blobs": 0,
        "scanned_records": 0,
        "error_like": 0,
        "warning_like": 0,
        "by_severity": {},      # e.g., {"ERROR": 12, "WARNING": 7, ...}
        "by_category": {},      # e.g., {"ContainerAppConsoleLogs": 10, ...}
        "samples": [],          # small sample of error-like lines
        "prefix": prefix,
        "since": since.isoformat() if since else None,
    }

    def bump(d: Dict[str, int], k: str) -> None:
        """Increment a counter in a dict."""
        if not k:
            return
        d[k] = d.get(k, 0) + 1

    scanned = 0
    error_like_blobs = set()
    
    for blob in cont.list_blobs(name_starts_with=prefix):
        if since and blob.last_modified and blob.last_modified.replace(tzinfo=timezone.utc) < since:
            continue

        bname = blob.name
        try:
            data = cont.download_blob(bname).readall()
        except Exception as e:
            print(f"[WARN] download failed: {bname}: {e}", file=sys.stderr)
            continue

        metrics["scanned_blobs"] += 1

        for rec in _iter_records_from_blob_bytes(data):
            metrics["scanned_records"] += 1

            # Category/Source bump (best-effort)
            cat = (rec.get("category") or rec.get("Category") or rec.get("source") or "").strip()
            bump(metrics["by_category"], cat)

            sev = (_get_severity(rec) or "").upper()
            if sev:
                bump(metrics["by_severity"], sev)

            is_err, why = _looks_like_error(rec)
            if is_err:
                error_like_blobs.add(blob.name)
                metrics["error_like"] += 1
                if print_samples and len(metrics["samples"]) < print_samples:
                    metrics["samples"].append({
                        "blob": bname,
                        "why": why,
                        "severity": sev or None,
                        "message": _get_message(rec),
                    })
            elif sev == "WARNING":
                metrics["warning_like"] += 1

            if limit and metrics["scanned_records"] >= limit:
                return metrics, error_like_blobs

        scanned += 1

    return metrics, error_like_blobs

# -------------- CLI --------------

def _parse_args() -> argparse.Namespace:
    """Parse CLI args."""
    p = argparse.ArgumentParser(description="Scan Blob logs for error-like records.")
    p.add_argument("--conn", default=os.getenv("BLOB_CONN", ""), help="Blob connection string (default: $BLOB_CONN)")
    p.add_argument("--container", default=os.getenv("BLOB_CONTAINER", "raw-logs"), help="Blob container (default: $BLOB_CONTAINER)")
    p.add_argument("--prefix", default=os.getenv("SCAN_PREFIX", "eh/"), help="Path prefix (default: $SCAN_PREFIX or 'eh/')")
    p.add_argument("--since", default=None, help="Only blobs modified since this ISO time (e.g., 2025-11-04T00:00:00Z)")
    p.add_argument("--limit", type=int, default=None, help="Stop after scanning N records")
    p.add_argument("--print-samples", type=int, default=5, help="Print up to N example error-like records")
    p.add_argument("--out", default=None, help="Write metrics JSON to this file")
    p.add_argument("--exit-on-error", action="store_true", help="Exit code 2 if any error-like records found")
    p.add_argument("--list-error-blobs", action="store_true", help="Print blob paths that contain at least one error-like record.")
    return p.parse_args()

def main() -> None:
    """Entry point for CLI."""
    args = _parse_args()
    if not args.conn or not args.container:
        print("ERROR: Missing --conn or --container (or env BLOB_CONN / BLOB_CONTAINER).", file=sys.stderr)
        sys.exit(1)

    since_dt = _iso_to_dt(args.since) if args.since else None
    metrics, error_like_blobs = scan_container(
        conn_str=args.conn,
        container=args.container,
        prefix=args.prefix,
        since=since_dt,
        limit=args.limit,
        print_samples=args.print_samples,
    )

    # Pretty print summary
    print(json.dumps(metrics, indent=2, ensure_ascii=False))

    if args.out:
        try:
            with open(args.out, "w", encoding="utf-8") as f:
                json.dump(metrics, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"[WARN] could not write {args.out}: {e}", file=sys.stderr)

    # One path per line; easy to pipe into other tools/scripts
    for p in sorted(error_like_blobs):
        print(p)

    if args.exit_on_error and metrics.get("error_like", 0) > 0:
        sys.exit(2)

if __name__ == "__main__":
    main()

# apps/ingestor/modules/normalize.py
# Normalize Azure Monitor → canonical {id?, source, ts, content, severity?}

from typing import Any, Dict, Optional, Tuple
import json
import os

# Canonical numeric levels
LEVEL_NUM = {
    "DEBUG": 10,
    "INFO": 20,
    "WARN": 30, "WARNING": 30,
    "ERROR": 40, "ERR": 40,
    "CRITICAL": 50, "FATAL": 50,
}
LEVEL_NAME = {v: k for k, v in LEVEL_NUM.items()}

SEVERITY_KEYS = (
    "severity", "Severity", "level", "Level", "logLevel",
    "severityLevel", "levelname", "LEVEL"
)

ALLOW_CATEGORIES = set(
    (os.getenv("ALLOW_CATEGORIES") or "ContainerAppConsoleLogs,ContainerAppSystemLogs")
    .split(",")
)

def _coerce_level(val: Any) -> Tuple[str, int]:
    """Return (LEVEL_NAME, LEVEL_NO). Accept strings or ints."""
    if val is None:
        return ("INFO", 20)
    # Numeric-ish?
    try:
        n = int(val)
        # map common numeric to nearest
        if n >= 50: return ("CRITICAL", 50)
        if n >= 40: return ("ERROR", 40)
        if n >= 30: return ("WARNING", 30)
        if n >= 20: return ("INFO", 20)
        return ("DEBUG", 10)
    except Exception:
        pass
    s = str(val).strip().upper()
    if s in LEVEL_NUM:
        return ( "WARNING" if s == "WARN" else ("ERROR" if s == "ERR" else s),
                 LEVEL_NUM[s] )
    # fuzzy
    if s.startswith("WARN"): return ("WARNING", 30)
    if s.startswith("ERR"):  return ("ERROR", 40)
    if s.startswith("CRIT") or s == "FATAL": return ("CRITICAL", 50)
    if s.startswith("DBG"):  return ("DEBUG", 10)
    return ("INFO", 20)

def extract_severity(obj: Dict[str, Any], fallback: Any = None) -> Tuple[str, int]:
    for k in SEVERITY_KEYS:
        if k in obj:
            return _coerce_level(obj.get(k))
    return _coerce_level(fallback)

def is_metric_payload(obj: Dict[str, Any]) -> bool:
    """True for Azure Monitor metrics envelopes (metricName present)."""
    if "records" in obj and isinstance(obj["records"], list):
        rec0 = obj["records"][0] if obj["records"] else {}
        return isinstance(rec0, dict) and "metricName" in rec0
    return "metricName" in obj

def _first(d: Dict[str, Any], keys: tuple) -> Optional[Any]:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None

def is_allowed_log(obj: Dict[str, Any]) -> bool:
    cat = (obj.get("category") or obj.get("Category") or "").strip()
    return (cat in ALLOW_CATEGORIES) or (not cat and ("message" in obj or "msg" in obj))
    
'''def normalize_payload(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Map various Azure/console shapes to {id?, source, ts, content, severity?}.
    - source: category / source / app resource path
    - ts: timeGenerated / timestamp / ts
    - content: message/msg/content or JSON of 'properties'
    """
    if not isinstance(payload, dict):
        return None

    source = _first(payload, ("category","Category","source","resourceId","ResourceId")) or "unknown"
    ts = _first(payload, ("timeGenerated","timestamp","ts","TimeGenerated"))
    content = _first(payload, ("message","msg","content","body","Body"))

    if content is None:
        props = payload.get("properties") or payload.get("log")
        if isinstance(props, dict):
            content = json.dumps(props, ensure_ascii=False)
        elif isinstance(props, str):
            content = props

    if content is None:
        return None

    level_name, _ = extract_severity(payload)

    return {
        # 'id' optional (main will compute stable id)
        "source": str(source),
        "ts": str(ts) if ts else None,
        "content": str(content),
        "severity": level_name,
    }'''

def normalize_payload(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # Drop metrics & non-allowed categories
    if is_metric_payload(payload):
        return None
    if not is_allowed_log(payload):
        return None

    # Pull “message” from common places; prefer Azure Container Apps shape
    props = payload.get("properties") if isinstance(payload.get("properties"), dict) else {}
    log_line = props.get("Log")
    content = (
        payload.get("message")
        or payload.get("msg")
        or payload.get("content")
        or (json.dumps(payload.get("log")) if isinstance(payload.get("log"), dict) else None)
        or log_line
    )
    if not content:
        return None

    # Identify app/infra attributes for provenance
    app = props.get("ContainerAppName") or payload.get("ContainerAppName") or ""
    container = props.get("ContainerName") or payload.get("ContainerName") or ""
    revision = props.get("RevisionName") or payload.get("RevisionName") or ""
    source = (payload.get("category") or payload.get("Category") or "unknown").strip()
    if app or container:
        source = f"{app}/{container}".strip("/")

    # Timestamp
    ts = (payload.get("timeGenerated")
          or payload.get("timestamp")
          or payload.get("time")
          or payload.get("ts"))
    ts_iso = utc_iso(str(ts) if ts else None)

    # Classify severity from the final message string
    sev = classify_severity(str(content))

    # Honor forward_min_level (drop noise before DB)
    if not _level_ok(sev):
        # counted by caller via dropped_by_level
        return {"_dropped_by_level": True}

    # Build canonical doc; keep content concise but useful
    doc = {
        "id": sha1_id(source, ts_iso, str(content)),
        "source": source[:128] or "unknown",
        "ts": ts_iso,
        "content": str(content)[:5000],
        "severity": sev,                 # <— NEW
        "meta": {                        # <— optional tags for later query
            "app": app or None,
            "container": container or None,
            "revision": revision or None,
            "category": payload.get("category") or payload.get("Category"),
            "resourceId": payload.get("resourceId"),
        },
    }
    return doc

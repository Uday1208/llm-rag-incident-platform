# modules/incidents.py
# Collapse Azure Monitor console logs → one actionable incident per blob.
# - Prefers traceback-based incidents (prelude + exception chain + /app/ frames)
# - If no traceback, falls back to concise HTTP 4xx/5xx summary
# - Shares regex/heuristics with extract_incident_to_pg_v3.py so behavior matches

from __future__ import annotations
import re, json
from typing import List, Tuple, Optional, Dict, Any

# -------------------------
# Regex & constants (mirror v3)
# -------------------------
TB_START_RE   = re.compile(r"Traceback \(most recent call last\):")
EXC_LINE_RE   = re.compile(r"^[A-Za-z_][\w.]*?(Error|Exception|Timeout|Failure|Unavailable)(?:: .*)?$")
APP_FRAME_RE  = re.compile(r'^\s*File\s+"(/app/[^"]+)",\s+line\s+(\d+),\s+in\s+([A-Za-z_]\w*)')
APP_NAME_RE   = re.compile(r'"ContainerAppName"\s*:\s*"([^"]+)"')

ERROR_BANNER_RE = re.compile(
    r"(EXCEPTION IN ASGI APPLICATION|CRITICAL:|ERROR:|Traceback \(most recent call last\):)",
    re.IGNORECASE,
)

HTTP_LINE_RX_1 = re.compile(
    r'"\s*(GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)\s+(/[^"\s]*)\s+HTTP/\d\.\d"\s+(\d{3})\b',
    re.I,
)
HTTP_LINE_RX_2 = re.compile(
    r'HTTP Request:\s*(GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)\s+(\S+)\s+"HTTP/\d\.\d\s+(\d{3})\b',
    re.I,
)
HTTP_STATUS_IN_JSON_RX = re.compile(r'"status"\s*:\s*(\d{3})\b')
IGNORED_HTTP_PATHS = {"/", "/health", "/metrics", "/livez", "/readyz"}

INTERNAL_HINTS = ("/site-packages/", "/usr/local/lib/python")
AZ_META_KEYS   = ("ContainerAppName", "EnvironmentName", "RevisionName", "ContainerImage")

LEVEL_RANK = {
    "DEBUG": 10, "VERBOSE": 10,
    "INFO": 20, "INFORMATION": 20,
    "WARNING": 30, "WARN": 30,
    "ERROR": 40,
    "CRITICAL": 50, "FATAL": 50,
}
NUM_LEVEL_MAP = {0: "DEBUG", 1: "INFO", 2: "WARNING", 3: "ERROR", 4: "CRITICAL"}


# -------------------------
# Public API
# -------------------------
def summarize_blob(blob_text: str, *, min_level: str = "WARNING") -> List[dict]:
    """
    Accepts the raw text of a blob (JSON / JSONL / {records:[...]})
    and returns a list with 0 or 1 incident dicts.
    """
    records = _iter_blob_records(blob_text)
    incidents, _stats = summarize_records(records, min_level=min_level)
    return incidents


def summarize_records(records: List[dict], *, min_level: str = "WARNING") -> Tuple[List[dict], Dict[str, Any]]:
    """
    Accepts parsed EventHub/Azure Monitor records.
    Returns (incidents, stats). Incidents list size is 0 or 1 by design.
    """
    stats: Dict[str, Any] = {
        "records": len(records),
        "max_level": None,
        "kept": 0,
        "dropped_below_min": False,
        "used_fallback_http": False,
    }

    # Gate on max severity in this blob
    max_lvl_str, max_lvl_rank = _max_level_rank(records)
    stats["max_level"] = max_lvl_str
    min_level = (min_level or "WARNING").strip().upper()
    if LEVEL_RANK.get(max_lvl_str, 20) < LEVEL_RANK.get(min_level, 30):
        stats["dropped_below_min"] = True
        return [], stats

    # Flatten console text; drop Azure metadata JSON lines early
    lines = _join_console_lines(records)
    lines = [ln for ln in lines if not _is_meta_line(ln)]

    # Primary: traceback-based composition
    content = _compose_content(lines)
    severity = "ERROR" if content else None

    # Fallback: single HTTP 4xx/5xx line (concise)
    if not content:
        fb = _http_fallback_content(lines)
        if fb:
            content, severity = fb
            stats["used_fallback_http"] = True

    if not content:
        return [], stats

    incident = {
        "source": _guess_source(lines),
        "severity": severity or "ERROR",
        "content": content,
        "ts": None,  # we don’t have a reliable per-incident ts at this stage
    }
    stats["kept"] = 1
    return [incident], stats


# -------------------------
# Helpers (mirror v3 behavior)
# -------------------------
def _iter_blob_records(blob_text: str) -> List[dict]:
    out: List[dict] = []
    t = (blob_text or "").strip()
    if not t:
        return out

    # Try JSONL first
    if "\n" in t and t.lstrip().startswith("{"):
        for ln in t.splitlines():
            ln = ln.strip()
            if not ln:
                continue
            try:
                o = json.loads(ln)
                if isinstance(o, dict) and "records" in o and isinstance(o["records"], list):
                    out.extend([r for r in o["records"] if isinstance(r, dict)])
                elif isinstance(o, dict):
                    out.append(o)
            except Exception:
                pass
        if out:
            return out

    # Then single JSON
    try:
        o = json.loads(t)
        if isinstance(o, dict) and "records" in o and isinstance(o["records"], list):
            return [r for r in o["records"] if isinstance(r, dict)]
        if isinstance(o, list):
            return [r for r in o if isinstance(r, dict)]
        if isinstance(o, dict):
            return [o]
    except Exception:
        pass
    return out


def _record_text(rec: dict) -> str:
    if isinstance(rec.get("message"), (str, bytes)):
        v = rec["message"]
        return v if isinstance(v, str) else v.decode("utf-8", "ignore")
    if isinstance(rec.get("msg"), (str, bytes)):
        v = rec["msg"]
        return v if isinstance(v, str) else v.decode("utf-8", "ignore")
    props = rec.get("properties")
    if isinstance(props, dict):
        v = props.get("Log")
        if isinstance(v, (str, bytes)):
            return v if isinstance(v, str) else v.decode("utf-8", "ignore")
        try:
            return json.dumps(props, ensure_ascii=False)
        except Exception:
            return ""
    return ""


def _join_console_lines(records: List[dict]) -> List[str]:
    lines: List[str] = []
    for r in records:
        msg = _record_text(r)
        if not msg:
            continue
        for ln in msg.splitlines():
            ln = ln.rstrip("\r")
            if ln.strip():
                lines.append(ln)
    return lines


def _is_meta_line(s: str) -> bool:
    s = s.strip()
    if not s.startswith("{") or "}" not in s:
        return False
    return any(k in s for k in AZ_META_KEYS)


def _guess_source(lines: List[str]) -> str:
    for ln in lines:
        m = APP_NAME_RE.search(ln)
        if m:
            return m.group(1)
    return "ContainerAppConsoleLogs"


def _prelude_two_before_tb(lines: List[str], tb_idx: int) -> List[str]:
    out = []
    if tb_idx - 2 >= 0:
        out.append(lines[tb_idx - 2])
    if tb_idx - 1 >= 0:
        out.append(lines[tb_idx - 1])

    keep: List[str] = []
    for ln in out:
        if "Exception in ASGI application" in ln:
            keep.append(ln.strip())
        # include HTTP 5xx banners if present in prelude
        if re.search(r'"HTTP/1\.\d"\s*5\d{2}\b', ln):
            keep.append(ln.strip())
    return keep


def _exception_chain(lines: List[str], start_idx: int) -> List[str]:
    chain = []
    for ln in lines[start_idx:]:
        m = EXC_LINE_RE.match(ln.strip())
        if m:
            cls = ln.split(":", 1)[0].strip()
            if not chain or chain[-1] != cls:
                chain.append(cls)
    return chain[:8]


def _app_frames(lines: List[str]) -> List[str]:
    frames = []
    for i, ln in enumerate(lines):
        m = APP_FRAME_RE.match(ln)
        if not m:
            continue
        file_path, line_no, func = m.groups()
        code = ""
        if i + 1 < len(lines):
            nxt = lines[i + 1]
            if nxt.startswith("    ") or nxt.lstrip().startswith(("return", "raise", "await", "with", "for", "if")):
                code = nxt.strip()
        if any(h in ln for h in INTERNAL_HINTS):
            continue
        frames.append(f'{file_path}:{line_no} in {func}' + (f' → {code}' if code else ""))
    return frames[-3:] if frames else []


def _compose_content(lines: List[str]) -> Optional[str]:
    """
    Build incident text:
      1) If traceback exists: prelude + exception chain + /app/ frames
      2) Else return None (HTTP 4xx/5xx fallback handled by caller)
    """
    tb_idx = next((i for i, ln in enumerate(lines) if TB_START_RE.search(ln)), None)
    if tb_idx is None:
        return None

    prelude = _prelude_two_before_tb(lines, tb_idx)
    chain = _exception_chain(lines, tb_idx)
    chain_txt = " → ".join(chain) if chain else ""
    frames = _app_frames(lines)

    out: List[str] = []
    out.extend(prelude)
    if chain_txt:
        out.append(f"Errors: {chain_txt}")
    if frames:
        out.append("At:")
        out.extend(frames)

    if not out:
        kept = []
        for ln in lines[tb_idx:]:
            if _is_meta_line(ln):
                continue
            if any(h in ln for h in INTERNAL_HINTS):
                continue
            kept.append(ln)
            if len(kept) >= 15:
                break
        out = ["(raw snippet)"] + kept

    return "\n".join(out).strip()


def _scan_http_errors_in_text(s: str) -> list[dict]:
    found = []
    # server access log style
    for m in HTTP_LINE_RX_1.finditer(s):
        method, path, code = m.group(1), m.group(2), int(m.group(3))
        if code >= 400 and not (code in (200, 204) and path in IGNORED_HTTP_PATHS):
            found.append({"code": code, "kind": "api", "method": method, "uri": path, "line": m.group(0)})
    # application "HTTP Request:" style
    for m in HTTP_LINE_RX_2.finditer(s):
        method, url, code = m.group(1), m.group(2), int(m.group(3))
        if code >= 400:
            found.append({"code": code, "kind": "client", "method": method, "uri": url, "line": m.group(0)})
    # JSON status fragments
    for m in HTTP_STATUS_IN_JSON_RX.finditer(s):
        code = int(m.group(1))
        if code >= 400:
            start = max(0, m.start() - 160)
            end = min(len(s), m.end() + 160)
            found.append({"code": code, "kind": "json", "method": "n/a", "uri": "n/a", "line": s[start:end]})
    return found


def _pick_worst_http(errs: list[dict]) -> dict | None:
    if not errs:
        return None
    # Prefer 5xx first, then highest code
    return sorted(errs, key=lambda e: (-(500 <= e["code"] <= 599), -e["code"]))[0]


def _http_code_to_severity(code: int) -> str:
    return "CRITICAL" if code >= 500 else "ERROR"


def _http_fallback_content(lines: List[str]) -> Optional[tuple[str, str]]:
    raw_text = "\n".join(lines)
    worst = _pick_worst_http(_scan_http_errors_in_text(raw_text))
    if not worst:
        return None
    headline = f"HTTP {worst['code']} {worst.get('method','?')} {worst.get('uri','?')}"
    out = [f"Headline: {headline}", "Snippet:", worst["line"].strip()]
    frames = _app_frames(lines)
    if frames:
        out.append("At:")
        out.extend(frames)
    return ("\n".join(out).strip(), _http_code_to_severity(worst["code"]))


def _record_level(rec: dict) -> str:
    val = rec.get("level") or rec.get("Level") or rec.get("severity") or rec.get("severityLevel")
    if isinstance(val, (int, float)):
        return NUM_LEVEL_MAP.get(int(val), "INFO")
    if isinstance(val, str):
        s = val.strip().upper()
        if s in LEVEL_RANK:
            return s

    txt = _record_text(rec)

    # treat HTTP codes as level hints (404->WARNING, 5xx->ERROR)
    m = re.search(r'\b(\d{3})\b', txt)
    if m:
        try:
            code = int(m.group(1))
            if code >= 500: return "ERROR"
            if code >= 400: return "WARNING"
        except Exception:
            pass

    up = txt.upper()
    for key in ("CRITICAL", "ERROR", "WARNING"):
        if key in up:
            return key
    return "INFO"


def _max_level_rank(records: List[dict]) -> tuple[str, int]:
    best = "DEBUG"
    best_rank = LEVEL_RANK[best]
    for r in records:
        lvl = _record_level(r)
        rk = LEVEL_RANK.get(lvl, 20)
        if rk > best_rank:
            best, best_rank = lvl, rk
            if best_rank >= LEVEL_RANK["CRITICAL"]:
                break
    return best, best_rank

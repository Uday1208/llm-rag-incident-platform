# apps/ingestor/modules/incidents.py
# Purpose: Build ONE concise incident from console lines:
#  - Prefer traceback summary: (banner two-lines prelude) + exception chain + /app/ frames
#  - If no traceback, fall back to a single HTTP 4xx/5xx summary
#  - Returns dict compatible with rag-worker /v1/ingest (id, source, ts, content, severity)

import re, hashlib
from typing import List, Optional

# --- regex + ranking (mirrors your working script) ---
TB_START_RE   = re.compile(r"Traceback \(most recent call last\):")
EXC_LINE_RE   = re.compile(r"^[A-Za-z_][\w.]*?(Error|Exception|Timeout|Failure|Unavailable)(?:: .*)?$")
APP_FRAME_RE  = re.compile(r'^\s*File\s+"(/app/[^"]+)",\s+line\s+(\d+),\s+in\s+([A-Za-z_]\w*)')
ERROR_BANNER_RE = re.compile(r"(EXCEPTION IN ASGI APPLICATION|CRITICAL:|ERROR:|Traceback \(most recent call last\):)", re.I)
INTERNAL_HINTS = ("/site-packages/", "/usr/local/lib/python")
APP_NAME_RE  = re.compile(r'"ContainerAppName"\s*:\s*"([^"]+)"')

HTTP_LINE_RX_1 = re.compile(r'"\s*(GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)\s+(/[^"\s]*)\s+HTTP/\d\.\d"\s+(\d{3})\b', re.I)
HTTP_LINE_RX_2 = re.compile(r'HTTP Request:\s*(GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)\s+(\S+)\s+"HTTP/\d\.\d\s+(\d{3})\b', re.I)
HTTP_STATUS_IN_JSON_RX = re.compile(r'"status"\s*:\s*(\d{3})\b')
IGNORED_HTTP_PATHS = {"/", "/health", "/metrics", "/livez", "/readyz"}

LEVEL_RANK = {"DEBUG":10,"VERBOSE":10,"INFO":20,"INFORMATION":20,"WARNING":30,"WARN":30,"ERROR":40,"CRITICAL":50,"FATAL":50}

def _guess_source(lines: List[str]) -> str:
    for ln in lines:
        m = APP_NAME_RE.search(ln)
        if m:
            return m.group(1)
    return "ContainerAppConsoleLogs"

def _app_frames(lines: List[str]) -> List[str]:
    frames = []
    for i, ln in enumerate(lines):
        m = APP_FRAME_RE.match(ln)
        if not m: continue
        file_path, line_no, func = m.groups()
        code = ""
        if i + 1 < len(lines):
            nxt = lines[i+1]
            if nxt.startswith("    ") or nxt.lstrip().startswith(("return","raise","await","with","for","if")):
                code = nxt.strip()
        if any(h in ln for h in INTERNAL_HINTS):
            continue
        frames.append(f'{file_path}:{line_no} in {func}' + (f' → {code}' if code else ""))
    return frames[-3:] if frames else []

def _prelude_two_before_tb(lines: List[str], tb_idx: int) -> List[str]:
    out = []
    if tb_idx - 2 >= 0: out.append(lines[tb_idx-2])
    if tb_idx - 1 >= 0: out.append(lines[tb_idx-1])
    keep = []
    for ln in out:
        if "Exception in ASGI application" in ln or "ERROR:" in ln or "CRITICAL" in ln:
            keep.append(ln.strip())
    return keep

def _exception_chain(lines: List[str], start_idx: int) -> List[str]:
    chain = []
    for ln in lines[start_idx:]:
        if EXC_LINE_RE.match(ln.strip()):
            cls = ln.split(":",1)[0].strip()
            if not chain or chain[-1] != cls:
                chain.append(cls)
    return chain[:8]

def _compose_content_from_tb(lines: List[str]) -> Optional[str]:
    tb_idx = next((i for i, ln in enumerate(lines) if TB_START_RE.search(ln)), None)
    if tb_idx is None:
        return None
    prelude = _prelude_two_before_tb(lines, tb_idx)
    chain   = _exception_chain(lines, tb_idx)
    frames  = _app_frames(lines)
    out = []
    out += prelude
    if chain:  out.append("Errors: " + " → ".join(chain))
    if frames: out += ["At:", *frames]
    if not out:
        # rare: fallback snippet limited to app/visible lines
        kept = []
        for ln in lines[tb_idx:]:
            if any(h in ln for h in INTERNAL_HINTS): continue
            if ERROR_BANNER_RE.search(ln) or ln.strip().startswith("File "): kept.append(ln)
            if len(kept) >= 15: break
        out = ["(raw snippet)", *kept]
    return "\n".join(out).strip()

def _scan_http_errors_in_text(s: str) -> list[dict]:
    found = []
    for m in HTTP_LINE_RX_1.finditer(s):
        method, path, code = m.group(1), m.group(2), int(m.group(3))
        if code >= 400 and not (code in (200,204) and path in IGNORED_HTTP_PATHS):
            found.append({"code": code, "kind":"api", "method": method, "uri": path, "line": m.group(0)})
    for m in HTTP_LINE_RX_2.finditer(s):
        method, url, code = m.group(1), m.group(2), int(m.group(3))
        if code >= 400:
            found.append({"code": code, "kind":"client", "method": method, "uri": url, "line": m.group(0)})
    for m in HTTP_STATUS_IN_JSON_RX.finditer(s):
        code = int(m.group(1))
        if code >= 400:
            start = max(0, m.start() - 160)
            end   = min(len(s), m.end() + 160)
            found.append({"code": code, "kind":"json", "method":"n/a", "uri":"n/a", "line": s[start:end]})
    return found

def _pick_worst_http(errs: list[dict]) -> dict | None:
    if not errs: return None
    return sorted(errs, key=lambda e: (-(500 <= e["code"] <= 599), -e["code"]))[0]

def summarize_from_lines(lines: List[str], min_level: str = "WARNING") -> Optional[dict]:
    """Return ONE incident doc {'id','source','ts','content','severity'} or None."""
    # Drop noisy azure meta json lines early
    clean = [ln for ln in lines if not (ln.strip().startswith("{") and "ContainerAppName" in ln)]
    if not clean:
        return None

    # 1) Try traceback composition
    content = _compose_content_from_tb(clean)
    severity = "ERROR" if content else None

    # 2) HTTP fallback (only if no TB)
    if not content:
        raw_text = "\n".join(clean)
        worst = _pick_worst_http(_scan_http_errors_in_text(raw_text))
        if worst:
            severity = "CRITICAL" if worst["code"] >= 500 else "ERROR"
            headline = f"HTTP {worst['code']} {worst.get('method','?')} {worst.get('uri','?')}"
            frames = _app_frames(clean)
            parts = [headline, "Snippet:", worst["line"].strip()]
            if frames: parts += ["At:", *frames]
            content = "\n".join(parts).strip()

    if not content:
        return None

    source = _guess_source(clean)
    basis  = f"{source}|{content}".encode("utf-8","ignore")
    doc_id = hashlib.sha1(basis).hexdigest()  # stable id; avoids dup inserts in rag-worker

    return {
        "id": doc_id,
        "source": source[:128],
        "ts": None,                # rag-worker fills NOW() if missing
        "content": content[:5000],
        "severity": severity or "ERROR",
    }

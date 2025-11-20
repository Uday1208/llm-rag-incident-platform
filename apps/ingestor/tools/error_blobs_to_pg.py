# tools/error_blobs_to_pg.py
"""
Read list of error-like blob paths, summarize incidents, and send to rag-worker /v1/ingest.

Env / CLI:
  --conn BLOB_CONN            (Azure Blob connection string)                [required]
  --container BLOB_CONTAINER  (e.g., raw-logs)                              [default: raw-logs]
  --paths-file PATHS_FILE     (newline-separated blob paths)                [required]
  --min-level LEVEL           (DEBUG|INFO|WARNING|ERROR|CRITICAL)           [default: WARNING]
  --keep-internal             (keep site-packages frames in snippet)        [default: False]
  --dry-run                   (preview only, no POST)                       [default: False]
  --rag-url RAG_WORKER_URL    (e.g., https://rag-worker... )                [env RAG_WORKER_URL]
  --rag-token RAG_WORKER_TOKEN(Bearer token if you enforce auth)            [env RAG_WORKER_TOKEN]
  --batch-size N              (#docs per POST)                              [default: 32]
  --timeout SECS              (POST timeout)                                [default: 10]

Notes:
- Generates id as sha1(source + content + blob_path) for dedupe
- ts comes from the blob record when present, else now()
- severity kept as the normalized string (ERROR/WARNING/...)
"""

import os, sys, json, hashlib, argparse
from datetime import datetime, timezone
from typing import Dict, Any, Iterable, List, Tuple, Optional

import httpx
from azure.storage.blob import BlobServiceClient

LEVEL_MAP = {
    "DEBUG": 10, "INFO": 20, "WARNING": 30, "WARN": 30, "ERROR": 40, "CRITICAL": 50, "FATAL": 50
}

def _utc_iso(dt: Optional[datetime]=None) -> str:
    return (dt or datetime.now(timezone.utc)).isoformat()

def _sha1_id(*parts: str) -> str:
    h = hashlib.sha1()
    for p in parts:
        h.update(p.encode("utf-8", errors="ignore"))
    return h.hexdigest()

def _coerce_level(v: Any) -> Tuple[str, int]:
    """Return (LEVEL_STR, RANK). Handles strings/ints/None."""
    if isinstance(v, int):
        # map 0..50-ish to buckets
        if v >= 50: return "CRITICAL", 50
        if v >= 40: return "ERROR", 40
        if v >= 30: return "WARNING", 30
        if v >= 20: return "INFO", 20
        return "DEBUG", 10
    s = str(v or "").strip().upper()
    if s in LEVEL_MAP: return s if s != "WARN" else "WARNING", LEVEL_MAP["WARNING" if s=="WARN" else s]
    # heuristics from message text
    return "ERROR", 40 if "ERROR" in s else ("WARNING", 30)[0=="__never__"]  # default ERROR if unknown

def _iter_jsonl_bytes(raw: bytes) -> Iterable[Dict[str, Any]]:
    # Accepts full blob bytes, yields dict records
    txt = raw.decode("utf-8", errors="ignore")
    for line in txt.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            yield json.loads(line)
        except Exception:
            # wrap non-json as message
            yield {"message": line}

def _is_tb_header(line: str) -> bool:
    return line.strip().startswith("Traceback (most recent call last):")

def _is_internal_frame(line: str) -> bool:
    return '"/usr/local/lib/python' in line or "/site-packages/" in line

def _is_app_frame(line: str) -> bool:
    return 'File "/app/' in line

def _first_non_empty(*candidates: Optional[str]) -> Optional[str]:
    for c in candidates:
        if c and str(c).strip():
            return str(c).strip()
    return None

def _extract_props_log(rec: Dict[str, Any]) -> Optional[str]:
    props = rec.get("properties") or rec.get("Properties") or {}
    if isinstance(props, dict):
        v = props.get("Log") or props.get("log")
        if isinstance(v, str) and v.strip():
            return v
    # fall back to top-level message/msg
    m = rec.get("message") or rec.get("msg")
    return m if isinstance(m, str) else None

def _normalize_ts(rec: Dict[str, Any]) -> Optional[str]:
    cand = _first_non_empty(
        rec.get("time"), rec.get("timestamp"), rec.get("timeGenerated"), rec.get("ts")
    )
    if not cand:
        return None
    try:
        # Accept both 'Z' and +00:00
        if cand.endswith("Z"):
            cand = cand[:-1] + "+00:00"
        datetime.fromisoformat(cand)
        return cand if cand.endswith("Z") else cand  # keep as-is
    except Exception:
        return None

def _normalize_source(rec: Dict[str, Any]) -> str:
    cat = rec.get("category") or rec.get("Category")
    app = rec.get("ContainerAppName") or rec.get("app") or rec.get("source")
    if cat and app: return f"{app}/{cat}"
    return str(cat or app or "ContainerAppConsoleLogs")

def _stitch_episodes(records: List[Dict[str, Any]]) -> List[List[str]]:
    """
    Group consecutive log lines into episodes using 'Traceback...' separators.
    Return list of episodes; each episode is a list of lines (strings).
    """
    episodes: List[List[str]] = []
    cur: List[str] = []
    for rec in records:
        line = _extract_props_log(rec)
        if not line: 
            continue
        if _is_tb_header(line) and cur:
            # new traceback starts, close previous
            episodes.append(cur)
            cur = [line]
        else:
            cur.append(line)
    if cur:
        episodes.append(cur)
    return episodes

def _format_incident(ep_lines: List[str], keep_internal: bool=False) -> str:
    """
    Create concise content:
      - Headline = the first strong error line (often the exception line before the traceback)
      - Snippet  = minimal traceback lines; prefer /app/ frames; suppress internal frames unless asked
    """
    # Headline = first line containing an Exception/ERROR-ish if present
    headline = None
    for s in ep_lines:
        if "Error" in s or "Exception" in s or "ERROR" in s or "CRITICAL" in s:
            headline = s.strip()
            break
    headline = headline or "n/a"

    # Snippet – keep the traceback header and relevant frames
    snippet_lines: List[str] = []
    seen_tb = False
    for s in ep_lines:
        if _is_tb_header(s):
            seen_tb = True
            snippet_lines.append("Traceback (most recent call last):")
            continue
        if not seen_tb:
            continue  # only after TB header

        # Keep app frames, drop internal unless keep_internal=True
        if _is_app_frame(s) or keep_internal:
            snippet_lines.append(s)

        # also keep the final exception lines following frames
        if not s.startswith('  File "') and ("Error" in s or "Exception" in s):
            snippet_lines.append(s)

    return f"Headline: {headline}\nSnippet:\n" + ("\n".join(snippet_lines) if snippet_lines else "(none)")

def _incidents_from_blob_bytes(raw: bytes, min_level: str="WARNING", keep_internal: bool=False) -> List[Dict[str, Any]]:
    recs = list(_iter_jsonl_bytes(raw))
    # Filter to only console/system logs that carry properties.Log/message
    filtered = [r for r in recs if _extract_props_log(r)]
    if not filtered:
        return []
    # level gate (use the max level across lines of an episode later)
    episodes = _stitch_episodes(filtered)
    out: List[Dict[str, Any]] = []
    for ep in episodes:
        # compute source/ts/level from the lines that have them
        lvl_rank = 0
        lvl_str = "INFO"
        src = "ContainerAppConsoleLogs"
        ts = None
        for rec in filtered:
            p = _extract_props_log(rec)
            if not p: 
                continue
            # naive membership; if needed you can improve by indexing
            if p in ep:
                s, r = _coerce_level(rec.get("level") or rec.get("Level") or rec.get("severity"))
                if r > lvl_rank: lvl_str, lvl_rank = s, r
                s2 = _normalize_source(rec)
                src = s2 or src
                ts = ts or _normalize_ts(rec)

        if lvl_rank < LEVEL_MAP.get(min_level.upper(), 30):
            continue

        content = _format_incident(ep, keep_internal=keep_internal)
        out.append({"source": src, "ts": ts, "severity": lvl_str, "content": content})
    return out

def _post_docs(rag_url: str, token: Optional[str], docs: List[Dict[str, Any]], timeout: float=10.0) -> bool:
    ing_url = rag_url.rstrip("/") + "/v1/ingest"
    headers = {"content-type":"application/json"}
    if token:
        headers["authorization"] = f"Bearer {token}"
    with httpx.Client(timeout=timeout) as http:
        resp = http.post(ing_url, json={"documents": docs}, headers=headers)
        ok = 200 <= resp.status_code < 300
        if not ok:
            body = resp.text[:400]
            print(f"[ingest] status={resp.status_code} body={body}")
        else:
            print(f"[ingest] inserted={len(docs)}")
        return ok

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--conn", required=True, help="Azure Blob connection string")
    ap.add_argument("--container", default="raw-logs", help="Blob container name")
    ap.add_argument("--paths-file", required=True, help="Text file with newline-separated blob paths")
    ap.add_argument("--min-level", default="WARNING", help="Min level to keep (DEBUG|INFO|WARNING|ERROR|CRITICAL)")
    ap.add_argument("--keep-internal", action="store_true", help="Keep site-packages frames in snippet")
    ap.add_argument("--dry-run", action="store_true", help="Preview only, no POST")
    ap.add_argument("--rag-url", default=os.getenv("RAG_WORKER_URL", ""), help="rag-worker base URL")
    ap.add_argument("--rag-token", default=os.getenv("RAG_WORKER_TOKEN", ""), help="Bearer token if any")
    ap.add_argument("--batch-size", type=int, default=32)
    ap.add_argument("--timeout", type=float, default=10.0)
    args = ap.parse_args()

    if not args.dry_run and not args.rag_url:
        print("ERROR: --rag-url (or env RAG_WORKER_URL) is required when not --dry-run")
        sys.exit(2)

    bsc = BlobServiceClient.from_connection_string(args.conn)
    cc = bsc.get_container_client(args.container)
    paths = [p.strip() for p in open(args.paths_file, "r", encoding="utf-8").read().splitlines() if p.strip()]

    print(f"[run] blobs={len(paths)} min_level={args.min_level} dry_run={args.dry_run}")

    staged: List[Dict[str, Any]] = []
    total = 0
    for i, blob in enumerate(paths, 1):
        try:
            raw = cc.download_blob(blob).readall()
        except Exception as e:
            print(f"[warn] failed to download {blob}: {e}")
            continue

        incidents = _incidents_from_blob_bytes(
            raw, min_level=args.min_level, keep_internal=args.keep_internal
        )
        if not incidents:
            print(f"[{i}/{len(paths)}] {blob} -> 0 incidents")
            continue

        docs = []
        for inc in incidents:
            source = inc["source"] or "ContainerAppConsoleLogs"
            ts = inc.get("ts") or _utc_iso()
            content = (inc.get("content") or "").strip()
            severity = inc.get("severity") or "ERROR"
            _id = _sha1_id(source, content[:200], blob)  # stable dedupe id
            docs.append({
                "id": _id,
                "source": source[:128],
                "ts": ts,
                "severity": severity,
                "content": content[:5000],
            })

        if args.dry_run:
            print(f"\n--- DRY-RUN {i}/{len(paths)}: {blob} ---")
            for d in docs:
                print(f"source={d['source']} severity={d['severity']} ts={d['ts']}\n{d['content'][:600]}\n")
        else:
            staged.extend(docs)
            if len(staged) >= args.batch_size:
                if not _post_docs(args.rag_url, args.rag_token, staged, timeout=args.timeout):
                    print("[err] POST failed; stopping")
                    sys.exit(1)
                total += len(staged)
                staged = []

    if not args.dry_run and staged:
        if not _post_docs(args.rag_url, args.rag_token, staged, timeout=args.timeout):
            print("[err] final POST failed")
            sys.exit(1)
        total += len(staged)

    print(f"[done] total prepared docs: {total if not args.dry_run else 'DRY-RUN only'}")

if __name__ == "__main__":
    main()

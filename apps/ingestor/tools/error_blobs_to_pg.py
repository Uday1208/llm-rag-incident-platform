# tools/error_blobs_to_pg.py
# Reads blob paths (one per line) from --list file, downloads each JSONL blob,
# summarizes with modules.incidents.summarize_blob (returns List[dict]),
# inserts into Postgres via psycopg2.
#
# Env Vars:
#   BLOB_CONN        : Azure Blob Storage connection string
#   BLOB_CONTAINER   : Container name (e.g., raw-logs)
#   PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD (or DATABASE_URL)
#
# Usage:
#   python -m tools.error_blobs_to_pg --list error_blobs.txt \
#     --min-level WARNING --dry-run

import os
import sys
import json
import argparse
import logging
import hashlib
from datetime import datetime, timezone
from typing import Iterable, Optional, List, Tuple

import psycopg2
from psycopg2.extras import execute_batch
from azure.storage.blob import BlobServiceClient

from modules.incidents import summarize_blob  # signature: (blob_text: str, *, min_level: str="WARNING") -> List[dict]

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO), format="%(levelname)s %(message)s")
log = logging.getLogger("error_blobs_to_pg")


# ------------ helpers ------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _parse_ts(value: Optional[str]) -> datetime:
    """Parse ISO8601 or fallback to now(UTC)."""
    if not value:
        return datetime.now(timezone.utc)
    try:
        v = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return datetime.now(timezone.utc)

def _sha1(*parts: str) -> str:
    h = hashlib.sha1()
    for p in parts:
        h.update(p.encode("utf-8", errors="ignore"))
    return h.hexdigest()

def _iter_blob_paths(pathfile: str) -> Iterable[str]:
    with open(pathfile, "r", encoding="utf-8") as fh:
        for line in fh:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if s.startswith('"') and s.endswith('"'):
                s = s[1:-1]
            yield s

def _open_pg():
    dsn = os.getenv("DATABASE_URL")
    if dsn:
        return psycopg2.connect(dsn)
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "postgres"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", ""),
    )

def _insert_rows(rows: List[Tuple[str, str, datetime, Optional[str], str]]) -> int:
    """rows: (id, source, ts, severity, content)"""
    if not rows:
        return 0
    sql = """
    INSERT INTO documents (id, source, ts, severity, content)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING
    """
    conn = _open_pg()
    try:
        with conn, conn.cursor() as cur:
            execute_batch(cur, sql, rows, page_size=200)
        return len(rows)
    finally:
        conn.close()


# ------------ main ------------

def main():
    ap = argparse.ArgumentParser(description="Summarize error blobs and write incidents to Postgres.")
    ap.add_argument("--list", required=True, help="Path to file containing blob paths (one per line).")
    ap.add_argument("--min-level", default=os.getenv("MIN_LEVEL", "WARNING"),
                    help="Minimum severity to keep: DEBUG|INFO|WARNING|ERROR|CRITICAL")
    ap.add_argument("--dry-run", action="store_true", help="Preview inserts without writing to Postgres.")
    args = ap.parse_args()

    blob_conn = os.getenv("BLOB_CONN", "")
    container = os.getenv("BLOB_CONTAINER", "raw-logs")
    if not blob_conn:
        log.error("BLOB_CONN is required")
        sys.exit(2)

    paths = list(_iter_blob_paths(args.list))
    if not paths:
        log.info("No blob paths in --list")
        return

    bsc = BlobServiceClient.from_connection_string(blob_conn)
    cont = bsc.get_container_client(container)

    log.info(f"[start] blobs={len(paths)} min_level={args.min_level} dry_run={args.dry_run}")

    to_insert: List[Tuple[str, str, datetime, Optional[str], str]] = []
    preview_count = 0
    total_found = 0

    for i, blob_path in enumerate(paths, 1):
        try:
            data = cont.download_blob(blob_path).readall()
            text = data.decode("utf-8", errors="ignore")
        except Exception as e:
            log.warning(f"[{i}/{len(paths)}] download failed: {blob_path} err={e}")
            continue

        try:
            incidents = summarize_blob(text, min_level=args.min_level)  # <- current signature
        except TypeError as e:
            log.error(f"[{i}/{len(paths)}] summarize_blob signature mismatch: {e}")
            continue
        except Exception as e:
            log.warning(f"[{i}/{len(paths)}] summarize_blob error: {e}")
            continue

        if not incidents:
            log.info(f"[{i}/{len(paths)}] no incident found: {blob_path}")
            continue

        for inc in incidents:
            total_found += 1
            source = (inc.get("source") or "ContainerAppConsoleLogs")[:128]
            ts_iso = inc.get("ts") or _now_iso()
            ts_dt = _parse_ts(ts_iso)
            severity = inc.get("severity")
            content = inc.get("content") or ""

            inc_id = inc.get("id") or _sha1(source, ts_iso, content)

            if args.dry_run:
                preview_count += 1
                sample = content[:600].rstrip()
                log.info(f"\n--- DRY-RUN #{preview_count} ---\nsource={source} severity={severity} ts={ts_iso}\nCONTENT:\n{sample}\n")
            else:
                to_insert.append((inc_id, source, ts_dt, severity, content))

    if args.dry_run:
        log.info(f"[done] incidents previewed: {preview_count} (from {total_found} found)")
        return

    wrote = _insert_rows(to_insert)
    log.info(f"[done] incidents inserted: {wrote} (requested {len(to_insert)})")


if __name__ == "__main__":
    main()

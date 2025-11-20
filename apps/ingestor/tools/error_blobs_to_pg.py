# tools/errors_blobs_to_pg.py
# Read blob paths (from --paths-file or stdin), summarize incidents via modules.incidents,
# and (optionally) insert into Postgres.
#
# Env:
#   BLOB_CONN, BLOB_CONTAINER
#   (optional) MIN_LEVEL=WARNING|ERROR|...
#   (optional) UPLIFT_FIRST_FRAMES=1
#   (optional) KEEP_INTERNAL=1
#   Postgres via PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE (or DATABASE_URL)
#
# Usage:
#   python -m tools.errors_blobs_to_pg \
#     --paths-file /tmp/error_blobs.txt \
#     --conn "$BLOB_CONN" \
#     --container raw-logs \
#     --min-level ERROR \
#     --dry-run
#
#   # Or pipe paths:
#   cat /tmp/error_blobs.txt | python -m tools.errors_blobs_to_pg --dry-run
#
# Notes:
# - Uses modules.incidents.summarize_blob() to produce a single incident dict per blob:
#       {"content": str, "source": str, "severity": str, "ts": Optional[str]}
# - If modules.incidents.store_single(...) exists, we’ll use it; otherwise we fall back to a local inserter.

from __future__ import annotations

import os
import sys
import json
import argparse
import logging
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Tuple

# Azure Blob
from azure.storage.blob import BlobServiceClient

# Postgres (psycopg v3)
import psycopg

# Your incident module (v3 logic)
try:
    from modules.incidents import summarize_blob as inc_summarize_blob
except Exception as e:
    print(f"FATAL: modules.incidents.summarize_blob not importable: {e}", file=sys.stderr)
    sys.exit(2)

try:
    from modules.incidents import store_single as inc_store_single  # optional
except Exception:
    inc_store_single = None

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format='{"asctime":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
)
log = logging.getLogger("errors_blobs_to_pg")

# --------------------------
# Helpers
# --------------------------
def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _pg_connect() -> psycopg.Connection:
    # Prefer DATABASE_URL if present, else build from PG* env
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        host = os.getenv("PGHOST", "localhost")
        port = os.getenv("PGPORT", "5432")
        user = os.getenv("PGUSER", "postgres")
        pwd  = os.getenv("PGPASSWORD", "")
        db   = os.getenv("PGDATABASE", "postgres")
        dsn = f"host={host} port={port} dbname={db} user={user} password={pwd}"
    return psycopg.connect(dsn, autocommit=True)

def _insert_local(conn: psycopg.Connection, content: str, source: str, severity: Optional[str], ts: Optional[str]) -> int:
    """
    Minimal local insert. Assumes documents(id, source, ts, severity, content, embedding, ttl)
    where:
      - id is TEXT PK
      - ts is NOT NULL (we default to now() when missing)
      - severity is TEXT (nullable)
      - embedding is nullable
      - ttl is integer default (nullable)
    """
    cur = conn.cursor()
    # Use sha1-like stable id made in SQL for simplicity
    # If your table already has ON CONFLICT DO UPDATE, this is idempotent.
    sql = """
    INSERT INTO documents (id, source, ts, severity, content, embedding, ttl)
    VALUES (
      encode(digest(%s || '|' || coalesce(%s, now()::text) || '|' || %s, 'sha1'),'hex'),
      %s,
      coalesce(%s::timestamptz, now()),
      %s,
      %s,
      NULL,
      2
    )
    ON CONFLICT (id) DO NOTHING
    """
    cur.execute(sql, (source, ts, content, source[:128], ts, severity, content[:5000]))
    return cur.rowcount or 0

def _read_paths(args: argparse.Namespace) -> List[str]:
    paths: List[str] = []
    if args.paths_file:
        with open(args.paths_file, "r", encoding="utf-8") as f:
            for line in f:
                p = line.strip()
                if p:
                    paths.append(p)
    else:
        # read from stdin
        for line in sys.stdin:
            p = line.strip()
            if p:
                paths.append(p)
    return paths

def _bool_env(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip() not in ("0", "false", "False", "")

# --------------------------
# Main
# --------------------------
def main():
    parser = argparse.ArgumentParser(description="Summarize error blobs and insert into Postgres.")
    parser.add_argument("--paths-file", help="File containing blob paths (one per line). If not set, read from stdin.")
    parser.add_argument("--conn", default=os.getenv("BLOB_CONN", ""), help="Azure Blob connection string")
    parser.add_argument("--container", default=os.getenv("BLOB_CONTAINER", "raw-logs"), help="Blob container name")
    parser.add_argument("--min-level", default=os.getenv("MIN_LEVEL", "WARNING"), help="Minimum severity gate (e.g., WARNING, ERROR)")
    parser.add_argument("--keep-internal", action="store_true", default=_bool_env("KEEP_INTERNAL", False), help="Keep internal frames (/site-packages).")
    parser.add_argument("--uplift-first-frames", action="store_true", default=_bool_env("UPLIFT_FIRST_FRAMES", True), help="Boost first user frames near the head.")
    parser.add_argument("--dry-run", action="store_true", help="Do not insert into Postgres; just preview.")
    parser.add_argument("--source-override", default=None, help="Override 'source' field for all incidents.")
    args = parser.parse_args()

    if not args.conn:
        log.error(json.dumps({"msg":"missing blob connection string (--conn or BLOB_CONN)"}))
        sys.exit(2)

    blob_paths = _read_paths(args)
    if not blob_paths:
        log.warning(json.dumps({"msg":"no blob paths provided"}))
        return

    log.info(json.dumps({
        "msg": "begin",
        "count_paths": len(blob_paths),
        "container": args.container,
        "min_level": args.min_level,
        "uplift_first_frames": args.uplift_first_frames,
        "keep_internal": args.keep_internal,
        "dry_run": args.dry_run
    }))

    blob_svc = BlobServiceClient.from_connection_string(args.conn)

    inserted = 0
    processed = 0

    # Postgres connection (only if not dry-run)
    conn = None
    if not args.dry_run:
        try:
            conn = _pg_connect()
        except Exception as e:
            log.error(json.dumps({"msg":"pg connect failed","err":str(e)}))
            sys.exit(2)

    for i, path in enumerate(blob_paths, 1):
        processed += 1
        try:
            # Use the v3 summarizer from modules.incidents
            # It should return either None (no incident) or a dict:
            #   {"content": str, "source": str, "severity": str, "ts": Optional[str]}
            incident = inc_summarize_blob(
                blob_svc=blob_svc,
                container=args.container,
                blob_path=path,
                min_level=args.min_level,
                uplift_first_frames=args.uplift_first_frames,
                keep_internal=args.keep_internal,
            )
        except TypeError:
            # backward-compat call signature (if module was older)
            incident = inc_summarize_blob(
                blob_svc, args.container, path, args.min_level, args.uplift_first_frames, args.keep_internal
            )
        except Exception as e:
            log.warning(json.dumps({"msg":"summarize failed","blob":path,"err":str(e)}))
            continue

        if not incident:
            log.info(json.dumps({"msg":"no-incident","blob":path}))
            continue

        # Normalize fields
        content: str = (incident.get("content") or "").strip()
        source: str = (args.source_override or incident.get("source") or "ContainerAppConsoleLogs")[:128]
        severity: Optional[str] = (incident.get("severity") or None)
        ts: Optional[str] = incident.get("ts") or None

        if args.dry_run:
            print("\n--- DRY-RUN PREVIEW ---")
            print(f"blob={path}")
            print(f"source={source} severity={severity} ts={ts}")
            print("CONTENT:\n" + (content if content else "(no content)"))
            continue

        # Store using module helper if available, else local insert
        try:
            if inc_store_single is not None:
                # store_single(conn, content, source, severity, ts=None) -> int
                n = inc_store_single(conn, content, source, severity, ts)
            else:
                n = _insert_local(conn, content, source, severity, ts)
            inserted += n
            log.info(json.dumps({"msg":"stored","blob":path,"rows":n}))
        except Exception as e:
            log.error(json.dumps({"msg":"store failed","blob":path,"err":str(e)}))

    if conn:
        try:
            conn.close()
        except Exception:
            pass

    log.info(json.dumps({"msg":"done","processed":processed,"inserted":inserted}))

if __name__ == "__main__":
    main()

# tools/reingest_missing_embeddings.py
# Re-send docs with NULL embeddings to rag-worker /v1/ingest so embeddings are computed and upserted.
#
# Env:
#   RAG_WORKER_URL  : e.g. https://<rag-worker-fqdn>
#   PGHOST PGPORT PGDATABASE PGUSER PGPASSWORD (or DATABASE_URL)
#
# Usage:
#   python -m tools.reingest_missing_embeddings --limit 500 --batch 50 --dry-run
#   python -m tools.reingest_missing_embeddings --limit 500 --batch 50

import os, sys, json, time, argparse, logging
from datetime import datetime, timezone
from typing import List, Dict, Any

import psycopg2
import psycopg2.extras
import httpx

logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
                    format="%(levelname)s %(message)s")
log = logging.getLogger("reingest_missing_embeddings")

def _open_pg():
    dsn = os.getenv("DATABASE_URL")
    if dsn:
        return psycopg2.connect(dsn)
    return psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PG_DB", "postgres"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASS", "")
    )

def _iso(dt) -> str:
    if dt is None:
        return datetime.now(timezone.utc).isoformat()
    if isinstance(dt, str):
        return dt
    # psycopg returns aware datetimes -> isoformat ok
    return dt.isoformat()

def _fetch_batch(limit: int) -> List[Dict[str, Any]]:
    sql = """
      SELECT id, source, ts, severity, content
      FROM documents
      WHERE embedding IS NULL
      ORDER BY ts NULLS LAST, id
      LIMIT %s
    """
    with _open_pg() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
        return [dict(r) for r in rows]

def _to_ingest_docs(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    docs = []
    for r in rows:
        docs.append({
            "id": r["id"],
            "source": r.get("source") or "unknown",
            "ts": _iso(r.get("ts")),
            "content": r.get("content") or "",
            "severity": r.get("severity")
        })
    return docs

def main():
    ap = argparse.ArgumentParser(description="Backfill embeddings by re-sending rows with NULL embedding to rag-worker.")
    ap.add_argument("--limit", type=int, default=200, help="Max rows to fetch from DB")
    ap.add_argument("--batch", type=int, default=50, help="Docs per POST to /v1/ingest")
    ap.add_argument("--sleep", type=float, default=0.2, help="Seconds between POSTs")
    ap.add_argument("--dry-run", action="store_true", help="Print what would be sent")
    args = ap.parse_args()

    base = (os.getenv("RAG_WORKER_URL") or "").rstrip("/")
    if not base:
        log.error("RAG_WORKER_URL is required")
        sys.exit(2)
    url = f"{base}/v1/ingest"

    rows = _fetch_batch(args.limit)
    if not rows:
        log.info("No rows with NULL embedding.")
        return
    docs = _to_ingest_docs(rows)

    log.info(f"[start] rows={len(rows)} -> batches of {args.batch} dry_run={args.dry_run}")
    if args.dry_run:
        sample = docs[:3]
        log.info(json.dumps({"sample": sample}, indent=2)[:800])

    if args.dry_run:
        log.info("[done] (dry-run)")
        return

    client = httpx.Client(timeout=30)
    try:
        sent = 0
        for i in range(0, len(docs), args.batch):
            chunk = docs[i:i+args.batch]
            resp = client.post(url, json={"documents": chunk})
            ok = 200 <= resp.status_code < 300
            log.info(f"[{i//args.batch+1}] POST {url} count={len(chunk)} -> {resp.status_code}{'' if ok else ' body='+resp.text[:200]}")
            sent += len(chunk)
            time.sleep(args.sleep)
        log.info(f"[done] posted={sent}")
    finally:
        client.close()

if __name__ == "__main__":
    main()

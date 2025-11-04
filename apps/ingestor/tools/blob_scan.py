"""
File: tools/blob_scan.py
Purpose: Scan existing JSONL blobs in a container to find malformed lines and summarize stats.
Usage (inside container or locally with creds):
  python -m tools.blob_scan
Env:
  BLOB_CONN          (existing) connection string
  BLOB_CONTAINER     (existing) container name, e.g., "raw-logs"
  SCAN_BLOB_PREFIX   optional, e.g., "y=2025/m=11/d=04/"
  SCAN_MAX_BLOBS     optional, default 50
  SCAN_MAX_BYTES     optional, default 10_000_000  (10 MB per blob safety cap)
  SCAN_SHOW_BAD      optional, default 3           (show N bad examples)
"""

import os
import sys
import json
from azure.storage.blob import BlobServiceClient  # already used in your project
from io import BytesIO

BLOB_CONN = os.environ["BLOB_CONN"]
BLOB_CONTAINER = os.environ["BLOB_CONTAINER"]
SCAN_BLOB_PREFIX = os.environ.get("SCAN_BLOB_PREFIX", "")
SCAN_MAX_BLOBS = int(os.environ.get("SCAN_MAX_BLOBS", "50"))
SCAN_MAX_BYTES = int(os.environ.get("SCAN_MAX_BYTES", "10000000"))
SCAN_SHOW_BAD = int(os.environ.get("SCAN_SHOW_BAD", "3"))

def iter_lines_stream(blob_client):
    """Stream blob bytes and yield lines safely across chunk boundaries."""
    buf = bytearray()
    total = 0
    for chunk in blob_client.download_blob().chunks():
        total += len(chunk)
        if total > SCAN_MAX_BYTES:
            break
        buf.extend(chunk)
        while True:
            nl = buf.find(b"\n")
            if nl == -1:
                break
            line = bytes(buf[:nl]).decode("utf-8", errors="replace").strip()
            del buf[:nl+1]
            yield line
    # tail
    if buf:
        yield bytes(buf).decode("utf-8", errors="replace").strip()

def main():
    svc = BlobServiceClient.from_connection_string(BLOB_CONN)
    cont = svc.get_container_client(BLOB_CONTAINER)

    bad_total = 0
    good_total = 0
    scanned = 0

    print(json.dumps({"msg": "scan_start", "prefix": SCAN_BLOB_PREFIX}))

    for b in cont.list_blobs(name_starts_with=SCAN_BLOB_PREFIX):
        if scanned >= SCAN_MAX_BLOBS:
            break
        scanned += 1
        bc = cont.get_blob_client(b.name)
        bad = []
        good = 0
        i = 0
        for line in iter_lines_stream(bc):
            i += 1
            if not line:
                continue
            try:
                json.loads(line)
                good += 1
            except json.JSONDecodeError as e:
                if len(bad) < SCAN_SHOW_BAD:
                    bad.append({"line_no": i, "err": str(e), "sample": line[:300]})
        good_total += good
        bad_total += len(bad)
        if bad:
            print(json.dumps({"blob": b.name, "bad_count": len(bad), "examples": bad}, ensure_ascii=False))
    print(json.dumps({"msg": "scan_done", "scanned": scanned, "good_total": good_total, "bad_total": bad_total}))

if __name__ == "__main__":
    main()

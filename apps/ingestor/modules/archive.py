# apps/ingestor/modules/archive.py
# Raw archive helpers for Azure Blob (async).

from typing import List, Optional
from datetime import datetime, timezone

from azure.storage.blob.aio import BlobServiceClient

def get_blob_client(conn_str: str) -> BlobServiceClient:
    return BlobServiceClient.from_connection_string(conn_str, logging_enable=False)

async def archive_raw(
    blob_svc: BlobServiceClient,
    container: str,
    prefix: str,
    partition_id: str,
    lines: List[str],
) -> Optional[str]:
    """
    Write JSONL text to blob path: {prefix}/{partition}/YYYY/MM/DD/HH/MM/{ts}.jsonl
    Returns blob path on success.
    """
    now = datetime.now(timezone.utc)
    path = f"{prefix}/{partition_id}/{now:%Y/%m/%d/%H/%M}/{int(now.timestamp()*1_000_000)}.jsonl"
    cont = blob_svc.get_container_client(container)
    data = "".join(lines).encode("utf-8", "ignore")
    await cont.upload_blob(name=path, data=data, overwrite=False)
    return path

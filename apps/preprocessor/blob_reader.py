"""
Blob storage operations for the preprocessor.

Reads Application Insights logs from Azure Blob Storage / ADLS Gen2.
"""

import json
import logging
from typing import List, Dict, Any, Optional, AsyncIterator
from datetime import datetime, timedelta

from azure.storage.blob.aio import BlobServiceClient, ContainerClient
from azure.core.exceptions import ResourceNotFoundError

log = logging.getLogger("preprocessor.blob")


class BlobReader:
    """Async blob reader for Application Insights logs."""
    
    def __init__(self, connection_string: str, container: str):
        self.connection_string = connection_string
        self.container_name = container
        self._client: Optional[BlobServiceClient] = None
    
    async def __aenter__(self):
        self._client = BlobServiceClient.from_connection_string(self.connection_string)
        return self
    
    async def __aexit__(self, *args):
        if self._client:
            await self._client.close()
    
    @property
    def container(self) -> ContainerClient:
        return self._client.get_container_client(self.container_name)
    
    async def list_blobs(
        self,
        prefix: str,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
    ) -> List[str]:
        """
        List blobs matching prefix and optional time range.
        
        Args:
            prefix: Blob path prefix (e.g., "appinsights/2025/12/27/")
            since: Only include blobs modified after this time
            until: Only include blobs modified before this time
            
        Returns:
            List of blob names
        """
        blobs = []
        async for blob in self.container.list_blobs(name_starts_with=prefix):
            # Filter by modification time if specified
            if since and blob.last_modified < since:
                continue
            if until and blob.last_modified > until:
                continue
            blobs.append(blob.name)
        
        log.info(f"Found {len(blobs)} blobs with prefix '{prefix}'")
        return blobs
    
    async def list_blobs_for_date(self, prefix: str, date: datetime) -> List[str]:
        """List blobs for a specific date (App Insights format: YYYY/MM/DD/)."""
        date_prefix = f"{prefix}{date.strftime('%Y/%m/%d')}/"
        return await self.list_blobs(date_prefix)
    
    async def list_unprocessed_blobs(
        self,
        source_prefix: str,
        processed_prefix: str,
        limit: int = 100,
    ) -> List[str]:
        """
        List blobs that haven't been processed yet.
        
        Uses a marker file approach: after processing blob X,
        we create a marker at processed_prefix/X.done
        """
        source_blobs = await self.list_blobs(source_prefix)
        
        unprocessed = []
        for blob_name in source_blobs:
            marker_name = f"{processed_prefix}{blob_name}.done"
            try:
                await self.container.get_blob_client(marker_name).get_blob_properties()
                # Marker exists, blob already processed
                continue
            except ResourceNotFoundError:
                unprocessed.append(blob_name)
            
            if len(unprocessed) >= limit:
                break
        
        log.info(f"Found {len(unprocessed)} unprocessed blobs")
        return unprocessed
    
    async def read_blob(self, blob_name: str) -> str:
        """Read blob content as string."""
        blob_client = self.container.get_blob_client(blob_name)
        download = await blob_client.download_blob()
        content = await download.readall()
        return content.decode("utf-8")
    
    async def read_blob_json(self, blob_name: str) -> List[Dict[str, Any]]:
        """
        Read and parse blob as JSON/JSONL.
        
        Handles:
        - JSONL (one JSON object per line)
        - Single JSON object
        - Azure export format ({"records": [...]})
        """
        content = await self.read_blob(blob_name)
        return parse_log_content(content)
    
    async def stream_blobs(
        self,
        blob_names: List[str],
    ) -> AsyncIterator[Dict[str, Any]]:
        """Stream parsed records from multiple blobs."""
        for blob_name in blob_names:
            try:
                records = await self.read_blob_json(blob_name)
                for record in records:
                    record["_blob_name"] = blob_name
                    yield record
            except Exception as e:
                log.warning(f"Failed to read blob {blob_name}: {e}")
                continue
    
    async def mark_processed(self, blob_name: str, processed_prefix: str) -> None:
        """Create a marker indicating blob has been processed."""
        marker_name = f"{processed_prefix}{blob_name}.done"
        blob_client = self.container.get_blob_client(marker_name)
        await blob_client.upload_blob(
            datetime.utcnow().isoformat().encode(),
            overwrite=True
        )


def parse_log_content(content: str) -> List[Dict[str, Any]]:
    """
    Parse log content in various formats.
    
    Supports:
    - JSONL (newline-delimited JSON)
    - Single JSON object
    - Azure export format ({"records": [...]})
    """
    content = content.strip()
    if not content:
        return []
    
    records = []
    
    # Try JSONL first (most common for streaming exports)
    if content.startswith("{") and "\n" in content:
        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    # Handle Azure export envelope
                    if "records" in obj and isinstance(obj["records"], list):
                        records.extend(obj["records"])
                    else:
                        records.append(obj)
                elif isinstance(obj, list):
                    records.extend(obj)
            except json.JSONDecodeError:
                continue
        
        if records:
            return records
    
    # Try single JSON
    try:
        obj = json.loads(content)
        if isinstance(obj, dict):
            if "records" in obj and isinstance(obj["records"], list):
                return obj["records"]
            return [obj]
        if isinstance(obj, list):
            return obj
    except json.JSONDecodeError:
        pass
    
    return records

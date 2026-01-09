"""
Blob storage operations for the preprocessor.

Reads Application Insights logs from Azure Blob Storage / ADLS Gen2.
"""

import json
import logging
from typing import List, Dict, Any, Optional, AsyncIterator
from datetime import datetime, timedelta

from .log_utils import parse_log_content

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
    ) -> List[Any]:
        """
        List blobs matching prefix and optional time range.
        
        Args:
            prefix: Blob path prefix (e.g., "appinsights/2025/12/27/")
            since: Only include blobs modified after this time
            until: Only include blobs modified before this time
            
        Returns:
            List of blob properties objects
        """
        blobs = []
        async for blob in self.container.list_blobs(name_starts_with=prefix):
            # Filter by modification time if specified
            if since and blob.last_modified < since:
                continue
            if until and blob.last_modified > until:
                continue
            blobs.append(blob)
        
        log.info(f"Found {len(blobs)} blobs with prefix '{prefix}'")
        return blobs
    
    async def list_blobs_for_date(self, prefix: str, date: datetime) -> List[str]:
        """List blobs for a specific date (App Insights format: YYYY/MM/DD/)."""
        date_prefix = f"{prefix}{date.strftime('%Y/%m/%d')}/"
        blobs = await self.list_blobs(date_prefix)
        return [b.name for b in blobs]
    
    async def list_unprocessed_blobs(
        self,
        source_prefix: str,
        processed_prefix: str,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        List blobs that haven't been processed yet or have new data.
        
        Returns:
            List of dicts: {"name": str, "offset": int}
        """
        source_blobs = await self.list_blobs(source_prefix)
        
        unprocessed = []
        for src_blob in source_blobs:
            blob_name = src_blob.name
            marker_name = f"{processed_prefix}{blob_name}.done"
            
            try:
                marker_client = self.container.get_blob_client(marker_name)
                stream = await marker_client.download_blob()
                marker_data = json.loads(await stream.readall())
                
                last_size = marker_data.get("offset", 0)
                
                # If source is LARGER than last processed size, we have new data
                if src_blob.size > last_size:
                    log.info(f"Blob {blob_name} has new data: {last_size} -> {src_blob.size} bytes")
                    unprocessed.append({"name": blob_name, "offset": last_size})
                else:
                    # Already fully processed
                    continue
            except (ResourceNotFoundError, json.JSONDecodeError, ValueError):
                # Marker doesn't exist or is invalid, process from start
                unprocessed.append({"name": blob_name, "offset": 0})
            
            if len(unprocessed) >= limit:
                break
        
        log.info(f"Found {len(unprocessed)} blobs with new data")
        return unprocessed
    
    async def read_blob(self, blob_name: str, offset: int = 0) -> str:
        """Read blob content as string starting from offset."""
        blob_client = self.container.get_blob_client(blob_name)
        
        # Get current size to know where we stop
        props = await blob_client.get_blob_properties()
        total_size = props.size
        
        if offset >= total_size:
            return ""
            
        download = await blob_client.download_blob(offset=offset)
        content = await download.readall()
        return content.decode("utf-8")
    
    async def read_blob_json(self, blob_name: str, offset: int = 0) -> List[Dict[str, Any]]:
        """Read and parse blob as JSON starting from offset."""
        content = await self.read_blob(blob_name, offset=offset)
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
        """Create a marker indicating current size of processed blob."""
        marker_name = f"{processed_prefix}{blob_name}.done"
        
        # Get current source size
        blob_client = self.container.get_blob_client(blob_name)
        props = await blob_client.get_blob_properties()
        
        marker_data = {
            "offset": props.size,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        marker_client = self.container.get_blob_client(marker_name)
        await marker_client.upload_blob(
            json.dumps(marker_data).encode(),
            overwrite=True
        )



"""
Core processing pipeline.

Orchestrates:
1. Read logs from Blob Storage
2. Normalize using trace_context
3. Group by TraceID using trace_bundler
4. Summarize with LLM
5. Send to rag-worker for storage
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

import httpx
import pandas as pd

# Import from sibling ingestor modules
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "ingestor"))

try:
    from modules.trace_context import normalize_app_insights, normalize_batch
    from modules.trace_bundler import BatchTraceBundler, BundlerConfig
except ImportError:
    # Fallback for standalone execution
    from ingestor.modules.trace_context import normalize_app_insights, normalize_batch
    from ingestor.modules.trace_bundler import BatchTraceBundler, BundlerConfig

from config import PreprocessorConfig
from blob_reader import BlobReader
from summarizer import IncidentSummarizer, summarize_bundles

log = logging.getLogger("preprocessor.pipeline")


class ProcessingPipeline:
    """Main preprocessing pipeline."""
    
    def __init__(self, config: PreprocessorConfig):
        self.config = config
        self.summarizer = IncidentSummarizer(reasoning_agent_url=config.reasoning_agent_url) if config.enable_llm_summary else None
        self.bundler = BatchTraceBundler(BundlerConfig(
            window_seconds=config.trace_window_seconds,
            min_severity=config.min_severity,
            max_logs_per_bundle=config.max_logs_per_bundle,
        ))
    
    async def process_blobs(self, blob_names: List[str]) -> Dict[str, Any]:
        """
        Process a list of blobs.
        
        Args:
            blob_names: List of blob names or dicts with name/offset
            
        Returns:
            Processing statistics
        """
        stats = {
            "blobs_processed": 0,
            "logs_read": 0,
            "logs_normalized": 0,
            "bundles_created": 0,
            "bundles_stored": 0,
            "errors": [],
        }
        
        async with BlobReader(
            self.config.blob_connection_string,
            self.config.blob_container
        ) as reader:
            
            for blob_item in blob_names:
                # Handle both simple names and offset dicts
                if isinstance(blob_item, dict):
                    blob_name = blob_item["name"]
                    offset = blob_item.get("offset", 0)
                else:
                    blob_name = blob_item
                    offset = 0
                    
                try:
                    blob_stats = await self._process_single_blob(reader, blob_name, offset=offset)
                    stats["blobs_processed"] += 1
                    stats["logs_read"] += blob_stats["logs_read"]
                    stats["logs_normalized"] += blob_stats["logs_normalized"]
                    stats["bundles_created"] += blob_stats["bundles_created"]
                    stats["bundles_stored"] += blob_stats["bundles_stored"]
                    
                except Exception as e:
                    log.error(f"Failed to process blob {blob_name}: {e}")
                    stats["errors"].append({"blob": blob_name, "error": str(e)})
        
        return stats
    
    async def _process_single_blob(
        self,
        reader: BlobReader,
        blob_name: str,
        offset: int = 0
    ) -> Dict[str, int]:
        """Process a single blob."""
        log.info(f"Processing blob: {blob_name} (offset: {offset})")
        
        # 1. Read and parse logs
        records = await reader.read_blob_json(blob_name, offset=offset)
        logs_read = len(records)
        
        # 2. Normalize to canonical format
        normalized = normalize_batch(records, min_severity=self.config.min_severity)
        logs_normalized = len(normalized)
        
        if not normalized:
            log.debug(f"No logs above {self.config.min_severity} in {blob_name}")
            await reader.mark_processed(blob_name, self.config.processed_prefix)
            return {"logs_read": logs_read, "logs_normalized": 0, "bundles_created": 0, "bundles_stored": 0}
        
        # 3. Bundle by TraceID
        bundles = self.bundler.bundle_records(normalized)
        bundles_created = len(bundles)
        
        if not bundles:
            log.debug(f"No bundles created from {blob_name}")
            await reader.mark_processed(blob_name, self.config.processed_prefix)
            return {"logs_read": logs_read, "logs_normalized": logs_normalized, "bundles_created": 0, "bundles_stored": 0}
        
        # 4. Add blob reference
        for bundle in bundles:
            bundle["raw_blob_path"] = blob_name
        
        # 5. Summarize with LLM (optional)
        if self.summarizer:
            bundles = await summarize_bundles(bundles, self.summarizer)
        
        # 6. Send to rag-worker
        bundles_stored = await self._store_bundles(bundles)
        
        # 7. Mark as processed
        await reader.mark_processed(blob_name, self.config.processed_prefix)
        
        log.info(f"Processed {blob_name}: {logs_read} logs → {bundles_created} bundles → {bundles_stored} stored")
        
        return {
            "logs_read": logs_read,
            "logs_normalized": logs_normalized,
            "bundles_created": bundles_created,
            "bundles_stored": bundles_stored,
        }
    
    async def _store_bundles(self, bundles: List[Dict[str, Any]]) -> int:
        """Send bundles to rag-worker for embedding and storage."""
        if not bundles:
            return 0
        
        url = f"{self.config.rag_worker_url.rstrip('/')}/v1/ingest"
        headers = {"Content-Type": "application/json"}
        if self.config.rag_worker_token:
            headers["Authorization"] = f"Bearer {self.config.rag_worker_token}"
        
        # Convert bundles to rag-worker format
        docs = []
        for bundle in bundles:
            doc = {
                "id": bundle.get("id"),
                "source": bundle.get("service", "unknown"),
                "ts": bundle.get("first_ts").isoformat() if bundle.get("first_ts") else None,
                "content": bundle.get("content"),
                "severity": bundle.get("severity"),
                "meta": {
                    "trace_id": bundle.get("trace_id"),
                    "operation": bundle.get("operation"),
                    "log_count": bundle.get("log_count"),
                    "symptoms": bundle.get("symptoms"),
                    "failing_dependency": bundle.get("failing_dependency"),
                    "error_signature": bundle.get("error_signature"),
                    "raw_blob_path": bundle.get("raw_blob_path"),
                },
            }
            docs.append(doc)
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.post(url, headers=headers, json={"documents": docs})
                response.raise_for_status()
                return len(docs)
        except Exception as e:
            log.error(f"Failed to store bundles: {e}")
            return 0
    
    async def process_unprocessed(self, limit: int = 100) -> Dict[str, Any]:
        """Process all unprocessed blobs up to limit."""
        async with BlobReader(
            self.config.blob_connection_string,
            self.config.blob_container
        ) as reader:
            blob_names = await reader.list_unprocessed_blobs(
                self.config.blob_prefix,
                self.config.processed_prefix,
                limit=limit,
            )
        
        if not blob_names:
            log.info("No unprocessed blobs found")
            return {"blobs_processed": 0}
        
        return await self.process_blobs(blob_names)
    
    async def process_date(self, date: datetime) -> Dict[str, Any]:
        """Process all blobs for a specific date."""
        async with BlobReader(
            self.config.blob_connection_string,
            self.config.blob_container
        ) as reader:
            blob_names = await reader.list_blobs_for_date(self.config.blob_prefix, date)
        
        return await self.process_blobs(blob_names)

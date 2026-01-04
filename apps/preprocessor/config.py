"""
Preprocessor configuration.

Configurable trigger modes:
- schedule: Timer-based (cron expression)
- event: Blob trigger (when new logs arrive)
- on-demand: HTTP trigger (manual API call)
"""

import os
from enum import Enum
from dataclasses import dataclass
from typing import Optional


class TriggerMode(str, Enum):
    """Trigger modes for preprocessing pipeline."""
    SCHEDULE = "schedule"
    EVENT = "event"
    ON_DEMAND = "on-demand"


@dataclass
class PreprocessorConfig:
    """Configuration for the log preprocessor."""
    
    # Trigger settings
    trigger_mode: TriggerMode
    schedule_cron: str  # For schedule mode
    
    # Blob storage
    blob_connection_string: str
    blob_container: str
    blob_prefix: str  # e.g., "raw-logs/appinsights/"
    processed_prefix: str  # Track processed blobs
    
    # Processing settings
    batch_size: int
    min_severity: str
    trace_window_seconds: int
    max_logs_per_bundle: int
    
    # Output
    rag_worker_url: str
    rag_worker_token: Optional[str]
    
    # LLM for summarization (optional)
    enable_llm_summary: bool
    
    @classmethod
    def from_env(cls) -> "PreprocessorConfig":
        """Load configuration from environment variables."""
        trigger_str = os.getenv("PREPROCESS_TRIGGER", "schedule").lower()
        try:
            trigger_mode = TriggerMode(trigger_str)
        except ValueError:
            trigger_mode = TriggerMode.SCHEDULE
        
        return cls(
            trigger_mode=trigger_mode,
            schedule_cron=os.getenv("PREPROCESS_CRON", "*/15 * * * *"),  # Every 15 min
            
            blob_connection_string=os.getenv("BLOB_CONN", ""),
            blob_container=os.getenv("BLOB_CONTAINER", "raw-logs"),
            blob_prefix=os.getenv("BLOB_PREFIX", "appinsights/"),
            processed_prefix=os.getenv("PROCESSED_PREFIX", "processed/"),
            
            batch_size=int(os.getenv("PREPROCESS_BATCH_SIZE", "1000")),
            min_severity=os.getenv("PREPROCESS_MIN_SEVERITY", "INFO"),
            trace_window_seconds=int(os.getenv("TRACE_WINDOW_SECONDS", "60")),
            max_logs_per_bundle=int(os.getenv("MAX_LOGS_PER_BUNDLE", "100")),
            
            rag_worker_url=os.getenv("RAG_WORKER_URL", "http://rag-worker:8000"),
            rag_worker_token=os.getenv("RAG_WORKER_TOKEN"),
            
            enable_llm_summary=os.getenv("ENABLE_LLM_SUMMARY", "true").lower() == "true",
        )

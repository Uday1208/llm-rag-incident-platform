"""
Trace-aware incident bundler.

Groups normalized log entries by TraceID into coherent incident bundles.
Handles time windowing and severity-based filtering.
"""

import hashlib
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


# =============================================================================
# Configuration
# =============================================================================

@dataclass
class BundlerConfig:
    """Configuration for the TraceBundler."""
    window_seconds: int = 60          # Time window for grouping
    min_severity: str = "WARNING"     # Minimum severity to create bundle
    max_logs_per_bundle: int = 100    # Limit logs per bundle
    max_content_length: int = 10000   # Max content chars for embedding


SEVERITY_ORDER = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3, "CRITICAL": 4}


# =============================================================================
# In-Memory Bundler (for streaming/real-time)
# =============================================================================

@dataclass
class IncidentBundle:
    """Represents a single incident bundle."""
    trace_id: str
    service: str
    operation: Optional[str] = None
    severity: str = "INFO"
    logs: List[Dict[str, Any]] = field(default_factory=list)
    first_ts: Optional[datetime] = None
    last_ts: Optional[datetime] = None
    
    def add_log(self, log: Dict[str, Any]) -> None:
        """Add a log entry to the bundle."""
        self.logs.append(log)
        
        # Update timestamps
        ts = log.get("timestamp")
        if ts:
            if isinstance(ts, str):
                try:
                    ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                except ValueError:
                    ts = None
            
            if ts:
                if self.first_ts is None or ts < self.first_ts:
                    self.first_ts = ts
                if self.last_ts is None or ts > self.last_ts:
                    self.last_ts = ts
        
        # Update severity (keep highest)
        log_sev = log.get("severity", "INFO")
        if SEVERITY_ORDER.get(log_sev, 1) > SEVERITY_ORDER.get(self.severity, 1):
            self.severity = log_sev
        
        # Update service/operation if not set
        if not self.service or self.service == "unknown":
            self.service = log.get("service", "unknown")
        if not self.operation:
            self.operation = log.get("operation")
    
    def to_dict(self, config: BundlerConfig) -> Dict[str, Any]:
        """Convert bundle to dictionary for storage."""
        content = self._format_content(config.max_content_length)
        
        # Generate deterministic ID
        id_input = f"{self.trace_id}:{self.service}:{self.first_ts.isoformat() if self.first_ts else 'no-ts'}"
        bundle_id = hashlib.sha256(id_input.encode()).hexdigest()[:32]
        
        return {
            "id": bundle_id,
            "trace_id": self.trace_id,
            "service": self.service,
            "operation": self.operation,
            "severity": self.severity,
            "content": content,
            "log_count": len(self.logs),
            "first_ts": self.first_ts,
            "last_ts": self.last_ts,
            "logs_sample": self.logs[:10],  # Keep sample for debugging
        }
    
    def _format_content(self, max_length: int) -> str:
        """Format logs into searchable content."""
        lines = []
        char_count = 0
        
        for log in sorted(self.logs, key=lambda x: x.get("timestamp") or ""):
            sev = log.get("severity", "INFO")
            msg = log.get("message", "")
            
            line = f"[{sev}] {msg}"
            
            # Add exception details if present
            if log.get("exception_type"):
                line += f"\n  Exception: {log['exception_type']}"
            if log.get("stack_trace"):
                # Include first few lines of stack trace
                stack_lines = log["stack_trace"].split("\n")[:5]
                line += "\n  " + "\n  ".join(stack_lines)
            
            if char_count + len(line) > max_length:
                lines.append(f"... ({len(self.logs) - len(lines)} more logs truncated)")
                break
            
            lines.append(line)
            char_count += len(line)
        
        return "\n".join(lines)


class StreamingTraceBundler:
    """
    Streaming bundler for real-time log processing.
    Maintains in-memory state and emits completed bundles.
    """
    
    def __init__(self, config: Optional[BundlerConfig] = None):
        self.config = config or BundlerConfig()
        self.bundles: Dict[str, IncidentBundle] = {}
        self.bundle_start_times: Dict[str, datetime] = {}
    
    def add_log(self, log: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Add a log entry. Returns completed bundle if window expired.
        
        Args:
            log: Normalized log entry with trace_id
            
        Returns:
            Completed bundle dict if window expired, else None
        """
        trace_id = log.get("trace_id")
        if not trace_id:
            return None
        
        completed_bundle = None
        now = datetime.now()
        
        # Check if existing bundle should be completed
        if trace_id in self.bundles:
            start_time = self.bundle_start_times.get(trace_id, now)
            if (now - start_time).total_seconds() > self.config.window_seconds:
                completed_bundle = self._complete_bundle(trace_id)
        
        # Add to bundle (create if needed)
        if trace_id not in self.bundles:
            self.bundles[trace_id] = IncidentBundle(trace_id=trace_id, service="unknown")
            self.bundle_start_times[trace_id] = now
        
        self.bundles[trace_id].add_log(log)
        
        return completed_bundle
    
    def flush(self) -> List[Dict[str, Any]]:
        """Complete and return all pending bundles."""
        results = []
        for trace_id in list(self.bundles.keys()):
            bundle = self._complete_bundle(trace_id)
            if bundle:
                results.append(bundle)
        return results
    
    def _complete_bundle(self, trace_id: str) -> Optional[Dict[str, Any]]:
        """Complete a bundle and remove from state."""
        bundle = self.bundles.pop(trace_id, None)
        self.bundle_start_times.pop(trace_id, None)
        
        if not bundle:
            return None
        
        # Check severity threshold
        if SEVERITY_ORDER.get(bundle.severity, 1) < SEVERITY_ORDER.get(self.config.min_severity, 2):
            return None
        
        return bundle.to_dict(self.config)


# =============================================================================
# Batch Bundler (for Pandas-based preprocessing)
# =============================================================================

class BatchTraceBundler:
    """
    Batch bundler using Pandas for preprocessing jobs.
    Processes all logs at once (not streaming).
    """
    
    def __init__(self, config: Optional[BundlerConfig] = None):
        if not HAS_PANDAS:
            raise ImportError("pandas required for BatchTraceBundler")
        self.config = config or BundlerConfig()
    
    def bundle_dataframe(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Group logs by trace_id and create bundles.
        
        Args:
            df: DataFrame with normalized log entries
            
        Returns:
            List of incident bundle dicts
        """
        if df.empty or "trace_id" not in df.columns:
            return []
        
        # Filter out logs without trace_id
        df = df[df["trace_id"].notna()].copy()
        if df.empty:
            return []
        
        # Map severity to numeric for comparison
        df["sev_num"] = df["severity"].map(lambda x: SEVERITY_ORDER.get(x, 1))
        
        # Find traces with at least one log at or above min_severity
        min_sev_num = SEVERITY_ORDER.get(self.config.min_severity, 2)
        trace_max_sev = df.groupby("trace_id")["sev_num"].max()
        relevant_traces = trace_max_sev[trace_max_sev >= min_sev_num].index
        
        bundles = []
        for trace_id in relevant_traces:
            trace_logs = df[df["trace_id"] == trace_id].sort_values("timestamp")
            
            # Limit logs per bundle
            if len(trace_logs) > self.config.max_logs_per_bundle:
                trace_logs = trace_logs.head(self.config.max_logs_per_bundle)
            
            bundle = self._create_bundle(trace_id, trace_logs)
            bundles.append(bundle)
        
        return bundles
    
    def bundle_records(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Convenience method to bundle from list of dicts.
        
        Args:
            records: List of normalized log entries
            
        Returns:
            List of incident bundle dicts
        """
        df = pd.DataFrame(records)
        return self.bundle_dataframe(df)
    
    def _create_bundle(self, trace_id: str, logs_df: pd.DataFrame) -> Dict[str, Any]:
        """Create a bundle from a trace's logs."""
        # Get most common service
        service = "unknown"
        if "service" in logs_df.columns:
            service_counts = logs_df["service"].value_counts()
            if not service_counts.empty:
                service = service_counts.index[0]
        
        # Get first operation
        operation = None
        if "operation" in logs_df.columns:
            first_op = logs_df["operation"].dropna().head(1)
            if not first_op.empty:
                operation = first_op.iloc[0]
        
        # Get max severity
        max_sev_num = logs_df["sev_num"].max()
        severity = next((s for s, n in SEVERITY_ORDER.items() if n == max_sev_num), "INFO")
        
        # Get timestamps
        first_ts = None
        last_ts = None
        if "timestamp" in logs_df.columns:
            ts_col = pd.to_datetime(logs_df["timestamp"], errors="coerce")
            valid_ts = ts_col.dropna()
            if not valid_ts.empty:
                first_ts = valid_ts.min()
                last_ts = valid_ts.max()
        
        # Format content
        content = self._format_content(logs_df)
        
        # Generate ID
        id_input = f"{trace_id}:{service}:{first_ts.isoformat() if first_ts else 'no-ts'}"
        bundle_id = hashlib.sha256(id_input.encode()).hexdigest()[:32]
        
        return {
            "id": bundle_id,
            "trace_id": trace_id,
            "service": service,
            "operation": operation,
            "severity": severity,
            "content": content,
            "log_count": len(logs_df),
            "first_ts": first_ts,
            "last_ts": last_ts,
        }
    
    def _format_content(self, logs_df: pd.DataFrame) -> str:
        """Format DataFrame logs into content string with deduplication."""
        if logs_df.empty:
            return ""
            
        # Prioritize Errors: if we have any ERROR logs, only keep WARNING+ or unique INFO messages
        has_errors = (logs_df["sev_num"] >= SEVERITY_ORDER["ERROR"]).any()
        
        lines = []
        char_count = 0
        seen_messages = set()
        
        # Sort logs by timestamp
        sorted_logs = logs_df.sort_values("timestamp")
        
        for _, row in sorted_logs.iterrows():
            sev = row.get("severity", "INFO")
            msg = row.get("message", "").strip()
            
            # Simple deduplication: skip if message seen recently in this trace
            if msg in seen_messages:
                continue
            
            # If we have real errors, skip generic INFO chatter like "Response status: 200"
            if has_errors and SEVERITY_ORDER.get(sev, 1) < SEVERITY_ORDER["WARNING"]:
                if any(p in msg for p in ["Response status: 20", "Request URL", "Job", "Scheduler"]):
                    continue
            
            seen_messages.add(msg)
            
            line = f"[{sev}] {msg}"
            
            # Add stack trace if explicitly present
            if "stack_trace" in row and pd.notna(row["stack_trace"]):
                line += f"\n  Stack Trace: {str(row['stack_trace'])[:500]}..."
            
            if char_count + len(line) > self.config.max_content_length:
                lines.append(f"... ({len(logs_df) - len(lines)} more logs truncated)")
                break
            
            lines.append(line)
            char_count += len(line)
        
        return "\n".join(lines)

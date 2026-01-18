"""
Trace-aware incident bundler.

Groups normalized log entries by TraceID into coherent incident bundles.
Handles time windowing and severity-based filtering.
"""

import re
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
        Group logs by trace_id and create bundles with TraceID propagation.
        """
        if df.empty:
            return []

        # 1. Ensure timestamps are proper objects and sort
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.sort_values("timestamp").reset_index(drop=True)
        
        # 2. Identify replica for each log (used for isolation and propagation)
        def get_replica(row):
            return row.get("container_group") or row.get("container_id") or row.get("revision") or "global"
        
        df["replica"] = df.apply(get_replica, axis=1)
        
        # 3. Propagate TraceIDs within the same replica
        # If a log has no ID, it might belong to the previous active trace on that replica
        last_trace = {} # (service, replica) -> (trace_id, timestamp)
        
        final_trace_ids = []
        for _, row in df.iterrows():
            t_id = row.get("trace_id")
            service = row.get("service", "unknown")
            replica = row["replica"]
            ts = row["timestamp"]
            key = (service, replica)
            
            # If we have a valid ID, update our "session" memory
            if pd.notna(t_id) and str(t_id) != "0":
                final_trace_ids.append(t_id)
                last_trace[key] = (t_id, ts)
                continue
                
            # If no ID, check if we can "borrow" from the last record in this replica
            if key in last_trace:
                prev_id, prev_ts = last_trace[key]
                # If the previous log was recent (within 30s), use its TraceID
                if pd.notna(ts) and pd.notna(prev_ts) and (ts - prev_ts).total_seconds() < 30:
                    final_trace_ids.append(prev_id)
                    continue
            
            # Otherwise, fall back to synthetic (bucketed by time)
            if pd.notna(ts):
                bucket = ts.replace(minute=(ts.minute // 5) * 5, second=0, microsecond=0)
                synth_id = f"synth-{service}-{replica}-{bucket.strftime('%Y%m%d%H%M')}"
                final_trace_ids.append(synth_id)
            else:
                final_trace_ids.append(f"orphan-{service}-{replica}-unknown")
                
        df["trace_id"] = final_trace_ids
        
        # 4. Filter and group
        df["sev_num"] = df["severity"].map(lambda x: SEVERITY_ORDER.get(x, 1))
        min_sev_num = SEVERITY_ORDER.get(self.config.min_severity, 2)
        
        # Find traces that are relevant
        trace_max_sev = df.groupby("trace_id")["sev_num"].max()
        relevant_traces = trace_max_sev[trace_max_sev >= min_sev_num].index
        
        # 5. Deduplicate similar incidents using content fingerprinting
        fingerprints = {} # fingerprint -> bundle
        
        for trace_id in relevant_traces:
            trace_logs = df[df["trace_id"] == trace_id].sort_values("timestamp")
            
            bundle = self._create_bundle(trace_id, trace_logs)
            
            # Generate fingerprint to identify recurring errors
            fp = self._generate_fingerprint(bundle["content"])
            
            if fp in fingerprints:
                existing = fingerprints[fp]
                
                # Merge line ranges
                ranges = [existing["raw_line_range"], bundle["raw_line_range"]]
                merged_range = ", ".join(filter(None, ranges))
                existing["raw_line_range"] = merged_range
                
                # Sum logs
                existing["log_count"] += bundle["log_count"]
                
                # Keep earliest TS
                if bundle["first_ts"] and (not existing["first_ts"] or bundle["first_ts"] < existing["first_ts"]):
                    existing["first_ts"] = bundle["first_ts"]
                
                # Keep highest severity
                if SEVERITY_ORDER.get(bundle["severity"], 1) > SEVERITY_ORDER.get(existing["severity"], 1):
                    existing["severity"] = bundle["severity"]
            else:
                fingerprints[fp] = bundle
        
        final_bundles = list(fingerprints.values())
        
        # 6. Final Sort: return incidents in chronological order of their START time
        final_bundles.sort(key=lambda b: b.get("first_ts") or datetime.min)
        
        return final_bundles
    
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
        
        # Get line range
        line_range = None
        if "raw_line" in logs_df.columns:
            valid_lines = logs_df["raw_line"].dropna()
            if not valid_lines.empty:
                min_line = int(valid_lines.min())
                max_line = int(valid_lines.max())
                line_range = f"{min_line}-{max_line}" if min_line != max_line else str(min_line)

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
        
        # Extract Propagation Flow: unique sequence of services in chronological order
        propagation = []
        if "service" in logs_df.columns:
            # Drop NA and get the sequence
            sequence = logs_df["service"].dropna().tolist()
            # Remove consecutive duplicates to show the "hops"
            for s in sequence:
                if not propagation or propagation[-1] != s:
                    propagation.append(s)

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
            "raw_line_range": line_range,
            "propagation": propagation,
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
        
        last_sev = None
        in_traceback = False
        skip_segment = False
        
        for _, row in sorted_logs.iterrows():
            sev = row.get("severity", "INFO")
            msg = row.get("message", "").rstrip()
            
            # Simple deduplication: skip if message seen recently in this trace
            if msg in seen_messages:
                continue
            
            # Traceback filtering logic
            is_file_line = 'File "' in msg and (", line " in msg or "line " in msg)
            is_traceback_start = "Traceback (most recent call last):" in msg
            
            if is_traceback_start:
                in_traceback = True
                skip_segment = False
            elif is_file_line:
                # Decide if we skip this segment (internal libs vs custom code)
                is_internal = any(p in msg for p in ["<frozen", "runpy.py", "lib/python", "site-packages"])
                skip_segment = is_internal
                
                # Global rule: if it's an internal file line, we always skip it
                if is_internal:
                    continue
            elif msg.startswith(" "):
                # If it's a continuation line and we are in a skip zone, skip it
                if skip_segment:
                    continue
            else:
                # It's a non-indented line. Not a traceback start or file line.
                # It's likely the final exception or a new log message.
                in_traceback = False
                skip_segment = False
            
            # If we were in a skipped traceback and hit a file line, we already skipped it above

            # If we have real errors, skip generic INFO chatter like "Response status: 200"
            if has_errors and SEVERITY_ORDER.get(sev, 1) < SEVERITY_ORDER["WARNING"]:
                if any(p in msg for p in ["Response status: 20", "Request URL", "Job", "Scheduler"]):
                    continue
            
            seen_messages.add(msg)
            
            # Clean traceback formatting: omit prefix if same as previous line
            is_traceback_cont = msg.startswith(" ") or is_file_line or is_traceback_start
            is_exception_marker = "During handling of the above exception" in msg or "The above exception was the direct cause" in msg
            
            if sev == last_sev and (is_traceback_cont or is_exception_marker):
                line = f"            {msg}"
            else:
                line = f"[{sev}] {msg}"
            
            last_sev = sev
            
            # Add stack trace if explicitly present
            if "stack_trace" in row and pd.notna(row["stack_trace"]):
                line += f"\n  Stack Trace: {str(row['stack_trace'])[:500]}..."
            
            if char_count + len(line) > self.config.max_content_length:
                lines.append(f"... ({len(logs_df) - len(lines)} more logs truncated)")
                break
            
            lines.append(line)
            char_count += len(line)
        
        return "\n".join(lines)

    def _generate_fingerprint(self, content: str) -> str:
        """
        Create a stable fingerprint of log content by stripping dynamic strings.
        Now operates on unique line patterns to handle sampling and volume variations.
        """
        if not content:
            return "empty"
            
        unique_patterns = set()
        
        # Split content into lines to analyze patterns
        for line in content.splitlines():
            # 1. Normalize whitespace and case
            p = line.lower().strip()
            
            # 2. Strip Hex IDs (TraceIDs, SpanIDs, etc.)
            p = re.sub(r'[a-f0-9]{32}', '[id32]', p)
            p = re.sub(r'[a-f0-9]{16}', '[id16]', p)
            p = re.sub(r'0x[a-f0-9]+', '[hex]', p)
            
            # 3. Strip Timestamps (ISO and variations)
            p = re.sub(r'\d{4}-\d{2}-\d{2}[t\s]\d{2}:\d{2}:\d{2}(\.\d+)?(z|[+-]\d{2}:?\d{2})?', '[ts]', p)
            p = re.sub(r'\d{2}:\d{2}:\d{2}(,\d{3})?', '[time]', p)
            
            # 4. Strip Variable numbers (ports, counts, indices)
            p = re.sub(r'\b\d+\b', '[num]', p)
            
            # 5. Strip common dynamic paths/URLs segments
            p = re.sub(r'https?://[^\s]+', '[url]', p)
            
            # 6. Strip trailing punctuation and extra whitespace
            p = p.strip('.:; ')
            
            if p:
                unique_patterns.add(p)
        
        if not unique_patterns:
            return "empty"
            
        # Sort patterns to ensure stable hash regardless of message order
        stable_content = "\n".join(sorted(list(unique_patterns)))
        
        # Take hash as fingerprint
        return hashlib.md5(stable_content.encode()).hexdigest()

"""
TraceID extraction and Application Insights schema normalization.

Extracts distributed trace context (operation_Id, spanId) from
Application Insights logs for incident correlation.
"""

import re
import hashlib
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime, timezone


# =============================================================================
# Application Insights Field Mappings
# =============================================================================

# TraceID fields (operation_Id is the primary one in App Insights)
TRACE_ID_KEYS = (
    "operation_Id",      # Application Insights standard
    "operationId",       # Alternative casing
    "traceId",           # W3C Trace Context
    "TraceId",           # .NET style
    "x-ms-request-id",   # Azure internal
    "otelTraceId",       # OpenTelemetry custom injection
)

# SpanID / RequestID fields
SPAN_ID_KEYS = (
    "operation_ParentId",  # App Insights parent span
    "spanId",              # W3C
    "SpanId",              # .NET style
    "id",                  # App Insights item ID
    "requestId",           # Azure
    "RequestId",           # .NET style
    "otelSpanId",          # OpenTelemetry custom injection
)

# Service identification
SERVICE_KEYS = (
    "cloud_RoleName",      # App Insights standard (best)
    "appName",             # Alternative
    "serviceName",         # OpenTelemetry
    "ContainerAppName",    # Azure Container Apps
)

# Operation name (HTTP method + path typically)
OPERATION_KEYS = (
    "operation_Name",      # App Insights
    "operationName",       # Alternative
    "name",                # Generic
)

# Severity mapping from App Insights numeric levels
AI_SEVERITY_MAP = {
    0: "DEBUG",
    1: "INFO", 
    2: "WARNING",
    3: "ERROR",
    4: "CRITICAL",
}

import json

# ...

# Severity string normalization
SEVERITY_NORMALIZE = {
    "verbose": "DEBUG",
    "debug": "DEBUG",
    "information": "INFO",
    "info": "INFO",
    "warning": "WARNING",
    "warn": "WARNING",
    "error": "ERROR",
    "err": "ERROR",
    "critical": "CRITICAL",
    "fatal": "CRITICAL",
}


def _expand_nested_json(payload: Dict[str, Any]) -> None:
    """
    Parses nested JSON string from 'Log' or 'Message' field and merges it into payload.
    This is necessary when container logs (JSON) are wrapped as a string by the platform.
    """
    dims = payload.get("customDimensions") or payload.get("properties") or {}
    
    # Candidates for nested JSON string
    keys_to_check = ["Log", "Message", "msg", "log", "message"]
    
    # Also check inside dimensions
    candidates = []
    for k in keys_to_check:
        if k in payload:
            candidates.append(payload[k])
        if k in dims:
            candidates.append(dims[k])
            
    for val in candidates:
        if isinstance(val, str) and val.strip().startswith("{"):
            try:
                parsed = json.loads(val)
                if isinstance(parsed, dict):
                    # Merge parsed fields into payload (top-level) so extractors can find them
                    # We prioritize existing payload keys, but for trace IDs we want the inner ones
                    for k, v in parsed.items():
                        if k not in payload:
                            payload[k] = v
                        # Special case for logging fields we definitely want from the app
                        if k in ["otelTraceId", "otelSpanId", "service", "name", "level", "levelname", "Stream"]:
                            payload[k] = v
            except (json.JSONDecodeError, TypeError):
                continue



# =============================================================================
# Core Extraction Functions
# =============================================================================

def extract_trace_context(payload: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract TraceID and SpanID from Application Insights log entry.
    
    Args:
        payload: Raw log entry from App Insights export
        
    Returns:
        Tuple of (trace_id, span_id), either may be None
    """
    # Check both root level and customDimensions
    dims = payload.get("customDimensions") or payload.get("properties") or {}
    
    trace_id = None
    for key in TRACE_ID_KEYS:
        trace_id = payload.get(key) or dims.get(key)
        if trace_id:
            break
    
    span_id = None
    for key in SPAN_ID_KEYS:
        span_id = payload.get(key) or dims.get(key)
        if span_id:
            break
    
    return (trace_id, span_id)


def extract_service(payload: Dict[str, Any]) -> str:
    """
    Extract service name from log entry.
    Falls back to 'unknown' if not found.
    """
    dims = payload.get("customDimensions") or payload.get("properties") or {}
    
    for key in SERVICE_KEYS:
        service = payload.get(key) or dims.get(key)
        if service:
            return str(service).strip()
    
    # Try resourceId as fallback (extract app name from Azure resource path)
    resource_id = payload.get("resourceId") or payload.get("_ResourceId")
    if resource_id:
        # Format: /subscriptions/.../resourceGroups/.../providers/.../managedEnvironments/.../containerApps/APP_NAME
        match = re.search(r'/containerApps/([^/]+)', str(resource_id), re.I)
        if match:
            return match.group(1)
    
    return "unknown"


def extract_operation(payload: Dict[str, Any]) -> Optional[str]:
    """Extract operation name (e.g., 'POST /api/payment')."""
    dims = payload.get("customDimensions") or payload.get("properties") or {}
    
    for key in OPERATION_KEYS:
        op = payload.get(key) or dims.get(key)
        if op:
            return str(op).strip()[:256]
    
    return None


def extract_severity(payload: Dict[str, Any]) -> str:
    """
    Extract and normalize severity level.
    Handles both numeric (App Insights) and string formats.
    """
    dims = payload.get("customDimensions") or payload.get("properties") or {}
    
    # Try numeric severityLevel first (App Insights standard)
    sev_level = payload.get("severityLevel")
    if sev_level is not None:
        try:
            return AI_SEVERITY_MAP.get(int(sev_level), "INFO")
        except (ValueError, TypeError):
            pass
    
    # Try string severity fields
    for key in ("severity", "level", "Level", "logLevel", "levelname"):
        sev = payload.get(key)
        if sev:
            normalized = SEVERITY_NORMALIZE.get(str(sev).lower().strip())
            if normalized:
                return normalized
    
    # NEW: Detect stderr and elevate to ERROR
    stream = payload.get("Stream") or dims.get("Stream")
    if stream == "stderr":
        return "ERROR"
    
    # Infer from message content
    message = extract_message(payload)
    if message:
        return infer_severity_from_message(message)
    
    return "INFO"


def extract_message(payload: Dict[str, Any]) -> str:
    """Extract the log message content."""
    # Direct message fields
    for key in ("message", "msg", "renderedMessage"):
        msg = payload.get(key)
        if msg:
            return str(msg)
    
    # App Insights exception structure
    if payload.get("itemType") == "exception":
        outer = payload.get("outerMessage") or payload.get("outerType")
        if outer:
            return str(outer)
    
    # Properties/customDimensions fallback
    dims = payload.get("customDimensions") or payload.get("properties") or {}
    for key in ("Log", "Message", "Details", "error"):
        msg = dims.get(key)
        if msg:
            return str(msg)
    
    return ""


def extract_timestamp(payload: Dict[str, Any]) -> Optional[datetime]:
    """Extract and parse timestamp."""
    for key in ("timestamp", "timeGenerated", "time", "ts"):
        ts = payload.get(key)
        if ts:
            try:
                # Handle ISO format with Z suffix
                ts_str = str(ts).replace("Z", "+00:00")
                return datetime.fromisoformat(ts_str)
            except (ValueError, TypeError):
                continue
    return None


def extract_exception_details(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract exception details for exception-type logs.
    """
    if payload.get("itemType") != "exception":
        return {}
    
    return {
        "exception_type": payload.get("type") or payload.get("outerType"),
        "exception_message": payload.get("outerMessage"),
        "stack_trace": payload.get("details", [{}])[0].get("rawStack") if payload.get("details") else None,
        "assembly": payload.get("assembly"),
    }


def infer_severity_from_message(message: str) -> str:
    """Infer severity from message content patterns."""
    msg_upper = message.upper()
    
    # Exception/error patterns
    if any(p in msg_upper for p in ["TRACEBACK", "FATAL", "CRITICAL"]):
        return "CRITICAL"
    
    if any(p in msg_upper for p in ["EXCEPTION", "ERROR", "FAILED", "FAILURE", "MODULE_NOT_FOUND"]):
        return "ERROR"
    
    # HTTP status codes
    if re.search(r'\b5\d{2}\b', message):
        return "ERROR"
    if re.search(r'\b4\d{2}\b', message):
        return "WARNING"
    
    if any(p in msg_upper for p in ["WARN", "WARNING"]):
        return "WARNING"
    
    return "INFO"


# =============================================================================
# Normalization Function (Main Entry Point)
# =============================================================================

def normalize_app_insights(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize an Application Insights log entry to canonical format.
    
    Args:
        payload: Raw log entry from App Insights Blob export
        
    Returns:
        Normalized document dict or None if invalid/filtered
    """
    if not isinstance(payload, dict):
        return None
    
    # Skip metrics (we only want traces/exceptions)
    if payload.get("itemType") == "metric" or "metricName" in payload:
        return None
    
    # NEW: Attempt to parse nested JSON from the 'Log'/'Message' field
    # Azure Container Apps often wraps the app's stdout JSON in a "Log" string field.
    # We need to unwrap it to find 'otelTraceId' and other fields.
    _expand_nested_json(payload)
    
    # Extract core fields
    trace_id, span_id = extract_trace_context(payload)
    service = extract_service(payload)
    operation = extract_operation(payload)
    severity = extract_severity(payload)
    message = extract_message(payload)
    timestamp = extract_timestamp(payload)
    
    # Skip empty messages or noise patterns
    if not message or not message.strip():
        return None
        
    # Surgical Noise Filter: skip common operational "chatter"
    noise_patterns = [
        r"^Request URL:", 
        r"^Request method:", 
        r"^Request headers:", 
        r"^Response headers:",
        r"^Response status: 20[0-9]",  # Skip successful response status lines
        r"Managed Identity token",     # Azure internal noise
    ]
    if any(re.search(p, message) for p in noise_patterns):
        return None
    
    # Generate deterministic ID
    id_input = f"{trace_id or 'no-trace'}:{service}:{timestamp.isoformat() if timestamp else 'no-ts'}:{message[:100]}"
    doc_id = hashlib.sha256(id_input.encode()).hexdigest()[:32]
    
    # Build normalized document
    doc = {
        "id": doc_id,
        "trace_id": trace_id,
        "span_id": span_id,
        "service": service,
        "operation": operation,
        "severity": severity,
        "message": message.strip()[:5000],  # Limit length
        "timestamp": timestamp,
        
        # Environment context
        "environment": (payload.get("customDimensions") or {}).get("Environment"),
        
        # Original App Insights metadata
        "item_type": payload.get("itemType"),
        "item_id": payload.get("itemId"),
        
        # Exception details if applicable
        **extract_exception_details(payload),
    }
    
    return doc


def normalize_batch(payloads: List[Dict[str, Any]], min_severity: str = "INFO") -> List[Dict[str, Any]]:
    """
    Normalize a batch of log entries with severity filtering.
    
    Args:
        payloads: List of raw log entries
        min_severity: Minimum severity to include (default: INFO)
        
    Returns:
        List of normalized documents above minimum severity
    """
    severity_order = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3, "CRITICAL": 4}
    min_level = severity_order.get(min_severity.upper(), 1)
    
    results = []
    for payload in payloads:
        doc = normalize_app_insights(payload)
        if doc and severity_order.get(doc["severity"], 1) >= min_level:
            results.append(doc)
    
    return results

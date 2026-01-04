"""
File: logging_setup.py
Purpose: Configure structured JSON logging for production observability.
"""

import logging
from pythonjsonlogger import jsonlogger
from opentelemetry import trace
from .config import settings

class TraceIdFilter(logging.Filter):
    """Inject OpenTelemetry trace_id into log records."""
    def filter(self, record):
        span = trace.get_current_span()
        ctx = span.get_span_context()
        record.otelTraceId = format(ctx.trace_id, '032x') if ctx.is_valid else "0"
        record.otelSpanId = format(ctx.span_id, '016x') if ctx.is_valid else "0"
        return True

def configure_logging() -> None:
    """Configure root logger for JSON output and level from settings."""
    logger = logging.getLogger()
    logger.setLevel(settings.LOG_LEVEL)
    handler = logging.StreamHandler()
    formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s %(otelTraceId)s %(otelSpanId)s"
    )
    handler.setFormatter(formatter)
    handler.addFilter(TraceIdFilter())
    logger.handlers = [handler]


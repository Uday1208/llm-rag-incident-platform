"""
Preprocessor main application.

FastAPI app with multiple trigger modes:
- schedule: Timer-based using APScheduler
- event: Blob trigger (for Azure Functions integration)
- on-demand: HTTP endpoint
"""

import asyncio
import logging
import os
from datetime import datetime
from typing import Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from pythonjsonlogger import jsonlogger

# OpenTelemetry
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from config import PreprocessorConfig, TriggerMode
from pipeline import ProcessingPipeline

# =============================================================================
# OpenTelemetry Setup
# =============================================================================

trace.set_tracer_provider(TracerProvider())
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

# =============================================================================
# Logging with Trace ID
# =============================================================================

class TraceIdFilter(logging.Filter):
    """Inject OpenTelemetry trace_id into log records."""
    def filter(self, record):
        span = trace.get_current_span()
        ctx = span.get_span_context()
        record.otelTraceId = format(ctx.trace_id, '032x') if ctx.is_valid else "0"
        record.otelSpanId = format(ctx.span_id, '016x') if ctx.is_valid else "0"
        return True

def configure_logging():
    logger = logging.getLogger()
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))
    handler = logging.StreamHandler()
    formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s %(otelTraceId)s %(otelSpanId)s"
    )
    handler.setFormatter(formatter)
    handler.addFilter(TraceIdFilter())
    logger.handlers = [handler]

log = logging.getLogger("preprocessor")


# =============================================================================
# Request/Response Models
# =============================================================================

class ProcessRequest(BaseModel):
    """Request body for on-demand processing."""
    date: Optional[str] = None  # YYYY-MM-DD format
    blob_names: Optional[list[str]] = None
    limit: int = 100


class ProcessResponse(BaseModel):
    """Response from processing."""
    status: str
    blobs_processed: int
    logs_read: int
    logs_normalized: int
    bundles_created: int
    bundles_stored: int
    errors: list[dict] = []
    duration_seconds: float


# =============================================================================
# Scheduler (for schedule mode)
# =============================================================================

_scheduler = None


def setup_scheduler(pipeline: ProcessingPipeline, cron: str):
    """Setup APScheduler for scheduled processing."""
    global _scheduler
    
    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        log.warning("APScheduler not installed, scheduled mode disabled")
        return
    
    _scheduler = AsyncIOScheduler()
    
    async def scheduled_job():
        log.info("Scheduled processing started")
        try:
            stats = await pipeline.process_unprocessed()
            log.info(f"Scheduled processing complete: {stats}")
        except Exception as e:
            log.error(f"Scheduled processing failed: {e}")
    
    # Parse cron expression (minute hour day month day_of_week)
    parts = cron.split()
    if len(parts) == 5:
        trigger = CronTrigger(
            minute=parts[0],
            hour=parts[1],
            day=parts[2],
            month=parts[3],
            day_of_week=parts[4],
        )
        _scheduler.add_job(scheduled_job, trigger, id="preprocess")
        _scheduler.start()
        log.info(f"Scheduler started with cron: {cron}")
    else:
        log.warning(f"Invalid cron expression: {cron}")


# =============================================================================
# Application
# =============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    configure_logging()  # Initialize structured logging with trace IDs
    
    config = PreprocessorConfig.from_env()
    pipeline = ProcessingPipeline(config)
    
    app.state.config = config
    app.state.pipeline = pipeline
    
    # Setup scheduler if in schedule mode
    if config.trigger_mode == TriggerMode.SCHEDULE:
        setup_scheduler(pipeline, config.schedule_cron)
    
    log.info(f"Preprocessor started in {config.trigger_mode.value} mode")
    
    yield
    
    # Cleanup
    global _scheduler
    if _scheduler:
        _scheduler.shutdown()
    
    log.info("Preprocessor stopped")


app = FastAPI(
    title="LLM-RAG Incident Preprocessor",
    description="Batch preprocessing pipeline for Application Insights logs",
    version="1.0.0",
    lifespan=lifespan,
)

# Instrument FastAPI for distributed tracing
FastAPIInstrumentor.instrument_app(app)


# =============================================================================
# Endpoints
# =============================================================================

@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy", "mode": app.state.config.trigger_mode.value}


@app.get("/config")
async def get_config():
    """Get current configuration (redacted)."""
    config = app.state.config
    return {
        "trigger_mode": config.trigger_mode.value,
        "schedule_cron": config.schedule_cron if config.trigger_mode == TriggerMode.SCHEDULE else None,
        "blob_container": config.blob_container,
        "blob_prefix": config.blob_prefix,
        "min_severity": config.min_severity,
        "enable_llm_summary": config.enable_llm_summary,
    }


@app.post("/process", response_model=ProcessResponse)
async def process_logs(request: ProcessRequest, background_tasks: BackgroundTasks):
    """
    Trigger log processing (on-demand mode).
    
    Options:
    - date: Process all logs for a specific date (YYYY-MM-DD)
    - blob_names: Process specific blobs
    - limit: Max blobs to process (default: 100)
    """
    pipeline: ProcessingPipeline = app.state.pipeline
    start_time = datetime.utcnow()
    
    try:
        if request.blob_names:
            # Process specific blobs
            stats = await pipeline.process_blobs(request.blob_names)
        elif request.date:
            # Process by date
            date = datetime.strptime(request.date, "%Y-%m-%d")
            stats = await pipeline.process_date(date)
        else:
            # Process unprocessed blobs
            stats = await pipeline.process_unprocessed(limit=request.limit)
        
        duration = (datetime.utcnow() - start_time).total_seconds()
        
        return ProcessResponse(
            status="success",
            blobs_processed=stats.get("blobs_processed", 0),
            logs_read=stats.get("logs_read", 0),
            logs_normalized=stats.get("logs_normalized", 0),
            bundles_created=stats.get("bundles_created", 0),
            bundles_stored=stats.get("bundles_stored", 0),
            errors=stats.get("errors", []),
            duration_seconds=duration,
        )
        
    except Exception as e:
        log.error(f"Processing failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/process/async")
async def process_logs_async(request: ProcessRequest, background_tasks: BackgroundTasks):
    """
    Trigger log processing in background (returns immediately).
    """
    pipeline: ProcessingPipeline = app.state.pipeline
    
    async def background_process():
        try:
            if request.blob_names:
                await pipeline.process_blobs(request.blob_names)
            elif request.date:
                date = datetime.strptime(request.date, "%Y-%m-%d")
                await pipeline.process_date(date)
            else:
                await pipeline.process_unprocessed(limit=request.limit)
        except Exception as e:
            log.error(f"Background processing failed: {e}")
    
    background_tasks.add_task(background_process)
    return {"status": "processing", "message": "Processing started in background"}


# =============================================================================
# CLI Entry Point
# =============================================================================

def main():
    """CLI entry point."""
    import uvicorn
    
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8080"))
    
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()

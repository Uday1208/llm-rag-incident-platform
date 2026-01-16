"""
Export router for fine-tuning data.
"""
import json
import logging
from typing import Dict, Any, Generator
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from worker.db import get_conn
from worker.exporters import format_for_finetuning

router = APIRouter()
log = logging.getLogger(__name__)

def generate_jsonl(limit: int = 1000) -> Generator[str, None, None]:
    """Yields JSONL lines for fine-tuning."""
    
    query = """
        SELECT 
            i.incident_id, i.incident_id, 'N/A', i.severity, 
            i.title, '', i.title,
            ir.summary, ir.actions, '', ''
        FROM incidents i
        JOIN incident_resolutions ir ON ir.incident_id = i.incident_id
        ORDER BY i.started_at DESC
        LIMIT %s
    """

    
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (limit,))
                
                for row in cur:
                    # Map tuple to dicts
                    bundle = {
                        "id": row[0],
                        "trace_id": row[1],
                        "service": row[2],
                        "severity": row[3],
                        "symptoms": row[4],
                        "error_signature": row[5],
                        "content": row[6],
                    }
                    
                    resolution = {
                        "summary": row[7],
                        "steps": row[8],
                        "root_cause": row[9],
                        "preventive_action": row[10]
                    }
                    
                    # Format
                    example = format_for_finetuning(bundle, resolution)
                    yield json.dumps(example) + "\n"
                    
    except Exception as e:
        log.error(f"Export failed: {e}")
        # Note: In StreamingResponse, raising HTTP exception mid-stream 
        # just cuts the connection, user sees partial file.
        yield ""

@router.get("/v1/export/fine-tuning")
async def export_fine_tuning_data(limit: int = 1000):
    """
    Export incident execution history in OpenAI JSONL format for fine-tuning.
    
    Returns a stream of JSON lines, where each line is a chat completion example:
    {"messages": [{"role": "system"...}, {"role": "user"...}, {"role": "assistant"...}]}
    """
    return StreamingResponse(
        generate_jsonl(limit),
        media_type="application/x-jsonlines",
        headers={"Content-Disposition": "attachment; filename=incidents_finetune.jsonl"}
    )

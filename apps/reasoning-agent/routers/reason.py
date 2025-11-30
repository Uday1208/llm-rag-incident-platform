# apps/reasoning-agent/routers/reason.py  (example)
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional

import os
import logging

from services.llm_client import chat_reasoning, LLMTimeout, LLMUnavailable
from services.retrieval import top_contexts_for_query  # NEW import

log = logging.getLogger("reason")

router = APIRouter()

class ReasonRequest(BaseModel):
    query: str
    #contexts: Optional[List[str]] = []
    contexts: Optional[List[str]] = None
    temperature: Optional[float] = 0.2
    max_tokens: Optional[int] = 512
    max_suggestions: Optional[int] = 3
    format: Optional[str] = "text"

class ReasonOut(BaseModel):
    answer: str
    
@router.post("/v1/reason", response_model=ReasonOut)
async def reason(req: ReasonRequest) -> ReasonOut:
    """Answer a query; if no contexts provided, auto-augment via RAG search."""
    use_rag = os.getenv("RAG_ENABLE", "1") != "0"
    top_k = int(os.getenv("RAG_TOP_K", "3"))
    max_lines = int(os.getenv("RAG_MAX_LINES", "3"))
    min_score = float(os.getenv("RAG_MIN_SCORE", "0"))

    # 1) Build contexts: prefer user-supplied; else fetch from RAG.
    contexts = req.contexts or []
    if use_rag and not contexts:
        try:
            contexts = await top_contexts_for_query(
                req.query, top_k=top_k, max_lines=max_lines, min_score=min_score
            )
            if not contexts:
                log.info("RAG produced no contexts; proceeding with zero-shot.")
        except Exception as e:
            log.warning(f"RAG lookup failed: {e}; proceeding with zero-shot.")
            contexts = []

    # 2) Call LLM exactly as before (we’re not changing your prompt flow here)
    try:
        text = await chat_reasoning(
            question=req.query,
            #contexts=req.contexts or [],
            contexts=contexts,
            out_format=req.format or "text",
            temperature=req.temperature or 0.2,
            max_tokens=req.max_tokens or 512,
        )
        #return {"answer": text}
        return ReasonOut(answer=text)
    except LLMTimeout as e:
        raise HTTPException(status_code=504, detail=f"LLM timeout: {e}")
    except LLMUnavailable as e:
        raise HTTPException(status_code=503, detail=f"LLM unavailable: {e}")
    except Exception as e:
        log.exception("reason failed")
        raise HTTPException(status_code=500, detail=f"reason failed: {e}")

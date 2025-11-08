# apps/reasoning-agent/routers/reason.py  (example)
from fastapi import APIRouter
from pydantic import BaseModel
from typing import List, Optional

from services.llm_client import chat_reasoning

router = APIRouter()

class ReasonRequest(BaseModel):
    query: str
    contexts: Optional[List[str]] = []
    temperature: Optional[float] = 0.2
    max_tokens: Optional[int] = 512

@router.post("/v1/reason")
async def reason(req: ReasonRequest):
    text = await chat_reasoning(
        question=req.query,
        contexts=req.contexts or [],
        temperature=req.temperature or 0.2,
        max_tokens=req.max_tokens or 512,
    )
    return {"answer": text}

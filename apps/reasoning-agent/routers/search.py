"""Search endpoint that embeds the query and returns nearest documents."""

# reasoning-agent/routers/search.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
from services.retrieval import embed_query, search_by_embedding

router = APIRouter(prefix="/v1", tags=["search"])

class SearchIn(BaseModel):
    query: str
    top_k: int = 5

@router.post("/search")
async def search(inb: SearchIn) -> Dict[str, Any]:
    try:
        vec = await embed_query(inb.query)
        hits = await search_by_embedding(vec, top_k=inb.top_k)
        return {"query": inb.query, "results": hits}
    except ValueError as e:
        # worker contract mismatch → surface as 422 to client
        raise HTTPException(status_code=422, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        # logged by FastAPI error middleware; keep payload clean
        raise HTTPException(status_code=500, detail=f"search failed: {e}")

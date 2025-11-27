"""Internal embed API that reuses rag-worker's embedder for on-demand vectors."""

from typing import List
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from ..embeddings import embed_texts

router = APIRouter(tags=["internal-embed"], prefix="/internal")

class EmbedReq(BaseModel):
    """Request payload containing texts to embed."""
    texts: List[str]

class EmbedResp(BaseModel):
    """Response payload with embeddings for each input text."""
    vectors: List[List[float]]

@router.post("/embed", response_model=EmbedResp)
async def embed(req: EmbedReq) -> EmbedResp:
    """Return embeddings for the provided texts."""
    try:
        vecs = await embed_texts(req.texts)
        out = [[float(x) for x in row] for row in vecs]
        return EmbedResp(vectors=out)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"embed failed: {e}")

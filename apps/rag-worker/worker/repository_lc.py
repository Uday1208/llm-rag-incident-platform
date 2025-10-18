"""
File: repository_lc.py
Purpose: LangChain-backed repository using PGVector for vector storage and search.
"""

from typing import List, Tuple, Optional
from langchain.vectorstores.pgvector import PGVector
from langchain.embeddings.base import Embeddings
from sentence_transformers import SentenceTransformer
from .config import settings

_store: Optional[PGVector] = None

class STEmbeddings(Embeddings):
    """LangChain Embeddings adapter using SentenceTransformer."""
    def __init__(self, model_name: str):
        self.model = SentenceTransformer(model_name)

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """Return embeddings for a list of texts."""
        return self.model.encode(texts, normalize_embeddings=True).tolist()

    def embed_query(self, text: str) -> List[float]:
        """Return embedding for a single query."""
        return self.model.encode([text], normalize_embeddings=True)[0].tolist()

def init_store() -> None:
    """Initialize the global PGVector store if needed."""
    global _store
    if _store is not None:
        return
    embeddings = STEmbeddings(settings.EMBED_MODEL_NAME)
    _store = PGVector(
        connection_string=settings.PG_SQLALCHEMY_URL,
        collection_name=settings.LC_COLLECTION,
        embedding_function=embeddings,
        use_jsonb=True,           # rich metadata
    )

def get_store() -> PGVector:
    """Return the initialized PGVector store."""
    if _store is None:
        init_store()
    assert _store is not None
    return _store

def upsert_texts(ids: List[str], sources: List[str], contents: List[str], ts_iso: List[Optional[str]]) -> int:
    """Upsert documents by id with metadata; embeddings computed via LC."""
    vs = get_store()
    metadatas = []
    for i in range(len(ids)):
        metadatas.append({
            "id": ids[i],
            "source": sources[i],
            "ts": ts_iso[i],
        })
    # PGVector.add_texts handles insert/upsert behavior by ID if present in LC version.
    # If duplicates occur, PGVector will add new rows; we rely on id metadata for de-dup in app-level.
    vs.add_texts(texts=contents, metadatas=metadatas, ids=ids)
    return len(ids)

def search_by_query(query: str, top_k: int) -> List[Tuple[str, str, float]]:
    """Return (id, content, similarity) using LC similarity_search_with_score."""
    vs = get_store()
    results = vs.similarity_search_with_score(query, k=top_k)
    out: List[Tuple[str, str, float]] = []
    for doc, score in results:
        # score is distance; convert to cosine-like similarity [0..1]
        sim = float(1.0 - score) if isinstance(score, (int, float)) else 0.0
        doc_id = (doc.metadata or {}).get("id") or ""  # we stored our id in metadata
        out.append((doc_id, doc.page_content, sim))
    return out

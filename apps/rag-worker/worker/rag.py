"""
File: rag.py
Purpose: RAG orchestration utilities (retrieval + simple answer composition).
"""

from typing import List, Tuple, Optional
from transformers import pipeline
from .config import settings

_gen = None

def _init_gen():
    """Lazy-init a local generation pipeline if MODEL_ID is set."""
    global _gen
    if settings.MODEL_ID and _gen is None:
        _gen = pipeline("text-generation", model=settings.MODEL_ID, device=-1)

def build_prompt(question: str, contexts: List[str]) -> str:
    """Build a simple prompt with inlined context snippets."""
    ctx = "\n\n".join(contexts)
    prompt = (
        "You are a helpful incident analyst. Use the context to answer. "
        "If unknown, say you don't know.\n\n"
        f"Context:\n{ctx}\n\nQuestion: {question}\nAnswer:"
    )
    return prompt[: settings.MAX_CONTEXT_CHARS + len(question) + 200]

def compose_answer(question: str, contexts: List[str]) -> str:
    """Compose an answer using optional generator; else return extractive summary."""
    _init_gen()
    if _gen:
        prompt = build_prompt(question, contexts)
        out = _gen(prompt, max_new_tokens=160, do_sample=False)[0]["generated_text"]
        return out.split("Answer:", 1)[-1].strip() or out[-256:]
    # Fallback: extractive – return joined key sentences
    return " ".join(contexts)[:512]

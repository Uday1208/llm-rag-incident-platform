"""
File: rag.py
Purpose: RAG orchestration utilities (retrieval + simple answer composition).
"""

from typing import List
from transformers import pipeline
from langchain.prompts import PromptTemplate
from .config import settings

_gen = None
_prompt = PromptTemplate.from_template(
    "You are a helpful incident analyst. Use ONLY the context to answer. "
    "If unknown, say you don't know.\n\n"
    "Context:\n{context}\n\nQuestion: {question}\nAnswer:"
)

def _init_gen():
    """Lazy-init a local generation pipeline if MODEL_ID is set."""
    global _gen
    if settings.MODEL_ID and _gen is None:
        _gen = pipeline("text-generation", model=settings.MODEL_ID, device=-1)

def build_prompt(question: str, contexts: List[str]) -> str:
    """Build a prompt using LangChain PromptTemplate."""
    ctx = "\n\n".join(contexts)
    text = _prompt.format(context=ctx[: settings.MAX_CONTEXT_CHARS], question=question)
    return text

def compose_answer(question: str, contexts: List[str]) -> str:
    """Compose an answer via transformers if available; else return extractive summary."""
    _init_gen()
    if _gen:
        prompt = build_prompt(question, contexts)
        out = _gen(prompt, max_new_tokens=160, do_sample=False)[0]["generated_text"]
        # Prefer the segment after 'Answer:' if present
        return out.split("Answer:", 1)[-1].strip() or out[-256:]
    # Fallback: extractive – return joined key sentences
    return " ".join(contexts)[:512]

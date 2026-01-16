# Description: Production Dockerfile for RAG Worker (embeddings + pgvector retrieval).
FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app

# System deps (minimal)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install -r requirements.txt

# App code
COPY worker ./worker
COPY tools ./tools

# Non-root
RUN useradd -m -u 10002 appuser
USER appuser

EXPOSE 8081
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD curl -f http://127.0.0.1:8081/health || exit 1

CMD ["uvicorn", "worker.main:app", "--host", "0.0.0.0", "--port", "8081"]

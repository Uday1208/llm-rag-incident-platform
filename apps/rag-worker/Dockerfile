# Description: Production Dockerfile for RAG Worker (embeddings + pgvector retrieval).
FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app   # <— ensure /app is always on sys.path

WORKDIR /app

# System deps (minimal)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# ---------- Install deps
COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt

# ---------- Copy the WHOLE app directory to avoid path mismatches
# (If your repo layout changes slightly, this still brings worker/ into the image)
COPY . ./

# Sanity check at build-time: list key dirs and fail early if package is missing
RUN set -e; \
    echo "== Build sanity check =="; \
    ls -la /app; \
    ls -la /app/worker; \
    ls -la /app/worker/schemas; \
    python - <<'PY' || (echo "FATAL: cannot import worker.schemas.rag"; exit 1)
import sys
print("PYTHONPATH:", sys.path)
import worker
import worker.schemas.rag as r
print("Import OK ->", r.__file__)
PY

# Non-root
RUN useradd -m -u 10002 appuser
USER appuser

EXPOSE 8081
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD curl -f http://127.0.0.1:8081/health || exit 1

CMD ["uvicorn", "worker.main:app", "--host", "0.0.0.0", "--port", "8081"]

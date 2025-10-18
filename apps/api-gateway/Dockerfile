# Description: Production Dockerfile for API Gateway service (FastAPI + Uvicorn).
# Exposes port 8080 and uses a non-root user; optimized for small image size.
FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# System deps (psql client optional for runtime checks; keep small)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code
COPY app ./app

# Create non-root user
RUN useradd -m -u 10001 appuser
USER appuser

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD curl -f http://127.0.0.1:8080/health || exit 1

# Use Uvicorn directly (simple), or swap to gunicorn+uvicorn workers later.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]

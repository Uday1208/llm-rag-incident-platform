# apps/ingestor/Dockerfile
# Purpose: Event Hubs consumer with Blob archive + forward to rag-worker.

FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System certs + curl for simple health/debug
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

# Copy only reqs first to maximize layer cache
COPY requirements.txt /app/requirements.txt

# Install Python deps
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r /app/requirements.txt

# App source
COPY . /app

# Make sure package-style imports work for modules/
# (if not already present in your repo)
RUN mkdir -p /app/modules && touch /app/modules/__init__.py || true

# Non-root user
RUN useradd -m svc && chown -R svc:svc /app
USER svc

ENV PORT=8080
EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]

# apps/ingestor/Dockerfile
# Purpose: Event Hubs consumer with Blob archive + forward to rag-worker.

FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

# Requirements kept inside image to avoid extra file
RUN pip install --no-cache-dir \
      fastapi==0.103.2 \
      uvicorn[standard]==0.23.2 \
      httpx==0.24.1 \
      pydantic==1.10.13 \
      prometheus-client==0.17.1 \
      python-json-logger==2.0.7 \
      tenacity==8.2.3 \
      azure-eventhub==5.11.5 \
      azure-storage-blob==12.19.0

COPY . /app

# Create non-root user
RUN useradd -m svc && chown -R svc:svc /app
USER svc

ENV PORT=8080
EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]

# apps/reasoning-agent/Dockerfile
# Purpose: Build a minimal FastAPI service that calls Azure OpenAI.

FROM python:3.10-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# System deps (ssl/certs) + build essentials only if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

# App requirements (keep lean)
# If you need local fallback (transformers), uncomment the last two lines
RUN pip install --no-cache-dir \
      fastapi==0.103.2 \
      uvicorn[standard]==0.23.2 \
      httpx==0.24.1 \
      pydantic==1.10.13 \
      anyio>=4.4 \
      prometheus-client==0.17.1 \
      psycopg2-binary==2.9.9 \
      PyYAML>=6,<7
#   transformers==4.36.2 \
#   sentence-transformers==2.2.2

# Copy only the app
COPY . /app

# Non-root
RUN useradd -m svc && chown -R svc:svc /app
USER svc

ENV PORT=8080
EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]

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
# Install dependencies from requirements.txt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only the app
COPY . /app

# Non-root
RUN useradd -m svc && chown -R svc:svc /app
USER svc

ENV PORT=8080
EXPOSE 8080

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]

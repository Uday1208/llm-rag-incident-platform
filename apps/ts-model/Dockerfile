# apps/ts-model/Dockerfile
FROM pytorch/torchserve:0.9.0-cpu
WORKDIR /home/model-server

# Copy config + handler + pre-built MAR (from CI step)
COPY config.properties /home/model-server/config.properties
COPY mar.properties    /home/model-server/mar.properties
COPY handlers/         /home/model-server/handlers/
COPY model-store/      /home/model-server/model-store/

# Runtime deps for the handler
RUN pip install --no-cache-dir sentence-transformers==2.2.2 transformers==4.36.2 \
 && apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

ENV EMBED_MODEL_NAME=sentence-transformers/all-MiniLM-L6-v2 \
    ANOMALY_THRESHOLD=0.7

EXPOSE 8082 8083 8084
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD curl -f http://127.0.0.1:8083/ping || exit 1

# Keep JSON-array on ONE line
CMD ["torchserve","--start","--model-store","/home/model-server/model-store","--models","log_anom=log_anom.mar","--ts-config","/home/model-server/config.properties","--foreground"]

# Production TorchServe image that builds MAR at container startup if missing.
FROM pytorch/torchserve:0.9.0-cpu

WORKDIR /home/model-server

# TorchServe config and handler code
COPY config.properties /home/model-server/config.properties
COPY mar.properties    /home/model-server/mar.properties
COPY handlers/         /home/model-server/handlers/

# Optional artifacts directory (may be empty; that's OK)
# If you don't have this folder in git, add an empty .gitkeep or leave this COPY out.
# If you leave it out, the start script will create it.
COPY model-artifacts/  /home/model-server/model-artifacts/

# Runtime deps for handler + model archiver for on-start MAR build
RUN pip install --no-cache-dir \
      sentence-transformers==2.2.2 \
      transformers==4.36.2 \
      torchserve==0.9.0 \
      torch-model-archiver==0.9.0 \
 && apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Startup script builds MAR if missing, then launches TorchServe
COPY start_ts.sh /home/model-server/start_ts.sh
RUN chmod +x /home/model-server/start_ts.sh

ENV EMBED_MODEL_NAME=sentence-transformers/all-MiniLM-L6-v2 \
    ANOMALY_THRESHOLD=0.7

EXPOSE 8082 8083 8084
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD curl -f http://127.0.0.1:8083/ping || exit 1

# Keep JSON-array CMD on one line
CMD ["/home/model-server/start_ts.sh"]

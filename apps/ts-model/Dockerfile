# ===== Stage 1: MAR builder (plain Python; no circular dependency)
FROM python:3.10-slim AS mar_builder
WORKDIR /build

# Tools needed only to build the .mar
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir \
      torchserve==0.9.0 \
      torch-model-archiver==0.9.0 \
      sentence-transformers==2.2.2 \
      transformers==4.36.2

# Copy only what's necessary for MAR build (keeps cache hot)
COPY handlers/ ./handlers/
COPY mar.properties ./mar.properties
COPY pack_model.sh ./pack_model.sh

# Ensure optional artifacts dir exists even if your repo doesn't have it
RUN mkdir -p model-artifacts

# Build MAR (verbose so CI logs show any failure)
RUN chmod +x pack_model.sh && bash -x ./pack_model.sh  # -> /build/model-store/log_anom.mar


# ===== Stage 2: TorchServe runtime
FROM pytorch/torchserve:0.9.0-cpu
WORKDIR /home/model-server

# TorchServe config & model
COPY config.properties /home/model-server/config.properties
COPY --from=mar_builder /build/model-store/ /home/model-server/model-store/
# (Optional) copy handler for introspection
COPY handlers/ /home/model-server/handlers/

# Runtime deps the handler needs
RUN pip install --no-cache-dir sentence-transformers==2.2.2 transformers==4.36.2 \
 && apt-get update && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

ENV EMBED_MODEL_NAME=sentence-transformers/all-MiniLM-L6-v2 \
    ANOMALY_THRESHOLD=0.7

EXPOSE 8082 8083 8084
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD curl -f http://127.0.0.1:8083/ping || exit 1

# Keep JSON-array CMD on one line
CMD ["torchserve","--start","--model-store","/home/model-server/model-store","--models","log_anom=log_anom.mar","--ts-config","/home/model-server/config.properties","--foreground"]

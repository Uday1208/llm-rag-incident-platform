# ===== Stage 1: build the MAR (cached unless handlers/artifacts change)
FROM pytorch/torchserve:0.9.0-cpu AS builder
WORKDIR /build

# Tools needed to build the .mar
# torch_model_archiver sometimes isn't on PATH in slim images -> use module form
RUN pip install --no-cache-dir \
      torchserve==0.9.0 \
      torch-model-archiver==0.9.0 \
      sentence-transformers==2.2.2 \
      transformers==4.36.2

# Copy only what’s needed to build the MAR (improves cache hit)
COPY handlers/ ./handlers/
COPY mar.properties ./mar.properties
COPY pack_model.sh ./pack_model.sh

# Ensure optional artifacts dir exists even if empty (so the script doesn't fail)
RUN mkdir -p model-artifacts

# Build the MAR; make failures visible in build logs
RUN chmod +x pack_model.sh && bash -x ./pack_model.sh

COPY config.properties /home/model-server/config.properties
COPY --from=builder /build/model-store/ /home/model-server/model-store/
COPY handlers/ /home/model-server/handlers/

# Runtime deps for handler
RUN pip install --no-cache-dir sentence-transformers==2.2.2 transformers==4.36.2 \
 && apt-get update && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

ENV EMBED_MODEL_NAME=sentence-transformers/all-MiniLM-L6-v2 \
    ANOMALY_THRESHOLD=0.7

EXPOSE 8082 8083 8084
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD curl -f http://127.0.0.1:8083/ping || exit 1

CMD ["torchserve","--start","--model-store","/home/model-server/model-store","--models","log_anom=log_anom.mar","--ts-config","/home/model-server/config.properties","--foreground"]

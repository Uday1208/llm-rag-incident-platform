# apps/ts-model/Dockerfile
FROM pytorch/torchserve:0.9.0-cpu
WORKDIR /home/model-server

# Copy config + handler + pre-built MAR (both produced by CI step)
COPY config.properties /home/model-server/config.properties
COPY mar.properties    /home/model-server/mar.properties
COPY handlers/         /home/model-server/handlers/
COPY model-store/      /home/model-server/model-store/

# Copy the pre-fetched SentenceTransformers cache produced in CI
# (If the folder doesn't exist yet, keep an empty model-cache/.gitkeep in repo)
COPY model-cache/      /home/model-server/model-cache/

# IMPORTANT: install deps into TorchServe's interpreter so workers can import them
RUN /home/venv/bin/pip install --no-cache-dir --only-binary=:all: sentence-transformers==3.0.1 transformers==4.41.2 tokenizers==0.19.1 huggingface_hub==0.23.2

# Tell sentence-transformers where to find the cache we copied
ENV SENTENCE_TRANSFORMERS_HOME=/home/model-server/model-cache \
    EMBED_MODEL_NAME=sentence-transformers/all-MiniLM-L6-v2 \
    ANOMALY_THRESHOLD=0.7

EXPOSE 8082 8083 8084

# JSON-array CMD on one line
CMD ["torchserve","--start","--model-store","/home/model-server/model-store","--models","log_anom=log_anom.mar","--ts-config","/home/model-server/config.properties","--foreground"]

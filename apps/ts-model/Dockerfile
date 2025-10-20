# apps/ts-model/Dockerfile
FROM pytorch/torchserve:0.9.0-cpu
WORKDIR /home/model-server

# Copy config + handler + the MAR that your CI built into apps/ts-model/model-store/
COPY config.properties /home/model-server/config.properties
COPY mar.properties    /home/model-server/mar.properties
COPY handlers/         /home/model-server/handlers/
COPY model-store/      /home/model-server/model-store/

# Python deps needed by the handler (no apt, no curl)
RUN pip install --no-cache-dir sentence-transformers==2.2.2 transformers==4.36.2

ENV EMBED_MODEL_NAME=sentence-transformers/all-MiniLM-L6-v2 \
    ANOMALY_THRESHOLD=0.7

EXPOSE 8082 8083 8084

# IMPORTANT: keep JSON-array on one line
CMD ["torchserve","--start","--model-store","/home/model-server/model-store","--models","log_anom=log_anom.mar","--ts-config","/home/model-server/config.properties","--foreground"]

# apps/ts-model/Dockerfile
FROM pytorch/torchserve:0.9.0-cpu
WORKDIR /home/model-server

# Copy config + handler + pre-built MAR (built in CI before this step)
COPY config.properties /home/model-server/config.properties
COPY mar.properties    /home/model-server/mar.properties
COPY handlers/         /home/model-server/handlers/
COPY model-store/      /home/model-server/model-store/

# IMPORTANT: install deps into TorchServe's own venv (/home/venv)
# Avoid apt entirely (ACA base sometimes blocks apt sources)
RUN /home/venv/bin/pip install --no-cache-dir sentence-transformers==2.2.2 transformers==4.36.2

# Pre-download the Sentence-Transformers model at build time
# so the worker doesn't try to fetch from the internet at runtime.
# If you change EMBED_MODEL_NAME later, update the model here too.
ENV SENTENCE_TRANSFORMERS_HOME=/home/model-server/.cache/sentencetransformers
RUN /home/venv/bin/python - <<'PY'
from sentence_transformers import SentenceTransformer
# Preload the default model used by the handler
SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
print("Pre-downloaded all-MiniLM-L6-v2 OK")
PY

# Runtime config
ENV EMBED_MODEL_NAME=sentence-transformers/all-MiniLM-L6-v2 \
    ANOMALY_THRESHOLD=0.7

EXPOSE 8082 8083 8084

# ONE-LINE JSON array (TorchServe runs in foreground so ACA can monitor it)
CMD ["torchserve","--start","--model-store","/home/model-server/model-store","--models","log_anom=log_anom.mar","--ts-config","/home/model-server/config.properties","--foreground"]

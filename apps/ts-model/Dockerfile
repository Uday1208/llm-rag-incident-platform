# apps/ts-model/Dockerfile
FROM pytorch/torchserve:0.9.0-cpu
WORKDIR /home/model-server

# Copy config + handler + pre-built MAR (built in CI before this step)
COPY config.properties /home/model-server/config.properties
COPY mar.properties    /home/model-server/mar.properties
COPY handlers/         /home/model-server/handlers/
COPY model-store/      /home/model-server/model-store/

# Runtime Python deps for the handler
RUN pip install --no-cache-dir sentence-transformers==2.2.2 transformers==4.36.2

ENV EMBED_MODEL_NAME=sentence-transformers/all-MiniLM-L6-v2 \
    ANOMALY_THRESHOLD=0.7

EXPOSE 8082 8083 8084

# Use Python for healthcheck (avoid apt/curl entirely)
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
  CMD python - <<'PY' || exit 1
import urllib.request, sys
try:
    with urllib.request.urlopen("http://127.0.0.1:8083/ping", timeout=5) as r:
        b = r.read()
    # TorchServe returns 'Healthy' or similar on /ping; accept any 2xx
    sys.exit(0)
except Exception:
    sys.exit(1)
PY

# Keep JSON-array CMD on one line
CMD ["torchserve","--start","--model-store","/home/model-server/model-store","--models","log_anom=log_anom.mar","--ts-config","/home/model-server/config.properties","--foreground"]

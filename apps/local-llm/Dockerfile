# Small, stable base
FROM ollama/ollama:0.3.12

# Reduce surprise restarts / tune memory usage a bit
ENV OLLAMA_KEEP_ALIVE=5m \
    OLLAMA_NUM_PARALLEL=1 \
    OLLAMA_MAX_LOADED_MODELS=1

# Which model to pull on startup (override via env)
ENV MODEL=llama3.2:1b

# Copy startup helper
COPY start.sh /start.sh
RUN chmod +x /start.sh

EXPOSE 11434
ENTRYPOINT ["/start.sh"]

#!/usr/bin/env bash
set -euo pipefail

# Start the Ollama HTTP server
ollama serve &

# Wait for server to accept requests
for i in {1..90}; do
  if curl -sf http://127.0.0.1:11434/api/tags > /dev/null; then
    break
  fi
  sleep 1
done

# Pull the configured model (if already cached, this is quick)
echo "[local-llm] pulling model: ${MODEL}"
ollama pull "${MODEL}" || true

echo "[local-llm] ready on :11434 with model=${MODEL}"
# Keep server in foreground
wait -n

#!/usr/bin/env bash
set -euo pipefail

MS_HOME="/home/model-server"
MODEL_STORE="$MS_HOME/model-store"
ART_DIR="$MS_HOME/model-artifacts"
HANDLER="$MS_HOME/handlers/log_anom_handler.py"
MAR_PROPS="$MS_HOME/mar.properties"

echo "[start_ts] Starting TorchServe bootstrap..."
mkdir -p "$MODEL_STORE" "$ART_DIR"

# Build MAR if missing (handler-only if model_head.pt not present)
if [[ ! -f "$MODEL_STORE/log_anom.mar" ]]; then
  echo "[start_ts] log_anom.mar not found. Building with torch_model_archiver..."
  SER_FILE=""
  if [[ -f "$ART_DIR/model_head.pt" ]]; then
    SER_FILE="--serialized-file $ART_DIR/model_head.pt"
    echo "[start_ts] Including model_head.pt in MAR."
  else
    echo "[start_ts] No model_head.pt found. Building handler-only MAR (heuristic fallback will be used)."
  fi

  python -m torch_model_archiver \
    --model-name log_anom \
    --version 1.0 \
    --handler "$HANDLER" \
    $SER_FILE \
    --extra-files "$MAR_PROPS" \
    --export-path "$MODEL_STORE" \
    --force

  echo "[start_ts] Built $MODEL_STORE/log_anom.mar"
else
  echo "[start_ts] Found existing $MODEL_STORE/log_anom.mar — skipping build."
fi

echo "[start_ts] Launching TorchServe..."
exec torchserve --start \
  --model-store "$MODEL_STORE" \
  --models "log_anom=log_anom.mar" \
  --ts-config "$MS_HOME/config.properties" \
  --foreground

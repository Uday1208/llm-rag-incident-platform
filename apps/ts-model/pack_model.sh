#!/usr/bin/env bash
set -euo pipefail

THIS_DIR="$(cd "$(dirname "$0")" && pwd)"
HANDLER="$THIS_DIR/handlers/log_anom_handler.py"
MODEL_STORE="$THIS_DIR/model-store"
ART_DIR="$THIS_DIR/model-artifacts"
EXTRA="--extra-files $THIS_DIR/mar.properties"

mkdir -p "$MODEL_STORE" "$ART_DIR"

SER_FILE=""
if [[ -f "$ART_DIR/model_head.pt" ]]; then
  SER_FILE="--serialized-file $ART_DIR/model_head.pt"
fi

echo "== pack_model.sh inputs =="
echo "HANDLER:      $HANDLER"
echo "MODEL_STORE:  $MODEL_STORE"
echo "ART_DIR:      $ART_DIR"
echo "SER_FILE:     ${SER_FILE:-<none>}"
echo "EXTRA:        $EXTRA"
echo "Listing dirs:"
ls -la "$THIS_DIR"
ls -la "$MODEL_STORE" || true
ls -la "$ART_DIR" || true

# Use module form to avoid PATH issues with torch-model-archiver
python -m torch_model_archiver \
  --model-name log_anom \
  --version 1.0 \
  --handler "$HANDLER" \
  $SER_FILE \
  $EXTRA \
  --export-path "$MODEL_STORE" \
  --force

echo "Built $MODEL_STORE/log_anom.mar"

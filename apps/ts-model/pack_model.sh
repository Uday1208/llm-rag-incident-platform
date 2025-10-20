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
  echo ">> Including model_head.pt in MAR"
else
  echo ">> No model_head.pt found; building handler-only MAR"
fi

echo "== pack_model.sh inputs =="
echo "HANDLER:     $HANDLER"
echo "MODEL_STORE: $MODEL_STORE"
echo "ART_DIR:     $ART_DIR"
echo "SER_FILE:    ${SER_FILE:-<none>}"
echo "EXTRA:       $EXTRA"

# Use module form to avoid PATH issues
python -m torch_model_archiver \
  --model-name log_anom \
  --version 1.0 \
  --handler "$HANDLER" \
  $SER_FILE \
  $EXTRA \
  --export-path "$MODEL_STORE" \
  --force

echo ">> Built $MODEL_STORE/log_anom.mar"

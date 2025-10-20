#!/usr/bin/env bash
set -euo pipefail

THIS_DIR="$(cd "$(dirname "$0")" && pwd)"
HANDLER="$THIS_DIR/handlers/log_anom_handler.py"
MODEL_STORE="$THIS_DIR/model-store"
ART_DIR="$THIS_DIR/model-artifacts"

mkdir -p "$MODEL_STORE"
mkdir -p "$ART_DIR"    # <-- ensure it exists even if empty

SER_FILE=""
EXTRA="--extra-files $THIS_DIR/mar.properties"

if [[ -f "$ART_DIR/model_head.pt" ]]; then
  SER_FILE="--serialized-file $ART_DIR/model_head.pt"
fi

torch-model-archiver \
  --model-name log_anom \
  --version 1.0 \
  --handler "$HANDLER" \
  $SER_FILE \
  $EXTRA \
  --export-path "$MODEL_STORE" \
  --force

echo "Built $MODEL_STORE/log_anom.mar"

"""
File: log_anom_handler.py
Purpose: TorchServe handler for log anomaly scoring. Real model = Sentence-Transformers embeddings + TorchScript MLP head.
I/O:
  - Input JSON: {"lines": ["text line 1", "text line 2", ...]}
  - Output JSON: {"scores": [0.01, 0.73, ...]} where 0..1 is anomaly probability.
Notes:
  - If model_head.pt is not present, falls back to a safe heuristic (no downtime).
  - Batch-friendly and CPU-friendly by default.
"""

import os
import json
import torch
from ts.torch_handler.base_handler import BaseHandler

# Optional: faster startup if you pin via env
DEFAULT_EMBED_MODEL = os.getenv("EMBED_MODEL_NAME", "sentence-transformers/all-MiniLM-L6-v2")
THRESHOLD = float(os.getenv("ANOMALY_THRESHOLD", "0.7"))  # informational; we return scores, not labels

class LogAnomHandler(BaseHandler):
    """TorchServe custom handler for log anomaly scoring."""

    def initialize(self, ctx):
        """Load embedding model + TorchScript head if present; set device; read properties."""
        self.manifest = ctx.manifest
        sys_props = ctx.system_properties
        self.model_dir = sys_props.get("model_dir")  # where serialized-file (model_head.pt) lives
        self.device = torch.device("cuda:0" if torch.cuda.is_available() else "cpu")

        # Load embedding model (CPU ok)
        from sentence_transformers import SentenceTransformer
        self.embed = SentenceTransformer(DEFAULT_EMBED_MODEL)

        # Try to load TorchScript head
        head_path = os.path.join(self.model_dir, "model_head.pt")
        if os.path.exists(head_path):
            self.head = torch.jit.load(head_path, map_location=self.device)
            self.head.eval()
        else:
            self.head = None  # fallback path (heuristic)

    def preprocess(self, data):
        """Parse HTTP payload -> list[str] log lines."""
        if not data:
            return []
        # TorchServe batches requests; we only look at first item body by convention
        body = data[0].get("body")
        if isinstance(body, (bytes, bytearray)):
            body = body.decode("utf-8")
        if isinstance(body, str):
            body = json.loads(body)
        lines = body.get("lines", [])
        lines = [str(x) for x in lines]
        return lines

    @torch.no_grad()
    def inference(self, inputs):
        """Compute anomaly scores for each log line."""
        if not inputs:
            return []

        # Embeddings (L2-normalized float32)
        embs = self.embed.encode(inputs, normalize_embeddings=True, convert_to_numpy=True)
        embs = torch.from_numpy(embs).to(self.device)  # [B, D], dtype float32

        if self.head is not None:
            logits = self.head(embs)                    # [B] or [B,1]
            if logits.dim() == 2 and logits.size(1) == 1:
                logits = logits[:, 0]
            probs = torch.sigmoid(logits).float().cpu().tolist()
            return [float(p) for p in probs]

        # Fallback heuristic (no head present): rule-based score
        scores = []
        for line in inputs:
            l = line.lower()
            s = 0.0
            if any(k in l for k in ["error", "exception", "timeout", "oom", "traceback", "failed", "crash"]):
                s += 0.6
            if any(ch.isdigit() for ch in l):
                s += 0.2
            if "http" in l or "grpc" in l or "kafka" in l:
                s += 0.1
            s = min(1.0, s)
            scores.append(float(s))
        return scores

    def postprocess(self, outputs):
        """Return TorchServe-friendly JSON."""
        return [json.dumps({"scores": outputs, "threshold": THRESHOLD})]

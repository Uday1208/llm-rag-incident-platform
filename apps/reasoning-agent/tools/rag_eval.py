#!/usr/bin/env python3
"""
Offline RAG evaluation: Recall@k, MRR, and phrase hits for the final answer.
Env:
  BASE_AGENT  : https://<agent-fqdn>
  BASE_WORKER : https://<worker-fqdn>
CLI:
  python -m tools.rag_eval --suite tools/rag_eval.yaml --k 5 --out eval.tsv
"""
import os, sys, json, time, argparse
from typing import List, Dict, Any, Optional
import httpx
import yaml

def _bool_hit(answer: str, phrases: List[str]) -> tuple[bool, bool]:
    """Return (exact_hit, partial_hit) for phrase list in answer (case-insensitive)."""
    if not phrases:
        return (False, False)
    a = (answer or "").lower()
    want = [p.lower() for p in phrases if p]
    exact = all(p in a for p in want)
    partial = any(p in a for p in want)
    return (exact, partial)

def _mrr(ranked_ids: List[str], gold: List[str]) -> float:
    """Compute MRR for ranked_ids given gold ids (first relevant position)."""
    if not ranked_ids or not gold:
        return 0.0
    gold_set = set(gold)
    for i, rid in enumerate(ranked_ids, start=1):
        if rid in gold_set:
            return 1.0 / i
    return 0.0

def _recall_at_k(ranked_ids: List[str], gold: List[str], k: int) -> float:
    """Recall@k for the retrieved list vs gold ids."""
    if not ranked_ids or not gold:
        return 0.0
    top = set(ranked_ids[:k])
    g = set(gold)
    if not g:
        return 0.0
    return len(top & g) / len(g)

def run_suite(suite_path: str, k: int, timeout: float, out_tsv: Optional[str]) -> None:
    base_agent = os.environ.get("BASE_AGENT", "").rstrip("/")
    base_worker = os.environ.get("BASE_WORKER", "").rstrip("/")
    if not base_agent or not base_worker:
        print("ERROR: set BASE_AGENT and BASE_WORKER", file=sys.stderr)
        sys.exit(2)

    with open(suite_path, "r", encoding="utf-8") as f:
        cases = yaml.safe_load(f) or {}
    items: List[Dict[str, Any]] = cases.get("cases", [])

    rows = []
    tot_mrr = 0.0
    tot_rk = 0.0
    tot_exact = 0
    tot_partial = 0

    with httpx.Client(timeout=timeout) as http:
        for idx, c in enumerate(items, start=1):
            q = c.get("query", "")
            exp_ids = c.get("expected_doc_ids") or []
            exp_phrases = c.get("expected_answer_phrases") or []

            # 1) embed
            r = http.post(f"{base_worker}/internal/embed", json={"texts":[q]})
            r.raise_for_status()
            vec = r.json()["vectors"][0]

            # 2) search
            r = http.post(f"{base_worker}/internal/search",
                          json={"embedding": vec, "top_k": k})
            r.raise_for_status()
            hits = r.json().get("results", [])
            ranked_ids = [h.get("id") for h in hits if h.get("id")]

            # 3) reason (ask agent)
            payload = {"query": q, "max_suggestions": 3, "format": "json"}
            r = http.post(f"{base_agent}/v1/reason", json=payload)
            # Fall back to text if your agent sometimes returns non-JSON
            answer = ""
            try:
                r.raise_for_status()
                j = r.json()
                # accept either {"answer": "..."} or raw string body
                answer = j.get("answer") if isinstance(j, dict) else r.text
            except Exception:
                answer = r.text

            # 4) metrics
            mrr = _mrr(ranked_ids, exp_ids)
            rk = _recall_at_k(ranked_ids, exp_ids, k)
            exact, partial = _bool_hit(answer, exp_phrases)

            tot_mrr += mrr
            tot_rk += rk
            tot_exact += 1 if exact else 0
            tot_partial += 1 if partial else 0

            rows.append({
                "i": idx,
                "query": q,
                "mrr": round(mrr, 4),
                f"recall@{k}": round(rk, 4),
                "exact": int(exact),
                "partial": int(partial),
                "top_ids": ",".join(ranked_ids[:k]),
            })

    # Print summary
    n = max(len(items), 1)
    print(f"Cases={len(items)}  MRR={tot_mrr/n:.4f}  Recall@{k}={tot_rk/n:.4f}  "
          f"Exact={tot_exact}/{n}  Partial={tot_partial}/{n}")

    if out_tsv:
        with open(out_tsv, "w", encoding="utf-8") as f:
            f.write("i\tmrr\trecall\texact\tpartial\ttop_ids\tquery\n")
            for r in rows:
                f.write(f"{r['i']}\t{r['mrr']}\t{r[f'recall@{k}']}\t{r['exact']}\t{r['partial']}\t{r['top_ids']}\t{r['query']}\n")
#!/usr/bin/env python3
"""
Offline RAG evaluation: Recall@k, MRR, and phrase hits for the final answer.
Env:
  BASE_AGENT  : https://<agent-fqdn>
  BASE_WORKER : https://<worker-fqdn>
CLI:
  python -m tools.rag_eval --suite tools/rag_eval.yaml --k 5 --out eval.tsv
"""
import os, sys, json, time, argparse
from typing import List, Dict, Any, Optional
import httpx
import yaml

def _bool_hit(answer: str, phrases: List[str]) -> tuple[bool, bool]:
    """Return (exact_hit, partial_hit) for phrase list in answer (case-insensitive)."""
    if not phrases:
        return (False, False)
    a = (answer or "").lower()
    want = [p.lower() for p in phrases if p]
    exact = all(p in a for p in want)
    partial = any(p in a for p in want)
    return (exact, partial)

def _mrr(ranked_ids: List[str], gold: List[str]) -> float:
    """Compute MRR for ranked_ids given gold ids (first relevant position)."""
    if not ranked_ids or not gold:
        return 0.0
    gold_set = set(gold)
    for i, rid in enumerate(ranked_ids, start=1):
        if rid in gold_set:
            return 1.0 / i
    return 0.0

def _recall_at_k(ranked_ids: List[str], gold: List[str], k: int) -> float:
    """Recall@k for the retrieved list vs gold ids."""
    if not ranked_ids or not gold:
        return 0.0
    top = set(ranked_ids[:k])
    g = set(gold)
    if not g:
        return 0.0
    return len(top & g) / len(g)

def run_suite(suite_path: str, k: int, timeout: float, out_tsv: Optional[str]) -> None:
    base_agent = os.environ.get("BASE_AGENT", "").rstrip("/")
    base_worker = os.environ.get("BASE_WORKER", "").rstrip("/")
    if not base_agent or not base_worker:
        print("ERROR: set BASE_AGENT and BASE_WORKER", file=sys.stderr)
        sys.exit(2)

    with open(suite_path, "r", encoding="utf-8") as f:
        cases = yaml.safe_load(f) or {}
    items: List[Dict[str, Any]] = cases.get("cases", [])

    rows = []
    tot_mrr = 0.0
    tot_rk = 0.0
    tot_exact = 0
    tot_partial = 0

    with httpx.Client(timeout=timeout) as http:
        for idx, c in enumerate(items, start=1):
            q = c.get("query", "")
            exp_ids = c.get("expected_doc_ids") or []
            exp_phrases = c.get("expected_answer_phrases") or []

            # 1) embed
            r = http.post(f"{base_worker}/internal/embed", json={"texts":[q]})
            r.raise_for_status()
            vec = r.json()["vectors"][0]

            # 2) search
            r = http.post(f"{base_worker}/internal/search",
                          json={"embedding": vec, "top_k": k})
            r.raise_for_status()
            hits = r.json().get("results", [])
            ranked_ids = [h.get("id") for h in hits if h.get("id")]

            # 3) reason (ask agent)
            payload = {"query": q, "max_suggestions": 3, "format": "json"}
            r = http.post(f"{base_agent}/v1/reason", json=payload)
            # Fall back to text if your agent sometimes returns non-JSON
            answer = ""
            try:
                r.raise_for_status()
                j = r.json()
                # accept either {"answer": "..."} or raw string body
                answer = j.get("answer") if isinstance(j, dict) else r.text
            except Exception:
                answer = r.text

            # 4) metrics
            mrr = _mrr(ranked_ids, exp_ids)
            rk = _recall_at_k(ranked_ids, exp_ids, k)
            exact, partial = _bool_hit(answer, exp_phrases)

            tot_mrr += mrr
            tot_rk += rk
            tot_exact += 1 if exact else 0
            tot_partial += 1 if partial else 0

            rows.append({
                "i": idx,
                "query": q,
                "mrr": round(mrr, 4),
                f"recall@{k}": round(rk, 4),
                "exact": int(exact),
                "partial": int(partial),
                "top_ids": ",".join(ranked_ids[:k]),
            })

    # Print summary
    n = max(len(items), 1)
    print(f"Cases={len(items)}  MRR={tot_mrr/n:.4f}  Recall@{k}={tot_rk/n:.4f}  "
          f"Exact={tot_exact}/{n}  Partial={tot_partial}/{n}")

    if out_tsv:
        with open(out_tsv, "w", encoding="utf-8") as f:
            f.write("i\tmrr\trecall\texact\tpartial\ttop_ids\tquery\n")
            for r in rows:
                f.write(f"{r['i']}\t{r['mrr']}\t{r[f'recall@{k}']}\t{r['exact']}\t{r['partial']}\t{r['top_ids']}\t{r['query']}\n")

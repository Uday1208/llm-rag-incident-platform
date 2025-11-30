# tools/rag_eval.py
# One-liner: Offline RAG eval harness that embeds queries, searches via rag-worker, and writes per-case metrics to TSV.

import os, sys, json, time, argparse, csv
from typing import List, Dict, Any
import yaml

# Reuse your existing worker client helpers
from services.retrieval import embed_query, search_by_embedding  # one-liner: thin HTTP wrappers around /internal/embed and /internal/search

def _contains_any_phrase(text: str, phrases: List[str]) -> bool:
    """One-liner: Case-insensitive check whether any phrase occurs in text."""
    t = (text or "").lower()
    return any((p or "").lower() in t for p in phrases)

def _first_hit_rank(hits: List[Dict[str, Any]], phrases: List[str]) -> int:
    """One-liner: Return 1-based rank of first doc whose content/title contains any expected phrase, else -1."""
    for i, h in enumerate(hits, 1):
        blob = " ".join(
            [
                str(h.get("title") or ""),
                str(h.get("content") or ""),
                str(h.get("source") or ""),
            ]
        )
        if _contains_any_phrase(blob, phrases):
            return i
    return -1

def run_eval(suite_path: str, top_k: int) -> List[Dict[str, Any]]:
    """One-liner: Load YAML cases, run embed+search for each, compute simple retrieval metrics."""
    with open(suite_path, "r", encoding="utf-8") as f:
        suite = yaml.safe_load(f)

    cases = suite.get("cases") or []
    rows: List[Dict[str, Any]] = []

    for idx, case in enumerate(cases, 1):
        cid = case.get("id") or f"case_{idx}"
        q = case["query"]
        expect = case.get("expect_phrases", []) or []
        t0 = time.time()

        # embed + search
        try:
            emb = embed_query(q)  # returns List[float]
            hits = search_by_embedding(emb, top_k=top_k)  # returns List[dict] with id, score, content, title, source
        except Exception as e:
            rows.append({
                "id": cid,
                "query": q,
                "error": f"{type(e).__name__}: {e}",
                "elapsed_ms": int((time.time() - t0) * 1000),
                "top_k": top_k,
                "k": 0,
                "first_hit_rank": -1,
                "any_phrase_hit": 0,
            })
            continue

        elapsed = int((time.time() - t0) * 1000)

        # metrics
        k = len(hits)
        first_rank = _first_hit_rank(hits, expect) if expect else -1
        any_phrase_hit = 1 if (first_rank != -1) else 0

        rows.append({
            "id": cid,
            "query": q,
            "elapsed_ms": elapsed,
            "top_k": top_k,
            "k": k,
            "first_hit_rank": first_rank,     # 1..k or -1 if none
            "any_phrase_hit": any_phrase_hit, # 1 if some expected phrase is present in retrieved docs
            # keep quick debug columns short
            "debug_top1_title": (hits[0].get("title") if hits else None),
            "debug_top1_score": (hits[0].get("score") if hits else None),
        })

    return rows

def write_tsv(rows: List[Dict[str, Any]], out_path: str) -> None:
    """One-liner: Write eval rows to a TSV with stable columns."""
    fieldnames = [
        "id", "query", "elapsed_ms", "top_k", "k",
        "first_hit_rank", "any_phrase_hit", "debug_top1_title", "debug_top1_score", "error"
    ]
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t", extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)

def main() -> int:
    """One-liner: CLI entrypoint — parse args, run eval, write TSV, and print a short summary."""
    ap = argparse.ArgumentParser(description="RAG retrieval eval harness")
    ap.add_argument("--suite", required=True, help="Path to YAML with .cases[].query and .cases[].expect_phrases")
    ap.add_argument("--k", type=int, default=5, help="Top-k docs to retrieve")
    ap.add_argument("--out", required=True, help="Output TSV path")
    args = ap.parse_args()

    rows = run_eval(args.suite, args.k)
    write_tsv(rows, args.out)

    total = len(rows)
    ok = sum(1 for r in rows if r.get("any_phrase_hit") == 1)
    print(json.dumps({"cases": total, "any_phrase_hit@k": ok, "hit_rate": (ok / total if total else 0.0)}, indent=2))
    print(f"[ok] wrote {args.out}")
    return 0

if __name__ == "__main__":
    sys.exit(main())

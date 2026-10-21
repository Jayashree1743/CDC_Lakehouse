#!/usr/bin/env python3
"""Evaluate retrieval quality for the CDC Lakehouse search API.

This is deliberately separate from the latency benchmark. It answers:

  - Did search retrieve the labelled relevant documents?   (recall)
  - Are the top results relevant?                          (precision)
  - How high is the first relevant result?                 (MRR)
  - Is the ranking ordered well?                           (nDCG)

Baseline:
    python eval/evaluate.py

Optional cross-encoder reranker:
    python eval/evaluate.py --rerank

The API must be running. The default endpoint is http://localhost:8000/search.
The default key is the demo key configured in docker-compose.yml.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import urllib.error
import urllib.request
from pathlib import Path
from statistics import mean
from typing import Any, Iterable


def post_json(url: str, payload: dict[str, Any], api_key: str, timeout: float) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "x-api-key": api_key,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(f"API returned HTTP {exc.code}: {body}") from exc
    except urllib.error.URLError as exc:
        raise SystemExit(
            f"Could not reach {url}: {exc.reason}\n"
            "Start the stack with ./run_all.sh first."
        ) from exc


def load_queries(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise SystemExit(f"{path}:{line_number}: invalid JSON: {exc}") from exc
            if not row.get("query"):
                raise SystemExit(f"{path}:{line_number}: missing query")
            if not row.get("relevant_document_ids"):
                raise SystemExit(f"{path}:{line_number}: missing relevant_document_ids")
            row["relevant_document_ids"] = [str(x) for x in row["relevant_document_ids"]]
            rows.append(row)
    if not rows:
        raise SystemExit(f"{path}: no queries found")
    return rows


def dedupe_by_document(hits: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    """The API returns chunks; for document-level metrics keep the best chunk."""
    seen: set[str] = set()
    unique: list[dict[str, Any]] = []
    for hit in hits:
        document_id = hit.get("document_id")
        if document_id is None:
            continue
        document_id = str(document_id)
        if document_id in seen:
            continue
        seen.add(document_id)
        unique.append(hit)
    return unique


def recall_at_k(retrieved: list[str], relevant: set[str], k: int) -> float | None:
    if not relevant:
        return None
    return len(set(retrieved[:k]) & relevant) / len(relevant)


def precision_at_k(retrieved: list[str], relevant: set[str], k: int) -> float:
    if k <= 0:
        return 0.0
    return len(set(retrieved[:k]) & relevant) / k


def mrr_at_k(retrieved: list[str], relevant: set[str], k: int) -> float:
    for rank, document_id in enumerate(retrieved[:k], start=1):
        if document_id in relevant:
            return 1.0 / rank
    return 0.0


def ndcg_at_k(retrieved: list[str], relevant: set[str], k: int) -> float | None:
    if not relevant or k <= 0:
        return None
    dcg = sum(
        1.0 / math.log2(rank + 1)
        for rank, document_id in enumerate(retrieved[:k], start=1)
        if document_id in relevant
    )
    ideal_hits = min(len(relevant), k)
    idcg = sum(1.0 / math.log2(rank + 1) for rank in range(1, ideal_hits + 1))
    return dcg / idcg if idcg else None


def rerank_hits(
    query: str,
    hits: list[dict[str, Any]],
    model_name: str,
) -> list[dict[str, Any]]:
    try:
        from fastembed.rerank.cross_encoder import TextCrossEncoder
    except ImportError as exc:
        raise SystemExit(
            "Reranking requires fastembed. Run this script inside the API image "
            "or install fastembed in the current environment."
        ) from exc

    reranker = TextCrossEncoder(model_name)
    documents = [hit.get("text", "") for hit in hits]
    scores = list(reranker.rerank(query, documents))
    ranked = sorted(zip(scores, hits), key=lambda pair: pair[0], reverse=True)
    return [hit for _, hit in ranked]


def parse_ks(value: str) -> list[int]:
    try:
        ks = sorted({int(part.strip()) for part in value.split(",") if part.strip()})
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--k must be comma-separated integers") from exc
    if not ks or any(k <= 0 for k in ks):
        raise argparse.ArgumentTypeError("--k values must be positive")
    return ks


def aggregate(values: list[float | None]) -> float | None:
    present = [value for value in values if value is not None]
    return mean(present) if present else None


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate retrieval quality")
    parser.add_argument("--api", default="http://localhost:8000/search", help="Search endpoint")
    parser.add_argument("--api-key", default="demo-key", help="x-api-key header")
    parser.add_argument("--queries", default="eval/queries.jsonl", help="JSONL labelled queries")
    parser.add_argument("--retrieve-k", type=int, default=20, help="Chunks to retrieve before ranking")
    parser.add_argument("--k", type=parse_ks, default=parse_ks("5,10"), help="Metrics@k, e.g. 5,10")
    parser.add_argument("--min-score", type=float, default=0.0, help="Qdrant score threshold")
    parser.add_argument("--timeout", type=float, default=60.0, help="Request timeout in seconds")
    parser.add_argument("--rerank", action="store_true", help="Apply a cross-encoder reranker")
    parser.add_argument(
        "--rerank-model",
        default="jinaai/jina-reranker-v1-turbo-en",
        help="fastembed cross-encoder model",
    )
    parser.add_argument("--output", help="Write per-query and aggregate results to JSON")
    args = parser.parse_args()

    query_path = Path(args.queries)
    queries = load_queries(query_path)
    ks = args.k
    rows: list[dict[str, Any]] = []

    for index, row in enumerate(queries, start=1):
        payload = {
            "query": row["query"],
            "top_k": args.retrieve_k,
            "min_score": args.min_score,
        }
        if row.get("customer_id") is not None:
            payload["customer_id"] = row["customer_id"]

        response = post_json(args.api, payload, args.api_key, args.timeout)
        hits = dedupe_by_document(response.get("hits", []))

        if args.rerank:
            hits = rerank_hits(row["query"], hits, args.rerank_model)

        retrieved = [str(hit["document_id"]) for hit in hits]
        relevant = set(row["relevant_document_ids"])

        metrics: dict[str, Any] = {}
        for k in ks:
            metrics[f"recall@{k}"] = recall_at_k(retrieved, relevant, k)
            metrics[f"precision@{k}"] = precision_at_k(retrieved, relevant, k)
            metrics[f"mrr@{k}"] = mrr_at_k(retrieved, relevant, k)
            metrics[f"ndcg@{k}"] = ndcg_at_k(retrieved, relevant, k)

        rows.append(
            {
                "query": row["query"],
                "relevant_document_ids": sorted(relevant),
                "retrieved_document_ids": retrieved,
                "metrics": metrics,
                "top_score": hits[0]["score"] if hits else None,
            }
        )
        print(f"[{index:>3}/{len(queries)}] {row['query']}")

    aggregates: dict[str, float | None] = {}
    for key in rows[0]["metrics"]:
        aggregates[key] = aggregate([row["metrics"][key] for row in rows])

    print("\n=== Retrieval evaluation ===")
    print(f"queries: {len(rows)}")
    print(f"mode:    {'vector + reranker' if args.rerank else 'vector only'}")
    print(f"retrieve_k: {args.retrieve_k}")
    for key, value in aggregates.items():
        if value is not None:
            print(f"{key:>12}: {value:.4f}")

    if args.output:
        output = {
            "config": {
                "api": args.api,
                "queries": str(query_path),
                "retrieve_k": args.retrieve_k,
                "k": ks,
                "min_score": args.min_score,
                "rerank": args.rerank,
                "rerank_model": args.rerank_model if args.rerank else None,
            },
            "aggregates": aggregates,
            "rows": rows,
        }
        Path(args.output).write_text(json.dumps(output, indent=2), encoding="utf-8")
        print(f"\nwrote {args.output}")


if __name__ == "__main__":
    main()

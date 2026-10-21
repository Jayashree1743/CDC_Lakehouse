# Retrieval evaluation

This folder contains a small, labelled retrieval set and a script that measures
document-level ranking quality against the running `/search` endpoint.

It is intentionally separate from the latency benchmark. Latency asks "how fast
is search?"; this asks "did search put the right document first?"

## Metrics

For each query, the script deduplicates chunks by `document_id` and computes:

| Metric | Meaning |
|---|---|
| `recall@k` | Fraction of labelled relevant documents in the top `k` |
| `precision@k` | Fraction of the top `k` results that are relevant |
| `mrr@k` | Reciprocal rank of the first relevant result |
| `ndcg@k` | Ranking quality with binary relevance |

## Run the vector-only baseline

The stack must be running (`./run_all.sh`). The script uses only the Python
standard library, so it can run on the host:

```bash
python3 eval/evaluate.py
```

Useful options:

```bash
python3 eval/evaluate.py --k 5,10 --retrieve-k 20
python3 eval/evaluate.py --output /tmp/retrieval.json
python3 eval/evaluate.py --api http://localhost:8000/search --api-key demo-key
```

## Run with a cross-encoder reranker

Reranking needs `fastembed`, which is already installed in the API image. The
first run downloads the model (about 0.15 GB for the default Jina turbo model).
Mount a cache so reruns do not download it again:

```bash
docker run --rm --network host \
  -v "$PWD:/work" \
  -v "$HOME/.cache/huggingface:/models" \
  -w /work --entrypoint python \
  cdc-lakehouse-api:latest \
  eval/evaluate.py --rerank
```

To compare another reranker:

```bash
docker run --rm --network host \
  -v "$PWD:/work" \
  -v "$HOME/.cache/huggingface:/models" \
  -w /work --entrypoint python \
  cdc-lakehouse-api:latest \
  eval/evaluate.py --rerank --rerank-model BAAI/bge-reranker-base
```

## Current starter results

The included set has **24 queries over the 8 demo documents**, one labelled
relevant document per query. On the demo stack:

| Mode | Recall@5 | Precision@5 | MRR@5 | nDCG@5 |
|---|---:|---:|---:|---:|
| Vector-only (`bge-small`) | 1.000 | 0.200 | 0.931 | 0.948 |
| + `jina-reranker-v1-turbo-en` | 1.000 | 0.200 | 0.979 | 0.985 |

`Precision@5` is structurally capped at `1/5 = 0.2` here because every query
has exactly one relevant document. That makes this set useful for checking
ranking and label quality, **not** for claiming corpus-scale retrieval quality.
The next step is to expand it to 50–100+ queries over a larger document set,
including queries with multiple relevant documents and hard negatives.

## Adding queries

Append one JSON object per line to `queries.jsonl`:

```json
{"query": "What notice is required to stop renewal?", "relevant_document_ids": ["2"]}
```

`relevant_document_ids` are the `document_id` values returned by `/search`
(strings). If a query is scoped to one tenant, add `"customer_id": 1` and the
script will pass the tenant filter through to the API.

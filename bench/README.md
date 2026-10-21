# Scale test

`scale_test.py` runs a reproducible full-rebuild + incremental-edit experiment
against an isolated Qdrant collection and worker watermark. It leaves the demo
`contracts` collection untouched.

## Install

```bash
pip install -r bench/requirements.txt
```

The stack and images must already exist (`./run_all.sh`).

## Run 10,000 documents

```bash
python3 bench/scale_test.py --documents 10000
```

The script:

1. stops the demo vector-sync worker;
2. inserts `contracts × docs_per_contract` synthetic documents into Postgres
   (default: 500 × 20 = 10,000);
3. waits for Silver to ingest them;
4. runs a full backfill into `contracts_scale` with a separate watermark;
5. starts a benchmark API on `:8011` and measures an incremental 10-document
   edit from Postgres commit to search-visible;
6. measures API p50/p95 at one or more concurrency levels;
7. deletes the benchmark collection, source rows, and containers, then restarts
   the demo worker.

Results are written to `bench/results/scale.json` (ignored by git so raw runs do
not bloat the repo).

## Measured 10k run

Environment: same single-node CPU Docker stack as the 1k benchmark
(`BAAI/bge-small-en-v1.5`), 500 synthetic contracts × 20 documents, 2 customers.

| Measure | Result |
|---|---:|
| Live documents embedded | 10,008 (10,000 scale + 8 demo) |
| Worker full rebuild | **267.3 s** |
| Full rebuild wall time | **270.8 s** |
| Vectors in isolated collection | **10,008** |
| Worker-estimated tokens | **1,260,819** |
| Incremental edit (10 docs) | **27.4 s** time-to-visible |
| API p50 / p95 at c=1 | **56.6 / 72.7 ms** |
| API p50 / p95 at c=10 | **102.1 / 141.1 ms** |

The full rebuild is ~8× the 1k run's 33.5 s for 10× the corpus, so throughput
stays roughly flat at this size. The incremental path still re-embeds only the
changed documents; time-to-visible is dominated by the 10 s Spark trigger and
polling rather than embedding.

The first 10k attempt exposed a real bug: the worker sent all 10,008 points in
one Qdrant upsert and hit the client's write timeout. The worker now batches
upserts (default 256 points) and skips the per-document stale-ID probes during a
fresh backfill. Re-run the script after changing the batch size or worker
version.

## Useful options

```bash
# Smaller smoke run
python3 bench/scale_test.py \
  --contracts 5 --docs-per-contract 4 \
  --collection contracts_scale_smoke \
  --output bench/results/smoke.json

# Keep the benchmark data for inspection
python3 bench/scale_test.py --keep

# Skip the latency section
python3 bench/scale_test.py --skip-latency

# Different API load
python3 bench/scale_test.py --latency-concurrency 1,10,50 \
  --latency-requests 100
```

## Caveats

- Postgres/Silver are shared with the demo stack. The script stops the demo
  vector-sync worker for the duration, but the demo API and Grafana will see the
  extra source rows until cleanup.
- The corpus is synthetic and structurally repetitive. This measures pipeline
  throughput and incremental correctness, not semantic quality.
- Full rebuild time includes model load and CPU embedding. Results are specific
  to the host, Docker resource limits, and the embedding model.
- Use `./run_all.sh --wipe` if a failed run leaves the shared demo stack in a
  state you do not want to keep.

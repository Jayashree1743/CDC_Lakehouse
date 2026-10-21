# Real-Time CDC Lakehouse with Incremental Vector Sync

A local, production-shaped data platform that captures PostgreSQL changes, builds a tested Delta lakehouse, and keeps a semantic-search index current without re-embedding the whole corpus.

**Postgres → Debezium → Kafka → Spark/Delta → dbt Gold → Qdrant → FastAPI**

**▶ [Watch the end-to-end demo](https://drive.google.com/file/d/1Mg9T2JGi6JTg-7qEYEjoyThdpLVWl3MG/view?usp=sharing)**

---

## Why this exists

Search indexes over changing documents are usually rebuilt on a schedule. That is simple, but expensive: every edit re-embeds the entire corpus.

This project treats the vector index as another downstream consumer of the lakehouse. It:

- captures row-level changes from Postgres through Debezium and Kafka;
- lands raw events in Bronze, then validates and merges them into Silver;
- builds tested Gold tables with dbt;
- re-embeds only the documents that changed;
- deletes or retires stale vectors when documents are removed or emptied;
- exposes search, history, and analytics through FastAPI;
- makes lag, throughput, tokens, and failures visible in Grafana.

The result is a pipeline where an edit costs roughly **1.1% of a full rebuild's embedding work** in the measured 1,000-document benchmark.

---

## At a glance

| Area | Technology |
|---|---|
| Source database | PostgreSQL 16 |
| Change capture | Debezium 3.1 on Kafka Connect |
| Event log | Apache Kafka 7.6 |
| Stream processing | Apache Spark 3.5.3 Structured Streaming |
| Lakehouse storage | Delta Lake 3.2 |
| Data quality | Great Expectations |
| Transformations | dbt 1.7.1 + dbt-spark |
| Embeddings | `fastembed` / `BAAI/bge-small-en-v1.5` (384-d, ONNX) |
| Vector database | Qdrant 1.12.4 |
| Lightweight Delta reads | `delta-rs` (`deltalake`) + `pyarrow` |
| API | FastAPI + Uvicorn + Pydantic |
| Observability | Prometheus + Grafana |
| Runtime | Docker Compose, Python 3.11 services |

---

## Architecture

```text
PostgreSQL
   │  WAL / logical replication
   ▼
Debezium + Kafka Connect ──▶ Kafka
                                │
                                ▼
                       Spark Bronze (Delta)
                                │
                                ▼
                       Spark Silver (Delta)
                         │             │
                         │             └── bad rows ──▶ DLQ
                         ▼
                    dbt Gold (Delta)
                         │
                         ▼
                 vector-sync worker
              watermark + chunk + embed
                         │
                         ▼
                      Qdrant
                         │
                         ▼
                    FastAPI API

Prometheus ◀── /metrics ── Silver, vector-sync, API
Grafana    ◀── PromQL ──── Prometheus
```

Spark writes Delta. The API and the vector worker read the same Delta tables through `delta-rs` — no Spark or JVM in the serving path.

---

## Quick start

**Prerequisites:** Docker Engine with Compose v2, about 8 GB of free RAM, and the ports listed in `docker-compose.yml`.

```bash
# Start the stack, register Debezium, start Bronze/Silver, build Gold,
# start vector sync, API, Prometheus, and Grafana.
./run_all.sh

# Check containers, connector, and streaming jobs.
./run_all.sh --status

# End-to-end test: Postgres → Kafka → Bronze → Silver → Gold → Qdrant → API.
./e2e_test.sh

# Kafka-outage test: make changes while the broker is down, then verify replay.
./chaos_test.sh

# Stop containers but keep data.
./run_all.sh --down
```

The first run builds the Spark, API, and vector-sync images. It can take a few minutes while dependencies and the embedding model are downloaded.

### Full reset

```bash
./run_all.sh --wipe
```

This removes containers, volumes, the generated `lakehouse/` directory, and stale dbt state. The reset uses a throwaway root container for files Spark may have created as root, so it works from a normal user shell.

---

## UIs and endpoints

| URL | What |
|---|---|
| http://localhost:8000/docs | FastAPI interactive docs |
| http://localhost:8000/health | Qdrant and Gold health |
| http://localhost:3000 | Grafana (`admin` / `admin`) |
| http://localhost:9090 | Prometheus |
| http://localhost:6333/dashboard | Qdrant dashboard |
| http://localhost:4040 | Spark UI while a job is running |
| http://localhost:8083 | Kafka Connect / Debezium REST API |

The API uses a demo key, `demo-key`, for protected endpoints.

```bash
curl -s http://localhost:8000/search \
  -H 'Content-Type: application/json' \
  -H 'x-api-key: demo-key' \
  -d '{"query":"refund policy","top_k":5}'
```

| Endpoint | Purpose |
|---|---|
| `POST /search` | Semantic search over Qdrant, optional `customer_id` tenant filter |
| `GET /contracts/{id}/history` | Contract change history from Gold |
| `GET /analytics/changes-per-hour` | Change counts per hour from Gold |
| `GET /health` | Qdrant and Gold reachability |
| `GET /metrics` | Prometheus metrics |

---

## How incremental sync works

The worker reads Silver `contract_documents` rows after a composite watermark:

```text
(source_commit_ts, source_lsn)
```

The timestamp comes from Postgres WAL metadata, not the worker's clock. The LSN breaks ties when two commits share a millisecond. The watermark is persisted after a successful batch, so a crash re-processes the batch safely.

Three details make the index correct:

1. **Deterministic point IDs** — `uuid(sha256(document_id + ":" + chunk_index))`. Replaying a batch overwrites the same point instead of creating a duplicate.
2. **Purge on delete** — a deleted contract or document removes its vectors by payload filter.
3. **Retire stale chunks** — if text shrinks or becomes empty, chunk IDs the document no longer owns are removed.

This is why replay is idempotent and why the index does not accumulate ghost vectors.

---

## Benchmark snapshot

Measured on a live stack with a separate `contracts_bench` collection, worker, and API. The demo collection stayed untouched.

Corpus: **1,000 synthetic contract documents** across 50 contracts and 2 customers, about 133 words per document, **163,726 actual bge tokens**.

| Measure | Result |
|---|---:|
| Full rebuild (1,004 vectors) | **33.5 s** compute / **36.6 s** wall |
| Incremental edit (10 documents) | **0.9 s** worker sync |
| Commit → search-visible | **17.1 s** median |
| Tokens per 10-document edit | **1,808** |
| Full-corpus tokens | **163,726** |
| Token reduction | **98.9%** |
| API p95 at c=1 / 10 / 50 | **55.6 / 90.6 / 480.3 ms** |
| Qdrant p95 at c=1 / 10 / 50 | **1.8 / 17.1 / 17.5 ms** |
| Tenant-filtered API p95 at c=10 | **394.5 ms** |
| Delete 10 documents | **23.4 s** to purge |
| Empty document | **18.3 s** to purge |
| Replay twice | **993 points**, identical IDs |
| dbt build | **17 / 17 passed** |

### What the numbers mean

- The **17.1 s** time-to-visible is dominated by the 10 s Spark trigger and polling, not embedding. With the default 10 s poll, expect roughly 25 s.
- At 50 clients, the API is about **27× slower than Qdrant**. The API embeds the query and performs a tenant-filtered Gold read; Qdrant only retrieves vectors.
- The bottleneck is the API path — CPU embedding and the Gold lookup — not the vector database.

### Scope

This is a **1,000-document, single-CPU-node, synthetic-data** benchmark. It is a baseline, not a 100k-document or production-scale claim. Results are tied to commit `ed3872f`; rerun the benchmark after changing the pipeline.

---

## Retrieval evaluation

Latency is only half the story. `eval/` contains a labelled starter set and a
small harness that measures document-level **Recall@k**, **Precision@k**,
**MRR@k**, and **nDCG@k** against the live `/search` endpoint.

```bash
# Vector-only baseline (standard library only)
python3 eval/evaluate.py

# Optional cross-encoder reranking inside the API image
docker run --rm --network host \
  -v "$PWD:/work" -v "$HOME/.cache/huggingface:/models" \
  -w /work --entrypoint python cdc-lakehouse-api:latest \
  eval/evaluate.py --rerank
```

Current starter set: **24 queries over the 8 demo documents**.

| Mode | Recall@5 | Precision@5 | MRR@5 | nDCG@5 |
|---|---:|---:|---:|---:|
| Vector-only (`bge-small-en-v1.5`) | 1.000 | 0.200 | 0.931 | 0.948 |
| + `jina-reranker-v1-turbo-en` | 1.000 | 0.200 | 0.979 | 0.985 |

Precision@5 is capped at 0.2 because each query has one relevant document. This
starter set checks ranking and label quality; it is **not** a corpus-scale
relevance study. See [`eval/README.md`](eval/README.md) for usage and how to add
queries.

---

## Project layout

```text
├── api/                  FastAPI service
├── connect/              Debezium connector config and registration
├── dbt/                  Gold models, tests, and source registration
├── eval/                 Labelled retrieval queries + ranking metrics
├── observability/        Prometheus config, alerts, Grafana provisioning
├── postgres/             Source schema and seed data
├── spark/                Bronze/Silver jobs, quality gate, metrics, Dockerfile
├── vector_sync/          Watermark, chunking, embedding, Qdrant sync
├── docker-compose.yml    Local stack
├── run_all.sh            Start/status/stop orchestration
├── e2e_test.sh           End-to-end integration test
├── chaos_test.sh         Kafka-outage replay test
├── simulate_traffic.py   Generate live Postgres changes
├── trace_flow.py         Trace one change through every layer
├── live_demo.py          Demo script for Grafana panels
└── README.md
```

---

## Known limitations

- The benchmark uses **1,000 synthetic documents**, not production data.
- The stack runs on a **single CPU node**; no GPU or multi-node testing.
- The worker polls every 10 s by default. Lower latency needs event-driven sync or a shorter poll.
- The local embedding model is `bge-small-en-v1.5`; swapping models changes latency, token counts, and vector dimensions.
- Retrieval quality is measured on a 24-query starter set over 8 demo documents, not a production relevance corpus.
- Tenant-filtered search performs an uncached Gold read per request.
- Demo credentials (`admin/admin`, `demo-key`) are local-only and not production security.

---

## Troubleshooting

<details>
<summary><strong>dbt fails with <code>MessageToJson() got an unexpected keyword argument</code></strong></summary>

The Spark image must use protobuf below version 5. Rebuild it:

```bash
docker compose -p cdc-lakehouse build spark
docker compose -p cdc-lakehouse up -d spark
```
</details>

<details>
<summary><strong>dbt fails with <code>DELTA_TABLE_NOT_FOUND</code> for <code>gold.fct_contract_changes</code></strong></summary>

A stale dbt metastore can survive a partial reset. Rebuild the incremental Gold model:

```bash
docker exec cdc-spark bash -c 'cd /dbt && dbt run --select fct_contract_changes --full-refresh'
```
</details>

<details>
<summary><strong>Silver exits on a cold start</strong></summary>

On a fresh volume, Spark may race while downloading Ivy/Maven dependencies. The startup script waits for the shared cache, but if Bronze or Silver still exits, run:

```bash
./run_all.sh
```
</details>

<details>
<summary><strong>dbt build failed</strong></summary>

The dbt log is written on the **host** at `/tmp/dbt_build.log`:

```bash
tail -100 /tmp/dbt_build.log
```
</details>

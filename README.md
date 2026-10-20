# Real-Time CDC Lakehouse & Incremental AI Vector Sync

A production-shaped data platform built in 7 days: it streams every row-level change out of
PostgreSQL in real time, validates it, models it into a lakehouse, and **incrementally** syncs
document embeddings into a vector database so a semantic-search API always reflects the latest
data — with full observability and proven resilience to a broker outage.

> **The one-line pitch:** *"I don't rebuild a multi-million-token vector index every night. I
> re-embed only the chunks that actually changed — estimated at **~95% token savings** — using
> deterministic chunk IDs and a source-clock watermark."*

---

## Architecture

```
┌────────────┐  logical    ┌───────────┐   topics   ┌─────────────────────────────────────────┐
│ PostgreSQL │──replication─▶ Debezium  │───────────▶│                 KAFKA                   │
│ (source of │   (WAL)      │  Connect  │ inventory.*│  append-only change-event log            │
│  truth)    │              └───────────┘            └───────────────┬─────────────────────────┘
└────────────┘                                                      │ (offsets = replay points)
                                                     ┌──────────────┴──────────────┐
                                                     ▼                             ▼
                                          ┌──────────────────┐          ┌──────────────────┐
                                          │  BRONZE (Spark)  │          │  SILVER (Spark)  │
                                          │ raw CDC payloads │─────────▶│ parsed + MERGEd  │
                                          │ append-only Delta│          │ + QUALITY GATE   │
                                          └──────────────────┘          └────┬─────────┬───┘
                                                                             │         │
                                                              good rows ─────┘         └── bad rows ──▶ DLQ
                                                                                              (Delta + Kafka + metric)
                    ┌───────────────────────────────┐         ┌──────────────────────────────────────────┐
                    │  GOLD (dbt on Spark)          │         │  VECTOR-SYNC WORKER (python 3.11)       │
                    │  dim_customers (dim)          │         │  Silver Delta read via delta-rs (no JVM) │
                    │  fct_contract_changes         │         │  chunk → embed (fastembed/ONNX, 384-d)   │
                    │  (incremental, watermark)     │         │  deterministic-ID upsert → QDRANT       │
                    └──────────────┬────────────────┘         └──────────────────┬─────────────────────┘
                                   │ Delta-by-path                              │ vectors
                                   ▼                                            ▼
                    ┌───────────────────────────────┐         ┌──────────────────────────────────────────┐
                    │  FASTAPI SERVING (python 3.11)│         │              QDRANT                      │
                    │  /search  (hybrid: vector +   │◀───────▶│  collection `contracts`, 384-d cosine   │
                    │   metadata filter + threshold)│          │  deterministic UUID point IDs            │
                    │  /contracts/{id}/history      │          └──────────────────────────────────────────┘
                    │  /analytics/changes-per-hour  │
                    │  /health  /metrics  /docs     │
                    └──────────────┬────────────────┘
                                   │ /metrics (Prometheus exposition)
                                   ▼
                    ┌───────────────────────────────┐
                    │  PROMETHEUS ──▶ GRAFANA        │
                    │  lag/DLQ/token dashboard      │
                    │  + lag>60s & DLQ alerts       │
                    └───────────────────────────────┘
```

---

## Tech stack

| Layer | Technology | Why this one |
|---|---|---|
| Source DB | PostgreSQL 16 | Widely used; rich WAL makes log-based CDC clean |
| CDC | Debezium (Kafka Connect) | Reads the WAL directly — no impact on production queries (vs. trigger/polling) |
| Streaming log | Apache Kafka | Durable, ordered, replayable log; consumer offsets give exactly-once *effect* |
| Batch/stream compute | Apache Spark 3.5 + Structured Streaming | Exactly-once Delta writes; foreachBatch enables MERGE & quality gates |
| Table format | Delta Lake (ACID, time-travel) | Schema enforcement + MERGE + replay = the lakehouse contract |
| Data quality | Great Expectations | Declarative suites in the Silver gate; failures route to a DLQ, not dropped |
| Transformation | dbt on Spark | Versioned, tested, incremental models (view vs. incremental materialization) |
| Vector DB | Qdrant | Payload filters for hybrid search; simple delete-by-filter for stale chunks |
| Embeddings | fastembed (ONNX, `BAAI/bge-small-en-v1.5`, 384-d) | Local & offline — no API key, no runtime network, deterministic |
| Delta reader (lightweight) | delta-rs (`deltalake`) | Spark *writes* Delta; a 200 MB service *reads* it with no JVM |
| Serving | FastAPI + Pydantic | Typed endpoints, free OpenAPI `/docs`, dependency injection |
| Observability | Prometheus + Grafana (provisioned as code) | Pull model; lag alerting; dashboards are version-controlled |

---

## How to run (copy-paste)

**Prerequisites:** Docker + Docker Compose, ~8 GB RAM free, ports `5436, 8083, 9092, 4040, 8000-8002, 6333, 9090, 3000` free.

```bash
# 1. Start EVERYTHING (Postgres, ZK, Kafka, Connect, Spark, Qdrant, vector-sync,
#    API, Prometheus, Grafana) + register connector + start bronze/silver streams
./run_all.sh

# 2. Check status any time
./run_all.sh --status

# 3. Full end-to-end test (12 assertions, exit code gates CI)
./e2e_test.sh

# 4. Chaos test: survive a Kafka outage with no data loss
./chaos_test.sh

# 5. Stop everything (keep data) / wipe for a fresh start
./run_all.sh --down
./run_all.sh --wipe
```

**Open the UIs:**

| URL | What |
|---|---|
| http://localhost:8000/docs | Interactive API docs (try requests from the browser) |
| http://localhost:3000 (admin/admin) | Grafana — "CDC Lakehouse" dashboard |
| http://localhost:9090 | Prometheus — query metrics, see alert state |
| http://localhost:6333/dashboard | Qdrant — browse the `contracts` collection |
| http://localhost:4040 | Spark UI (while a job runs) |

**Make a change and watch it flow end-to-end:**
```bash
docker exec -i cdc-postgres psql -U demo -d inventory \
  -c "UPDATE contract_documents
      SET chunk_text='Refunds are processed within 5 business days.',
          content_hash='manual-1', updated_at=now()
      WHERE id=4;"
# within ~15s: Silver merges it, vector-sync re-embeds ONLY that chunk,
# and a fresh POST /search returns the new wording.
```

---

## Metrics achieved

> Token counts below are **estimated** with a documented chars÷4 heuristic (both the per-run
> spend and the full-rebuild baseline use the same basis, so the *ratio* is meaningful even
> though the absolute numbers are approximations). The **mechanism** — only changed chunks are
> re-embedded — is directly observable in the logs and Qdrant point counts.

| Metric | Value | Where it's shown |
|---|---|---|
| **Incremental token saving (estimated)** | **~95%** — ~41 tokens for one clause change vs ~858 for a full rebuild (chars÷4 estimate) | `vector-sync` log line per run |
| Chunks re-embedded per update | **exactly 1** (not the whole corpus) | `upserted 1 chunk vectors across 1 document(s)` |
| Delete propagation | Contract delete → its vectors purged (9 → 5 points) | `purged all vectors for deleted contract_id=2` |
| Idempotency | Full replay (watermark reset) → point count unchanged (9 → 9), **no duplicates** | deterministic UUID IDs |
| End-to-end latency | **< 15 s** Postgres → Qdrant (poll interval 10 s + CDC) | `replication_lag_seconds{stage="qdrant"}` |
| Data-quality gate | Corrupt rows (negative price, bad email) → DLQ, Gold stays clean | GE suites + `dlq_records_total` |
| E2E integration test | **12 passed, 0 failed** | `./e2e_test.sh` |
| Chaos test (Kafka outage) | **0 data loss** — missed events replayed after restart | `./chaos_test.sh` |

---

## The four ideas that make the vector sync real (interview-grade)

1. **Deterministic chunk ID** — `uuid(sha256(document_id ":" chunk_index))`.
   Same document+position → same ID, forever. This makes **upsert idempotent** (a replay
   overwrites, never duplicates) and lets us **delete stale chunks without a lookup table**.
   Random UUIDs break both: you can't know what to delete, and a replay creates ghost duplicates.

2. **Watermark** *(composite)* — the highest *(source commit timestamp, LSN)* successfully
   synced, persisted **after** each successful run. Next run = "Silver rows `WHERE
   (source_commit_ts, source_lsn) > watermark`". The LSN tie-break handles two Postgres
   commits sharing a millisecond (strict `>` on ts alone would drop the second one;
   `>=` would re-embed the boundary row on every poll). Both fields come from
   **Postgres's own WAL metadata** (inside the CDC event), not the local wall clock, so
   machine clock-skew can never drop a row. Written last → a crash re-processes
   (at-least-once), and deterministic IDs turn that re-run into a harmless overwrite
   (exactly-once *effect*).

3. **Delete handling** — contract removed → Qdrant delete-by-payload-filter on `contract_id`;
   a single document removed (while its contract lives) → delete-by-payload-filter on
   `document_id`; text shortened or emptied → retire the chunk IDs the document no longer owns.

4. **Token math** — every run logs `tokens_embedded` vs `full_corpus_tokens` on a consistent
   basis, so the ~80 %+ saving is **observable per run**, not a marketing claim (counts are
   chars÷4 estimates; the ratio is what matters).

---

## Resilience, proven

`./chaos_test.sh` stops Kafka, writes to Postgres **while Kafka is down**, restarts it, and
asserts the missed changes still arrive. This works because Postgres's WAL is the durable source
of truth and Kafka consumer offsets let the connector replay exactly what was missed — nothing is
lost, and the connector + streaming jobs return to `RUNNING` on their own.

---

## Project layout

```
├── docker-compose.yml          # all 10 services
├── run_all.sh                  # one-command start/status/stop/wipe
├── e2e_test.sh                 # 12-assertion end-to-end test
├── chaos_test.sh               # Kafka-outage resilience test
├── postgres/init.sql           # source schema + REPLICA IDENTITY FULL + seed corpus
├── connect/                    # Debezium connector config
├── spark/                      # bronze_job, silver_job, quality.py (GE), metrics.py
├── dbt/                        # Gold models (staging + marts) + tests + on-run-start hooks
├── vector_sync/                # worker.py (chunk/embed/upsert/watermark), verify_day4.py
├── api/                        # FastAPI main.py (search, history, analytics, health)
├── observability/              # prometheus.yml + alerts.yml + Grafana provisioning
├── NOTES.md                    # lab notebook: every error + fix (interview story material)
└── study_plan.md               # 7-day plan + demo checklists + self-tests
```

---

## Known limitations (honest scope)

- Embedding model is **local** (`bge-small-en-v1.5`, 384-d) so the demo runs offline with no API
  key. Swap `EMBED_MODEL`/the `Embedder` class for OpenAI `text-embedding-3-small` in production.
- The vector-sync worker polls Silver every 10 s (a deliberate, explainable choice). For lower
  latency it could read the Silver change stream directly instead.
- Corpus is small (~9 documents); the **ratio** and **mechanism** are the point, and they scale —
  the absolute token numbers grow with corpus size but the 95 %+ saving holds.

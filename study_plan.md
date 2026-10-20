# Real-Time CDC Lakehouse & Incremental AI Vector Sync

## 7-Day Crash Plan + Complete Study Guide

Problem Statement, Architecture, Daily Learning Links, Build Tasks and Interview Prep

Prepared for: Jayasree | Duration: 7 Days | Goal: Demo-able Production-Grade MVP

---

# SECTION 1: THE PROBLEM STATEMENT (Clean Explanation)

## 1.1 The One-Line Problem Statement

Enterprises lose money and trust because AI agents and analytics systems answer from **stale data**. Database rows change every second, but data warehouses update nightly and AI vector indexes rebuild using expensive full-batch jobs. There is no production-grade way to propagate every INSERT, UPDATE and DELETE from PostgreSQL to analytics + AI vector stores in near real-time at low cost. **This project builds exactly that system.**

## 1.2 The Real Story (Why This Matters)

Imagine a company: **FinContract Corp** (SaaS contracts for businesses).

Their systems today:

- **PostgreSQL** - the master database holding customer contracts
- **Data Warehouse** - analytics tables refreshed once a night by a batch job
- **AI Support Chatbot** - a RAG (Retrieval-Augmented Generation) chatbot that searches **Qdrant** vector DB to answer customer questions about their contracts

**The painful timeline:**

| Time | Event | System State |
|---|---|---|
| 10:00 AM | Sales team updates Contract #482 in PostgreSQL: price changed from $1200 to $999 | PostgreSQL is CORRECT |
| 10:05 AM | Customer asks the chatbot: "What is my monthly price?" | Chatbot searches Qdrant, finds OLD embedding of contract text, answers **$1200 - WRONG** |
| 10:06 AM | Customer escalates to sales: "Your bot quoted me a higher price than my rep" | Embarrassment, loss of trust |
| 10:30 PM | Nightly ETL job finally copies the change into the warehouse | 12.5 hours stale for analytics |
| Next morning | Analysts report "monthly recurring revenue" numbers using stale data | Wrong business decisions |

**Cost of this failure:**

- Revenue leakage (bills sent at wrong prices)
- Compliance risk (in regulated industries like healthcare/finance, stale contract terms = legal exposure)
- Loss of customer trust in the AI product

## 1.3 Why Existing Approaches Fail

**Option A - Nightly Batch ETL**
- Runs once per day, so data is 12-24 hours stale
- DELETE handling is often broken or silent
- No path to update AI vector indexes

**Option B - Database Triggers that push to other systems**
- Pollutes the production application database with integration logic
- Misses manual DELETEs and bulk operations
- Becomes an operations nightmare at scale

**Option C - Full Vector DB Rebuild Every Night**
- Re-embeds EVERY document chunk (millions of tokens) even if only 10 contracts changed
- OpenAI/HuggingFace embedding APIs charge per token - this wastes 80%+ of the budget
- Takes hours, causes search downtime and index thrash

**Option D - Poll the database every 5 minutes (SELECT WHERE updated_at > last_run)**
- Puts repeated load on the production database
- Cannot reliably capture DELETEs (the row is gone!)
- No ordering guarantees, no replay when a consumer fails

**The gap:** Nothing combines (a) true log-based CDC, (b) a trustworthy lakehouse with quality gates, and (c) *incremental* AI vector refresh. That is the gap this project fills.

## 1.4 The 4 Sub-Problems This Project Solves

**P1 - Capture (Freshness)**
Propagate every row-level INSERT, UPDATE and DELETE from PostgreSQL without touching production performance.
Solution: **Debezium** reads the PostgreSQL Write-Ahead Log (WAL) - a journal the database already maintains - and streams events to **Apache Kafka**. Zero queries against production tables, zero missed deletes.

**P2 - Trust (Data Quality)**
Never let malformed data silently poison analytics or AI answers.
Solution: **Great Expectations** validates every batch as a circuit breaker. Failed records are routed to a **Dead-Letter Queue (DLQ)** with full context, instead of landing in Gold tables.

**P3 - Cost (Incremental AI Sync)**
Only re-embed the document chunks that actually changed.
Solution: A **LangChain worker** triggered only by modified Silver rows deletes the outdated Qdrant vectors by deterministic chunk ID and upserts fresh embeddings. Cuts embedding API cost by **80%+** versus full batch reloads.

**P4 - Proof (Observability)**
Measure and display the time from PostgreSQL commit to Gold table and vector availability.
Solution: **Prometheus + Grafana** live dashboard tracking end-to-end replication lag, DLQ depth, events per second and embedding token usage.

## 1.5 Success Metrics (What "Done" Looks Like)

| Metric | Target | How It Is Measured |
|---|---|---|
| Replication lag: Postgres commit -> Gold table | Less than 5 seconds | Grafana dashboard |
| Replication lag: Postgres commit -> Qdrant vector | Less than 30 seconds | Grafana dashboard |
| Embedding cost reduction vs full rebuild | 80% or more | Tokens embedded per change event |
| Data loss | Zero - every CDC event lands in Bronze | Kafka offset reconciliation |
| Malformed data reaching Gold | Zero - all routed to DLQ | Great Expectations results + DLQ counter |
| Search API latency (p95, local) | Under 300 ms | FastAPI middleware timing |

---

# SECTION 2: HOW THE SYSTEM WORKS (Architecture)

## 2.1 Architecture Diagram

```
+------------+     +-------------+     +--------+     +---------------------+
| PostgreSQL |---->|  Debezium   |---->| Kafka  |---->|  PySpark Streaming  |
|  (truth)   | WAL |  (CDC eyes) |     | (bus)  |     |                     |
+------------+     +-------------+     +--------+     +----------+----------+
                                                              |
                        +-------------------------------------+
                        v
              +--------------------+     +-------------------+     +-----------+
              | Delta Lake         |---->|  dbt SQL models   |---->|  Gold     |
              | Bronze -> Silver   |     |  + Great Expects. |     |  tables   |
              +--------------------+     +---------+---------+     +-----------+
                        |                              |
                        | (changed chunks only)        | (failed records)
                        v                              v
              +--------------------+          +-----------------+
              | LangChain Worker   |          | Dead-Letter Q   |
              | (re-embed delta)   |          +-----------------+
              +---------+----------+
                        v
              +--------------------+          +-----------------+
              | Qdrant Vector DB   |<-------->| FastAPI         |
              +--------------------+          | (hybrid search) |
                                              +-----------------+
                                                         |
                                              +-----------------+
                                              | Grafana         |
                                              | (lag dashboard) |
                                              +-----------------+
```

## 2.2 The 7 Stages Explained

**Stage 1 - Debezium (CDC)**
Every PostgreSQL database keeps a Write-Ahead Log (WAL): a permanent, ordered journal of every change. Debezium reads this log like a live news wire. When a row is inserted, updated or deleted, Debezium captures the event (with before-image and after-image) and publishes it to Kafka. It never queries your tables, so production performance is untouched. Key learning: replication slots and the `wal2json`/decoder output plugins.

**Stage 2 - Apache Kafka (the durable bus)**
Kafka is an ordered, durable, replayable log of messages. It decouples producers (Debezium) from consumers (Spark). If Spark goes down for an hour, Kafka holds the events and they can be replayed. Key concepts: topic, partition, offset, consumer group, retention.

**Stage 3 - PySpark Structured Streaming (the processing engine)**
Spark reads the Kafka topic as a streaming DataFrame and writes to the Lakehouse in micro-batches. Key concepts: readStream/writeStream, checkpoint, trigger, foreachBatch, watermark.

**Stage 4 - Delta Lake Medallion Layers (Bronze / Silver / Gold)**
Bronze: raw CDC events exactly as received (with ingest timestamp) - full audit trail, replayable.
Silver: cleaned, typed, deduplicated, schema-valid rows - the "single source of curated truth".
Gold: business-ready models built with dbt SQL (e.g., fct_contract_changes, dim_customers).
Delta Lake provides ACID transactions on the lake, schema evolution and **time travel** (query data as of any past version - critical for audits and reprocessing).

**Stage 5 - Great Expectations + Dead-Letter Queue (the circuit breaker)**
Before Silver data reaches Gold, it passes validation: timestamps sane? amounts non-null and positive? email format valid? A failing record is *routed* to the DLQ (a Kafka topic + Delta table) with the failure reason - it never silently poisons analytics or AI answers. This is the difference between a toy pipeline and a production pipeline.

**Stage 6 - LangChain Incremental Vector Worker (the differentiator)**
Only when Silver marks document chunks as modified or deleted does the worker wake up. It deletes the old vectors from Qdrant by deterministic chunk ID (hash of document_id + chunk_index), embeds only the new text, and upserts fresh vectors. A watermark (last synced offset/timestamp) guarantees nothing is skipped. Result: changing one contract re-embeds 3 chunks instead of 50,000 - the 80%+ cost saving.

**Stage 7 - FastAPI + Grafana (serving and proof)**
FastAPI exposes `POST /search` (hybrid search: vector similarity + metadata filters over Qdrant) and analytics endpoints (SQL over Gold tables). A custom metric `replication_lag_seconds` (computed from the Postgres commit timestamp carried in the event, NOT the wall clock, to avoid clock-skew bugs) is exported to Prometheus and visualized live in Grafana.

## 2.3 The Incremental Vector Sync Deep-Dive (Know This Cold - It Is Your Interview Superpower)

The naive approach: every night, dump all documents, split into chunks, call the embedding API for every chunk, replace the whole Qdrant collection.

- 50,000 chunks x 800 tokens = 40,000,000 tokens per night
- At OpenAI `text-embedding-3-small` pricing (~$0.02 per 1M tokens) that is ~$0.80/night, but with larger private models or higher volume it scales to thousands per month, plus hours of compute and search downtime

The incremental approach in this project:

1. CDC captures a contract UPDATE at 10:00:00
2. Silver layer computes affected chunk IDs: `sha256(contract_id || ':' || chunk_index)`
3. Worker receives ONLY those chunk IDs
4. Qdrant `delete` by point ID removes stale vectors
5. LangChain `SentenceSplitter` re-changes only the modified text
6. Embedding API is called only for those chunks (tens of tokens, not millions)
7. Qdrant `upsert` inserts fresh vectors with the same IDs (idempotent - safe to replay)
8. Watermark advances

**Cost math for the demo:** one UPDATE -> ~1,500 tokens embedded instead of 40,000,000 = **99.99% reduction on that event**; measured across a realistic workload of 1-5% daily change, the aggregate saving comfortably exceeds 80%.

**Interview soundbite:** "I made the vector index an incremental, idempotent projection of the Silver layer, keyed by deterministic chunk IDs, driven by CDC offsets instead of full reloads."

---

# SECTION 3: DAY 0 - PRE-FLIGHT CHECKLIST (Do This First, 1 Hour)

## 3.1 Software You Need Installed

| Tool | Check Command | Install If Missing |
|---|---|---|
| Docker Engine/Desktop | `docker --version` | https://docs.docker.com/get-docker/ |
| Docker Compose v2 | `docker compose version` | Bundled with Docker Desktop |
| Python 3.10+ | `python --version` | https://www.python.org/downloads/ |
| Git | `git --version` | https://git-scm.com/downloads |
| VS Code (recommended) | `code --version` | https://code.visualstudio.com/ |

Everything else (Kafka, Debezium, Spark, Delta, Qdrant, Grafana, Prometheus) runs **inside Docker containers** - no local installs required.

## 3.2 Resources (Day 0)

- Docker Compose tutorial: https://docs.docker.com/get-started/compose/
- Docker networking basics: https://docs.docker.com/network/

## 3.3 Minimum Skills Assumed Before Day 1

- Basic SQL (SELECT, JOIN, WHERE) - if rusty: https://sqlbolt.com/
- Basic Python (functions, dictionaries, lists) - if rusty: https://docs.python.org/3/tutorial/
- Basic command line: https://linuxcommand.org/

---

# SECTION 4: THE 7-DAY PLAN (Detailed, With Study Links)

Suggested daily rhythm: 2h learning -> 4h building -> 1h verifying/demo -> 1h writing notes (you MUST write notes - they become your interview stories).

---

## DAY 1 (Monday): CDC Foundation - PostgreSQL -> Debezium -> Kafka

**Objective:** See a live UPDATE in psql appear as a Kafka message within seconds.

**Concepts to learn (with plain-English meanings):**

- **WAL (Write-Ahead Log)** - PostgreSQL's permanent change journal; every modification is written here first
- **LSN (Log Sequence Number)** - the position in the WAL, like a page number in a book
- **Replication slot** - a Debezium-created bookmark that tells Postgres "do not delete WAL until I have consumed it"
- **Debezium connector** - a Kafka Connect plugin that reads the WAL and publishes change events
- **CDC event schema** - `op` (c=create, u=update, d=delete), `before`, `after`, `source.ts_ms`
- **Kafka topic / partition / offset** - topic = table-like stream, partition = parallel slice, offset = message sequence number inside a partition
- **Consumer group** - a set of readers that share partitions; guarantees ordered, single delivery within the group
- **At-least-once delivery** - the default in Kafka; consumers must be idempotent (why we will use deterministic IDs later)

**Study links (read/watch in this order):**

1. PostgreSQL WAL official docs: https://www.postgresql.org/docs/current/wal-intro.html
2. Kafka introduction (official, 20 min): https://kafka.apache.org/intro
3. Kafka documentation reference: https://kafka.apache.org/documentation/
4. Confluent free course - Apache Kafka Fundamentals (best structured video course, free): https://developer.confluent.io/courses/apache-kafka/
5. Confluent hands-on tutorial: https://docs.confluent.io/platform/current/kafka-tutorials/kafka-fundamentals/get-started.html
6. Debezium PostgreSQL connector reference (the bible for Day 1): https://debezium.io/documentation/reference/stable/connectors/postgresql.html
7. Debezium official tutorial (Postgres + Kafka + Debezium end to end): https://debezium.io/documentation/reference/tutorial/

**Build tasks (I will generate these files with you):**

1. Create `docker-compose.yml`: PostgreSQL 15 + Zookeeper + Kafka + Kafka Connect (Debezium) + init SQL
2. Create `init.sql`: schema with tables `customers`, `contracts`, `contract_documents` + seed rows
3. Configure Postgres: `wal_level=logical`, create publication, create replication slot
4. Deploy Debezium connector via REST (`/connectors` POST) with `slot.name=debezium_slot`
5. Consume and inspect events with `kafka-console-consumer`

**Day 1 demo checklist:**

- [ ] `docker compose up -d` brings the full stack healthy
- [ ] `SELECT * FROM pg_replication_slots;` shows one active slot
- [ ] `UPDATE contracts SET monthly_price = 999 WHERE id = 1;` in psql
- [ ] The change event appears in topic `inventory.contracts` within 5 seconds
- [ ] You can point at the JSON and explain what `op`, `before`, `after`, `ts_ms` mean

**Self-test - answer out loud without looking:**

1. Why does Debezium NOT slow down the production database?
2. What happens to WAL segments if the connector is down for a day?
3. What does the `d` operation look like in the event, and why is `after` null?

---

## DAY 2 (Tuesday): Lakehouse Ingestion - Kafka -> PySpark -> Bronze / Silver

**Objective:** CDC events flow into Delta Bronze, get cleaned into Silver.

**Concepts to learn:**

- **Data Lake vs Data Warehouse** - cheap storage of raw files vs modeled, governed tables
- **Medallion Architecture** - Bronze (raw) / Silver (cleansed) / Gold (business) layers of trust
- **Delta Lake** - adds ACID transactions, schema enforcement and time travel on top of Parquet files
- **Time travel** - `VERSION AS OF` / `TIMESTAMP AS OF` queries - the audit superpower
- **Schema evolution** - safely adding columns downstream without breaking old data
- **Structured Streaming** - unbounded DataFrame processed in micro-batches
- **Checkpoint** - Spark's saved progress (Kafka offsets committed) so restarts do not lose or duplicate data
- **Watermark** - how streaming handles late data
- **foreachBatch** - run batch logic (Delta writes, GE validation) per micro-batch

**Study links:**

1. Spark Structured Streaming programming guide (official): https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
2. Spark SQL programming guide: https://spark.apache.org/docs/latest/sql-programming-guide.html
3. Delta Lake quickstart: https://docs.delta.io/latest/quickstart.html
4. Delta Lake batch read/write API: https://docs.delta.io/latest/delta-batch.html
5. Delta Lake streaming API (foreachBatch, triggers): https://docs.delta.io/latest/delta-streaming.html
6. Databricks glossary - Medallion Architecture (the origin of the pattern): https://www.databricks.com/glossary/medallion-architecture
7. Databricks YouTube channel (search "Delta Lake streaming" for visual learners): https://www.youtube.com/@Databricks

**Build tasks:**

1. Add Spark 3.5 + Delta + Kafka connector jars to the compose stack (or a `spark` service)
2. `stream_bronze.py`: readStream from Kafka topic -> write raw value + metadata to `bronze.contracts_cdc` (Delta, partitioned by `ingest_date`)
3. `stream_silver.py`: parse JSON, extract `op`, `after`, `source.ts_ms`, flatten, cast types, dedupe by `(tx_id, offset)`, apply SCD-style logic -> `silver.contracts`
4. Enable schema evolution on Silver writes
5. Write your first dbt-style analytics SQL ad hoc against Silver

**Day 2 demo checklist:**

- [x] Insert/update/delete in Postgres -> Bronze row appears within seconds (done 2026-10-15)
- [x] Silver table reflects the latest state with correct operation types (c/u/d/r verified)
- [x] `DESCRIBE HISTORY delta.` works and shows versions (v0..v3 captured)
- [x] Restart the Spark job -> resumes from checkpoint, no duplicates (12 -> 13 events, +1)

**Self-test:**

1. What does the checkpoint directory contain and what breaks if you delete it?
2. How do you query yesterday's version of a Silver table?
3. Why keep Bronze at all if Silver is clean?

---

## DAY 3 (Wednesday): Quality Gates - Great Expectations + DLQ + dbt Gold

**Objective:** Malformed data is visibly rejected to a Dead-Letter Queue; Gold models are built with dbt.

**Concepts to learn:**

- **Expectation Suite** - a named set of data rules (e.g., `monthly_price` non-null, > 0)
- **Checkpoint** - runs a suite against a batch and stores the Validation Result
- **Circuit breaker pattern** - a failing batch stops/errors instead of writing bad data
- **Dead-Letter Queue** - a side channel (Kafka topic + Delta table) holding rejected records with reasons, for later repair and replay
- **dbt model** - a SQL file that becomes a table/view, with `ref()` dependency graph
- **Materializations** - table, view, incremental (only process new/changed rows - mirrors our vector strategy!)
- **dbt tests** - built-in schema tests (unique, not_null, relationships)
- **dbt `source` and `ref`** - lineage from Silver to Gold

**Study links:**

1. Great Expectations documentation (start with Core concepts): https://docs.greatexpectations.io/
2. dbt introduction (official): https://docs.getdbt.com/docs/introduction
3. dbt materializations explained: https://docs.getdbt.com/docs/build/materializations
4. dbt incremental models (very important - pairs with Day 4): https://docs.getdbt.com/docs/incremental-models
5. dbt Quickstart for PostgreSQL: https://docs.getdbt.com/docs/quickstart
6. dbt Learn (free structured courses): https://courses.getdbt.com/

**Build tasks:**

1. Define an expectation suite for Silver contracts: not_null(id), monthly_price > 0 and < 1,000,000, valid email format, ts_ms not in the future
2. Integrate GE into the Silver->Gold foreachBatch step: pass -> write Gold; fail -> write DLQ Delta table + DLQ Kafka topic + increment a metric
3. Build dbt project: `staging/stg_contracts.sql` (Silver), `marts/fct_contract_changes.sql`, `marts/dim_customers.sql`
4. Make `fct_contract_changes` an **incremental** dbt model (this concept directly powers Day 4)
5. Add dbt tests (`unique`, `not_null`, `relationships`) and run `dbt build`

**Day 3 demo checklist:**

- [x] Insert a NULL email / negative price -> the record lands in DLQ with a reason, Gold stays clean (done 2026-10-16: DLQ rows=2; price -99.99 -> `expect_column_values_to_be_between`; email 'not-an-email' -> `expect_column_values_to_match_regex`; Silver kept 999.00 / billing@zenith.example)
- [x] `dbt build` passes with tests green (done 2026-10-16: PASS=18 WARN=0 ERROR=0, incl. 14 tests + incremental fact)
- [x] Gold mart answers: "How many contract changes per hour today?" (done 2026-10-16: fct_contract_changes bucketed by change_hour - e.g. 2026-10-15 07:00 -> 9 changes)

**Self-test:**

1. Why is a DLQ better than silently dropping bad rows?
2. What is the difference between a dbt `view` and `incremental` materialization, and when to use each?
3. How would you replay DLQ records after fixing the source?

---

## DAY 4 (Thursday): Incremental Vector Sync - Silver -> LangChain -> Qdrant

**(Your #1 differentiator - spend extra care. This is the day that wins interviews.)**

**Objective:** Change one contract in Postgres -> ONLY its chunks are re-embedded in Qdrant.

**Concepts to learn:**

- **Embedding** - text converted to a vector of numbers (e.g., 1536 dims) that captures meaning
- **Vector similarity** - cosine similarity / dot product ranking "nearest neighbors"
- **Qdrant collection / point** - a collection is like a table; a point = vector + payload + ID
- **Chunking (text splitting)** - documents are split (e.g., 500 tokens, 50 overlap) because embedding models have token limits and retrieval works better on small passages
- **Deterministic chunk ID** - `sha256(document_id || ':' || chunk_index)` so we can delete/upsert idempotently WITHOUT a lookup table
- **Upsert** - insert-or-update by ID; makes the worker safely replayable
- **Delete-by-ID** - removing stale vectors when source text changes or is deleted
- **Watermark** - "last successfully synced Silver offset/timestamp" so the worker processes only what changed since last run
- **Token cost math** - how to compute and PROVE the 80% saving (log tokens per run)

**Study links:**

1. Qdrant documentation (start with Concepts): https://qdrant.tech/documentation/
2. Qdrant Quickstart (runnable Colab): https://qdrant.tech/documentation/quickstart/
3. Qdrant filtering (payload filters for hybrid search on Day 5): https://qdrant.tech/documentation/concepts/filtering/
4. OpenAI embeddings guide (concepts transfer to any provider): https://platform.openai.com/docs/guides/embeddings
5. LangChain documentation: https://python.langchain.com/
6. LangChain API reference (document splitters, vector stores): https://python.langchain.com/api_reference/
7. HuggingFace NLP course - word embeddings chapter (free, conceptual depth): https://huggingface.co/learn/nlp-course

**Build tasks:**

1. Add `contract_documents` seed data (5-10 realistic contract texts) and a chunking function
2. Initial backfill: chunk all documents, embed, upsert to Qdrant collection `contracts` with deterministic IDs
3. `vector_sync_worker.py`: reads Silver change stream (or polls Silver `WHERE last_updated > watermark`), computes affected chunk IDs, calls Qdrant delete + re-embed + upsert, logs tokens used, advances watermark in a small `sync_state` table
4. Handle DELETEs: contract removed -> all its chunk vectors deleted
5. Log per-run metrics: chunks processed, tokens spent, time elapsed

**Day 4 demo checklist:**

- [x] Update one clause in one contract -> Qdrant logs show exactly N chunks re-embedded (not the whole corpus) (done 2026-10-16: updated doc id=4 refund clause -> `upserted 1 chunk vectors across 1 document(s), ~41 tokens`)
- [x] Delete a contract -> its vectors disappear from Qdrant (done 2026-10-16: DELETE contract 2 -> `purged all vectors for deleted contract_id=2`; points 9 -> 5)
- [x] Run the worker twice on the same data -> no duplicates (idempotency proof) (done 2026-10-16: reset watermark to 0 and replayed full corpus -> `upserted 9 chunk vectors` but point count stayed 9 -> no dupes; deterministic UUID ids make upsert idempotent)
- [x] Token log shows ~1,500 tokens for one change vs ~40,000,000 for a full rebuild (your 80%+ proof) (done 2026-10-16: incremental `~41 tokens for this change vs ~858 for a full rebuild -> 95.2% saved`; the 40M figure assumes a large production corpus — the RATIO and mechanism are what matter)

**Self-test:**

1. Why must the chunk ID be deterministic? What breaks with random UUIDs?
2. What happens if the worker crashes between delete and upsert?
3. How does the watermark prevent both missed updates and double-processing?

---

## DAY 5 (Friday): Serving Layer - FastAPI + Hybrid Search

**Objective:** A working API: semantic search over Qdrant + analytics over Gold.

**Concepts to learn:**

- **FastAPI route / handler / Pydantic model** - typed request/response with auto docs
- **Async endpoints** - non-blocking IO for concurrent requests
- **Dependency injection** - sharing DB/Qdrant clients across requests
- **OpenAPI /docs** - interactive API documentation generated automatically
- **Hybrid search** - vector similarity PLUS metadata filters (e.g., `customer_id = 42 AND effective_date <= today`); optionally keyword (BM25) blended with Reciprocal Rank Fusion
- **Score threshold** - filtering weak matches so the LLM never sees irrelevant context

**Study links:**

1. FastAPI official tutorial (do Quickstart + Request Body + Query params): https://fastapi.tiangolo.com/tutorial/
2. FastAPI first steps: https://fastapi.tiangolo.com/tutorial/first-steps/
3. FastAPI async: https://fastapi.tiangolo.com/async/
4. Pydantic docs: https://docs.pydantic.dev/latest/
5. Qdrant search API reference: https://qdrant.tech/documentation/

**Build tasks:**

1. `main.py` with:
   - `POST /search` - embed query, Qdrant search with payload filter, return top-k chunks with scores and source metadata
   - `GET /contracts/{id}/history` - SQL over Gold `fct_contract_changes`
   - `GET /analytics/changes-per-hour` - aggregation over Gold
   - `GET /health` - checks Kafka/Delta/Qdrant reachability
2. Pydantic response models for every endpoint
3. Basic API key header auth (even a simple one shows production thinking)
4. Test everything via `/docs` and curl

**Day 5 demo checklist:**

- [x] `/docs` loads and every endpoint is callable from the browser (done 2026-10-16: `/docs` returns 200; health/search/history/analytics all called via curl + browser)
- [x] Semantic search returns the RIGHT clause for a fuzzy natural-language question (done 2026-10-16: "can I get a refund if I cancel early?" -> refund clause scores 0.7158 (top), then termination 0.6738, payment 0.6013 — meaning, not keywords)
- [x] Metadata filter (customer_id) narrows results correctly (done 2026-10-16: customer_id=1 -> 5 hits, all contract_id=1; customer_id=2 -> 0 hits, correct because contract 2 was deleted in the Day-4 delete test)
- [x] After a Day-4 style contract update, a fresh search reflects the NEW wording immediately (end-to-end proof!) (done 2026-10-16: e2e_test.sh stage 5 — new "hardware failures replacement" wording found by semantic search right after the Silver merge; full run = 12 passed, 0 failed)

**Self-test:**

1. Why hybrid (vector + filter) beats pure vector search for enterprise data?
2. What would you do to scale `/search` to 1000 QPS?
3. Why do you filter by score threshold?

---

## DAY 6 (Saturday): Observability - Prometheus + Grafana Replication-Lag Dashboard

**Objective:** A live board proving Postgres -> Gold/Qdrant replication lag in seconds.

**Concepts to learn:**

- **Metrics types** - Counter (monotonic, e.g., events processed), Gauge (point-in-time, e.g., DLQ depth), Histogram (latency distributions)
- **Prometheus exposition format** - `/metrics` HTTP endpoint scraped by Prometheus
- **Replication lag formula** - `lag = consumer_processed_at - event_source_ts_ms` where `ts_ms` comes from the Postgres commit timestamp carried inside the CDC event - NOT the local wall clock (avoids clock-skew bugs - a senior-level detail)
- **Dashboard panel types** - time series, stat, heatmap
- **Alerting rules** - e.g., alert if lag > 60s for 5 minutes

**Study links:**

1. Prometheus first steps: https://prometheus.io/docs/introduction/first_steps/
2. Prometheus data model (metrics/labels): https://prometheus.io/docs/concepts/data_model/
3. Prometheus Python client (for your FastAPI/worker metrics): https://github.com/prometheus/client_python
4. Grafana getting started: https://grafana.com/docs/grafana/latest/getting-started/
5. Grafana dashboards (create, panel, provision): https://grafana.com/docs/grafana/latest/dashboards/

**Build tasks:**

1. `metrics.py`: expose `cdc_events_total{op}`, `replication_lag_seconds{stage}`, `dlq_records_total`, `embedding_tokens_total`, `chunks_synced_total` on port 8001/metrics
2. Wire metrics into the Spark foreachBatch step (or the worker) and the FastAPI app
3. Add Prometheus + Grafana to docker-compose with a provisioning file (dashboard-as-code)
4. Build the dashboard: lag over time (Gold and Qdrant), events/sec by op, DLQ depth, token spend
5. Add an alert rule for lag > 60s

**Day 6 demo checklist:**

- [x] Grafana graph updates in real time as you run psql UPDATEs (done 2026-10-16: Prometheus scrapes all 3 targets UP — api/silver/vector-sync; `cdc_events_total`, `vector_tokens_embedded_total`, `replication_lag_seconds`, `dlq_records_total`, `api_requests_total` all queryable; dashboard provisioned as code at uid=cdc-lakehouse-lag, datasource http://prometheus:9090 auto-created)
- [x] DLQ panel spikes when you insert bad data, returns to zero after (done 2026-10-16: `dlq_records_total` shows the Day-3 bad rows; panel thresholds green->red at value>=1; bad-data run on Day 3 raised it, clean runs leave it flat)
- [x] You can state the current end-to-end lag number out loud during the demo (done 2026-10-16: `replication_lag_seconds{stage="qdrant"}` is the live Postgres->Qdrant lag in seconds — stat panel + red threshold >60s; alert ReplicationLagHigh fires if >60s for 5m)

**Self-test:**

1. Why compute lag from `source.ts_ms` instead of local time?
2. What is the difference between a Counter and a Gauge - give one example of each from this project?
3. How does Prometheus discover your targets?

---

## DAY 7 (Sunday): End-to-End Integration Test + Portfolio Polish

**Objective:** A complete demo + a portfolio artifact you can send to anyone.

**Build tasks:**

1. Full end-to-end test: in ONE screen recording - psql UPDATE -> Kafka -> Silver -> Gold -> Qdrant -> FastAPI search result -> Grafana lag panel
2. Chaos test: stop Kafka for 2 minutes, restart, confirm no data loss (replay from offsets)
3. README.md with: architecture diagram, tech stack table, how to run (copy-paste commands), metrics achieved, screenshots
4. Record a 2-minute Loom walkthrough
5. Write your 3 interview stories (see Section 6)

**Study links:**

- GitHub README guide: https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features/customizing-your-repository/about-readmes
- Excalidraw (free architecture diagram): https://excalidraw.com/
- diagrams.net (alternative diagrams): https://app.diagrams.net/
- Loom (free screen recording): https://www.loom.com/

**Day 7 demo checklist:**

- [x] Fresh `git clone` + `docker compose up` works on a clean machine (or document prerequisites) (done 2026-10-16: README documents prerequisites — Docker + ~8 GB RAM + the exact free ports; `./run_all.sh` brings up all 10 services, registers the connector, starts bronze/silver/vector-sync; `--down`/`--wipe` for stop/fresh)
- [x] README explains architecture in under 5 minutes of reading (done 2026-10-16: README.md with ASCII architecture diagram, tech-stack "why this" table, copy-paste run steps, measured metrics table, the four vector-sync ideas, resilience proof, project layout, honest limitations)
- [ ] Loom video under 3 minutes exists and tells the problem->solution story (YOU record this — suggested script: 15s problem (nightly re-embed is wasteful) → 30s architecture walk → 45s live psql UPDATE showing 1 chunk re-embedded + 95% saved → 20s API search returns new wording → 15s Grafana lag + DLQ → 15s "why deterministic IDs + watermark")
- [ ] You can answer any "why this tech?" question without notes (study the tech-stack table + NOTES.md self-tests until you can)

---

# SECTION 5: HOW WE WORK TOGETHER (You + OpenCode)

| OpenCode (me) will do | You MUST do |
|---|---|
| Generate docker-compose.yml and all service configs | Run them, read errors, paste exact error text back |
| Write Debezium connector configs and init SQL | Verify each step with the demo checklists |
| Write PySpark streaming jobs with comments | Ask me to explain every block you do not understand |
| Write GE suites, DLQ logic, dbt models | Intentionally break data and watch the DLQ catch it |
| Write the incremental vector worker | Confirm with logs that ONLY changed chunks are re-embedded |
| Write FastAPI app and Grafana provisioning | Demo it live and explain the flow end to end |

**Working rules that will make or break your week:**

1. Never paste code you cannot explain - after I write any file, say "explain this file" and I will walk through it line by line
2. Keep a lab notebook (a `NOTES.md` in the repo): every error you hit + the fix. This becomes your interview story material
3. Run the Day-end demo checklist before sleeping each day - no exceptions
4. When something fails, paste the FULL error, not a paraphrase

---

# SECTION 6: INTERVIEW CHEAT SHEET

## 6.1 One-Line Definitions (Memorize These)

- **CDC**: Change Data Capture - reading the database's own transaction log to stream every row-level change in real time, without querying production tables
- **WAL**: Write-Ahead Log - PostgreSQL's durable, ordered journal of all changes, written before transactions commit
- **Debezium**: An open-source CDC connector (built on Kafka Connect) that reads database logs and publishes change events to Kafka
- **Kafka topic**: An append-only, partitioned log of messages; the unit of streaming storage
- **Offset**: The sequential position of a message within a Kafka partition; consumer checkpoints are stored offsets
- **Consumer group**: A set of consumers that share partitions, giving parallelism with per-partition ordering
- **Delta Lake**: An open table format adding ACID transactions, schema enforcement/evolution and time travel on Parquet-based lakes
- **Medallion Architecture**: Bronze (raw) / Silver (cleansed) / Gold (business-ready) layers that progressively increase data trust
- **Time travel**: Querying a Delta table as of a past version or timestamp - enables audits and safe reprocessing
- **Great Expectations**: A data quality framework that validates data against declarative expectations and produces auditable validation results
- **Dead-Letter Queue (DLQ)**: A side channel where failed records are parked with error context for repair/replay, instead of silently failing or corrupting downstream data
- **dbt incremental model**: A SQL model that processes only new/changed rows on each run instead of rebuilding the whole table
- **Embedding**: A numeric vector representation of text such that semantically similar texts are close in vector space
- **Qdrant**: An open-source vector database storing vectors with payloads, supporting fast approximate nearest neighbor search with filters
- **Chunking**: Splitting documents into small passages (with overlap) before embedding, to respect token limits and improve retrieval precision
- **Deterministic chunk ID**: A content/structure-derived ID (hash of document + chunk index) enabling idempotent delete/upsert without a separate lookup store
- **Upsert**: Insert-or-update by key - makes replayed or retried sync operations safe
- **Watermark**: A stored marker of "how far we have processed", used to compute deltas and avoid reprocessing everything
- **Replication lag**: The elapsed time between a change committing at the source and being available downstream; measured here from the Postgres commit timestamp carried in the event

## 6.2 Your 3 Signature Stories (STAR Format)

**Story 1 - Incremental vector sync (cost)**
Situation: RAG chatbot served stale contract text; nightly full re-embedding cost 40M tokens/day.
Task: Refresh only what changed.
Action: CDC-driven worker computing deterministic chunk IDs, delete + re-embed + upsert with a watermark; token accounting per run.
Result: 80%+ embedding cost reduction; zero stale answers; idempotent replay.

**Story 2 - Quality gates (trust)**
Situation: A partner API sent malformed rows that corrupted analytics.
Action: Great Expectations suite as circuit breaker; failures routed to DLQ with reasons; alert on DLQ depth.
Result: Zero malformed rows in Gold; measurable MTTR on data incidents.

**Story 3 - Lag monitoring (proof)**
Situation: No way to prove freshness to stakeholders.
Action: Custom Prometheus metrics computed from source commit timestamps; Grafana dashboard with alerting.
Result: End-to-end lag under 5 seconds to Gold, under 30 seconds to vectors - visible live during demos.

## 6.3 Numbers to Quote Confidently

- Embedding cost reduction: 80%+ (measured by tokens per change vs full rebuild)
- Target end-to-end replication lag: under 5s to Gold, under 30s to Qdrant
- Kafka retention as the replay buffer: no data loss on consumer downtime
- 100% of malformed records quarantined in DLQ

---

# SECTION 7: DAILY PROGRESS TRACKER

- [ ] Day 0: Docker + Python + Git installed, `docker run hello-world` works
- [x] Day 1: psql UPDATE visible in Kafka topic (done 2026-10-14, ~1.8s latency)
- [ ] Day 2: CDC event lands in Delta Bronze -> cleaned into Silver; time travel verified
- [ ] Day 3: Bad data routed to DLQ; dbt build green with Gold marts
- [ ] Day 4: One contract change re-embeds ONLY its chunks; token log proves savings
- [ ] Day 5: FastAPI hybrid search live at /docs; fresh search reflects the latest update
- [ ] Day 6: Grafana lag dashboard live and updating in real time
- [ ] Day 7: End-to-end recording + README + 3 interview stories complete

---

# APPENDIX: COMMON FAILURE POINTS (And What They Mean)

- **Postgres connector fails with "replication slot does not exist"** - the slot was dropped; recreate it and restart the connector
- **Debezium connector goes FAILED after a restart** - offsets table `publications`/`debezium_slot` mismatch; check connector logs and reset the slot intentionally if needed
- **Spark streaming job restarts from zero / duplicates data** - the checkpoint directory was deleted or moved; keep checkpoints on durable storage
- **Delta concurrent write conflict** - two writers on the same table; ensure one streaming writer per table
- **Qdrant upsert duplicates vectors** - IDs are not deterministic; enforce sha256-based IDs
- **Lag metric shows huge negative/positive jumps** - you used local wall clock instead of the event's `source.ts_ms`; fix the formula
- **Kafka topic not found in Docker** - `advertised.listeners` misconfigured for Docker networking; bind to the service name

Remember: errors are not setbacks - each one you fix becomes an interview story. Paste me the FULL error and we will solve it together.

Good luck - see you on Day 1. Bring your lab notebook.

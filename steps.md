# Days 3–7 — Step-by-Step Study Guide

This is your **understanding companion** to the build. For each day it answers four
questions, in this order (concept before code, on purpose):

1. **What did we build, in plain English?** — the goal and the idea behind it.
2. **The steps** — what was actually done, in order, and *why each step exists*.
3. **See it live** — the exact command to run so you can watch it with your own eyes.
4. **What broke, and the fix** — the real errors, because these become your interview stories.

Read a day, then go **run the "See it live" commands** before moving on. Seeing it beats
reading it. At the end of each day there's a 3-question self-check — answer out loud.

> Recall the anchor before you start: a single `UPDATE` in Postgres travels
> **Postgres → Debezium → Kafka → Bronze → Silver → (Gold) → Qdrant → API/Grafana.**
> Days 3–7 are the later half of that journey: quality (3), vectors (4), serving (5),
> observability (6), and proving it all works together (7).

---

# DAY 3 — Quality Gates (Great Expectations + DLQ + dbt Gold)

## 1. What we built, in plain English

Real production data is dirty. A `monthly_price` can arrive as a negative number; an `email`
can be `not-an-email`. If bad rows silently flow into your analytics, your reports lie and
nobody notices for weeks.

Day 3 adds a **gate** between Silver and Gold: every batch of cleansed rows is validated
against a set of declarative rules (**Great Expectations suites**). Rows that pass go forward
and get **MERGE**d into the clean table. Rows that fail are **not dropped** — they're routed to
a **DLQ (Dead-Letter Queue)**, a parking lot that keeps the bad row *and the reason it failed*,
so a human can fix the source and replay it. Gold (the business-ready tables) is then built by
**dbt** from the clean Silver data only.

> The principle: **data loss is silent and unrecoverable; a DLQ is a parking lot with receipts.**
> Turning a data bug into an *observable, reversible* event is the senior move.

## 2. The steps, and why each exists

| Step | What was done | Why it exists |
|---|---|---|
| 2.1 | Wrote GE expectation suites in `spark/quality.py`: `not_null(monthly_price)`, `between` on price, regex on email, valid `status` | Declarative rules read like English; they're testable and reusable. Explicit `not_null` was added because `between` **ignores NaN** — a corrupt base64 price casts to NULL and would otherwise slip through. |
| 2.2 | Wired `validate_batch()` into the Silver `foreachBatch` step | Validation must run *before* the MERGE, per micro-batch, so bad rows never reach the clean table. |
| 2.3 | On **fail** → write to DLQ **three ways**: a Delta table, a Kafka topic `inventory.dlq.<table>`, and a Prometheus counter `dlq_records_total` | Delta = queryable archive; Kafka topic = so an automated repair worker could subscribe and replay; counter = so Grafana can *alert* on bad data arriving. |
| 2.4 | Added `ensure_dlq_table()` — pre-create the DLQ table **once** at job startup | Fixed a real bug (see §4): creating it lazily inside the stream caused a `DELTA_PROTOCOL_CHANGED` race that killed the Silver stream. |
| 2.5 | Built the **dbt** project at `./dbt`: staging views (`stg_contracts`, `stg_contract_changes`), then marts `dim_customers` (dimension) and `fct_contract_changes` (incremental fact) | dbt separates *light cleaning* (views, always fresh) from *heavy facts* (incremental, only new rows via a watermark on `source_commit_ts`). |
| 2.6 | Ran `dbt build` inside the Spark container (`method: session`), with `on-run-start` hooks registering the external Delta tables | dbt runs where Spark lives. **One statement per hook list item** — Spark rejects multi-statement `spark.sql()` calls (a real bug we hit). |

**Concept — view vs. incremental (memorize):** a **view** is a saved query — zero storage, always
current, but re-executes on every read; use it for staging. An **incremental** table physically
materializes only rows newer than a watermark and MERGEs them — fast reads, cheap at scale, but
you own the watermark and it can go stale; use it for large facts.

## 3. See it live

```bash
# Watch the quality gate catch a bad row. Insert a NEGATIVE price:
docker exec -i cdc-postgres psql -U demo -d inventory \
  -c "INSERT INTO contracts (customer_id, title, monthly_price, status)
      VALUES (1, 'Bad Price Test', -99.99, 'active');"

# Within ~15s the Silver log shows it routed to DLQ, not merged:
docker exec cdc-spark bash -c 'grep -iE "dlq|quality" /tmp/silver_job.log | tail -5'

# Query the DLQ table to see the row + the EXACT expectation it failed:
docker exec cdc-spark bash -c 'SPARK_CONF_DIR=/tmp /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  /jobs/verify_day3.py 2>/dev/null'

# Gold: changes per hour (the business question Gold answers):
docker exec -w /dbt cdc-spark dbt build
```

## 4. What broke, and the fix

1. **`DELTA_PROTOCOL_CHANGED` killed the Silver stream.** Cause: the DLQ table was created
   lazily *inside* the streaming batch, racing with concurrent writers. Fix: `ensure_dlq_table()`
   pre-creates it once at job startup. *Lesson: create auxiliary tables eagerly, not mid-stream.*
2. **Kafka publish for the DLQ failed — `Failed to find data source: kafka`.** Fix: added
   `spark-sql-kafka-0-10` to the Silver job's `--packages`. *Lesson: Spark needs the Kafka
   connector package explicitly; it isn't bundled.*
3. **dbt hook failed — Spark rejects multi-statement `spark.sql()`.** Fix: one statement per
   list item in the `on-run-start` hook.
4. **A corrupt base64 price (`'AYY8'`) cast to NULL silently.** Fix: added explicit
   `not_null(monthly_price)` because `between` ignores NaN. *This was a data-quality find, not a
   crash — 3 historic rows were flagged as known data debt.* Great "I found a silent data bug" story.

## 5. Self-check (answer out loud)

1. Why is a DLQ better than silently dropping a bad row?
2. When would you use a dbt `view` vs. an `incremental` model?
3. How would you replay a DLQ record after fixing the source?

---

# DAY 4 — Incremental Vector Sync (the differentiator)

## 1. What we built, in plain English

To answer a fuzzy question like *"can I get my money back?"* by **meaning**, contract text is
turned into vectors (embeddings) in **Qdrant**, a vector database. The naive approach re-embeds
the *entire corpus* every night — expensive and wasteful, since almost nothing changed.

Day 4 builds a **worker** that processes **only what changed** since a watermark. When one clause
of one contract is edited, it re-embeds **exactly that one chunk** — measured at **~41 tokens vs
~858 for a full rebuild, a 95.2% saving.**

> This is the #1 thing that makes you stand out. If you can explain *why* the chunk ID must be
> deterministic and *how* the watermark prevents both missed and double-processing, you've
> demonstrated real data-engineering depth.

## 2. The four ideas (this is the interview gold)

| Idea | What it is | Why it matters |
|---|---|---|
| **Deterministic chunk ID** | `uuid(sha256(document_id ":" chunk_index))` — same doc+position → same ID, forever | Makes **upsert idempotent** (a replay overwrites, never duplicates) and lets us **delete stale chunks without a lookup table** (we know exactly which IDs a doc should own). Random UUIDs break both. |
| **Watermark** | The highest *source commit timestamp* successfully synced, persisted **after** each run. Next run = `WHERE source_commit_ts > watermark`. | Time comes from **Postgres's own commit clock** (inside the CDC event), not the local wall clock, so **clock skew can't drop a row.** Written last → a crash re-processes (at-least-once); deterministic IDs turn that re-run into a harmless overwrite (exactly-once *effect*). |
| **Delete handling** | Contract removed → Qdrant delete-by-payload-filter on `contract_id`. Text shortened → retire the trailing chunk IDs the doc no longer owns. | Keeps the index from accumulating ghost vectors. |
| **Token math** | Every run logs `tokens_embedded` vs `full_corpus_tokens`. | The 80%+ saving is a **measured number**, not a claim. |

**Architecture note (say this in interviews):** the worker is a **separate Python 3.11 service**,
not the 3.8 Spark image. It reads Spark-written Silver Delta with **delta-rs** (no JVM) and embeds
with **fastembed** (ONNX, local, no API key). *"Spark WRITES Delta; a lightweight service READS
the same tables with a different engine — that's the lakehouse contract."*

## 3. The steps

| Step | What was done | Why |
|---|---|---|
| 3.1 | Added a `qdrant` service to compose; created collection `contracts` (384-d, cosine) | Vector store; cosine = similarity metric matching the embedding model. |
| 3.2 | Seeded ~9 realistic contract documents into `contract_documents` | A real corpus so the token-saving ratio is meaningful. |
| 3.3 | Wrote `vector_sync/worker.py`: chunk → embed → deterministic-ID upsert → retire stale → advance watermark | The core loop, in the safe order (see below). |
| 3.4 | Backfill mode (watermark=0 → processes everything) then loop mode (poll every 10s) | First run = natural backfill; steady state = incremental. |
| 3.5 | Verified: update one clause → 1 chunk; delete a contract → vectors purged; replay → no dupes | The four demo checklist items. |

**The safe processing order (memorize the *why*):** embed → upsert → retire stale → **advance
watermark LAST.** A crash anywhere before the last step leaves the watermark behind, so the next
run re-processes and self-heals. Never lose data, only be temporarily behind.

## 4. See it live

```bash
# Change ONE clause, then watch the worker re-embed only that chunk:
docker exec -i cdc-postgres psql -U demo -d inventory \
  -c "UPDATE contract_documents
      SET chunk_text='Refunds are processed within 5 business days.',
          content_hash='manual-1', updated_at=now() WHERE id=4;"

# Within ~15s the log shows the incremental proof + the token saving:
docker logs cdc-vector-sync 2>&1 | grep -E 'upserted|SYNC done' | tail -2
#   -> "upserted 1 chunk vectors across 1 document(s), ~41 tokens"
#   -> "SYNC done: ~41 tokens ... vs ~858 for a full rebuild -> 95.2% saved"

# Inspect the vectors directly:
curl -s http://localhost:6333/collections/contracts | python3 -m json.tool
```

## 5. What broke, and the fix

1. **`'>' not supported between datetime and int`.** delta-rs returns `source_commit_ts` as a
   `datetime`, but the watermark is an int (epoch-ms). Fix: a `to_ms()` normalizer. *Lesson:
   know your reader's types — pyspark and delta-rs type timestamps differently.*
2. **HTTP 400: `... is not a valid point ID (unsigned integer or UUID)`.** A bare 64-char sha256
   hex is neither. Fix: `chunk_id = uuid.UUID(sha256_hex[:32])` — still deterministic, valid for
   Qdrant.
3. **`'dict' object has no attribute 'schema'`.** `write_deltalake` wants a PyArrow Table, not a
   list of dicts. Fix: build an explicit `pa.table(...)`.
4. **`No matching distribution for qdrant-client==1.12.4`.** The *server* tag ≠ a *client*
   version. Fix: pin the client to the nearest compatible (1.12.2).
5. **`Pyarrow is required`** from deltalake reads. Fix: add `pyarrow` to requirements.

## 6. Self-check

1. Why must the chunk ID be deterministic? What breaks with random UUIDs?
2. What happens if the worker crashes between delete and upsert?
3. How does the watermark prevent *both* missed updates and double-processing?

---

# DAY 5 — Serving Layer (FastAPI + Hybrid Search)

## 1. What we built, in plain English

A working **API** over the two hot paths a real app needs: **semantic search** over Qdrant, and
**analytics** over the Gold tables. `api/main.py` serves it on port 8000 with auto-generated
interactive docs at `/docs`.

## 2. The concepts

- **Hybrid search** = vector similarity **PLUS a metadata filter** (e.g. only this customer's
  contracts). *Why it beats pure vector search:* enterprise data is **tenanted and governed** — a
  pure vector query returns whatever is *closest in meaning*, which can be another tenant's
  contract or an expired clause. The filter is a **correctness AND security** fix, and it runs in
  Qdrant's index so it costs no full scan.
- **Score threshold** = drop weak matches. There's always a "closest" result even when nothing is
  relevant; for RAG, irrelevant context is *worse* than none (it poisons the LLM's answer). A
  threshold converts "always something" into an honest "no good match."
- **Dependency injection** = `Depends()` + `lru_cache` share the expensive Qdrant client and
  embedding model across requests instead of rebuilding per call.
- **Same model for query AND documents** — mismatching models silently breaks similarity. The API
  image pre-downloads the same `bge-small` the worker uses.

**Endpoints:** `POST /search` (hybrid), `GET /contracts/{id}/history` (SQL over Gold),
`GET /analytics/changes-per-hour` (aggregation over Gold), `GET /health`, `GET /metrics`,
`/docs`. Auth via `x-api-key` header (wrong key → 401).

## 3. The steps

| Step | What | Why |
|---|---|---|
| 3.1 | Wrote `api/main.py` with Pydantic response models for every endpoint | Typed request/response → validation + free OpenAPI docs. |
| 3.2 | `/search` embeds the query with the same model, builds a Qdrant `Filter` if `customer_id` given, applies `score_threshold` | The hybrid-search concept, concretely. |
| 3.3 | History + analytics read Gold **by path** (`/lakehouse/gold.db/fct_contract_changes`) via delta-rs | No Spark, no metastore needed to *serve* the lakehouse. |
| 3.4 | Added `x-api-key` auth + `/metrics` endpoint (see Day 6) | Production thinking + observability. |

## 4. See it live

```bash
# Fuzzy question → the RIGHT clause by meaning (top score):
curl -s -X POST http://localhost:8000/search -H 'x-api-key: demo-key' \
  -H 'Content-Type: application/json' \
  -d '{"query":"can I get a refund if I cancel early?","top_k":3,"min_score":0.3}' \
  | python3 -m json.tool

# Hybrid filter narrows to one tenant:
curl -s -X POST http://localhost:8000/search -H 'x-api-key: demo-key' \
  -H 'Content-Type: application/json' \
  -d '{"query":"payment terms","customer_id":1}'

# Open the interactive docs in a browser:  http://localhost:8000/docs
# Analytics + history:
curl -s http://localhost:8000/analytics/changes-per-hour -H 'x-api-key: demo-key'
curl -s http://localhost:8000/contracts/1/history -H 'x-api-key: demo-key'
```

## 5. What broke, and the fix

1. **`Could not import module "main"`** — Dockerfile did `COPY main.py /` (root) but WORKDIR was
   `/app`. Fix: `COPY main.py /app/main.py`. *Lesson: WORKDIR + COPY destination must agree.*
2. **HTTP 500: `pydantic ValidationError: Input should be a valid string`** — delta-rs returns
   timestamps as `datetime`, but the Pydantic models declare `str`. Fix: stringify
   `.isoformat()`-able values before building response models.
3. **A zero-hit result looked like a bug but was correct** — `customer_id=2` returned 0 hits
   because contract 2 was deleted in the Day-4 delete test. *Lesson: verify the reason before
   "fixing" a zero result.*

## 6. Self-check

1. Why does hybrid (vector + filter) beat pure vector search for enterprise data?
2. How would you scale `/search` to 1000 QPS?
3. Why filter by score threshold?

---

# DAY 6 — Observability (Prometheus + Grafana)

## 1. What we built, in plain English

A live board proving **Postgres → Qdrant replication lag in seconds**, plus DLQ and token-spend
panels, with alerts. Prometheus **pulls** metrics from each service's `/metrics` endpoint; Grafana
**queries** Prometheus and draws panels — all **provisioned as code** (files, not clicks).

## 2. The concepts

- **Counter** = monotonic, only climbs; you `rate()` it. *Examples here:* `cdc_events_total`,
  `vector_tokens_embedded_total`.
- **Gauge** = point-in-time, moves up *and* down; read it directly. *Examples:* 
  `replication_lag_seconds`, DLQ depth. (A Gauge can even be *absent* when idle — that's not an
  error.)
- **Histogram** = distribution/percentiles (request latency; not used here).
- **Replication lag formula (senior detail):** `lag = consumer_processed_at − event_source_ts_ms`,
  where `ts_ms` is the **Postgres commit timestamp inside the CDC event**, NOT the local wall
  clock. Using the source clock means **clock skew can never fake a negative or inflated lag.**
- **Provisioning = config-as-code:** the datasource (`prometheus.yml`) and dashboard
  (`cdc_lakehouse.json`) are mounted read-only, so the stack rebuilds identical from git.

## 3. The steps

| Step | What | Why |
|---|---|---|
| 3.1 | Exposed `/metrics` on each service: silver (8001), vector-sync (8002), api (8000) | Prometheus needs an endpoint to pull. |
| 3.2 | Wrote `observability/prometheus/prometheus.yml` (3 scrape targets) + `alerts.yml` | Declarative scrape config + alert rules. |
| 3.3 | Wrote Grafana datasource + dashboard provisioning + the JSON dashboard | Dashboard-as-code. |
| 3.4 | Added alerts: `ReplicationLagHigh` (>60s for 5m), `DlqRecordsArriving`, `CdcPipelineStalled` | The three things that actually go wrong in a pipeline. |

## 4. See it live

```bash
# Prometheus targets — all three should be "up":
curl -s http://localhost:9090/api/v1/targets | python3 -c "import json,sys;[print(t['labels']['job'],t['health']) for t in json.load(sys.stdin)['data']['activeTargets']]"

# A live PromQL query (Counter wrapped in rate()):
curl -s 'http://localhost:9090/api/v1/query?query=sum%20by%20(op)(rate(cdc_events_total[1m]))'

# Grafana: open http://localhost:3000  (admin/admin) → "CDC Lakehouse" dashboard.
# Panels update live as you run psql UPDATEs.
```

## 5. What broke, and the fix

1. **Prometheus target `api: down — HTTP 404`.** The FastAPI app had **no `/metrics` route**
   (Prometheus's default scrape path). Fix: added a `prometheus_client` Counter
   (`api_requests_total{endpoint,status_code}`), a middleware to increment it, and a `/metrics`
   route returning `generate_latest()`. *Lesson: FastAPI returns 404 on /metrics by default.*
2. **`docker compose up -d prometheus grafana` timed out** — first-time image pulls outlived the
   command window. Fix: pull/start each separately with a longer timeout.

## 6. Self-check

1. Why compute lag from `source.ts_ms` instead of local time?
2. Counter vs. Gauge — one example of each from this project?
3. How does Prometheus discover its targets (and is it push or pull)?

---

# DAY 7 — End-to-End Test + Chaos + Portfolio

## 1. What we built, in plain English

Two executable proofs plus the portfolio packaging: an **E2E test** that asserts the whole chain
works, a **chaos test** that proves a Kafka outage loses zero data, and a **README** anyone can
read in 5 minutes.

## 2. The concepts

- **The chaos test's whole point:** Postgres's **WAL is the durable source of truth** and the
  Debezium **replication slot** is the bookmark. When Kafka goes down, writes keep landing in the
  WAL; when it comes back, the connector **resumes from its last offset** and replays the missed
  window. Nothing is lost. This is the practical meaning of "replay from offsets."
- **At-least-once + idempotent = exactly-once *effect*.** True exactly-once needs distributed
  transactions (2PC) — a coordinator, a failure mode, a latency cost. At-least-once is simple
  (just replay); make the consumer **idempotent** (deterministic vector IDs = overwrite; Delta
  MERGE = no-op) and processing twice = processing once. You get the *effect* with none of the
  coordination cost.
- **Tests as CI gates:** `e2e_test.sh` exits non-zero if any assertion fails, so it can gate a
  pipeline, not just demo.

## 3. The steps

| Step | What | Why |
|---|---|---|
| 3.1 | Wrote `e2e_test.sh` — 12 assertions: preconditions → psql UPDATE → Bronze → Silver → Qdrant → API search → analytics → auth | Proves every hop end-to-end with a unique token grepped through the whole chain. |
| 3.2 | Wrote `chaos_test.sh` — stop Kafka, write during the outage, restart, assert no loss | Proves durability/resume-from-offset. |
| 3.3 | Wrote `README.md` — ASCII architecture, tech-stack "why" table, copy-paste run steps, measured metrics, the four vector-sync ideas, honest limitations | The portfolio artifact. |
| 3.4 | Extended `run_all.sh` to orchestrate all 10 services + verify jobs are alive | One-command start. |

## 4. See it live

```bash
# 12-assertion end-to-end test (exit code gates CI):
./e2e_test.sh
#   -> RESULT: 12 passed, 0 failed

# Chaos test — a Kafka outage with zero data loss:
./chaos_test.sh
#   -> baseline replicates -> STOP Kafka -> write while down (correctly NOT in Bronze)
#      -> START Kafka -> the missed change reaches Bronze (replayed) -> jobs still alive
```

## 5. What broke, and the fix

1. **E2E first run failed at stage 2 — "token never reached Bronze."** Cause: both Spark
   streaming jobs were **dead** (killed during an earlier container restart) — the *container*
   was up but the *jobs* weren't. Fix: restarted bronze+silver. *Lesson: after any container
   restart, verify the JOBS are alive, not just the container.*
2. **`./run_all.sh` timed out** — it bundled slow image pulls + many waits into one command.
   Fix: started services individually with explicit timeouts; the script now verifies jobs are
   alive rather than blocking on everything.

## 6. Self-check

1. Explain the WHOLE flow end-to-end in 60 seconds (psql UPDATE → … → Grafana panel).
2. To cut latency from ~15s to <1s, what are the first three things you'd change?
3. Why is "at-least-once + idempotent" usually a better trade than "exactly-once"?

---

# The one page to revise the night before

**The journey of one change:** `UPDATE` → WAL (`wal_level=logical`) → Debezium (publication +
replication slot) → Kafka (topic, offsets, replayable) → Bronze (raw Delta) → Silver (parse +
GE gate → good=MERGE, bad=DLQ) → Gold (dbt marts) → vector-sync (chunk only what changed →
deterministic-ID upsert to Qdrant) → FastAPI (hybrid search + analytics) → Prometheus (pull
/metrics) → Grafana (panels + lag alert).

**The three ideas that set you apart:**
1. **Deterministic chunk IDs** → idempotent upserts + lookup-free deletes.
2. **Watermark from the source clock** → no missed rows, no clock-skew bugs, crash-safe.
3. **At-least-once + idempotent = exactly-once effect** → no distributed transactions needed.

**Measured numbers you can quote:** 95.2% token saving on an incremental change (1 chunk vs full
rebuild); E2E test 12/12; chaos test 0 data loss; lag alert at >60s for 5m.

**Honest limitation to say out loud:** the corpus is ~9 documents, so *absolute* token numbers are
small — the **ratio (95%+) and the mechanism** are what transfer to production.

# Lab Notebook — CDC Lakehouse & Incremental Vector Sync

> Rule from the study plan: **every error you hit + the fix goes here.** These
> become your interview stories (STAR format).

## Day 1 — CDC Foundation (PostgreSQL → Debezium → Kafka)

### What I ran and saw

| Step | Command | Result |
|---|---|---|
| 1 | `docker compose -p cdc-lakehouse up -d` | 4 containers healthy (postgres, zookeeper, kafka, connect) |
| 2 | `SHOW wal_level;` | **logical** ✅ |
| 3 | `./connect/register.sh` | HTTP 201 created; connector + task **RUNNING** |
| 4 | `kafka-topics --list` | `inventory.public.contracts`, `.customers`, `.contract_documents` |
| 5 | UPDATE + console consumer | Kafka message in **~1.8 seconds** ✅ |
| — | `pg_replication_slots` | `debezium_slot` active, plugin `pgoutput`, LSN advancing |

### The exact Kafka message (paste it here from Step 5)

UPDATE event (`op: "u"`) — key `{"id":1}`, value:

```json
{
  "before": {"id": 1, "customer_id": 1, "title": "Acme - Premium Support Plan",
             "monthly_price": "1200.00", "status": "active",
             "updated_at": "2026-10-14T14:34:17.559825Z"},
  "after":  {"id": 1, "customer_id": 1, "title": "Acme - Premium Support Plan",
             "monthly_price": "999.00", "status": "active",
             "updated_at": "2026-10-14T14:34:25.464361Z"},
  "source": {"ts_ms": 1791988465466, "lsn": 26806560, "txId": 760,
             "table": "contracts", "db": "inventory", "snapshot": false},
  "op": "u", "ts_ms": 1791988465927
}
```

DELETE event (`op: "d"`) — `after: null`, `before` = full old row, followed by a
**tombstone** message (key `{"id":3}`, value `null`).

Round-trip: UPDATE sent → Kafka message received = **~1.8 s** (target was < 5 s).

### Errors I hit + fixes

1. Error: `FATAL: unrecognized configuration parameter " wal_level"` — postgres container exited at first boot.
   Fix: In docker-compose.yml, `- -c wal_level=logical` was passed as ONE argument (with a leading
   space). Split into separate list items: `- "-c"` / `- wal_level=logical`. Then wiped the
   half-initialized volume with `down -v` and started fresh.
   Learning: Docker `command:` list items map 1:1 to argv — Postgres needs `-c` and `name=value`
   as two separate argv entries.

2. Error: UPDATE event showed `"before": null` and `"monthly_price": "AYY8"`.
   Fix (a): Postgres `REPLICA IDENTITY DEFAULT` only records the PK on changes → ran
   `ALTER TABLE ... REPLICA IDENTITY FULL` so the old row (needed Day 4 to delete stale
   embeddings) is fully captured.
   Fix (b): Debezium encodes `numeric` as base64-encoded unscaled decimal by default
   (`AYY8` = bytes [1,134,60] = 99900 ÷ scale 2 = 999.00). Set
   `decimal.handling.mode=string` in the connector config for human-readable money.
   Learning: read the raw bytes before assuming a bug — half of "weird CDC bugs" are
   encoding choices, not data loss.

### Self-test answers (Day 1)

1. Why does Debezium NOT slow down the production database?
   >
2. What happens to WAL segments if the connector is down for a day?
   >
3. What does the `d` operation look like, and why is `after` null?
   >

### My own words — what I can now explain to an interviewer

- WAL / LSN:
- Replication slot:
- At-least-once delivery & idempotency:

---

## Day 2 — Lakehouse Ingestion (Kafka → PySpark → Bronze/Silver)

### What I ran and saw

| Step | Command | Result |
|---|---|---|
| 1 | `docker compose up -d spark` | apache/spark:3.5.3, Python 3.8, jars cached in `spark_ivy` volume |
| 2 | `spark-submit ... bronze_job.py` | Batch 0 drained the Day-1 backlog: 2+5+14 events → Delta (10s micro-batches) |
| 3 | `simulate_traffic.py` + controlled c/u/d SQL | Bronze grew to 5/13/16 events |
| 4 | `spark-submit ... silver_job.py` | 3 streams, MERGE upserts committing (v0 create, v1..vN merges) |
| 5 | `spark-submit ... query_lakehouse.py` | Full proof below → `day2_demo_output.txt` |

### Bronze / Silver counts + Delta history proof

```
bronze.customers_cdc : 5 events        DESCRIBE HISTORY silver.contracts:
bronze.contracts_cdc : 13 events         v3 MERGE  (850.00 - the restart test change)
bronze.contract_documents_cdc : 16       v2 MERGE  (999.00)
                                         v1 MERGE  (1050.73)
                                         v0 WRITE  (bootstrap, 0 rows)
```

### Time travel evidence (old version vs current — different prices)

```
AS OF version 2:  id1=999.00   id2=807.74
CURRENT version3: id1=999.00   id2=850.00     <- same table, two truths, no backup copy
```

### Operation types across Silver (c / u / d / r all present)

```
silver.customers:          r (initial snapshot) 2 | c (inserts) 3
silver.contracts:          u (updates) 2
silver.contract_documents: c 1 | d (is_deleted=true) 2
```

### Restart-resume test (checkpoint proof)

```
BEFORE kill: contracts = 12 events
KILL bronze -> UPDATE contracts SET price=850 while DOWN -> RESTART bronze
AFTER:      [bronze batch 5] wrote 1 rows -> contracts = 13 events
=> exactly +1: no loss (checkpoint resumed after last offset), no duplicates
```

### Schema evolution proof

```
ALTER TABLE contracts ADD COLUMN region;  UPDATE ... region='bengaluru'
1. Bronze: new column appears inside raw JSON with ZERO code change (schema-on-read)
2. Silver: added StructField("region") in ONE line + restart
   -> Delta autoMerge appended `region string` to the table schema
   -> new row region='blr-east', OLD row region=NULL (old files never rewritten)
```

### Errors I hit + fixes

1. Error: Kafka container exited at morning restart: `NodeExists` in ZooKeeper.
   Fix: waited for the old broker session to expire (~18s), `compose up -d` again.
   Learning: ZK ephemeral nodes vanish only on session expiry — cold restarts need patience.

2. Error: `AnalysisException [INVALID_SCHEMA.PARSE_ERROR] ... Unrecognized token 'STRUCT'`.
   Fix: Spark 3.5 `from_json` rejects DDL strings like `STRUCT<...>`; rebuilt the
   CDC envelope programmatically as `StructType` objects in `cdc_common.py`.
   Learning: schema strings in Spark APIs are usually JSON-schema, not DDL.

3. Error: `DELTA_MISSING_DELTA_TABLE ... is not a Delta table` in Silver's first MERGE.
   Fix: bootstrap — if `DeltaTable.isDeltaTable()` is false, write an empty DataFrame
   with the batch schema first, then MERGE. Learning: MERGE targets must exist.
   (Bonus learning: because the failed batch never committed its checkpoint offset,
   it reprocessed automatically on restart = at-least-once working as designed.)

4. Error: two all-NULL rows appeared in `silver.contract_documents`.
   Fix: those were Kafka TOMBSTONES (value=null after a delete). Added
   `.filter(kafka_value.isNotNull())` in Silver (Bronze keeps them raw), deleted the
   2 bad rows with a Delta DELETE. Learning: tombstones are a signal, not data.

5. Error: my own `grep -v INFO | head -60` pipeline killed a streaming job's stdout
   (SIGPIPE). Fix: redirected logs to a file instead of piping through head.
   Learning: never truncate the stdout of a long-running job you care about.

### Self-test answers (Day 2)

1. What does the checkpoint directory contain and what breaks if you delete it?
   >
2. How do you query yesterday's version of a Silver table?
   >
3. Why keep Bronze at all if Silver is clean?
   >

---

## Day 3 — Quality Gates (Great Expectations + DLQ + dbt Gold)

### What I ran and saw

```
1. GE suite wired into Silver's foreachBatch: validate -> pass to MERGE / fail to DLQ
2. Inject bad data:
     UPDATE contracts SET monthly_price = -99.99 WHERE id = 1;
     UPDATE customers SET email = 'not-an-email' WHERE id = 2;
   -> DLQ now has 2 rows, each with the exact failed expectation + value:
        contracts id=1  FAILED expect_column_values_to_be_between  monthly_price=['-99.99']
        customers id=2  FAILED expect_column_values_to_match_regex email=['not-an-email']
   -> Silver KEPT the good values (999.00 / billing@zenith.example): MERGE skipped for bad rows
3. dbt build: PASS=18 WARN=0 ERROR=0  (4 models + 14 tests + 6 hooks)
4. Gold answers the business question:
     select change_hour, cdc_op, count(*) from gold.fct_contract_changes group by 1,2;
     2026-10-15 07:00 | u | 9   <- "changes per hour"
```

### Architecture (interview-grade)

```
CDC event -> dedup/latest-per-key -> GE EXPECTATION SUITE
                                      |--pass--> Silver MERGE --> dbt (staging view -> Gold marts)
                                      +--fail--> DLQ (Delta table + Kafka topic + Prometheus counter)
                                                        with reason per row, for repair & replay
```
- **Circuit breaker**: a failing row never reaches Silver/Gold — it's parked with context.
- **dbt DAG**: `source()` = Silver/Bronze I don't own; `ref()` = model-to-model deps.
- **Materializations**: staging = `view` (always fresh, zero storage); `fct_contract_changes`
  = `incremental` (only NEW events since `max(source_commit_ts)` watermark, MERGE by
  `change_uid`). This is the SAME "process only what changed" idea as Day 4 vectors.

### Errors I hit + fixes (Day 3)

1. Error: `dbt-spark[session]==1.7.4` -> `No matching distribution` (versions jump 1.7.1 -> 1.8.0).
   Fix: pinned `dbt-core==1.7.1` + `dbt-spark==1.7.1`. Learning: always check real versions.

2. Error: pip crashed `InvalidVersion: '2004d'` during image build.
   Fix: use `python3 -m pip` (NOT bare `pip`) after upgrading — bare `pip` still resolved to the
   OLD pip20 binary in /usr/bin. Learning: upgrade + invoke the same interpreter path.

3. Error: GE pulled `jupyterlab>=4.4.9` (needs Python 3.9+, image is 3.8).
   Fix: `notebook<7` pin (last 3.8-compatible). Learning: read the transitive dep chain.

4. Error: `dbt-spark[session]` tried to download the ~300MB pyspark wheel.
   Fix: install `dbt-spark` BARE + set `PYTHONPATH=/opt/spark/python:...py4j...zip` to reuse the
   pyspark already in the image. Learning: don't ship two copies of a big runtime.

5. Error: every `dbt` command -> `TypeError: MessageToJson() got an unexpected keyword
   'including_default_value_fields'`.
   Fix: `protobuf<5` (dbt-core 1.7 uses a kwarg protobuf 5.29 removed). Learning: pin when a
   library calls into a fast-moving dependency.

6. Error: `[TABLE_OR_VIEW_NOT_FOUND] silver.contracts` — the on-run-start hook silently did nothing.
   Two bugs: (a) Jinja comment closed as `-#%}` instead of `-#}` swallowed the CREATE SCHEMA lines;
   (b) dbt runs each hook string as ONE `spark.sql()` call and Spark rejects multiple `;` statements
   in one call. Fix: fixed the comment + split into one statement per list item.
   Learning: Jinja comment delimiters are easy to typo; Spark is single-statement per call.

7. Error: `Invalid incremental strategy provided: merge ... set file_format to 'delta'`.
   Fix: add `file_format='delta'` to the model config. Learning: dbt-spark merge needs Delta.

8. Error: GE `between` threw `TypeError: ... must either be None or of the same type` — Silver's
   `monthly_price` is a STRING (decimal.handling.mode=string) but min/max were floats.
   Fix: cast numeric columns in a VALIDATION-ONLY pandas copy (`pd.to_numeric(..., errors='coerce')`)
   so the merged row is untouched. Bonus: corrupt base64 `'AYY8'` coerces to NaN -> caught by not_null.
   Also: GE's validate kwarg is `expectation_suite=` (an ExpectationSuite object), not `expectations=`.
   And `between` IGNORES NaN by default -> must add an explicit not_null on the money column.

9. Error (REAL concurrency bug): `[DELTA_PROTOCOL_CHANGED] ... multiple writers writing to an empty
   directory` KILLED the silver-contracts stream when 2 streams failed validation in the same tick.
   Fix: `ensure_dlq_table()` pre-creates the shared DLQ Delta table ONCE at startup before streams
   start (Delta's own error message prescribes this). After that, streams only blind-append.
   Learning: sharing a Delta table across concurrent writers requires creating it ahead of time.

10. Error: `[dlq] Kafka publish failed ... Failed to find data source: kafka`.
    Fix: the Silver job needs `spark-sql-kafka-0-10` on --packages (added in run_all.sh).

11. Data finding (not a crash): 3 HISTORIC base64 prices (`'AYY8'`) from Day 1 events captured
    BEFORE `decimal.handling.mode=string` was set. Spark `cast()` turns them into NULL silently.
    Caught going forward by the new not_null(money) expectation; documented as known data debt.

### Self-test answers (Day 3)

1. Why is a DLQ better than silently dropping bad rows?
   >
2. What is the difference between a dbt `view` and `incremental` materialization, and when to use each?
   >
3. How would you replay DLQ records after fixing the source?
   > (1) Read DLQ Delta, filter `replayed_at_ms IS NULL`. (2) Fix the source row in Postgres.
   > (3) The fix emits a NEW CDC event that re-enters the pipeline and passes validation — you
   > let the correction flow rather than re-pushing the DLQ row. (4) Set `replayed_at_ms`.
   > The DLQ Kafka topic (inventory.dlq.*) exists so an automated repair worker could replay
   > without a human.

---

## Day 4 — Incremental Vector Sync (Silver -> chunk -> embed -> Qdrant)

### What I ran and saw

```
1. Backfill: 11 Silver docs -> chunk -> embed (bge-small-en-v1.5, 384 dims)
   -> Qdrant collection 'contracts' with DETERMINISTIC UUID point ids.
   Result: 9 chunk vectors (contract 1: 5, contract 2: 4).
2. Semantic search sanity check:
     query "can I get my money back if I cancel?"
     -> top hits are the Termination/refund clauses (scores 0.654, 0.640, 0.624)
        i.e. the RIGHT clauses by MEANING, not keyword.
3. INCREMENTAL (the money shot):
     UPDATE contract_documents SET chunk_text='Refund policy ... full refund within 90 days' WHERE id=4;
     -> [vector-sync] upserted 1 chunk vectors across 1 document(s), ~41 tokens
     -> SYNC done: ~41 tokens for this change vs ~858 for a full rebuild -> 95.2% SAVED
4. DELETE:
     DELETE contract 2 -> [vector-sync] purged all vectors for deleted contract_id=2
     -> Qdrant points 9 -> 5 (contract 1 remains)
5. IDEMPOTENCY:
     reset watermark to 0, replayed the whole corpus
     -> upserted 9 chunk vectors but Qdrant point count stayed 9 -> NO DUPLICATES
```

### The four ideas (interview-grade)

```
1. DETERMINISTIC CHUNK ID  = uuid(sha256(document_id || ':' || chunk_index))
     same doc+position -> same id, forever
     -> upsert is idempotent (replay overwrites, never duplicates)
     -> we can delete stale chunks WITHOUT a lookup table (we know what a doc SHOULD own)
2. WATERMARK = highest source-commit-ts successfully synced, persisted AFTER the run
     next run = "Silver rows WHERE source_commit_ts > watermark"
     source time comes from POSTGRES's commit clock (inside the CDC event), not local wall
     clock -> clock skew can't drop rows. Written LAST -> crash re-processes (at-least-once),
     deterministic ids make that re-run a harmless overwrite (exactly-once EFFECT).
3. DELETE HANDLING: contract removed -> Qdrant delete-by-payload-filter contract_id;
     text shortened -> retire trailing chunk ids using the tracked chunk_count.
4. TOKEN MATH: log tokens_embedded vs full_corpus_tokens -> the 80%+ saving is MEASURED.
```

Architecture note: the worker is a SEPARATE python:3.11 service (not the 3.8 Spark image).
It reads the Spark-written Silver Delta tables with **delta-rs** (no JVM) and embeds with
**fastembed** (ONNX, local, no API key). Interview point: "Spark WRITES Delta; a
lightweight service READS the same tables with a different engine — that's the lakehouse
contract." Embeddings are LOCAL (bge-small, 384 dims), so no OpenAI key or network at runtime.

### Errors I hit + fixes (Day 4)

1. Error: `pip install` in the vector-sync image -> `ReadTimeoutError` from files.pythonhosted.org
   (large onnxruntime wheel on a slow link).
   Fix: `pip install --timeout 120 --retries 10`. Learning: raise pip's timeout/retries for big wheels.

2. Error: `'DeltaTable' object has no attribute 'to_pylist'`.
   Fix: delta-rs exposes `.to_pyarrow_table()` (pyspark uses `.to_pylist()`); then
   `.to_pylist()` on the *PyArrow Table*. Two different objects with confusingly similar names.

3. Error: `Pyarrow is required, install deltalake[pyarrow] ...`.
   Fix: add `pyarrow` to requirements (deltalake needs it for reads).

4. Error: `No matching distribution found for qdrant-client==1.12.4`.
   Learning: the SERVER tag (qdrant/qdrant:v1.12.4) is not a CLIENT version — client jumps
   1.12.2 -> 1.13.0. Pin the client to the nearest compatible (1.12.2; major equal, minor<1).

5. Error: `'>' not supported between instances of 'datetime.datetime' and 'int'`.
   Root cause: delta-rs returns source_commit_ts as a datetime, but the watermark is an int
   (epoch-ms). Fix: `to_ms()` normaliser converts every timestamp to epoch-ms before compare.

6. Error: HTTP 400 `value <64-hex-sha256> is not a valid point ID (unsigned integer or UUID)`.
   Root cause: Qdrant point ids must be an int or a UUID — a bare sha256 hex is neither.
   Fix: `chunk_id = uuid.UUID(sha256_hex[:32])` -> still deterministic, valid for Qdrant.

7. Error: `'dict' object has no attribute 'schema'` when persisting the watermark.
   Fix: `write_deltalake` wants a PyArrow Table / DataFrame, not a list of dicts -> build an
   explicit `pa.table(...)` schema so watermark stays int64 across runs.

8. Design fix: the container's default process ran `--mode sync` once and EXITED. Set the
   compose command to `--mode loop` (watermark=0 on first run = natural backfill, then it
   keeps polling). Manual one-shots use `--no-metrics` so they don't collide on port 8002.

### Self-test answers (Day 4)

1. Why must the chunk ID be deterministic? What breaks with random UUIDs?
   > Determinism gives two properties for free: (a) **idempotent upsert** — replaying the same
   > change overwrites the same point instead of creating a ghost duplicate, and (b) **lookup-free
   > deletion** — we can compute exactly which IDs a document *should* own and retire the stale
   > tail. With random UUIDs, a re-run mints brand-new IDs (duplicates accumulate) and you cannot
   > know what to delete without keeping a separate chunk-ID ledger that can itself drift.
2. What happens if the worker crashes between delete and upsert?
   > The watermark is only written AFTER the whole batch succeeds, so it still points before this
   > change. On restart the worker re-reads the same rows and re-embeds them → the gap self-heals.
   > This is why the order is (embed → upsert → retire stale → advance watermark): a crash can
   > never leave data permanently missing, only temporarily behind (at-least-once + idempotent
   > overwrite = exactly-once effect).
3. How does the watermark prevent both missed updates and double-processing?
   > It prevents **missed** updates because every run scans `source_commit_ts > watermark`, and the
   > source timestamp is Postgres's own commit clock (carried inside the CDC event), so no row can
   > be skipped by clock skew. It prevents **double-processing** because the watermark advances only
   > past what was fully handled, so the next scan starts exactly there. And even if a crash forces
   > a small overlap (double-processing a few rows), deterministic IDs make the re-upsert a
   > harmless overwrite — so double-processing is safe, not corrupting.

---


---

## Day 5 — Serving Layer (FastAPI + Hybrid Search)

### What I ran and saw

```
POST /search {"query":"can I get a refund if I cancel early?","top_k":3}
  -> score=0.7158 contract=1 :: Refund policy updated: Acme is entitled to a full ...   (RIGHT clause, top)
  -> score=0.6738 contract=1 :: Termination for cause permits either party to exit ...
  -> score=0.6013 contract=1 :: Payment terms for this agreement are net thirty days ...
Hybrid filter {"query":"payment terms invoice","customer_id":1} -> 5 hits, ALL contract_id=1
                                       (customer_id=2 -> 0 hits; correct, contract 2 was deleted on Day 4)
GET /contracts/1/history   -> 9 rows, newest first (u price=999.0 @ 2026-10-15T07:59:46 ...)
GET /analytics/changes-per-hour -> 4 buckets (2026-10-15T07 u -> 9, 2026-10-14T14 r -> 2 ...)
POST /search with wrong x-api-key -> 401      /docs -> 200
```

### The ideas (interview-grade)

```
* Hybrid search = vector similarity + METADATA filter (e.g. only this customer's contracts).
  Pure vector search happily returns another tenant's clauses -> a DATA-LEAK bug. Filters are
  not a nicety in enterprise RAG, they are a correctness/security requirement.
* Score threshold: below X we return nothing rather than irrelevant context — for an LLM (RAG)
  bad context is worse than no context (it poisons the answer).
* Same embedding model for query AND documents (bge-small, 384-d) — mismatching models silently
  break similarity. The API image pre-downloads the same model the worker uses.
* FastAPI: Depends() shares the expensive Qdrant client + embedder across requests (lru_cache),
  Pydantic models give validation + the free /docs. The API reads GOLD Delta by PATH with
  delta-rs — no Spark, no metastore. Any engine that speaks Delta can serve the lakehouse.
```

### Errors I hit + fixes (Day 5)

1. Error: `Could not import module "main"` — uvicorn started in WORKDIR /app but the Dockerfile
   did `COPY main.py /` (root). Fix: `COPY main.py /app/main.py`. Learning: WORKDIR + COPY
   destination must agree; the module must be on the process's cwd/sys.path.
2. Error: HTTP 500 on /history + /analytics — `pydantic ValidationError: HourlyBucket ... Input
   should be a valid string`. Root cause: delta-rs returns timestamp columns as `datetime`
   objects, but the Pydantic models declare `str`. Fix: stringify any `.isoformat()`-able value
   in `read_gold_fct()` before building response models.
3. Note: customer_id=2 filter returned 0 hits and looked like a bug — it was CORRECT (contract 2
   was purged from Qdrant in the Day-4 delete demo). Lesson: verify the *reason* before
   "fixing" a zero result.
4. Bug I caught by reading (self-review): used `MatchAny` before importing it. Fixed the import
   order before it ever ran.

### Self-test answers (Day 5)

1. Why hybrid (vector + filter) beats pure vector search for enterprise data?
   > Because enterprise data is TENANTED and GOVERNED. A pure vector query "returns whatever is
   > closest in meaning" — which can be another customer's contract, a draft, or an expired
   > clause. Hybrid search adds a metadata filter (customer_id, effective_date <= today, status)
   > so similarity is only computed *inside* the allowed slice. It's both a correctness fix
   > (don't surface stale/expired terms) and a security fix (no cross-tenant leakage). The filter
   > runs in Qdrant's index, so it doesn't cost you a full scan.
2. What would you do to scale `/search` to 1000 QPS?
   > (1) The embedder + Qdrant client are already lru_cached/shared — the per-request cost is one
   > ONNX forward pass + a ANN query, both fast. (2) Add replicas behind a load balancer (the API
   > is stateless; Qdrant scales by shards/replicas). (3) Cache hot queries (exact-query LRU or
   > Redis) since the corpus changes ~1/10s. (4) Pre-warm/batch embeddings if query volume makes
   > the single forward pass a bottleneck, or move embedding to a dedicated small model server.
   > (5) Push filters + top-k into Qdrant (already done) and keep payloads small. The Gold
   > analytics endpoints would move to a materialized/OLAP store rather than reading Delta per call.
3. Why do you filter by score threshold?
   > Cosine similarity is unbounded in *usefulness* — there's always a "closest" match even when
   > nothing is relevant. Without a threshold, a fuzzy question about something absent from the
   > corpus still returns top-k hits, and for RAG that's poison (the LLM confidently answers from
   > irrelevant context). A threshold converts "always something" into an honest "no good match,"
   > which the caller can handle (ask a clarifying question, fall back to keyword, etc.).

---

## Day 6 — Observability (Prometheus + Grafana)

### What I ran and saw

```
Prometheus targets (all UP):  api  up | silver  up | vector-sync  up
Queryable series: replication_lag_seconds(1) dlq_records_total(1) cdc_events_total(1)
                  vector_tokens_embedded_total(1) api_requests_total(4 by endpoint+status)
Grafana (provisioned AS CODE): datasource Prometheus -> http://prometheus:9090
                  dashboard uid=cdc-lakehouse-lag "CDC Lakehouse - Replication Lag & Vector Sync"
Panels: lag stat + lag over time by stage, CDC events/sec by op, chunks+tokens embedded,
        DLQ total stat (green->red at >=1), active-alert list.
Alerts: ReplicationLagHigh (qdrant lag > 60s for 5m), DlqRecordsArriving (increase>0/10m),
        CdcPipelineStalled (no events 10m).
```

### The ideas (interview-grade)

```
* Counter vs Gauge vs Histogram: Counter = monotonic count (cdc_events_total, tokens spent —
  you RATE them); Gauge = point-in-time value that goes up AND down (replication_lag_seconds,
  DLQ depth); Histogram = distribution/percentiles (request latency — not used here).
* Prometheus is a PULL model: it scrapes each service's /metrics (Prometheus exposition format).
  The API needed an explicit /metrics route — FastAPI returns 404 there by default (that's the
  'api: down 404' I fixed). Silver serves 8001, vector-sync 8002, api 8000.
* Replication lag = consumer_processed_at - event_source_ts_ms, and event_source_ts comes from
  the POSTGRES commit clock inside the CDC event, NOT the local wall clock. Using the source
  clock means clock skew between machines can never fake a negative/inflated lag. Senior detail.
* Provisioning = config-as-code: datasources + dashboards are files mounted read-only, so the
  whole observability stack is reproducible from git (no clicking through the UI).
* The API metrics port: prometheus scrapes host.docker.internal:800X (extra_hosts host-gateway
  on Linux maps that to the host where the ports are published).
```

### Errors I hit + fixes (Day 6)

1. Error: Prometheus target `api: down — server returned HTTP status 404`. Root cause: the
   FastAPI app had no `/metrics` route (Prometheus's default scrape path). Fix: added a
   prometheus_client Counter (api_requests_total{endpoint,status_code}) + a middleware to
   increment it + a `/metrics` endpoint returning generate_latest(). All 3 targets now `up`.
2. Error: the earlier `docker compose up -d prometheus grafana` timed out with no output —
   the images weren't pulled yet and the pull outlived the command's window. Fix: pull/start
   them separately with a longer timeout (`up -d prometheus` then `up -d grafana`). Learning:
   first-run image pulls are slow; don't bundle a big pull into a short command.
3. Note: silver metrics (8001) needed the port published from the spark container — added
   "8001:8001" to the spark service so the Prometheus container can reach it via the host.

### Self-test answers (Day 6)

1. Why compute lag from `source.ts_ms` instead of local time?
   > Because `source.ts_ms` is the POSTGRES COMMIT timestamp carried inside the CDC event — a
   > single, authoritative clock. If you used each machine's local clock, clock skew between the
   > database, the connector host, and the consumer would produce fake lag (even negative lag).
   > Using the source clock means lag = "how old is the newest event I've processed, measured by
   > the clock that created it" — accurate across any number of machines with unsynchronized clocks.
2. What is the difference between a Counter and a Gauge - give one example of each from this project?
   > A Counter only goes up (you `rate()`/`increase()` it): `cdc_events_total` and
   > `vector_tokens_embedded_total` — "how many events/tokens total, and how fast right now."
   > A Gauge can go up AND down and represents a point-in-time value: `replication_lag_seconds`
   > (lag rises and falls) and DLQ depth. That's why the lag alert uses the Gauge directly
   > (`> 60`) while a "no events" alert must use `increase(counter[10m]) == 0`.
3. How does Prometheus discover your targets?
   > By PULL: Prometheus scrapes a configured list of `/metrics` HTTP endpoints on an interval.
   > Here it's static service-discovery — `scrape_configs` names `host.docker.internal:8001`
   > (silver), `:8002` (vector-sync), `:8000` (api). In production you'd swap static configs for
   > dynamic discovery (Kubernetes pod annotations, Consul, etc.) so targets are added/removed
   > automatically. The services themselves just expose the exposition format; they never call out.

---

## Day 7 — End-to-End Test + Chaos + Portfolio

### What I ran and saw

```
./e2e_test.sh  ->  12 passed, 0 failed
  preconditions(connect/spark/qdrant/api) + psql UPDATE -> Bronze -> Silver -> Qdrant
  -> FastAPI search reflects NEW wording -> analytics endpoints -> auth 401.
  The one change re-embedded exactly 1 chunk (~27 tokens).

./chaos_test.sh ->  PASS everywhere, ZERO data loss
  baseline token replicates -> STOP Kafka -> write to Postgres WHILE Kafka is down
     (correctly NOT in Bronze yet; source WAL still has it) -> START Kafka
     -> the missed change reaches Bronze (replayed from offsets) + the concurrent
     contracts price update reaches Silver -> connector back RUNNING, jobs alive.

README.md written: ASCII architecture, tech-stack "why" table, copy-paste run steps,
  measured metrics table, the four vector-sync ideas, resilience proof, layout, limitations.
```

### The ideas (interview-grade)

```
* Why the Kafka outage loses NOTHING: Postgres's WAL is the durable source of truth and the
  Debezium offset is stored in Kafka — on restart the connector resumes from its last committed
  offset and replays exactly the missed window. The demo PROVES it: a write during the outage
  appears in Bronze after recovery.
* e2e_test.sh gates on exit code (FAIL count), so it can be a CI gate, not just a demo.
* At-least-once delivery + idempotent consumers (deterministic vector IDs, Delta MERGE) = the
  practical route to exactly-once EFFECT without distributed transactions.
* Portfolio honesty: the README states the corpus is small and the ABSOLUTE token numbers scale
  with corpus size — the RATIO (95%+) and the MECHANISM are what transfer to production.
```

### Errors I hit + fixes (Day 7)

1. Error: e2e_test.sh first run FAILED at stage 2 — "token never reached Bronze". Root cause:
   both Spark streaming jobs were DEAD (0 processes) — killed during earlier container
   restarts. Fix: restarted bronze+silver via spark-submit; re-ran e2e -> 12/12 PASS. Learning:
   after any container restart, verify the JOBS are alive (not just the container).
2. Error: `./run_all.sh` timed out with no output. Root cause: it bundled slow first-time image
   pulls + multiple `docker exec` waits into one long command. Fix: started services/jobs
   individually with explicit timeouts; run_all.sh now starts jobs and just verifies they're
   alive rather than blocking on everything.
3. Note: run_all.sh's vector-sync step was stale (it manually spawned backfill+loop). The
   container now runs `--mode loop` via its compose command (watermark=0 first run = natural
   backfill, then poll). Simplified the script to just confirm it's alive.

### Self-test answers (Day 7)

1. Explain the WHOLE flow end-to-end in 60 seconds (psql UPDATE -> ... -> Grafana panel).
   > "A row changes in Postgres. Debezium is reading Postgres's WAL, so it sees the change
   > instantly and publishes an event to a Kafka topic — no queries against production, no impact.
   > The Spark bronze stream lands that raw payload into Delta. The silver stream parses it,
   > runs it through a Great Expectations gate, and MERGEs good rows into the cleansed table —
   > bad rows go to a DLQ, not Gold. dbt turns Silver into Gold marts (an incremental fact with a
   > watermark). In parallel, a small Python worker polls Silver, chunks only the documents that
   > changed since its watermark, embeds them, and upserts into Qdrant under deterministic IDs —
   > so only the touched chunks are re-embedded, not the corpus. FastAPI serves semantic search
   > over Qdrant plus analytics over Gold. Prometheus scrapes all of it and Grafana shows the
   > Postgres-to-Qdrant lag in seconds, with an alert if it exceeds 60s."
2. If you had to cut latency from ~15s to <1s, what are the first three things you'd change?
   > (1) Kill the 10s poll: have the vector worker consume the Silver/Kafka change stream
   > directly (structured streaming / a Kafka consumer) instead of polling Delta. (2) The CDC
   > path itself is already near-real-time, so the biggest wins are removing batching windows in
   > the Spark triggers (use `availableNow`/trigger=processingTime minimal or continuous) and
   > moving embedding to a small always-warm service so there's no cold-start. (3) Bypass per-row
   > MERGE overhead for hot paths by micro-batching or writing straight through where correctness
   > allows. Each step trades some of the current simplicity/throughput guarantees for latency —
   > worth stating that trade out loud.
3. Why is "at-least-once + idempotent" usually a better trade than "exactly-once"?
   > True exactly-once across systems needs distributed transactions (2PC) or coordinated
   > commits, which add a coordinator, a failure mode, and latency — and most systems don't
   > actually offer it end-to-end anyway. At-least-once delivery is simple and resilient (just
   > replay), and if the consumer is IDEMPOTENT — deterministic vector IDs mean an overwrite, Delta
   > MERGE means a no-op on duplicates — then processing a message twice has the same effect as
   > once. You get exactly-once EFFECT with none of the distributed-coordination cost. This project
   > demonstrates it: the chaos test replays after an outage, and the idempotency test shows a
   > full replay leaves Qdrant's point count unchanged.

## Post-Day-7 Live Debugging Session — Three Real Bugs (split-brain / stale checkpoint / offset-collision dedup)

> Triggered by: I asked to watch the Grafana dashboard move on a live psql change. The
> metrics stayed flat, and chasing that surfaced three genuine, senior-level production bugs
> in the running system — plus two mistakes I made while fixing them. This is the richest
> interview material in the whole project.

### Bug 1 — Split-brain stream processors (counters read 0 while data flowed)

- **Symptom:** `sum(cdc_events_total)` = 0 on :8001, yet the psql change clearly reached
  Bronze AND Silver (grep found the new value in both tables). "Data moved, metric lied."
- **Root cause:** After earlier restarts I had **5 competing `silver_job.py` processes**. The
  metrics HTTP server binds :8001 to whichever process grabs the port FIRST — an old, stale one
  that emits `# HELP`/`# TYPE` but never calls `.inc()`. The other 4 did the real work, so their
  increments never reached the endpoint Prometheus scrapes. (`cdc_events_total` is *labelled* by
  `op`, so a labelled Counter with no observations prints HELP/TYPE but **no value line** — that
  exact signature is how I knew `.inc()` never ran in the serving process.)
- **Fix:** Kill every silver/bronze PID **explicitly by number** (see mistake #1 below for why
  `pkill -f` failed), restart exactly ONE of each, verify with `ps -eo pid,args | grep
  "python3 /jobs/silver_job.py"`. Confirmed a single process (e.g. PID 15015) after cleanup.
- **Interview quote:** *"After a restart I had duplicate stream processors; a stale one held the
  metrics port, so counters read zero while data actually flowed. The lesson: process liveness ≠
  correct metrics, and you must guarantee a single consumer per checkpoint or you get split-brain."*
- **Hidden trap I hit:** counting processes with `grep -c silver_job.py` lied, because
  `docker exec ... bash -c '... silver_job.py'` creates a wrapper whose OWN command line contains
  the string, plus grep's own subshell. Always inspect the **actual `ps` list**, not a count.

### Bug 2 — Stale checkpoint after `kill -9` (Bronze had the data, Silver silently skipped it)

- **Symptom:** Committed **Bronze contained `force-1`** (verified via Delta read, not grep), but
  committed **Silver id=4 still showed the Oct-16 chaos value**. Silver's log showed:
  `[quality 12] contract_documents: 0 passed, 0 parked` → `all rows rejected -> MERGE skipped`.
- **Root cause:** I ran `kill -9` on the Spark streaming job **mid-batch**. Spark writes the batch
  **offset at the start** and the **commit marker at the end**; a hard kill left the checkpoint
  saying "this range is processed" while the Delta MERGE never committed. On restart Silver trusts
  its checkpoint, reads "nothing new," and **permanently skips** rows sitting in Bronze.
- **Fix:** Reset the Silver checkpoint and **replay from Bronze** — safe because the MERGE is
  idempotent (at-least-once + idempotent = exactly-once *effect*). Proof: after
  `rm -rf /lakehouse/_checkpoints/silver_*` + restart, Silver id=4 became `force-1` (fresh
  `source_commit_ts 2026-10-18 15:01:08`). **Never `kill -9` a streaming job — use SIGTERM** so
  it can commit cleanly.
- **Interview quote:** *"A hard kill left my Silver table stale — Bronze had the data but the
  checkpoint claimed it was already processed, so the merge was silently skipped. I reset the
  checkpoint and replayed from Bronze. That's exactly why the medallion architecture keeps Bronze
  as a durable replay log and why the merge is idempotent."*

### Bug 3 ⭐ — Offset-collision dedup (new updates vanished as "duplicates") — the crown jewel

- **Symptom:** Brand-new document changes reached committed Bronze but Silver's batch read
  `0 passed, 0 parked` and dropped them. A "fresh" message carried **kafka_offset 9** while
  Bronze's max was **36** — a new message should never have a low, old offset.
- **Evidence (the smoking gun):**
  ```
  NEW msg (Oct 18): offsets 5,6,7,8,9   <- live-demo-1 = offset 9
  OLD msg (Oct 16): offsets 25-36
  COLLISIONS: offsets 5,6,7,8,9 each appear 2x   in Bronze
  offset 9: lsn 26805368 (op=c, OLD) AND lsn 26949120 (op=u, NEW)
  ```
- **Root cause:** The Debezium connector restarted during the chaos and began producing with
  **reused low Kafka offsets**. Bronze (raw, at-least-once) appended both copies. Silver's
  `dropDuplicates(["kafka_partition","kafka_offset"])` — meant to collapse the *same* message
  delivered twice — kept the first (usually OLD) copy and **silently discarded the new data** as
  a duplicate. At-least-once dedup keyed on Kafka offset is only safe if offsets are globally
  unique forever; they are not across a connector/topic restart.
- **Fix:** Change the dedup key to **`(LSN, kafka_key)`** in `spark/silver_job.py`:
  - **LSN** (Postgres log-sequence number) is a **monotonic WAL position** → the same change
    always replays to the same LSN, stable across any restart.
  - **`kafka_key` (the PK) is required** because every row in an initial **SNAPSHOT shares one
    LSN** (verified: LSN `26774088` appeared 3× for `op=r`). Without the key, snapshot rows would
    collapse into one. With it: `contract_documents: 11 passed` on replay — nothing collapsed.
  - Implementation: `.withColumn("_lsn", F.col("evt.source.lsn").cast("string"))` then
    `.dropDuplicates(["_lsn", "kafka_key"])`.
  - **Proof:** after fix + checkpoint reset, Silver id=1 became `live-demo-1` (the exact change
    offset-dedup had been eating) and id=5 became `probe-fix-1`.
- **Interview quote:** *"At-least-once dedup keyed on Kafka offset is only safe if offsets are
  globally unique forever. When my connector restarted and reused low offsets, dedup silently
  discarded new data. I moved the key to the Postgres LSN — a monotonic WAL position — combined
  with the primary key, because snapshot rows all share one LSN."*

### Two mistakes I made while fixing (honest, and both are teachable)

1. **`pkill -f "silver_job.py"` killed its own wrapper.** The `bash -c '... silver_job.py ...'`
   wrapper's command line *contains* the pattern, so pkill matched and terminated it before
   signalling the real processes — my "kill all" silently did nothing and a restart ADDED a 3rd
   process tree. **Fix:** kill by explicit PID (`kill -9 <pid>`), never by pattern here.
2. **`dropDuplicates` with Column expressions crashed Silver.** I first wrote
   `.dropDuplicates([F.col("evt.source.lsn").cast("string").alias("lsn"), F.col("kafka_key")])`.
   This Spark build's `dropDuplicates` takes column-**name strings**, not Column objects:
   `PySparkTypeError: [NOT_ITERABLE] Column is not iterable` → job died at startup. **Fix:**
   materialize the column first with `withColumn`, then pass names: `.dropDuplicates(["_lsn",
   "kafka_key"])`. Lesson: `py_compile` only checks *syntax* — it passed, but the bug was an API
   misuse that only surfaced at runtime.
3. **Bonus:** my "live" trigger updated a **deleted row** (`WHERE id=2` — deleted in Day 4), so
   the UPDATE matched 0 rows, Debezium correctly emitted nothing, and I briefly misread a flat
   metric as a pipeline bug. **Fix:** always verify the target row exists
   (`select id from ... where id=1`) before triggering a demo.

### The working live demo (after all fixes) — what a healthy spike looks like

```
t        cdc_events   chunks_embedded   lag_s
baseline   17            4              338.4   (stale gauge from earlier restart)
t+20s      18 <- +1         4              338.4   Silver merged the op="u" event
t+25s      18            5  <- +1      20.1 <- drop   worker embedded the chunk, lag recomputed
```
- `cdc_events` +1 → Silver processed & merged the update (honest counter now).
- `chunks_embedded` +1 → vector-sync read Silver, embedded, upserted to Qdrant.
- `lag_s` 338.4 → 20.1 → the gauge recomputed from the **source-commit clock**: the change was
  ~20s old when it landed in the vector DB. The 338.4 was stale; a fresh sync overwrote it.

### Self-test answers (Live Debugging)

1. Why could data be flowing while the metric reads zero?
   > Because the metric endpoint can be held by a *different*, stale process than the one doing
   > the work. After a restart I had 5 Silver processes; the oldest held :8001 and never
   > incremented, while the others wrote the data. Also, a *labelled* Counter prints only HELP/TYPE
   > until `.inc()` is called — so "registered but no value" literally means the increment path
   > never ran in the serving process. Process liveness is not the same as correct metrics.
2. Why is `kill -9` dangerous on a streaming job, and how do you recover?
   > Spark writes the batch offset at the start and the commit marker at the end. A hard kill in
   > between leaves the checkpoint claiming the range is processed while the sink (Delta MERGE)
   > never committed — so on restart the job skips that data forever. Use SIGTERM so it can
   > commit cleanly. Recovery: reset the checkpoint and replay from the durable Bronze log; it's
   > safe because the MERGE is idempotent (at-least-once + idempotent = exactly-once effect).
3. Why not dedup on Kafka offset, and what's the correct key here?
   > Offsets are only unique within a topic's lifetime — a connector/topic restart can reuse low
   > offsets, causing dedup to discard NEW data that collides with OLD. The right key is the
   > Postgres LSN (monotonic WAL position) combined with the primary key: LSN survives restarts,
   > and the PK keeps initial-snapshot rows distinct because they all share one LSN.

---

# Post-Portfolio Audit Session (Tier 1 + Tier 2 hardening)

An external audit reviewed the project after Day 7. We verified every claim
against the code (all were accurate), then fixed Tier 1 + Tier 2. This section
is the errors+fixes record for that session.

## TIER 1 — repo hygiene & privacy

### Fix 1: PII was committed to a PUBLIC GitHub repo (highest severity)
- **Found:** `build_resume.py` (phone, email, LinkedIn, GitHub handle) + 2 resume
  PDFs were tracked, and the repo `Jayashree1743/CDC_Lakehouse` is **public**.
  The single commit held them, so `git rm` alone was NOT enough — the objects
  lived in history.
- **Fix (in order):**
  1. Moved resume files out of the repo to `~/private_resume/`.
  2. Deleted `__pycache__/build_resume.cpython-312.pyc` — a *compiled* file that
     still embedded the phone number on disk after the source was gone. Lesson:
     **grep -r after deleting sources; bytecode caches hold string literals.**
  3. Added `.gitignore` (lakehouse/, dbt target/logs/metastore, __pycache__,
     *.pdf, .vscode/) and untracked 886 junk files.
  4. **History rewrite**: repo had exactly 1 commit, so we made a fresh orphan
     root commit with the clean tree, force-pushed over `main`.
  5. Changed commit author/committer email to `Jayashree1743@users.noreply.github.com`
     — the *metadata* itself carried the personal gmail (`git log --format='%ae'`
     to check this; most people forget it).
- **The hidden keep-alive:** after the rewrite, `git rev-list --all` STILL
  returned the old PII commit even though `show-ref` showed one ref. Cause: a
  **linked worktree** (`~/.local/share/kilo/worktree-pool/...`) left by another
  tool held a *detached HEAD* at the old commit. Every ref — including detached
  HEADs — keeps objects alive. `git worktree list` was the detector.
- **Verified:** remote history grep = clean, no PII filenames in remote tree,
  emails in metadata = noreply only, objects purged (938 → 69).
- **Interview quote:** *"Purging PII from a public repo isn't `git rm` — you need
  to rewrite history, fix the committer metadata, and hunt down every ref that
  pins the old objects. In my case a detached worktree was silently keeping the
  old commit reachable."*

### Fix 2: `dbt build` never ran in the automation
- **Found:** `run_all.sh` had zero dbt mentions. Fresh clone → Gold tables never
  built → `/analytics/*` = 503, e2e would fail 2 assertions. It "worked" only
  because a stale committed `gold.db` existed.
- **Fix:** new step **5/7 "Gold (dbt build)"** in run_all.sh — runs
  `docker exec cdc-spark bash -c 'cd /dbt && dbt build --profiles-dir /root/.dbt'`
  (dbt runs INSIDE the spark container, session method).
- **Verified:** full `./run_all.sh` now prints `OK dbt build passed`.

## TIER 2 — correctness

### Fix 3: dbt was broken on the checked-in data (3 latent failures)
| # | Cause | Fix |
|---|---|---|
| a | `stg_contract_changes` read only `$.after.*` → DELETE events had `contract_id = NULL`, failing not_null/relationships | `coalesce($.after.x, $.before.x)` for every column (REPLICA IDENTITY FULL guarantees the before-image) |
| b | `change_uid = concat(partition,'-',offset)` — the *same offset-reuse bug Silver had fixed*; duplicate offsets broke `unique` | `change_uid = concat(contract_id, '-', source_lsn)` + `row_number()` dedup (at-least-once replays collapse) |
| c | `fct` (built from Bronze) keeps history for DLQ-rejected/soft-deleted contracts, but `stg_contracts` filtered `is_deleted=false` → relationships test failed on *correct* data | `stg_contracts` now keeps soft-deleted rows (flag exposed); removed the fct→stg FK test with a written rationale: **Bronze-sourced fact and Silver-sourced dimension deliberately have different populations — that's the quality gate working, not a data bug.** |
- **Also found & fixed (not in audit):** the Spark image's `protobuf<5` pin was
  in the *same pip command* as its dependencies → pip's resolver silently
  upgraded protobuf to 5.29.6 → `dbt` crashed with `MessageToJson() got an
  unexpected keyword argument 'including_default_value_fields'`. Fixed by a
  **separate final `pip install --force-reinstall "protobuf<5"` step** in the
  Dockerfile. (Only surfaced because we ran dbt from a *dropped* gold.db — the
  stale committed artifacts had been masking a broken toolchain.)
- **Verified:** dropped gold.db + metastore tables, rebuilt from scratch:
  `PASS=17 ERROR=0`.

### Fix 4: document-level vector leak (2.1) — delete handling was incomplete
- **Found:** deleted `contract_documents` rows were skipped by the worker and
  only *contracts* got purged → a document deleted while its contract lived
  kept its vectors in Qdrant **forever**. Same hole when a doc's text became
  empty (0 chunks → never registered → stale tail never retired).
- **Fix:** `sync_documents` now (a) deletes-by-payload `document_id` for every
  soft-deleted doc, (b) registers **every** processed doc in `doc_chunk_counts`
  *before* retirement — including 0-chunk ones.
- **Live proof:** hard-deleted doc id=1 (contract 1 alive) → Qdrant count for
  document_id=1 went 1 → 0 in ~27 s; sibling docs 4/5/6/7 untouched (scoped
  delete, not a nuke).

### Fix 5: watermark — first `>=`, then the real answer (composite ts+lsn)
- **Mistake we caught ourselves:** audit suggested `>=`. We implemented it, then
  the logs showed the **regression**: watermark stuck, every poll re-embedded
  the boundary row (~27 tokens/run forever, "no-op" never reached). Idempotent
  but infinitely wasteful.
- **Fix (the audit's own better option):** composite watermark
  `(source_commit_ts, source_lsn)` with **strictly greater** comparison:
  - LSN tie-break → two commits sharing a Postgres ms are both processed
  - strict `>` on the pair → boundary row not re-read, no re-embed loop
  - `sync_state` gained a `watermark_lsn` column via
    `write_deltalake(..., schema_mode="merge")` so old 5-column state files
    still read (missing lsn → 0).
- **Verified:** steady state logs `nothing changed since watermark -> no-op`
  (0 tokens); a new change still visible in semantic search in 3 s.

### Fix 6: Silver latest-per-key tie-break used the untrusted key (2.5)
- `Window.orderBy(source_ts_ms, kafka_offset)` — kafka_offset is the key we
  *proved* is reusable across restarts. Changed to `source_lsn`
  (monotonic WAL position). Dead duplicate `whenMatchedUpdateAll(condition=
  cdc_op='d')` branch removed with a comment explaining why one branch suffices.

### Fix 7: chaos test could never fail CI (3.3)
- Printed PASS/FAIL but always exited 0. Added `FAILURES` counter in `fail()`,
  terminal `exit 1` if >0, `exit 0` otherwise. **Verified both paths** by
  sourcing the real exit block with FAILURES=0/1.

### Fix 8: README overclaims (2.7)
- "95.2% measured" → "~95% estimated (chars÷4 heuristic)" with an explicit
  note that the *mechanism* (only changed chunks re-embedded) is the observable
  fact; the denominator now counts LIVE docs only (was inconsistent between
  backfill and sync). Token math described as estimated everywhere.

## Test results after Tier 1 + Tier 2
- `./run_all.sh` — all 7 steps OK (incl. new dbt step)
- `dbt build` from dropped gold — **PASS=17 ERROR=0**
- `./e2e_test.sh` — **12 passed, 0 failed**
- `./chaos_test.sh` — all PASS, exit 0 (and provably exit 1 on failure)
- document-delete purge — live-verified (1 → 0 vectors, siblings intact)
- composite watermark — no-op at steady state + 3 s change propagation
- remote repo — no PII in tree, history, or commit metadata

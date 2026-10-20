# Day 2 Lab Steps — Lakehouse Ingestion: Kafka → PySpark → Bronze / Silver

**Objective:** CDC events flow into Delta **Bronze** (raw), get parsed/merged into
**Silver** (cleansed current state) — with time travel as your proof.

Run from `/home/jayasree/incremental`. Day 1 stack must be up:
`docker compose -p cdc-lakehouse ps` → all healthy.

---

## Concept map (2h study before building — links in study_plan.md Day 2)

| Concept | Plain English | Where you see it today |
|---|---|---|
| Data Lake vs Warehouse | Cheap files (Parquet) vs modeled/governed tables | `./lakehouse/` is a folder of Parquet files |
| Medallion Architecture | Bronze=raw, Silver=cleansed, Gold=business | `lakehouse/bronze.db`, `lakehouse/silver.db` |
| Delta Lake | ACID + schema enforcement + time travel over Parquet | `_delta_log/` folder in every table |
| Time travel | Query yesterday's version without a backup | `VERSION AS OF` in step 5 |
| Schema evolution | New column appears without breaking old data | `mergeSchema` / `autoMerge` configs |
| Structured Streaming | Process an unbounded stream as tiny micro-batches | `readStream` / `trigger 10s` |
| Checkpoint | Saved offsets — restart resumes, never reprocesses | `lakehouse/_checkpoints/` |
| foreachBatch | Run your own batch logic (Delta writes/MERGE) per batch | `write_bronze_batch`, `upsert_silver` |
| Idempotent write | Replay-safe: same batch twice = no duplicates | `txnAppId`/`txnVersion` in Bronze |

---

## Step 1 — Start the Spark container

```bash
docker compose -p cdc-lakehouse up -d spark
docker exec cdc-spark python3 --version      # python inside the image?
```

## Step 2 — Bronze job (Kafka → raw Delta)

```bash
docker exec cdc-spark /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  /jobs/bronze_job.py
```

First run **downloads jars from Maven Central (~2 min)**, then drains Kafka from
`earliest` — yesterday's Day-1 backlog lands immediately. You should see:

```
[bronze batch 0] wrote 5 rows -> bronze.customers_cdc
[bronze batch 0] wrote 7 rows -> bronze.contracts_cdc
...
```

Leave it running (background it). Spark UI: http://localhost:4040

## Step 3 — Generate changes, watch Bronze grow

```bash
python3 simulate_traffic.py --interval 3 --duration 60 &
```

Count Bronze rows (run in a second terminal):

```bash
docker exec cdc-spark /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  /jobs/query_lakehouse.py
```

## Step 4 — Silver job (parse + MERGE upsert)

In a second background terminal:

```bash
docker exec cdc-spark /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  /jobs/silver_job.py
```

Silver keeps **one current row per id** (`MERGE`), marks deletes with
`is_deleted = true` (soft delete — we can still see what was deleted).

## Step 5 — The Day 2 demo checklist

Run `query_lakehouse.py` again and verify:

- [ ] **Bronze row appears within seconds** of a Postgres change (section 1 count grows)
- [ ] **Silver shows latest state** with correct `cdc_op` (c/u/d) — section 4
- [ ] **`DESCRIBE HISTORY` works** and lists versions (section 2) → *Delta is live*
- [ ] **Time travel works** — old version vs current show different `monthly_price` (section 3)
- [ ] **Analytics SQL** runs on Silver — MRR by status (section 5)
- [ ] **Lag metric** — Postgres commit → Silver ingest, avg seconds (section 6)

### Restart-resume test (checkpoint proof)

```bash
# 1. note bronze counts, then Ctrl+C (or kill) the bronze job
# 2. restart the same spark-submit command
# 3. while it is down, make a change in psql
# 4. after restart: NO duplicated rows (Delta counts unchanged for old batches),
#    the change made while down appears exactly once
```

Why: the checkpoint stored the Kafka offset — Spark resumes *after* it, and even
if a batch were replayed, `txnAppId`/`txnVersion` makes the Delta write idempotent.

### Schema evolution proof

Add a column to source data (simulating an app migration):

```bash
docker exec -i cdc-postgres psql -U demo -d inventory \
  -c "ALTER TABLE contracts ADD COLUMN region text DEFAULT 'south';" \
  -c "UPDATE contracts SET region = 'south' WHERE id = 1;"
```

Within one batch, Silver gains `region` automatically (`autoMerge` on) — old
files keep working. That's schema evolution.

---

## Self-test (answer out loud)

1. What does the checkpoint directory contain and what breaks if you delete it?
2. How do you query yesterday's version of a Silver table?
3. Why keep Bronze at all if Silver is clean?

## Teardown / reset

```bash
docker compose -p cdc-lakehouse stop spark        # stop just the jobs
rm -rf lakehouse                                  # NUKE all lake data + checkpoints
```

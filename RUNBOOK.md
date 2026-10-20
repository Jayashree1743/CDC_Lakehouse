# RUNBOOK — How to Run the Entire CDC Lakehouse Pipeline

One project, five containers, two streaming jobs. Read top to bottom once,
then you only ever need the **Quick Start** box.

---

## Quick Start (fresh machine, from zero to running)

```bash
cd /home/jayasree/incremental
./run_all.sh              # starts EVERYTHING: stack + connector + bronze + silver
./run_all.sh --status     # check it any time
```

Prove the pipeline in 30 seconds:

```bash
# 1. make a change exactly like the problem statement
docker exec -i cdc-postgres psql -U demo -d inventory \
  -c "UPDATE contracts SET monthly_price = 777 WHERE id = 1;"

# 2. verify it landed in Bronze + Silver (and see time travel, MRR SQL, lag)
docker exec cdc-spark /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0 /jobs/query_lakehouse.py
```

---

## What runs what (the map)

```
YOU / simulate_traffic.py / any app
        │  INSERT / UPDATE / DELETE
        ▼
[cdc-postgres :5436]  wal_level=logical  ── writes every change to WAL
        │                                        │
        │   (Debezium reads the WAL via          │
        │    replication slot debezium_slot)     │
        ▼                                        │
[cdc-connect :8083]  Debezium connector ◄───────┘
        │  JSON change events (op=c/u/d, before, after, ts_ms)
        ▼
[cdc-kafka :9092]  topics: inventory.public.{customers,contracts,contract_documents}
        │
        │  Structured Streaming (10s micro-batches, checkpointed)
        ▼
[cdc-spark]  bronze_job.py  ── raw append-only Delta  → lakehouse/bronze.db/*
        │                         (idempotent writes: txnAppId/txnVersion)
        ▼            silver_job.py ── parse + MERGE upsert → lakehouse/silver.db/*
                              (current state per id, soft deletes, autoMerge)
        ▼
query_lakehouse.py ── counts, DESCRIBE HISTORY, time travel, MRR SQL, lag metric
```

Containers: `cdc-postgres` (5436), `cdc-zookeeper` (2181), `cdc-kafka` (9092/29092),
`cdc-connect` (8083), `cdc-spark` (4040).

---

## The commands you'll actually use

### Control

| Action | Command |
|---|---|
| Start / ensure everything running | `./run_all.sh` |
| Show status (containers, connector, jobs, slot) | `./run_all.sh --status` |
| Stop only the Spark jobs (containers stay) | `./run_all.sh --stop` |
| Stop containers, keep all data | `./run_all.sh --down` |
| Stop + **delete all data** (completely fresh start) | `./run_all.sh --wipe` |

### Watch data move

```bash
# raw Kafka events (the Day 1 magic)
docker exec -it cdc-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic inventory.public.contracts

# simulate an application writing changes every 3s for 60s
python3 simulate_traffic.py --interval 3 --duration 60

# full lakehouse verification (Day 2 proof)
docker exec cdc-spark /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0 /jobs/query_lakehouse.py
```

### Run one job by hand (instead of run_all.sh)

```bash
# Bronze (Kafka -> raw Delta) — long running, Ctrl+C to stop
docker exec cdc-spark /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  /jobs/bronze_job.py

# Silver (parse + MERGE) — second terminal, long running
docker exec cdc-spark /opt/spark/bin/spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  /jobs/silver_job.py
```

### Inspect

```bash
# Debezium connector status
curl -s http://localhost:8083/connectors/inventory-connector/status | python3 -m json.tool

# replication slot (the WAL bookmark)
docker exec cdc-postgres psql -U demo -d inventory \
  -c "SELECT slot_name, active, restart_lsn FROM pg_replication_slots;"

# WAL level + source data
docker exec -i cdc-postgres psql -U demo -d inventory \
  -c "SHOW wal_level;" -c "SELECT * FROM contracts;"

# Delta files on disk
ls -R lakehouse/ | head -40

# logs of a streaming job (started by run_all.sh)
docker exec cdc-spark tail -50 /tmp/bronze_job.log
docker exec cdc-spark tail -50 /tmp/silver_job.log
```

### Web UIs (browser)

- **Spark**: http://localhost:4040 (while a job is running; 4041/4042 if several)
- **Debezium/Kafka Connect**: http://localhost:8083/connectors

---

## After a reboot / sleep (typical morning)

```bash
./run_all.sh          # brings back containers, re-registers if needed,
                      # restarts jobs WHERE THEIR CHECKPOINTS LEFT OFF
./run_all.sh --status
```

Nothing is lost: Kafka holds the events, the replication slot holds the WAL,
checkpoints hold the offsets, Delta holds committed batches.

---

## Troubleshooting (real errors we hit)

| Symptom | Cause | Fix |
|---|---|---|
| `cdc-kafka` exits: `NodeExists` in ZooKeeper | cold restart, old broker session not expired yet | wait ~20s, `./run_all.sh` again |
| `cdc-postgres` exits: `unrecognized parameter " wal_level"` | compose `command:` needs `-c` and `name=value` as **separate** list items | already fixed in `docker-compose.yml` — if it reappears, compare with the file |
| connector state FAILED | check `docker logs cdc-connect \| tail -30` | `curl -X DELETE http://localhost:8083/connectors/inventory-connector` then `./run_all.sh` |
| Silver rows all NULL | tombstone messages (`value: null`) | already fixed (`kafka_value IS NOT NULL` filter) |
| Silver job says "not a Delta table" | first run, table not bootstrapped | already fixed (auto-create empty table) |
| Jobs not listed in `--status` | they died | check `/tmp/*_job.log` inside cdc-spark, then `./run_all.sh` |
| Ports 5432/5435 busy | other projects on this machine | our Postgres is on **5436** by design |

---

## Fresh start (nuke everything)

```bash
./run_all.sh --wipe     # removes containers, volumes, lakehouse/, checkpoints
./run_all.sh            # rebuild from scratch (init.sql reseeds the data)
```

---

## File map

| File | What it is |
|---|---|
| `run_all.sh` | one-command start/status/stop for the whole pipeline |
| `docker-compose.yml` | all 5 services + volumes |
| `postgres/init.sql` | schema, seed data, `REPLICA IDENTITY FULL` |
| `connect/register-connector.json` + `register.sh` | Debezium connector |
| `day1_demo.sh` / `day1_demo_output.txt` | Day 1 proof (Kafka events, latency) |
| `spark/cdc_common.py` | shared Delta config + CDC schemas |
| `spark/bronze_job.py` | Kafka → raw Delta stream |
| `spark/silver_job.py` | parse → MERGE upsert stream |
| `spark/query_lakehouse.py` | verification + analytics |
| `day2_demo_output.txt` | Day 2 proof (history, time travel, MRR) |
| `simulate_traffic.py` | fake application writing changes |
| `DAY1_STEPS.md` / `DAY2_STEPS.md` | lab guides + self-tests |
| `NOTES.md` | your lab notebook (errors + fixes + answers) |
| `study_plan.md` / PDF | the 7-day plan |

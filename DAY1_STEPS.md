# Day 1 Lab Steps — CDC Foundation: PostgreSQL → Debezium → Kafka

**Objective:** See a live `UPDATE` in psql appear as a Kafka message within seconds.

Run everything from the project root `/home/jayasree/incremental`.

---

## Step 0 — Sanity (already verified, listed for your notebook)

| Tool | Status on this machine |
|---|---|
| Docker 29.1.3 + Compose v2.40.3 | OK |
| Python 3.12.3 (command is `python3`, no `python` alias) | OK |
| Git 2.43, Java 21 | OK |
| Ports 5432 and 5435 | **Already in use** → our Postgres uses **5436** |

---

## Step 1 — Start the stack

```bash
docker compose -p cdc-lakehouse up -d
docker compose -p cdc-lakehouse ps
```

Wait until `postgres`, `kafka` and `connect` show `healthy` (about 60–90 s first time; images pull).

## Step 2 — Verify the CDC prerequisites inside PostgreSQL

```bash
docker exec -it cdc-postgres psql -U demo -d inventory
```

Inside psql:

```sql
SHOW wal_level;                            -- MUST be 'logical'
SELECT * FROM pg_replication_slots;        -- empty until we register the connector
SELECT count(*) FROM contracts;            -- 2 seed rows
\q
```

## Step 3 — Register the Debezium connector

```bash
chmod +x connect/register.sh
./connect/register.sh
```

Then check status any time:

```bash
curl -s http://localhost:8083/connectors/inventory-connector/status | python3 -m json.tool
```

Confirm the replication slot now exists (re-run the psql command from Step 2).
`restart_lsn` / `confirmed_flush_lsn` are the bookmark positions in the WAL.

## Step 4 — Watch the Kafka topics Debezium created

```bash
docker exec -it cdc-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

You should see at least:

```
inventory.public.contracts
inventory.public.contract_documents
inventory.public.customers
```

Inspect one topic's shape:

```bash
docker exec -it cdc-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 --topic inventory.public.contracts
```

## Step 5 — The magic moment (demo checklist)

Terminal A — start a consumer **before** you make the change:

```bash
docker exec -it cdc-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic inventory.public.contracts \
  --from-beginning
```

Terminal B — make the change exactly like the problem statement:

```bash
docker exec -it cdc-postgres psql -U demo -d inventory \
  -c "UPDATE contracts SET monthly_price = 999 WHERE id = 1;"
```

Terminal A should print a JSON **within a few seconds**. Save that JSON into `NOTES.md`.

## Step 6 — Read the event like an analyst

For the UPDATE above, the message has (raw Debezium envelope — deliberately NOT flattened):

| Field | What it means |
|---|---|
| `op: "u"` | u = update (c = create, d = delete, r = initial snapshot read) |
| `before` | the FULL old row: `monthly_price: 1200` — our vector worker uses this to delete the stale embedding |
| `after` | the FULL new row: `monthly_price: 999` — the new state to re-embed |
| `source.ts_ms` | commit time in Postgres — start of our replication-lag measurement (Day 6) |
| `source.lsn` | exact WAL position, like a page number |
| `ts_ms` | when Debezium published to Kafka |
| `key` | `{"id": 1}` — same key for every change to this row ⇒ same Kafka partition ⇒ **ordering guaranteed** |
| null value (after the event) | the *tombstone*; tells downstream "this key is fully gone" on DELETE |

Then test DELETE handling:

```sql
DELETE FROM contract_documents WHERE id = 3;
```

You should see an event with `op: "d"`, `after: null`, `before` = the deleted row, followed by a tombstone (null value) message.

## Step 7 — Clean restart test (understand durability)

```bash
docker compose -p cdc-lakehouse restart connect
# make another UPDATE while connect is starting
docker exec -it cdc-postgres psql -U demo -d inventory \
  -c "UPDATE contracts SET monthly_price = 1500 WHERE id = 1;"
# wait for connect to be RUNNING, then check the consumer: the change still arrives
```

The replication slot keeps WAL from being deleted, so **no change is ever lost** while the connector is down — but WAL grows (that's the trade-off; you'll explain it in interviews).

## Self-test (answer out loud, no peeking)

1. Why does Debezium NOT slow down the production database?
2. What happens to WAL segments if the connector is down for a day?
3. What does the `d` operation look like in the event, and why is `after` null?

## Teardown

```bash
docker compose -p cdc-lakehouse down        # keeps data volume
docker compose -p cdc-lakehouse down -v     # WIPES data — only to replay Day 1 from scratch
```

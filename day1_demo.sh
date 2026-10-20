#!/usr/bin/env bash
# =============================================================================
# DAY 1 AUTOMATED DEMO: proves a psql UPDATE becomes a Kafka event in seconds.
# Usage:  ./day1_demo.sh
# Captures: 1) an UPDATE event   2) a DELETE event + tombstone
# Writes results into day1_demo_output.txt (paste into NOTES.md)
# =============================================================================
set -euo pipefail

OUT="$(cd "$(dirname "$0")" && pwd)/day1_demo_output.txt"
: > "$OUT"

PSQL="docker exec -i cdc-postgres psql -U demo -d inventory"
CONSUME() { # topic, seconds
  timeout "$2" docker exec -i cdc-kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 --topic "$1" \
    --property print.key=true --property key.separator=' => ' \
    --max-messages 1 2>/dev/null || true
}

echo "=== DAY 1 DEMO RUN $(date) ===" | tee -a "$OUT"

# ---------------------------------------------------------------------------
# DEMO A: the problem-statement UPDATE ($1200 -> $999)
# ---------------------------------------------------------------------------
echo "" | tee -a "$OUT"
echo "--- A) UPDATE contracts SET monthly_price = 999 WHERE id = 1 ---" | tee -a "$OUT"

# start consumer FIRST (in background), then fire the update
( timeout 45 docker exec -i cdc-kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 --topic inventory.public.contracts \
    --property print.key=true --property key.separator=' => ' \
    --max-messages 1 > /tmp/demo_a.json 2>/dev/null || true ) &
CONSUMER_PID=$!
sleep 3

START_MS=$(date +%s%3N)
$PSQL -c "UPDATE contracts SET monthly_price = 999, updated_at = now() WHERE id = 1;" >/dev/null
echo "  UPDATE sent at epoch-ms: $START_MS" | tee -a "$OUT"

wait $CONSUMER_PID
END_MS=$(date +%s%3N)
echo "  Kafka message received. Round-trip: $(( END_MS - START_MS )) ms" | tee -a "$OUT"
echo "" >> "$OUT"
cat /tmp/demo_a.json | tee -a "$OUT"

# ---------------------------------------------------------------------------
# DEMO B: a DELETE (op=d, after=null) + the tombstone
# ---------------------------------------------------------------------------
echo "" | tee -a "$OUT"
echo "--- B) DELETE FROM contract_documents WHERE id = 3 ---" | tee -a "$OUT"

( timeout 45 docker exec -i cdc-kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 --topic inventory.public.contract_documents \
    --property print.key=true --property key.separator=' => ' \
    --max-messages 2 > /tmp/demo_b.json 2>/dev/null || true ) &
CONSUMER_PID=$!
sleep 3

$PSQL -c "DELETE FROM contract_documents WHERE id = 3;" >/dev/null
wait $CONSUMER_PID
echo "  (message 1 = delete event, message 2 = tombstone/null value)" >> "$OUT"
cat /tmp/demo_b.json | tee -a "$OUT"

# ---------------------------------------------------------------------------
# State checks for the checklist
# ---------------------------------------------------------------------------
echo "" | tee -a "$OUT"
echo "--- C) Replication slot (bookmark in the WAL) ---" | tee -a "$OUT"
$PSQL -c "SELECT slot_name, active, restart_lsn, confirmed_flush_lsn FROM pg_replication_slots;" | tee -a "$OUT"

echo "" | tee -a "$OUT"
echo "--- D) Current row state ---" | tee -a "$OUT"
$PSQL -c "SELECT id, title, monthly_price FROM contracts ORDER BY id;" | tee -a "$OUT"

echo "" | tee -a "$OUT"
echo "Full demo transcript saved to: $OUT" | tee -a "$OUT"

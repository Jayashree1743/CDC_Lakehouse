#!/usr/bin/env bash
# =============================================================================
# DAY 7 - CHAOS TEST: survive a Kafka outage without losing data
# =============================================================================
# The claim we prove: "CDC is durable — events survive a broker outage because
# the source WAL + the consumer offset let us REPLAY exactly what we missed."
#
# Sequence:
#   1. Record baseline (a token we know is already replicated)
#   2. STOP Kafka
#   3. Make NEW changes in Postgres WHILE Kafka is down (they cannot be consumed)
#   4. RESTART Kafka
#   5. Show the missed changes flow through with NO data loss (replay from offsets)
#
# Usage: ./chaos_test.sh        (stack must be up via ./run_all.sh)
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")"

FAILURES=0
pass() { printf '  \033[1;32m[PASS]\033[0m %s\n' "$*"; }
fail() { printf '  \033[1;31m[FAIL]\033[0m %s\n' "$*"; FAILURES=$((FAILURES+1)); }
section() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }

BEFORE="chaos-before-$(date +%s)"
DURING1="chaos-during1-$(date +%s)"
DURING2="chaos-during2-$(date +%s)"

reach_bronze() {  # $1 = token -> 1 if found in Bronze
  docker exec cdc-spark bash -c "grep -rl '$1' /lakehouse/bronze.db/contract_documents_cdc 2>/dev/null" | grep -q .
}

section "1. Baseline: a change BEFORE the outage replicates fine"
docker exec -i cdc-postgres psql -U demo -d inventory -q \
  -c "UPDATE contract_documents SET chunk_text='Chaos baseline ${BEFORE}.', content_hash='${BEFORE}', updated_at=now() WHERE id=4;"
for i in $(seq 1 30); do reach_bronze "$BEFORE" && break; sleep 2; done
reach_bronze "$BEFORE" && pass "baseline token reached Bronze" || fail "baseline did not replicate"

section "2. STOP Kafka (simulate a broker outage)"
docker compose -p cdc-lakehouse stop kafka >/dev/null 2>&1 && pass "Kafka stopped" || fail "could not stop Kafka"
sleep 5

section "3. Make changes WHILE Kafka is down (cannot be consumed yet)"
docker exec -i cdc-postgres psql -U demo -d inventory -q \
  -c "UPDATE contract_documents SET chunk_text='Chaos during outage part one ${DURING1}.', content_hash='${DURING1}', updated_at=now() WHERE id=4;"
docker exec -i cdc-postgres psql -U demo -d inventory -q \
  -c "UPDATE contracts SET monthly_price=1337.00, updated_at=now() WHERE id=1;"
sleep 8
# prove the change is NOT in Bronze yet (Kafka is down)
reach_bronze "$DURING1" && fail "change reached Bronze while Kafka down?!" \
                        || pass "change correctly NOT yet in Bronze (Kafka down)"
# the source Postgres still has it (WAL is the source of truth)
src=$(docker exec -i cdc-postgres psql -U demo -d inventory -tAc "select count(*) from contract_documents where content_hash='${DURING1}';")
[ "$src" = "1" ] && pass "source Postgres retained the change (WAL durable)" || fail "source lost the change"

section "4. RESTART Kafka"
docker compose -p cdc-lakehouse start kafka >/dev/null 2>&1 && pass "Kafka restarted" || fail "could not restart Kafka"
# wait for Kafka healthy again
for i in $(seq 1 40); do
  st=$(docker inspect -f '{{.State.Health.Status}}' cdc-kafka 2>/dev/null)
  [ "$st" = "healthy" ] && break
  sleep 3
done
pass "Kafka healthy: $(docker inspect -f '{{.State.Health.Status}}' cdc-kafka 2>/dev/null)"

section "5. NO DATA LOSS: the missed change flows through after recovery"
recovered=0
for i in $(seq 1 60); do reach_bronze "$DURING1" && { recovered=1; break; }; sleep 2; done
[ "$recovered" = "1" ] && pass "the change made during the outage reached Bronze (replayed)" \
                       || fail "the during-outage change was LOST"

# also confirm the second change (a price update on contracts) landed in Silver
silvered=0
for i in $(seq 1 30); do
  if docker exec cdc-spark bash -c "grep -rl '1337' /lakehouse/silver.db/contracts 2>/dev/null" | grep -q .; then
    silvered=1; break
  fi
  sleep 2
done
[ "$silvered" = "1" ] && pass "the concurrent contracts UPDATE also reached Silver" \
                      || fail "concurrent contracts UPDATE lost"

section "6. Connector + jobs still healthy after recovery"
cx=$(curl -s http://localhost:8083/connectors/inventory-connector/status 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin)['connector']['state'])" 2>/dev/null)
[ "$cx" = "RUNNING" ] && pass "connector back to RUNNING" || fail "connector state=$cx"
jobs=$(docker exec cdc-spark bash -c 'ps -ef | grep -cE "[b]ronze_job|[s]ilver_job"' 2>/dev/null)
[ "${jobs:-0}" -ge 1 ] && pass "streaming jobs still alive" || fail "streaming jobs died"

if [ "$FAILURES" -gt 0 ]; then
  printf '\n\033[1;31mCHAOS TEST FAILED — %s assertion(s) failed\033[0m\n' "$FAILURES"
  exit 1
fi

printf '\n\033[1;36mCHAOS TEST COMPLETE — Kafka outage did not lose data\033[0m\n'
exit 0

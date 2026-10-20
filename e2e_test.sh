#!/usr/bin/env bash
# =============================================================================
# DAY 7 - END-TO-END INTEGRATION TEST
# =============================================================================
# Proves the WHOLE chain works in one shot, with clear PASS/FAIL per stage:
#
#   Postgres UPDATE -> Debezium -> Kafka -> Bronze -> Silver -> (quality gate)
#        -> Gold (dbt) -> Qdrant (vector sync) -> FastAPI search
#
# Usage:  ./e2e_test.sh            (stack must already be up via ./run_all.sh)
#
# Each assertion prints [PASS] or [FAIL]. Exit code is non-zero if any fail,
# so this can gate CI.
# =============================================================================
set -uo pipefail
cd "$(dirname "$0")"

PASS=0; FAIL=0
pass() { printf '  \033[1;32m[PASS]\033[0m %s\n' "$*"; PASS=$((PASS+1)); }
fail() { printf '  \033[1;31m[FAIL]\033[0m %s\n' "$*"; FAIL=$((FAIL+1)); }
section() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }

# a unique token so we can grep for THIS run's change through the whole chain
TOKEN="e2e-$(date +%s)"

# --------------------------------------------------------------------------- #
section "0. Preconditions"
# --------------------------------------------------------------------------- #
curl -sf http://localhost:8083/connectors >/dev/null 2>&1 \
  && pass "Debezium Connect reachable (:8083)" || fail "Connect down"
docker exec cdc-spark true 2>/dev/null \
  && pass "Spark container up" || fail "Spark container down"
curl -sf http://localhost:6333/collections >/dev/null 2>&1 \
  && pass "Qdrant reachable (:6333)" || fail "Qdrant down"
curl -sf http://localhost:8000/health >/dev/null 2>&1 \
  && pass "FastAPI reachable (:8000)" || fail "FastAPI down"

# --------------------------------------------------------------------------- #
section "1. Make a change in Postgres (the source of truth)"
# --------------------------------------------------------------------------- #
docker exec -i cdc-postgres psql -U demo -d inventory -q \
  -c "UPDATE contract_documents SET chunk_text='Coverage clause ${TOKEN}: Acme is covered for all hardware failures with next-business-day replacement.', content_hash='${TOKEN}', updated_at=now() WHERE id=4;" \
  && pass "UPDATE issued (token=${TOKEN})" || fail "could not UPDATE source"

# --------------------------------------------------------------------------- #
section "2. Change flows to Bronze (raw Kafka -> Delta), wait up to 60s"
# --------------------------------------------------------------------------- #
bronzed=0
for i in $(seq 1 30); do
  if docker exec cdc-spark bash -c "grep -rl '${TOKEN}' /lakehouse/bronze.db/contract_documents_cdc 2>/dev/null" | grep -q .; then
    bronzed=1; break
  fi
  sleep 2
done
[ "$bronzed" = "1" ] && pass "token found in Bronze (raw CDC persisted)" \
                     || fail "token never reached Bronze"

# --------------------------------------------------------------------------- #
section "3. Silver picks it up (parsed + MERGE), wait up to 60s"
# --------------------------------------------------------------------------- #
silvered=0
for i in $(seq 1 30); do
  if docker exec cdc-spark bash -c "grep -rl '${TOKEN}' /lakehouse/silver.db/contract_documents 2>/dev/null" | grep -q .; then
    silvered=1; break
  fi
  sleep 2
done
[ "$silvered" = "1" ] && pass "token found in Silver (cleansed table updated)" \
                     || fail "token never reached Silver"

# --------------------------------------------------------------------------- #
section "4. Vector sync re-embeds ONLY the changed chunk -> Qdrant"
# --------------------------------------------------------------------------- #
qdrant_hit=0
for i in $(seq 1 30); do
  if curl -s http://localhost:6333/collections/contracts/points/scroll \
       -X POST -H 'Content-Type: application/json' \
       -d '{"limit":100,"with_payload":true}' 2>/dev/null | grep -q "${TOKEN}"; then
    qdrant_hit=1; break
  fi
  sleep 2
done
[ "$qdrant_hit" = "1" ] && pass "new wording is now a VECTOR in Qdrant" \
                        || fail "Qdrant does not contain the new wording"
docker logs cdc-vector-sync 2>&1 | grep "upserted" | tail -1 | sed 's/^/    /'

# --------------------------------------------------------------------------- #
section "5. FastAPI semantic search reflects the NEW wording"
# --------------------------------------------------------------------------- #
resp=$(curl -s -X POST http://localhost:8000/search \
  -H 'Content-Type: application/json' -H 'x-api-key: demo-key' \
  -d '{"query":"hardware failures replacement coverage","top_k":3,"min_score":0.0}')
echo "$resp" | grep -q "${TOKEN}" \
  && pass "semantic search returns the updated clause" \
  || fail "search did not surface the new clause (got: ${resp:0:120}...)"

# --------------------------------------------------------------------------- #
section "6. Analytics endpoints over Gold"
# --------------------------------------------------------------------------- #
curl -s http://localhost:8000/analytics/changes-per-hour -H 'x-api-key: demo-key' \
  | grep -q "change_hour" \
  && pass "/analytics/changes-per-hour returns buckets" || fail "changes-per-hour broken"
curl -s "http://localhost:8000/contracts/1/history" -H 'x-api-key: demo-key' \
  | grep -q "change_uid" \
  && pass "/contracts/1/history returns rows" || fail "contract history broken"

# --------------------------------------------------------------------------- #
section "7. Auth: bad key is rejected"
# --------------------------------------------------------------------------- #
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:8000/search \
  -H 'Content-Type: application/json' -H 'x-api-key: WRONG' -d '{"query":"x"}')
[ "$code" = "401" ] && pass "wrong API key -> 401" || fail "expected 401, got $code"

# --------------------------------------------------------------------------- #
printf '\n\033[1;36mRESULT: %d passed, %d failed\033[0m\n' "$PASS" "$FAIL"
[ "$FAIL" = "0" ] && exit 0 || exit 1

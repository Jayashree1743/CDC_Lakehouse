#!/usr/bin/env bash
# =============================================================================
# run_all.sh — start the ENTIRE CDC Lakehouse pipeline in one command
#
#   ./run_all.sh            start stack + connector + bronze + silver
#   ./run_all.sh --status   show what is running (does not start anything)
#   ./run_all.sh --stop     stop the Spark streaming jobs (containers stay up)
#   ./run_all.sh --down     stop everything (data is kept)
#   ./run_all.sh --wipe     stop everything and DELETE all data (fresh start)
#
# Safe to run repeatedly: it only starts what is not already running.
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")"

PROJECT="cdc-lakehouse"
CONNECT="http://localhost:8083"

# --- helpers -----------------------------------------------------------------
log()  { printf '\033[1;36m==>\033[0m %s\n' "$*"; }
ok()   { printf '  \033[1;32mOK\033[0m  %s\n' "$*"; }
warn() { printf '  \033[1;33m..\033[0m  %s\n' "$*"; }

running() { docker exec cdc-spark ps -ef 2>/dev/null | grep -F "$1" | grep -v grep >/dev/null; }

# vector-sync's image is python:3.11-slim, which ships NO `ps` binary, so
# `docker exec ... ps -ef` always fails. Read PID 1's cmdline from /proc instead.
vector_sync_running() {
  docker exec cdc-vector-sync sh -c \
    'tr "\000" " " </proc/1/cmdline 2>/dev/null | grep -F worker.py >/dev/null' 2>/dev/null
}

# Qdrant v1.12 answers GET /collections with  {"result":{"collections":[...]}}
# (a dict). Older builds returned a bare list. Handle both shapes.
qdrant_collections() {
  curl -s -m 5 http://localhost:6333/collections 2>/dev/null \
    | python3 -c "
import json,sys
r = json.load(sys.stdin).get('result', [])
items = r.get('collections', []) if isinstance(r, dict) else r
print('  collections:', [c['name'] for c in items] or '(none yet)')
" 2>/dev/null
}

start_job() {   # name, script, extra packages
  local name="$1" script="$2" pkgs="$3"
  if running "$script"; then
    ok "$name already running"
    return
  fi
  log "starting $name ..."
  docker exec -d cdc-spark bash -c \
    "/opt/spark/bin/spark-submit --packages '$pkgs' /jobs/$script > /tmp/${script%.py}.log 2>&1"
  sleep 3
  running "$script" && ok "$name started (logs: /tmp/${script%.py}.log inside cdc-spark)" \
                     || warn "$name failed - exec into cdc-spark and check /tmp/${script%.py}.log"
}

status() {
  log "Containers:"
  docker compose -p "$PROJECT" ps --format 'table {{.Service}}\t{{.Status}}' || true
  log "Debezium connector:"
  curl -s "$CONNECT/connectors/inventory-connector/status" 2>/dev/null \
    | python3 -c "import json,sys; s=json.load(sys.stdin); print('  connector:',s['connector']['state'],'| task:',s['tasks'][0]['state'])" \
    2>/dev/null || warn "connector API not reachable"
  log "Spark streaming jobs:"
  docker exec cdc-spark ps -ef 2>/dev/null | grep -E "[b]ronze_job|[s]ilver_job" | awk '$NF ~ /_job\.py$/ {print "  running: "$NF}' | sort -u \
    || warn "no streaming jobs"
  log "Qdrant:"
  qdrant_collections || warn "Qdrant not reachable on :6333"
  vector_sync_running \
    && ok "vector-sync worker running (metrics :8002/metrics)" \
    || warn "vector-sync not running"
  log "Replication slot:"
  docker exec cdc-postgres psql -U demo -d inventory -t \
    -c "SELECT '  '||slot_name||' active='||active FROM pg_replication_slots;" 2>/dev/null || true
}

stop_jobs() {
  log "stopping streaming jobs (checkpoints saved - nothing is lost)"
  docker exec cdc-spark pkill -f bronze_job 2>/dev/null || true
  docker exec cdc-spark pkill -f silver_job  2>/dev/null || true
  ok "jobs stopped - restart anytime, they resume from their checkpoints"
}

# --- modes -------------------------------------------------------------------
case "${1:-}" in
  --status) status; exit 0 ;;
  --stop)   stop_jobs; exit 0 ;;
  --down)   stop_jobs; log "stopping containers (data KEPT)"; docker compose -p "$PROJECT" down; exit 0 ;;
  --wipe)   stop_jobs; log "stopping containers and DELETING all data"; docker compose -p "$PROJECT" down -v; rm -rf lakehouse; exit 0 ;;
  "")       ;;   # fall through to start
  *)        echo "usage: $0 [--status|--stop|--down|--wipe]"; exit 1 ;;
esac

# --- 1. containers -----------------------------------------------------------
log "1/7 Starting containers (postgres, zookeeper, kafka, connect, spark, qdrant, vector-sync) ..."
docker compose -p "$PROJECT" up -d

log "2/7 Waiting for PostgreSQL + Kafka + Kafka Connect ..."
for i in $(seq 1 60); do
  pg_ok=$(docker exec cdc-postgres pg_isready -U demo -d inventory 2>/dev/null | grep -c accepting || true)
  kf_ok=$(docker exec cdc-kafka kafka-topics --bootstrap-server localhost:9092 --list >/dev/null 2>&1 && echo 1 || echo 0)
  cx_ok=$(curl -sf "$CONNECT/connectors" >/dev/null 2>&1 && echo 1 || echo 0)
  [ "$pg_ok" -ge 1 ] && [ "$kf_ok" = "1" ] && [ "$cx_ok" = "1" ] && break
  sleep 3
done

# Don't limp on with a lying "OK" - if Connect never answered, name the culprit.
if [ "${cx_ok:-0}" != "1" ]; then
  warn "Kafka/Connect did not become ready. Container states + Kafka logs:"
  docker compose -p "$PROJECT" ps --format 'table {{.Service}}\t{{.Status}}' || true
  docker logs cdc-kafka --tail 40 2>&1 | tail -40 || true
  exit 1
fi
ok "Postgres ready, Kafka up, Connect REST up"

# --- 2. connector ------------------------------------------------------------
log "3/7 Debezium connector ..."
if curl -s "$CONNECT/connectors" 2>/dev/null | grep -q inventory-connector; then
  ok "connector already registered"
else
  ./connect/register.sh >/dev/null && ok "connector registered + RUNNING"
fi

# --- 3/4. streaming jobs -----------------------------------------------------
log "4/7 Spark streaming jobs ..."
start_job "bronze (Kafka -> raw Delta)"  "bronze_job.py" \
  "io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
start_job "silver (validate -> MERGE/DLQ)"  "silver_job.py"  \
  "io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

# --- 5. Gold (dbt) ------------------------------------------------------------
# dbt runs INSIDE the spark container (session method -> its own SparkSession).
# This is what actually BUILDS the Gold tables the API's /analytics endpoints
# read. Without this step a fresh clone returns 503 on those endpoints.
log "5/7 Gold (dbt build) ..."
if docker exec cdc-spark bash -c 'cd /dbt && dbt build --profiles-dir /root/.dbt' \
     > /tmp/dbt_build.log 2>&1; then
  ok "dbt build passed (models + tests)"
else
  warn "dbt build FAILED - run: docker exec cdc-spark cat /tmp/dbt_build.log"
fi

# --- 6. vector sync (Day 4) --------------------------------------------------
log "6/7 Vector sync (Qdrant) ..."
# vector-sync runs in --mode loop via its compose command (watermark=0 first run
# = natural backfill, then it polls). Just confirm it is alive.
sleep 5
if vector_sync_running; then
  ok "vector-sync running (metrics :8002/metrics)"
else
  warn "vector-sync not running - docker logs cdc-vector-sync"
fi

# --- 7. serving + observability (Day 5-6) ------------------------------------
log "7/7 API + Prometheus + Grafana ..."
curl -sf http://localhost:8000/health >/dev/null 2>&1 && ok "API up on :8000 (/docs)" || warn "API not up yet (docker logs cdc-api)"
curl -sf http://localhost:9090/-/ready >/dev/null 2>&1 && ok "Prometheus up on :9090" || warn "Prometheus not up"
curl -sf http://localhost:3000/api/health >/dev/null 2>&1 && ok "Grafana up on :3000 (admin/admin)" || warn "Grafana not up"

log "Pipeline status"
sleep 5
status

cat <<'EOF'

==> NEXT (verify the pipeline end-to-end):
  Make a change:      docker exec -i cdc-postgres psql -U demo -d inventory \
                        -c "UPDATE contracts SET monthly_price = 777 WHERE id = 1;"
  Watch it land:      ./run_all.sh --status          (bronze counts below via verifier)
  Verify everything:  docker exec cdc-spark /opt/spark/bin/spark-submit \
                        --packages io.delta:delta-spark_2.12:3.2.0 /jobs/query_lakehouse.py
  Fake app traffic:   python3 simulate_traffic.py --interval 3 --duration 60
  Kafka raw view:     docker exec -it cdc-kafka kafka-console-consumer \
                        --bootstrap-server localhost:9092 --topic inventory.public.contracts
  Web UIs:            Spark http://localhost:4040   Debezium http://localhost:8083/connectors
EOF

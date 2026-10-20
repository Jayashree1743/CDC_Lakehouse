#!/usr/bin/env bash
# =============================================================================
# DAY 1: Register the Debezium PostgreSQL connector and watch its status.
# Usage:  ./connect/register.sh
# =============================================================================
set -euo pipefail

CONNECT_URL="http://localhost:8083"
CONNECTOR_NAME="inventory-connector"
CONFIG_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "==> [1/4] Waiting for Kafka Connect REST API at $CONNECT_URL ..."
for i in $(seq 1 60); do
  if curl -sf "$CONNECT_URL/connectors" >/dev/null 2>&1; then
    echo "        Connect is UP (after ${i} checks)."
    break
  fi
  [ "$i" -eq 60 ] && { echo "ERROR: Connect never came up. Run: docker logs cdc-connect"; exit 1; }
  sleep 2
done

echo "==> [2/4] Checking connector plugin is loaded ..."
curl -s "$CONNECT_URL/connector-plugins" | tr ',' '\n' | grep -i postgresql || true

echo "==> [3/4] Registering connector '$CONNECTOR_NAME' ..."
HTTP_CODE=$(curl -s -o /tmp/connector_resp.json -w "%{http_code}" \
  -X POST -H "Content-Type: application/json" \
  --data @"$CONFIG_DIR/register-connector.json" \
  "$CONNECT_URL/connectors")

if [ "$HTTP_CODE" = "200" ] || [ "$HTTP_CODE" = "201" ]; then
  echo "        Registered OK (HTTP $HTTP_CODE)."
elif [ "$HTTP_CODE" = "409" ]; then
  echo "        Connector already exists (HTTP 409) - skipping."
else
  echo "        Registration FAILED (HTTP $HTTP_CODE):"
  cat /tmp/connector_resp.json
  exit 1
fi

echo "==> [4/4] Connector status (wait until state = RUNNING):"
for i in $(seq 1 15); do
  STATUS=$(curl -s "$CONNECT_URL/connectors/$CONNECTOR_NAME/status")
  echo "$STATUS"
  echo "$STATUS" | grep -q '"connector":{"state":"RUNNING"' && \
    echo "$STATUS" | grep -q '"tasks":\[{"id":0,"state":"RUNNING"' && \
    { echo "        Connector + task are RUNNING. Day 1 pipeline is LIVE."; exit 0; }
  sleep 2
done
echo "WARNING: connector not RUNNING yet. Check: docker logs cdc-connect | tail -30"
exit 1

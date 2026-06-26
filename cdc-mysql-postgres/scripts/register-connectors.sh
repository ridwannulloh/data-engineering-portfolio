#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────
# Registers the Debezium MySQL source + JDBC Postgres sink connectors with the
# running Kafka Connect worker. Idempotent: deletes any existing connector of
# the same name first, so re-running picks up config edits.
#
# Run this AFTER ./scripts/seed-dbs.sh (the source connector's initial snapshot
# needs the `ecommerce` database to already exist in MySQL).
#
# Order matters: the SOURCE is registered first and we wait until it has
# produced the change topics, THEN the SINK is registered. A sink subscribing
# via topics.regex only matches topics that already exist at subscribe time, so
# registering it too early leaves it with an empty partition assignment until
# the next metadata refresh (~5 min).
# ─────────────────────────────────────────────────────────────────────────
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONN_DIR="$SCRIPT_DIR/../connectors"

register() {  # <file-without-.json> <connector-name>
  local file="$1" name="$2" code
  echo "[connect] Registering '$name' ..."
  curl -s -o /dev/null -X DELETE "$CONNECT_URL/connectors/$name" || true
  sleep 1
  code=$(curl -s -o /tmp/cdc-conn-resp -w '%{http_code}' \
    -X POST -H 'Content-Type: application/json' \
    --data @"$CONN_DIR/$file.json" "$CONNECT_URL/connectors")
  if [[ "$code" =~ ^2 ]]; then
    echo "  -> created ($code)"
  else
    echo "  -> FAILED ($code):"; cat /tmp/cdc-conn-resp; echo; exit 1
  fi
}

echo "[connect] Waiting for Kafka Connect at $CONNECT_URL ..."
until curl -sf "$CONNECT_URL/" >/dev/null 2>&1; do echo "  ... still waiting"; sleep 3; done
echo "[connect] Kafka Connect is up."

# 1) Source first — it creates the dbserver1.ecommerce.* topics via snapshot.
register source-mysql mysql-source

# 2) Wait until the source has actually produced the change topics.
echo "[connect] Waiting for source to produce change topics ..."
for i in $(seq 1 40); do
  if curl -s "$CONNECT_URL/connectors/mysql-source/topics" | grep -q "dbserver1.ecommerce."; then
    echo "  -> topics are present."
    break
  fi
  sleep 3
done

# 3) Sink now subscribes to topics that already exist.
register sink-postgres postgres-sink

echo
echo "[connect] Connector status (give it a few seconds to reach RUNNING):"
for name in mysql-source postgres-sink; do
  echo "  $name: $(curl -s "$CONNECT_URL/connectors/$name/status" || true)"
done

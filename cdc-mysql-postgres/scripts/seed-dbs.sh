#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────
# Seeds the EXISTING databases for the CDC demo:
#   • applies sql/mysql_schema.sql to the MySQL container (tables + rows + user)
#   • ensures the target `ecommerce` database exists in the Postgres container
#
# Container names / credentials default to the detected local setup but can be
# overridden via environment variables (see .env.example).
# ─────────────────────────────────────────────────────────────────────────
set -euo pipefail

MYSQL_CONTAINER="${MYSQL_CONTAINER:-mysql-local}"
MYSQL_ROOT_PW="${MYSQL_ROOT_PW:-root123}"
PG_CONTAINER="${PG_CONTAINER:-local-postgres}"
PG_USER="${PG_USER:-postgres}"
PG_DB="${PG_DB:-ecommerce}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_DIR="$SCRIPT_DIR/../sql"

echo "[seed] Applying MySQL schema + seed to container '$MYSQL_CONTAINER' ..."
docker exec -i "$MYSQL_CONTAINER" \
  mysql -uroot -p"$MYSQL_ROOT_PW" < "$SQL_DIR/mysql_schema.sql"
echo "[seed] MySQL ready: database 'ecommerce' with 4 tables + seed rows + 'debezium' user."

echo "[seed] Ensuring target database '$PG_DB' exists in container '$PG_CONTAINER' ..."
if docker exec -i "$PG_CONTAINER" psql -U "$PG_USER" -tAc \
      "SELECT 1 FROM pg_database WHERE datname='$PG_DB'" | grep -q 1; then
  echo "[seed] Postgres database '$PG_DB' already exists."
else
  docker exec -i "$PG_CONTAINER" psql -U "$PG_USER" -c "CREATE DATABASE $PG_DB"
  echo "[seed] Created Postgres database '$PG_DB'."
fi

echo "[seed] Done. Target tables are auto-created by the sink connector on first event."

# Realtime CDC — MySQL → PostgreSQL (Debezium + Kafka Connect)

Log-based **Change Data Capture**: every row-level `INSERT` / `UPDATE` / `DELETE`
on a MySQL ecommerce database is streamed to PostgreSQL in **real time**, with
**no application code in the data path** — only connector configuration.

```
 Existing MySQL container (binlog, ROW format)
   │  io.debezium.connector.mysql.MySqlConnector        (source)
   ▼
 Kafka topics:  dbserver1.ecommerce.customers / .products / .orders / .order_items
   │  io.debezium.connector.jdbc.JdbcSinkConnector      (sink — upsert + delete)
   ▼
 Existing PostgreSQL container (ecommerce db)
```

The **Debezium JDBC sink** consumes the Debezium change envelope natively:
`insert.mode=upsert`, `delete.enabled=true` (tombstone-driven deletes),
`schema.evolution=basic`, and `auto.create=true` — so the target tables are
created automatically from the inferred schema. Source and sink run in one
Kafka Connect worker.

## What this stack does (and doesn't) run

This project brings up **only the streaming infrastructure**: Kafka, Zookeeper,
Kafka Connect, and Kafka UI. The **databases are your existing Docker
containers** — the connectors reach them through the host via
`host.docker.internal` (their `3306` / `5432` ports are published). Nothing
here creates or modifies your DB containers beyond seeding one demo database.

| Service | Image | Port | Purpose |
|---|---|---|---|
| zookeeper | `confluentinc/cp-zookeeper:7.5.0` | — | Kafka coordination |
| kafka | `confluentinc/cp-kafka:7.5.0` | 9092 | broker |
| kafka-ui | `provectuslabs/kafka-ui:latest` | 8080 | topic + connector observability |
| connect | `./connect` (Debezium 2.7.3 + JDBC sink + PG driver) | 8083 | source + sink worker |

## Prerequisites

- Docker + Docker Compose.
- A running **MySQL 8** container with **binlog enabled in `ROW` format**
  (the MySQL 8 Docker defaults satisfy this — verify with
  `SHOW VARIABLES LIKE 'binlog_format';` → `ROW`).
- A running **PostgreSQL** container.
- Both containers publish their ports to the host (`3306` / `5432`).

Defaults assume the local setup detected for this project:

| | container | host:port | credentials |
|---|---|---|---|
| Source (MySQL) | `mysql-local` | `host.docker.internal:3306` | root / `root123` |
| Target (Postgres) | `local-postgres` | `host.docker.internal:5432` | `postgres` / `postgres` |

Override via environment variables — see [`.env.example`](./.env.example). If
your hosts/ports/credentials differ, also update the values in
[`connectors/source-mysql.json`](./connectors/source-mysql.json) and
[`connectors/sink-postgres.json`](./connectors/sink-postgres.json).

## Run it

```bash
# 1) Start the Kafka / Connect stack (builds the custom Connect image once)
docker compose up -d --build

# 2) Seed the source schema + ~20 rows in MySQL, create the target db in Postgres
./scripts/seed-dbs.sh

# 3) Register the connectors (source first, then sink — see note below)
./scripts/register-connectors.sh
```

Within a few seconds PostgreSQL has the four tables auto-created and the seed
rows replicated. Open **Kafka UI at http://localhost:8080** to watch the
`dbserver1.ecommerce.*` change topics and connector status.

> **Order matters.** A sink connector subscribing via `topics.regex` only
> matches topics that already exist when it joins the consumer group. The
> registration script therefore registers the **source first**, waits until the
> change topics appear, then registers the **sink** — avoiding an empty-
> assignment cold start that would otherwise delay replication by ~5 minutes.

## Watch CDC in action

Make changes on the **source** (MySQL) …

```bash
docker exec -i mysql-local mysql -uroot -proot123 ecommerce <<'SQL'
INSERT INTO customers (name, email, city) VALUES ('Gita Rahma','gita@example.com','Malang');
UPDATE customers SET city='Denpasar' WHERE email='budi@example.com';
DELETE FROM order_items WHERE id = 7;
SQL
```

… and see them on the **target** (PostgreSQL) a few seconds later:

```bash
docker exec local-postgres psql -U postgres -d ecommerce -c \
  "SELECT name, city FROM customers ORDER BY id;"
```

`INSERT`s and `UPDATE`s arrive as upserts; `DELETE`s are propagated too
(`delete.enabled=true`).

## Schema

`sql/mysql_schema.sql` defines four related tables — each with a primary key,
which the JDBC sink uses as the upsert key (`primary.key.mode=record_key`):

- `customers (id, name, email, city, created_at, updated_at)`
- `products (id, name, category, price, created_at, updated_at)`
- `orders (id, customer_id→customers, status, total_amount, created_at, updated_at)`
- `order_items (id, order_id→orders, product_id→products, qty, unit_price)`

`sql/postgres_init.sql` only creates the target `ecommerce` database; the sink
auto-creates the tables on first event.

## Layout

```
cdc-mysql-postgres/
├── docker-compose.yml          # Kafka + Connect + Kafka UI (no DBs)
├── .env.example                # DB host/port/creds reference
├── connect/Dockerfile          # Debezium connect + JDBC sink + PG driver
├── sql/
│   ├── mysql_schema.sql        # 4 tables + ~20 seed rows + debezium user
│   └── postgres_init.sql       # create target ecommerce db
├── connectors/
│   ├── source-mysql.json       # Debezium MySQL source
│   └── sink-postgres.json      # Debezium JDBC sink (upsert + delete + auto.create)
└── scripts/
    ├── seed-dbs.sh             # seed the EXISTING mysql + postgres containers
    └── register-connectors.sh  # source-first connector registration
```

## Operating notes

- **Connector status:** `curl -s localhost:8083/connectors/mysql-source/status`
  and `.../postgres-sink/status` should report `RUNNING`.
- **Re-run safe:** `register-connectors.sh` deletes and recreates the
  connectors, so editing a config and re-running picks up the change.
- **`database.server.id`** (`184054`) must differ from the MySQL server's own
  `server_id` and be unique among any replicas.

## Teardown

```bash
docker compose down -v        # removes the Kafka/Connect stack + its volumes
```

Your `mysql-local` and `local-postgres` containers are left untouched. To also
drop the demo data: `DROP DATABASE ecommerce;` on each.

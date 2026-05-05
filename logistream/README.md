# LogiStream — Real-Time Supply Chain Streaming Pipeline

A production-grade real-time data pipeline that tracks shipment events across
carriers and warehouses, detects SLA breaches, and surfaces delay alerts through
a REST API — all running in a single `docker-compose up --build`.

---

## How the data flows

```
┌─────────────────────────────────────────────────────────────────┐
│  INGEST — ingest/producer.py                                    │
│                                                                 │
│  Generates synthetic logistics events (shipments, carrier       │
│  updates, warehouse scans) and publishes to 3 Kafka topics      │
│  at a configurable rate (default 5 events/sec).                 │
└──────────────────────┬──────────────────────────────────────────┘
                       │  3 Kafka topics
           ┌───────────┴───────────┐
    shipment-events         carrier-updates
    (6 partitions)          warehouse-ops

┌──────────▼──────────────────────────────────────────────────────┐
│  PROCESS — spark/streaming_job.py                               │
│                                                                 │
│  Spark Structured Streaming reads all 3 topics continuously     │
│  and applies Medallion Architecture transformations:            │
│                                                                 │
│  BRONZE  Parse raw JSON → typed schema, store as-is            │
│    ↓                                                            │
│  SILVER  Clean + enrich:                                        │
│          • cast timestamps, drop nulls, deduplicate            │
│          • classify delay: NONE / LOW / MEDIUM / HIGH           │
│          • SLA breach check (STANDARD 48h / EXPRESS 24h /       │
│            OVERNIGHT 12h thresholds)                            │
│          • geo_region bucket from longitude                     │
│    ↓                                                            │
│  GOLD    Aggregate every 60 seconds:                            │
│          • carrier KPIs: avg delay, p95 delay, SLA breach rate  │
│          • active alerts: EXCEPTION + HIGH severity + breached  │
└──────────────────────┬──────────────────────────────────────────┘
                       │  writes to shared Docker volume
              /data/delta/{bronze,silver,gold}

┌──────────▼──────────────────────────────────────────────────────┐
│  SERVE — api/main.py                                            │
│                                                                 │
│  FastAPI reads Gold Delta tables and exposes:                   │
│  GET /alerts   → shipments with HIGH delay or SLA breach        │
│  GET /metrics  → carrier KPIs over a configurable time window   │
│  GET /health   → confirms all 4 Delta tables are reachable      │
│  GET /api/docs → interactive Swagger UI                         │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  ORCHESTRATE — airflow/dags/logistream_dag.py                   │
│                                                                 │
│  Hourly Airflow DAG handles maintenance:                        │
│  health_check → silver_optimize (ZORDER) → gold_optimize        │
│              → bronze_vacuum (7-day retention) → alert_summary  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer       | Technology                              |
|-------------|------------------------------------------|
| Ingest      | Apache Kafka 3.6, kafka-python-ng        |
| Processing  | PySpark 3.5, Spark Structured Streaming  |
| Storage     | Delta Lake 3.1, Parquet, Medallion arch  |
| Serving     | FastAPI 0.111, Uvicorn, Pydantic v2      |
| Orchestrate | Apache Airflow 2.9                       |
| Container   | Docker, Docker Compose                   |
| Testing     | pytest 8.2, PySpark local mode           |
| Config      | Pydantic Settings, python-dotenv         |

---

## Quickstart (Docker — recommended)

**Prerequisites:** Docker Desktop with at least **4 GB RAM** allocated
(Settings → Resources → Memory).

```bash
# Clone and enter the project
git clone https://github.com/ridwannulloh/data-engineering-portfolio.git
cd data-engineering-portfolio/logistream

# Start the full pipeline — everything runs automatically
docker-compose up --build
```

That single command:
1. Starts **Zookeeper + Kafka** and waits until Kafka is healthy
2. Runs **`init`** — creates the 3 Kafka topics, then exits
3. Starts **`producer`** — streams synthetic events at 5/sec
4. Starts **`spark`** — Structured Streaming job writing to Delta Lake
5. Starts **`api`** — FastAPI server backed by Gold Delta tables
6. Starts **`kafka-ui`** — visual dashboard for Kafka topics

### Verify the pipeline is running

| URL | Expected |
|-----|----------|
| http://localhost:8080 | Kafka UI — topics with live messages |
| http://localhost:4040 | Spark UI — 6 streaming queries active |
| http://localhost:8000/health | `{"status": "healthy"}` |
| http://localhost:8000/alerts | List of delay alerts |
| http://localhost:8000/metrics | Carrier KPI aggregates |
| http://localhost:8000/api/docs | Interactive Swagger UI |

> **Note:** The API returns data after Spark completes its first micro-batch
> (~60 seconds after startup). `/health` will show `"degraded"` until then.

### Clean up

```bash
# Stop everything and remove all data (containers + volumes + image)
docker-compose down -v --rmi local
```

---

## Running Tests

No Docker or Kafka needed — tests run in PySpark local mode:

```bash
pip install -r requirements.txt
pytest tests/ -v
```

**Test coverage:**
- `TestCleanAndEnrich` — SLA breach logic, delay classification, geo bucketing, deduplication
- `TestActiveAlerts` — alert generation rules
- `TestProducer` — event serialisation, state machine progression
- `TestAPI` — endpoint contracts, query parameter validation, mock responses

---

## Project Structure

```
logistream/
├── ingest/
│   ├── producer.py          # Synthetic shipment event generator (stateful state machine)
│   ├── schemas.py           # Event dataclasses: ShipmentEvent, CarrierUpdate, WarehouseOp
│   └── topics.py            # Kafka topic definitions and partition config
├── spark/
│   ├── streaming_job.py     # Main Spark Structured Streaming job (Bronze → Silver → Gold)
│   ├── transforms.py        # StructType schemas, SLA classification, windowed aggregations
│   └── delta_writer.py      # Delta Lake writeStream helper
├── api/
│   ├── main.py              # FastAPI app — /alerts, /metrics, /health
│   ├── models.py            # Pydantic v2 response models
│   └── queries.py           # Delta Lake read queries (PySpark local mode)
├── airflow/
│   └── dags/
│       └── logistream_dag.py  # Hourly maintenance DAG (OPTIMIZE, ZORDER, VACUUM)
├── tests/
│   └── test_logistream.py   # 12 pytest tests — no external services required
├── config/
│   └── settings.py          # Pydantic Settings — all config via env vars or .env
├── scripts/
│   ├── create_topics.py     # One-shot topic creation (called automatically by Docker)
│   └── warmup_spark.py      # Pre-downloads Spark JARs during Docker image build
├── Dockerfile               # Java 17 + Python — single image for all app services
├── docker-compose.yml       # Full stack: Kafka + producer + Spark + API + Kafka UI
├── requirements.txt
└── .env.example             # Environment variable template
```

---

## Configuration

Copy `.env.example` to `.env` and adjust if needed.
All settings can also be passed as environment variables (used by Docker Compose).

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| `SPARK_MASTER` | `local[*]` | Spark master URL |
| `DELTA_BRONZE_PATH` | `/tmp/logistream/delta/bronze` | Bronze layer path |
| `DELTA_SILVER_PATH` | `/tmp/logistream/delta/silver` | Silver layer path |
| `DELTA_GOLD_PATH` | `/tmp/logistream/delta/gold` | Gold layer path |
| `SLA_STANDARD_HOURS` | `48` | Standard service SLA threshold |
| `SLA_EXPRESS_HOURS` | `24` | Express service SLA threshold |
| `SLA_OVERNIGHT_HOURS` | `12` | Overnight service SLA threshold |

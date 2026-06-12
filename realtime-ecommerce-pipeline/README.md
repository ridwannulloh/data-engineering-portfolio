# Real-Time E-Commerce Intelligence Platform

> **End-to-end streaming data pipeline** — Kafka → PyFlink → Lakehouse (Bronze/Silver/Gold) → Analytical Gold tables  
> Stack: Python · Apache Kafka · PyFlink · MinIO (S3) · Apache Airflow · Parquet

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES (Simulated)                        │
│   Order Events   │   Inventory Updates   │   Clickstream Events         │
└────────┬─────────┴──────────┬────────────┴──────────┬───────────────────┘
         │                    │                        │
         ▼                    ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        APACHE KAFKA (Topics)                            │
│  ecommerce.orders  │  ecommerce.inventory_updates  │  ecommerce.click.. │
└────────┬───────────┴───────────────┬───────────────┴────────────────────┘
         │                           │
    ┌────┴──────────┐         ┌──────┴───────────┐
    │  PYFLINK JOB  │         │  PYFLINK JOB     │
    │  GMV Hourly   │         │  Stockout Risk   │
    │  (Tumbling)   │         │  (Sliding Window)│
    └───────────────┘         └──────────────────┘
         │                           │
         ▼                           ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    LAKEHOUSE — MinIO / S3                               │
│                                                                         │
│  Bronze/                Silver/                 Gold/                   │
│  ├── orders/            ├── orders_enriched/    ├── gmv_hourly/         │
│  ├── inventory/         └── inventory_snapshot/ ├── stockout_risk/      │
│  └── clickstream/                               └── customer_rfm/       │
│                                                                         │
│  Raw NDJSON             Cleaned Parquet          Analytical Parquet     │
│  (append-only)          (+ quarantine)           (Snappy compressed)    │
└─────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      APACHE AIRFLOW (Orchestration)                     │
│  start → bronze_to_silver → silver_to_gold → end   (every 30 min)      │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
realtime-ecommerce-pipeline/
├── config.py                        # Shared constants (topics, paths, credentials)
├── requirements.txt
│
├── producers/
│   ├── order_producer.py            # Simulates order + status events → Kafka
│   ├── inventory_producer.py        # Simulates warehouse stock changes → Kafka
│   └── clickstream_producer.py      # Simulates user browsing events → Kafka
│
├── flink_jobs/
│   ├── gmv_aggregation.py           # Tumbling 1-hr window: GMV by channel
│   └── stockout_detector.py         # Sliding 15-min window: stockout risk alerts
│
├── consumers/
│   └── bronze_sink.py               # Kafka → MinIO Bronze layer (NDJSON, partitioned)
│
├── pipelines/
│   ├── bronze_to_silver.py          # Batch: clean + enrich → Silver Parquet
│   ├── silver_to_gold.py            # Batch: GMV hourly + Customer RFM → Gold Parquet
│   └── airflow_dag.py               # Airflow DAG (30-min schedule)
│
├── tests/
│   └── test_transformations.py      # Unit tests (pytest) — no external deps needed
│
└── docker/
    ├── docker-compose.yml           # Full stack: infra + app services
    ├── Dockerfile                   # App image (producers, sink, pipelines)
    └── requirements-docker.txt      # Lean runtime deps for the app image
```

---

## Key Engineering Decisions

| Decision | Rationale |
|---|---|
| **Partitioned S3 keys** (`year=/month=/day=/hour=`) | Enables partition pruning on downstream reads |
| **NDJSON for Bronze** | Append-friendly, schema-flexible, human-readable for debugging |
| **Parquet + Snappy for Silver/Gold** | Columnar format reduces I/O for analytical workloads |
| **BatchWriter in bronze_sink** | Avoids small-file problem — flushes every 100 records or 30s |
| **Quarantine layer** | Malformed records never silently dropped; available for replay |
| **Sliding window (stockout)** | Captures velocity, not just point-in-time stock levels |
| **Quintile RFM scoring** | Robust to skewed distributions vs. fixed-threshold scoring |

---

## Gold Layer Outputs

### `gold/gmv_hourly/`

| Column | Type | Description |
|---|---|---|
| `window_hour` | timestamp | UTC hour start |
| `channel` | string | web / mobile / partner_api |
| `total_gmv` | float | Gross Merchandise Value |
| `order_count` | int | Number of orders |
| `avg_order_value` | float | GMV / orders |
| `unique_customers` | int | Distinct buyers |

### `gold/customer_rfm/`

| Column | Type | Description |
|---|---|---|
| `customer_id` | string | |
| `recency_days` | int | Days since last order |
| `frequency` | int | Total order count |
| `monetary` | float | Total spend |
| `r_score` | int (1–5) | Recency quintile |
| `f_score` | int (1–5) | Frequency quintile |
| `m_score` | int (1–5) | Monetary quintile |
| `segment` | string | Champions / Loyal / At Risk / Lost … |

---

## Quickstart

### Run everything with one command

```bash
cd docker
docker compose up --build
```

This builds the app image and starts the full stack — infrastructure **and**
the pipeline services. Out of the box you get a live medallion flow:
producers → Kafka → Bronze sink (MinIO) → batch loop (Silver → Gold, every 2 min).

Services / UIs:
- Kafka:      `localhost:9092`
- Kafka UI:   `http://localhost:8080`
- Flink UI:   `http://localhost:8081`
- MinIO UI:   `http://localhost:9001`  (user: `minioadmin` / `minioadmin`)

Containerized app services (all run from one built image, `docker/Dockerfile`):

| Service | Role |
|---|---|
| `order-producer` / `inventory-producer` / `clickstream-producer` | Stream simulated events → Kafka |
| `bronze-sink` | Kafka → MinIO Bronze (partitioned NDJSON) |
| `batch-pipeline` | Loops `bronze_to_silver` → `silver_to_gold` every 120s |

Watch the data land:

```bash
docker compose logs -f bronze-sink batch-pipeline
# then browse http://localhost:9001 → ecommerce-lakehouse → bronze/ silver/ gold/
```

Tear down (add `-v` to also wipe the MinIO volume):

```bash
docker compose down -v
```

> **Connection endpoints** are environment-driven (`config.py`). In Compose they
> point at `kafka:29092` / `minio:9000`; running on the host they default to
> `localhost`. So the same code works both ways.

### Run pieces individually on the host (optional)

```bash
pip install -r requirements.txt           # full deps incl. PyFlink + pytest
docker compose up -d kafka minio minio-init   # infra only

python producers/order_producer.py        # each in its own terminal
python consumers/bronze_sink.py
python pipelines/bronze_to_silver.py
python pipelines/silver_to_gold.py
```

### Flink streaming jobs (run manually)

The PyFlink jobs are **not** auto-started by Compose — they require a JVM and
the Flink Kafka connector jar, so they run as a separate step against the
Flink cluster (or a local mini-cluster):

```bash
python flink_jobs/gmv_aggregation.py
python flink_jobs/stockout_detector.py
```

### Run tests

```bash
pip install -r requirements.txt
pytest tests/ -v --cov=pipelines
```

---

## Upwork Positioning

This project demonstrates:

- **Real-time ingestion** at configurable throughput via Kafka producers
- **Stream processing** with PyFlink: tumbling windows (GMV) and sliding windows (stockout risk)
- **Medallion Architecture** (Bronze → Silver → Gold) as the industry standard lakehouse pattern
- **Data quality enforcement** — quarantine pattern, null checks, deduplication
- **RFM segmentation** — economics-informed customer analytics using quintile scoring
- **Orchestration** — production-style Airflow DAG with retry logic
- **Unit testing** — all business logic covered without external service dependencies

---

## Monitoring Checklist

| What to monitor | Where |
|---|---|
| Kafka consumer lag | Kafka UI → Consumer Groups |
| Flink job health | Flink UI → Jobs |
| S3 Bronze file count | MinIO UI → `ecommerce-lakehouse/bronze/` |
| Quarantine record count | MinIO UI → `ecommerce-lakehouse/quarantine/` |
| Gold RFM segment distribution | Query Gold Parquet with DuckDB or pandas |

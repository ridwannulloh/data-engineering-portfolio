# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is a multi-project data engineering portfolio. Each subdirectory is an independent, self-contained project with its own `requirements.txt`, `Dockerfile`, and `docker-compose.yml`. There is no shared virtual environment or top-level build system — work inside the project directory you're targeting.

## Projects & How to Run Them

### logistream — Real-Time Supply Chain Streaming Pipeline

Full stack: Kafka → PySpark Structured Streaming → Delta Lake (Medallion) → FastAPI.

```bash
cd logistream
docker compose up --build          # starts everything: kafka, spark, api, airflow
```

After startup, services are at:
- FastAPI + Swagger: `http://localhost:8000/api/docs`
- Kafka UI: `http://localhost:8080`
- Spark UI: `http://localhost:4040`

Run tests (no external services needed — PySpark local mode):
```bash
cd logistream
pytest tests/ -v
pytest tests/test_logistream.py::TestCleanAndEnrich::test_zero_delay_no_breach -v  # single test
```

### econ-dashboard — Global Economic Indicators Dashboard

Streamlit app reading from BigQuery, deployed on GCP Cloud Run.

```bash
cd econ-dashboard
pip install -r requirements.txt
streamlit run app.py               # requires GCP credentials set in env
python ingest.py                   # run ETL manually
python ingest.py --dry-run         # preview rows without loading to BigQuery
```

CI/CD: `.github/workflows/deploy.yml` auto-deploys to Cloud Run on every push to `main` that touches `econ-dashboard/**`. Requires `GCP_PROJECT_ID` and `GCP_SA_KEY` GitHub secrets.

### olist-etl-pipeline — Batch ETL with Airflow + PySpark

Medallion Architecture on the Brazilian Olist e-commerce dataset. Dataset is auto-downloaded from Kaggle via `kagglehub` at runtime — set `KAGGLE_API_TOKEN` in `.env`.

```bash
cd olist-etl-pipeline
cp .env.example .env               # then set KAGGLE_API_TOKEN
docker compose up --build          # Airflow webserver + scheduler + PostgreSQL
```

Airflow UI: `http://localhost:8080` (user: `airflow` / pass: `airflow`)

Run tests:
```bash
cd olist-etl-pipeline
pytest tests/ -v
```

### deploy-ai-fastapi — NYSE Stock Price Prediction API

XGBoost models served via FastAPI. Two Docker strategies:

```bash
cd deploy-ai-fastapi
# Strategy A — fast build with pre-trained models (~2 min)
docker compose -f docker-compose.prebuilt.yml up --build

# Strategy B — full retrain on build (~10 min)
docker compose up --build

# Run locally without Docker
pip install -r requirements.txt
uvicorn app.main:app --reload      # API at http://localhost:8000, Swagger at /docs
```

API key is required via `X-API-Key` header. Rate limit: 10 req/min per key.

### realtime-ecommerce-pipeline — Flink + Kafka + MinIO Lakehouse

Kafka → PyFlink (streaming) → MinIO (Bronze/Silver/Gold Parquet) → Airflow (batch).

```bash
cd realtime-ecommerce-pipeline/docker
docker compose up -d               # Kafka + Flink + MinIO

cd ..
pip install -r requirements.txt

# Start producers (separate terminals)
python producers/order_producer.py
python producers/inventory_producer.py
python producers/clickstream_producer.py

# Sink to Bronze
python consumers/bronze_sink.py

# Batch pipelines
python pipelines/bronze_to_silver.py
python pipelines/silver_to_gold.py

# Flink streaming jobs
python flink_jobs/gmv_aggregation.py
python flink_jobs/stockout_detector.py

# Tests
pytest tests/ -v --cov=pipelines
```

Services: Kafka `localhost:9092`, Kafka UI `localhost:8080`, Flink UI `localhost:8081`, MinIO UI `localhost:9001` (minioadmin/minioadmin).

### fraudlens-lakehouse — Financial Fraud Detection Lakehouse

PySpark (local mode) + Delta Lake on MinIO, orchestrated by Airflow. Medallion Architecture over seeded PaySim-style transactions; the DAG triggers itself on startup (`@once`, unpaused).

```bash
cd fraudlens-lakehouse
docker compose up --build -d       # MinIO + Postgres + Airflow; DAG runs automatically
docker compose exec airflow-scheduler python /opt/airflow/scripts/show_results.py   # print Gold tables
```

Airflow UI: `http://localhost:8080` (admin/admin). MinIO console: `http://localhost:9001` (minioadmin/minioadmin). Port 8080 conflicts with olist-etl-pipeline's Airflow and logistream's Kafka UI — run one stack at a time.

Run tests (no Docker needed — Spark `local[1]` with `STORAGE_BACKEND=local` writing Delta to a tmp dir):
```bash
cd fraudlens-lakehouse
pytest tests/ -v
```

### dbt_redshit — dbt Models for Redshift

```bash
cd dbt_redshit
dbt deps                           # install packages from packages.yml
dbt run                            # run all models
dbt test                           # run schema/data tests
dbt run --select staging           # run only staging layer
dbt run --select marts             # run only marts layer
```

Profile: `portfolio_redshift` — configure credentials in `~/.dbt/profiles.yml` or use `profiles.yml` in the project root. Staging models are views; marts are tables with a `GRANT SELECT TO GROUP reporters` post-hook.

### personal-web — Portfolio Website

FastAPI + Jinja2 + TailwindCSS.

```bash
cd personal-web
pip install -r requirements.txt
uvicorn app:app --reload           # http://localhost:8000
```

## Architecture Patterns Used Across Projects

**Medallion Architecture** (Bronze → Silver → Gold) is the dominant storage pattern used in `logistream`, `olist-etl-pipeline`, and `realtime-ecommerce-pipeline`. Raw data lands in Bronze unchanged; Silver adds cleaning/enrichment; Gold holds aggregated, analytical-ready tables.

**Configuration via environment variables**: All projects read config from `.env` files using either `pydantic-settings` (`logistream`) or `os.environ` directly. Never hardcode credentials — copy `.env.example` to `.env`.

**Tests run without external dependencies**: PySpark tests use `local[1]` mode; API tests mock Delta Lake reads; producer tests mock Kafka. This is intentional — `pytest tests/ -v` should always work without running Docker.

**Docker Compose is the primary local development method**: Each project's `docker-compose.yml` starts the complete stack including infrastructure (Kafka, Postgres, MinIO) and application services with correct dependency ordering and health checks.

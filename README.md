# Data Engineering Portfolio

Welcome to my Data Engineering Portfolio! This repository showcases my expertise in building scalable data pipelines, designing data architectures, and implementing modern data engineering solutions.

## 👨‍💻 About Me

I am a passionate Data Engineer with experience in designing, building, and maintaining robust data infrastructure. My work focuses on transforming raw data into actionable insights through efficient ETL/ELT pipelines, data warehousing, and cloud-based solutions.

## 🛠️ Technical Skills

### Languages
- Python
- SQL
- Scala
- Java

### Big Data Technologies
- Apache Spark
- Apache Kafka
- Apache Airflow
- Apache Flink
- Hadoop

### Cloud Platforms
- AWS (S3, Redshift, Glue, EMR, Lambda, Kinesis)
- Google Cloud Platform (BigQuery, Dataflow, Pub/Sub)
- Azure (Data Factory, Synapse Analytics, Databricks)

### Databases & Data Warehouses
- PostgreSQL
- MySQL
- MongoDB
- Redis
- Snowflake
- Redshift
- BigQuery

### Tools & Frameworks
- Docker & Kubernetes
- dbt (Data Build Tool)
- Git & GitHub Actions
- Terraform
- CI/CD Pipelines

## 📂 Projects

### Project 1: LogiStream — Real-Time Supply Chain Streaming Pipeline
**Description:** Production-grade real-time streaming pipeline that tracks shipment events across carriers and warehouses, detects SLA breaches, and surfaces delay alerts via a REST API. Events flow from a synthetic Kafka producer through Spark Structured Streaming into a Delta Lake Medallion Architecture, then served by FastAPI. Fully containerised — one `docker-compose up --build` starts the entire pipeline with zero manual steps.

**Technologies:** Apache Kafka, PySpark 3.5, Spark Structured Streaming, Delta Lake, FastAPI, Apache Airflow, Docker Compose

**Key Features:**
- 3 Kafka topics (shipment-events, carrier-updates, warehouse-ops) with stateful synthetic producer
- Spark Structured Streaming with schema enforcement, deduplication, watermarking
- Medallion Architecture: Bronze (raw) → Silver (SLA-enriched) → Gold (carrier KPIs + alerts)
- Configurable SLA thresholds per service tier (Standard 48h / Express 24h / Overnight 12h)
- FastAPI alert service with interactive Swagger UI at `/api/docs`
- Hourly Airflow DAG for OPTIMIZE, ZORDER, VACUUM maintenance
- 12 pytest tests covering transforms, producer state machine, and API endpoints

**[View Project →](./logistream)**

---

### Project 2: Global Economic Indicators Dashboard
**Description:** Live end-to-end data pipeline that fetches economic indicators from the World Bank API, stores data in BigQuery, and displays interactive visualizations through a Streamlit dashboard deployed on GCP Cloud Run. Features automated monthly data ingestion via Cloud Scheduler.

**Technologies:** Python, BigQuery, Streamlit, Docker, GCP Cloud Run, Cloud Scheduler, GitHub Actions

**Key Features:**
- Real-time economic data visualization for G7 + Indonesia (8 countries)
- Automated monthly ETL pipeline with truncate-and-load strategy
- Dark-themed interactive dashboard with Plotly charts
- Fully containerized deployment with CI/CD automation
- Cost-optimized (~$0.10-0.20/month) within GCP free tier

**[View Project →](./econ-dashboard)** | **[Live Demo →](https://econ-dashboard-986403815263.us-central1.run.app)**

---

### Project 3: Olist E-Commerce ETL Pipeline
**Description:** End-to-end batch data pipeline on the Brazilian Olist e-commerce dataset implementing Medallion Architecture (Bronze → Silver → Gold). Ingests 100k+ orders across 8 source tables, applies multi-table joins and cleaning in PySpark, and produces three analytical gold tables covering customer RFM segmentation, monthly revenue trends, and category performance. Fully containerised with Docker Compose and orchestrated via Apache Airflow. Dataset is auto-downloaded from Kaggle at runtime via kagglehub.

**Technologies:** Python, PySpark, Delta Lake, Apache Airflow, Docker, DuckDB, kagglehub

**Key Features:**
- Medallion Architecture (Bronze → Silver → Gold) with Delta Lake storage
- Automated Kaggle dataset ingestion via `kagglehub` — no manual download needed
- Customer RFM segmentation (Champions, Loyal Customers, At Risk, Lost, etc.)
- Monthly GMV trends and category performance analytics
- Containerised stack: Airflow scheduler + webserver + PostgreSQL in Docker Compose
- DuckDB + DBeaver integration for ad-hoc SQL querying on gold layer outputs
- Airflow DAG with retry logic and daily scheduling (02:00 UTC)
- pytest unit tests for Silver transformation logic

**[View Project →](./olist-etl-pipeline)**

---

### Project 4: NYSE Stock Price Prediction API
**Description:** Production-ready FastAPI application that predicts S&P 500 stock close prices using XGBoost models trained on the NYSE Kaggle dataset. Supports multiple prediction horizons from 1 day to 1 year, with 19 engineered technical indicators per stock. Features async endpoints, API key authentication, rate limiting, and two Docker deployment strategies — full retrain on build or fast deploy with pre-trained models.

**Technologies:** Python, XGBoost, FastAPI, scikit-learn, joblib, pandas, Docker, Docker Compose

**Key Features:**
- 6 XGBoost regressors (one per horizon: `1d`, `7d`, `1m`, `3m`, `6m`, `1y`) with high accuracy (R² up to 0.9988)
- 19 engineered technical indicators: SMA/EMA ratios, MACD, RSI, volatility, volume, price returns, lag features
- API key authentication via `X-API-Key` header with rate limiting (10 req/min per key, configurable)
- Pydantic v2 request/response validation with enum horizon + ticker constraints
- Two Docker strategies: **Strategy A** (prebuilt — ~2 min build using saved models) and **Strategy B** (full retrain — ~10 min, bakes fresh models into image)
- Structured logging, `/health` check endpoint, `/stocks` ticker list, and `/predict` endpoint
- Auto-generated Swagger UI at `/docs`

**[View Project →](./deploy-ai-fastapi)**

---

### Project 5: FraudLens Lakehouse — Financial Fraud Detection
**Description:** Live-demonstrable financial fraud detection pipeline on a Medallion lakehouse (Bronze → Silver → Gold). Airflow orchestrates PySpark jobs that write Delta Lake tables to MinIO (S3-compatible object storage), detecting mobile-money fraud in PaySim-style transaction data with an explainable rule-based risk score — **91% precision / 87% F1**, evaluated honestly against labels in a dedicated Gold table. Fully local: no cluster, no cloud account, no API keys. The DAG triggers itself on startup — one `docker compose up` and the lakehouse builds while you watch.

**Technologies:** PySpark 3.5, Delta Lake 3.2, Apache Airflow, MinIO (S3), Docker Compose, pandas, pytest

**Key Features:**
- Medallion Architecture on a real object store: Delta tables written to MinIO via `s3a://`, browsable in the MinIO console
- Seeded PaySim-schema data generator — credential-free, fully reproducible, and the real 6.3M-row Kaggle dataset drops in unchanged
- Silver-layer behavioural feature engineering: full balance drain, stale mule-account balances, time-of-day patterns
- Four Gold analytics tables: fraud exposure by channel, hourly fraud patterns (01:00–06:00 concentration), ranked high-risk account review queue, and detection precision/recall/F1
- Explainable rule-based detector (no black box) — weighted risk score with labels used only for evaluation, never as input
- Spark jars pre-resolved at Docker build time — first DAG run never touches Maven
- pytest suite runs without Docker: Spark `local[1]` mode with Delta tables in a tmp directory

**[View Project →](./fraudlens-lakehouse)**

---

## 📫 Contact

- **LinkedIn:** [linkedin.com/in/ridwannulloh](https://linkedin.com/in/ridwannulloh)
- **GitHub:** [github.com/ridwannulloh](https://github.com/ridwannulloh)

## 📄 License

This repository is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

⭐️ If you find my work interesting, feel free to star this repository!

# olist-etl-pipeline

PySpark ETL pipeline on the Brazilian Olist e-commerce dataset.
Implements a Medallion Architecture (Bronze → Silver → Gold) with customer
RFM segmentation, revenue analytics, and Airflow orchestration.
The dataset is downloaded automatically from Kaggle at runtime via `kagglehub`.

## Architecture

```
Kaggle (auto-downloaded via kagglehub)
    │
    ▼
data/raw/       ← source CSVs (8 files, ~100k orders)
    │
    ▼
data/bronze/    ← raw Delta tables + ingestion metadata (_ingested_at, _source_file)
    │
    ▼
data/silver/
    └── orders_fact    ← cleaned wide fact table, partitioned by purchase_year/month
    │
    ▼
data/gold/
    ├── customer_rfm           ← RFM segments (Champions, Loyal, At Risk, …)
    ├── monthly_revenue        ← GMV, order count, AOV by state & month
    └── category_performance   ← revenue and delivery metrics by product category
```

## Stack

| Layer         | Tool                  |
|---------------|-----------------------|
| Processing    | PySpark 3.5           |
| Storage       | Delta Lake 3.1        |
| Orchestration | Apache Airflow 2.8    |
| Data download | kagglehub 0.3.4       |
| Container     | Docker + Compose      |
| Query / View  | DuckDB + DBeaver      |
| Testing       | pytest                |

## Quickstart

### Option 1: Docker (Recommended)

Requires Docker Desktop with at least **6 GB RAM** allocated.

```powershell
# 1. Copy env file and add your Kaggle API token
#    Get it at https://www.kaggle.com/settings → API → Generate New Token
cp .env.example .env
# Edit .env and set KAGGLE_API_TOKEN=<your_token>

# 2. Build the image
docker-compose build

# 3. Initialise Airflow (run once)
docker-compose up airflow-init

# 4. Start all services
docker-compose up -d

# 5. Access Airflow UI at http://localhost:8080
#    Username: airflow  |  Password: airflow
#    Enable the olist_pipeline DAG and trigger a run.
#    The dataset downloads automatically on the first run.
```

### Option 2: Local Installation

Requires Python 3.11+ and Java 17.

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set your Kaggle API token
export KAGGLE_API_TOKEN=<your_token>   # Linux/Mac
# $env:KAGGLE_API_TOKEN="<your_token>" # Windows PowerShell

# 3. Run the pipeline (dataset downloads automatically on first run)
python -m src.ingestion.ingest_raw
python -m src.transformation.clean_orders
python -m src.aggregation.customer_ltv

# 4. Run tests
pytest tests/ -v
```

## Docker Services

| Service             | Port | Description                              |
|---------------------|------|------------------------------------------|
| airflow-webserver   | 8080 | Airflow Web UI                           |
| airflow-scheduler   | —    | Triggers and monitors DAG runs           |
| postgres            | 5432 | Airflow metadata database                |
| airflow-init        | —    | One-time DB migration + admin user setup |

PySpark runs as a library **inside** the Airflow containers (not a separate service).
Java 17 is bundled in the Docker image.

## DAG: `olist_pipeline`

Scheduled daily at **02:00 UTC**. Task dependency:

```
bronze_ingestion → silver_transformation → gold_aggregation
```

| Task                   | Module                          | Output                    |
|------------------------|---------------------------------|---------------------------|
| `bronze_ingestion`     | `src.ingestion.ingest_raw`      | `data/bronze/<table>/`    |
| `silver_transformation`| `src.transformation.clean_orders`| `data/silver/orders_fact/`|
| `gold_aggregation`     | `src.aggregation.customer_ltv`  | `data/gold/<table>/`      |

## Project Structure

```
olist-etl-pipeline/
├── Dockerfile                     # Airflow + Java 17 + pipeline deps
├── docker-compose.yml             # Postgres + Airflow webserver/scheduler/init
├── .env.example                   # Environment variable template
├── requirements.txt               # Python dependencies
├── src/
│   ├── ingestion/
│   │   └── ingest_raw.py          # Bronze layer (+ kagglehub auto-download)
│   ├── transformation/
│   │   └── clean_orders.py        # Silver layer — joins & cleans orders fact
│   ├── aggregation/
│   │   └── customer_ltv.py        # Gold layer — RFM, revenue, category tables
│   └── utils/
│       └── spark_session.py       # SparkSession factory (Delta Lake config)
├── dags/
│   └── olist_pipeline_dag.py      # Airflow DAG definition
├── tests/
│   └── test_transformations.py    # pytest unit tests for Silver transforms
├── view_results.py                # Print gold table summaries via DuckDB
├── create_duckdb.py               # Create results.duckdb for DBeaver
└── data/
    ├── raw/                       # Downloaded CSVs (gitignored)
    ├── bronze/                    # Delta tables (gitignored)
    ├── silver/                    # Delta tables (gitignored)
    └── gold/                      # Delta tables (gitignored)
```

## Querying Results with DuckDB

DuckDB reads the Parquet files produced by the pipeline directly — no database
server needed. Install it alongside the rest of the dependencies:

```powershell
pip install duckdb pandas
```

### Quick terminal view

Print formatted summaries of all gold tables to the terminal:

```powershell
# All three gold tables
python view_results.py

# Single table
python view_results.py customer_rfm

# Multiple specific tables
python view_results.py monthly_revenue category_performance
```

### DBeaver (GUI)

Generate a persistent `results.duckdb` file with pre-built views for every
layer (bronze, silver, gold), then open it in DBeaver for full SQL exploration:

```powershell
python create_duckdb.py
```

This prints the absolute path to the generated file. In DBeaver:

1. **Database** → **New Connection** → search `DuckDB` → select it
2. Click **Download** if prompted to install the driver
3. Set **Path** to the file path printed by the script
4. **Test Connection** → **Finish**

All views appear in the schema browser under `main/`:

```
main/
├── gold_customer_rfm
├── gold_monthly_revenue
├── gold_category_performance
├── silver_orders_fact
├── bronze_orders
├── bronze_order_items
├── bronze_customers
└── ...
```

Example queries in DBeaver:

```sql
-- Revenue by segment
SELECT segment,
       COUNT(*)                    AS customers,
       ROUND(AVG(monetary), 2)     AS avg_spend_brl,
       ROUND(AVG(recency_days), 1) AS avg_recency_days
FROM gold_customer_rfm
GROUP BY segment
ORDER BY avg_spend_brl DESC;

-- Monthly GMV trend
SELECT purchase_year,
       purchase_month,
       ROUND(SUM(revenue), 2) AS total_revenue_brl,
       SUM(order_count)       AS total_orders
FROM gold_monthly_revenue
GROUP BY purchase_year, purchase_month
ORDER BY purchase_year, purchase_month;

-- Top 10 categories by revenue
SELECT primary_category,
       ROUND(revenue, 2)           AS revenue_brl,
       order_count,
       ROUND(late_rate * 100, 1)   AS late_pct
FROM gold_category_performance
ORDER BY revenue_brl DESC
LIMIT 10;
```

> **Note:** Re-run `python create_duckdb.py` after each pipeline run to refresh
> the views against the latest Parquet files.

## Gold Layer Outputs

### `customer_rfm`
| Column | Description |
|---|---|
| `customer_unique_id` | Unique customer identifier |
| `recency_days` | Days since last order |
| `frequency` | Number of distinct orders |
| `monetary` | Total spend (BRL) |
| `avg_delivery_days` | Average delivery time |
| `late_rate` | Proportion of late deliveries |
| `r_score` / `f_score` / `m_score` | RFM quintile scores (1–5) |
| `rfm_score` | Combined score (3–15) |
| `segment` | Champions / Loyal Customers / New Customers / At Risk / Lost / Potential Loyalists |
| `state` | Customer state (Brazil) |
| `top_category` | Most purchased product category |

### `monthly_revenue`
| Column | Description |
|---|---|
| `purchase_year` / `purchase_month` | Time dimension |
| `state` | Customer state |
| `revenue` | Total GMV (BRL) |
| `order_count` | Number of delivered orders |
| `avg_order_value` | Average order value |
| `total_items_sold` | Sum of items across orders |
| `avg_delivery_days` | Average delivery time |

### `category_performance`
| Column | Description |
|---|---|
| `primary_category` | Product category (English) |
| `revenue` | Total GMV (BRL) |
| `order_count` | Number of delivered orders |
| `avg_order_value` | Average order value |
| `avg_delivery_days` | Average delivery time |
| `late_rate` | Proportion of late deliveries |

## Dataset

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
— 100,000 orders across 8 CSV files, 2016–2018.

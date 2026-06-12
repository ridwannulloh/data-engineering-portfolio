# 🔍 FraudLens Lakehouse

**Financial fraud detection on a Medallion lakehouse — PySpark + Delta Lake + Airflow + MinIO, fully local, live in under 2 minutes.**

![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.2-00ADD4)
![Airflow](https://img.shields.io/badge/Airflow-2.9-017CEE?logo=apacheairflow&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-S3-C72E49?logo=minio&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Tests](https://img.shields.io/badge/tests-pytest-0A9EDC?logo=pytest&logoColor=white)

An end-to-end **Medallion Architecture** (Bronze → Silver → Gold) pipeline that
detects mobile-money fraud in PaySim-style transaction data. Apache Airflow
orchestrates PySpark jobs that write **Delta Lake** tables to **MinIO** (S3-compatible
object storage). Everything runs on a laptop — no cluster, no cloud account,
no API keys. The DAG triggers itself on startup: `docker compose up`, open the
Airflow UI, and watch the lakehouse build.

---

## Architecture

```
                       ┌────────────────────────────────────────────────────┐
                       │            Apache Airflow  (LocalExecutor)         │
                       │                                                    │
  generate_raw_data ───┼──▶ bronze_ingest ───▶ silver_clean ───▶ gold_metrics
                       └───────┼──────────────────┼──────────────────┼──────┘
        │                      │                  │                  │
        ▼                      ▼                  ▼                  ▼
  data/raw/*.csv      s3a://lakehouse/    s3a://lakehouse/   s3a://lakehouse/
  PaySim-style          bronze/             silver/            gold/
  synthetic txns      raw + ingest        typed, deduped,    fraud KPIs, risk
  (seeded, 50k)       metadata only       fraud features     scores, P/R/F1
        │                      │                  │                  │
        └──────────────┬───────┴──────────────────┴──────────────────┘
                       │        Delta Lake tables on MinIO (S3)
                       │        PySpark local[*] — no cluster needed
                       └────────────────────────────────────────────
```

| Layer | Table(s) | What happens |
|---|---|---|
| **Bronze** | `bronze/transactions` | Raw CSV landed byte-faithful with explicit schema + ingestion metadata. Replayable source of truth. |
| **Silver** | `silver/transactions` | snake_case conformance, dedup on natural key, data-quality filters, behavioural feature engineering (`full_balance_drain`, `dest_balance_stale`, time-of-day). Partitioned by transaction type. |
| **Gold** | `fraud_by_type`, `hourly_fraud_pattern`, `high_risk_accounts`, `detection_performance` | Business-ready fraud analytics: exposure per channel, temporal fraud patterns, a ranked account review queue, and honest precision/recall/F1 of the rule-based detector. |

### The data

Synthetic transactions matching the schema of the public
[PaySim dataset](https://www.kaggle.com/datasets/ealaxi/paysim1) (mobile-money
fraud simulation). The built-in seeded generator keeps the demo
credential-free and reproducible; drop the real 6.3M-row PaySim CSV into
`data/raw/transactions.csv` and the pipeline runs on it unchanged.

### The fraud detector

A transparent, rule-based risk score (no black box — explainable to a
compliance team):

| Signal | Weight |
|---|---|
| Account fully drained in one transaction | 40 |
| Destination balance never increased (mule account) | 30 |
| High-risk channel (`TRANSFER` / `CASH_OUT`) | 15 |
| Amount above the 95th percentile | 15 |

Transactions scoring **≥ 70** are alerted. Labels are used **only** to
evaluate the rules (Gold `detection_performance` table) — never as a model
input.

---

## 🚀 Run it (3 commands)

```bash
git clone <your-repo-url> && cd fraudlens-lakehouse
docker compose up --build -d
# wait ~60s, then open http://localhost:8080  (admin / admin) — the DAG is already running
```

When the `fraud_lakehouse` DAG goes green (≈90 seconds), print the Gold results:

```bash
docker compose exec airflow-scheduler python /opt/airflow/scripts/show_results.py
```

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | `admin` / `admin` |
| MinIO console (browse bronze/silver/gold) | http://localhost:9001 | `minioadmin` / `minioadmin` |

## What the output looks like

Actual output from the default seeded 50k-row run:

```
=== GOLD: rule-based detection performance ===
+-------------+--------------+--------------+---------------+---------------+--------------+---------+------+--------+
|model        |risk_threshold|true_positives|false_positives|false_negatives|true_negatives|precision|recall|f1_score|
+-------------+--------------+--------------+---------------+---------------+--------------+---------+------+--------+
|rule_based_v1|70            |335           |32             |65             |49568         |0.9128   |0.8375|0.8735  |
+-------------+--------------+--------------+---------------+---------------+--------------+---------+------+--------+

=== GOLD: fraud exposure by transaction type ===
+--------+---------+-------------+-----------+-------------+--------------+
|txn_type|txn_count|total_amount |fraud_count|fraud_amount |fraud_rate_pct|
+--------+---------+-------------+-----------+-------------+--------------+
|CASH_OUT|16663    |4.029208858E7|205        |2.442050934E7|1.23          |
|TRANSFER|4592     |2.614934259E7|195        |2.166457227E7|4.247         |
|PAYMENT |17808    |1.725190564E7|0          |0.0          |0.0           |
|CASH_IN |9473     |8721356.01   |0          |0.0          |0.0           |
|DEBIT   |1464     |1368615.8    |0          |0.0          |0.0           |
+--------+---------+-------------+-----------+-------------+--------------+
```

*(The generator is seeded — this exact output reproduces on every machine.)*

The `hourly_fraud_pattern` table shows fraud concentrating between **01:00
and 06:00** — exactly the kind of operational insight a fraud-ops team turns
into alerting policy. `high_risk_accounts` is the ranked daily review queue
with total money at risk per account.

## Run the tests (no Docker needed)

```bash
pip install -r requirements.txt
pytest tests/ -v
```

Tests run PySpark in `local[1]` mode and write Delta tables to a pytest tmp
directory — no MinIO, no Airflow, no containers.

```
tests/test_pipeline.py::TestGenerator::test_deterministic_with_seed PASSED
tests/test_pipeline.py::TestGenerator::test_fraud_profile_matches_paysim PASSED
tests/test_pipeline.py::TestPipelineEndToEnd::test_bronze_silver_gold PASSED
========================= 3 passed in 86.46s =========================
```

The end-to-end test exercises the full Bronze → Silver → Gold flow on 3,000
generated rows and asserts the detector beats 0.5 precision **and** recall.

## Project structure

```
fraudlens-lakehouse/
├── docker-compose.yml          # MinIO + Postgres + Airflow (web, scheduler)
├── Dockerfile                  # Airflow image + JDK 17 + pre-warmed Spark jars
├── requirements.txt
├── dags/
│   └── fraud_lakehouse_dag.py  # generate → bronze → silver → gold
├── src/
│   ├── config.py               # env-driven paths (s3a:// or local)
│   ├── spark_session.py        # Delta-enabled local SparkSession factory
│   ├── generate_data.py        # seeded PaySim-style generator
│   ├── bronze/ingest_transactions.py
│   ├── silver/clean_transactions.py
│   └── gold/fraud_metrics.py   # 4 analytical tables + rule-based detector
├── scripts/
│   ├── show_results.py         # pretty-print all Gold tables
│   └── warm_spark_jars.py      # build-time jar caching (fast first run)
└── tests/
    ├── conftest.py
    └── test_pipeline.py        # generator unit tests + Bronze→Gold e2e
```

## Design decisions

- **Spark in `local[*]` inside Airflow tasks** — no cluster to babysit. The
  Medallion code is identical to what runs on Databricks/EMR; only the
  session factory would change.
- **Delta on MinIO via `s3a://`** — a real object-store lakehouse with ACID
  writes and time travel, browsable in the MinIO console.
- **Explicit schemas everywhere** — no `inferSchema` in pipelines.
- **Jars resolved at image build time** — the first DAG run never downloads
  from Maven, keeping the demo fast and offline-safe.
- **Idempotent runs** — every layer overwrites; re-trigger the DAG freely.
  Delta versioning preserves history regardless.

## Troubleshooting

- **Airflow tasks fail on the very first seconds after startup** — MinIO may
  finish initializing after the scheduler; tasks retry automatically (2× /
  30s) and recover.
- **Linux hosts with permission errors on `./data`** — run
  `mkdir -p data && chmod 777 data` once, or set `AIRFLOW_UID` per the
  [official docs](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html).

---

**License:** MIT

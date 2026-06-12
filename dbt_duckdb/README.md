# portfolio_dbt — Analytics Engineering on DuckDB

A self-contained dbt project demonstrating a full **seed → staging → marts** pipeline
on **DuckDB**, runnable end-to-end with a single `docker compose up`. Built as a
portfolio piece to showcase analytics-engineering best practices: layered models,
sources & freshness, lookup seeds, custom macros, generic + custom tests, and a
containerised, reproducible build.

> Originally modelled on an Amazon Redshift warehouse; ported to DuckDB so the
> entire pipeline runs locally with zero cloud credentials or external data.

---

## Architecture

```
seeds/raw/*.csv  (emulates AWS DMS / S3 COPY landing the Bronze/ODS layer)
        │
        ▼  dbt seed                →  raw schema
    raw.orders / customers / products
        │
        ▼  dbt staging models (VIEWs)
  staging.stg_*                    ← type-cast, renamed, UTC→WIB
        │
        ▼  dbt mart models (TABLEs)   + lookup seeds (country_codes, order_status_labels)
   marts.mart_orders               ← wide denormalised fact (orders × customers × products)
   marts.mart_customer_summary     ← customer RFM aggregation
        │
        ▼
  Metabase / Superset / BI
```

---

## Quick start (Docker)

```bash
cd dbt_duckdb

# Build seeds -> models -> tests in DAG order.
docker compose up --build

# Browse the lineage graph + docs at http://localhost:8080
docker compose run --rm --service-ports docs
```

The built warehouse is persisted to the `dbt_warehouse` Docker volume at
`/app/warehouse/portfolio.duckdb`.

## Quick start (local, no Docker)

```bash
pip install "dbt-duckdb==1.9.*"

export DBT_PROFILES_DIR=$PWD
mkdir -p warehouse
dbt build            # seed + run + test
```

`dbt build` runs everything in dependency order. To run layers individually:

```bash
dbt seed                       # load raw + lookup CSVs
dbt run  --select staging      # build staging views
dbt run  --select marts        # build mart tables
dbt test                       # run all 48 data tests
dbt source freshness           # check ODS freshness thresholds
dbt docs generate && dbt docs serve
```

---

## Project structure

```
dbt_duckdb/
├── models/
│   ├── staging/
│   │   ├── sources/sources.yml     # Source declarations + freshness checks
│   │   ├── stg_orders.sql
│   │   ├── stg_customers.sql
│   │   ├── stg_products.sql
│   │   └── schema.yml
│   └── marts/
│       ├── mart_orders.sql         # Wide denormalised fact table
│       ├── mart_customer_summary.sql   # Customer RFM aggregation
│       └── schema.yml
├── seeds/
│   ├── raw/                        # Emulated Bronze/ODS layer (loaded as `raw` schema)
│   │   ├── orders.csv
│   │   ├── customers.csv
│   │   └── products.csv
│   ├── country_codes.csv           # Lookup → region enrichment
│   └── order_status_labels.csv     # Lookup → status label + revenue flag
├── macros/
│   └── duckdb_helpers.sql          # to_wib(), jakarta_today(), surrogate_key(), schema naming
├── tests/
│   └── generic/test_non_negative.sql
├── scripts/
│   └── gen_seed_data.py            # Regenerates the raw seed CSVs (deterministic)
├── dbt_project.yml
├── profiles.yml                    # DuckDB profile (picked up via DBT_PROFILES_DIR)
├── Dockerfile
└── docker-compose.yml
```

---

## Key design decisions

| Decision | Rationale |
|---|---|
| DuckDB engine | Zero-infra, single-file warehouse — the whole project runs in CI or a laptop |
| Raw data as seeds | Self-contained & reproducible; emulates a DMS/S3 landing zone |
| Staging → VIEW | No storage cost; always reflects latest raw data |
| Marts → TABLE | Materialised for fast BI reads |
| Timestamps → WIB (UTC+7) | All business reporting in Asia/Jakarta timezone |
| Lookup seeds joined in marts | Region enrichment + revenue flagging without hardcoded CASE |
| Custom `generate_schema_name` | Clean literal schemas (`raw`, `staging`, `marts`) instead of prefixed |
| No external dbt packages | Hermetic build — no `dbt deps` / network needed in the container |
| `_dbt_loaded_at` audit column | Lineage / incremental debugging |

---

## Macros

```sql
{{ to_wib('created_at') }}                  -- convert a UTC column to Asia/Jakarta
{{ jakarta_today() }}                        -- current date in Jakarta tz
{{ surrogate_key(['order_id']) }}            -- MD5 surrogate key
```

## Tests

48 data tests run on every build: `unique`, `not_null`, `accepted_values`, plus a
custom **`non_negative`** generic test (`tests/generic/test_non_negative.sql`)
applied to monetary and count columns.

## Source freshness

```bash
dbt source freshness
```

Warns if raw data is older than 6 hours; errors after 24 hours
(`loaded_at_field: _dms_timestamp`).

---

## Author

**Ridwan** — Data Engineer · Analytics Engineer
Stack: Python · dbt · DuckDB · Redshift · Airflow · AWS

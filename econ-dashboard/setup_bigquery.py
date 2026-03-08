"""
setup_bigquery.py
-----------------
One-time setup: creates the BigQuery dataset and table.

Usage:
    python setup_bigquery.py
    python setup_bigquery.py --project my-gcp-project-id
"""

import os
import argparse
import logging

from google.cloud import bigquery
from google.api_core.exceptions import Conflict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ── Schema ────────────────────────────────────────────────────────────────────
TABLE_SCHEMA = [
    bigquery.SchemaField("country_code",   "STRING",    description="ISO-3 country code"),
    bigquery.SchemaField("country_name",   "STRING",    description="Full country name"),
    bigquery.SchemaField("indicator_code", "STRING",    description="World Bank indicator code"),
    bigquery.SchemaField("indicator_name", "STRING",    description="Human-readable indicator name"),
    bigquery.SchemaField("year",           "INTEGER",   description="Data year"),
    bigquery.SchemaField("value",          "FLOAT64",   description="Indicator value"),
    bigquery.SchemaField("loaded_at",      "TIMESTAMP", description="When this row was ingested"),
]

# ── Setup ─────────────────────────────────────────────────────────────────────
def setup(project_id: str, dataset_id: str = "economic_data", table_id: str = "indicators"):
    client = bigquery.Client(project=project_id)

    # 1. Create dataset
    dataset_ref = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset_ref.location = "US"
    dataset_ref.description = "Economic indicators from World Bank Open Data"

    try:
        client.create_dataset(dataset_ref)
        log.info(f"✓ Dataset created: {project_id}.{dataset_id}")
    except Conflict:
        log.info(f"  Dataset already exists: {project_id}.{dataset_id}")

    # 2. Create table
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    table = bigquery.Table(full_table_id, schema=TABLE_SCHEMA)
    table.description = "World Bank economic indicators — GDP, inflation, unemployment, exports, debt"

    # Partition by year for query efficiency (cost optimization)
    table.range_partitioning = bigquery.RangePartitioning(
        field="year",
        range_=bigquery.PartitionRange(start=2000, end=2030, interval=1),
    )

    # Cluster by country for faster filtering
    table.clustering_fields = ["country_code", "indicator_code"]

    try:
        table = client.create_table(table)
        log.info(f"✓ Table created: {full_table_id}")
    except Conflict:
        log.info(f"  Table already exists: {full_table_id}")

    # 3. Verify
    t = client.get_table(full_table_id)
    log.info(f"\n{'=' * 50}")
    log.info(f"  Table ID        : {t.full_table_id}")
    log.info(f"  Schema fields   : {[f.name for f in t.schema]}")
    log.info(f"  Partitioned on  : {t.range_partitioning.field if t.range_partitioning else 'None'}")
    log.info(f"  Clustered on    : {t.clustering_fields}")
    log.info(f"  Num rows        : {t.num_rows}")
    log.info(f"{'=' * 50}")
    log.info("\n✅ BigQuery setup complete. Ready to ingest data.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BigQuery setup for econ-dashboard")
    parser.add_argument(
        "--project", "-p",
        default=os.environ.get("GCP_PROJECT_ID", "YOUR_PROJECT_ID"),
        help="GCP project ID",
    )
    args = parser.parse_args()
    setup(project_id=args.project)

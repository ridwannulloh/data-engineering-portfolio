"""
ingest.py
---------
Fetches economic indicators from the World Bank API
and loads them into BigQuery.

Usage:
    python ingest.py                    # ingest all indicators & countries
    python ingest.py --indicator GDP    # ingest specific indicator key
    python ingest.py --dry-run          # print rows, don't load to BQ
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime, timezone
from typing import Optional

import requests
from google.cloud import bigquery

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "YOUR_PROJECT_ID")
DATASET    = "economic_data"
TABLE      = "indicators"
TABLE_ID   = f"{PROJECT_ID}.{DATASET}.{TABLE}"

INDICATORS = {
    "GDP":        ("NY.GDP.MKTP.CD",    "GDP (Current USD)"),
    "INFLATION":  ("FP.CPI.TOTL.ZG",   "Inflation Rate (%)"),
    "UNEMPLOY":   ("SL.UEM.TOTL.ZS",   "Unemployment Rate (%)"),
    "EXPORTS":    ("NE.EXP.GNFS.ZS",   "Exports (% of GDP)"),
    "DEBT":       ("GC.DOD.TOTL.GD.ZS","Government Debt (% of GDP)"),
}

# ISO-2 country codes for G20 countries (World Bank format)
COUNTRIES = [
    "AR",  # Argentina
    "AU",  # Australia
    "BR",  # Brazil
    "CA",  # Canada
    "CN",  # China
    "FR",  # France
    "DE",  # Germany
    "IN",  # India
    "ID",  # Indonesia
    "IT",  # Italy
    "JP",  # Japan
    "MX",  # Mexico
    "RU",  # Russia
    "SA",  # Saudi Arabia
    "ZA",  # South Africa
    "KR",  # South Korea
    "TR",  # Turkey
    "GB",  # United Kingdom
    "US",  # United States
]

WB_BASE_URL = "https://api.worldbank.org/v2"
YEARS_BACK  = 15   # how many historical years to fetch
RETRY_LIMIT = 3
RETRY_DELAY = 2    # seconds between retries

# ── World Bank API ─────────────────────────────────────────────────────────────
def fetch_indicator(
    indicator_code: str,
    countries: list[str],
    years_back: int = YEARS_BACK,
) -> list[dict]:
    """
    Fetch a single indicator for a list of countries from the World Bank API.
    Returns a list of raw API records.
    """
    country_str = ";".join(countries)
    url = (
        f"{WB_BASE_URL}/country/{country_str}"
        f"/indicator/{indicator_code}"
        f"?format=json&per_page=1000&mrv={years_back}"
    )

    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            log.info(f"  Fetching {indicator_code} (attempt {attempt})...")
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            payload = resp.json()

            # World Bank returns [metadata, data] — data is index 1
            if not payload or len(payload) < 2 or payload[1] is None:
                log.warning(f"  No data returned for {indicator_code}")
                return []

            return payload[1]

        except requests.exceptions.RequestException as e:
            log.warning(f"  Request failed: {e}")
            if attempt < RETRY_LIMIT:
                time.sleep(RETRY_DELAY)

    log.error(f"  Giving up on {indicator_code} after {RETRY_LIMIT} attempts")
    return []


# ── Transform ─────────────────────────────────────────────────────────────────
def transform_records(
    raw_records: list[dict],
    indicator_code: str,
    indicator_name: str,
    loaded_at: str,
) -> list[dict]:
    """
    Convert raw World Bank API records into BigQuery-ready rows.
    Skips records with null values.
    """
    rows = []
    for r in raw_records:
        if r.get("value") is None:
            continue
        try:
            rows.append({
                "country_code":    r.get("countryiso3code", ""),
                "country_name":    r["country"]["value"],
                "indicator_code":  indicator_code,
                "indicator_name":  indicator_name,
                "year":            int(r["date"]),
                "value":           float(r["value"]),
                "loaded_at":       loaded_at,
            })
        except (KeyError, ValueError, TypeError) as e:
            log.warning(f"  Skipping malformed record: {r} — {e}")

    return rows


# ── Load to BigQuery ───────────────────────────────────────────────────────────
def truncate_table(client: bigquery.Client) -> None:
    """
    Delete all existing data from the BigQuery table before fresh load.
    """
    log.info("  Truncating existing data from BigQuery...")
    query = f"DELETE FROM `{TABLE_ID}` WHERE TRUE"
    job = client.query(query)
    job.result()  # wait for completion
    log.info("  ✓ Table truncated")


def load_to_bigquery(rows: list[dict], client: bigquery.Client) -> int:
    """
    Insert rows into BigQuery using streaming insert.
    Returns the number of rows successfully inserted.
    """
    if not rows:
        log.info("  No rows to insert.")
        return 0

    errors = client.insert_rows_json(TABLE_ID, rows)

    if errors:
        for err in errors:
            log.error(f"  BigQuery insert error: {err}")
        raise RuntimeError(f"BigQuery insert failed with {len(errors)} errors")

    return len(rows)


# ── Main pipeline ─────────────────────────────────────────────────────────────
def run_ingestion(
    indicator_keys: Optional[list[str]] = None,
    countries: list[str] = COUNTRIES,
    dry_run: bool = False,
) -> dict:
    """
    Run the full ingestion pipeline.

    Args:
        indicator_keys: List of keys from INDICATORS dict (e.g. ["GDP", "INFLATION"]).
                        If None, ingests all indicators.
        countries:      List of ISO-2 country codes.
        dry_run:        If True, prints rows without loading to BigQuery.

    Returns:
        Summary dict with counts per indicator.
    """
    targets = indicator_keys or list(INDICATORS.keys())
    loaded_at = datetime.now(timezone.utc).isoformat()
    client = bigquery.Client(project=PROJECT_ID) if not dry_run else None

    summary = {}
    total_rows = 0

    log.info(f"{'=' * 55}")
    log.info(f"  Ingestion run: {loaded_at}")
    log.info(f"  Indicators : {targets}")
    log.info(f"  Countries  : {countries}")
    log.info(f"  Dry run    : {dry_run}")
    log.info(f"{'=' * 55}")

    # Truncate existing data before loading fresh data
    if not dry_run:
        log.info("\n[Pre-flight] Clearing old data...")
        truncate_table(client)

    for key in targets:
        if key not in INDICATORS:
            log.warning(f"Unknown indicator key: {key} — skipping")
            continue

        code, name = INDICATORS[key]
        log.info(f"\n[{key}] {name}")

        raw     = fetch_indicator(code, countries)
        rows    = transform_records(raw, code, name, loaded_at)

        log.info(f"  Transformed: {len(rows)} valid rows")

        if dry_run:
            for row in rows[:3]:
                log.info(f"    SAMPLE: {row}")
            log.info(f"    ... (dry run, not loading to BigQuery)")
            summary[key] = len(rows)
        else:
            inserted = load_to_bigquery(rows, client)
            log.info(f"  Loaded {inserted} rows → BigQuery")
            summary[key] = inserted
            total_rows += inserted

    log.info(f"\n{'=' * 55}")
    log.info(f"  DONE. Total rows loaded: {total_rows}")
    log.info(f"{'=' * 55}\n")

    return summary


# ── CLI entrypoint ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="World Bank → BigQuery ingestion pipeline")
    parser.add_argument(
        "--indicator", "-i",
        nargs="+",
        choices=list(INDICATORS.keys()),
        help="Which indicator(s) to ingest. Default: all.",
    )
    parser.add_argument(
        "--dry-run", "-d",
        action="store_true",
        help="Print rows without loading to BigQuery.",
    )
    args = parser.parse_args()

    try:
        run_ingestion(
            indicator_keys=args.indicator,
            dry_run=args.dry_run,
        )
    except Exception as e:
        log.error(f"Pipeline failed: {e}")
        sys.exit(1)

"""Opt-in live test: migrates project tables and replaces the January 2024 taxi snapshot twice.

Run against a development service with CH_ETL_INTEGRATION=1. The server must be able to
download the official TLC file. This is deliberately not replaced with a mocked success.
"""

import os
from datetime import date

import pytest

from ch_etl import migrate, taxi
from ch_etl.client import connect
from ch_etl.config import MIGRATIONS_DIR, Settings

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.environ.get("CH_ETL_INTEGRATION") != "1", reason="live server opt-in"),
]


def test_real_tlc_snapshot_replay_and_materialized_results():
    client = connect(Settings.from_env())
    month = date(2024, 1, 1)
    try:
        migrate.run(client, MIGRATIONS_DIR)
        first = taxi.load_month(client, month)
        assert first.rows > 0 and first.valid_rows > 0
        before = client.query(taxi.sql("detail_daily"), parameters={"month": month}).result_rows
        second = taxi.load_month(client, month)
        assert second.rows == first.rows
        assert second.valid_rows == first.valid_rows
        actual = client.query(
            "SELECT count() FROM raw.taxi_trips WHERE source_month = {month:Date}",
            parameters={"month": month},
            settings={"select_sequential_consistency": 1},
        ).first_row[0]
        assert actual == first.rows  # A replay must not double the data.
        after = client.query(taxi.sql("detail_daily"), parameters={"month": month}).result_rows
        assert before == after
        report = taxi.demo(client, month, repeats=2)
        assert report["benchmark"]["results_match"]
        assert report["storage"]["rows"]
        assert report["pruning"]["rows"]
    finally:
        client.close()

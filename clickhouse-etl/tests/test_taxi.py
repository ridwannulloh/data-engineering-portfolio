from datetime import date
from decimal import Decimal
from types import SimpleNamespace

import pytest
from clickhouse_connect.driver.exceptions import DatabaseError

from ch_etl import taxi

MONTH = date(2024, 1, 1)


class FakeClient:
    def __init__(self, *, rows=(10, 8, 0), fail_at=None):
        self.rows = rows
        self.fail_at = fail_at
        self.calls = []

    def command(self, query, **kwargs):
        self.calls.append((query, kwargs))
        if self.fail_at and self.fail_at in query:
            raise RuntimeError("simulated database failure")

    def query(self, query, **kwargs):
        self.calls.append((query, kwargs))
        return SimpleNamespace(first_row=self.rows)


@pytest.mark.parametrize("value", ["2024-1", "2024-13", "2024-00", "2024-01-01", "2016-01", "x'"])
def test_reject_invalid_months(value):
    with pytest.raises(ValueError):
        taxi.parse_month(value)


def test_official_source_url():
    assert taxi.parse_month("2024-01") == MONTH
    assert taxi.source_url(MONTH) == (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
    )


def test_load_validates_before_publishing_and_refreshes_afterwards():
    client = FakeClient()
    result = taxi.load_month(client, MONTH)
    queries = [q for q, _ in client.calls]
    assert result.rows == 10
    assert result.valid_rows == 8
    assert result.excluded_rows == 2
    validation = next(i for i, q in enumerate(queries) if q.startswith("SELECT count()"))
    publish = next(i for i, q in enumerate(queries) if "REPLACE PARTITION" in q)
    cleanup = next(i for i, q in enumerate(queries) if q.startswith("DROP TABLE"))
    assert validation < publish < cleanup
    assert queries[-3:] == [
        "SYSTEM WAIT VIEW mart.taxi_daily_mv",
        "SYSTEM REFRESH VIEW mart.taxi_daily_mv",
        "SYSTEM WAIT VIEW mart.taxi_daily_mv",
    ]
    assert client.calls[publish][1]["parameters"]["partition"] == 202401


def test_partition_source_is_a_literal_identifier_for_clickhouse_parser():
    client = FakeClient()
    taxi.load_month(client, MONTH)
    target = client.calls[0][1]["parameters"]["target"]
    query, kwargs = next(call for call in client.calls if "REPLACE PARTITION" in call[0])
    # ClickHouse 26.6 rejects Identifier parameters specifically after REPLACE ... FROM.
    # The source must name the same internally generated scratch table that was loaded.
    assert query == (
        f"ALTER TABLE raw.taxi_trips REPLACE PARTITION {{partition:UInt32}} FROM raw.`{target}`"
    )
    assert kwargs["parameters"] == {"partition": 202401}


@pytest.mark.parametrize("value", ["2024-02", "2024-12", "2025-01"])
def test_load_uses_selected_month_for_source_and_partition(value):
    client = FakeClient()
    result = taxi.load_month(client, taxi.parse_month(value))
    assert result.month == value
    assert result.source_url.endswith(f"yellow_tripdata_{value}.parquet")
    query, kwargs = next(call for call in client.calls if "REPLACE PARTITION" in call[0])
    assert kwargs["parameters"]["partition"] == int(value.replace("-", ""))


@pytest.mark.parametrize("rows", [(0, 0, 0), (10, 0, 0), (10, 8, 1)])
def test_bad_snapshot_never_replaces_existing_month(rows):
    client = FakeClient(rows=rows)
    with pytest.raises(ValueError, match="refusing to publish"):
        taxi.load_month(client, MONTH)
    assert not any("REPLACE PARTITION" in q for q, _ in client.calls)
    assert client.calls[-1][0].startswith("DROP TABLE")


def test_failed_import_does_not_publish_partial_data():
    client = FakeClient(fail_at="INSERT INTO")
    with pytest.raises(RuntimeError, match="simulated"):
        taxi.load_month(client, MONTH)
    assert not any("REPLACE PARTITION" in q for q, _ in client.calls)
    assert client.calls[-1][0].startswith("DROP TABLE")


def test_cleanup_failure_preserves_original_error(caplog):
    client = FakeClient(rows=(0, 0, 0), fail_at="DROP TABLE")
    with pytest.raises(ValueError, match="refusing to publish"):
        taxi.load_month(client, MONTH)
    assert "scratch table cleanup failed" in caplog.text


def test_publish_failure_cleans_up_and_does_not_refresh():
    client = FakeClient(fail_at="REPLACE PARTITION")
    with pytest.raises(RuntimeError, match="simulated"):
        taxi.load_month(client, MONTH)
    assert client.calls[-1][0].startswith("DROP TABLE")
    assert not any("REFRESH VIEW" in q for q, _ in client.calls)


def test_previous_failed_refresh_can_be_retried():
    calls = []

    def command(query):
        calls.append(query)
        if len(calls) == 1:
            raise DatabaseError("previous refresh failed")

    taxi.refresh_mart(SimpleNamespace(command=command))
    assert len(calls) == 3
    assert calls[1] == "SYSTEM REFRESH VIEW mart.taxi_daily_mv"


def test_current_refresh_failure_is_not_swallowed():
    calls = []

    def command(query):
        calls.append(query)
        if len(calls) == 3:
            raise DatabaseError("current refresh failed")

    with pytest.raises(DatabaseError, match="current"):
        taxi.refresh_mart(SimpleNamespace(command=command))


def test_non_month_start_is_rejected_before_writing():
    client = FakeClient()
    with pytest.raises(ValueError, match="first day"):
        taxi.load_month(client, date(2024, 1, 2))
    assert not client.calls


class BenchmarkClient:
    def __init__(self, mismatch_at=None):
        self.calls = []
        self.mismatch_at = mismatch_at

    def query(self, query, **kwargs):
        self.calls.append((query, kwargs))
        # Keep total counts the same when mutating a grouping key: validation must check
        # every group, not just the grand total.
        zone = 2 if len(self.calls) == self.mismatch_at else 1
        return SimpleNamespace(
            result_rows=[(MONTH, zone, 1, 10, Decimal("100.00"), Decimal("20.00"))],
            summary={"read_rows": "1000", "read_bytes": "40000", "elapsed_ns": "1500000"},
        )


def test_benchmark_checks_groups_and_collects_actual_metrics():
    client = BenchmarkClient()
    report = taxi.benchmark(client, MONTH, repeats=2)
    assert report["results_match"] is True
    assert report["groups_checked"] == 1
    assert len(client.calls) == 6  # Two warm-ups, two measured pairs.
    assert all(c[1]["settings"]["use_query_cache"] == 0 for c in client.calls)
    assert report["samples"]["detail"][0]["read_rows"] == 1000
    assert report["samples"]["materialized"][1]["read_bytes"] == 40000
    assert "mart.taxi_daily" in client.calls[4][0]  # Alternate query order.


@pytest.mark.parametrize("mismatch_at", [2, 3, 4])
def test_benchmark_rejects_changed_group_even_with_same_total(mismatch_at):
    with pytest.raises(ValueError, match="differ"):
        taxi.benchmark(BenchmarkClient(mismatch_at), MONTH)


def test_benchmark_rejects_empty_dataset():
    client = SimpleNamespace(query=lambda *a, **kw: SimpleNamespace(result_rows=[]))
    with pytest.raises(ValueError, match="load-taxi first"):
        taxi.benchmark(client, MONTH)


@pytest.mark.parametrize("repeats", [0, 21])
def test_invalid_repeats_do_not_query_server(repeats):
    client = BenchmarkClient()
    with pytest.raises(ValueError, match="repeats"):
        taxi.benchmark(client, MONTH, repeats=repeats)
    assert not client.calls

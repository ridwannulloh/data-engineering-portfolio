"""Official NYC TLC snapshots and a measured, correctness-checked ClickHouse demo."""

from __future__ import annotations

import logging
import re
import statistics
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from typing import Any
from urllib.request import urlopen

from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import DatabaseError

from ch_etl.config import TAXI_SQL_DIR

log = logging.getLogger(__name__)
SOURCE_PAGE = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
_READ_SETTINGS = {"use_query_cache": 0, "select_sequential_consistency": 1}


def parse_month(value: str) -> date:
    if not re.fullmatch(r"\d{4}-\d{2}", value):
        raise ValueError("month must be YYYY-MM, for example 2024-01")
    try:
        month = date.fromisoformat(f"{value}-01")
    except ValueError as exc:
        raise ValueError("month must be a valid YYYY-MM") from exc
    if month.year < 2017:
        raise ValueError("use 2017 or later: this loader requires TLC location-ID columns")
    return month


def source_url(month: date) -> str:
    return f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month:%Y-%m}.parquet"


def sql(name: str) -> str:
    return (TAXI_SQL_DIR / f"{name}.sql").read_text()


@dataclass(frozen=True)
class LoadResult:
    month: str
    source_url: str
    rows: int
    valid_rows: int
    excluded_rows: int
    elapsed_seconds: float


def refresh_mart(client: Client) -> None:
    # Wait for an already-running scheduled refresh before requesting our own snapshot.
    try:
        client.command("SYSTEM WAIT VIEW mart.taxi_daily_mv")
    except DatabaseError:
        # WAIT also raises when the previous attempt failed. Permit recovery with a new run;
        # the REFRESH and final WAIT still propagate any current error to the caller.
        log.warning("could not wait for the previous refresh; requesting a fresh run")
    client.command("SYSTEM REFRESH VIEW mart.taxi_daily_mv")
    client.command("SYSTEM WAIT VIEW mart.taxi_daily_mv")


def load_month(client: Client, month: date, *, refresh: bool = True) -> LoadResult:
    """Publish one complete file atomically, then refresh the aggregate snapshot.

    A failed import cannot expose a partial month. Rerunning replaces that month instead of
    appending. Run one publisher at a time; this is not a distributed ingestion coordinator.
    """
    if month.day != 1:
        raise ValueError("month must be the first day of a month")
    started = time.perf_counter()
    target = f"_taxi_import_{uuid.uuid4().hex}"
    url = source_url(month)
    params = {
        "target": target,
        "month": month,
        "partition": month.year * 100 + month.month,
        "url": url,
    }
    log.info("loading %s on the ClickHouse server", url)
    # Copy columns, but use an explicit engine for a fresh Cloud/replication metadata path.
    # Partition/sorting keys must match the destination for REPLACE PARTITION.
    client.command(
        "CREATE TABLE raw.{target:Identifier} AS raw.taxi_trips "
        "ENGINE = MergeTree PARTITION BY toYYYYMM(source_month) "
        "ORDER BY (pickup_date, pickup_location_id)",
        parameters=params,
    )
    try:
        client.command(
            sql("import"),
            parameters=params,
            settings={"async_insert": 0, "wait_end_of_query": 1},
        )
        rows, valid, wrong_month = client.query(
            "SELECT count(), countIf(quality_issue = 'valid'), "
            "countIf(source_month != {month:Date}) FROM raw.{target:Identifier}",
            parameters=params,
            settings=_READ_SETTINGS,
        ).first_row
        if rows == 0 or valid == 0 or wrong_month:
            raise ValueError(
                f"refusing to publish invalid snapshot: rows={rows}, "
                f"valid={valid}, wrong_month={wrong_month}"
            )
        # REPLACE PARTITION ... FROM does not accept an Identifier parameter on 26.6.
        # target is generated above from a UUID hex string, never supplied by the user.
        client.command(
            "ALTER TABLE raw.taxi_trips REPLACE PARTITION {partition:UInt32} "
            f"FROM raw.`{target}`",
            parameters={"partition": params["partition"]},
        )
    finally:
        try:
            client.command("DROP TABLE IF EXISTS raw.{target:Identifier}", parameters=params)
        except Exception:
            # A cleanup error must not hide the original load/publish error.
            log.exception("scratch table cleanup failed: raw.%s", target)
    if refresh:
        refresh_mart(client)
    return LoadResult(
        month.isoformat()[:7],
        url,
        int(rows),
        int(valid),
        int(rows - valid),
        round(time.perf_counter() - started, 3),
    )


def _table(result: Any) -> dict[str, Any]:
    return {"columns": list(result.column_names), "rows": list(result.result_rows)}


def _metric(summary: dict[str, Any], name: str) -> int | None:
    value = summary.get(name)
    return int(value) if value is not None else None


def benchmark(client: Client, month: date, *, repeats: int = 3) -> dict[str, Any]:
    """Alternate measured queries, disable result caching, compare EVERY returned group.

    Wall time includes network transfer. This is a warm benchmark, not a cold-cache test.
    summary counters come from the HTTP response, so query_log/cluster access is unnecessary.
    """
    if not 1 <= repeats <= 20:
        raise ValueError("repeats must be between 1 and 20")
    params = {"month": month}
    queries = {"detail": sql("detail_daily"), "materialized": sql("mart_daily")}
    expected = client.query(
        queries["detail"], parameters=params, settings=_READ_SETTINGS
    ).result_rows
    if not expected:
        raise ValueError("no valid taxi data for this month; run load-taxi first")
    # Explicit warm-up for both paths before timing.
    warm = client.query(queries["materialized"], parameters=params, settings=_READ_SETTINGS)
    if warm.result_rows != expected:
        raise ValueError(
            "detail and materialized results differ; refresh and stop concurrent loads"
        )
    samples: dict[str, list[dict[str, Any]]] = {"detail": [], "materialized": []}
    for repetition in range(repeats):
        order = ("detail", "materialized") if repetition % 2 == 0 else ("materialized", "detail")
        for name in order:
            started = time.perf_counter()
            result = client.query(queries[name], parameters=params, settings=_READ_SETTINGS)
            elapsed_ms = (time.perf_counter() - started) * 1000
            if result.result_rows != expected:
                raise ValueError(
                    f"{name} results changed or differ; stop concurrent loads and retry"
                )
            summary = result.summary or {}
            samples[name].append(
                {
                    "wall_ms": round(elapsed_ms, 3),
                    "read_rows": _metric(summary, "read_rows"),
                    "read_bytes": _metric(summary, "read_bytes"),
                    "server_elapsed_ns": _metric(summary, "elapsed_ns"),
                }
            )
    medians = {
        name: statistics.median(sample["wall_ms"] for sample in values)
        for name, values in samples.items()
    }
    return {
        "results_match": True,
        "groups_checked": len(expected),
        "repeats": repeats,
        "median_wall_ms": medians,
        "detail_over_materialized_ratio": (
            round(medians["detail"] / medians["materialized"], 3)
            if medians["materialized"]
            else None
        ),
        "samples": samples,
        "method": "warm runs; query result cache disabled; alternating order; includes network",
    }


def demo(client: Client, month: date, *, repeats: int = 3) -> dict[str, Any]:
    if not 1 <= repeats <= 20:
        raise ValueError("repeats must be between 1 and 20")
    refresh_mart(client)
    params = {"month": month, "day": month}
    report: dict[str, Any] = {
        "generated_at": datetime.now(UTC).isoformat(),
        "month": month.isoformat()[:7],
        "source": SOURCE_PAGE,
        "source_file": source_url(month),
        "server_version": client.query("SELECT version()").first_row[0],
        "benchmark": benchmark(client, month, repeats=repeats),
    }
    report["quality"] = _table(
        client.query(
            "SELECT quality_issue, count() AS rows FROM raw.taxi_trips "
            "WHERE source_month = {month:Date} GROUP BY quality_issue ORDER BY rows DESC",
            parameters=params,
            settings=_READ_SETTINGS,
        )
    )
    # One replica's current parts, never sum identical SharedMergeTree data across replicas.
    report["storage"] = _table(
        client.query(
            "SELECT database, table, sum(rows) AS physical_rows, "
            "sum(data_uncompressed_bytes) AS uncompressed_column_bytes, "
            "sum(data_compressed_bytes) AS compressed_column_bytes, "
            "round(sum(data_uncompressed_bytes) / nullIf(sum(data_compressed_bytes), 0), 2) "
            "AS compression_ratio FROM system.parts "
            "WHERE active AND ((database = 'raw' AND table = 'taxi_trips') "
            "OR (database = 'mart' AND table = 'taxi_daily')) "
            "AND partition = {partition:String} GROUP BY database, table ORDER BY database, table",
            parameters={"partition": f"{month:%Y%m}"},
        )
    )
    for name in ("busy_hours", "top_locations", "pruning"):
        report[name] = _table(client.query(sql(name), parameters=params, settings=_READ_SETTINGS))
    return report


def load_result_dict(result: LoadResult) -> dict[str, Any]:
    return asdict(result)


def month_range(start: date, end: date) -> list[date]:
    if start.day != 1 or end.day != 1 or start > end:
        raise ValueError("use month starts with start <= end")
    months = []
    current = start
    while current <= end:
        months.append(current)
        current = date(current.year + (current.month == 12), current.month % 12 + 1, 1)
    return months


def published_months() -> set[date]:
    """Read the official catalog; network errors must not masquerade as missing files."""
    with urlopen(SOURCE_PAGE, timeout=30) as response:
        page = response.read().decode("utf-8")
    matches = re.findall(
        r"https://d37ci6vzurychx\.cloudfront\.net/trip-data/yellow_tripdata_(\d{4}-\d{2})\.parquet",
        page,
    )
    if not matches:
        raise ValueError("no Yellow Taxi files found in TLC catalog; check source page changes")
    return {parse_month(value) for value in matches if value >= "2017-01"}


def load_range(
    client: Client,
    start: date,
    end: date,
    *,
    reload: bool = False,
    strict: bool = False,
) -> dict[str, Any]:
    requested = month_range(start, end)
    available = published_months()
    existing = {
        month
        for (month,) in client.query(
            "SELECT DISTINCT source_month FROM raw.taxi_trips "
            "WHERE source_month BETWEEN {start:Date} AND {end:Date}",
            parameters={"start": start, "end": end},
            settings=_READ_SETTINGS,
        ).result_rows
    }
    unavailable = [m for m in requested if m not in available and (reload or m not in existing)]
    if strict and unavailable:
        raise ValueError(f"TLC has not listed these months: {unavailable}")
    loaded: list[dict[str, Any]] = []
    skipped = []
    try:
        for index, month in enumerate(requested, 1):
            label = month.strftime("%Y-%m")
            if not reload and month in existing:
                skipped.append(label)
                log.info("[%d/%d] %s already loaded; skipping", index, len(requested), label)
            elif month not in available:
                log.warning("[%d/%d] %s not listed by TLC; skipping", index, len(requested), label)
            else:
                log.info("[%d/%d] importing %s", index, len(requested), label)
                loaded.append(asdict(load_month(client, month, refresh=False)))
    except BaseException:
        # Publish completed months even if a later file fails. Preserve the original exception.
        if loaded:
            try:
                refresh_mart(client)
            except Exception:
                log.exception("mart refresh also failed; rerun this command to resume")
        raise
    if loaded or skipped:
        refresh_mart(client)
    return {
        "start": start.strftime("%Y-%m"),
        "end": end.strftime("%Y-%m"),
        "loaded": loaded,
        "skipped_existing": skipped,
        "unavailable": [m.strftime("%Y-%m") for m in unavailable],
        "loaded_rows": sum(item["rows"] for item in loaded),
    }

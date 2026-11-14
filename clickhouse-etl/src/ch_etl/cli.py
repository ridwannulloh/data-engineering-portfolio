from __future__ import annotations

import argparse
import json
import logging
import uuid
from collections.abc import Sequence
from datetime import date
from pathlib import Path

from clickhouse_connect.driver.client import Client

from ch_etl import dashboard, migrate, taxi, transform
from ch_etl.client import connect
from ch_etl.config import DASHBOARD_SQL_DIR, MIGRATIONS_DIR, OPS_SQL_DIR, Settings
from ch_etl.load import insert_batches
from ch_etl.runlog import track_run
from ch_etl.sample import RAW_COLUMNS, generate_events

log = logging.getLogger("ch_etl")

_STATS = """
SELECT 'raw.events rows', count() FROM raw.events
UNION ALL SELECT 'staging.events rows (pre-merge)', count() FROM staging.events
UNION ALL SELECT 'staging.events rows (FINAL)', count() FROM staging.events FINAL
UNION ALL SELECT 'distinct event_types in staging', uniqExact(event_type) FROM staging.events
UNION ALL SELECT 'mart events total (FINAL)', sum(events) FROM mart.daily_event_stats FINAL
UNION ALL SELECT 'mart days', uniqExact(event_date) FROM mart.daily_event_stats
"""


def _print_table(headers: Sequence[str], rows: Sequence[Sequence[object]]) -> None:
    cells = [[str(h) for h in headers]] + [[str(v) for v in row] for row in rows]
    widths = [max(len(r[i]) for r in cells) for i in range(len(headers))]
    for n, row in enumerate(cells):
        print("  ".join(v.ljust(w) for v, w in zip(row, widths, strict=True)))
        if n == 0:
            print("  ".join("-" * w for w in widths))
    if not rows:
        print("(no rows)")


def cmd_ping(client: Client, _: argparse.Namespace, __: Settings) -> None:
    version, uptime = client.query("SELECT version(), uptime()").first_row
    print(f"connected: ClickHouse {version}, uptime {uptime}s")


def cmd_migrate(client: Client, args: argparse.Namespace, _: Settings) -> None:
    pending = migrate.run(client, MIGRATIONS_DIR, dry_run=args.dry_run)
    print(f"{len(pending)} migration(s) {'pending' if args.dry_run else 'applied'}")


def cmd_load_sample(client: Client, args: argparse.Namespace, settings: Settings) -> None:
    batch_id = args.batch_id or f"sample-{uuid.uuid4()}"
    with track_run(client, "load-sample->raw.events") as run:
        run.rows = insert_batches(
            client,
            "raw.events",
            RAW_COLUMNS,
            generate_events(args.rows, batch_id=batch_id, seed=args.seed),
            batch_id=batch_id,
            batch_size=args.batch_size or settings.batch_size,
            use_async=args.async_insert,
        )
    print(f"loaded {run.rows} rows into raw.events (batch_id={batch_id})")


def cmd_transform(client: Client, _: argparse.Namespace, __: Settings) -> None:
    with track_run(client, transform.PIPELINE) as run:
        result = transform.run_daily_stats(client)
        run.rows = len(result.days)
    print(f"rebuilt {len(result.days)} day(s) of mart.daily_event_stats")


def cmd_stats(client: Client, _: argparse.Namespace, __: Settings) -> None:
    _print_table(["metric", "value"], client.query(_STATS).result_rows)


def cmd_ops(client: Client, _: argparse.Namespace, __: Settings) -> None:
    for path in sorted(OPS_SQL_DIR.glob("*.sql")):
        print(f"\n== {path.stem} ==")
        result = client.query(path.read_text())
        _print_table(result.column_names, result.result_rows)


def cmd_load_taxi(client: Client, args: argparse.Namespace, _: Settings) -> None:
    with track_run(client, "nyc-tlc->raw.taxi_trips") as run:
        result = taxi.load_month(client, args.month)
        run.rows = result.rows
    print(json.dumps(taxi.load_result_dict(result), indent=2))
    print("Monthly snapshot published and materialized view refreshed.")


def cmd_taxi_demo(client: Client, args: argparse.Namespace, _: Settings) -> None:
    report = taxi.demo(client, args.month, repeats=args.repeats)
    bench = report["benchmark"]
    print(f"Verified: all {bench['groups_checked']} aggregate groups match the detail query.")
    _print_table(["query", "median wall ms"], list(bench["median_wall_ms"].items()))
    print(f"Detail/materialized time ratio: {bench['detail_over_materialized_ratio']}x")
    print(bench["method"])
    for name in ("quality", "storage", "busy_hours", "top_locations", "pruning"):
        print(f"\n{name}")
        _print_table(report[name]["columns"], report[name]["rows"])
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2, default=str) + "\n")
        print(f"\nReport saved to {args.output}")


def cmd_load_taxi_range(client: Client, args: argparse.Namespace, _: Settings) -> None:
    end = date.today().replace(day=1) if args.end == "current" else taxi.parse_month(args.end)
    with track_run(client, "nyc-tlc-range->raw.taxi_trips") as run:
        report = taxi.load_range(client, args.start, end, reload=args.reload, strict=args.strict)
        run.rows = report["loaded_rows"]
    print(json.dumps(report, indent=2))
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n")


def cmd_dashboard(client: Client, args: argparse.Namespace, _: Settings) -> None:
    dashboard.serve(client, DASHBOARD_SQL_DIR, host=args.host, port=args.port)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="ch-etl", description="ClickHouse ETL toolkit")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("ping", help="check connectivity").set_defaults(fn=cmd_ping)

    p = sub.add_parser("migrate", help="apply pending migrations")
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(fn=cmd_migrate)

    p = sub.add_parser("load-sample", help="load synthetic events into raw.events")
    p.add_argument("--rows", type=int, default=100_000)
    p.add_argument("--batch-size", type=int, help="rows per INSERT (default ETL_BATCH_SIZE)")
    p.add_argument("--batch-id", help="stable source id; reuse it to test idempotent replays")
    p.add_argument("--seed", type=int, help="seed for reproducible data")
    p.add_argument("--async-insert", action="store_true", help="let the server batch inserts")
    p.set_defaults(fn=cmd_load_sample)

    sub.add_parser("transform", help="incremental staging -> mart").set_defaults(fn=cmd_transform)
    sub.add_parser("stats", help="row counts across layers").set_defaults(fn=cmd_stats)
    sub.add_parser("ops", help="parts, compression, insert profile").set_defaults(fn=cmd_ops)

    p = sub.add_parser("load-taxi", help="import an official NYC TLC monthly Parquet snapshot")
    p.add_argument("--month", type=taxi.parse_month, default="2024-01", help="YYYY-MM")
    p.set_defaults(fn=cmd_load_taxi)

    p = sub.add_parser("load-taxi-range", help="load monthly TLC files; resume existing snapshots")
    p.add_argument("--start", type=taxi.parse_month, default="2024-01", help="inclusive YYYY-MM")
    p.add_argument("--end", default="current", help="inclusive YYYY-MM or current (default)")
    p.add_argument("--reload", action="store_true", help="replace already loaded months too")
    p.add_argument("--strict", action="store_true", help="fail if requested months are unlisted")
    p.add_argument("--output", type=Path, default=Path("reports/taxi-range.json"))
    p.set_defaults(fn=cmd_load_taxi_range)

    p = sub.add_parser("taxi-demo", help="verify aggregates and measure ClickHouse features")
    p.add_argument("--month", type=taxi.parse_month, default="2024-01", help="YYYY-MM")
    p.add_argument("--repeats", type=int, choices=range(1, 21), default=3, metavar="1..20")
    p.add_argument("--output", type=Path, help="save measured results as JSON")
    p.set_defaults(fn=cmd_taxi_demo)

    p = sub.add_parser("dashboard", help="serve the taxi dashboard web page")
    p.add_argument("--host", default="127.0.0.1", help="bind address (default: this machine only)")
    p.add_argument("--port", type=int, default=8050)
    p.set_defaults(fn=cmd_dashboard)

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    settings = Settings.from_env()
    client = connect(settings)
    try:
        args.fn(client, args, settings)
    finally:
        client.close()


if __name__ == "__main__":
    main()

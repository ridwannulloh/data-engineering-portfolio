# clickhouse-etl

ETL pipelines on ClickHouse Cloud, with a web dashboard. The main example loads the **real NYC
Yellow Taxi dataset**: ClickHouse reads the official monthly Parquet files directly, publishes
each month as an atomic snapshot, and keeps a daily aggregate in a refreshable materialized view.
A smaller synthetic event pipeline shows the insert, dedup, and incremental-transform patterns.

## Quickstart

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/).

```bash
cp .env.example .env      # Cloud console → service → Connect → HTTPS: host and password
uv sync
uv run ch-etl ping
uv run ch-etl migrate
uv run ch-etl load-taxi-range --start 2024-01 --end current
uv run ch-etl dashboard   # http://127.0.0.1:8050
```

`load-taxi-range` loads every month the official TLC catalog lists, skips months already loaded,
and refreshes the aggregate once at the end. Rerun it to resume. To load a single month instead,
use `uv run ch-etl load-taxi --month 2024-01`. The ClickHouse server downloads the files, so it
needs HTTPS access to `d37ci6vzurychx.cloudfront.net`. Your IP must be allowed in the service's
access settings.

For a local server instead of Cloud: `make up`, then switch `.env` to the local block.

## Dashboard

```bash
make dashboard            # or: uv run ch-etl dashboard --port 8050
```

Open <http://127.0.0.1:8050>. The page shows:

- KPI cards with a monthly sparkline and the change versus the previous month: valid trips,
  recorded total, average per trip, recorded tips, and the share of excluded rows
- a trend chart for any of those metrics, monthly or daily (daily adds a 7-day average)
- year-over-year monthly trips, and average trips per day of the week
- top 10 pickup zones colored by borough (names come from the TLC zone lookup file; the page
  falls back to location IDs), and the payment mix
- excluded rows per month by reason, and a coverage table for every loaded month

Pick a period with the presets (all time, last 12 months, each year) or **From/To**. The period is
kept in the URL (`?start=2024-01&end=2024-03`) so a view can be shared. Results are cached for
5 minutes; **Refresh** queries ClickHouse again. Light and dark themes are available; the choice is
remembered in the browser.

How it works: `ch-etl dashboard` is a small HTTP service (Python standard library, no extra
dependencies). The browser only receives query results. Credentials stay in the server process,
and the page can only run the fixed queries in `sql/dashboard/` with a validated month range.
Charts load Chart.js from `cdn.jsdelivr.net` and the Inter font from Google Fonts. `GET /healthz` returns `ok` for health checks.

The service has **no login**. By default it listens on `127.0.0.1` only. `--host 0.0.0.0` exposes
it to everyone who can reach the port: put it behind a VPN or an authenticating proxy, and use a
read-only ClickHouse user for it.

### Run it with Docker

```bash
make dashboard-docker        # build and start in the background
make dashboard-docker-down   # stop
```

The container reads ClickHouse settings from `.env` and is published on `127.0.0.1:8050` only.
With the local ClickHouse from `make up`, set `CLICKHOUSE_HOST=clickhouse` for the container
(inside Docker, `localhost` is the container itself).

### SQL Console

The same SQL files also work in the Cloud SQL Console: it prompts for `{start:Date}` and
`{end:Date}` (first day of a month, for example `2024-01-01`). See
[docs/experiment-summary.md](docs/experiment-summary.md).

## Commands

| Command | Make target | What it does |
|---|---|---|
| `ch-etl ping` | | check connectivity |
| `ch-etl migrate [--dry-run]` | `make migrate` | apply pending migrations |
| `ch-etl load-taxi --month YYYY-MM` | `make taxi MONTH=...` | load or replace one TLC month |
| `ch-etl load-taxi-range --start ... --end ...` | `make taxi-range` | load a range, resuming loaded months (`--reload`, `--strict`) |
| `ch-etl taxi-demo --month YYYY-MM` | `make taxi-demo MONTH=...` | verify the aggregate and benchmark detail vs. materialized queries |
| `ch-etl dashboard [--host] [--port]` | `make dashboard` | serve the web dashboard |
| | `make dashboard-docker` | build and run the dashboard container |
| `ch-etl load-sample` | `make load` | synthetic events into `raw.events` |
| `ch-etl transform` | `make transform` | incremental staging → mart for synthetic events |
| `ch-etl stats` | `make stats` | row counts per layer for synthetic events |
| `ch-etl ops` | `make ops` | parts, compression, insert profile, merges |

Run one loader at a time. There is no distributed lock for parallel publishers.

## NYC taxi pipeline

```text
NYC TLC Parquet over HTTPS
  → per-process import table (parse + quality flag)
  → row-count validation
  → REPLACE PARTITION (monthly) into raw.taxi_trips
  → staging.taxi_trips   view: quality_issue = 'valid'
  → mart.taxi_daily_mv   refreshable materialized view
  → mart.taxi_daily      day × pickup location × payment type
```

- Reloading a month replaces it atomically, so reruns never append duplicates.
- Every source row is kept in raw with a `quality_issue`, so the dashboard can account for every row.
- USD amounts are recorded passenger charges, not driver profit. Cash tips are not in the data.
- `taxi-demo` checks that every aggregate group matches the detail query before it reports
  timings, compression, and an index-pruning plan. It makes no fixed speedup promise.

Details, failure behavior, and the quality rules: [docs/nyc-taxi.md](docs/nyc-taxi.md).
Loading a range and writing up results: [docs/experiment-summary.md](docs/experiment-summary.md).

## Synthetic event pipeline

```
 source ──batched, idempotent INSERT──▶ raw.events            MergeTree, 30-day TTL, replay buffer
                                            │
                                 materialized view (runs at insert time: clean, parse JSON)
                                            ▼
                                       staging.events        ReplacingMergeTree(_version): dedup
                                            │
                       ch-etl transform (watermark → rebuild only the days that changed)
                                            ▼
                                  mart.daily_event_stats     ReplacingMergeTree: idempotent reruns
```

```bash
make migrate
make load                 # 200k synthetic events, with duplicates and late arrivals
make transform            # incremental rebuild of the mart
make stats                # row counts per layer: watch the duplicates disappear
make ops
```

### Optimizations in this project

| Practice | Why | Where |
|---|---|---|
| Insert 10k–100k+ rows per INSERT | Each INSERT creates a part. Small inserts cause too many parts, merge pressure, then throttled inserts | `load.py`, `ETL_BATCH_SIZE` |
| `async_insert` for trickle sources | Server-side batching when the client can't batch | `--async-insert`, `runlog.py` |
| `insert_deduplication_token` per batch | Retrying after a timeout won't duplicate rows | `load.dedup_token` |
| Coarse monthly `PARTITION BY` | Partitions are for data lifecycle (TTL/drop), not for query speed | all tables |
| `ORDER BY` from low to high cardinality | Better index pruning and compression | `003`, `004` |
| `LowCardinality(String)`, no `Nullable` | Dictionary encoding; Nullable adds a null-map per column | `002`, `003` |
| `Delta + ZSTD` codecs on timestamps | Monotonic values compress very well | `002`, `003` |
| Materialized view for row-level transforms | Transform happens during the INSERT, with no second pass | `003` |
| `ReplacingMergeTree` + `FINAL` on a bounded range | Upsert/dedup semantics without `ALTER UPDATE` mutations | `003`, `transform.py` |
| Watermark on `_ingested_at`, rebuild affected days | Handles late data; reruns are idempotent | `transform.py` |
| minmax skip index on `_ingested_at` | Watermark lookups don't scan the whole table | `003` |
| TTL with `ttl_only_drop_parts` | Raw retention drops whole parts instead of rewriting them | `002` |

### Things to avoid in pipelines

- `OPTIMIZE TABLE ... FINAL` on a schedule. It rewrites everything. Use `FINAL` at query time instead.
- `ALTER TABLE ... UPDATE/DELETE` per batch. These are mutations that rewrite parts. Model
  changes as new versions in a ReplacingMergeTree.
- Partitioning by day or by a high-cardinality key "for speed".
- Using a random `batch_id` per run. Derive it from the source (file name, offset range) so
  replays are deduplicated.

### Adding a real source

1. Write an extractor that yields tuples in table-column order. `sample.py` is the template.
2. Choose a `batch_id` that identifies the source data, not the run.
3. Call `insert_batches(...)` inside `track_run(...)` so every run lands in `etl.runs`.
4. Schema changes go in a new `migrations/NNN_*.sql`. Never edit an applied one.

## Gotchas

- **Idle wake-up:** Cloud services can go idle and wake on the first query. `client.connect`
  retries with backoff, and `CLICKHOUSE_CONNECT_TIMEOUT` defaults to 30s.
- **ReplacingMergeTree dedup is eventual.** It only collapses rows within a partition, during
  merges. Read with `FINAL` (or `argMax`) when exact counts matter.
- **MVs see every inserted block, duplicates included.** That's why the event mart is built from
  `staging FINAL` in a batch step rather than by a second MV.
- **`REPLACE PARTITION` does not trigger incremental MVs.** The taxi mart uses a refreshable MV
  for that reason; loaders refresh it and wait.
- **`query_log` is per replica on Cloud.** Use `clusterAllReplicas('default', system.query_log)`
  to see the whole service.

## Layout

```
migrations/      NNN_name.sql, forward-only, every statement IF NOT EXISTS
sql/dashboard/   dashboard queries ({start:Date}/{end:Date}), used by `ch-etl dashboard`
sql/taxi/        taxi import, verification, and benchmark queries
sql/ops/         health queries run by `ch-etl ops`
src/ch_etl/      cli, config, client, migrate, taxi, dashboard (+ dashboard.html),
                 load, transform, runlog, sample
tests/           unit tests (no server needed) and an opt-in live integration test
docs/            taxi walkthrough and experiment summary (Indonesian)
docker/          local-only server config (enables insert dedup on plain MergeTree)
Dockerfile       dashboard service image (docker-compose.yml: `dashboard` profile)
reports/         JSON results written by the taxi commands (git-ignored)
```

## Tests

```bash
make test                 # unit tests, no server needed
make lint
make test-integration     # live: uses .env, imports January 2024 twice
```

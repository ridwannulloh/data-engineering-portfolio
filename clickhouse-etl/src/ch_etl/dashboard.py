"""Web dashboard service for the NYC taxi tables.

The browser only receives query results; ClickHouse credentials stay in this process. The
page can only run the fixed queries in sql/dashboard/ with a validated month range.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import threading
import time
import urllib.request
from datetime import date, datetime
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from importlib import resources
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from clickhouse_connect.driver.client import Client

from ch_etl.taxi import parse_month

log = logging.getLogger(__name__)

ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
DEFAULT_START = date(2024, 1, 1)
CACHE_SECONDS = 300
CACHE_ENTRIES = 32


def parse_range(query: dict[str, list[str]], *, today: date | None = None) -> tuple[date, date]:
    """Read ?start=YYYY-MM&end=YYYY-MM; defaults to January 2024 through the current month."""
    start = parse_month(query["start"][0]) if query.get("start") else DEFAULT_START
    end_default = (today or date.today()).replace(day=1)
    end = parse_month(query["end"][0]) if query.get("end") else end_default
    if start > end:
        raise ValueError("start month must not be after end month")
    return start, end


def _json_value(value: object) -> object:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, date | datetime):
        return value.isoformat()
    return value


def run_queries(client: Client, sql_dir: Path, start: date, end: date) -> dict[str, Any]:
    """Run every sql/dashboard/*.sql file; results are keyed by file stem without the NN_ prefix."""
    results = {}
    for path in sorted(sql_dir.glob("*.sql")):
        result = client.query(path.read_text(), parameters={"start": start, "end": end})
        results[path.stem.split("_", 1)[-1]] = {
            "columns": list(result.column_names),
            "rows": [[_json_value(v) for v in row] for row in result.result_rows],
        }
    return results


def parse_zones(text: str) -> dict[int, dict[str, str]]:
    zones = {}
    for row in csv.DictReader(io.StringIO(text)):
        try:
            zones[int(row["LocationID"])] = {"borough": row["Borough"], "zone": row["Zone"]}
        except (KeyError, TypeError, ValueError):
            continue
    return zones


def fetch_zones() -> dict[int, dict[str, str]]:
    """Best effort: without the lookup file the dashboard shows location IDs."""
    try:
        with urllib.request.urlopen(ZONE_LOOKUP_URL, timeout=15) as response:
            return parse_zones(response.read().decode("utf-8"))
    except OSError as exc:
        log.warning("zone lookup unavailable (%s); showing location IDs only", exc)
        return {}


class DashboardData:
    """Caches results per month range so reloads don't rescan raw.taxi_trips every time."""

    def __init__(
        self,
        client: Client,
        sql_dir: Path,
        *,
        ttl: float = CACHE_SECONDS,
        zones_loader=fetch_zones,
    ) -> None:
        self._client = client
        self._sql_dir = sql_dir
        self._ttl = ttl
        self._zones_loader = zones_loader
        self._zones: dict[int, dict[str, str]] | None = None
        self._cache: dict[tuple[date, date], tuple[float, dict[str, Any]]] = {}
        # One ClickHouse client is shared by the request threads; run its queries one at a time.
        self._lock = threading.Lock()

    def get(self, start: date, end: date, *, refresh: bool = False) -> dict[str, Any]:
        key = (start, end)
        with self._lock:
            cached = self._cache.get(key)
            if cached and not refresh and time.monotonic() - cached[0] <= self._ttl:
                return cached[1]
            if self._zones is None:
                self._zones = self._zones_loader()
            queries = run_queries(self._client, self._sql_dir, start, end)
            ids = {row[0] for row in queries.get("top_pickup_locations", {}).get("rows", [])}
            payload = {
                "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
                "range": {"start": f"{start:%Y-%m}", "end": f"{end:%Y-%m}"},
                "queries": queries,
                "zones": {str(i): self._zones[i] for i in ids if i in self._zones},
            }
            if len(self._cache) >= CACHE_ENTRIES:
                self._cache.clear()
            self._cache[key] = (time.monotonic(), payload)
            return payload


def _make_handler(data: DashboardData) -> type[BaseHTTPRequestHandler]:
    page = resources.files("ch_etl").joinpath("dashboard.html").read_bytes()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - http.server naming
            url = urlparse(self.path)
            if url.path in ("/", "/index.html"):
                self._send(200, "text/html; charset=utf-8", page)
            elif url.path == "/healthz":
                self._send(200, "text/plain; charset=utf-8", b"ok")
            elif url.path == "/api/data":
                self._send_data(parse_qs(url.query))
            else:
                self._send(404, "text/plain; charset=utf-8", b"not found")

        def _send_data(self, query: dict[str, list[str]]) -> None:
            try:
                start, end = parse_range(query)
            except ValueError as exc:
                self._send_json(400, {"error": str(exc)})
                return
            try:
                payload = data.get(start, end, refresh=query.get("refresh") == ["1"])
            except Exception as exc:  # keep serving; the page shows the error
                log.exception("dashboard query failed")
                self._send_json(502, {"error": f"ClickHouse query failed: {exc}"})
                return
            self._send_json(200, payload)

        def _send_json(self, status: int, body: dict[str, Any]) -> None:
            self._send(status, "application/json", json.dumps(body).encode())

        def _send(self, status: int, content_type: str, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, fmt: str, *args: object) -> None:
            log.debug("%s %s", self.address_string(), fmt % args)

    return Handler


def serve(client: Client, sql_dir: Path, *, host: str, port: int) -> None:
    server = ThreadingHTTPServer((host, port), _make_handler(DashboardData(client, sql_dir)))
    if host not in ("127.0.0.1", "localhost", "::1"):
        log.warning("dashboard has no login; anyone who can reach %s:%s can view it", host, port)
    print(f"Dashboard running at http://{host}:{port}  (Ctrl+C to stop)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

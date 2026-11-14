import json
import threading
import urllib.error
import urllib.request
from datetime import date
from decimal import Decimal
from http.server import ThreadingHTTPServer
from types import SimpleNamespace

import pytest

from ch_etl import dashboard
from ch_etl.config import DASHBOARD_SQL_DIR

JAN = date(2024, 1, 1)
MAR = date(2024, 3, 1)


class FakeClient:
    def __init__(self):
        self.calls = []

    def query(self, sql, parameters=None):
        self.calls.append((sql, parameters))
        rows = [(132, 10, Decimal("12.50"))] if "pickup_location_id" in sql else [(JAN, 1)]
        return SimpleNamespace(column_names=("a", "b"), result_rows=rows)


def test_range_defaults_to_january_2024_through_current_month():
    assert dashboard.parse_range({}, today=date(2026, 11, 14)) == (JAN, date(2026, 11, 1))
    assert dashboard.parse_range({"start": ["2024-01"], "end": ["2024-03"]}) == (JAN, MAR)


@pytest.mark.parametrize(
    "query",
    [{"start": ["2024-03"], "end": ["2024-01"]}, {"start": ["2024-1"]}, {"end": ["2016-12"]}],
)
def test_range_rejects_bad_months(query):
    with pytest.raises(ValueError):
        dashboard.parse_range(query)


def test_every_dashboard_query_is_bound_to_the_range():
    client = FakeClient()
    result = dashboard.run_queries(client, DASHBOARD_SQL_DIR, JAN, MAR)
    assert set(result) == {
        "available_months",
        "kpis",
        "monthly_trend",
        "daily_trend",
        "top_pickup_locations",
        "quality_and_coverage",
        "payment_types",
    }
    assert all(params == {"start": JAN, "end": MAR} for _, params in client.calls)
    for path in DASHBOARD_SQL_DIR.glob("0[1-9]_*.sql"):
        assert "{start:Date}" in path.read_text() and "{end:Date}" in path.read_text(), path.name
    assert result["top_pickup_locations"]["rows"] == [[132, 10, 12.5]]
    assert result["available_months"]["rows"] == [["2024-01-01", 1]]


def test_zone_lookup_skips_malformed_rows():
    text = 'LocationID,Borough,Zone\n132,"Queens","JFK Airport"\nx,Bad,Row\n'
    assert dashboard.parse_zones(text) == {132: {"borough": "Queens", "zone": "JFK Airport"}}


def test_cache_reuses_results_until_refresh_and_sends_only_needed_zones():
    client = FakeClient()
    zones = {132: {"borough": "Queens", "zone": "JFK Airport"}, 1: {"borough": "EWR", "zone": "x"}}
    data = dashboard.DashboardData(client, DASHBOARD_SQL_DIR, zones_loader=lambda: zones)
    first = data.get(JAN, MAR)
    calls = len(client.calls)
    assert data.get(JAN, MAR) is first
    assert len(client.calls) == calls
    data.get(JAN, MAR, refresh=True)
    assert len(client.calls) == 2 * calls
    assert first["zones"] == {"132": zones[132]}
    assert first["range"] == {"start": "2024-01", "end": "2024-03"}


@pytest.fixture
def server():
    data = dashboard.DashboardData(FakeClient(), DASHBOARD_SQL_DIR, zones_loader=dict)
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), dashboard._make_handler(data))
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{httpd.server_port}"
    httpd.shutdown()
    httpd.server_close()


def test_http_routes(server):
    with urllib.request.urlopen(f"{server}/") as r:
        assert b"<title>NYC Taxi Dashboard</title>" in r.read()
    with urllib.request.urlopen(f"{server}/api/data?start=2024-01&end=2024-03") as r:
        assert json.load(r)["range"] == {"start": "2024-01", "end": "2024-03"}
    with pytest.raises(urllib.error.HTTPError) as err:
        urllib.request.urlopen(f"{server}/api/data?start=2024-03&end=2024-01")
    assert err.value.code == 400
    with pytest.raises(urllib.error.HTTPError) as err:
        urllib.request.urlopen(f"{server}/missing")
    assert err.value.code == 404

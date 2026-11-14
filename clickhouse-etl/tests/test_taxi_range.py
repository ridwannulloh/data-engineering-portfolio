from datetime import date
from types import SimpleNamespace

import pytest

from ch_etl import taxi

JAN = date(2024, 1, 1)
FEB = date(2024, 2, 1)
MAR = date(2024, 3, 1)


def test_inclusive_months_cross_year_and_reversed_range():
    assert taxi.month_range(date(2024, 12, 1), date(2025, 2, 1)) == [
        date(2024, 12, 1),
        date(2025, 1, 1),
        date(2025, 2, 1),
    ]
    assert len(taxi.month_range(JAN, date(2026, 11, 1))) == 35
    with pytest.raises(ValueError):
        taxi.month_range(FEB, JAN)


def setup_range(monkeypatch, *, existing=(), available=(JAN, FEB)):
    client = SimpleNamespace(
        query=lambda *a, **kw: SimpleNamespace(result_rows=[(m,) for m in existing])
    )
    loaded, refreshed = [], []

    def load(client, month, *, refresh):
        assert refresh is False
        loaded.append(month)
        return taxi.LoadResult(month.strftime("%Y-%m"), taxi.source_url(month), 100, 90, 10, 1.0)

    monkeypatch.setattr(taxi, "published_months", lambda: set(available))
    monkeypatch.setattr(taxi, "load_month", load)
    monkeypatch.setattr(taxi, "refresh_mart", lambda client: refreshed.append(True))
    return client, loaded, refreshed


def test_resume_skips_existing_and_unpublished_and_refreshes_once(monkeypatch):
    client, loaded, refreshed = setup_range(monkeypatch, existing=(JAN,))
    report = taxi.load_range(client, JAN, MAR)
    assert loaded == [FEB]
    assert report["skipped_existing"] == ["2024-01"]
    assert report["unavailable"] == ["2024-03"]
    assert report["loaded_rows"] == 100
    assert refreshed == [True]


def test_reload_replaces_existing_months(monkeypatch):
    client, loaded, refreshed = setup_range(monkeypatch, existing=(JAN, FEB))
    taxi.load_range(client, JAN, FEB, reload=True)
    assert loaded == [JAN, FEB]
    assert refreshed == [True]


def test_resume_refreshes_mart_even_when_all_months_already_exist(monkeypatch):
    client, loaded, refreshed = setup_range(monkeypatch, existing=(JAN, FEB))
    taxi.load_range(client, JAN, FEB)
    assert loaded == []
    assert refreshed == [True]


def test_strict_checks_unavailable_before_publishing(monkeypatch):
    client, loaded, refreshed = setup_range(monkeypatch)
    with pytest.raises(ValueError, match="not listed"):
        taxi.load_range(client, JAN, MAR, strict=True)
    assert loaded == refreshed == []


def test_failure_preserves_completed_months_and_stops_later_loads(monkeypatch):
    client, loaded, refreshed = setup_range(monkeypatch, available=(JAN, FEB, MAR))

    def load(client, month, *, refresh):
        loaded.append(month)
        if month == FEB:
            raise RuntimeError("source failed")
        return taxi.LoadResult("2024-01", taxi.source_url(month), 100, 90, 10, 1)

    monkeypatch.setattr(taxi, "load_month", load)
    with pytest.raises(RuntimeError, match="source failed"):
        taxi.load_range(client, JAN, MAR)
    assert loaded == [JAN, FEB]
    assert refreshed == [True]


def test_catalog_extracts_only_official_yellow_files(monkeypatch):
    page = b"""<a href="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet">Jan</a>
    <a href="https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-02.parquet">Green</a>
    <a href="https://example.com/yellow_tripdata_2024-03.parquet">Other</a>"""

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return page

    monkeypatch.setattr(taxi, "urlopen", lambda *a, **kw: Response())
    assert taxi.published_months() == {JAN}


def test_catalog_network_error_is_not_reported_as_missing_month(monkeypatch):
    def fail(*args, **kwargs):
        raise OSError("DNS failed")

    monkeypatch.setattr(taxi, "urlopen", fail)
    with pytest.raises(OSError, match="DNS failed"):
        taxi.published_months()

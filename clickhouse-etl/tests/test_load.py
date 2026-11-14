import pytest
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError

from ch_etl import load
from ch_etl.load import chunked, insert_batches, insert_settings
from ch_etl.sample import RAW_COLUMNS, generate_events


class FakeClient:
    def __init__(self, failures: list[Exception] | None = None):
        self.failures = list(failures or [])
        self.calls: list[dict] = []

    def insert(self, table, data, column_names, database, settings):
        self.calls.append(
            {"table": table, "database": database, "rows": len(data), "settings": settings}
        )
        if self.failures:
            raise self.failures.pop(0)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(load.time, "sleep", lambda _: None)


def test_chunked():
    assert [len(c) for c in chunked(range(7), 3)] == [3, 3, 1]
    assert list(chunked([], 3)) == []
    with pytest.raises(ValueError):
        list(chunked([1], 0))


def test_sync_settings_carry_dedup_token():
    assert insert_settings("b:0", use_async=False) == {
        "async_insert": 0,
        "insert_deduplication_token": "b:0",
    }
    assert insert_settings("b:0", use_async=True)["async_insert"] == 1


def test_insert_batches_uses_stable_tokens_per_chunk():
    client = FakeClient()
    total = insert_batches(
        client, "raw.events", ["a"], [[i] for i in range(5)], batch_id="file-1", batch_size=2
    )
    assert total == 5
    assert [c["rows"] for c in client.calls] == [2, 2, 1]
    assert [c["settings"]["insert_deduplication_token"] for c in client.calls] == [
        "file-1:0",
        "file-1:1",
        "file-1:2",
    ]
    assert {(c["database"], c["table"]) for c in client.calls} == {("raw", "events")}


def test_transient_errors_are_retried_with_same_token():
    client = FakeClient([OperationalError("timeout"), OperationalError("reset")])
    insert_batches(client, "raw.events", ["a"], [[1]], batch_id="b", batch_size=10)
    assert len(client.calls) == 3
    assert {c["settings"]["insert_deduplication_token"] for c in client.calls} == {"b:0"}


def test_database_errors_are_not_retried():
    client = FakeClient([DatabaseError("type mismatch")])
    with pytest.raises(DatabaseError):
        insert_batches(client, "raw.events", ["a"], [[1]], batch_id="b", batch_size=10)
    assert len(client.calls) == 1


def test_sample_rows_match_columns_and_include_duplicates():
    rows = list(generate_events(5000, batch_id="t", seed=1))
    assert len(rows) == 5000
    assert all(len(r) == len(RAW_COLUMNS) for r in rows)
    assert len({r[0] for r in rows}) < len(rows)

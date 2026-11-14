from __future__ import annotations

import logging
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime

from clickhouse_connect.driver.client import Client

log = logging.getLogger(__name__)

_COLUMNS = ["run_id", "pipeline", "status", "rows", "started_at", "finished_at", "error"]


@dataclass
class Run:
    pipeline: str
    rows: int = 0


@contextmanager
def track_run(client: Client, pipeline: str) -> Iterator[Run]:
    """Record one row in etl.runs per pipeline run, success or failure."""
    run = Run(pipeline)
    run_id, started = uuid.uuid4(), datetime.now(UTC)
    status, error = "success", ""
    try:
        yield run
    except BaseException as exc:
        status, error = "failed", repr(exc)[:2000]
        raise
    finally:
        try:
            client.insert(
                "runs",
                [[run_id, pipeline, status, run.rows, started, datetime.now(UTC), error]],
                column_names=_COLUMNS,
                database="etl",
                # One row per run: let the server batch these instead of creating a part each.
                settings={"async_insert": 1, "wait_for_async_insert": 1},
            )
        except Exception:
            log.exception("could not record run for %s", pipeline)

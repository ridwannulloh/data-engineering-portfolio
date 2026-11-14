from __future__ import annotations

import logging
import time

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import OperationalError

from ch_etl.config import Settings

log = logging.getLogger(__name__)


def connect(settings: Settings, *, attempts: int = 5) -> Client:
    """Open an HTTP(S) client, retrying while an idle Cloud service wakes up."""
    for attempt in range(1, attempts + 1):
        try:
            return clickhouse_connect.get_client(
                host=settings.host,
                port=settings.port,
                username=settings.user,
                password=settings.password,
                secure=settings.secure,
                connect_timeout=settings.connect_timeout,
                send_receive_timeout=600,
                # Sessions serialize queries per client; ETL doesn't need temp tables.
                autogenerate_session_id=False,
            )
        except OperationalError as exc:
            if attempt == attempts:
                raise
            delay = min(2**attempt, 30)
            log.warning(
                "connect attempt %d/%d failed (%s); retrying in %ss", attempt, attempts, exc, delay
            )
            time.sleep(delay)
    raise AssertionError("unreachable")


def split_table(qualified: str) -> tuple[str, str]:
    """'raw.events' -> ('raw', 'events')."""
    database, _, table = qualified.partition(".")
    if not table:
        raise ValueError(f"expected database.table, got {qualified!r}")
    return database, table

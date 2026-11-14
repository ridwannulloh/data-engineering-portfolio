"""Batched, idempotent loading into ClickHouse.

The rules this module encodes:
  * Few large inserts beat many small ones. Every INSERT creates a part on disk; too many parts
    and ClickHouse throttles, then rejects, inserts.
  * Retries must be safe. Each batch carries a deterministic insert_deduplication_token, so
    re-sending a batch after a timeout is dropped server-side instead of duplicating rows.
  * For trickle sources that can't batch client-side, let the server batch (async_insert).
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterable, Iterator, Sequence
from typing import Any, TypeVar

from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import OperationalError

from ch_etl.client import split_table

log = logging.getLogger(__name__)

T = TypeVar("T")


def chunked(rows: Iterable[T], size: int) -> Iterator[list[T]]:
    if size < 1:
        raise ValueError("batch size must be >= 1")
    batch: list[T] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def dedup_token(batch_id: str, chunk_no: int) -> str:
    """Same batch_id + same chunking => same token, so a replay of the batch is a no-op.

    batch_id must therefore identify the *source data* (file name, Kafka offset range, API
    page cursor), not the run.
    """
    return f"{batch_id}:{chunk_no}"


def insert_settings(token: str, *, use_async: bool) -> dict[str, Any]:
    if use_async:
        # Server buffers small inserts and flushes them as one part. wait=1 means the call only
        # returns once data is durable, so errors still surface to the pipeline.
        return {"async_insert": 1, "wait_for_async_insert": 1}
    return {"async_insert": 0, "insert_deduplication_token": token}


def insert_batches(
    client: Client,
    table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
    *,
    batch_id: str,
    batch_size: int,
    use_async: bool = False,
    retries: int = 3,
) -> int:
    database, name = split_table(table)
    total = 0
    for chunk_no, chunk in enumerate(chunked(rows, batch_size)):
        settings = insert_settings(dedup_token(batch_id, chunk_no), use_async=use_async)
        _insert_with_retry(client, database, name, columns, chunk, settings, retries)
        total += len(chunk)
        log.info("%s: chunk %d inserted (%d rows, %d total)", table, chunk_no, len(chunk), total)
    return total


def _insert_with_retry(
    client: Client,
    database: str,
    table: str,
    columns: Sequence[str],
    chunk: list[Sequence[Any]],
    settings: dict[str, Any],
    retries: int,
) -> None:
    for attempt in range(retries + 1):
        try:
            client.insert(
                table, chunk, column_names=list(columns), database=database, settings=settings
            )
            return
        except OperationalError as exc:
            # Network/timeout errors only: the dedup token makes the resend safe.
            # DatabaseError (bad types, missing table) is not retryable and propagates.
            if attempt == retries:
                raise
            delay = 2**attempt
            log.warning(
                "insert into %s.%s failed (%s); retry %d/%d in %ss",
                database,
                table,
                exc,
                attempt + 1,
                retries,
                delay,
            )
            time.sleep(delay)

"""Minimal forward-only migration runner.

ClickHouse DDL is not transactional, so every statement in migrations/ uses IF NOT EXISTS:
re-running a half-applied migration is safe.
"""

from __future__ import annotations

import hashlib
import logging
import re
from dataclasses import dataclass
from pathlib import Path

from clickhouse_connect.driver.client import Client

log = logging.getLogger(__name__)

_NAME = re.compile(r"^(\d+)_([\w-]+)\.sql$")

_BOOTSTRAP = [
    "CREATE DATABASE IF NOT EXISTS etl",
    """
    CREATE TABLE IF NOT EXISTS etl.schema_migrations
    (
        version    UInt32,
        name       String,
        checksum   String,
        applied_at DateTime64(3, 'UTC') DEFAULT now64(3)
    )
    ENGINE = MergeTree
    ORDER BY version
    """,
]


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    sql: str

    @property
    def checksum(self) -> str:
        return hashlib.sha256(self.sql.encode()).hexdigest()[:16]

    @property
    def statements(self) -> list[str]:
        return split_statements(self.sql)


def split_statements(sql: str) -> list[str]:
    """Split on semicolons, ignoring those inside '...' literals and -- comments."""
    statements: list[str] = []
    buf: list[str] = []
    in_string = False
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        if in_string:
            buf.append(c)
            if c == "\\" and i + 1 < n:
                buf.append(sql[i + 1])
                i += 2
                continue
            if c == "'":
                in_string = False
        elif c == "'":
            in_string = True
            buf.append(c)
        elif sql.startswith("--", i):
            newline = sql.find("\n", i)
            i = n if newline == -1 else newline
            continue
        elif c == ";":
            statement = "".join(buf).strip()
            if statement:
                statements.append(statement)
            buf = []
        else:
            buf.append(c)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def discover(directory: Path) -> list[Migration]:
    migrations: dict[int, Migration] = {}
    for path in sorted(directory.glob("*.sql")):
        match = _NAME.match(path.name)
        if not match:
            raise ValueError(f"bad migration filename {path.name!r}; expected NNN_name.sql")
        version = int(match.group(1))
        if version in migrations:
            raise ValueError(f"duplicate migration version {version}: {path.name}")
        migrations[version] = Migration(version, match.group(2), path.read_text())
    return [migrations[v] for v in sorted(migrations)]


def applied(client: Client) -> dict[int, str]:
    rows = client.query("SELECT version, checksum FROM etl.schema_migrations").result_rows
    return {int(version): checksum for version, checksum in rows}


def run(client: Client, directory: Path, *, dry_run: bool = False) -> list[Migration]:
    for statement in _BOOTSTRAP:
        client.command(statement)

    done = applied(client)
    pending: list[Migration] = []
    for migration in discover(directory):
        if migration.version in done:
            if done[migration.version] != migration.checksum:
                log.warning(
                    "migration %03d_%s changed after it was applied; "
                    "add a new migration instead of editing old ones",
                    migration.version,
                    migration.name,
                )
            continue
        pending.append(migration)

    for migration in pending:
        log.info(
            "%s %03d_%s (%d statements)",
            "would apply" if dry_run else "applying",
            migration.version,
            migration.name,
            len(migration.statements),
        )
        if dry_run:
            continue
        for statement in migration.statements:
            client.command(statement)
        client.insert(
            "schema_migrations",
            [[migration.version, migration.name, migration.checksum]],
            column_names=["version", "name", "checksum"],
            database="etl",
        )
    return pending

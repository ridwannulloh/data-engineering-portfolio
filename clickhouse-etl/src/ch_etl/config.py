from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS_DIR = PROJECT_ROOT / "migrations"
OPS_SQL_DIR = PROJECT_ROOT / "sql" / "ops"
TAXI_SQL_DIR = PROJECT_ROOT / "sql" / "taxi"
DASHBOARD_SQL_DIR = PROJECT_ROOT / "sql" / "dashboard"


def _bool(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class Settings:
    host: str
    port: int
    user: str
    password: str
    secure: bool
    connect_timeout: int
    batch_size: int

    @classmethod
    def from_env(cls) -> Settings:
        load_dotenv(PROJECT_ROOT / ".env")
        host = os.environ.get("CLICKHOUSE_HOST", "").strip()
        if not host:
            raise SystemExit("CLICKHOUSE_HOST is not set. Copy .env.example to .env and fill it.")
        secure = _bool(os.environ.get("CLICKHOUSE_SECURE", "true"))
        return cls(
            host=host,
            port=int(os.environ.get("CLICKHOUSE_PORT") or (8443 if secure else 8123)),
            user=os.environ.get("CLICKHOUSE_USER", "default"),
            password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
            secure=secure,
            connect_timeout=int(os.environ.get("CLICKHOUSE_CONNECT_TIMEOUT", "30")),
            batch_size=int(os.environ.get("ETL_BATCH_SIZE", "50000")),
        )

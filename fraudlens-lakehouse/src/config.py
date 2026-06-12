"""Environment-driven configuration.

Everything is a function (not a module-level constant) so values are resolved
at call time — this lets tests repoint the lakehouse at a tmp directory with
monkeypatch, and lets the same code run against MinIO (Docker) or the local
filesystem (pytest) without branching in the pipeline code.
"""
import os


def storage_backend() -> str:
    """'s3' (MinIO via s3a://) or 'local' (plain filesystem, used by tests)."""
    return os.environ.get("STORAGE_BACKEND", "local")


def minio_endpoint() -> str:
    return os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")


def minio_access_key() -> str:
    return os.environ.get("MINIO_ACCESS_KEY", "minioadmin")


def minio_secret_key() -> str:
    return os.environ.get("MINIO_SECRET_KEY", "minioadmin")


def lakehouse_bucket() -> str:
    return os.environ.get("LAKEHOUSE_BUCKET", "lakehouse")


def local_lakehouse_dir() -> str:
    return os.environ.get("LOCAL_LAKEHOUSE_DIR", "./lakehouse")


def _layer_root(layer: str) -> str:
    if storage_backend() == "s3":
        return f"s3a://{lakehouse_bucket()}/{layer}"
    return f"{local_lakehouse_dir()}/{layer}"


def bronze_path() -> str:
    return f"{_layer_root('bronze')}/transactions"


def silver_path() -> str:
    return f"{_layer_root('silver')}/transactions"


def gold_path(table_name: str) -> str:
    return f"{_layer_root('gold')}/{table_name}"


def raw_data_dir() -> str:
    return os.environ.get("RAW_DATA_DIR", "./data/raw")


def raw_transactions_path() -> str:
    return f"{raw_data_dir()}/transactions.csv"

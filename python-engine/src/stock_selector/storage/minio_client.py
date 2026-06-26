import os
from typing import Iterable

from minio import Minio


class MinioConfigError(RuntimeError):
    pass


def _env_required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise MinioConfigError(f"missing environment variable: {name}")
    return value


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def create_minio_client(settings: dict) -> Minio:
    endpoint = os.getenv("STOCK_MINIO_ENDPOINT") or settings["storage"]["minio_endpoint"]
    return Minio(
        endpoint,
        access_key=_env_required("STOCK_MINIO_ACCESS_KEY"),
        secret_key=_env_required("STOCK_MINIO_SECRET_KEY"),
        secure=_env_bool("STOCK_MINIO_SECURE", default=False),
    )


def get_required_buckets(settings: dict) -> list[str]:
    storage = settings["storage"]
    return [
        storage["minio_bucket_raw"],
        storage["minio_bucket_processed"],
        storage["minio_bucket_backtest"],
    ]


def ensure_buckets(client: Minio, buckets: Iterable[str]) -> None:
    for bucket in buckets:
        if client.bucket_exists(bucket):
            continue
        client.make_bucket(bucket)

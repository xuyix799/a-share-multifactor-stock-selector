from pathlib import Path
import tempfile
from typing import Any

import pandas as pd
from minio.error import S3Error

from stock_selector.config.config_loader import load_settings
from stock_selector.storage.atomic_writer import AtomicObjectWriter, write_parquet_local_atomic
from stock_selector.storage.minio_client import create_minio_client, ensure_buckets
from stock_selector.storage.partition import build_partition, validate_dataset
from stock_selector.utils.date_validator import validate_date_range


def read_dataset_history_between(dataset: str, start_date: str, end_date: str) -> pd.DataFrame:
    dataset = validate_dataset(dataset)
    start_date, end_date = validate_date_range(start_date, end_date)
    settings = load_settings()
    frames = _read_local_history(dataset, start_date, end_date, settings) if _storage_backend(settings) == "local" else _read_minio_history(dataset, start_date, end_date, settings)
    if not frames:
        raise FileNotFoundError(f"missing parquet history for {dataset} {start_date}..{end_date}")
    return pd.concat(frames, ignore_index=True)


def write_backtest_detail(run_key: str, detail: pd.DataFrame) -> str:
    settings = load_settings()
    if _storage_backend(settings) == "local":
        path = _local_root(settings) / "backtest" / "detail" / f"run_key={run_key}" / "part.parquet"
        write_parquet_local_atomic(detail, path)
        return path.as_posix()

    minio_client = create_minio_client(settings)
    bucket = settings["storage"]["minio_bucket_backtest"]
    ensure_buckets(minio_client, [bucket])
    object_key = f"backtest/detail/run_key={run_key}/part.parquet"
    with tempfile.TemporaryDirectory(prefix="stock-backtest-write-") as tmp:
        local_file = Path(tmp) / "part.parquet"
        detail.to_parquet(local_file, index=False)
        result = AtomicObjectWriter(minio_client, tmp_dir=Path(tmp)).write_file_atomic(bucket, object_key, local_file)
    return result.final_key


def _read_local_history(dataset: str, start_date: str, end_date: str, settings: dict[str, Any]) -> list[pd.DataFrame]:
    sample_partition = build_partition(dataset, start_date, local_root=_local_root(settings))
    root = sample_partition.local_path.parents[1]
    if not root.exists():
        return []
    frames = []
    for parquet_path in sorted(root.glob("trade_date=*/part.parquet")):
        partition_date = _date_from_partition_name(parquet_path.parent.name)
        if start_date <= partition_date <= end_date:
            frames.append(pd.read_parquet(parquet_path))
    return frames


def _read_minio_history(dataset: str, start_date: str, end_date: str, settings: dict[str, Any]) -> list[pd.DataFrame]:
    minio_client = create_minio_client(settings)
    bucket = settings["storage"]["minio_bucket_raw"]
    sample_partition = build_partition(dataset, start_date)
    prefix = sample_partition.object_key.split("trade_date=", 1)[0] + "trade_date="
    frames = []
    with tempfile.TemporaryDirectory(prefix="stock-backtest-history-") as tmp:
        tmp_root = Path(tmp)
        for item in minio_client.list_objects(bucket, prefix=prefix, recursive=True):
            key = item.object_name
            if not key.endswith("/part.parquet"):
                continue
            partition_name = key.removeprefix(prefix.rsplit("/", 1)[0] + "/").split("/", 1)[0]
            partition_date = _date_from_partition_name(partition_name)
            if not (start_date <= partition_date <= end_date):
                continue
            target = tmp_root / key
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                minio_client.fget_object(bucket, key, str(target))
            except S3Error as exc:
                if exc.code in {"NoSuchKey", "NoSuchBucket", "NoSuchObject"}:
                    continue
                raise
            frames.append(pd.read_parquet(target))
    return frames


def _date_from_partition_name(partition_name: str) -> str:
    if not partition_name.startswith("trade_date="):
        raise ValueError(f"invalid partition name: {partition_name}")
    return partition_name.split("=", 1)[1]


def _storage_backend(settings: dict[str, Any]) -> str:
    import os

    backend = os.getenv("STOCK_PARQUET_BACKEND") or settings["storage"].get("parquet_backend", "minio")
    if backend not in {"local", "minio"}:
        raise ValueError(f"unsupported storage backend: {backend}")
    return backend


def _local_root(settings: dict[str, Any]) -> Path:
    import os

    return Path(os.getenv("STOCK_LOCAL_DATA_DIR") or settings["storage"].get("local_data_dir", "data"))

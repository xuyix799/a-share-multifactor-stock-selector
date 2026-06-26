from collections.abc import Callable
from pathlib import Path
import tempfile
from typing import Any

import pandas as pd
from minio.error import S3Error

from stock_selector.cleaning.adjust_price import build_adjusted_price
from stock_selector.cleaning.snapshot_builder import build_clean_daily_snapshot
from stock_selector.cleaning.snapshot_validator import validate_clean_daily_snapshot
from stock_selector.config.config_loader import load_settings
from stock_selector.data.data_validator import validate_dataset_frame
from stock_selector.data.update_log import UpdateLogRepository, create_update_log_repository
from stock_selector.storage.atomic_writer import AtomicObjectWriter, write_parquet_local_atomic
from stock_selector.storage.minio_client import create_minio_client, ensure_buckets
from stock_selector.storage.partition import build_partition, validate_dataset
from stock_selector.utils.date_validator import validate_trade_date


ReadDatasetFn = Callable[[str, str], pd.DataFrame]
WriteDatasetFn = Callable[[str, str, pd.DataFrame], str]


def build_adjusted_price_for_date(
    trade_date: str,
    force: bool = False,
    update_log_repo: UpdateLogRepository | None = None,
    read_dataset_fn: ReadDatasetFn | None = None,
    write_dataset_fn: WriteDatasetFn | None = None,
) -> dict[str, Any]:
    trade_date = validate_trade_date(trade_date)
    step_name = "cleaning:adjusted_price"
    repo = update_log_repo or create_update_log_repository()
    if not repo.should_run_step(trade_date, step_name, force=force):
        return {"dataset": "adjusted_price", "step_name": step_name, "status": "skipped"}

    reader = read_dataset_fn or _default_read_dataset
    writer = write_dataset_fn or _default_write_dataset
    try:
        repo.mark_step_running(trade_date, step_name)
        adjusted = build_adjusted_price(reader("daily_price", trade_date), reader("adj_factor", trade_date), trade_date)
        object_key = writer("adjusted_price", trade_date, adjusted)
        repo.mark_step_done(trade_date, step_name, object_key)
        return {"dataset": "adjusted_price", "step_name": step_name, "status": "done", "object_key": object_key, "row_count": len(adjusted)}
    except Exception as exc:
        repo.mark_step_failed(trade_date, step_name, str(exc))
        raise


def build_clean_snapshot_for_date(
    trade_date: str,
    force: bool = False,
    update_log_repo: UpdateLogRepository | None = None,
    read_dataset_fn: ReadDatasetFn | None = None,
    write_dataset_fn: WriteDatasetFn | None = None,
) -> dict[str, Any]:
    trade_date = validate_trade_date(trade_date)
    step_name = "cleaning:clean_daily_snapshot"
    repo = update_log_repo or create_update_log_repository()
    if not repo.should_run_step(trade_date, step_name, force=force):
        return {"dataset": "clean_daily_snapshot", "step_name": step_name, "status": "skipped"}

    reader = read_dataset_fn or _default_read_dataset
    writer = write_dataset_fn or _default_write_dataset
    try:
        repo.mark_step_running(trade_date, step_name)
        financial = reader("financial", trade_date) if read_dataset_fn else read_dataset_history("financial", trade_date)
        st_history = reader("st_history", trade_date) if read_dataset_fn else read_dataset_history("st_history", trade_date)
        snapshot = build_clean_daily_snapshot(
            stock_basic=reader("stock_basic", trade_date),
            daily_price=reader("daily_price", trade_date),
            adj_factor=reader("adj_factor", trade_date),
            daily_basic=reader("daily_basic", trade_date),
            financial=financial,
            st_history=st_history,
            benchmark_price=reader("benchmark_price", trade_date),
            trade_date=trade_date,
        )
        validate_clean_daily_snapshot(snapshot, trade_date)
        object_key = writer("clean_daily_snapshot", trade_date, snapshot)
        repo.mark_step_done(trade_date, step_name, object_key)
        return {"dataset": "clean_daily_snapshot", "step_name": step_name, "status": "done", "object_key": object_key, "row_count": len(snapshot)}
    except Exception as exc:
        repo.mark_step_failed(trade_date, step_name, str(exc))
        raise


def _default_read_dataset(dataset: str, trade_date: str) -> pd.DataFrame:
    settings = load_settings()
    partition = build_partition(dataset, trade_date, local_root=_local_root(settings))
    if _storage_backend(settings) == "local":
        if not partition.local_path.exists():
            raise FileNotFoundError(partition.local_path)
        return pd.read_parquet(partition.local_path)

    minio_client = create_minio_client(settings)
    bucket = settings["storage"]["minio_bucket_raw"]
    with tempfile.TemporaryDirectory(prefix="stock-clean-read-") as tmp:
        target = Path(tmp) / partition.object_key
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            minio_client.fget_object(bucket, partition.object_key, str(target))
        except S3Error as exc:
            if exc.code in {"NoSuchKey", "NoSuchBucket", "NoSuchObject"}:
                raise FileNotFoundError(partition.object_key) from exc
            raise
        return pd.read_parquet(target)


def read_dataset_history(dataset: str, trade_date: str) -> pd.DataFrame:
    dataset = validate_dataset(dataset)
    trade_date = validate_trade_date(trade_date)
    settings = load_settings()
    if _storage_backend(settings) == "local":
        frames = _read_local_dataset_history(dataset, trade_date, settings)
    else:
        frames = _read_minio_dataset_history(dataset, trade_date, settings)
    if not frames:
        raise FileNotFoundError(f"missing parquet history for {dataset} <= {trade_date}")
    return pd.concat(frames, ignore_index=True)


def _read_local_dataset_history(dataset: str, trade_date: str, settings: dict[str, Any]) -> list[pd.DataFrame]:
    root = _local_root(settings) / "raw" / dataset
    if not root.exists():
        return []
    frames = []
    for parquet_path in sorted(root.glob("trade_date=*/part.parquet")):
        partition_date = _date_from_partition_name(parquet_path.parent.name)
        if partition_date <= trade_date:
            frames.append(pd.read_parquet(parquet_path))
    return frames


def _read_minio_dataset_history(dataset: str, trade_date: str, settings: dict[str, Any]) -> list[pd.DataFrame]:
    minio_client = create_minio_client(settings)
    bucket = settings["storage"]["minio_bucket_raw"]
    prefix = f"raw/{dataset}/trade_date="
    frames = []
    with tempfile.TemporaryDirectory(prefix="stock-clean-history-") as tmp:
        tmp_root = Path(tmp)
        for item in minio_client.list_objects(bucket, prefix=prefix, recursive=True):
            key = item.object_name
            if not key.endswith("/part.parquet"):
                continue
            partition_name = key.removeprefix(f"raw/{dataset}/").split("/", 1)[0]
            partition_date = _date_from_partition_name(partition_name)
            if partition_date > trade_date:
                continue
            target = tmp_root / key
            target.parent.mkdir(parents=True, exist_ok=True)
            minio_client.fget_object(bucket, key, str(target))
            frames.append(pd.read_parquet(target))
    return frames


def _date_from_partition_name(partition_name: str) -> str:
    if not partition_name.startswith("trade_date="):
        raise ValueError(f"invalid partition name: {partition_name}")
    return validate_trade_date(partition_name.split("=", 1)[1])


def _default_write_dataset(dataset: str, trade_date: str, df: pd.DataFrame) -> str:
    validate_dataset_frame(dataset, df, trade_date)
    settings = load_settings()
    partition = build_partition(dataset, trade_date, local_root=_local_root(settings))
    if _storage_backend(settings) == "local":
        write_parquet_local_atomic(df, partition.local_path)
        return partition.local_path.as_posix()

    minio_client = create_minio_client(settings)
    bucket = settings["storage"]["minio_bucket_raw"]
    ensure_buckets(minio_client, [bucket])
    with tempfile.TemporaryDirectory(prefix="stock-clean-write-") as tmp:
        local_file = Path(tmp) / "part.parquet"
        df.to_parquet(local_file, index=False)
        result = AtomicObjectWriter(minio_client, tmp_dir=Path(tmp)).write_file_atomic(bucket, partition.object_key, local_file)
    return result.final_key


def _storage_backend(settings: dict[str, Any]) -> str:
    import os

    backend = os.getenv("STOCK_PARQUET_BACKEND") or settings["storage"].get("parquet_backend", "minio")
    if backend not in {"local", "minio"}:
        raise ValueError(f"unsupported storage backend: {backend}")
    return backend


def _local_root(settings: dict[str, Any]) -> Path:
    import os

    return Path(os.getenv("STOCK_LOCAL_DATA_DIR") or settings["storage"].get("local_data_dir", "data"))

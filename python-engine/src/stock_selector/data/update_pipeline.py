from collections.abc import Callable, Iterable
from pathlib import Path
import tempfile
from typing import Any

import pandas as pd

from stock_selector.config.config_loader import load_settings
from stock_selector.data.update_log import UpdateLogRepository, create_update_log_repository
from stock_selector.providers.provider_factory import create_provider
from stock_selector.providers.schema_mapper import map_provider_frame
from stock_selector.storage.atomic_writer import AtomicObjectWriter, write_parquet_local_atomic
from stock_selector.storage.minio_client import create_minio_client, ensure_buckets
from stock_selector.storage.partition import PROVIDER_DATASETS, SMOKE_ONLY_DATASETS, build_partition, validate_provider_dataset, validate_provider_smoke_dataset
from stock_selector.utils.date_validator import validate_trade_date


WriteDatasetFn = Callable[[str, str, pd.DataFrame], str]


def update_provider_data(
    trade_date: str,
    provider_name: str = "mock",
    force: bool = False,
    datasets: Iterable[str] | str | None = None,
    step_prefix: str = "provider_data",
    update_log_repo: UpdateLogRepository | None = None,
    write_dataset_fn: WriteDatasetFn | None = None,
    settings: dict[str, Any] | None = None,
    allow_smoke_datasets: bool = False,
) -> list[dict[str, Any]]:
    trade_date = validate_trade_date(trade_date)
    datasets_to_update = _resolve_provider_datasets(datasets, allow_smoke_datasets=allow_smoke_datasets)
    settings = settings or load_settings()
    provider = create_provider(provider_name, settings=settings)
    repo = update_log_repo or create_update_log_repository()
    writer = write_dataset_fn or _default_write_dataset

    results = []
    for dataset in datasets_to_update:
        step_name = f"{step_prefix}:{dataset}"
        if not repo.should_run_step(trade_date, step_name, force=force):
            results.append({"dataset": dataset, "step_name": step_name, "status": "skipped"})
            continue

        try:
            repo.mark_step_running(trade_date, step_name)
            raw_df = provider.fetch_dataset(dataset, trade_date)
            mapped_df = raw_df.copy() if dataset in SMOKE_ONLY_DATASETS else map_provider_frame(provider.name, dataset, raw_df, trade_date)
            object_key = writer(dataset, trade_date, mapped_df)
            repo.mark_step_done(trade_date, step_name, object_key)
            results.append({"dataset": dataset, "step_name": step_name, "status": "done", "object_key": object_key})
        except Exception as exc:
            repo.mark_step_failed(trade_date, step_name, str(exc))
            raise
    return results


def _resolve_provider_datasets(datasets: Iterable[str] | str | None, allow_smoke_datasets: bool = False) -> list[str]:
    if datasets is None:
        return list(PROVIDER_DATASETS)
    if isinstance(datasets, str):
        datasets = [datasets]
    validator = validate_provider_smoke_dataset if allow_smoke_datasets else validate_provider_dataset
    return [validator(dataset) for dataset in datasets]


def _default_write_dataset(dataset: str, trade_date: str, df: pd.DataFrame) -> str:
    settings = load_settings()
    partition = build_partition(dataset, trade_date, local_root=_local_root(settings))
    if _storage_backend(settings) == "local":
        write_parquet_local_atomic(df, partition.local_path)
        return partition.local_path.as_posix()

    minio_client = create_minio_client(settings)
    bucket = settings["storage"]["minio_bucket_raw"]
    ensure_buckets(minio_client, [bucket])
    with tempfile.TemporaryDirectory(prefix="stock-provider-write-") as tmp:
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


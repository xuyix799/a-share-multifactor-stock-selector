from dataclasses import dataclass
from pathlib import Path
import re

from stock_selector.utils.date_validator import validate_trade_date


RAW_DATASETS = (
    "stock_basic",
    "daily_price",
    "adj_factor",
    "daily_basic",
    "financial",
    "st_history",
    "benchmark_price",
)

DERIVED_DATASETS = (
    "adjusted_price",
    "clean_daily_snapshot",
    "risk_filter",
    "eligible_universe",
    "factor_input_table",
    "factor_daily",
    "selection_result",
)

PROVIDER_DATASETS = RAW_DATASETS
SMOKE_ONLY_DATASETS = (
    "daily_price_raw_smoke",
)
PROVIDER_SMOKE_DATASETS = PROVIDER_DATASETS + SMOKE_ONLY_DATASETS
SUPPORTED_DATASETS = RAW_DATASETS + DERIVED_DATASETS
PROCESSED_PREFIX_DATASETS = {"selection_result"}


class DatasetValidationError(ValueError):
    pass


PROVIDER_NAME_PATTERN = re.compile(r"^[a-z0-9_-]+$")


@dataclass(frozen=True)
class DatasetPartition:
    dataset: str
    trade_date: str
    local_path: Path
    object_key: str
    tmp_prefix: str


def validate_dataset(dataset: str) -> str:
    if not isinstance(dataset, str) or dataset not in SUPPORTED_DATASETS:
        raise DatasetValidationError(f"unsupported dataset: {dataset}")
    return dataset


def validate_provider_dataset(dataset: str) -> str:
    if dataset not in PROVIDER_DATASETS:
        raise DatasetValidationError(f"unsupported provider dataset: {dataset}")
    return dataset


def validate_provider_smoke_dataset(dataset: str) -> str:
    if not isinstance(dataset, str) or dataset not in PROVIDER_SMOKE_DATASETS:
        raise DatasetValidationError(f"unsupported provider smoke dataset: {dataset}")
    return dataset


def build_partition(dataset: str, trade_date: str, local_root: str | Path = "data") -> DatasetPartition:
    dataset = validate_dataset(dataset)
    trade_date = validate_trade_date(trade_date)
    prefix = _storage_prefix(dataset)
    local_path = Path(local_root) / prefix / dataset / f"trade_date={trade_date}" / "part.parquet"
    object_key = f"{prefix}/{dataset}/trade_date={trade_date}/part.parquet"
    tmp_prefix = f"_{prefix}_tmp/{dataset}/trade_date={trade_date}"
    return DatasetPartition(
        dataset=dataset,
        trade_date=trade_date,
        local_path=local_path,
        object_key=object_key,
        tmp_prefix=tmp_prefix,
    )


def build_provider_smoke_partition(provider_name: str, dataset: str, trade_date: str, local_root: str | Path = "data") -> DatasetPartition:
    provider_name = _validate_provider_name(provider_name)
    dataset = validate_provider_smoke_dataset(dataset)
    trade_date = validate_trade_date(trade_date)
    local_path = Path(local_root) / "smoke" / provider_name / dataset / f"trade_date={trade_date}" / "part.parquet"
    object_key = f"smoke/{provider_name}/{dataset}/trade_date={trade_date}/part.parquet"
    tmp_prefix = f"_smoke_tmp/{provider_name}/{dataset}/trade_date={trade_date}"
    return DatasetPartition(
        dataset=dataset,
        trade_date=trade_date,
        local_path=local_path,
        object_key=object_key,
        tmp_prefix=tmp_prefix,
    )


def _storage_prefix(dataset: str) -> str:
    return "processed" if dataset in PROCESSED_PREFIX_DATASETS else "raw"


def _validate_provider_name(provider_name: str) -> str:
    if not isinstance(provider_name, str) or not PROVIDER_NAME_PATTERN.fullmatch(provider_name):
        raise DatasetValidationError(f"unsupported provider name: {provider_name}")
    return provider_name

from dataclasses import dataclass
from pathlib import Path

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
SUPPORTED_DATASETS = RAW_DATASETS + DERIVED_DATASETS


class DatasetValidationError(ValueError):
    pass


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


def build_partition(dataset: str, trade_date: str, local_root: str | Path = "data") -> DatasetPartition:
    dataset = validate_dataset(dataset)
    trade_date = validate_trade_date(trade_date)
    local_path = Path(local_root) / "raw" / dataset / f"trade_date={trade_date}" / "part.parquet"
    object_key = f"raw/{dataset}/trade_date={trade_date}/part.parquet"
    tmp_prefix = f"_raw_tmp/{dataset}/trade_date={trade_date}"
    return DatasetPartition(
        dataset=dataset,
        trade_date=trade_date,
        local_path=local_path,
        object_key=object_key,
        tmp_prefix=tmp_prefix,
    )

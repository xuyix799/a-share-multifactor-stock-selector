import pytest

from stock_selector.storage.partition import DatasetValidationError, build_partition, validate_dataset


def test_validate_dataset_accepts_whitelisted_dataset():
    assert validate_dataset("daily_price") == "daily_price"


def test_validate_dataset_accepts_goal5_universe_datasets():
    assert validate_dataset("risk_filter") == "risk_filter"
    assert validate_dataset("eligible_universe") == "eligible_universe"
    assert validate_dataset("factor_input_table") == "factor_input_table"


def test_validate_dataset_accepts_goal6_factor_daily_dataset():
    assert validate_dataset("factor_daily") == "factor_daily"


def test_validate_dataset_accepts_goal7_selection_result_dataset():
    assert validate_dataset("selection_result") == "selection_result"


def test_validate_dataset_rejects_unknown_or_unsafe_dataset():
    with pytest.raises(DatasetValidationError):
        validate_dataset("../daily_price")
    with pytest.raises(DatasetValidationError):
        validate_dataset("minute_price")


def test_build_partition_returns_local_path_object_key_and_tmp_prefix():
    partition = build_partition("daily_price", "2026-06-19", local_root="data")

    assert partition.dataset == "daily_price"
    assert partition.trade_date == "2026-06-19"
    assert partition.local_path.as_posix() == "data/raw/daily_price/trade_date=2026-06-19/part.parquet"
    assert partition.object_key == "raw/daily_price/trade_date=2026-06-19/part.parquet"
    assert partition.tmp_prefix == "_raw_tmp/daily_price/trade_date=2026-06-19"


def test_build_partition_uses_processed_prefix_for_selection_result():
    partition = build_partition("selection_result", "2026-06-19", local_root="data")

    assert partition.local_path.as_posix() == "data/processed/selection_result/trade_date=2026-06-19/part.parquet"
    assert partition.object_key == "processed/selection_result/trade_date=2026-06-19/part.parquet"
    assert partition.tmp_prefix == "_processed_tmp/selection_result/trade_date=2026-06-19"

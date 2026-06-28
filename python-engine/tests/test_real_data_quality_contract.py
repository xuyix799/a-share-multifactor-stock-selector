import pandas as pd
import pytest

from stock_selector.data.data_validator import DataValidationError, validate_dataset_frame
from stock_selector.data.quality_contract import (
    BacktestMode,
    DataQualityLevel,
    can_promote_to_daily_price,
    can_use_in_price_only_diagnostic,
    can_use_in_strict_tradable_required,
    classify_provider_dataset,
    get_backtest_mode_contract,
)
from stock_selector.providers.schema_contract import get_schema_contract


DAILY_PRICE_FIELDS = {
    "stock_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "volume",
    "amount",
    "is_paused",
    "limit_up",
    "limit_down",
}


def test_dq1_and_dq2_cannot_promote_to_daily_price():
    assert can_promote_to_daily_price(DataQualityLevel.DQ1, DAILY_PRICE_FIELDS) is False
    assert can_promote_to_daily_price(DataQualityLevel.DQ2, DAILY_PRICE_FIELDS) is False


def test_strict_tradable_rejects_missing_limit_and_pause_fields():
    fields = DAILY_PRICE_FIELDS - {"limit_up", "limit_down", "is_paused"}

    assert can_use_in_strict_tradable_required(DataQualityLevel.DQ3, "daily_price", fields) is False


def test_akshare_stock_daily_is_dq1_non_tradable_raw_only():
    contract = classify_provider_dataset("akshare", "daily_price_raw_smoke")

    assert contract.provider_name == "akshare"
    assert contract.dataset == "daily_price_raw_smoke"
    assert contract.dq_level == DataQualityLevel.DQ1
    assert contract.non_tradable is True
    assert "daily_price_raw" in contract.allowed_datasets
    assert "daily_price" not in contract.allowed_datasets
    assert contract.allowed_backtest_modes == ()
    assert "limit_up" in contract.reason
    assert "limit_down" in contract.reason
    assert "is_paused" in contract.reason


def test_baostock_stock_daily_is_dq1_even_if_login_later_succeeds():
    contract = classify_provider_dataset("baostock", "daily_price")

    assert contract.dq_level == DataQualityLevel.DQ1
    assert contract.non_tradable is True
    assert "daily_price_raw_smoke" in contract.allowed_datasets
    assert "daily_price" not in contract.allowed_datasets
    assert "login succeeds" in contract.reason
    assert "limit_up" in contract.reason
    assert "limit_down" in contract.reason
    assert "is_paused" in contract.reason


def test_akshare_benchmark_price_is_dq2_price_only_diagnostic():
    contract = classify_provider_dataset("akshare", "benchmark_price")

    assert contract.dq_level == DataQualityLevel.DQ2
    assert contract.non_tradable is True
    assert contract.allowed_datasets == ("benchmark_price",)
    assert contract.allowed_backtest_modes == (BacktestMode.PRICE_ONLY_DIAGNOSTIC,)
    assert can_use_in_price_only_diagnostic(contract.dq_level, contract.dataset) is True
    assert can_promote_to_daily_price(contract.dq_level, DAILY_PRICE_FIELDS) is False


def test_strict_tradable_accepts_only_dq3_or_dq4_daily_price_with_trade_fields():
    assert can_use_in_strict_tradable_required(DataQualityLevel.DQ3, "daily_price", DAILY_PRICE_FIELDS) is True
    assert can_use_in_strict_tradable_required(DataQualityLevel.DQ4, "daily_price", DAILY_PRICE_FIELDS) is True
    assert can_use_in_strict_tradable_required(DataQualityLevel.DQ2, "daily_price", DAILY_PRICE_FIELDS) is False
    assert can_use_in_strict_tradable_required(DataQualityLevel.DQ3, "benchmark_price", DAILY_PRICE_FIELDS) is False


def test_price_only_diagnostic_contract_is_marked_non_tradable():
    contract = get_backtest_mode_contract(BacktestMode.PRICE_ONLY_DIAGNOSTIC)

    assert contract.mode == BacktestMode.PRICE_ONLY_DIAGNOSTIC
    assert contract.allowed_dq_levels == (DataQualityLevel.DQ2,)
    assert contract.allowed_datasets == ("benchmark_price",)
    assert contract.tradable is False
    assert contract.result_label == "diagnostic_non_tradable"


def test_tushare_requires_current_goal10r_matrix_and_pause_source_before_dq3():
    contract = classify_provider_dataset("tushare", "daily_price")

    assert contract.dq_level == DataQualityLevel.DQ1
    assert contract.non_tradable is True
    assert contract.strict_tradable_ready is False
    assert "Goal 10R capability matrix" in contract.reason
    assert "stk_limit" in contract.reason
    assert "adj_factor" in contract.reason
    assert "daily_basic" in contract.reason
    assert "is_paused" in contract.reason


def test_daily_price_schema_contract_keeps_trade_constraint_fields():
    contract = get_schema_contract("daily_price")

    assert "pre_close" in contract.columns
    assert "is_paused" in contract.columns
    assert "limit_up" in contract.columns
    assert "limit_down" in contract.columns
    assert "is_paused" in contract.bool_columns
    assert "limit_up" in contract.numeric_columns
    assert "limit_down" in contract.numeric_columns


def test_daily_price_validator_still_rejects_missing_trade_constraints():
    raw_like_daily = pd.DataFrame(
        [
            {
                "stock_code": "000001.SZ",
                "trade_date": "2026-06-19",
                "open": 10.0,
                "high": 10.3,
                "low": 9.9,
                "close": 10.2,
                "pre_close": 10.0,
                "volume": 1000.0,
                "amount": 10200.0,
                "pct_chg": 2.0,
            }
        ]
    )

    with pytest.raises(DataValidationError, match="missing columns: is_paused, limit_up, limit_down"):
        validate_dataset_frame("daily_price", raw_like_daily, "2026-06-19")

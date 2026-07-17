import pytest
import pandas as pd

from stock_selector.cleaning.snapshot_builder import build_clean_daily_snapshot
from stock_selector.data.data_validator import DataValidationError
from stock_selector.data.mock_data import generate_mock_dataset
from stock_selector.universe.risk_filter import build_risk_filter
from stock_selector.universe.universe_builder import build_eligible_universe, build_factor_input_table
from stock_selector.universe.universe_validator import (
    ELIGIBLE_UNIVERSE_COLUMNS,
    FACTOR_INPUT_TABLE_COLUMNS,
    validate_eligible_universe,
    validate_factor_input_table,
    validate_risk_filter,
)


def _clean_snapshot(trade_date: str):
    return build_clean_daily_snapshot(
        stock_basic=generate_mock_dataset("stock_basic", trade_date),
        daily_price=generate_mock_dataset("daily_price", trade_date),
        adj_factor=generate_mock_dataset("adj_factor", trade_date),
        daily_basic=generate_mock_dataset("daily_basic", trade_date),
        financial=generate_mock_dataset("financial", trade_date),
        st_history=generate_mock_dataset("st_history", trade_date),
        benchmark_price=generate_mock_dataset("benchmark_price", trade_date),
        trade_date=trade_date,
    )


def test_universe_validators_accept_goal5_tables():
    trade_date = "2026-06-19"
    snapshot = _clean_snapshot(trade_date)
    risk_filter = build_risk_filter(snapshot, trade_date)
    eligible = build_eligible_universe(snapshot, risk_filter, trade_date)
    factor_input = build_factor_input_table(snapshot, eligible, trade_date)

    validate_risk_filter(risk_filter, trade_date)
    validate_eligible_universe(eligible, trade_date)
    validate_factor_input_table(factor_input, trade_date)


def test_risk_filter_validator_rejects_missing_reason_column():
    trade_date = "2026-06-19"
    snapshot = _clean_snapshot(trade_date)
    risk_filter = build_risk_filter(snapshot, trade_date).drop(columns=["exclude_reasons"])

    with pytest.raises(DataValidationError):
        validate_risk_filter(risk_filter, trade_date)


def test_universe_validators_accept_schema_complete_empty_downstream_tables():
    trade_date = "2026-06-19"
    eligible = pd.DataFrame(columns=ELIGIBLE_UNIVERSE_COLUMNS)
    factor_input = pd.DataFrame(columns=FACTOR_INPUT_TABLE_COLUMNS)

    validate_eligible_universe(eligible, trade_date)
    validate_factor_input_table(factor_input, trade_date)

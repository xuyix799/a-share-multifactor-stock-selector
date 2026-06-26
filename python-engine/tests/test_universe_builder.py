from stock_selector.cleaning.snapshot_builder import build_clean_daily_snapshot
from stock_selector.data.mock_data import generate_mock_dataset
from stock_selector.universe.risk_filter import build_risk_filter
from stock_selector.universe.universe_builder import (
    ELIGIBLE_UNIVERSE_COLUMNS,
    FACTOR_INPUT_TABLE_COLUMNS,
    build_eligible_universe,
    build_factor_input_table,
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


def test_eligible_universe_contains_only_eligible_stocks():
    trade_date = "2026-06-19"
    snapshot = _clean_snapshot(trade_date)
    risk_filter = build_risk_filter(snapshot, trade_date)

    eligible = build_eligible_universe(snapshot, risk_filter, trade_date)

    assert list(eligible.columns) == ELIGIBLE_UNIVERSE_COLUMNS
    assert set(eligible["stock_code"]) == set(risk_filter.loc[risk_filter["is_eligible"], "stock_code"])
    assert "000002.SZ" not in set(eligible["stock_code"])


def test_factor_input_table_contains_only_eligible_stocks_and_no_scores():
    trade_date = "2026-06-19"
    snapshot = _clean_snapshot(trade_date)
    risk_filter = build_risk_filter(snapshot, trade_date)
    eligible = build_eligible_universe(snapshot, risk_filter, trade_date)

    factor_input = build_factor_input_table(snapshot, eligible, trade_date)

    assert list(factor_input.columns) == FACTOR_INPUT_TABLE_COLUMNS
    assert set(factor_input["stock_code"]) == set(eligible["stock_code"])
    assert all("score" not in column for column in factor_input.columns)

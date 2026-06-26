import pandas as pd

from stock_selector.cleaning.snapshot_builder import build_clean_daily_snapshot
from stock_selector.data.mock_data import generate_mock_dataset
from stock_selector.universe.risk_filter import RISK_FILTER_COLUMNS, build_risk_filter


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


def test_risk_filter_records_all_hard_filter_reasons():
    trade_date = "2026-06-19"
    snapshot = _clean_snapshot(trade_date)
    snapshot["amount"] = 100_000_000
    snapshot.loc[snapshot["stock_code"] == "000001.SZ", "roe"] = pd.NA
    snapshot.loc[snapshot["stock_code"] == "600000.SH", "is_paused"] = True
    snapshot.loc[snapshot["stock_code"] == "600519.SH", "listed_days"] = 10
    snapshot.loc[snapshot["stock_code"] == "600519.SH", "amount"] = 1_000_000
    snapshot.loc[snapshot["stock_code"] == "300750.SZ", "roe"] = 0.01
    snapshot.loc[snapshot["stock_code"] == "300750.SZ", "debt_ratio"] = 0.90

    result = build_risk_filter(snapshot, trade_date)

    assert list(result.columns) == RISK_FILTER_COLUMNS
    assert len(result) == len(snapshot)
    by_code = result.set_index("stock_code")
    assert "FINANCIAL_MISSING" in by_code.loc["000001.SZ", "exclude_reasons"].split(";")
    assert "ST" in by_code.loc["000002.SZ", "exclude_reasons"].split(";")
    assert by_code.loc["600000.SH", "exclude_reasons"] == "PAUSED"
    assert by_code.loc["600519.SH", "exclude_reasons"] == "LISTED_DAYS_LT_MIN;AMOUNT_LT_MIN"
    assert by_code.loc["300750.SZ", "exclude_reasons"] == "ROE_LT_MIN;DEBT_RATIO_GT_MAX"
    assert by_code.loc["000002.SZ", "risk_flags"] == by_code.loc["000002.SZ", "exclude_reasons"]
    assert by_code.loc["000002.SZ", "is_eligible"] is False


def test_risk_filter_keeps_eligible_rows_with_empty_reason_strings():
    trade_date = "2026-06-19"
    snapshot = _clean_snapshot(trade_date)
    snapshot["amount"] = 100_000_000

    result = build_risk_filter(snapshot, trade_date)

    by_code = result.set_index("stock_code")
    assert by_code.loc["000001.SZ", "is_eligible"] is True
    assert by_code.loc["000001.SZ", "exclude_reasons"] == ""
    assert by_code.loc["000001.SZ", "risk_flags"] == ""

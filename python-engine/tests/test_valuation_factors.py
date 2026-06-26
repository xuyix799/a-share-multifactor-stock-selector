import pytest

from factor_test_helpers import clean_snapshot_history, factor_input_frame
from stock_selector.factors.valuation_factors import build_valuation_factors


def test_valuation_factors_use_available_history_and_ignore_future_rows():
    trade_date = "2026-06-19"
    factor_input = factor_input_frame(trade_date)
    clean_history = clean_snapshot_history(trade_date, days=2, include_future=True)

    result = build_valuation_factors(factor_input, clean_history, trade_date)

    by_code = result.set_index("stock_code")
    assert by_code.loc["000001.SZ", "valuation_pe_ttm"] == 10.0
    assert by_code.loc["000001.SZ", "valuation_pb"] == 1.0
    assert by_code.loc["000001.SZ", "valuation_ps_ttm"] == 2.0
    assert by_code.loc["000001.SZ", "valuation_pe_percentile_3y"] == pytest.approx(1 / 2)
    assert by_code.loc["000001.SZ", "valuation_pb_percentile_3y"] == pytest.approx(1 / 2)

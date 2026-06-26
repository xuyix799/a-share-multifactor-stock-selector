import pandas as pd
import pytest

from factor_test_helpers import adjusted_price_history, factor_input_frame
from stock_selector.factors.trend_factors import build_trend_factors


def test_trend_factors_use_adjusted_price_history_without_future_rows():
    trade_date = "2026-06-19"
    factor_input = factor_input_frame(trade_date)
    adjusted_history = adjusted_price_history(trade_date, days=130, include_future=True)

    result = build_trend_factors(factor_input, adjusted_history, trade_date)

    by_code = result.set_index("stock_code")
    assert by_code.loc["000001.SZ", "trend_ret_20d"] == pytest.approx(149 / 129 - 1)
    assert by_code.loc["000001.SZ", "trend_ma60"] == pytest.approx(sum(range(90, 150)) / 60)
    assert by_code.loc["000001.SZ", "trend_price_ma60_ratio"] == pytest.approx(149 / (sum(range(90, 150)) / 60))


def test_trend_factors_return_nulls_when_history_is_short():
    trade_date = "2026-06-19"
    factor_input = factor_input_frame(trade_date)
    adjusted_history = adjusted_price_history(trade_date, days=10)

    result = build_trend_factors(factor_input, adjusted_history, trade_date)

    row = result.set_index("stock_code").loc["000001.SZ"]
    assert pd.isna(row["trend_ret_20d"])
    assert pd.isna(row["trend_ma20"])

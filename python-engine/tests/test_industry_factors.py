import pandas as pd
import pytest

from factor_test_helpers import adjusted_price_history, benchmark_price_history, factor_input_frame
from stock_selector.factors.industry_factors import build_industry_factors


def test_industry_factors_compare_industry_return_with_benchmark():
    trade_date = "2026-06-19"
    factor_input = factor_input_frame(trade_date)
    adjusted_history = adjusted_price_history(trade_date, days=130)
    benchmark_history = benchmark_price_history(trade_date, days=130)

    result = build_industry_factors(factor_input, adjusted_history, benchmark_history, trade_date)

    bank = result.loc[result["industry"] == "银行"].iloc[0]
    expected_bank_ret_60d = ((149 / 89 - 1) + (159 / 99 - 1)) / 2
    expected_benchmark_ret_60d = 4129 / 4069 - 1
    assert bank["industry_ret_60d"] == pytest.approx(expected_bank_ret_60d)
    assert bank["industry_strength_60d"] == pytest.approx(expected_bank_ret_60d - expected_benchmark_ret_60d)


def test_industry_strength_is_null_when_benchmark_history_is_short():
    trade_date = "2026-06-19"
    factor_input = factor_input_frame(trade_date)
    adjusted_history = adjusted_price_history(trade_date, days=130)
    benchmark_history = benchmark_price_history(trade_date, days=10)

    result = build_industry_factors(factor_input, adjusted_history, benchmark_history, trade_date)

    assert result["industry_ret_60d"].notna().all()
    assert result["industry_strength_60d"].isna().all()
    assert result["industry_strength_120d"].isna().all()

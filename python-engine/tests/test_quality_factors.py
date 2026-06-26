import pandas as pd

from factor_test_helpers import factor_input_frame
from stock_selector.factors.quality_factors import build_quality_factors


def test_quality_factors_copy_asof_inputs_and_leave_cashflow_ratio_null():
    factor_input = factor_input_frame()

    result = build_quality_factors(factor_input, "2026-06-19")

    by_code = result.set_index("stock_code")
    assert by_code.loc["000001.SZ", "quality_roe"] == 0.10
    assert by_code.loc["000001.SZ", "quality_gross_margin"] == 0.25
    assert by_code.loc["000001.SZ", "quality_debt_ratio"] == 0.40
    assert pd.isna(by_code.loc["000001.SZ", "quality_cashflow_profit_ratio"])

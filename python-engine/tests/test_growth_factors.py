from factor_test_helpers import factor_input_frame
from stock_selector.factors.growth_factors import build_growth_factors


def test_growth_factors_copy_current_asof_growth_fields():
    factor_input = factor_input_frame()

    result = build_growth_factors(factor_input, "2026-06-19")

    by_code = result.set_index("stock_code")
    assert by_code.loc["000001.SZ", "growth_revenue_yoy"] == 0.08
    assert by_code.loc["000001.SZ", "growth_net_profit_yoy"] == 0.06

from factor_test_helpers import adjusted_price_history, benchmark_price_history, clean_snapshot_history, factor_input_frame
from stock_selector.factors.factor_builder import build_factor_daily


def test_factor_daily_does_not_use_future_price_or_valuation_rows():
    trade_date = "2026-06-19"
    factor_input = factor_input_frame(trade_date)
    without_future = build_factor_daily(
        factor_input_table=factor_input,
        adjusted_price_history=adjusted_price_history(trade_date, days=130, include_future=False),
        clean_snapshot_history=clean_snapshot_history(trade_date, days=5, include_future=False),
        benchmark_price_history=benchmark_price_history(trade_date, days=130),
        trade_date=trade_date,
        factor_weights={},
    )
    with_future = build_factor_daily(
        factor_input_table=factor_input,
        adjusted_price_history=adjusted_price_history(trade_date, days=130, include_future=True),
        clean_snapshot_history=clean_snapshot_history(trade_date, days=5, include_future=True),
        benchmark_price_history=benchmark_price_history(trade_date, days=130),
        trade_date=trade_date,
        factor_weights={},
    )

    columns = ["trend_ret_60d", "trend_ma60", "valuation_pe_percentile_3y", "valuation_pb_percentile_3y"]
    assert without_future[columns].round(10).equals(with_future[columns].round(10))

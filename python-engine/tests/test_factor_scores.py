from factor_test_helpers import adjusted_price_history, benchmark_price_history, clean_snapshot_history, factor_input_frame
from stock_selector.factors.factor_builder import build_factor_daily


def test_factor_scores_are_bounded_and_no_total_score_is_created():
    trade_date = "2026-06-19"

    result = build_factor_daily(
        factor_input_table=factor_input_frame(trade_date),
        adjusted_price_history=adjusted_price_history(trade_date, days=130),
        clean_snapshot_history=clean_snapshot_history(trade_date, days=5),
        benchmark_price_history=benchmark_price_history(trade_date, days=130),
        trade_date=trade_date,
        factor_weights={"quality_score": 0.3, "growth_score": 0.25, "valuation_score": 0.2, "industry_score": 0.15, "trend_score": 0.1},
    )

    score_columns = ["quality_score", "growth_score", "valuation_score", "trend_score", "industry_score"]
    for column in score_columns:
        assert result[column].between(0, 100).all()
    assert "total_score" not in result.columns

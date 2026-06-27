from selection_test_helpers import eligible_universe_frame, factor_daily_frame, risk_filter_frame
from stock_selector.scoring.score_engine import parse_scoring_config
from stock_selector.scoring.selection_builder import SELECTION_RESULT_COLUMNS, build_selection_result


def _config(top_n=50):
    return parse_scoring_config(
        {
            "quality_score": 0.30,
            "growth_score": 0.25,
            "valuation_score": 0.20,
            "industry_score": 0.15,
            "trend_score": 0.10,
            "scoring": {"null_score_policy": "neutral", "neutral_score": 50, "top_n": top_n},
        }
    )


def test_selection_result_contains_only_eligible_stocks_and_expected_columns():
    trade_date = "2026-06-19"

    result = build_selection_result(
        factor_daily=factor_daily_frame(trade_date),
        risk_filter=risk_filter_frame(trade_date),
        eligible_universe=eligible_universe_frame(trade_date),
        factor_input_table=None,
        trade_date=trade_date,
        scoring_config=_config(),
    )

    assert list(result.columns) == SELECTION_RESULT_COLUMNS
    assert set(result["stock_code"]) == {"000001.SZ", "600519.SH"}
    assert "600000.SH" not in set(result["stock_code"])


def test_selection_result_sorts_by_total_score_desc_and_ranks_continuously():
    trade_date = "2026-06-19"

    result = build_selection_result(
        factor_daily=factor_daily_frame(trade_date),
        risk_filter=risk_filter_frame(trade_date),
        eligible_universe=eligible_universe_frame(trade_date),
        factor_input_table=None,
        trade_date=trade_date,
        scoring_config=_config(),
    )

    assert result["total_score"].tolist() == sorted(result["total_score"].tolist(), reverse=True)
    assert result["rank"].tolist() == [1, 2]
    assert result.iloc[0]["stock_code"] == "600519.SH"


def test_selection_result_applies_top_n_after_ranking_order():
    trade_date = "2026-06-19"

    result = build_selection_result(
        factor_daily=factor_daily_frame(trade_date),
        risk_filter=risk_filter_frame(trade_date),
        eligible_universe=eligible_universe_frame(trade_date),
        factor_input_table=None,
        trade_date=trade_date,
        scoring_config=_config(top_n=1),
    )

    assert len(result) == 1
    assert result.iloc[0]["rank"] == 1
    assert result.iloc[0]["stock_code"] == "600519.SH"

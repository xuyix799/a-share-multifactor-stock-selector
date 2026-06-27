import pandas as pd
import pytest

from selection_test_helpers import eligible_universe_frame, factor_daily_frame, risk_filter_frame
from stock_selector.data.data_validator import DataValidationError, validate_dataset_frame
from stock_selector.scoring.score_engine import parse_scoring_config
from stock_selector.scoring.selection_builder import build_selection_result
from stock_selector.scoring.selection_validator import validate_selection_result


def _selection(trade_date="2026-06-19"):
    return build_selection_result(
        factor_daily=factor_daily_frame(trade_date),
        risk_filter=risk_filter_frame(trade_date),
        eligible_universe=eligible_universe_frame(trade_date),
        factor_input_table=None,
        trade_date=trade_date,
        scoring_config=parse_scoring_config(
            {
                "quality_score": 0.30,
                "growth_score": 0.25,
                "valuation_score": 0.20,
                "industry_score": 0.15,
                "trend_score": 0.10,
            }
        ),
    )


def test_validate_selection_result_accepts_valid_frame():
    validate_selection_result(_selection(), "2026-06-19")
    validate_dataset_frame("selection_result", _selection(), "2026-06-19")


def test_validate_selection_result_rejects_forbidden_words():
    df = _selection()
    df.loc[0, "suggestion"] = "无脑买入"

    with pytest.raises(DataValidationError, match="forbidden"):
        validate_selection_result(df, "2026-06-19")


def test_validate_selection_result_rejects_non_continuous_rank():
    df = _selection()
    df.loc[df.index[-1], "rank"] = 10

    with pytest.raises(DataValidationError, match="rank"):
        validate_selection_result(df, "2026-06-19")


def test_validate_selection_result_rejects_unsorted_scores():
    df = _selection()
    df.loc[0, "total_score"] = 0.0

    with pytest.raises(DataValidationError, match="descending"):
        validate_selection_result(df, "2026-06-19")

import math

import pandas as pd
import pytest

from stock_selector.scoring.score_engine import ScoringConfigError, compute_total_scores, parse_scoring_config


def test_factor_weights_must_sum_to_one():
    with pytest.raises(ScoringConfigError, match="sum"):
        parse_scoring_config(
            {
                "quality_score": 0.30,
                "growth_score": 0.25,
                "valuation_score": 0.20,
                "industry_score": 0.15,
                "trend_score": 0.05,
            }
        )


def test_fractional_top_n_is_rejected_instead_of_truncated():
    with pytest.raises(ScoringConfigError, match="positive integer"):
        parse_scoring_config(
            {
                "quality_score": 0.30,
                "growth_score": 0.25,
                "valuation_score": 0.20,
                "industry_score": 0.15,
                "trend_score": 0.10,
                "scoring": {"top_n": 50.9},
            }
        )


def test_total_score_uses_configured_weights():
    config = parse_scoring_config(
        {
            "quality_score": 0.30,
            "growth_score": 0.25,
            "valuation_score": 0.20,
            "industry_score": 0.15,
            "trend_score": 0.10,
        }
    )
    df = pd.DataFrame(
        [
            {
                "quality_score": 80,
                "growth_score": 70,
                "valuation_score": 60,
                "industry_score": 50,
                "trend_score": 40,
            }
        ]
    )

    result = compute_total_scores(df, config)

    assert math.isclose(result.loc[0, "total_score"], 65.0)


def test_null_sub_scores_use_neutral_score():
    config = parse_scoring_config(
        {
            "quality_score": 0.30,
            "growth_score": 0.25,
            "valuation_score": 0.20,
            "industry_score": 0.15,
            "trend_score": 0.10,
            "scoring": {"null_score_policy": "neutral", "neutral_score": 50, "top_n": 50},
        }
    )
    df = pd.DataFrame(
        [
            {
                "quality_score": None,
                "growth_score": 70,
                "valuation_score": 60,
                "industry_score": 50,
                "trend_score": 40,
            }
        ]
    )

    result = compute_total_scores(df, config)

    assert math.isclose(result.loc[0, "total_score"], 56.0)


def test_total_score_is_clipped_to_zero_to_one_hundred():
    config = parse_scoring_config(
        {
            "quality_score": 0.30,
            "growth_score": 0.25,
            "valuation_score": 0.20,
            "industry_score": 0.15,
            "trend_score": 0.10,
        }
    )
    df = pd.DataFrame(
        [
            {
                "quality_score": 200,
                "growth_score": 200,
                "valuation_score": 200,
                "industry_score": 200,
                "trend_score": 200,
            },
            {
                "quality_score": -20,
                "growth_score": -20,
                "valuation_score": -20,
                "industry_score": -20,
                "trend_score": -20,
            },
        ]
    )

    result = compute_total_scores(df, config)

    assert result["total_score"].tolist() == [100.0, 0.0]

import pandas as pd


SCORE_COLUMNS = ["quality_score", "growth_score", "valuation_score", "trend_score", "industry_score"]


def add_factor_scores(factors: pd.DataFrame, factor_weights: dict[str, float] | None = None) -> pd.DataFrame:
    _ = factor_weights or {}
    result = factors.copy()
    result["quality_score"] = _mean_score(
        [
            _scale_positive(result["quality_roe"], 0.03, 0.20),
            _scale_positive(result["quality_gross_margin"], 0.10, 0.60),
            _scale_inverse(result["quality_debt_ratio"], 0.20, 0.80),
        ]
    )
    result["growth_score"] = _mean_score(
        [
            _scale_positive(result["growth_revenue_yoy"], -0.20, 0.30),
            _scale_positive(result["growth_net_profit_yoy"], -0.20, 0.30),
        ]
    )
    result["valuation_score"] = _mean_score(
        [
            _scale_inverse(result["valuation_pe_ttm"], 5.0, 60.0),
            _scale_inverse(result["valuation_pb"], 0.5, 8.0),
            _scale_inverse(result["valuation_ps_ttm"], 0.5, 15.0),
            _scale_inverse(result["valuation_pe_percentile_3y"], 0.0, 1.0),
            _scale_inverse(result["valuation_pb_percentile_3y"], 0.0, 1.0),
        ]
    )
    result["trend_score"] = _mean_score(
        [
            _scale_positive(result["trend_ret_20d"], -0.20, 0.40),
            _scale_positive(result["trend_ret_60d"], -0.30, 0.80),
            _scale_positive(result["trend_ret_120d"], -0.40, 1.20),
            _scale_positive(result["trend_price_ma60_ratio"], 0.70, 1.30),
        ]
    )
    result["industry_score"] = _mean_score(
        [
            _scale_positive(result["industry_ret_60d"], -0.30, 0.80),
            _scale_positive(result["industry_ret_120d"], -0.40, 1.20),
            _scale_positive(result["industry_strength_60d"], -0.20, 0.20),
            _scale_positive(result["industry_strength_120d"], -0.30, 0.30),
        ]
    )
    for column in SCORE_COLUMNS:
        result[column] = result[column].clip(0, 100).fillna(50.0)
    return result


def _scale_positive(values, low: float, high: float) -> pd.Series:
    series = pd.to_numeric(values, errors="coerce")
    return ((series - low) / (high - low) * 100).clip(0, 100)


def _scale_inverse(values, low: float, high: float) -> pd.Series:
    return 100 - _scale_positive(values, low, high)


def _mean_score(parts: list[pd.Series]) -> pd.Series:
    if not parts:
        return pd.Series(dtype=float)
    frame = pd.concat(parts, axis=1)
    return frame.mean(axis=1, skipna=True)

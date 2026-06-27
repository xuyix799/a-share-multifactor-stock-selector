from dataclasses import dataclass
from typing import Any

import pandas as pd


SCORE_WEIGHT_COLUMNS = ["quality_score", "growth_score", "valuation_score", "industry_score", "trend_score"]


class ScoringConfigError(ValueError):
    pass


@dataclass(frozen=True)
class ScoringConfig:
    weights: dict[str, float]
    null_score_policy: str = "neutral"
    neutral_score: float = 50.0
    top_n: int = 50


def parse_scoring_config(raw_config: dict[str, Any]) -> ScoringConfig:
    weights = {}
    for column in SCORE_WEIGHT_COLUMNS:
        if column not in raw_config:
            raise ScoringConfigError(f"missing weight: {column}")
        weights[column] = float(raw_config[column])

    weight_sum = sum(weights.values())
    if abs(weight_sum - 1.0) > 1e-9:
        raise ScoringConfigError(f"factor weight sum must equal 1, got {weight_sum}")

    scoring = raw_config.get("scoring") or {}
    if not isinstance(scoring, dict):
        raise ScoringConfigError("scoring config must be a mapping")
    null_policy = str(scoring.get("null_score_policy", "neutral"))
    if null_policy != "neutral":
        raise ScoringConfigError(f"unsupported null_score_policy: {null_policy}")
    neutral_score = float(scoring.get("neutral_score", 50.0))
    if neutral_score < 0 or neutral_score > 100:
        raise ScoringConfigError("neutral_score must be between 0 and 100")
    top_n = int(scoring.get("top_n", 50))
    if top_n <= 0:
        raise ScoringConfigError("top_n must be positive")
    return ScoringConfig(weights=weights, null_score_policy=null_policy, neutral_score=neutral_score, top_n=top_n)


def compute_total_scores(df: pd.DataFrame, scoring_config: ScoringConfig) -> pd.DataFrame:
    result = df.copy()
    total = pd.Series(0.0, index=result.index)
    for column, weight in scoring_config.weights.items():
        if column not in result.columns:
            raise ScoringConfigError(f"missing score column: {column}")
        scores = pd.to_numeric(result[column], errors="coerce")
        if scoring_config.null_score_policy == "neutral":
            scores = scores.fillna(scoring_config.neutral_score)
        total = total + scores * weight
    result["total_score"] = total.clip(0, 100).round(6)
    return result

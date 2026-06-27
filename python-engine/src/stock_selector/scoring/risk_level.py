import math
import re
from typing import Any


SEVERE_RISK_FLAGS = {
    "high_debt_ratio",
    "negative_roe",
    "missing_financial",
    "is_st_on_date",
    "is_paused",
    "financial_missing",
    "debt_ratio_gt_max",
    "roe_lt_min",
    "st",
    "paused",
}


def determine_risk_level(*, total_score: Any, quality_score: Any, growth_score: Any, risk_flags: Any) -> str:
    total = _to_float(total_score, 0.0)
    quality = _to_float(quality_score, 0.0)
    growth = _to_float(growth_score, 0.0)
    has_severe_risk = bool(parse_risk_flags(risk_flags) & SEVERE_RISK_FLAGS)

    if total < 55 or quality < 40 or growth < 30 or has_severe_risk:
        return "high"
    if total >= 75 and quality >= 60 and growth >= 50:
        return "low"
    if total >= 55:
        return "medium"
    return "high"


def parse_risk_flags(risk_flags: Any) -> set[str]:
    if risk_flags is None:
        return set()
    try:
        if math.isnan(risk_flags):
            return set()
    except TypeError:
        pass
    text = str(risk_flags).strip()
    if not text:
        return set()
    return {item.strip().lower() for item in re.split(r"[;,]", text) if item.strip()}


def _to_float(value: Any, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(parsed):
        return default
    return parsed

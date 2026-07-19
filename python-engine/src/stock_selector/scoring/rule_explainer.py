from typing import Any


FORBIDDEN_WORDS = [
    "必涨",
    "稳赚",
    "必赚",
    "保证收益",
    "确定收益",
    "承诺收益",
    "满仓",
    "目标价",
    "无脑买入",
    "自动交易",
    "自动下单",
]


def build_reason(row: dict[str, Any]) -> str:
    clauses = []
    if _score(row, "quality_score") >= 70:
        clauses.append("盈利质量较好")
    if _score(row, "growth_score") >= 70:
        clauses.append("成长表现较稳定")
    if _score(row, "valuation_score") >= 70:
        clauses.append("估值相对合理")
    if _score(row, "industry_score") >= 70:
        clauses.append("行业相对强度较高")
    if _score(row, "trend_score") >= 70:
        clauses.append("中期趋势确认较强")
    if _score(row, "valuation_score") < 40:
        clauses.append("估值偏高或估值吸引力不足")
    if _score(row, "trend_score") < 40:
        clauses.append("趋势仍需修复")
    if not clauses:
        clauses.append("综合评分处于中性区间，需继续跟踪基本面、估值和趋势变化")
    return _sanitize("；".join(clauses) + "。")


def build_suggestion(row: dict[str, Any]) -> str:
    total_score = _score(row, "total_score")
    risk_level = str(row.get("risk_level", "high"))
    if total_score >= 80 and risk_level == "low":
        return "可作为中长线重点候选"
    if total_score >= 70 and risk_level in {"low", "medium"}:
        return "观察，等待合适买点"
    if total_score >= 60:
        return "继续观察基本面和趋势确认"
    return "暂不纳入候选"


def validate_no_forbidden_words(text: str) -> None:
    found = [word for word in FORBIDDEN_WORDS if word in text]
    if found:
        raise ValueError(f"forbidden words in rule text: {', '.join(found)}")


def _score(row: dict[str, Any], key: str) -> float:
    try:
        return float(row.get(key, 0.0))
    except (TypeError, ValueError):
        return 0.0


def _sanitize(text: str) -> str:
    validate_no_forbidden_words(text)
    return text

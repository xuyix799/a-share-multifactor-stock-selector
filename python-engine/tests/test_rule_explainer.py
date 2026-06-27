from stock_selector.scoring.rule_explainer import FORBIDDEN_WORDS, build_reason, build_suggestion


def test_reason_is_rule_based_and_non_empty():
    reason = build_reason(
        {
            "quality_score": 75,
            "growth_score": 72,
            "valuation_score": 71,
            "industry_score": 70,
            "trend_score": 69,
        }
    )

    assert "盈利质量较好" in reason
    assert "成长表现较稳定" in reason
    assert reason


def test_reason_and_suggestion_do_not_contain_forbidden_words():
    row = {
        "quality_score": 30,
        "growth_score": 35,
        "valuation_score": 30,
        "industry_score": 50,
        "trend_score": 30,
        "total_score": 58,
        "risk_level": "medium",
    }

    reason = build_reason(row)
    suggestion = build_suggestion(row)

    assert reason
    assert suggestion
    for word in FORBIDDEN_WORDS:
        assert word not in reason
        assert word not in suggestion


def test_suggestion_uses_total_score_and_risk_level_rules():
    assert build_suggestion({"total_score": 82, "risk_level": "low"}) == "可作为中长线重点候选"
    assert build_suggestion({"total_score": 72, "risk_level": "medium"}) == "观察，等待合适买点"
    assert build_suggestion({"total_score": 62, "risk_level": "high"}) == "继续观察基本面和趋势确认"
    assert build_suggestion({"total_score": 52, "risk_level": "high"}) == "暂不纳入候选"

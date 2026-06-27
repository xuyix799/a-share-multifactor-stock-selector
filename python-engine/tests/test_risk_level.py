from stock_selector.scoring.risk_level import determine_risk_level


def test_risk_level_low_for_strong_score_without_severe_flags():
    assert determine_risk_level(total_score=80, quality_score=70, growth_score=60, risk_flags="") == "low"


def test_risk_level_medium_for_mid_score_without_severe_flags():
    assert determine_risk_level(total_score=65, quality_score=55, growth_score=45, risk_flags="") == "medium"


def test_risk_level_high_for_low_total_or_weak_core_scores():
    assert determine_risk_level(total_score=54, quality_score=80, growth_score=80, risk_flags="") == "high"
    assert determine_risk_level(total_score=70, quality_score=39, growth_score=80, risk_flags="") == "high"
    assert determine_risk_level(total_score=70, quality_score=80, growth_score=29, risk_flags="") == "high"


def test_risk_level_high_for_severe_flags_from_goal5_or_goal7_names():
    assert determine_risk_level(total_score=80, quality_score=80, growth_score=80, risk_flags="FINANCIAL_MISSING") == "high"
    assert determine_risk_level(total_score=80, quality_score=80, growth_score=80, risk_flags="is_paused") == "high"

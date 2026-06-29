import pandas as pd
import pytest

from stock_selector.data.data_validator import DataValidationError, validate_dataset_frame
from stock_selector.data.quality_contract import (
    BacktestMode,
    DAILY_PRICE_CANDIDATE_REQUIRED_INPUTS,
    DataQualityLevel,
    DQ3ReadinessStatus,
    PauseStatus,
    SuspensionCoverageStatus,
    TUSHARE_CANDIDATE_BATCH_REQUIRED_INTERFACES,
    can_build_suspension_status_candidate,
    can_build_tushare_candidate_staging_batch,
    can_promote_to_daily_price,
    can_promote_daily_price_candidate_to_standard,
    can_promote_suspension_status_candidate,
    can_mark_tushare_candidate_batch_ready_for_promotion_validator,
    can_write_standard_daily_price_from_tushare_candidate_batch,
    can_use_suspend_miss_as_false_candidate,
    can_use_in_price_only_diagnostic,
    can_use_in_strict_tradable_required,
    can_build_daily_price_candidate_dry_run,
    classify_tushare_daily_price_candidate,
    classify_tushare_candidate_batch,
    classify_provider_dataset,
    get_backtest_mode_contract,
)
from stock_selector.providers.schema_contract import get_schema_contract


DAILY_PRICE_FIELDS = {
    "stock_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "volume",
    "amount",
    "is_paused",
    "limit_up",
    "limit_down",
}


def test_dq1_and_dq2_cannot_promote_to_daily_price():
    assert can_promote_to_daily_price(DataQualityLevel.DQ1, DAILY_PRICE_FIELDS) is False
    assert can_promote_to_daily_price(DataQualityLevel.DQ2, DAILY_PRICE_FIELDS) is False


def test_strict_tradable_rejects_missing_limit_and_pause_fields():
    fields = DAILY_PRICE_FIELDS - {"limit_up", "limit_down", "is_paused"}

    assert can_use_in_strict_tradable_required(DataQualityLevel.DQ3, "daily_price", fields) is False


def test_akshare_stock_daily_is_dq1_non_tradable_raw_only():
    contract = classify_provider_dataset("akshare", "daily_price_raw_smoke")

    assert contract.provider_name == "akshare"
    assert contract.dataset == "daily_price_raw_smoke"
    assert contract.dq_level == DataQualityLevel.DQ1
    assert contract.non_tradable is True
    assert "daily_price_raw" in contract.allowed_datasets
    assert "daily_price" not in contract.allowed_datasets
    assert contract.allowed_backtest_modes == ()
    assert "limit_up" in contract.reason
    assert "limit_down" in contract.reason
    assert "is_paused" in contract.reason


def test_baostock_stock_daily_is_dq1_even_if_login_later_succeeds():
    contract = classify_provider_dataset("baostock", "daily_price")

    assert contract.dq_level == DataQualityLevel.DQ1
    assert contract.non_tradable is True
    assert "daily_price_raw_smoke" in contract.allowed_datasets
    assert "daily_price" not in contract.allowed_datasets
    assert "login succeeds" in contract.reason
    assert "limit_up" in contract.reason
    assert "limit_down" in contract.reason
    assert "is_paused" in contract.reason


def test_akshare_benchmark_price_is_dq2_price_only_diagnostic():
    contract = classify_provider_dataset("akshare", "benchmark_price")

    assert contract.dq_level == DataQualityLevel.DQ2
    assert contract.non_tradable is True
    assert contract.allowed_datasets == ("benchmark_price",)
    assert contract.allowed_backtest_modes == (BacktestMode.PRICE_ONLY_DIAGNOSTIC,)
    assert can_use_in_price_only_diagnostic(contract.dq_level, contract.dataset) is True
    assert can_promote_to_daily_price(contract.dq_level, DAILY_PRICE_FIELDS) is False


def test_strict_tradable_accepts_only_dq3_or_dq4_daily_price_with_trade_fields():
    assert can_use_in_strict_tradable_required(DataQualityLevel.DQ3, "daily_price", DAILY_PRICE_FIELDS) is True
    assert can_use_in_strict_tradable_required(DataQualityLevel.DQ4, "daily_price", DAILY_PRICE_FIELDS) is True
    assert can_use_in_strict_tradable_required(DataQualityLevel.DQ2, "daily_price", DAILY_PRICE_FIELDS) is False
    assert can_use_in_strict_tradable_required(DataQualityLevel.DQ3, "benchmark_price", DAILY_PRICE_FIELDS) is False


def test_price_only_diagnostic_contract_is_marked_non_tradable():
    contract = get_backtest_mode_contract(BacktestMode.PRICE_ONLY_DIAGNOSTIC)

    assert contract.mode == BacktestMode.PRICE_ONLY_DIAGNOSTIC
    assert contract.allowed_dq_levels == (DataQualityLevel.DQ2,)
    assert contract.allowed_datasets == ("benchmark_price",)
    assert contract.tradable is False
    assert contract.result_label == "diagnostic_non_tradable"


def test_tushare_requires_current_goal10r_matrix_and_pause_source_before_dq3():
    contract = classify_provider_dataset("tushare", "daily_price")

    assert contract.dq_level == DataQualityLevel.DQ1
    assert contract.non_tradable is True
    assert contract.strict_tradable_ready is False
    assert "Goal 10R capability matrix" in contract.reason
    assert "stk_limit" in contract.reason
    assert "adj_factor" in contract.reason
    assert "daily_basic" in contract.reason
    assert "is_paused" in contract.reason


def test_tushare_daily_price_candidate_dry_run_contract_is_smoke_only_and_non_promotable():
    contract = classify_tushare_daily_price_candidate(source_layer="smoke")

    assert contract.provider_name == "tushare"
    assert contract.candidate_dataset == "daily_price_candidate"
    assert contract.source_layer == "smoke"
    assert contract.dq_level == DataQualityLevel.DQ1
    assert contract.required_inputs == DAILY_PRICE_CANDIDATE_REQUIRED_INPUTS
    assert contract.standard_daily_price_ready is False
    assert contract.standard_daily_price_written is False
    assert contract.real_backtest_allowed is False
    assert "coverage_audit" in contract.required_future_gates
    assert "validator" in contract.required_future_gates


def test_daily_price_candidate_dry_run_requires_all_tushare_smoke_inputs():
    assert can_build_daily_price_candidate_dry_run({"daily", "stk_limit", "adj_factor", "trade_cal", "suspend_d"}) is True
    assert can_build_daily_price_candidate_dry_run({"daily", "adj_factor", "trade_cal", "suspend_d"}) is False


def test_tushare_candidate_staging_batch_contract_is_candidate_only_and_not_standard_writable():
    contract = classify_tushare_candidate_batch(source_layer="candidate")

    assert contract.provider_name == "tushare"
    assert contract.candidate_dataset == "tushare_candidate_staging_batch"
    assert contract.source_layer == "candidate"
    assert contract.dq_level == DataQualityLevel.DQ1
    assert contract.required_interfaces == TUSHARE_CANDIDATE_BATCH_REQUIRED_INTERFACES
    assert contract.standard_daily_price_written is False
    assert contract.standard_suspension_status_written is False
    assert contract.real_backtest_allowed is False
    assert "coverage_audit" in contract.required_future_gates
    assert "promotion_validator" in contract.required_future_gates

    assert (
        can_build_tushare_candidate_staging_batch(
            available_interfaces={"daily", "stk_limit", "adj_factor", "daily_basic", "trade_cal", "suspend_d"},
            provider_enabled=True,
            token_available=True,
        )
        is True
    )
    assert (
        can_build_tushare_candidate_staging_batch(
            available_interfaces={"daily", "stk_limit", "adj_factor", "trade_cal", "suspend_d"},
            provider_enabled=True,
            token_available=True,
        )
        is False
    )
    assert (
        can_build_tushare_candidate_staging_batch(
            available_interfaces={"daily", "stk_limit", "adj_factor", "daily_basic", "trade_cal", "suspend_d"},
            provider_enabled=False,
            token_available=True,
        )
        is False
    )


def test_tushare_candidate_batch_can_only_reach_ready_for_promotion_validator_not_standard_write():
    readiness = can_mark_tushare_candidate_batch_ready_for_promotion_validator(
        field_completeness_ok=True,
        coverage_complete=True,
        pause_statuses=[PauseStatus.TRUE_CANDIDATE, PauseStatus.FALSE_CANDIDATE],
        duplicate_check_ok=True,
        schema_check_ok=True,
        validator_precheck_passed=True,
        dq_level=DataQualityLevel.DQ3,
    )

    assert readiness.ready_for_promotion_validator is True
    assert readiness.ready_for_dq3_promotion is False
    assert readiness.status == DQ3ReadinessStatus.READY_FOR_PROMOTION_VALIDATOR
    assert can_write_standard_daily_price_from_tushare_candidate_batch(readiness, explicit_standard_write_enabled=False) is False

    blocked = can_mark_tushare_candidate_batch_ready_for_promotion_validator(
        field_completeness_ok=False,
        coverage_complete=False,
        pause_statuses=[PauseStatus.TRUE_CANDIDATE, PauseStatus.UNKNOWN],
        duplicate_check_ok=True,
        schema_check_ok=True,
        validator_precheck_passed=False,
        dq_level=DataQualityLevel.DQ1,
    )

    assert blocked.ready_for_promotion_validator is False
    assert blocked.ready_for_dq3_promotion is False
    assert blocked.status == DQ3ReadinessStatus.BLOCKED_BY_UNRESOLVED_IS_PAUSED
    assert "pause_status contains unresolved unknown rows" in blocked.reasons
    assert "candidate batch field completeness is incomplete" in blocked.reasons
    assert can_write_standard_daily_price_from_tushare_candidate_batch(blocked, explicit_standard_write_enabled=True) is False


def test_daily_price_candidate_promotion_requires_candidate_source_audit_boolean_pause_validator_and_dq3():
    readiness = can_promote_daily_price_candidate_to_standard(
        source_layer="smoke",
        fields=DAILY_PRICE_FIELDS,
        stk_limit_fields_complete=True,
        trade_cal_valid=True,
        suspension_status_coverage_audited=True,
        is_paused_boolean=True,
        validator_passed=True,
        dq_level=DataQualityLevel.DQ3,
    )

    assert readiness.ready_for_dq3_promotion is False
    assert readiness.status == "BLOCKED_BY_SMOKE_SOURCE"
    assert "source layer must be candidate, not smoke" in readiness.reasons

    readiness = can_promote_daily_price_candidate_to_standard(
        source_layer="candidate",
        fields=DAILY_PRICE_FIELDS - {"is_paused"},
        stk_limit_fields_complete=True,
        trade_cal_valid=True,
        suspension_status_coverage_audited=False,
        is_paused_boolean=False,
        validator_passed=False,
        dq_level=DataQualityLevel.DQ2,
    )

    assert readiness.ready_for_dq3_promotion is False
    assert readiness.status == "BLOCKED_BY_UNRESOLVED_IS_PAUSED"
    assert "suspension_status coverage audit is required" in readiness.reasons
    assert "is_paused must be an explicit boolean field" in readiness.reasons
    assert "dq_level must be DQ3 or DQ4" in readiness.reasons

    readiness = can_promote_daily_price_candidate_to_standard(
        source_layer="candidate",
        fields=DAILY_PRICE_FIELDS,
        stk_limit_fields_complete=True,
        trade_cal_valid=True,
        suspension_status_coverage_audited=True,
        is_paused_boolean=True,
        validator_passed=True,
        dq_level=DataQualityLevel.DQ3,
    )

    assert readiness.ready_for_dq3_promotion is True
    assert readiness.status == "READY_FOR_DQ3_PROMOTION"


def test_suspension_status_candidate_requires_candidate_report_calendar_and_event_source():
    assert can_build_suspension_status_candidate({"daily_price_candidate", "daily_price_candidate_report", "trade_cal", "suspend_d"}) is True
    assert can_build_suspension_status_candidate({"daily_price_candidate", "trade_cal", "suspend_d"}) is False


def test_suspend_miss_can_only_be_false_candidate_with_full_coverage_and_no_inference_shortcuts():
    assert (
        can_use_suspend_miss_as_false_candidate(
            coverage_status=SuspensionCoverageStatus.FULL_EVENT_COVERAGE,
            trade_cal_valid=True,
            schema_valid=True,
            volume_used_as_pause=False,
            amount_used_as_pause=False,
            missing_daily_used_as_pause=False,
            unchanged_price_used_as_pause=False,
        )
        is True
    )
    assert (
        can_use_suspend_miss_as_false_candidate(
            coverage_status=SuspensionCoverageStatus.SAMPLE_TRUNCATED,
            trade_cal_valid=True,
            schema_valid=True,
            volume_used_as_pause=False,
            amount_used_as_pause=False,
            missing_daily_used_as_pause=False,
            unchanged_price_used_as_pause=False,
        )
        is False
    )
    assert (
        can_use_suspend_miss_as_false_candidate(
            coverage_status=SuspensionCoverageStatus.COVERAGE_UNKNOWN,
            trade_cal_valid=True,
            schema_valid=True,
            volume_used_as_pause=False,
            amount_used_as_pause=False,
            missing_daily_used_as_pause=False,
            unchanged_price_used_as_pause=False,
        )
        is False
    )
    assert (
        can_use_suspend_miss_as_false_candidate(
            coverage_status=SuspensionCoverageStatus.FULL_EVENT_COVERAGE,
            trade_cal_valid=True,
            schema_valid=True,
            volume_used_as_pause=True,
            amount_used_as_pause=False,
            missing_daily_used_as_pause=False,
            unchanged_price_used_as_pause=False,
        )
        is False
    )


def test_suspension_status_candidate_promotion_remains_blocked_without_validator_and_dq3():
    readiness = can_promote_suspension_status_candidate(
        coverage_status=SuspensionCoverageStatus.SAMPLE_TRUNCATED,
        pause_statuses=[PauseStatus.TRUE_CANDIDATE, PauseStatus.UNKNOWN],
        validator_passed=False,
        dq_level=DataQualityLevel.DQ1,
    )

    assert readiness.ready_for_dq3_promotion is False
    assert readiness.status == "BLOCKED_BY_INCOMPLETE_SUSPEND_D_COVERAGE"
    assert "suspend_d event source coverage is not complete" in readiness.reasons

    readiness = can_promote_suspension_status_candidate(
        coverage_status=SuspensionCoverageStatus.FULL_EVENT_COVERAGE,
        pause_statuses=[PauseStatus.TRUE_CANDIDATE, PauseStatus.FALSE_CANDIDATE],
        validator_passed=False,
        dq_level=DataQualityLevel.DQ2,
    )

    assert readiness.ready_for_dq3_promotion is False
    assert readiness.status == "CANDIDATE_AUDIT_COMPLETED_NOT_PROMOTABLE"
    assert "suspension_status promotion validator must pass" in readiness.reasons
    assert "dq_level must be DQ3 or DQ4" in readiness.reasons


def test_daily_price_schema_contract_keeps_trade_constraint_fields():
    contract = get_schema_contract("daily_price")

    assert "pre_close" in contract.columns
    assert "is_paused" in contract.columns
    assert "limit_up" in contract.columns
    assert "limit_down" in contract.columns
    assert "is_paused" in contract.bool_columns
    assert "limit_up" in contract.numeric_columns
    assert "limit_down" in contract.numeric_columns


def test_daily_price_validator_still_rejects_missing_trade_constraints():
    raw_like_daily = pd.DataFrame(
        [
            {
                "stock_code": "000001.SZ",
                "trade_date": "2026-06-19",
                "open": 10.0,
                "high": 10.3,
                "low": 9.9,
                "close": 10.2,
                "pre_close": 10.0,
                "volume": 1000.0,
                "amount": 10200.0,
                "pct_chg": 2.0,
            }
        ]
    )

    with pytest.raises(DataValidationError, match="missing columns: is_paused, limit_up, limit_down"):
        validate_dataset_frame("daily_price", raw_like_daily, "2026-06-19")

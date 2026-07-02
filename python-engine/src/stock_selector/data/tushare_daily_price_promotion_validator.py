from collections.abc import Callable
from datetime import datetime, timezone
import hashlib
from typing import Any

import pandas as pd

from stock_selector.data.data_validator import DataValidationError, validate_dataset_frame, validate_stock_code
from stock_selector.utils.date_validator import validate_date_range


StandardDailyPriceReadFn = Callable[[str, str], pd.DataFrame | None]
StandardDailyPriceWriteFn = Callable[[str, str, pd.DataFrame], str]
GeneratedAtFn = Callable[[], str]

READY_FOR_PROMOTION_VALIDATOR = "READY_FOR_PROMOTION_VALIDATOR"
VALIDATOR_PASS = "VALIDATOR_PASS"
BLOCKED = "BLOCKED"

DEFAULT_MAX_CODES = 5
DEFAULT_MAX_TRADE_DAYS = 10
DEFAULT_MAX_ROWS = 50

GOAL14_SAFETY = {
    "standard_suspension_status_write_performed": False,
    "real_backtest_performed": False,
    "clean_factor_selection_backtest_entered": False,
    "standard_suspension_status_write_allowed": False,
    "clean_daily_snapshot_entered": False,
    "factor_input_table_entered": False,
    "factor_daily_entered": False,
    "selection_result_entered": False,
}

INFERENCE_GUARDS = {
    "volume_used_as_pause": False,
    "amount_used_as_pause": False,
    "missing_daily_used_as_pause": False,
    "unchanged_price_used_as_pause": False,
    "suspend_d_miss_used_as_false_without_coverage": False,
}

BLOCKED_REASON_CATALOG = (
    "GOAL13C_PREFLIGHT_REPORT_MISSING",
    "GOAL13C_PREFLIGHT_NOT_READY",
    "INCOMPLETE_DAILY_COVERAGE",
    "INCOMPLETE_LIMIT_PRICE_COVERAGE",
    "INCOMPLETE_ADJ_FACTOR_COVERAGE",
    "INCOMPLETE_DAILY_BASIC_COVERAGE",
    "UNRESOLVED_IS_PAUSED",
    "PROVIDER_EMPTY_AFTER_RETRIES",
    "PROVIDER_FETCH_INCOMPLETE",
    "CANDIDATE_BATCH_MISSING",
    "CANDIDATE_ROW_COUNT_MISMATCH",
    "CANDIDATE_DUPLICATE_CODE_DATE",
    "CANDIDATE_NON_OPEN_TRADE_DATE",
    "CANDIDATE_SCHEMA_INCOMPATIBLE_WITH_DAILY_PRICE_CONTRACT",
    "MISSING_REQUIRED_DAILY_PRICE_FIELD",
    "INVALID_OHLC",
    "INVALID_VOLUME_OR_AMOUNT",
    "LIMIT_PRICE_SOURCE_NOT_AUDITABLE",
    "PRE_CLOSE_SOURCE_NOT_AUDITABLE",
    "IS_PAUSED_SOURCE_NOT_AUDITABLE",
    "BATCH_TOO_LARGE_FOR_GOAL14_SMALL_RANGE_VALIDATOR",
    "STANDARD_WRITE_REQUIRES_EXPLICIT_EXECUTE_FLAG",
    "STANDARD_WRITE_REQUIRES_EXPLICIT_APPLY_FLAG",
    "CANONICAL_DAILY_PRICE_READ_REQUIRED_FOR_APPLY",
    "CANONICAL_DAILY_PRICE_DUPLICATE_KEYS_BEFORE_APPLY",
    "CANONICAL_DAILY_PRICE_INVALID_BEFORE_APPLY",
    "READ_BACK_ROW_COUNT_MISMATCH",
    "READ_BACK_PROMOTED_ROW_COUNT_MISMATCH",
    "READ_BACK_DUPLICATE_CANONICAL_KEYS",
    "READ_BACK_TRADE_DATE_RANGE_MISMATCH",
    "READ_BACK_SCHEMA_INVALID",
    "READ_BACK_CANONICAL_SEMANTICS_INVALID",
    "STANDARD_SUSPENSION_STATUS_WRITE_NOT_ALLOWED_IN_GOAL14",
    "CLEAN_FACTOR_SELECTION_BACKTEST_NOT_ALLOWED_IN_GOAL14",
)

REQUIRED_CANDIDATE_FIELDS = (
    "ts_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "amount",
    "adj_factor",
    "limit_up",
    "limit_down",
    "pause_candidate_status",
    "is_paused_candidate",
    "resolution_source",
    "daily_source_object_key",
    "limit_source_object_key",
    "event_source_object_key",
    "calendar_source_object_key",
)
ALLOWED_PAUSE_RESOLUTION_SOURCES = {
    "SUSPEND_D_EVENT_ROW",
    "SUSPEND_D_FULL_COVERAGE_MISS_AS_FALSE_CANDIDATE",
}
STANDARD_DAILY_PRICE_COLUMNS = (
    "stock_code",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "volume",
    "amount",
    "pct_chg",
    "is_paused",
    "limit_up",
    "limit_down",
)


def build_tushare_daily_price_promotion_validator_output_keys(batch_id: str) -> dict[str, str]:
    batch_id = _validate_batch_id(batch_id)
    return {
        "daily_price_promotion_validator_report": (
            f"candidate/tushare/daily_price_promotion_validator_report/batch_id={batch_id}/report.json"
        ),
        "standard_daily_price_promotion_dry_run_report": (
            f"candidate/tushare/standard_daily_price_promotion_dry_run_report/batch_id={batch_id}/report.json"
        ),
    }


def build_tushare_daily_price_promotion_validator(
    *,
    batch_id: str,
    promotion_preflight_report: dict[str, Any] | None,
    daily_price_candidate_batch: pd.DataFrame | None,
    suspension_status_candidate_batch: pd.DataFrame | None,
    max_codes: int = DEFAULT_MAX_CODES,
    max_trade_days: int = DEFAULT_MAX_TRADE_DAYS,
    max_rows: int = DEFAULT_MAX_ROWS,
    request_standard_write: bool = False,
    execute_standard_write: bool = False,
    apply_standard_write: bool = False,
    apply_codes: list[str] | None = None,
    apply_start_date: str | None = None,
    apply_end_date: str | None = None,
    standard_daily_price_read_fn: StandardDailyPriceReadFn | None = None,
    standard_daily_price_write_fn: StandardDailyPriceWriteFn | None = None,
    generated_at_fn: GeneratedAtFn | None = None,
    source_object_keys: dict[str, str] | None = None,
) -> dict[str, Any]:
    generated_at_fn = generated_at_fn or _utc_now_iso
    generated_at = generated_at_fn()
    batch_id = _validate_batch_id(batch_id)
    output_keys = build_tushare_daily_price_promotion_validator_output_keys(batch_id)
    apply_requested = bool(apply_standard_write or execute_standard_write)
    if apply_requested:
        output_keys["standard_daily_price_promotion_apply_report"] = (
            f"candidate/tushare/standard_daily_price_promotion_apply_report/batch_id={batch_id}/report.json"
        )
    source_object_keys = dict(source_object_keys or _default_source_object_keys(batch_id))

    candidate = daily_price_candidate_batch.copy() if daily_price_candidate_batch is not None else None
    suspension_candidate = suspension_status_candidate_batch.copy() if suspension_status_candidate_batch is not None else None
    blocked_reasons: list[str] = []

    if max_codes <= 0 or max_trade_days <= 0 or max_rows <= 0:
        raise ValueError("Goal 14 max limits must be positive")

    preflight_codes = list((promotion_preflight_report or {}).get("codes") or [])
    preflight_trade_dates = list((promotion_preflight_report or {}).get("trade_dates") or [])
    preflight_blocked_reasons = list((promotion_preflight_report or {}).get("blocked_reasons") or [])
    apply_scope = _normalize_apply_scope(apply_codes, apply_start_date, apply_end_date)
    preflight_codes = _filter_codes_for_scope(preflight_codes, apply_scope)
    preflight_trade_dates = _filter_trade_dates_for_scope(preflight_trade_dates, apply_scope)
    candidate = _filter_candidate_for_scope(candidate, apply_scope)
    suspension_candidate = _filter_candidate_for_scope(suspension_candidate, apply_scope)

    if promotion_preflight_report is None:
        blocked_reasons.append("GOAL13C_PREFLIGHT_REPORT_MISSING")
    elif promotion_preflight_report.get("status") != READY_FOR_PROMOTION_VALIDATOR:
        blocked_reasons.append("GOAL13C_PREFLIGHT_NOT_READY")
        blocked_reasons.extend(preflight_blocked_reasons)

    if promotion_preflight_report is not None:
        if promotion_preflight_report.get("standard_daily_price_write_performed") is not False:
            blocked_reasons.append("GOAL13C_PREFLIGHT_NOT_READY")
        if promotion_preflight_report.get("real_backtest_performed") is not False:
            blocked_reasons.append("GOAL13C_PREFLIGHT_NOT_READY")

    coverage_check = _build_coverage_check(promotion_preflight_report)
    blocked_reasons.extend(coverage_check["blocked_reasons"])
    blocked_reasons.extend(_provider_error_blocked_reasons(promotion_preflight_report))

    row_count = int(len(candidate)) if candidate is not None else 0
    candidate_codes = sorted(candidate["ts_code"].dropna().astype(str).unique().tolist()) if candidate is not None and "ts_code" in candidate.columns else []
    candidate_trade_dates = (
        sorted(candidate["trade_date"].dropna().astype(str).unique().tolist())
        if candidate is not None and "trade_date" in candidate.columns
        else []
    )
    small_range_guard = {
        "max_codes": max_codes,
        "max_trade_days": max_trade_days,
        "max_rows": max_rows,
        "code_count": len(candidate_codes or preflight_codes),
        "trade_day_count": len(candidate_trade_dates or preflight_trade_dates),
        "row_count": row_count,
        "passed": True,
    }
    if (
        small_range_guard["code_count"] > max_codes
        or small_range_guard["trade_day_count"] > max_trade_days
        or small_range_guard["row_count"] > max_rows
    ):
        small_range_guard["passed"] = False
        blocked_reasons.append("BATCH_TOO_LARGE_FOR_GOAL14_SMALL_RANGE_VALIDATOR")

    candidate_checks = _validate_candidate_batch(
        candidate=candidate,
        preflight_codes=preflight_codes,
        preflight_trade_dates=preflight_trade_dates,
    )
    blocked_reasons.extend(candidate_checks["blocked_reasons"])

    pause_resolution_check = candidate_checks["pause_resolution_check"]
    schema_contract_check = candidate_checks["schema_contract_check"]
    standard_daily_price_frames = candidate_checks["standard_daily_price_frames"]

    if request_standard_write and not apply_requested:
        blocked_reasons.append("STANDARD_WRITE_REQUIRES_EXPLICIT_EXECUTE_FLAG")
        blocked_reasons.append("STANDARD_WRITE_REQUIRES_EXPLICIT_APPLY_FLAG")
    if apply_requested and standard_daily_price_write_fn is None:
        blocked_reasons.append("STANDARD_WRITE_REQUIRES_EXPLICIT_EXECUTE_FLAG")
        blocked_reasons.append("STANDARD_WRITE_REQUIRES_EXPLICIT_APPLY_FLAG")
    if apply_requested and standard_daily_price_read_fn is None:
        blocked_reasons.append("CANONICAL_DAILY_PRICE_READ_REQUIRED_FOR_APPLY")

    blocked_reasons = _dedupe(blocked_reasons)
    status = VALIDATOR_PASS if not blocked_reasons else BLOCKED
    validation_passed_before_write = status == VALIDATOR_PASS

    standard_write_performed = False
    standard_write_results: list[dict[str, Any]] = []
    apply_report = None
    read_back_verification = None
    upsert_summary = {"inserted_rows": 0, "updated_rows": 0, "unchanged_rows": 0}
    if apply_requested:
        apply_result = _apply_standard_daily_price(
            validator_status_before_apply=status,
            blocked_reasons_before_apply=blocked_reasons,
            standard_daily_price_frames=standard_daily_price_frames,
            standard_daily_price_read_fn=standard_daily_price_read_fn,
            standard_daily_price_write_fn=standard_daily_price_write_fn,
        )
        standard_write_performed = apply_result["standard_write_performed"]
        standard_write_results = apply_result["write_results"]
        upsert_summary = apply_result["upsert_summary"]
        read_back_verification = apply_result["read_back_verification"]
        apply_blocked_reasons = apply_result["blocked_reasons"]
        if apply_blocked_reasons:
            blocked_reasons = _dedupe(blocked_reasons + apply_blocked_reasons)
            status = BLOCKED
        apply_report = _build_apply_report(
            batch_id=batch_id,
            generated_at=generated_at,
            validator_status=status,
            blocked_reasons=blocked_reasons,
            standard_write_performed=standard_write_performed,
            standard_write_results=standard_write_results,
            idempotency_key=_idempotency_key(batch_id, candidate),
            upsert_summary=upsert_summary,
            read_back_verification=read_back_verification,
        )

    dry_run_report = _build_dry_run_report(
        batch_id=batch_id,
        generated_at=generated_at,
        validator_status=status,
        candidate=candidate,
        blocked_reasons=blocked_reasons,
        idempotency_key=_idempotency_key(batch_id, candidate),
    )
    validator_report = {
        "schema_version": "goal14.daily_price_promotion_validator_report.v1",
        "goal": "15" if apply_requested else "14",
        "provider": "tushare",
        "batch_id": batch_id,
        "generated_at": generated_at,
        "status": status,
        "source_reports": {
            "promotion_preflight_report": source_object_keys.get("promotion_preflight_report"),
            "daily_price_candidate_batch": source_object_keys.get("daily_price_candidate_batch"),
            "suspension_status_candidate_batch": source_object_keys.get("suspension_status_candidate_batch"),
        },
        "apply_scope": apply_scope,
        "small_range_guard": small_range_guard,
        "coverage_check": coverage_check,
        "pause_resolution_check": pause_resolution_check,
        "schema_contract_check": schema_contract_check,
        "blocked_reasons": blocked_reasons,
        "blocked_reason_catalog": list(BLOCKED_REASON_CATALOG),
        "standard_daily_price_write_allowed_with_explicit_flag": validation_passed_before_write,
        "standard_daily_price_write_performed": standard_write_performed,
        "standard_daily_price_write_results": standard_write_results,
        "standard_daily_price_apply_requested": apply_requested,
        "read_back_verification": read_back_verification,
        "upsert_summary": upsert_summary,
        "standard_suspension_status_write_performed": False,
        "clean_performed": False,
        "factor_performed": False,
        "selection_performed": False,
        "real_backtest_performed": False,
        "clean_factor_selection_backtest_entered": False,
        "safety": dict(GOAL14_SAFETY),
        "inference_guards": dict(INFERENCE_GUARDS),
        "not_allowed_actions": [
            "STANDARD_SUSPENSION_STATUS_WRITE_NOT_ALLOWED_IN_GOAL14",
            "CLEAN_FACTOR_SELECTION_BACKTEST_NOT_ALLOWED_IN_GOAL14",
        ],
    }
    return {
        "goal": "15" if apply_requested else "14",
        "provider": "tushare",
        "batch_id": batch_id,
        "status": status,
        "output_object_keys": output_keys,
        "daily_price_promotion_validator_report": validator_report,
        "standard_daily_price_promotion_dry_run_report": dry_run_report,
        "standard_daily_price_promotion_apply_report": apply_report,
        "blocked_reasons": blocked_reasons,
        "standard_daily_price_write_performed": standard_write_performed,
        "standard_suspension_status_write_performed": False,
        "real_backtest_performed": False,
        "clean_factor_selection_backtest_entered": False,
        "candidate_row_count": row_count,
        "apply_scope": apply_scope,
        "standard_write_results": standard_write_results,
        "upsert_summary": upsert_summary,
        "read_back_verification": read_back_verification,
    }


def _build_coverage_check(promotion_preflight_report: dict[str, Any] | None) -> dict[str, Any]:
    interfaces = {
        "daily": "INCOMPLETE_DAILY_COVERAGE",
        "stk_limit": "INCOMPLETE_LIMIT_PRICE_COVERAGE",
        "adj_factor": "INCOMPLETE_ADJ_FACTOR_COVERAGE",
        "daily_basic": "INCOMPLETE_DAILY_BASIC_COVERAGE",
    }
    price_coverage = (promotion_preflight_report or {}).get("price_coverage") or {}
    details = {}
    blocked = []
    for interface, reason in interfaces.items():
        coverage = dict(price_coverage.get(interface) or {})
        complete = _coverage_complete(coverage)
        details[interface] = {"passed": complete, **coverage}
        if promotion_preflight_report is not None and not complete:
            blocked.append(reason)
    return {
        "passed": not blocked and promotion_preflight_report is not None,
        "interfaces": details,
        "blocked_reasons": blocked,
    }


def _normalize_apply_scope(codes: list[str] | None, start_date: str | None, end_date: str | None) -> dict[str, Any]:
    normalized_codes = None
    if codes is not None:
        normalized_codes = [validate_stock_code(str(code).strip().upper()) for code in codes if str(code).strip()]
    normalized_start = None
    normalized_end = None
    if start_date is not None or end_date is not None:
        if start_date is None or end_date is None:
            raise ValueError("apply_start_date and apply_end_date must be provided together")
        normalized_start, normalized_end = validate_date_range(start_date, end_date)
    return {
        "codes": normalized_codes,
        "start_date": normalized_start,
        "end_date": normalized_end,
    }


def _filter_codes_for_scope(codes: list[str], scope: dict[str, Any]) -> list[str]:
    if not scope.get("codes"):
        return list(codes)
    allowed = set(scope["codes"])
    return [code for code in codes if code in allowed]


def _filter_trade_dates_for_scope(trade_dates: list[str], scope: dict[str, Any]) -> list[str]:
    start_date = scope.get("start_date")
    end_date = scope.get("end_date")
    if not start_date and not end_date:
        return list(trade_dates)
    return [trade_date for trade_date in trade_dates if start_date <= str(trade_date) <= end_date]


def _filter_candidate_for_scope(candidate: pd.DataFrame | None, scope: dict[str, Any]) -> pd.DataFrame | None:
    if candidate is None:
        return None
    filtered = candidate.copy()
    if scope.get("codes") and "ts_code" in filtered.columns:
        filtered = filtered[filtered["ts_code"].astype(str).isin(set(scope["codes"]))]
    start_date = scope.get("start_date")
    end_date = scope.get("end_date")
    if start_date and end_date and "trade_date" in filtered.columns:
        trade_dates = filtered["trade_date"].astype(str)
        filtered = filtered[(trade_dates >= start_date) & (trade_dates <= end_date)]
    return filtered.reset_index(drop=True)


def _coverage_complete(coverage: dict[str, Any]) -> bool:
    if not coverage:
        return False
    denominator = coverage.get("denominator")
    numerator = coverage.get("numerator", coverage.get("matched_rows"))
    missing = coverage.get("missing_rows")
    blocked_reason = coverage.get("blocked_reason")
    ratio = coverage.get("ratio", coverage.get("coverage_rate"))
    return (
        denominator is not None
        and int(denominator) > 0
        and int(numerator or 0) == int(denominator)
        and int(missing or 0) == 0
        and blocked_reason in {None, ""}
        and float(ratio or 0.0) >= 1.0
    )


def _provider_error_blocked_reasons(promotion_preflight_report: dict[str, Any] | None) -> list[str]:
    if not promotion_preflight_report:
        return []
    errors = promotion_preflight_report.get("provider_errors") or []
    reasons = []
    if any(error.get("reason_code") == "PROVIDER_EMPTY_AFTER_RETRIES" for error in errors):
        reasons.append("PROVIDER_EMPTY_AFTER_RETRIES")
    if errors:
        reasons.append("PROVIDER_FETCH_INCOMPLETE")
    return reasons


def _validate_candidate_batch(
    *,
    candidate: pd.DataFrame | None,
    preflight_codes: list[str],
    preflight_trade_dates: list[str],
) -> dict[str, Any]:
    blocked: list[str] = []
    standard_frames: dict[str, pd.DataFrame] = {}
    if candidate is None or candidate.empty:
        return {
            "blocked_reasons": ["CANDIDATE_BATCH_MISSING"],
            "pause_resolution_check": {"passed": False, "resolved_count": 0, "unknown_count": 0},
            "schema_contract_check": {"passed": False, "errors": ["candidate batch missing"]},
            "standard_daily_price_frames": standard_frames,
        }

    missing_fields = _missing_candidate_fields(candidate)
    if missing_fields:
        blocked.extend(["MISSING_REQUIRED_DAILY_PRICE_FIELD", "CANDIDATE_SCHEMA_INCOMPATIBLE_WITH_DAILY_PRICE_CONTRACT"])

    if {"ts_code", "trade_date"}.issubset(candidate.columns):
        if candidate.duplicated(["ts_code", "trade_date"]).any():
            blocked.append("CANDIDATE_DUPLICATE_CODE_DATE")
        expected_count = len(preflight_codes) * len(preflight_trade_dates) if preflight_codes and preflight_trade_dates else None
        if expected_count is not None and int(len(candidate)) != int(expected_count):
            blocked.append("CANDIDATE_ROW_COUNT_MISMATCH")
        if preflight_trade_dates:
            if not set(candidate["trade_date"].astype(str)).issubset(set(preflight_trade_dates)):
                blocked.append("CANDIDATE_NON_OPEN_TRADE_DATE")
        if "trading_day_confirmed" in candidate.columns and not candidate["trading_day_confirmed"].fillna(False).astype(bool).all():
            blocked.append("CANDIDATE_NON_OPEN_TRADE_DATE")

    pause_status = candidate["pause_candidate_status"].astype(str) if "pause_candidate_status" in candidate.columns else pd.Series([], dtype=str)
    unknown_count = int((pause_status == "unknown").sum()) if len(pause_status) else int(len(candidate))
    if unknown_count:
        blocked.append("UNRESOLVED_IS_PAUSED")

    if _has_missing_required_values(candidate):
        blocked.extend(["MISSING_REQUIRED_DAILY_PRICE_FIELD", "CANDIDATE_SCHEMA_INCOMPATIBLE_WITH_DAILY_PRICE_CONTRACT"])
    if _invalid_ohlc(candidate):
        blocked.append("INVALID_OHLC")
    if _invalid_volume_or_amount(candidate):
        blocked.append("INVALID_VOLUME_OR_AMOUNT")
    if _invalid_adj_factor(candidate):
        blocked.append("CANDIDATE_SCHEMA_INCOMPATIBLE_WITH_DAILY_PRICE_CONTRACT")
    if _missing_source(candidate, "limit_source_object_key"):
        blocked.append("LIMIT_PRICE_SOURCE_NOT_AUDITABLE")
    if _missing_source(candidate, "daily_source_object_key"):
        blocked.append("PRE_CLOSE_SOURCE_NOT_AUDITABLE")
    if _pause_source_not_auditable(candidate):
        blocked.append("IS_PAUSED_SOURCE_NOT_AUDITABLE")

    contract_errors = []
    if not missing_fields:
        try:
            standard = _candidate_to_standard_daily_price(candidate)
            for trade_date, frame in standard.groupby("trade_date", sort=True):
                trade_date_text = str(trade_date)
                validate_dataset_frame("daily_price", frame.reset_index(drop=True), trade_date_text)
                standard_frames[trade_date_text] = frame.reset_index(drop=True)
        except (DataValidationError, ValueError, TypeError) as exc:
            contract_errors.append(str(exc))
            blocked.append("CANDIDATE_SCHEMA_INCOMPATIBLE_WITH_DAILY_PRICE_CONTRACT")

    blocked = _dedupe(blocked)
    return {
        "blocked_reasons": blocked,
        "pause_resolution_check": {
            "passed": unknown_count == 0 and "IS_PAUSED_SOURCE_NOT_AUDITABLE" not in blocked,
            "resolved_count": int(len(candidate) - unknown_count),
            "unknown_count": unknown_count,
        },
        "schema_contract_check": {
            "passed": not contract_errors and not (
                set(blocked)
                & {
                    "MISSING_REQUIRED_DAILY_PRICE_FIELD",
                    "CANDIDATE_SCHEMA_INCOMPATIBLE_WITH_DAILY_PRICE_CONTRACT",
                    "INVALID_OHLC",
                    "INVALID_VOLUME_OR_AMOUNT",
                    "CANDIDATE_DUPLICATE_CODE_DATE",
                    "CANDIDATE_NON_OPEN_TRADE_DATE",
                }
            ),
            "missing_fields": missing_fields,
            "errors": contract_errors,
        },
        "standard_daily_price_frames": standard_frames,
    }


def _missing_candidate_fields(candidate: pd.DataFrame) -> list[str]:
    missing = [field for field in REQUIRED_CANDIDATE_FIELDS if field not in candidate.columns]
    if "volume" not in candidate.columns and "vol" not in candidate.columns:
        missing.append("volume")
    return sorted(set(missing))


def _has_missing_required_values(candidate: pd.DataFrame) -> bool:
    fields = [field for field in REQUIRED_CANDIDATE_FIELDS if field in candidate.columns]
    volume_column = _volume_column(candidate)
    if volume_column:
        fields.append(volume_column)
    numeric_or_value_fields = [field for field in fields if field != "is_paused_candidate"]
    if numeric_or_value_fields and candidate[numeric_or_value_fields].isna().any().any():
        return True
    return "is_paused_candidate" in candidate.columns and candidate["is_paused_candidate"].isna().any()


def _invalid_ohlc(candidate: pd.DataFrame) -> bool:
    required = {"open", "high", "low", "close", "pre_close"}
    if not required.issubset(candidate.columns):
        return False
    values = candidate[list(required)].apply(pd.to_numeric, errors="coerce")
    if values.isna().any().any():
        return True
    return bool(
        (values[["open", "high", "low", "close", "pre_close"]] < 0).any().any()
        or (values["high"] < values["low"]).any()
        or (values["high"] < values["open"]).any()
        or (values["high"] < values["close"]).any()
        or (values["low"] > values["open"]).any()
        or (values["low"] > values["close"]).any()
    )


def _invalid_volume_or_amount(candidate: pd.DataFrame) -> bool:
    volume_column = _volume_column(candidate)
    if volume_column is None or "amount" not in candidate.columns:
        return False
    values = candidate[[volume_column, "amount"]].apply(pd.to_numeric, errors="coerce")
    return bool(values.isna().any().any() or (values < 0).any().any())


def _invalid_adj_factor(candidate: pd.DataFrame) -> bool:
    if "adj_factor" not in candidate.columns:
        return False
    values = pd.to_numeric(candidate["adj_factor"], errors="coerce")
    return bool(values.isna().any() or (values <= 0).any())


def _missing_source(candidate: pd.DataFrame, column: str) -> bool:
    if column not in candidate.columns:
        return True
    values = candidate[column].fillna("").astype(str).str.strip()
    return bool((values == "").any())


def _pause_source_not_auditable(candidate: pd.DataFrame) -> bool:
    required = {"resolution_source", "is_paused_candidate", "pause_candidate_status"}
    if not required.issubset(candidate.columns):
        return True
    sources = set(candidate["resolution_source"].fillna("").astype(str))
    if not sources.issubset(ALLOWED_PAUSE_RESOLUTION_SOURCES):
        return True
    statuses = set(candidate["pause_candidate_status"].fillna("").astype(str))
    if "unknown" in statuses:
        return True
    return bool(candidate["is_paused_candidate"].isna().any())


def _candidate_to_standard_daily_price(candidate: pd.DataFrame) -> pd.DataFrame:
    volume_column = _volume_column(candidate)
    if volume_column is None:
        raise DataValidationError("missing columns: volume")
    standard = pd.DataFrame(
        {
            "stock_code": candidate["ts_code"].astype(str),
            "trade_date": candidate["trade_date"].astype(str),
            "open": pd.to_numeric(candidate["open"], errors="raise"),
            "high": pd.to_numeric(candidate["high"], errors="raise"),
            "low": pd.to_numeric(candidate["low"], errors="raise"),
            "close": pd.to_numeric(candidate["close"], errors="raise"),
            "pre_close": pd.to_numeric(candidate["pre_close"], errors="raise"),
            "volume": pd.to_numeric(candidate[volume_column], errors="raise"),
            "amount": pd.to_numeric(candidate["amount"], errors="raise"),
            "limit_up": pd.to_numeric(candidate["limit_up"], errors="raise"),
            "limit_down": pd.to_numeric(candidate["limit_down"], errors="raise"),
            "is_paused": candidate["is_paused_candidate"].map(_strict_bool),
        }
    )
    standard["pct_chg"] = ((standard["close"] - standard["pre_close"]) / standard["pre_close"] * 100).round(6)
    return standard[
        [
            "stock_code",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "volume",
            "amount",
            "pct_chg",
            "is_paused",
            "limit_up",
            "limit_down",
        ]
    ]


def _strict_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if pd.isna(value):
        raise DataValidationError("is_paused must be explicit boolean")
    text = str(value).strip().lower()
    if text in {"true", "1"}:
        return True
    if text in {"false", "0"}:
        return False
    raise DataValidationError("is_paused must be explicit boolean")


def _volume_column(candidate: pd.DataFrame) -> str | None:
    if "volume" in candidate.columns:
        return "volume"
    if "vol" in candidate.columns:
        return "vol"
    return None


def _build_dry_run_report(
    *,
    batch_id: str,
    generated_at: str,
    validator_status: str,
    candidate: pd.DataFrame | None,
    blocked_reasons: list[str],
    idempotency_key: str,
) -> dict[str, Any]:
    row_count = int(len(candidate)) if candidate is not None else 0
    return {
        "schema_version": "goal14.standard_daily_price_promotion_dry_run_report.v1",
        "goal": "14",
        "provider": "tushare",
        "batch_id": batch_id,
        "generated_at": generated_at,
        "mode": "DRY_RUN",
        "validator_status": validator_status,
        "target_table": "daily_price",
        "would_insert_rows": row_count if validator_status == VALIDATOR_PASS else 0,
        "would_update_rows": 0,
        "would_skip_rows": 0,
        "idempotency_key": idempotency_key,
        "standard_write_performed": False,
        "requires_explicit_execute_flag": True,
        "blocked_reasons": blocked_reasons,
    }


def _apply_standard_daily_price(
    *,
    validator_status_before_apply: str,
    blocked_reasons_before_apply: list[str],
    standard_daily_price_frames: dict[str, pd.DataFrame],
    standard_daily_price_read_fn: StandardDailyPriceReadFn | None,
    standard_daily_price_write_fn: StandardDailyPriceWriteFn | None,
) -> dict[str, Any]:
    if validator_status_before_apply != VALIDATOR_PASS:
        return {
            "standard_write_performed": False,
            "write_results": [],
            "upsert_summary": {"inserted_rows": 0, "updated_rows": 0, "unchanged_rows": 0},
            "read_back_verification": _empty_read_back_verification(False, blocked_reasons_before_apply),
            "blocked_reasons": [],
        }
    if standard_daily_price_read_fn is None or standard_daily_price_write_fn is None:
        blocked = []
        if standard_daily_price_read_fn is None:
            blocked.append("CANONICAL_DAILY_PRICE_READ_REQUIRED_FOR_APPLY")
        if standard_daily_price_write_fn is None:
            blocked.append("STANDARD_WRITE_REQUIRES_EXPLICIT_APPLY_FLAG")
        return {
            "standard_write_performed": False,
            "write_results": [],
            "upsert_summary": {"inserted_rows": 0, "updated_rows": 0, "unchanged_rows": 0},
            "read_back_verification": _empty_read_back_verification(False, blocked),
            "blocked_reasons": blocked,
        }

    blocked: list[str] = []
    merged_frames: dict[str, pd.DataFrame] = {}
    promoted_frames: dict[str, pd.DataFrame] = {}
    upsert_summary = {"inserted_rows": 0, "updated_rows": 0, "unchanged_rows": 0}
    for trade_date, promoted in sorted(standard_daily_price_frames.items()):
        promoted = _normalize_canonical_daily_price_frame(promoted, trade_date)
        promoted_frames[trade_date] = promoted
        existing = standard_daily_price_read_fn("daily_price", trade_date)
        existing = _normalize_canonical_daily_price_frame(existing, trade_date)
        if not existing.empty and existing.duplicated(["stock_code", "trade_date"]).any():
            blocked.append("CANONICAL_DAILY_PRICE_DUPLICATE_KEYS_BEFORE_APPLY")
        if not existing.empty and _canonical_daily_price_validation_errors(existing, trade_date):
            blocked.append("CANONICAL_DAILY_PRICE_INVALID_BEFORE_APPLY")
        merged, summary = _merge_canonical_daily_price(existing, promoted)
        if _canonical_daily_price_validation_errors(merged, trade_date):
            blocked.append("CANONICAL_DAILY_PRICE_INVALID_BEFORE_APPLY")
        merged_frames[trade_date] = merged
        for key in upsert_summary:
            upsert_summary[key] += summary[key]

    blocked = _dedupe(blocked)
    if blocked:
        return {
            "standard_write_performed": False,
            "write_results": [],
            "upsert_summary": upsert_summary,
            "read_back_verification": _empty_read_back_verification(False, blocked),
            "blocked_reasons": blocked,
        }

    write_results: list[dict[str, Any]] = []
    for trade_date, merged in sorted(merged_frames.items()):
        object_key = standard_daily_price_write_fn("daily_price", trade_date, merged)
        write_results.append(
            {
                "dataset": "daily_price",
                "trade_date": trade_date,
                "promoted_row_count": int(len(promoted_frames[trade_date])),
                "canonical_row_count": int(len(merged)),
                "object_key": object_key,
                "conflict_policy": "upsert_candidate_wins_by_stock_code_trade_date",
            }
        )

    read_back_frames: dict[str, pd.DataFrame] = {}
    for trade_date in sorted(merged_frames):
        read_back = standard_daily_price_read_fn("daily_price", trade_date)
        read_back_frames[trade_date] = _normalize_canonical_daily_price_frame(read_back, trade_date)
    verification = _verify_standard_daily_price_read_back(
        promoted_frames=promoted_frames,
        expected_canonical_frames=merged_frames,
        read_back_frames=read_back_frames,
    )
    return {
        "standard_write_performed": bool(write_results),
        "write_results": write_results,
        "upsert_summary": upsert_summary,
        "read_back_verification": verification,
        "blocked_reasons": verification["blocked_reasons"],
    }


def _build_apply_report(
    *,
    batch_id: str,
    generated_at: str,
    validator_status: str,
    blocked_reasons: list[str],
    standard_write_performed: bool,
    standard_write_results: list[dict[str, Any]],
    idempotency_key: str,
    upsert_summary: dict[str, int],
    read_back_verification: dict[str, Any] | None,
) -> dict[str, Any]:
    read_back_verification = read_back_verification or _empty_read_back_verification(False, blocked_reasons)
    return {
        "schema_version": "goal15.standard_daily_price_promotion_apply_report.v1",
        "goal": "15",
        "provider": "tushare",
        "batch_id": batch_id,
        "generated_at": generated_at,
        "mode": "APPLY",
        "validator_status": validator_status,
        "target_table": "daily_price",
        "standard_write_performed": standard_write_performed,
        "expected_promoted_rows": read_back_verification["expected_promoted_rows"],
        "actual_promoted_rows": read_back_verification["actual_promoted_rows"],
        "written_rows": sum(int(item.get("canonical_row_count", 0)) for item in standard_write_results),
        "upsert_summary": upsert_summary,
        "write_results": standard_write_results,
        "read_back_verification": read_back_verification,
        "idempotency_key": idempotency_key,
        "standard_suspension_status_write_performed": False,
        "clean_performed": False,
        "factor_performed": False,
        "selection_performed": False,
        "real_backtest_performed": False,
        "blocked_reasons": blocked_reasons,
    }


def _normalize_canonical_daily_price_frame(frame: pd.DataFrame | None, trade_date: str) -> pd.DataFrame:
    if frame is None or frame.empty:
        return pd.DataFrame(columns=list(STANDARD_DAILY_PRICE_COLUMNS))
    normalized = frame.copy()
    if "trade_date" in normalized.columns:
        normalized["trade_date"] = normalized["trade_date"].map(_normalize_date_value)
    if "stock_code" in normalized.columns:
        normalized["stock_code"] = normalized["stock_code"].astype(str)
    return normalized.reset_index(drop=True)


def _normalize_date_value(value) -> str:
    if isinstance(value, pd.Timestamp):
        return value.date().isoformat()
    text = str(value)
    if len(text) >= 10:
        return text[:10]
    return text


def _merge_canonical_daily_price(existing: pd.DataFrame, promoted: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    existing = existing.copy().reset_index(drop=True)
    promoted = promoted.copy().reset_index(drop=True)
    promoted_keys = set(_canonical_key_records(promoted))
    existing_key_to_row = {
        key: existing.loc[index, list(STANDARD_DAILY_PRICE_COLUMNS)]
        for index, key in enumerate(_canonical_key_records(existing))
    }
    summary = {"inserted_rows": 0, "updated_rows": 0, "unchanged_rows": 0}
    for index, key in enumerate(_canonical_key_records(promoted)):
        if key not in existing_key_to_row:
            summary["inserted_rows"] += 1
        elif _canonical_row_equal(existing_key_to_row[key], promoted.loc[index, list(STANDARD_DAILY_PRICE_COLUMNS)]):
            summary["unchanged_rows"] += 1
        else:
            summary["updated_rows"] += 1

    if existing.empty:
        retained_existing = existing
    else:
        retained_existing = existing[~pd.Series(_canonical_key_records(existing)).isin(promoted_keys).to_numpy()]
    if retained_existing.empty:
        merged = promoted[list(STANDARD_DAILY_PRICE_COLUMNS)].copy()
    else:
        merged = pd.concat([retained_existing[list(STANDARD_DAILY_PRICE_COLUMNS)], promoted[list(STANDARD_DAILY_PRICE_COLUMNS)]], ignore_index=True)
    if not merged.empty:
        merged = merged.sort_values(["trade_date", "stock_code"]).reset_index(drop=True)
    return merged, summary


def _canonical_key_records(frame: pd.DataFrame) -> list[tuple[str, str]]:
    if frame is None or frame.empty or not {"stock_code", "trade_date"}.issubset(frame.columns):
        return []
    return list(zip(frame["stock_code"].astype(str), frame["trade_date"].map(_normalize_date_value)))


def _canonical_row_equal(left: pd.Series, right: pd.Series) -> bool:
    for column in STANDARD_DAILY_PRICE_COLUMNS:
        left_value = left[column]
        right_value = right[column]
        if pd.isna(left_value) and pd.isna(right_value):
            continue
        if isinstance(left_value, (int, float)) or isinstance(right_value, (int, float)):
            try:
                if float(left_value) == float(right_value):
                    continue
            except (TypeError, ValueError):
                pass
        if left_value != right_value:
            return False
    return True


def _verify_standard_daily_price_read_back(
    *,
    promoted_frames: dict[str, pd.DataFrame],
    expected_canonical_frames: dict[str, pd.DataFrame],
    read_back_frames: dict[str, pd.DataFrame],
) -> dict[str, Any]:
    blocked: list[str] = []
    expected_promoted_keys = set()
    for promoted in promoted_frames.values():
        expected_promoted_keys.update(_canonical_key_records(promoted))

    actual_promoted_keys = set()
    actual_promoted_dates = set()
    expected_total_rows = 0
    actual_total_rows = 0
    for trade_date, expected in sorted(expected_canonical_frames.items()):
        read_back = read_back_frames.get(trade_date, pd.DataFrame(columns=list(STANDARD_DAILY_PRICE_COLUMNS)))
        expected_total_rows += int(len(expected))
        actual_total_rows += int(len(read_back))
        if len(expected) != len(read_back):
            blocked.append("READ_BACK_ROW_COUNT_MISMATCH")
        if {"stock_code", "trade_date"}.issubset(read_back.columns) and read_back.duplicated(["stock_code", "trade_date"]).any():
            blocked.append("READ_BACK_DUPLICATE_CANONICAL_KEYS")
        validation_errors = _canonical_daily_price_validation_errors(read_back, trade_date)
        if "schema" in validation_errors:
            blocked.append("READ_BACK_SCHEMA_INVALID")
        if "semantics" in validation_errors:
            blocked.append("READ_BACK_CANONICAL_SEMANTICS_INVALID")
        for key in _canonical_key_records(read_back):
            if key in expected_promoted_keys:
                actual_promoted_keys.add(key)
                actual_promoted_dates.add(key[1])

    expected_trade_dates = sorted(promoted_frames)
    if sorted(actual_promoted_dates) != expected_trade_dates:
        blocked.append("READ_BACK_TRADE_DATE_RANGE_MISMATCH")
    if len(actual_promoted_keys) != len(expected_promoted_keys):
        blocked.append("READ_BACK_PROMOTED_ROW_COUNT_MISMATCH")
    blocked = _dedupe(blocked)
    return {
        "passed": not blocked,
        "expected_promoted_rows": int(len(expected_promoted_keys)),
        "actual_promoted_rows": int(len(actual_promoted_keys)),
        "expected_total_rows": int(expected_total_rows),
        "actual_total_rows": int(actual_total_rows),
        "expected_trade_dates": expected_trade_dates,
        "actual_promoted_trade_dates": sorted(actual_promoted_dates),
        "blocked_reasons": blocked,
    }


def _canonical_daily_price_validation_errors(frame: pd.DataFrame, trade_date: str) -> set[str]:
    errors: set[str] = set()
    if frame is None or frame.empty:
        errors.add("schema")
        return errors
    missing = [column for column in STANDARD_DAILY_PRICE_COLUMNS if column not in frame.columns]
    if missing:
        errors.add("schema")
        return errors
    required = frame[list(STANDARD_DAILY_PRICE_COLUMNS)]
    if required.isna().any().any():
        errors.add("schema")
    try:
        validate_dataset_frame("daily_price", required, trade_date)
    except (DataValidationError, ValueError, TypeError):
        errors.add("schema")
    numeric = required[["open", "high", "low", "close", "pre_close", "limit_up", "limit_down"]].apply(pd.to_numeric, errors="coerce")
    if numeric.isna().any().any():
        errors.add("schema")
    elif bool(
        (numeric[["open", "high", "low", "close", "pre_close"]] <= 0).any().any()
        or (numeric["high"] < numeric["open"]).any()
        or (numeric["high"] < numeric["close"]).any()
        or (numeric["low"] > numeric["open"]).any()
        or (numeric["low"] > numeric["close"]).any()
        or (numeric["limit_up"] < numeric["close"]).any()
        or (numeric["limit_down"] > numeric["close"]).any()
    ):
        errors.add("semantics")
    if not required["is_paused"].map(_is_canonical_bool).all():
        errors.add("semantics")
    return errors


def _is_canonical_bool(value) -> bool:
    return isinstance(value, bool) or type(value).__name__ == "bool_"


def _empty_read_back_verification(passed: bool, blocked_reasons: list[str]) -> dict[str, Any]:
    return {
        "passed": passed,
        "expected_promoted_rows": 0,
        "actual_promoted_rows": 0,
        "expected_total_rows": 0,
        "actual_total_rows": 0,
        "expected_trade_dates": [],
        "actual_promoted_trade_dates": [],
        "blocked_reasons": _dedupe(blocked_reasons),
    }


def _idempotency_key(batch_id: str, candidate: pd.DataFrame | None) -> str:
    if candidate is None:
        payload = batch_id
    elif {"ts_code", "trade_date"}.issubset(candidate.columns):
        ordered = candidate[["ts_code", "trade_date"]].astype(str).sort_values(["ts_code", "trade_date"])
        payload = batch_id + "\n" + ordered.to_csv(index=False)
    else:
        payload = batch_id + f"\nrows={len(candidate)}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _default_source_object_keys(batch_id: str) -> dict[str, str]:
    return {
        "promotion_preflight_report": f"candidate/tushare/promotion_preflight_report/batch_id={batch_id}/report.json",
        "daily_price_candidate_batch": f"candidate/tushare/daily_price_candidate_batch/batch_id={batch_id}/part.parquet",
        "suspension_status_candidate_batch": f"candidate/tushare/suspension_status_candidate_batch/batch_id={batch_id}/part.parquet",
    }


def _validate_batch_id(batch_id: str) -> str:
    value = str(batch_id).strip()
    if not value:
        raise ValueError("batch_id must not be empty")
    if any(char in value for char in "\\/:*?\"<>|"):
        raise ValueError("batch_id contains unsupported path characters")
    return value


def _dedupe(values: list[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

from collections.abc import Callable
from datetime import datetime, timezone
import hashlib
from typing import Any

import pandas as pd

from stock_selector.data.data_validator import DataValidationError, validate_dataset_frame


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
    standard_daily_price_write_fn: StandardDailyPriceWriteFn | None = None,
    generated_at_fn: GeneratedAtFn | None = None,
    source_object_keys: dict[str, str] | None = None,
) -> dict[str, Any]:
    generated_at_fn = generated_at_fn or _utc_now_iso
    generated_at = generated_at_fn()
    batch_id = _validate_batch_id(batch_id)
    output_keys = build_tushare_daily_price_promotion_validator_output_keys(batch_id)
    if execute_standard_write:
        output_keys["standard_daily_price_promotion_execution_report"] = (
            f"candidate/tushare/standard_daily_price_promotion_execution_report/batch_id={batch_id}/report.json"
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

    if request_standard_write and not execute_standard_write:
        blocked_reasons.append("STANDARD_WRITE_REQUIRES_EXPLICIT_EXECUTE_FLAG")
    if execute_standard_write and standard_daily_price_write_fn is None:
        blocked_reasons.append("STANDARD_WRITE_REQUIRES_EXPLICIT_EXECUTE_FLAG")

    blocked_reasons = _dedupe(blocked_reasons)
    status = VALIDATOR_PASS if not blocked_reasons else BLOCKED
    validation_passed_before_write = status == VALIDATOR_PASS

    standard_write_performed = False
    standard_write_results: list[dict[str, Any]] = []
    if execute_standard_write and validation_passed_before_write and standard_daily_price_write_fn is not None:
        for trade_date in sorted(standard_daily_price_frames):
            frame = standard_daily_price_frames[trade_date]
            object_key = standard_daily_price_write_fn("daily_price", trade_date, frame)
            standard_write_results.append(
                {"dataset": "daily_price", "trade_date": trade_date, "row_count": int(len(frame)), "object_key": object_key}
            )
        standard_write_performed = bool(standard_write_results)

    dry_run_report = _build_dry_run_report(
        batch_id=batch_id,
        generated_at=generated_at,
        validator_status=status,
        candidate=candidate,
        blocked_reasons=blocked_reasons,
        idempotency_key=_idempotency_key(batch_id, candidate),
    )
    execution_report = None
    if execute_standard_write:
        execution_report = _build_execution_report(
            batch_id=batch_id,
            generated_at=generated_at,
            validator_status=status,
            blocked_reasons=blocked_reasons,
            standard_write_performed=standard_write_performed,
            standard_write_results=standard_write_results,
            idempotency_key=dry_run_report["idempotency_key"],
        )
    validator_report = {
        "schema_version": "goal14.daily_price_promotion_validator_report.v1",
        "goal": "14",
        "provider": "tushare",
        "batch_id": batch_id,
        "generated_at": generated_at,
        "status": status,
        "source_reports": {
            "promotion_preflight_report": source_object_keys.get("promotion_preflight_report"),
            "daily_price_candidate_batch": source_object_keys.get("daily_price_candidate_batch"),
            "suspension_status_candidate_batch": source_object_keys.get("suspension_status_candidate_batch"),
        },
        "small_range_guard": small_range_guard,
        "coverage_check": coverage_check,
        "pause_resolution_check": pause_resolution_check,
        "schema_contract_check": schema_contract_check,
        "blocked_reasons": blocked_reasons,
        "blocked_reason_catalog": list(BLOCKED_REASON_CATALOG),
        "standard_daily_price_write_allowed_with_explicit_flag": validation_passed_before_write,
        "standard_daily_price_write_performed": standard_write_performed,
        "standard_daily_price_write_results": standard_write_results,
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
        "goal": "14",
        "provider": "tushare",
        "batch_id": batch_id,
        "status": status,
        "output_object_keys": output_keys,
        "daily_price_promotion_validator_report": validator_report,
        "standard_daily_price_promotion_dry_run_report": dry_run_report,
        "standard_daily_price_promotion_execution_report": execution_report,
        "blocked_reasons": blocked_reasons,
        "standard_daily_price_write_performed": standard_write_performed,
        "standard_suspension_status_write_performed": False,
        "real_backtest_performed": False,
        "clean_factor_selection_backtest_entered": False,
        "candidate_row_count": row_count,
        "standard_write_results": standard_write_results,
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


def _build_execution_report(
    *,
    batch_id: str,
    generated_at: str,
    validator_status: str,
    blocked_reasons: list[str],
    standard_write_performed: bool,
    standard_write_results: list[dict[str, Any]],
    idempotency_key: str,
) -> dict[str, Any]:
    written_rows = sum(int(item.get("row_count", 0)) for item in standard_write_results)
    return {
        "schema_version": "goal14.standard_daily_price_promotion_execution_report.v1",
        "goal": "14",
        "provider": "tushare",
        "batch_id": batch_id,
        "generated_at": generated_at,
        "mode": "EXECUTE",
        "validator_status": validator_status,
        "target_table": "daily_price",
        "standard_write_performed": standard_write_performed,
        "written_rows": written_rows,
        "write_results": standard_write_results,
        "idempotency_key": idempotency_key,
        "standard_suspension_status_write_performed": False,
        "clean_performed": False,
        "factor_performed": False,
        "selection_performed": False,
        "real_backtest_performed": False,
        "blocked_reasons": blocked_reasons,
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

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import pandas as pd

from stock_selector.data.quality_contract import (
    DataQualityLevel,
    PauseEvidence,
    PauseStatus,
    SUSPENSION_STATUS_CANDIDATE_REQUIRED_INPUTS,
    SuspensionCoverageStatus,
    can_build_suspension_status_candidate,
    can_promote_suspension_status_candidate,
    can_use_suspend_miss_as_false_candidate,
)
from stock_selector.utils.date_validator import validate_trade_date


@dataclass(frozen=True)
class SuspensionCandidateInput:
    dataset: str
    object_key: str
    frame: pd.DataFrame | None = None
    payload: dict[str, Any] | None = None


@dataclass(frozen=True)
class SuspensionCoverageMetadata:
    full_event_coverage_proven: bool = False
    api_total_rows_if_known: int | None = None
    rows_written_if_known: int | None = None


LoadSuspensionCandidateInputFn = Callable[[str, str], SuspensionCandidateInput]

JOIN_KEYS = ["ts_code", "trade_date"]

CANDIDATE_REQUIRED_FIELDS = ("ts_code", "trade_date")
TRADE_CAL_ALIASES = {
    "trade_date": ("trade_date", "cal_date"),
    "is_open": ("is_open",),
    "exchange": ("exchange",),
}
SUSPEND_D_ALIASES = {
    "ts_code": ("ts_code",),
    "trade_date": ("suspend_date", "trade_date"),
    "suspend_type": ("suspend_type",),
    "suspend_timing": ("suspend_timing",),
}

SAFETY_FLAGS = {
    "standard_daily_price_written": False,
    "standard_suspension_status_written": False,
    "real_raw_mainline_written": False,
    "cleaning_mainline_entered": False,
    "factor_mainline_entered": False,
    "selection_mainline_entered": False,
    "backtest_mainline_entered": False,
    "spring_api_changed": False,
    "is_paused_fabricated": False,
    "suspend_miss_inferred_as_false_without_coverage": False,
}

INFERENCE_GUARDS = {
    "volume_used_as_pause": False,
    "amount_used_as_pause": False,
    "missing_daily_used_as_pause": False,
    "unchanged_price_used_as_pause": False,
    "suspend_d_miss_used_as_false_without_coverage": False,
}


def build_suspension_status_candidate_output_keys(trade_date: str) -> dict[str, str]:
    trade_date = validate_trade_date(trade_date)
    return {
        "candidate": f"candidate/tushare/suspension_status_candidate/trade_date={trade_date}/part.parquet",
        "coverage_audit": f"candidate/tushare/suspension_status_coverage_audit/trade_date={trade_date}/report.json",
    }


def build_tushare_suspension_status_candidate(
    trade_date: str,
    *,
    sample_limit: int,
    load_input_fn: LoadSuspensionCandidateInputFn,
    coverage_metadata: SuspensionCoverageMetadata | None = None,
) -> dict[str, Any]:
    trade_date = validate_trade_date(trade_date)
    if sample_limit <= 0:
        raise ValueError("sample_limit must be positive")

    coverage_metadata = coverage_metadata or SuspensionCoverageMetadata()
    inputs = [load_input_fn(dataset, trade_date) for dataset in SUSPENSION_STATUS_CANDIDATE_REQUIRED_INPUTS]
    input_map = {item.dataset: item for item in inputs}
    output_keys = build_suspension_status_candidate_output_keys(trade_date)
    missing_inputs = _missing_inputs(inputs)
    input_object_keys = {item.dataset: item.object_key for item in inputs}
    input_row_counts = _input_row_counts(inputs)

    if missing_inputs:
        return _blocked_report(
            trade_date=trade_date,
            sample_limit=sample_limit,
            input_object_keys=input_object_keys,
            input_row_counts=input_row_counts,
            output_keys=output_keys,
            missing_inputs=missing_inputs,
            status="BLOCKED_BY_MISSING_INPUT",
            coverage_status=SuspensionCoverageStatus.MISSING_INPUT,
            blocked_reasons=[f"missing input: {item['object_key']}" for item in missing_inputs],
        )

    available_inputs = [item.dataset for item in inputs]
    if not can_build_suspension_status_candidate(available_inputs):
        return _blocked_report(
            trade_date=trade_date,
            sample_limit=sample_limit,
            input_object_keys=input_object_keys,
            input_row_counts=input_row_counts,
            output_keys=output_keys,
            missing_inputs=missing_inputs,
            status="BLOCKED_BY_MISSING_INPUT",
            coverage_status=SuspensionCoverageStatus.MISSING_INPUT,
            blocked_reasons=["not all required suspension candidate inputs are available"],
        )

    schema_check = _schema_check(input_map)
    if not schema_check["valid"]:
        return _blocked_report(
            trade_date=trade_date,
            sample_limit=sample_limit,
            input_object_keys=input_object_keys,
            input_row_counts=input_row_counts,
            output_keys=output_keys,
            missing_inputs=[],
            status="BLOCKED_BY_SCHEMA_MISMATCH",
            coverage_status=SuspensionCoverageStatus.SCHEMA_MISMATCH,
            blocked_reasons=schema_check["blocked_reasons"],
            schema_check=schema_check,
        )

    daily_price_candidate = _normalize_candidate(input_map["daily_price_candidate"].frame, trade_date)
    trade_cal = _normalize_trade_cal(input_map["trade_cal"].frame, trade_date)
    suspend_d = _normalize_suspend_d(input_map["suspend_d"].frame, trade_date)
    trade_cal_valid = _trade_cal_valid(trade_cal)
    event_coverage = _suspend_d_event_coverage(suspend_d, sample_limit, coverage_metadata, trade_cal_valid)
    candidate = _build_candidate_rows(
        daily_price_candidate=daily_price_candidate,
        suspend_d=suspend_d,
        trade_cal_valid=trade_cal_valid,
        event_coverage=event_coverage,
        input_object_keys=input_object_keys,
    )
    pause_status_counts = _pause_status_counts(candidate)
    evidence_counts = _evidence_counts(candidate)
    blocked_reasons = _blocked_reasons(event_coverage, pause_status_counts, trade_cal_valid)
    readiness = _readiness(event_coverage, candidate, blocked_reasons)

    return {
        "status": "CANDIDATE_AUDIT_COMPLETED_NOT_PROMOTABLE",
        "provider": "tushare",
        "goal": "12D",
        "trade_date": trade_date,
        "sample_limit": int(sample_limit),
        "input_object_keys": input_object_keys,
        "output_object_keys": output_keys,
        "input_row_counts": input_row_counts,
        "schema_check": schema_check,
        "coverage_universe": {
            "source": "daily_price_candidate_dry_run",
            "row_count": int(len(daily_price_candidate)),
            "is_full_market_universe": False,
            "is_explicit_candidate_universe": True,
            "sample_limit": int(sample_limit),
            "source_report_status": (input_map["daily_price_candidate_report"].payload or {}).get("readiness", {}).get("status"),
        },
        "suspend_d_event_coverage": event_coverage,
        "candidate_row_count": int(len(candidate)),
        "pause_status_counts": pause_status_counts,
        "evidence_counts": evidence_counts,
        "blocked_reasons": blocked_reasons,
        "readiness": readiness,
        "safety": SAFETY_FLAGS,
        "inference_guards": INFERENCE_GUARDS,
        "candidate_rows": _records(candidate),
    }


def candidate_frame_from_report(report: dict[str, Any]) -> pd.DataFrame:
    return pd.DataFrame(report.get("candidate_rows", []))


def _missing_inputs(inputs: list[SuspensionCandidateInput]) -> list[dict[str, str]]:
    result = []
    for item in inputs:
        if item.dataset == "daily_price_candidate_report":
            missing = item.payload is None
        else:
            missing = item.frame is None
        if missing:
            result.append({"dataset": item.dataset, "object_key": item.object_key})
    return result


def _input_row_counts(inputs: list[SuspensionCandidateInput]) -> dict[str, int]:
    counts = {}
    for item in inputs:
        if item.frame is not None:
            counts[item.dataset] = int(len(item.frame))
        elif item.payload is not None:
            counts[item.dataset] = 1
        else:
            counts[item.dataset] = 0
    return counts


def _blocked_report(
    *,
    trade_date: str,
    sample_limit: int,
    input_object_keys: dict[str, str],
    input_row_counts: dict[str, int],
    output_keys: dict[str, str],
    missing_inputs: list[dict[str, str]],
    status: str,
    coverage_status: SuspensionCoverageStatus,
    blocked_reasons: list[str],
    schema_check: dict[str, Any] | None = None,
) -> dict[str, Any]:
    coverage_status_value = coverage_status.value
    return {
        "status": status,
        "provider": "tushare",
        "goal": "12D",
        "trade_date": trade_date,
        "sample_limit": int(sample_limit),
        "input_object_keys": input_object_keys,
        "output_object_keys": output_keys,
        "input_row_counts": input_row_counts,
        "schema_check": schema_check or {"valid": False, "checks": {}, "blocked_reasons": blocked_reasons},
        "missing_inputs": missing_inputs,
        "coverage_universe": {
            "source": "daily_price_candidate_dry_run",
            "row_count": 0,
            "is_full_market_universe": False,
            "is_explicit_candidate_universe": False,
            "sample_limit": int(sample_limit),
        },
        "suspend_d_event_coverage": {
            "row_count": 0,
            "event_count": 0,
            "has_rows": False,
            "is_sample_truncated": coverage_status == SuspensionCoverageStatus.SAMPLE_TRUNCATED,
            "api_total_rows_if_known": None,
            "rows_written_if_known": 0,
            "full_coverage_proven": False,
            "coverage_status": coverage_status_value,
            "coverage_block_reason": "; ".join(blocked_reasons),
        },
        "candidate_row_count": 0,
        "pause_status_counts": {"true_candidate": 0, "false_candidate": 0, "unknown": 0},
        "evidence_counts": _empty_evidence_counts(),
        "blocked_reasons": blocked_reasons,
        "readiness": {
            "ready_for_dq3_promotion": False,
            "status": status,
            "blocked_reasons": blocked_reasons,
            "required_future_gates": [],
        },
        "safety": SAFETY_FLAGS,
        "inference_guards": INFERENCE_GUARDS,
        "candidate_rows": [],
    }


def _schema_check(input_map: dict[str, SuspensionCandidateInput]) -> dict[str, Any]:
    checks = {
        "daily_price_candidate": _required_field_check(input_map["daily_price_candidate"].frame, CANDIDATE_REQUIRED_FIELDS),
        "trade_cal": _alias_field_check(input_map["trade_cal"].frame, TRADE_CAL_ALIASES),
        "suspend_d": _alias_field_check(input_map["suspend_d"].frame, SUSPEND_D_ALIASES),
        "daily_price_candidate_report": {"missing_required_fields": [], "valid": input_map["daily_price_candidate_report"].payload is not None},
    }
    blocked_reasons = []
    for dataset, check in checks.items():
        if check["missing_required_fields"]:
            blocked_reasons.append(f"{dataset} missing fields: {', '.join(check['missing_required_fields'])}")
    return {"valid": not blocked_reasons, "checks": checks, "blocked_reasons": blocked_reasons}


def _required_field_check(frame: pd.DataFrame | None, fields: tuple[str, ...]) -> dict[str, Any]:
    columns = set(frame.columns) if frame is not None else set()
    missing = [field for field in fields if field not in columns]
    return {"missing_required_fields": missing, "valid": not missing}


def _alias_field_check(frame: pd.DataFrame | None, aliases: dict[str, tuple[str, ...]]) -> dict[str, Any]:
    missing = []
    selected = {}
    for field, options in aliases.items():
        column = _pick_column(frame, options) if frame is not None else None
        selected[field] = column
        if column is None:
            missing.append(field)
    return {"missing_required_fields": missing, "selected_columns": selected, "valid": not missing}


def _normalize_candidate(frame: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    data = frame.copy()
    data["trade_date"] = data["trade_date"].map(_normalize_tushare_date)
    return data[data["trade_date"] == trade_date].copy()


def _normalize_trade_cal(frame: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    data = pd.DataFrame()
    data["trade_date"] = frame[_pick_column(frame, TRADE_CAL_ALIASES["trade_date"])].map(_normalize_tushare_date)
    data["is_open"] = frame[_pick_column(frame, TRADE_CAL_ALIASES["is_open"])]
    data["exchange"] = frame[_pick_column(frame, TRADE_CAL_ALIASES["exchange"])]
    return data[data["trade_date"] == trade_date].copy()


def _normalize_suspend_d(frame: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    data = pd.DataFrame()
    for field, aliases in SUSPEND_D_ALIASES.items():
        data[field] = frame[_pick_column(frame, aliases)]
    data["trade_date"] = data["trade_date"].map(_normalize_tushare_date)
    return data[data["trade_date"] == trade_date].copy()


def _trade_cal_valid(trade_cal: pd.DataFrame) -> bool:
    if trade_cal.empty:
        return False
    return bool((pd.to_numeric(trade_cal["is_open"], errors="coerce") == 1).any())


def _suspend_d_event_coverage(
    suspend_d: pd.DataFrame,
    sample_limit: int,
    metadata: SuspensionCoverageMetadata,
    trade_cal_valid: bool,
) -> dict[str, Any]:
    row_count = int(len(suspend_d))
    event_count = int(suspend_d[JOIN_KEYS].drop_duplicates().shape[0]) if row_count else 0
    rows_written = metadata.rows_written_if_known if metadata.rows_written_if_known is not None else row_count
    api_total = metadata.api_total_rows_if_known
    is_sample_truncated = False
    block_reason = "full event coverage has not been proven"
    status = SuspensionCoverageStatus.COVERAGE_UNKNOWN

    if api_total is not None and rows_written is not None and rows_written < api_total:
        is_sample_truncated = True
        status = SuspensionCoverageStatus.SAMPLE_TRUNCATED
        block_reason = "suspend_d rows_written is less than api_total_rows"
    elif not metadata.full_event_coverage_proven and row_count >= sample_limit:
        is_sample_truncated = True
        status = SuspensionCoverageStatus.SAMPLE_TRUNCATED
        block_reason = "suspend_d row_count reached sample_limit; treat as sample-truncated until full coverage metadata is persisted"
    elif metadata.full_event_coverage_proven and trade_cal_valid:
        status = SuspensionCoverageStatus.FULL_EVENT_COVERAGE
        block_reason = ""
    elif not trade_cal_valid:
        block_reason = "trade_cal did not confirm an open trading day"

    return {
        "row_count": row_count,
        "event_count": event_count,
        "has_rows": row_count > 0,
        "is_sample_truncated": is_sample_truncated,
        "api_total_rows_if_known": api_total,
        "rows_written_if_known": rows_written,
        "full_coverage_proven": status == SuspensionCoverageStatus.FULL_EVENT_COVERAGE,
        "coverage_status": status.value,
        "coverage_block_reason": block_reason,
    }


def _build_candidate_rows(
    *,
    daily_price_candidate: pd.DataFrame,
    suspend_d: pd.DataFrame,
    trade_cal_valid: bool,
    event_coverage: dict[str, Any],
    input_object_keys: dict[str, str],
) -> pd.DataFrame:
    event_frame = suspend_d.drop_duplicates(JOIN_KEYS, keep="first").copy()
    event_frame["_event_match"] = True
    event_columns = JOIN_KEYS + ["suspend_type", "suspend_timing", "_event_match"]
    frame = daily_price_candidate.merge(event_frame[event_columns], on=JOIN_KEYS, how="left")
    coverage_status = SuspensionCoverageStatus(event_coverage["coverage_status"])
    schema_valid = coverage_status != SuspensionCoverageStatus.SCHEMA_MISMATCH

    statuses = []
    paused_values = []
    evidence_values = []
    event_matches = []
    promotable_values = []
    event_match_values = [bool(value) if pd.notna(value) else False for value in frame["_event_match"]]
    for event_match in event_match_values:
        event_matches.append(event_match)
        if event_match:
            statuses.append(PauseStatus.TRUE_CANDIDATE.value)
            paused_values.append(True)
            evidence_values.append(PauseEvidence.SUSPEND_D_MATCH.value)
            promotable_values.append(False)
            continue
        can_false = can_use_suspend_miss_as_false_candidate(
            coverage_status=coverage_status,
            trade_cal_valid=trade_cal_valid,
            schema_valid=schema_valid,
            volume_used_as_pause=False,
            amount_used_as_pause=False,
            missing_daily_used_as_pause=False,
            unchanged_price_used_as_pause=False,
        )
        if can_false:
            statuses.append(PauseStatus.FALSE_CANDIDATE.value)
            paused_values.append(False)
            evidence_values.append(PauseEvidence.FULL_EVENT_COVERAGE_NO_MATCH.value)
            promotable_values.append(True)
        else:
            statuses.append(PauseStatus.UNKNOWN.value)
            paused_values.append(None)
            if coverage_status == SuspensionCoverageStatus.SAMPLE_TRUNCATED:
                evidence_values.append(PauseEvidence.BLOCKED_BY_SAMPLE_TRUNCATED_SUSPEND_D.value)
            else:
                evidence_values.append(PauseEvidence.UNRESOLVED_NO_EVENT_MATCH.value)
            promotable_values.append(False)

    result = pd.DataFrame(
        {
            "ts_code": frame["ts_code"],
            "trade_date": frame["trade_date"],
            "provider": "tushare",
            "pause_status": statuses,
            "is_paused_candidate": paused_values,
            "pause_evidence": evidence_values,
            "event_match": event_matches,
            "event_source_object_key": input_object_keys["suspend_d"],
            "calendar_source_object_key": input_object_keys["trade_cal"],
            "candidate_source_object_key": input_object_keys["daily_price_candidate"],
            "coverage_status": coverage_status.value,
            "coverage_block_reason": event_coverage["coverage_block_reason"],
            "dq_level": DataQualityLevel.DQ1.value,
            "is_standard": False,
            "is_promotable": promotable_values,
            "generated_at": pd.Timestamp.utcnow().isoformat(),
        }
    )
    for source_column in ("open", "high", "low", "close", "pre_close", "volume", "amount"):
        if source_column in frame.columns:
            result[source_column] = frame[source_column]
    for source_column in ("suspend_type", "suspend_timing"):
        if source_column in frame.columns:
            result[source_column] = frame[source_column].astype(object).where(pd.notna(frame[source_column]), None)
    return result


def _pause_status_counts(candidate: pd.DataFrame) -> dict[str, int]:
    counts = candidate["pause_status"].value_counts(dropna=False).to_dict()
    return {
        "true_candidate": int(counts.get(PauseStatus.TRUE_CANDIDATE.value, 0)),
        "false_candidate": int(counts.get(PauseStatus.FALSE_CANDIDATE.value, 0)),
        "unknown": int(counts.get(PauseStatus.UNKNOWN.value, 0)),
    }


def _evidence_counts(candidate: pd.DataFrame) -> dict[str, int]:
    counts = candidate["pause_evidence"].value_counts(dropna=False).to_dict() if "pause_evidence" in candidate.columns else {}
    result = _empty_evidence_counts()
    for key in result:
        result[key] = int(counts.get(key, 0))
    return result


def _empty_evidence_counts() -> dict[str, int]:
    return {
        PauseEvidence.SUSPEND_D_MATCH.value: 0,
        PauseEvidence.FULL_EVENT_COVERAGE_NO_MATCH.value: 0,
        PauseEvidence.UNRESOLVED_NO_EVENT_MATCH.value: 0,
        PauseEvidence.BLOCKED_BY_SAMPLE_TRUNCATED_SUSPEND_D.value: 0,
        PauseEvidence.BLOCKED_BY_MISSING_COVERAGE_METADATA.value: 0,
    }


def _blocked_reasons(event_coverage: dict[str, Any], pause_status_counts: dict[str, int], trade_cal_valid: bool) -> list[str]:
    reasons = []
    coverage_status = SuspensionCoverageStatus(event_coverage["coverage_status"])
    if coverage_status == SuspensionCoverageStatus.SAMPLE_TRUNCATED:
        reasons.append("suspend_d event source coverage is incomplete because the smoke input is sample-truncated")
    elif coverage_status == SuspensionCoverageStatus.COVERAGE_UNKNOWN:
        reasons.append("suspend_d event source full coverage is not proven")
    if pause_status_counts["unknown"]:
        reasons.append("pause_status contains unresolved unknown rows")
    if not trade_cal_valid:
        reasons.append("trade_cal did not confirm an open trading day")
    return reasons


def _readiness(event_coverage: dict[str, Any], candidate: pd.DataFrame, blocked_reasons: list[str]) -> dict[str, Any]:
    promotion = can_promote_suspension_status_candidate(
        coverage_status=event_coverage["coverage_status"],
        pause_statuses=list(candidate["pause_status"]) if "pause_status" in candidate.columns else [],
        validator_passed=False,
        dq_level=DataQualityLevel.DQ1,
    )
    reasons = list(dict.fromkeys(blocked_reasons + list(promotion.reasons)))
    return {
        "ready_for_dq3_promotion": False,
        "status": promotion.status,
        "blocked_reasons": reasons,
        "required_future_gates": list(promotion.required_future_gates),
    }


def _pick_column(frame: pd.DataFrame, aliases: tuple[str, ...]) -> str | None:
    columns = set(frame.columns)
    for alias in aliases:
        if alias in columns:
            return alias
    return None


def _normalize_tushare_date(value) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
    try:
        return validate_trade_date(text)
    except Exception:
        return text


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    sanitized = frame.astype(object).where(pd.notna(frame), None)
    records = sanitized.to_dict(orient="records")
    return [{key: _json_scalar(value) for key, value in row.items()} for row in records]


def _json_scalar(value):
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value

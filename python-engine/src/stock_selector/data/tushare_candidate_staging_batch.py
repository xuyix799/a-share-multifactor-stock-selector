from collections.abc import Callable
from dataclasses import asdict
from datetime import datetime, timezone
from time import sleep
from typing import Any

import pandas as pd

from stock_selector.data.quality_contract import (
    DataQualityLevel,
    PauseEvidence,
    PauseStatus,
    TUSHARE_CANDIDATE_BATCH_REQUIRED_INTERFACES,
    can_mark_tushare_candidate_batch_ready_for_promotion_validator,
    classify_tushare_candidate_batch,
)
from stock_selector.utils.date_validator import validate_date_range


WriteParquetFn = Callable[[str, pd.DataFrame], str]
WriteJsonFn = Callable[[str, dict[str, Any]], str]
SleepFn = Callable[[float], None]
GeneratedAtFn = Callable[[], str]

JOIN_KEYS = ["ts_code", "trade_date"]
PER_CODE_FETCH_INTERFACES = ("daily", "stk_limit", "adj_factor", "daily_basic")
RANGE_FETCH_INTERFACES = ("suspend_d",)

INTERFACE_FIELDS = {
    "daily": "ts_code,trade_date,open,high,low,close,pre_close,vol,amount",
    "stk_limit": "ts_code,trade_date,up_limit,down_limit",
    "adj_factor": "ts_code,trade_date,adj_factor",
    "daily_basic": "ts_code,trade_date,pe_ttm,pb,ps_ttm,total_mv,circ_mv,turnover_rate",
    "trade_cal": "exchange,cal_date,is_open,pretrade_date",
    "suspend_d": "ts_code,trade_date,suspend_timing,suspend_type",
}

FIELD_ALIASES = {
    "daily": {
        "ts_code": ("ts_code",),
        "trade_date": ("trade_date",),
        "open": ("open",),
        "high": ("high",),
        "low": ("low",),
        "close": ("close",),
        "pre_close": ("pre_close",),
        "volume": ("volume", "vol"),
        "amount": ("amount",),
    },
    "stk_limit": {
        "ts_code": ("ts_code",),
        "trade_date": ("trade_date",),
        "limit_up": ("limit_up", "up_limit"),
        "limit_down": ("limit_down", "down_limit"),
    },
    "adj_factor": {
        "ts_code": ("ts_code",),
        "trade_date": ("trade_date",),
        "adj_factor": ("adj_factor",),
    },
    "daily_basic": {
        "ts_code": ("ts_code",),
        "trade_date": ("trade_date",),
        "pe_ttm": ("pe_ttm",),
        "pb": ("pb",),
        "ps_ttm": ("ps_ttm",),
        "total_mv": ("total_mv",),
        "circ_mv": ("circ_mv",),
        "turnover_rate": ("turnover_rate",),
    },
    "trade_cal": {
        "trade_date": ("trade_date", "cal_date"),
        "is_open": ("is_open",),
        "exchange": ("exchange",),
    },
    "suspend_d": {
        "ts_code": ("ts_code",),
        "trade_date": ("suspend_date", "trade_date"),
    },
}

OPTIONAL_ALIASES = {
    "trade_cal": {"pretrade_date": ("pretrade_date",)},
    "suspend_d": {
        "suspend_type": ("suspend_type",),
        "suspend_timing": ("suspend_timing",),
        "resume_date": ("resume_date",),
    },
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

FIELD_SOURCES = {
    "open": "daily",
    "high": "daily",
    "low": "daily",
    "close": "daily",
    "pre_close": "daily",
    "volume": "daily",
    "amount": "daily",
    "limit_up": "stk_limit",
    "limit_down": "stk_limit",
    "adj_factor": "adj_factor",
    "pe_ttm": "daily_basic",
    "pb": "daily_basic",
    "ps_ttm": "daily_basic",
    "total_mv": "daily_basic",
    "circ_mv": "daily_basic",
    "turnover_rate": "daily_basic",
    "trading_day_confirmed": "trade_cal",
    "pause_status": "suspend_d_hit_or_unresolved_unknown",
    "is_paused_candidate": "suspend_d_hit_only_unless_full_event_coverage_is_proven",
    "pause_evidence": "suspend_d",
}


def build_tushare_candidate_staging_batch_output_keys(batch_id: str, trade_dates: list[str]) -> dict[str, Any]:
    batch_id = _validate_batch_id(batch_id)
    keys: dict[str, Any] = {
        "manifest": f"candidate/tushare/batch_manifest/batch_id={batch_id}/manifest.json",
        "daily_staging": {},
        "stk_limit_staging": {},
        "adj_factor_staging": {},
        "daily_basic_staging": {},
        "trade_cal_staging": f"candidate/tushare/trade_cal_staging/batch_id={batch_id}/part.parquet",
        "suspend_d_staging": f"candidate/tushare/suspend_d_staging/batch_id={batch_id}/part.parquet",
        "daily_price_candidate_batch": f"candidate/tushare/daily_price_candidate_batch/batch_id={batch_id}/part.parquet",
        "suspension_status_candidate_batch": f"candidate/tushare/suspension_status_candidate_batch/batch_id={batch_id}/part.parquet",
        "provider_coverage_report": f"candidate/tushare/provider_coverage_report/batch_id={batch_id}/report.json",
        "dq3_readiness_audit": f"candidate/tushare/dq3_readiness_audit/batch_id={batch_id}/report.json",
    }
    for trade_date in trade_dates:
        for dataset in ("daily", "stk_limit", "adj_factor", "daily_basic"):
            keys[f"{dataset}_staging"][trade_date] = (
                f"candidate/tushare/{dataset}_staging/batch_id={batch_id}/trade_date={trade_date}/part.parquet"
            )
    return keys


def build_tushare_candidate_staging_batch_blocked_report(
    *,
    start_date: str,
    end_date: str,
    codes: list[str],
    status: str,
    blocked_reasons: list[str],
    batch_id: str | None = None,
    generated_at_fn: GeneratedAtFn | None = None,
) -> dict[str, Any]:
    generated_at_fn = generated_at_fn or _utc_now_iso
    start_date, end_date = validate_date_range(start_date, end_date)
    batch_id = _validate_batch_id(batch_id or _default_batch_id(start_date, end_date, generated_at_fn()))
    return {
        "status": status,
        "provider": "tushare",
        "goal": "13",
        "batch_id": batch_id,
        "start_date": start_date,
        "end_date": end_date,
        "codes": list(codes),
        "trade_dates": [],
        "generated_at": generated_at_fn(),
        "blocked_reasons": blocked_reasons,
        "interfaces_requested": list(TUSHARE_CANDIDATE_BATCH_REQUIRED_INTERFACES),
        "interfaces_succeeded": [],
        "interfaces_failed": list(TUSHARE_CANDIDATE_BATCH_REQUIRED_INTERFACES),
        "staging_row_counts": _zero_row_counts(),
        "daily_price_candidate_row_count": 0,
        "suspension_status_candidate_row_count": 0,
        "coverage_summary": _empty_coverage_summary(0),
        "ready_for_promotion_validator": False,
        "ready_for_dq3_promotion": False,
        "safety": SAFETY_FLAGS,
        "inference_guards": INFERENCE_GUARDS,
        "manifest": {
            "batch_id": batch_id,
            "provider": "tushare",
            "is_standard": False,
            "is_promotable": False,
            "status": status,
        },
    }


def build_tushare_candidate_staging_batch(
    *,
    start_date: str,
    end_date: str,
    codes: list[str],
    provider,
    write_parquet_fn: WriteParquetFn,
    write_json_fn: WriteJsonFn,
    batch_id: str | None = None,
    sleep_seconds: float = 12.0,
    sleeper: SleepFn = sleep,
    max_codes: int | None = None,
    max_trade_days: int | None = None,
    no_provider_call: bool = False,
    suspend_d_full_event_coverage_proven: bool = False,
    generated_at_fn: GeneratedAtFn | None = None,
    cli_command: str | None = None,
) -> dict[str, Any]:
    generated_at_fn = generated_at_fn or _utc_now_iso
    start_date, end_date = validate_date_range(start_date, end_date)
    if sleep_seconds < 0:
        raise ValueError("sleep_seconds must be non-negative")
    codes = _normalize_codes(codes)
    if max_codes is not None:
        if max_codes <= 0:
            raise ValueError("max_codes must be positive")
        codes = codes[:max_codes]
    if max_trade_days is not None and max_trade_days <= 0:
        raise ValueError("max_trade_days must be positive")
    batch_id = _validate_batch_id(batch_id or _default_batch_id(start_date, end_date, generated_at_fn()))
    generated_at = generated_at_fn()

    if no_provider_call:
        return build_tushare_candidate_staging_batch_blocked_report(
            start_date=start_date,
            end_date=end_date,
            codes=codes,
            batch_id=batch_id,
            status="BLOCKED_BY_NO_PROVIDER_CALL",
            blocked_reasons=["--no-provider-call was set; no Tushare provider request was made"],
            generated_at_fn=lambda: generated_at,
        )

    fetch_result = _fetch_provider_frames(
        provider=provider,
        start_date=start_date,
        end_date=end_date,
        codes=codes,
        sleep_seconds=sleep_seconds,
        sleeper=sleeper,
    )
    raw_frames = fetch_result["frames"]
    provider_errors = fetch_result["provider_errors"]

    trade_cal = _normalize_trade_cal(raw_frames.get("trade_cal", pd.DataFrame()), start_date, end_date)
    trade_dates = _open_trade_dates(trade_cal)
    if max_trade_days is not None:
        trade_dates = trade_dates[:max_trade_days]
    expected_keys = _expected_keys(codes, trade_dates)
    output_keys = build_tushare_candidate_staging_batch_output_keys(batch_id, trade_dates)

    normalized = _normalize_all(raw_frames, codes, trade_dates)
    schema_checks = _schema_checks(raw_frames)
    duplicate_checks = _duplicate_key_checks(normalized)
    staging_row_counts = {dataset: int(len(frame)) for dataset, frame in normalized.items()}
    coverage_report = _build_coverage_report(
        batch_id=batch_id,
        start_date=start_date,
        end_date=end_date,
        codes=codes,
        trade_dates=trade_dates,
        expected_keys=expected_keys,
        normalized=normalized,
        schema_checks=schema_checks,
        duplicate_checks=duplicate_checks,
        provider_errors=provider_errors,
        suspend_d_full_event_coverage_proven=suspend_d_full_event_coverage_proven,
        generated_at=generated_at,
    )

    if provider_errors:
        return _blocked_provider_error_report(
            batch_id=batch_id,
            start_date=start_date,
            end_date=end_date,
            codes=codes,
            trade_dates=trade_dates,
            output_keys=output_keys,
            staging_row_counts=staging_row_counts,
            coverage_report=coverage_report,
            provider_errors=provider_errors,
            generated_at=generated_at,
        )

    if _schema_blocked(schema_checks):
        blocked_reasons = [f"{dataset} missing fields: {', '.join(check['missing_required_fields'])}" for dataset, check in schema_checks.items() if check["missing_required_fields"]]
        return _blocked_schema_report(
            batch_id=batch_id,
            start_date=start_date,
            end_date=end_date,
            codes=codes,
            trade_dates=trade_dates,
            output_keys=output_keys,
            staging_row_counts=staging_row_counts,
            coverage_report=coverage_report,
            blocked_reasons=blocked_reasons,
            generated_at=generated_at,
        )

    daily_candidate = _build_daily_price_candidate_batch(
        normalized=normalized,
        output_keys=output_keys,
        batch_id=batch_id,
        suspend_d_full_event_coverage_proven=suspend_d_full_event_coverage_proven,
        generated_at=generated_at,
    )
    suspension_candidate = _build_suspension_status_candidate_batch(
        daily_candidate=daily_candidate,
        normalized=normalized,
        output_keys=output_keys,
        batch_id=batch_id,
        suspend_d_full_event_coverage_proven=suspend_d_full_event_coverage_proven,
        generated_at=generated_at,
    )
    coverage_report["candidate_row_counts"] = {
        "daily_price_candidate_batch": int(len(daily_candidate)),
        "suspension_status_candidate_batch": int(len(suspension_candidate)),
    }
    coverage_report["pause_status_counts"] = _pause_status_counts(daily_candidate)
    dq3_audit = _build_dq3_audit(
        batch_id=batch_id,
        start_date=start_date,
        end_date=end_date,
        codes=codes,
        trade_dates=trade_dates,
        expected_keys=expected_keys,
        coverage_report=coverage_report,
        daily_candidate=daily_candidate,
        suspension_candidate=suspension_candidate,
        suspend_d_full_event_coverage_proven=suspend_d_full_event_coverage_proven,
        generated_at=generated_at,
    )
    manifest = _build_manifest(
        batch_id=batch_id,
        start_date=start_date,
        end_date=end_date,
        codes=codes,
        trade_dates=trade_dates,
        output_keys=output_keys,
        staging_row_counts=staging_row_counts,
        daily_candidate=daily_candidate,
        suspension_candidate=suspension_candidate,
        coverage_report=coverage_report,
        dq3_audit=dq3_audit,
        generated_at=generated_at,
        cli_command=cli_command,
    )

    _write_success_artifacts(
        output_keys=output_keys,
        normalized=normalized,
        daily_candidate=daily_candidate,
        suspension_candidate=suspension_candidate,
        coverage_report=coverage_report,
        dq3_audit=dq3_audit,
        manifest=manifest,
        write_parquet_fn=write_parquet_fn,
        write_json_fn=write_json_fn,
    )

    return _success_result(
        batch_id=batch_id,
        start_date=start_date,
        end_date=end_date,
        codes=codes,
        trade_dates=trade_dates,
        output_keys=output_keys,
        staging_row_counts=staging_row_counts,
        coverage_report=coverage_report,
        dq3_audit=dq3_audit,
        manifest=manifest,
        daily_candidate=daily_candidate,
        suspension_candidate=suspension_candidate,
        generated_at=generated_at,
    )


def _fetch_provider_frames(*, provider, start_date: str, end_date: str, codes: list[str], sleep_seconds: float, sleeper: SleepFn) -> dict[str, Any]:
    frame_parts: dict[str, list[pd.DataFrame]] = {interface: [] for interface in TUSHARE_CANDIDATE_BATCH_REQUIRED_INTERFACES}
    provider_errors = []
    call_specs = [("trade_cal", _provider_kwargs("trade_cal", start_date, end_date))]
    for interface in PER_CODE_FETCH_INTERFACES:
        call_specs.extend((interface, _provider_kwargs(interface, start_date, end_date, ts_code=code)) for code in codes)
    call_specs.extend((interface, _provider_kwargs(interface, start_date, end_date)) for interface in RANGE_FETCH_INTERFACES)

    for index, (interface, kwargs) in enumerate(call_specs):
        if index > 0 and sleep_seconds:
            sleeper(sleep_seconds)
        try:
            frame_parts[interface].append(provider.fetch_raw_endpoint_allow_empty(interface, **kwargs))
        except Exception as exc:
            provider_errors.append(
                {
                    "interface": interface,
                    "error_class": exc.__class__.__name__,
                    "error": str(exc),
                    "kwargs": _safe_kwargs(kwargs),
                }
            )
    frames = {interface: _concat_frames(parts) for interface, parts in frame_parts.items()}
    for interface, frame in frames.items():
        if interface != "suspend_d" and frame.empty:
            provider_errors.append(
                {
                    "interface": interface,
                    "error_class": "ProviderEmptyResult",
                    "error": "provider returned empty result",
                    "kwargs": {},
                }
            )
    return {"frames": frames, "provider_errors": provider_errors}


def _provider_kwargs(interface: str, start_date: str, end_date: str, ts_code: str | None = None) -> dict[str, str]:
    compact_start = start_date.replace("-", "")
    compact_end = end_date.replace("-", "")
    if interface == "trade_cal":
        return {"exchange": "", "start_date": compact_start, "end_date": compact_end, "fields": INTERFACE_FIELDS[interface]}
    kwargs = {"start_date": compact_start, "end_date": compact_end, "fields": INTERFACE_FIELDS[interface]}
    if ts_code:
        kwargs["ts_code"] = ts_code
    return kwargs


def _normalize_all(raw_frames: dict[str, pd.DataFrame], codes: list[str], trade_dates: list[str]) -> dict[str, pd.DataFrame]:
    normalized = {
        "daily": _normalize_dataset(raw_frames.get("daily", pd.DataFrame()), "daily"),
        "stk_limit": _normalize_dataset(raw_frames.get("stk_limit", pd.DataFrame()), "stk_limit"),
        "adj_factor": _normalize_dataset(raw_frames.get("adj_factor", pd.DataFrame()), "adj_factor"),
        "daily_basic": _normalize_dataset(raw_frames.get("daily_basic", pd.DataFrame()), "daily_basic"),
        "trade_cal": _normalize_dataset(raw_frames.get("trade_cal", pd.DataFrame()), "trade_cal"),
        "suspend_d": _normalize_dataset(raw_frames.get("suspend_d", pd.DataFrame()), "suspend_d"),
    }
    date_set = set(trade_dates)
    code_set = set(codes)
    for dataset in ("daily", "stk_limit", "adj_factor", "daily_basic"):
        frame = normalized[dataset]
        normalized[dataset] = frame[frame["trade_date"].isin(date_set) & frame["ts_code"].isin(code_set)].copy()
    normalized["suspend_d"] = normalized["suspend_d"][normalized["suspend_d"]["trade_date"].isin(date_set)].copy()
    return normalized


def _normalize_trade_cal(frame: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    normalized = _normalize_dataset(frame, "trade_cal")
    return normalized[(normalized["trade_date"] >= start_date) & (normalized["trade_date"] <= end_date)].copy()


def _normalize_dataset(frame: pd.DataFrame, dataset: str) -> pd.DataFrame:
    data = pd.DataFrame()
    aliases = FIELD_ALIASES[dataset]
    for field, options in aliases.items():
        column = _pick_column(frame, options)
        data[field] = frame[column] if column is not None else pd.Series([pd.NA] * len(frame))
    for field, options in OPTIONAL_ALIASES.get(dataset, {}).items():
        column = _pick_column(frame, options)
        if column is not None:
            data[field] = frame[column]
    if "trade_date" in data.columns:
        data["trade_date"] = data["trade_date"].map(_normalize_tushare_date)
    if "pretrade_date" in data.columns:
        data["pretrade_date"] = data["pretrade_date"].map(_normalize_tushare_date)
    if "resume_date" in data.columns:
        data["resume_date"] = data["resume_date"].map(_normalize_tushare_date)
    return data


def _open_trade_dates(trade_cal: pd.DataFrame) -> list[str]:
    if trade_cal.empty:
        return []
    open_rows = trade_cal[pd.to_numeric(trade_cal["is_open"], errors="coerce") == 1]
    return sorted(open_rows["trade_date"].dropna().astype(str).unique().tolist())


def _build_daily_price_candidate_batch(
    *,
    normalized: dict[str, pd.DataFrame],
    output_keys: dict[str, Any],
    batch_id: str,
    suspend_d_full_event_coverage_proven: bool,
    generated_at: str,
) -> pd.DataFrame:
    daily = _drop_duplicate_keys(normalized["daily"])
    limits = _drop_duplicate_keys(normalized["stk_limit"])[JOIN_KEYS + ["limit_up", "limit_down"]]
    adj_factor = _drop_duplicate_keys(normalized["adj_factor"])[JOIN_KEYS + ["adj_factor"]]
    daily_basic_columns = [
        "pe_ttm",
        "pb",
        "ps_ttm",
        "total_mv",
        "circ_mv",
        "turnover_rate",
    ]
    daily_basic = _drop_duplicate_keys(normalized["daily_basic"])[JOIN_KEYS + daily_basic_columns]
    suspend_columns = [column for column in JOIN_KEYS + ["suspend_type", "suspend_timing"] if column in normalized["suspend_d"].columns]
    suspend_d = _drop_duplicate_keys(normalized["suspend_d"])[suspend_columns].copy()
    suspend_d["_event_match"] = True

    candidate = daily.merge(limits, on=JOIN_KEYS, how="left")
    candidate = candidate.merge(adj_factor, on=JOIN_KEYS, how="left")
    candidate = candidate.merge(daily_basic, on=JOIN_KEYS, how="left")
    candidate = candidate.merge(suspend_d, on=JOIN_KEYS, how="left")

    event_matches = [bool(value) if pd.notna(value) else False for value in candidate["_event_match"]]
    candidate["provider"] = "tushare"
    candidate["batch_id"] = batch_id
    candidate["trading_day_confirmed"] = True
    statuses = []
    paused_values = []
    evidence_values = []
    for event_match in event_matches:
        if event_match:
            statuses.append(PauseStatus.TRUE_CANDIDATE.value)
            paused_values.append(True)
            evidence_values.append(PauseEvidence.SUSPEND_D_MATCH.value)
        elif suspend_d_full_event_coverage_proven:
            statuses.append(PauseStatus.FALSE_CANDIDATE.value)
            paused_values.append(False)
            evidence_values.append(PauseEvidence.FULL_EVENT_COVERAGE_NO_MATCH.value)
        else:
            statuses.append(PauseStatus.UNKNOWN.value)
            paused_values.append(None)
            evidence_values.append(PauseEvidence.UNRESOLVED_NO_EVENT_MATCH.value)
    candidate["pause_status"] = statuses
    candidate["is_paused_candidate"] = paused_values
    candidate["pause_evidence"] = evidence_values
    candidate["daily_source_object_key"] = _source_key_for_date(output_keys["daily_staging"], candidate["trade_date"])
    candidate["limit_source_object_key"] = _source_key_for_date(output_keys["stk_limit_staging"], candidate["trade_date"])
    candidate["adj_factor_source_object_key"] = _source_key_for_date(output_keys["adj_factor_staging"], candidate["trade_date"])
    candidate["daily_basic_source_object_key"] = _source_key_for_date(output_keys["daily_basic_staging"], candidate["trade_date"])
    candidate["event_source_object_key"] = output_keys["suspend_d_staging"]
    candidate["calendar_source_object_key"] = output_keys["trade_cal_staging"]
    candidate["dq_level"] = DataQualityLevel.DQ1.value
    candidate["is_standard"] = False
    candidate["is_promotable"] = False
    candidate["generated_at"] = generated_at
    return candidate.drop(columns=["_event_match"])


def _build_suspension_status_candidate_batch(
    *,
    daily_candidate: pd.DataFrame,
    normalized: dict[str, pd.DataFrame],
    output_keys: dict[str, Any],
    batch_id: str,
    suspend_d_full_event_coverage_proven: bool,
    generated_at: str,
) -> pd.DataFrame:
    _ = normalized
    coverage_status = "FULL_EVENT_COVERAGE" if suspend_d_full_event_coverage_proven else "COVERAGE_UNKNOWN"
    coverage_block_reason = "" if suspend_d_full_event_coverage_proven else "suspend_d event source full coverage is not proven"
    return pd.DataFrame(
        {
            "ts_code": daily_candidate["ts_code"],
            "trade_date": daily_candidate["trade_date"],
            "provider": "tushare",
            "batch_id": batch_id,
            "pause_status": daily_candidate["pause_status"],
            "is_paused_candidate": daily_candidate["is_paused_candidate"],
            "pause_evidence": daily_candidate["pause_evidence"],
            "event_match": daily_candidate["pause_status"] == PauseStatus.TRUE_CANDIDATE.value,
            "event_source_object_key": output_keys["suspend_d_staging"],
            "calendar_source_object_key": output_keys["trade_cal_staging"],
            "coverage_status": coverage_status,
            "coverage_block_reason": coverage_block_reason,
            "dq_level": DataQualityLevel.DQ1.value,
            "is_standard": False,
            "is_promotable": False,
            "generated_at": generated_at,
        }
    )


def _build_coverage_report(
    *,
    batch_id: str,
    start_date: str,
    end_date: str,
    codes: list[str],
    trade_dates: list[str],
    expected_keys: set[tuple[str, str]],
    normalized: dict[str, pd.DataFrame],
    schema_checks: dict[str, dict[str, Any]],
    duplicate_checks: dict[str, dict[str, Any]],
    provider_errors: list[dict[str, Any]],
    suspend_d_full_event_coverage_proven: bool,
    generated_at: str,
) -> dict[str, Any]:
    expected_count = len(expected_keys)
    interfaces = {}
    for dataset in ("daily", "stk_limit", "adj_factor", "daily_basic"):
        coverage = _interface_coverage(normalized[dataset], expected_keys)
        interfaces[dataset] = {
            "row_count": int(len(normalized[dataset])),
            "coverage": coverage,
            "columns": list(normalized[dataset].columns),
        }

    suspend_keys = set(map(tuple, normalized["suspend_d"][JOIN_KEYS].drop_duplicates().itertuples(index=False, name=None)))
    matched_suspend = suspend_keys & expected_keys
    suspend_d_coverage_status = "FULL_EVENT_COVERAGE" if suspend_d_full_event_coverage_proven else "COVERAGE_UNKNOWN"
    suspend_d_coverage_block_reason = "" if suspend_d_full_event_coverage_proven else "suspend_d event source full coverage is not proven"
    interfaces["suspend_d"] = {
        "row_count": int(len(normalized["suspend_d"])),
        "event_count": len(suspend_keys),
        "matched_candidate_events": len(matched_suspend),
        "events_not_in_requested_universe": len(suspend_keys - expected_keys),
        "coverage_status": suspend_d_coverage_status,
        "coverage_block_reason": suspend_d_coverage_block_reason,
        "columns": list(normalized["suspend_d"].columns),
    }
    interfaces["trade_cal"] = {
        "row_count": int(len(normalized["trade_cal"])),
        "trade_day_count": len(trade_dates),
        "confirmed_open_trading_days": len(trade_dates),
        "pause_source": False,
        "columns": list(normalized["trade_cal"].columns),
    }
    coverage_aliases = {dataset: _coverage_alias(interfaces[dataset]["coverage"]) for dataset in ("daily", "stk_limit", "adj_factor", "daily_basic")}
    return {
        "provider": "tushare",
        "goal": "13",
        "batch_id": batch_id,
        "generated_at": generated_at,
        "start_date": start_date,
        "end_date": end_date,
        "codes": codes,
        "code_count_requested": len(codes),
        "trade_dates": trade_dates,
        "trade_day_count": len(trade_dates),
        "expected_code_date_count": expected_count,
        "expected_code_trade_date_count": expected_count,
        "daily_coverage": coverage_aliases["daily"],
        "stk_limit_coverage": coverage_aliases["stk_limit"],
        "adj_factor_coverage": coverage_aliases["adj_factor"],
        "daily_basic_coverage": coverage_aliases["daily_basic"],
        "trade_cal_coverage": {
            "row_count": interfaces["trade_cal"]["row_count"],
            "trade_day_count": interfaces["trade_cal"]["trade_day_count"],
            "confirmed_open_trading_days": interfaces["trade_cal"]["confirmed_open_trading_days"],
            "pause_source": False,
        },
        "suspend_d_event_coverage": interfaces["suspend_d"],
        "duplicate_key_counts": {dataset: check["duplicate_rows"] for dataset, check in duplicate_checks.items()},
        "missing_key_counts": {dataset: interfaces[dataset]["coverage"]["missing_rows"] for dataset in ("daily", "stk_limit", "adj_factor", "daily_basic")},
        "schema_check": schema_checks,
        "date_range_check": {
            "start_date": start_date,
            "end_date": end_date,
            "trade_dates_min": min(trade_dates) if trade_dates else None,
            "trade_dates_max": max(trade_dates) if trade_dates else None,
            "valid": True,
        },
        "rate_limit_or_blocked_errors": provider_errors,
        "interfaces": interfaces,
        "duplicate_key_checks": duplicate_checks,
        "schema_checks": schema_checks,
        "provider_errors": provider_errors,
        "sample_truncated": False,
        "field_sources": FIELD_SOURCES,
        "safety": SAFETY_FLAGS,
        "inference_guards": INFERENCE_GUARDS,
    }


def _build_dq3_audit(
    *,
    batch_id: str,
    start_date: str,
    end_date: str,
    codes: list[str],
    trade_dates: list[str],
    expected_keys: set[tuple[str, str]],
    coverage_report: dict[str, Any],
    daily_candidate: pd.DataFrame,
    suspension_candidate: pd.DataFrame,
    suspend_d_full_event_coverage_proven: bool,
    generated_at: str,
) -> dict[str, Any]:
    expected_count = len(expected_keys)
    field_completeness_ok = all(
        coverage_report["interfaces"][dataset]["coverage"]["matched_rows"] == expected_count
        for dataset in ("daily", "stk_limit", "adj_factor", "daily_basic")
    )
    duplicate_check_ok = all(check["duplicate_rows"] == 0 for check in coverage_report["duplicate_key_checks"].values())
    schema_check_ok = all(not check["missing_required_fields"] for check in coverage_report["schema_checks"].values())
    pause_statuses = list(daily_candidate["pause_status"]) if "pause_status" in daily_candidate.columns else []
    readiness = can_mark_tushare_candidate_batch_ready_for_promotion_validator(
        field_completeness_ok=field_completeness_ok,
        coverage_complete=suspend_d_full_event_coverage_proven,
        pause_statuses=pause_statuses,
        duplicate_check_ok=duplicate_check_ok,
        schema_check_ok=schema_check_ok,
        validator_precheck_passed=False,
        dq_level=DataQualityLevel.DQ1,
    )
    pause_status_counts = _pause_status_counts(daily_candidate)
    blocked_reasons = _readiness_blocked_reason_codes(coverage_report, pause_status_counts) + list(readiness.reasons)
    if not suspend_d_full_event_coverage_proven and "suspend_d event source full coverage is not proven" not in blocked_reasons:
        blocked_reasons.insert(0, "suspend_d event source full coverage is not proven")
    return {
        "provider": "tushare",
        "goal": "13",
        "batch_id": batch_id,
        "generated_at": generated_at,
        "start_date": start_date,
        "end_date": end_date,
        "codes": codes,
        "trade_dates": trade_dates,
        "ready_for_promotion_validator": readiness.ready_for_promotion_validator,
        "ready_for_dq3_promotion": False,
        "status": readiness.status.value,
        "blocked_reasons": list(dict.fromkeys(blocked_reasons)),
        "field_completeness": {
            "daily": coverage_report["interfaces"]["daily"]["coverage"],
            "stk_limit": coverage_report["interfaces"]["stk_limit"]["coverage"],
            "adj_factor": coverage_report["interfaces"]["adj_factor"]["coverage"],
            "daily_basic": coverage_report["interfaces"]["daily_basic"]["coverage"],
            "ok": field_completeness_ok,
        },
        "coverage_summary": {
            "expected_code_date_count": expected_count,
            "expected_code_trade_date_count": expected_count,
            "suspend_d": coverage_report["interfaces"]["suspend_d"],
            "trade_cal": coverage_report["interfaces"]["trade_cal"],
        },
        "pause_status_summary": pause_status_counts,
        "duplicate_check_ok": duplicate_check_ok,
        "duplicate_check": {"ok": duplicate_check_ok, "checks": coverage_report["duplicate_key_checks"]},
        "schema_check_ok": schema_check_ok,
        "schema_check": {"ok": schema_check_ok, "checks": coverage_report["schema_checks"]},
        "validator_precheck": {"passed": False, "reason": "standard daily_price validator is not run for Goal 13 candidate-only batch"},
        "candidate_row_counts": {
            "daily_price_candidate_batch": int(len(daily_candidate)),
            "suspension_status_candidate_batch": int(len(suspension_candidate)),
        },
        "safety": SAFETY_FLAGS,
        "inference_guards": INFERENCE_GUARDS,
        "next_required_actions": list(readiness.required_future_gates),
    }


def _readiness_blocked_reason_codes(coverage_report: dict[str, Any], pause_status_counts: dict[str, int]) -> list[str]:
    reasons = []
    if coverage_report["interfaces"]["stk_limit"]["coverage"]["coverage_rate"] < 1.0:
        reasons.append("INCOMPLETE_LIMIT_PRICE_COVERAGE")
    if coverage_report["interfaces"]["adj_factor"]["coverage"]["coverage_rate"] < 1.0:
        reasons.append("INCOMPLETE_ADJ_FACTOR_COVERAGE")
    if pause_status_counts.get(PauseStatus.UNKNOWN.value, 0) > 0:
        reasons.append("UNRESOLVED_IS_PAUSED")
    if coverage_report["interfaces"]["suspend_d"]["coverage_status"] != "FULL_EVENT_COVERAGE":
        reasons.append("INCOMPLETE_OR_UNKNOWN_SUSPEND_D_COVERAGE")
    return reasons


def _build_manifest(
    *,
    batch_id: str,
    start_date: str,
    end_date: str,
    codes: list[str],
    trade_dates: list[str],
    output_keys: dict[str, Any],
    staging_row_counts: dict[str, int],
    daily_candidate: pd.DataFrame,
    suspension_candidate: pd.DataFrame,
    coverage_report: dict[str, Any],
    dq3_audit: dict[str, Any],
    generated_at: str,
    cli_command: str | None,
) -> dict[str, Any]:
    contract = classify_tushare_candidate_batch(source_layer="candidate")
    return {
        "provider": "tushare",
        "goal": "13",
        "batch_id": batch_id,
        "generated_at": generated_at,
        "start_date": start_date,
        "end_date": end_date,
        "codes": codes,
        "trade_dates": trade_dates,
        "cli_command": cli_command,
        "interfaces_requested": list(TUSHARE_CANDIDATE_BATCH_REQUIRED_INTERFACES),
        "interfaces_succeeded": list(TUSHARE_CANDIDATE_BATCH_REQUIRED_INTERFACES),
        "interfaces_failed": [],
        "object_keys": output_keys,
        "row_counts": {
            **staging_row_counts,
            "daily_price_candidate_batch": int(len(daily_candidate)),
            "suspension_status_candidate_batch": int(len(suspension_candidate)),
        },
        "schema_versions": {
            "daily_staging": "goal13.v1",
            "stk_limit_staging": "goal13.v1",
            "adj_factor_staging": "goal13.v1",
            "daily_basic_staging": "goal13.v1",
            "trade_cal_staging": "goal13.v1",
            "suspend_d_staging": "goal13.v1",
            "daily_price_candidate_batch": "goal13.v1",
            "suspension_status_candidate_batch": "goal13.v1",
            "provider_coverage_report": "goal13.v1",
            "dq3_readiness_audit": "goal13.v1",
        },
        "dq_level": DataQualityLevel.DQ1.value,
        "is_standard": False,
        "is_promotable": False,
        "ready_for_promotion_validator": dq3_audit["ready_for_promotion_validator"],
        "ready_for_dq3_promotion": dq3_audit["ready_for_dq3_promotion"],
        "coverage_status": dq3_audit["status"],
        "blocked_reasons": dq3_audit["blocked_reasons"],
        "contract": _contract_to_report(contract),
        "safety": SAFETY_FLAGS,
        "inference_guards": INFERENCE_GUARDS,
        "coverage_summary": {
            "expected_code_date_count": coverage_report["expected_code_date_count"],
            "pause_status_counts": dq3_audit["pause_status_summary"],
        },
    }


def _write_success_artifacts(
    *,
    output_keys: dict[str, Any],
    normalized: dict[str, pd.DataFrame],
    daily_candidate: pd.DataFrame,
    suspension_candidate: pd.DataFrame,
    coverage_report: dict[str, Any],
    dq3_audit: dict[str, Any],
    manifest: dict[str, Any],
    write_parquet_fn: WriteParquetFn,
    write_json_fn: WriteJsonFn,
) -> None:
    for dataset in ("daily", "stk_limit", "adj_factor", "daily_basic"):
        date_keys = output_keys[f"{dataset}_staging"]
        for trade_date, object_key in date_keys.items():
            frame = normalized[dataset][normalized[dataset]["trade_date"] == trade_date].copy()
            write_parquet_fn(object_key, frame)
    write_parquet_fn(output_keys["trade_cal_staging"], normalized["trade_cal"])
    write_parquet_fn(output_keys["suspend_d_staging"], normalized["suspend_d"])
    write_parquet_fn(output_keys["daily_price_candidate_batch"], daily_candidate)
    write_parquet_fn(output_keys["suspension_status_candidate_batch"], suspension_candidate)
    write_json_fn(output_keys["provider_coverage_report"], coverage_report)
    write_json_fn(output_keys["dq3_readiness_audit"], dq3_audit)
    write_json_fn(output_keys["manifest"], manifest)


def _success_result(
    *,
    batch_id: str,
    start_date: str,
    end_date: str,
    codes: list[str],
    trade_dates: list[str],
    output_keys: dict[str, Any],
    staging_row_counts: dict[str, int],
    coverage_report: dict[str, Any],
    dq3_audit: dict[str, Any],
    manifest: dict[str, Any],
    daily_candidate: pd.DataFrame,
    suspension_candidate: pd.DataFrame,
    generated_at: str,
) -> dict[str, Any]:
    return {
        "status": "CANDIDATE_BATCH_COMPLETED_NOT_PROMOTABLE",
        "provider": "tushare",
        "goal": "13",
        "batch_id": batch_id,
        "generated_at": generated_at,
        "start_date": start_date,
        "end_date": end_date,
        "codes": codes,
        "trade_dates": trade_dates,
        "output_object_keys": output_keys,
        "manifest": manifest,
        "provider_coverage_report": coverage_report,
        "dq3_readiness_audit": dq3_audit,
        "interfaces_requested": list(TUSHARE_CANDIDATE_BATCH_REQUIRED_INTERFACES),
        "interfaces_succeeded": list(TUSHARE_CANDIDATE_BATCH_REQUIRED_INTERFACES),
        "interfaces_failed": [],
        "staging_row_counts": staging_row_counts,
        "daily_price_candidate_row_count": int(len(daily_candidate)),
        "suspension_status_candidate_row_count": int(len(suspension_candidate)),
        "coverage_summary": _summary_coverage(coverage_report),
        "pause_status_counts": _pause_status_counts(daily_candidate),
        "ready_for_promotion_validator": dq3_audit["ready_for_promotion_validator"],
        "ready_for_dq3_promotion": dq3_audit["ready_for_dq3_promotion"],
        "blocked_reasons": dq3_audit["blocked_reasons"],
        "safety": SAFETY_FLAGS,
        "inference_guards": INFERENCE_GUARDS,
    }


def _blocked_provider_error_report(
    *,
    batch_id: str,
    start_date: str,
    end_date: str,
    codes: list[str],
    trade_dates: list[str],
    output_keys: dict[str, Any],
    staging_row_counts: dict[str, int],
    coverage_report: dict[str, Any],
    provider_errors: list[dict[str, Any]],
    generated_at: str,
) -> dict[str, Any]:
    failed = [error["interface"] for error in provider_errors]
    succeeded = [interface for interface in TUSHARE_CANDIDATE_BATCH_REQUIRED_INTERFACES if interface not in set(failed)]
    status = "BLOCKED_BY_PROVIDER_EMPTY_RESULT" if all(error.get("error_class") == "ProviderEmptyResult" for error in provider_errors) else "BLOCKED_BY_PROVIDER_ERROR"
    return {
        "status": status,
        "provider": "tushare",
        "goal": "13",
        "batch_id": batch_id,
        "generated_at": generated_at,
        "start_date": start_date,
        "end_date": end_date,
        "codes": codes,
        "trade_dates": trade_dates,
        "output_object_keys": {},
        "interfaces_requested": list(TUSHARE_CANDIDATE_BATCH_REQUIRED_INTERFACES),
        "interfaces_succeeded": succeeded,
        "interfaces_failed": failed,
        "staging_row_counts": staging_row_counts,
        "daily_price_candidate_row_count": 0,
        "suspension_status_candidate_row_count": 0,
        "coverage_summary": _summary_coverage(coverage_report),
        "provider_errors": provider_errors,
        "blocked_reasons": [_provider_blocked_reason(error) for error in provider_errors],
        "ready_for_promotion_validator": False,
        "ready_for_dq3_promotion": False,
        "safety": SAFETY_FLAGS,
        "inference_guards": INFERENCE_GUARDS,
    }


def _provider_blocked_reason(error: dict[str, Any]) -> str:
    if error.get("error_class") == "ProviderEmptyResult":
        return f"{error['interface']} provider returned empty result"
    return f"{error['interface']} provider error: {error['error']}"


def _blocked_schema_report(
    *,
    batch_id: str,
    start_date: str,
    end_date: str,
    codes: list[str],
    trade_dates: list[str],
    output_keys: dict[str, Any],
    staging_row_counts: dict[str, int],
    coverage_report: dict[str, Any],
    blocked_reasons: list[str],
    generated_at: str,
) -> dict[str, Any]:
    return {
        "status": "BLOCKED_BY_SCHEMA_MISMATCH",
        "provider": "tushare",
        "goal": "13",
        "batch_id": batch_id,
        "generated_at": generated_at,
        "start_date": start_date,
        "end_date": end_date,
        "codes": codes,
        "trade_dates": trade_dates,
        "output_object_keys": {},
        "interfaces_requested": list(TUSHARE_CANDIDATE_BATCH_REQUIRED_INTERFACES),
        "interfaces_succeeded": [],
        "interfaces_failed": list(TUSHARE_CANDIDATE_BATCH_REQUIRED_INTERFACES),
        "staging_row_counts": staging_row_counts,
        "daily_price_candidate_row_count": 0,
        "suspension_status_candidate_row_count": 0,
        "coverage_summary": _summary_coverage(coverage_report),
        "blocked_reasons": blocked_reasons,
        "ready_for_promotion_validator": False,
        "ready_for_dq3_promotion": False,
        "safety": SAFETY_FLAGS,
        "inference_guards": INFERENCE_GUARDS,
    }


def _schema_checks(raw_frames: dict[str, pd.DataFrame]) -> dict[str, dict[str, Any]]:
    return {dataset: _schema_check(raw_frames.get(dataset, pd.DataFrame()), dataset) for dataset in TUSHARE_CANDIDATE_BATCH_REQUIRED_INTERFACES}


def _concat_frames(parts: list[pd.DataFrame]) -> pd.DataFrame:
    if not parts:
        return pd.DataFrame()
    non_null_parts = [part.copy() for part in parts if part is not None]
    if not non_null_parts:
        return pd.DataFrame()
    return pd.concat(non_null_parts, ignore_index=True)


def _schema_check(frame: pd.DataFrame, dataset: str) -> dict[str, Any]:
    if dataset == "suspend_d" and frame.empty:
        return {
            "missing_required_fields": [],
            "selected_columns": {"ts_code": None, "trade_date": None},
            "valid": True,
            "columns": list(frame.columns),
            "empty_response_accepted": True,
        }
    missing = []
    selected = {}
    for field, aliases in FIELD_ALIASES[dataset].items():
        column = _pick_column(frame, aliases)
        selected[field] = column
        if column is None:
            missing.append(field)
    return {
        "missing_required_fields": missing,
        "selected_columns": selected,
        "valid": not missing,
        "columns": list(frame.columns),
    }


def _schema_blocked(schema_checks: dict[str, dict[str, Any]]) -> bool:
    return any(check["missing_required_fields"] for check in schema_checks.values())


def _duplicate_key_checks(normalized: dict[str, pd.DataFrame]) -> dict[str, dict[str, Any]]:
    checks = {}
    for dataset in ("daily", "stk_limit", "adj_factor", "daily_basic", "suspend_d"):
        frame = normalized[dataset]
        duplicate_rows = int(frame.duplicated(JOIN_KEYS, keep=False).sum()) if set(JOIN_KEYS).issubset(frame.columns) else 0
        checks[dataset] = {"key": JOIN_KEYS, "duplicate_rows": duplicate_rows}
    trade_cal = normalized["trade_cal"]
    checks["trade_cal"] = {
        "key": ["trade_date", "exchange"],
        "duplicate_rows": int(trade_cal.duplicated(["trade_date", "exchange"], keep=False).sum())
        if {"trade_date", "exchange"}.issubset(trade_cal.columns)
        else 0,
    }
    return checks


def _interface_coverage(frame: pd.DataFrame, expected_keys: set[tuple[str, str]]) -> dict[str, Any]:
    keys = set(map(tuple, frame[JOIN_KEYS].drop_duplicates().itertuples(index=False, name=None))) if set(JOIN_KEYS).issubset(frame.columns) else set()
    matched = len(keys & expected_keys)
    denominator = len(expected_keys)
    return {
        "matched_rows": matched,
        "denominator": denominator,
        "missing_rows": denominator - matched,
        "coverage_rate": float(matched / denominator) if denominator else 0.0,
    }


def _coverage_alias(coverage: dict[str, Any]) -> dict[str, Any]:
    return {
        "numerator": coverage["matched_rows"],
        "denominator": coverage["denominator"],
        "matched_rows": coverage["matched_rows"],
        "missing_rows": coverage["missing_rows"],
        "coverage_rate": coverage["coverage_rate"],
    }


def _summary_coverage(coverage_report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    summary = {}
    for dataset in ("daily", "stk_limit", "adj_factor", "daily_basic"):
        summary[dataset] = coverage_report["interfaces"][dataset]["coverage"]
    summary["suspend_d"] = coverage_report["interfaces"]["suspend_d"]
    summary["trade_cal"] = coverage_report["interfaces"]["trade_cal"]
    return summary


def _pause_status_counts(candidate: pd.DataFrame) -> dict[str, int]:
    counts = candidate["pause_status"].value_counts(dropna=False).to_dict() if "pause_status" in candidate.columns else {}
    return {
        "true_candidate": int(counts.get(PauseStatus.TRUE_CANDIDATE.value, 0)),
        "false_candidate": int(counts.get(PauseStatus.FALSE_CANDIDATE.value, 0)),
        "unknown": int(counts.get(PauseStatus.UNKNOWN.value, 0)),
    }


def _expected_keys(codes: list[str], trade_dates: list[str]) -> set[tuple[str, str]]:
    return {(code, trade_date) for trade_date in trade_dates for code in codes}


def _drop_duplicate_keys(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.drop_duplicates(JOIN_KEYS, keep="first").copy()


def _source_key_for_date(date_key_map: dict[str, str], trade_dates: pd.Series) -> list[str | None]:
    return [date_key_map.get(str(trade_date)) for trade_date in trade_dates]


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
    return text


def _normalize_codes(codes: list[str]) -> list[str]:
    normalized = []
    for code in codes:
        for item in str(code).split(","):
            value = item.strip().upper()
            if value:
                normalized.append(value)
    if not normalized:
        raise ValueError("codes must not be empty")
    return list(dict.fromkeys(normalized))


def _validate_batch_id(batch_id: str) -> str:
    value = str(batch_id).strip()
    if not value:
        raise ValueError("batch_id must not be empty")
    if any(char in value for char in "\\/:*?\"<>|"):
        raise ValueError("batch_id contains unsupported path characters")
    return value


def _default_batch_id(start_date: str, end_date: str, generated_at: str) -> str:
    stamp = "".join(ch for ch in generated_at if ch.isdigit())[:14]
    return f"goal13-{start_date}-{end_date}-{stamp}"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in kwargs.items() if "token" not in key.lower()}


def _zero_row_counts() -> dict[str, int]:
    return {
        "daily": 0,
        "stk_limit": 0,
        "adj_factor": 0,
        "daily_basic": 0,
        "trade_cal": 0,
        "suspend_d": 0,
    }


def _empty_coverage_summary(denominator: int) -> dict[str, dict[str, Any]]:
    item = {"matched_rows": 0, "denominator": denominator, "missing_rows": denominator, "coverage_rate": 0.0}
    return {
        "daily": dict(item),
        "stk_limit": dict(item),
        "adj_factor": dict(item),
        "daily_basic": dict(item),
        "suspend_d": {"matched_candidate_events": 0, "events_not_in_requested_universe": 0, "coverage_status": "COVERAGE_UNKNOWN"},
        "trade_cal": {"trade_day_count": 0, "pause_source": False},
    }


def _contract_to_report(contract) -> dict[str, Any]:
    data = asdict(contract)
    if "provider_name" in data:
        data["provider"] = data.pop("provider_name")
    data["dq_level"] = data["dq_level"].value
    return data

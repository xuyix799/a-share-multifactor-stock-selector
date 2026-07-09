from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from stock_selector.data.data_validator import validate_stock_code
from stock_selector.data.tushare_candidate_staging_batch import build_tushare_candidate_staging_batch
from stock_selector.data.tushare_daily_price_promotion_validator import (
    DEFAULT_MAX_CODES,
    DEFAULT_MAX_ROWS,
    DEFAULT_MAX_TRADE_DAYS,
    build_tushare_daily_price_promotion_validator,
)
from stock_selector.utils.date_validator import validate_date_range


WriteParquetFn = Callable[[str, pd.DataFrame], str]
WriteJsonFn = Callable[[str, dict[str, Any]], str]
LoadParquetFn = Callable[[str], pd.DataFrame]
LoadJsonFn = Callable[[str], dict[str, Any]]
StandardDailyPriceReadFn = Callable[[str, str], pd.DataFrame | None]
StandardDailyPriceWriteFn = Callable[[str, str, pd.DataFrame], str]
GeneratedAtFn = Callable[[], str]

SMALL_BATCH_REPORT_SCHEMA = "goal17.tushare_daily_price_small_batch_run_report.v1"

DOWNSTREAM_FIREWALLS = {
    "standard_suspension_status_write_performed": False,
    "clean_daily_snapshot_entered": False,
    "factor_input_table_entered": False,
    "factor_daily_entered": False,
    "selection_result_entered": False,
    "backtest_entered": False,
}


def build_tushare_daily_price_small_batch_output_keys(batch_id: str) -> dict[str, str]:
    batch_id = _validate_batch_id(batch_id)
    return {
        "small_batch_run_report": (
            f"candidate/tushare/daily_price_small_batch_run_report/batch_id={batch_id}/report.json"
        ),
        **_source_artifact_keys(batch_id),
        "daily_price_promotion_validator_report": (
            f"candidate/tushare/daily_price_promotion_validator_report/batch_id={batch_id}/report.json"
        ),
        "standard_daily_price_promotion_dry_run_report": (
            f"candidate/tushare/standard_daily_price_promotion_dry_run_report/batch_id={batch_id}/report.json"
        ),
        "standard_daily_price_promotion_apply_report": (
            f"candidate/tushare/standard_daily_price_promotion_apply_report/batch_id={batch_id}/report.json"
        ),
    }


def build_tushare_daily_price_small_batch_blocked_result(
    *,
    batch_id: str,
    start_date: str,
    end_date: str,
    codes: list[str],
    status: str,
    blocked_reasons: list[str],
    provider_call_enabled: bool,
    reuse_existing_staging: bool = False,
    apply_standard_write: bool = False,
    max_codes: int = DEFAULT_MAX_CODES,
    max_trade_days: int = DEFAULT_MAX_TRADE_DAYS,
    max_rows: int = DEFAULT_MAX_ROWS,
    generated_at_fn: GeneratedAtFn | None = None,
) -> dict[str, Any]:
    generated_at_fn = generated_at_fn or _utc_now_iso
    start_date, end_date = validate_date_range(start_date, end_date)
    codes = _normalize_codes(codes)
    batch_id = _validate_batch_id(batch_id)
    generated_at = generated_at_fn()
    source_keys = _source_artifact_keys(batch_id)
    output_keys = build_tushare_daily_price_small_batch_output_keys(batch_id)
    report = _build_run_report(
        batch_id=batch_id,
        generated_at=generated_at,
        status=status,
        requested_scope=_requested_scope(
            codes=codes,
            start_date=start_date,
            end_date=end_date,
            max_codes=max_codes,
            max_trade_days=max_trade_days,
            max_rows=max_rows,
        ),
        provider=_provider_report(
            provider_call_enabled=provider_call_enabled,
            reuse_existing_staging=reuse_existing_staging,
            provider_status=status,
        ),
        source_artifact_keys=source_keys,
        staging_result=None,
        promotion_preflight_report=None,
        promotion_result=None,
        apply_requested=apply_standard_write,
        blocked_reasons=blocked_reasons,
    )
    return _result_from_report(report=report, output_keys=output_keys, promotion_result=None)


def run_tushare_daily_price_small_batch(
    *,
    batch_id: str,
    start_date: str,
    end_date: str,
    codes: list[str],
    load_parquet_fn: LoadParquetFn,
    load_json_fn: LoadJsonFn,
    write_parquet_fn: WriteParquetFn,
    write_json_fn: WriteJsonFn,
    standard_daily_price_read_fn: StandardDailyPriceReadFn,
    standard_daily_price_write_fn: StandardDailyPriceWriteFn,
    provider: Any = None,
    provider_call_enabled: bool = False,
    reuse_existing_staging: bool = False,
    apply_standard_write: bool = False,
    max_codes: int = DEFAULT_MAX_CODES,
    max_trade_days: int = DEFAULT_MAX_TRADE_DAYS,
    max_rows: int = DEFAULT_MAX_ROWS,
    sleep_seconds: float = 12.0,
    generated_at_fn: GeneratedAtFn | None = None,
    cli_command: str | None = None,
) -> dict[str, Any]:
    generated_at_fn = generated_at_fn or _utc_now_iso
    start_date, end_date = validate_date_range(start_date, end_date)
    batch_id = _validate_batch_id(batch_id)
    codes = _normalize_codes(codes)
    _validate_positive_limits(max_codes=max_codes, max_trade_days=max_trade_days, max_rows=max_rows)
    if sleep_seconds < 0:
        raise ValueError("sleep_seconds must be non-negative")
    if provider_call_enabled and reuse_existing_staging:
        raise ValueError("provider_call_enabled and reuse_existing_staging cannot both be true")
    if provider_call_enabled and provider is None:
        raise ValueError("provider is required when provider_call_enabled is true")
    if len(codes) > max_codes:
        return build_tushare_daily_price_small_batch_blocked_result(
            batch_id=batch_id,
            start_date=start_date,
            end_date=end_date,
            codes=codes,
            status="BLOCKED",
            blocked_reasons=["BATCH_TOO_LARGE_FOR_GOAL17_SMALL_BATCH_WORKFLOW"],
            provider_call_enabled=provider_call_enabled,
            reuse_existing_staging=reuse_existing_staging,
            apply_standard_write=apply_standard_write,
            max_codes=max_codes,
            max_trade_days=max_trade_days,
            max_rows=max_rows,
            generated_at_fn=generated_at_fn,
        )

    generated_at = generated_at_fn()
    source_keys = _source_artifact_keys(batch_id)
    output_keys = build_tushare_daily_price_small_batch_output_keys(batch_id)
    staging_result = None
    blocked_reasons: list[str] = []
    promotion_preflight_report = None
    daily_price_candidate_batch = None
    suspension_status_candidate_batch = None

    if reuse_existing_staging and not provider_call_enabled:
        try:
            (
                promotion_preflight_report,
                daily_price_candidate_batch,
                suspension_status_candidate_batch,
            ) = _load_source_artifacts(
                source_keys=source_keys,
                load_json_fn=load_json_fn,
                load_parquet_fn=load_parquet_fn,
            )
            staging_result = {
                "status": "REUSED_EXISTING_CANDIDATE_ARTIFACTS",
                "blocked_reasons": [],
                "reused_existing_staging": True,
            }
        except FileNotFoundError:
            promotion_preflight_report = None
            daily_price_candidate_batch = None
            suspension_status_candidate_batch = None

    if provider_call_enabled or (reuse_existing_staging and promotion_preflight_report is None):
        staging_result = build_tushare_candidate_staging_batch(
            start_date=start_date,
            end_date=end_date,
            codes=codes,
            provider=provider,
            batch_id=batch_id,
            sleep_seconds=sleep_seconds,
            max_codes=max_codes,
            max_trade_days=max_trade_days,
            no_provider_call=not provider_call_enabled,
            reuse_existing_staging=reuse_existing_staging,
            coverage_expansion=True,
            fetch_semantics_audit=True,
            goal13c_preflight=True,
            load_parquet_fn=load_parquet_fn,
            write_parquet_fn=write_parquet_fn,
            write_json_fn=write_json_fn,
            cli_command=cli_command,
        )
        blocked_reasons.extend(staging_result.get("blocked_reasons", []))
        source_keys = _source_artifact_keys(batch_id)

    if promotion_preflight_report is None:
        try:
            (
                promotion_preflight_report,
                daily_price_candidate_batch,
                suspension_status_candidate_batch,
            ) = _load_source_artifacts(
                source_keys=source_keys,
                load_json_fn=load_json_fn,
                load_parquet_fn=load_parquet_fn,
            )
        except FileNotFoundError as exc:
            blocked_reasons.append("EXISTING_CANDIDATE_ARTIFACTS_MISSING")
            blocked_reasons.append(str(exc))

    promotion_result = None
    if promotion_preflight_report is not None and daily_price_candidate_batch is not None and suspension_status_candidate_batch is not None:
        promotion_result = build_tushare_daily_price_promotion_validator(
            batch_id=batch_id,
            promotion_preflight_report=promotion_preflight_report,
            daily_price_candidate_batch=daily_price_candidate_batch,
            suspension_status_candidate_batch=suspension_status_candidate_batch,
            max_codes=max_codes,
            max_trade_days=max_trade_days,
            max_rows=max_rows,
            request_standard_write=apply_standard_write,
            apply_standard_write=apply_standard_write,
            apply_codes=codes,
            apply_start_date=start_date,
            apply_end_date=end_date,
            standard_daily_price_read_fn=standard_daily_price_read_fn,
            standard_daily_price_write_fn=standard_daily_price_write_fn,
            source_object_keys=source_keys,
            generated_at_fn=lambda: generated_at,
        )
        blocked_reasons.extend(promotion_result.get("blocked_reasons", []))
        _write_promotion_reports(promotion_result=promotion_result, write_json_fn=write_json_fn)

    status = _resolve_status(promotion_result=promotion_result, blocked_reasons=blocked_reasons)
    report = _build_run_report(
        batch_id=batch_id,
        generated_at=generated_at,
        status=status,
        requested_scope=_requested_scope(
            codes=codes,
            start_date=start_date,
            end_date=end_date,
            max_codes=max_codes,
            max_trade_days=max_trade_days,
            max_rows=max_rows,
        ),
        provider=_provider_report(
            provider_call_enabled=provider_call_enabled,
            reuse_existing_staging=reuse_existing_staging,
            provider_status="ENABLED" if provider_call_enabled else "DISABLED",
        ),
        source_artifact_keys=source_keys,
        staging_result=staging_result,
        promotion_preflight_report=promotion_preflight_report,
        promotion_result=promotion_result,
        apply_requested=apply_standard_write,
        blocked_reasons=_dedupe(blocked_reasons),
    )
    write_json_fn(output_keys["small_batch_run_report"], report)
    return _result_from_report(report=report, output_keys=output_keys, promotion_result=promotion_result)


def _write_promotion_reports(*, promotion_result: dict[str, Any], write_json_fn: WriteJsonFn) -> None:
    output_keys = promotion_result["output_object_keys"]
    write_json_fn(
        output_keys["daily_price_promotion_validator_report"],
        promotion_result["daily_price_promotion_validator_report"],
    )
    write_json_fn(
        output_keys["standard_daily_price_promotion_dry_run_report"],
        promotion_result["standard_daily_price_promotion_dry_run_report"],
    )
    if promotion_result.get("standard_daily_price_promotion_apply_report") is not None:
        write_json_fn(
            output_keys["standard_daily_price_promotion_apply_report"],
            promotion_result["standard_daily_price_promotion_apply_report"],
        )


def _load_source_artifacts(
    *,
    source_keys: dict[str, str],
    load_json_fn: LoadJsonFn,
    load_parquet_fn: LoadParquetFn,
) -> tuple[dict[str, Any], pd.DataFrame, pd.DataFrame]:
    return (
        load_json_fn(source_keys["promotion_preflight_report"]),
        load_parquet_fn(source_keys["daily_price_candidate_batch"]),
        load_parquet_fn(source_keys["suspension_status_candidate_batch"]),
    )


def _build_run_report(
    *,
    batch_id: str,
    generated_at: str,
    status: str,
    requested_scope: dict[str, Any],
    provider: dict[str, Any],
    source_artifact_keys: dict[str, str],
    staging_result: dict[str, Any] | None,
    promotion_preflight_report: dict[str, Any] | None,
    promotion_result: dict[str, Any] | None,
    apply_requested: bool,
    blocked_reasons: list[str],
) -> dict[str, Any]:
    apply_report = (promotion_result or {}).get("standard_daily_price_promotion_apply_report") or {}
    read_back_verification = (promotion_result or {}).get("read_back_verification") or apply_report.get("read_back_verification")
    standard_write_performed = bool((promotion_result or {}).get("standard_daily_price_write_performed", False))
    return {
        "schema_version": SMALL_BATCH_REPORT_SCHEMA,
        "goal": "17",
        "provider_name": "tushare",
        "batch_id": batch_id,
        "generated_at": generated_at,
        "status": status,
        "requested_scope": requested_scope,
        "provider": provider,
        "source_artifact_keys": dict(source_artifact_keys),
        "staging_status": (staging_result or {}).get("status"),
        "candidate_preflight_status": (promotion_preflight_report or {}).get("status"),
        "promotion_validator_status": (promotion_result or {}).get("status"),
        "apply": {
            "requested": bool(apply_requested),
            "performed": standard_write_performed,
            "report_key": (promotion_result or {}).get("output_object_keys", {}).get("standard_daily_price_promotion_apply_report"),
        },
        "read_back_verification": read_back_verification,
        "upsert_summary": (promotion_result or {}).get("upsert_summary", {}),
        "blocked_reasons": _dedupe(blocked_reasons),
        "downstream_firewalls": dict(DOWNSTREAM_FIREWALLS),
        "standard_daily_price_write_performed": standard_write_performed,
        "standard_suspension_status_write_performed": False,
        "clean_factor_selection_backtest_entered": False,
        "real_backtest_performed": False,
    }


def _result_from_report(
    *,
    report: dict[str, Any],
    output_keys: dict[str, str],
    promotion_result: dict[str, Any] | None,
) -> dict[str, Any]:
    promotion_keys = (promotion_result or {}).get("output_object_keys", {})
    return {
        "goal": "17",
        "provider": "tushare",
        "status": report["status"],
        "batch_id": report["batch_id"],
        "mode": "APPLY" if report["apply"]["requested"] else "DRY_RUN",
        "output_object_keys": {
            "small_batch_run_report": output_keys["small_batch_run_report"],
            "daily_price_promotion_validator_report": promotion_keys.get("daily_price_promotion_validator_report"),
            "standard_daily_price_promotion_dry_run_report": promotion_keys.get("standard_daily_price_promotion_dry_run_report"),
            "standard_daily_price_promotion_apply_report": promotion_keys.get("standard_daily_price_promotion_apply_report"),
        },
        "small_batch_run_report": report,
        "small_batch_run_report_key": output_keys["small_batch_run_report"],
        "daily_price_promotion_validator_report_key": promotion_keys.get("daily_price_promotion_validator_report"),
        "standard_daily_price_promotion_dry_run_report_key": promotion_keys.get("standard_daily_price_promotion_dry_run_report"),
        "standard_daily_price_promotion_apply_report_key": promotion_keys.get("standard_daily_price_promotion_apply_report"),
        "provider_call_requested": bool(report["provider"]["enabled"]),
        "reused_existing_staging": bool(report["provider"]["reuse_existing_staging"]),
        "apply_requested": bool(report["apply"]["requested"]),
        "standard_daily_price_write_performed": bool(report["standard_daily_price_write_performed"]),
        "standard_suspension_status_write_performed": False,
        "clean_factor_selection_backtest_entered": False,
        "real_backtest_performed": False,
        "read_back_verification": report["read_back_verification"],
        "upsert_summary": report["upsert_summary"],
        "blocked_reasons": report["blocked_reasons"],
    }


def _resolve_status(*, promotion_result: dict[str, Any] | None, blocked_reasons: list[str]) -> str:
    if blocked_reasons:
        return (promotion_result or {}).get("status") or "BLOCKED"
    return (promotion_result or {}).get("status") or "BLOCKED"


def _source_artifact_keys(batch_id: str) -> dict[str, str]:
    batch_id = _validate_batch_id(batch_id)
    return {
        "promotion_preflight_report": f"candidate/tushare/promotion_preflight_report/batch_id={batch_id}/report.json",
        "daily_price_candidate_batch": f"candidate/tushare/daily_price_candidate_batch/batch_id={batch_id}/part.parquet",
        "suspension_status_candidate_batch": (
            f"candidate/tushare/suspension_status_candidate_batch/batch_id={batch_id}/part.parquet"
        ),
    }


def _requested_scope(
    *,
    codes: list[str],
    start_date: str,
    end_date: str,
    max_codes: int,
    max_trade_days: int,
    max_rows: int,
) -> dict[str, Any]:
    return {
        "codes": list(codes),
        "start_date": start_date,
        "end_date": end_date,
        "max_codes": max_codes,
        "max_trade_days": max_trade_days,
        "max_rows": max_rows,
    }


def _provider_report(
    *,
    provider_call_enabled: bool,
    reuse_existing_staging: bool,
    provider_status: str,
) -> dict[str, Any]:
    return {
        "enabled": bool(provider_call_enabled),
        "status": provider_status,
        "reuse_existing_staging": bool(reuse_existing_staging),
    }


def _normalize_codes(codes: list[str]) -> list[str]:
    normalized = []
    for code in codes:
        text = str(code).strip().upper()
        if text:
            normalized.append(validate_stock_code(text))
    if not normalized:
        raise ValueError("codes must not be empty")
    return normalized


def _validate_positive_limits(*, max_codes: int, max_trade_days: int, max_rows: int) -> None:
    if max_codes <= 0:
        raise ValueError("max_codes must be positive")
    if max_trade_days <= 0:
        raise ValueError("max_trade_days must be positive")
    if max_rows <= 0:
        raise ValueError("max_rows must be positive")


def _validate_batch_id(batch_id: str) -> str:
    text = str(batch_id or "").strip()
    if not text:
        raise ValueError("batch_id is required")
    if any(ch in text for ch in ("/", "\\", "..")):
        raise ValueError("batch_id must not contain path separators")
    return text


def _dedupe(values: list[str]) -> list[str]:
    seen = set()
    result = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

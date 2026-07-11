from __future__ import annotations

from datetime import datetime, timedelta, timezone
import math
import re
import time
from typing import Any, Callable

import pandas as pd

from stock_selector.data.data_validator import DataValidationError, validate_dataset_frame, validate_stock_code
from stock_selector.data.tushare_candidate_staging_batch import build_tushare_candidate_staging_batch_output_keys
from stock_selector.data.tushare_standard_inputs_landing import _upsert_frame
from stock_selector.providers.schema_contract import REQUIRED_BENCHMARK_INDEXES, get_schema_contract
from stock_selector.providers.schema_mapper import map_provider_frame
from stock_selector.storage.partition import build_partition
from stock_selector.utils.date_validator import validate_date_range


LoadParquetFn = Callable[[str], pd.DataFrame]
LoadJsonFn = Callable[[str], dict[str, Any]]
WriteParquetFn = Callable[[str, pd.DataFrame], str]
WriteJsonFn = Callable[[str, dict[str, Any]], str]
StandardReadFn = Callable[[str, str], pd.DataFrame]
StandardWriteFn = Callable[[str, str, pd.DataFrame], str]

REQUIRED_INPUTS = (
    "stock_basic",
    "daily_price",
    "adj_factor",
    "daily_basic",
    "financial",
    "st_history",
    "benchmark_price",
)
PROMOTED_INPUTS = ("stock_basic", "adj_factor", "st_history", "benchmark_price")
CANONICAL_INPUTS = ("daily_price", "daily_basic", "financial")
KEY_COLUMNS = {
    "stock_basic": ["stock_code", "trade_date"],
    "daily_price": ["stock_code", "trade_date"],
    "adj_factor": ["stock_code", "trade_date"],
    "daily_basic": ["stock_code", "trade_date"],
    "financial": ["stock_code", "report_period", "announce_date"],
    "st_history": ["stock_code", "st_type", "start_date", "source"],
    "benchmark_price": ["index_code", "trade_date"],
}
CURRENT_ST_MARKERS = {"CURRENT_NAME_SNAPSHOT", "CURRENT_ST_SNAPSHOT"}
CURRENT_SOURCE_MARKERS = {"current_stock_basic_snapshot", "current_name_snapshot", "current_st_snapshot"}
_BATCH_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


def build_real_clean_inputs_output_keys(batch_id: str, trade_dates: list[str]) -> dict[str, Any]:
    batch_id = _validate_batch_id(batch_id)
    return {
        "manifest": f"candidate/real_clean_inputs/manifest/batch_id={batch_id}/manifest.json",
        "readiness_report": f"candidate/real_clean_inputs/readiness_report/batch_id={batch_id}/report.json",
        "adj_factor_staging": {
            trade_date: (
                f"candidate/real_clean_inputs/adj_factor_staging/batch_id={batch_id}/"
                f"trade_date={trade_date}/part.parquet"
            )
            for trade_date in trade_dates
        },
        "benchmark_price_staging": {
            trade_date: (
                f"candidate/real_clean_inputs/benchmark_price_staging/batch_id={batch_id}/"
                f"trade_date={trade_date}/part.parquet"
            )
            for trade_date in trade_dates
        },
        "stock_basic_history_staging": {
            trade_date: (
                f"candidate/real_clean_inputs/stock_basic_history_staging/batch_id={batch_id}/"
                f"trade_date={trade_date}/part.parquet"
            )
            for trade_date in trade_dates
        },
        "st_history_interval_staging": (
            f"candidate/real_clean_inputs/st_history_interval_staging/batch_id={batch_id}/part.parquet"
        ),
        "st_history_interval_coverage": (
            f"candidate/real_clean_inputs/st_history_interval_staging/batch_id={batch_id}/coverage.json"
        ),
    }


def run_real_clean_inputs_small_batch(
    *,
    batch_id: str,
    start_date: str,
    end_date: str,
    codes: list[str],
    load_parquet_fn: LoadParquetFn,
    load_json_fn: LoadJsonFn,
    write_parquet_fn: WriteParquetFn,
    write_json_fn: WriteJsonFn,
    standard_read_fn: StandardReadFn,
    standard_write_fn: StandardWriteFn,
    adj_factor_provider=None,
    benchmark_provider=None,
    provider_errors: dict[str, str] | None = None,
    provider_call_enabled: bool = False,
    reuse_existing_staging: bool = False,
    apply_standard_write: bool = False,
    max_codes: int = 5,
    max_trade_days: int = 5,
    max_rows: int = 100,
    sleep_seconds: float = 0.0,
    generated_at_fn: Callable[[], str] | None = None,
    cli_command: str | None = None,
) -> dict[str, Any]:
    batch_id = _validate_batch_id(batch_id)
    start_date, end_date = validate_date_range(start_date, end_date)
    codes = _normalize_codes(codes)
    trade_dates = _trade_dates(start_date, end_date)
    _validate_scope_guards(
        codes=codes,
        trade_dates=trade_dates,
        max_codes=max_codes,
        max_trade_days=max_trade_days,
        max_rows=max_rows,
        sleep_seconds=sleep_seconds,
    )
    generated_at_fn = generated_at_fn or _utc_now_iso
    output_keys = build_real_clean_inputs_output_keys(batch_id, trade_dates)
    provider_errors = dict(provider_errors or {})

    provider_run = _fetch_provider_staging(
        trade_dates=trade_dates,
        codes=codes,
        output_keys=output_keys,
        adj_factor_provider=adj_factor_provider,
        benchmark_provider=benchmark_provider,
        provider_errors=provider_errors,
        provider_call_enabled=provider_call_enabled,
        max_rows=max_rows,
        sleep_seconds=sleep_seconds,
        write_parquet_fn=write_parquet_fn,
    )

    inputs: dict[str, dict[str, Any]] = {
        dataset: _empty_input_status(dataset, codes, trade_dates, apply_standard_write)
        for dataset in REQUIRED_INPUTS
    }
    source_frames: dict[str, dict[str, pd.DataFrame]] = {}

    for dataset in CANONICAL_INPUTS:
        status, frames = _audit_canonical_partitions(
            dataset=dataset,
            trade_dates=trade_dates,
            codes=codes,
            standard_read_fn=standard_read_fn,
        )
        inputs[dataset] = status
        source_frames[dataset] = frames

    stock_status, stock_frames = _audit_stock_basic_history(
        batch_id=batch_id,
        trade_dates=trade_dates,
        codes=codes,
        output_keys=output_keys,
        load_parquet_fn=load_parquet_fn,
        reuse_existing_staging=reuse_existing_staging,
    )
    inputs["stock_basic"] = stock_status
    source_frames["stock_basic"] = stock_frames

    adj_status, adj_frames = _audit_adj_factor(
        batch_id=batch_id,
        trade_dates=trade_dates,
        codes=codes,
        output_keys=output_keys,
        load_parquet_fn=load_parquet_fn,
        load_json_fn=load_json_fn,
        standard_read_fn=standard_read_fn,
        reuse_existing_staging=reuse_existing_staging,
    )
    _append_provider_error(adj_status, provider_run["errors"].get("adj_factor"), provider_call_enabled)
    inputs["adj_factor"] = adj_status
    source_frames["adj_factor"] = adj_frames

    benchmark_status, benchmark_frames = _audit_benchmark_price(
        trade_dates=trade_dates,
        output_keys=output_keys,
        load_parquet_fn=load_parquet_fn,
        standard_read_fn=standard_read_fn,
    )
    _append_provider_error(
        benchmark_status,
        provider_run["errors"].get("benchmark_price"),
        provider_call_enabled,
    )
    inputs["benchmark_price"] = benchmark_status
    source_frames["benchmark_price"] = benchmark_frames

    st_status, st_frames = _audit_st_history(
        batch_id=batch_id,
        start_date=start_date,
        end_date=end_date,
        trade_dates=trade_dates,
        codes=codes,
        output_keys=output_keys,
        load_parquet_fn=load_parquet_fn,
        load_json_fn=load_json_fn,
        reuse_existing_staging=reuse_existing_staging,
    )
    inputs["st_history"] = st_status
    source_frames["st_history"] = st_frames

    for dataset in PROMOTED_INPUTS:
        inputs[dataset]["write"]["requested"] = bool(apply_standard_write)

    ready_for_apply = all(status["ready_for_apply"] for status in inputs.values())
    upsert_summary = {dataset: _zero_summary() for dataset in PROMOTED_INPUTS}
    standard_writes_performed = False
    write_errors: list[str] = []

    if apply_standard_write and ready_for_apply:
        standard_writes_performed, upsert_summary, write_errors = _apply_promoted_inputs(
            trade_dates=trade_dates,
            inputs=inputs,
            source_frames=source_frames,
            standard_read_fn=standard_read_fn,
            standard_write_fn=standard_write_fn,
        )
    elif apply_standard_write:
        for dataset in PROMOTED_INPUTS:
            inputs[dataset]["write"]["status"] = "BLOCKED_BY_PREFLIGHT"

    read_back_verification = _verify_all_canonical_inputs(
        trade_dates=trade_dates,
        codes=codes,
        inputs=inputs,
        expected_frames=source_frames,
        standard_read_fn=standard_read_fn,
        apply_requested=apply_standard_write,
        ready_for_apply=ready_for_apply,
        writes_succeeded=standard_writes_performed and not write_errors,
    )
    ready_for_clean = bool(read_back_verification["passed"])
    for dataset in REQUIRED_INPUTS:
        inputs[dataset]["ready_for_clean"] = bool(inputs[dataset]["read_back"]["passed"] and ready_for_clean)

    blocked_reasons = _collect_blocked_reasons(inputs)
    blocked_reasons.extend(write_errors)
    if apply_standard_write and ready_for_apply and not ready_for_clean:
        blocked_reasons.append("READ_BACK_VERIFICATION_FAILED")
    blocked_reasons = _unique(blocked_reasons)
    status = "READY" if ready_for_clean else ("READY_FOR_APPLY" if ready_for_apply and not apply_standard_write else "BLOCKED")
    downstream_firewalls = {
        "adjusted_price_entered": False,
        "clean_daily_snapshot_entered": False,
        "universe_entered": False,
        "factor_entered": False,
        "selection_entered": False,
        "backtest_entered": False,
    }
    requested_scope = {
        "codes": codes,
        "start_date": start_date,
        "end_date": end_date,
        "trade_dates": trade_dates,
        "max_codes": max_codes,
        "max_trade_days": max_trade_days,
        "max_rows": max_rows,
    }
    report = {
        "schema_version": "goal20.real_clean_input_readiness.v1",
        "goal": "20",
        "batch_id": batch_id,
        "generated_at": generated_at_fn(),
        "status": status,
        "mode": "APPLY" if apply_standard_write else "DRY_RUN",
        "requested_scope": requested_scope,
        "provider": {
            "provider_call_requested": bool(provider_call_enabled),
            "reuse_existing_staging": bool(reuse_existing_staging),
            "staging_writes": provider_run["written_keys"],
            "errors": provider_run["errors"],
        },
        "apply_requested": bool(apply_standard_write),
        "standard_writes_performed": bool(standard_writes_performed),
        "ready_for_apply": ready_for_apply,
        "ready_for_clean": ready_for_clean,
        "inputs": inputs,
        "upsert_summary": upsert_summary,
        "read_back_verification": read_back_verification,
        "blocked_reasons": blocked_reasons,
        "downstream_firewalls": downstream_firewalls,
        "cli_command": cli_command,
        "output_object_keys": output_keys,
    }
    write_json_fn(output_keys["readiness_report"], report)
    manifest = {
        "schema_version": "goal20.real_clean_input_manifest.v1",
        "goal": "20",
        "batch_id": batch_id,
        "generated_at": generated_at_fn(),
        "status": "COMPLETED",
        "readiness_status": status,
        "ready_for_apply": ready_for_apply,
        "ready_for_clean": ready_for_clean,
        "requested_scope": requested_scope,
        "readiness_report_key": output_keys["readiness_report"],
        "source_keys": {dataset: inputs[dataset]["source_keys"] for dataset in REQUIRED_INPUTS},
        "blocked_reasons": blocked_reasons,
        "downstream_firewalls": downstream_firewalls,
    }
    write_json_fn(output_keys["manifest"], manifest)

    return {
        "goal": "20",
        "status": status,
        "batch_id": batch_id,
        "mode": report["mode"],
        "provider_call_requested": bool(provider_call_enabled),
        "reused_existing_staging": bool(reuse_existing_staging),
        "apply_requested": bool(apply_standard_write),
        "standard_writes_performed": bool(standard_writes_performed),
        "ready_for_apply": ready_for_apply,
        "ready_for_clean": ready_for_clean,
        "inputs": inputs,
        "upsert_summary": upsert_summary,
        "read_back_verification": read_back_verification,
        "blocked_reasons": blocked_reasons,
        "readiness_report_key": output_keys["readiness_report"],
        "manifest_key": output_keys["manifest"],
        "output_object_keys": output_keys,
        "readiness_report": report,
        "manifest": manifest,
        "clean_factor_selection_backtest_entered": False,
        "real_backtest_performed": False,
    }


def _fetch_provider_staging(
    *,
    trade_dates: list[str],
    codes: list[str],
    output_keys: dict[str, Any],
    adj_factor_provider,
    benchmark_provider,
    provider_errors: dict[str, str],
    provider_call_enabled: bool,
    max_rows: int,
    sleep_seconds: float,
    write_parquet_fn: WriteParquetFn,
) -> dict[str, Any]:
    errors = dict(provider_errors)
    written_keys: list[str] = []
    if not provider_call_enabled:
        return {"errors": errors, "written_keys": written_keys}

    if adj_factor_provider is None and "adj_factor" not in errors:
        errors["adj_factor"] = "ADJ_FACTOR_PROVIDER_UNAVAILABLE"
    if benchmark_provider is None and "benchmark_price" not in errors:
        errors["benchmark_price"] = "BENCHMARK_PROVIDER_UNAVAILABLE"

    for index, trade_date in enumerate(trade_dates):
        if adj_factor_provider is not None:
            try:
                raw = adj_factor_provider.fetch_adj_factor(trade_date)
                frame = _map_or_select_provider_frame("tushare", "adj_factor", raw, trade_date)
                frame = frame[frame["stock_code"].isin(codes)].reset_index(drop=True)
                _validate_adj_frame(frame, trade_date, codes)
                if len(frame) > max_rows:
                    raise DataValidationError("adj_factor exceeds max_rows")
                key = output_keys["adj_factor_staging"][trade_date]
                write_parquet_fn(key, frame)
                written_keys.append(key)
            except Exception as exc:
                errors["adj_factor"] = f"ADJ_FACTOR_PROVIDER_FETCH_FAILED: {exc}"
        if benchmark_provider is not None:
            try:
                raw = benchmark_provider.fetch_benchmark_price(trade_date)
                frame = _map_or_select_provider_frame("akshare", "benchmark_price", raw, trade_date)
                _validate_benchmark_frame(frame, trade_date)
                if len(frame) > max_rows:
                    raise DataValidationError("benchmark_price exceeds max_rows")
                key = output_keys["benchmark_price_staging"][trade_date]
                write_parquet_fn(key, frame)
                written_keys.append(key)
            except Exception as exc:
                errors["benchmark_price"] = f"BENCHMARK_PROVIDER_FETCH_FAILED: {exc}"
        if sleep_seconds and index < len(trade_dates) - 1:
            time.sleep(sleep_seconds)
    return {"errors": errors, "written_keys": written_keys}


def _audit_canonical_partitions(
    *,
    dataset: str,
    trade_dates: list[str],
    codes: list[str],
    standard_read_fn: StandardReadFn,
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    status = _empty_input_status(dataset, codes, trade_dates, False)
    status["dq_level"] = "DQ3_STANDARD_CANONICAL"
    frames: dict[str, pd.DataFrame] = {}
    errors: list[str] = []
    missing_pairs: list[dict[str, str]] = []
    for trade_date in trade_dates:
        key = build_partition(dataset, trade_date).object_key
        status["source_keys"].append(key)
        frame = pd.DataFrame()
        try:
            raw = standard_read_fn(dataset, trade_date)
            frame = _scope_frame(dataset, raw, codes)
            _validate_scoped_canonical(dataset, frame, trade_date, codes)
            frames[trade_date] = frame
        except Exception as exc:
            errors.append(f"{trade_date}: {exc}")
            missing_pairs.extend(_missing_pairs(dataset, frame, codes, trade_date))
    status["row_count"] = sum(len(frame) for frame in frames.values())
    status["coverage"] = _coverage_record(codes, trade_dates, missing_pairs)
    if errors:
        status["blocked_reasons"] = [_canonical_reason(dataset, errors)]
        status["validation"] = {"passed": False, "errors": errors}
    else:
        status["validation"] = {"passed": True, "errors": []}
        status["ready_for_apply"] = True
        status["read_back"] = {"passed": True, "status": "CANONICAL_AUDIT_PASS", "details": []}
    return status, frames


def _audit_adj_factor(
    *,
    batch_id: str,
    trade_dates: list[str],
    codes: list[str],
    output_keys: dict[str, Any],
    load_parquet_fn: LoadParquetFn,
    load_json_fn: LoadJsonFn,
    standard_read_fn: StandardReadFn,
    reuse_existing_staging: bool,
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    status = _empty_input_status("adj_factor", codes, trade_dates, False)
    status["dq_level"] = "DQ1_PROVIDER_STAGING"
    goal13_keys = build_tushare_candidate_staging_batch_output_keys(batch_id, trade_dates)
    goal13_manifest = None
    used_goal13 = False

    frames: dict[str, pd.DataFrame] = {}
    errors: list[str] = []
    missing_pairs: list[dict[str, str]] = []
    for trade_date in trade_dates:
        frame = None
        selected_key = None
        candidates = [output_keys["adj_factor_staging"][trade_date]]
        if reuse_existing_staging:
            candidates.append(goal13_keys["adj_factor_staging"][trade_date])
        for key in candidates:
            try:
                frame = load_parquet_fn(key)
                selected_key = key
                used_goal13 = used_goal13 or key == goal13_keys["adj_factor_staging"][trade_date]
                break
            except FileNotFoundError:
                continue
            except Exception as exc:
                errors.append(f"{trade_date}: source read failed: {exc}")
                break
        if frame is None:
            try:
                canonical = standard_read_fn("adj_factor", trade_date)
                if canonical is not None and not canonical.empty:
                    frame = canonical
                    selected_key = build_partition("adj_factor", trade_date).object_key
                    status["dq_level"] = "DQ3_STANDARD_CANONICAL"
            except Exception as exc:
                errors.append(f"{trade_date}: canonical read failed: {exc}")
        if selected_key:
            status["source_keys"].append(selected_key)
        if frame is None or frame.empty:
            errors.append(f"{trade_date}: adj_factor source missing")
            missing_pairs.extend({"stock_code": code, "trade_date": trade_date} for code in codes)
            continue
        try:
            normalized = pd.DataFrame()
            normalized = _normalize_adj_source(frame, trade_date)
            normalized = normalized[normalized["stock_code"].isin(codes)].reset_index(drop=True)
            _validate_adj_frame(normalized, trade_date, codes)
            frames[trade_date] = normalized
        except Exception as exc:
            errors.append(f"{trade_date}: {exc}")
            missing_pairs.extend(_missing_pairs("adj_factor", normalized, codes, trade_date))

    if used_goal13:
        try:
            goal13_manifest = load_json_fn(goal13_keys["manifest"])
        except FileNotFoundError:
            errors.append("Goal 13 adj_factor staging is missing its manifest")
            status["blocked_reasons"].append("GOAL13_ADJ_FACTOR_MANIFEST_MISSING")
        except Exception as exc:
            errors.append(f"Goal 13 manifest read failed: {exc}")
            status["blocked_reasons"].append("GOAL13_MANIFEST_READ_FAILED")
    if goal13_manifest is not None:
        status["lineage"] = {
            "goal13_manifest_key": goal13_keys["manifest"],
            "goal13_manifest_status": goal13_manifest.get("status"),
        }
        status["source_keys"].append(goal13_keys["manifest"])
        manifest_errors = _validate_goal13_adj_manifest(goal13_manifest, batch_id, codes, trade_dates)
        errors.extend(manifest_errors)
        if manifest_errors:
            status["blocked_reasons"].append("GOAL13_ADJ_FACTOR_MANIFEST_INVALID")
    status["source_keys"] = _unique(status["source_keys"])
    status["row_count"] = sum(len(frame) for frame in frames.values())
    status["coverage"] = _coverage_record(codes, trade_dates, missing_pairs)
    if errors or status["blocked_reasons"]:
        status["blocked_reasons"].extend(_adj_reasons(errors))
        status["blocked_reasons"] = _unique(status["blocked_reasons"])
        status["validation"] = {"passed": False, "errors": errors}
    else:
        status["validation"] = {"passed": True, "errors": []}
        status["ready_for_apply"] = True
    return status, frames


def _audit_benchmark_price(
    *,
    trade_dates: list[str],
    output_keys: dict[str, Any],
    load_parquet_fn: LoadParquetFn,
    standard_read_fn: StandardReadFn,
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    status = _empty_input_status("benchmark_price", [], trade_dates, False)
    status["dq_level"] = "DQ2_AKSHARE_BENCHMARK"
    status["coverage"]["required_indexes"] = sorted(REQUIRED_BENCHMARK_INDEXES)
    frames: dict[str, pd.DataFrame] = {}
    errors: list[str] = []
    missing_indexes: dict[str, list[str]] = {}
    for trade_date in trade_dates:
        staging_key = output_keys["benchmark_price_staging"][trade_date]
        frame = None
        try:
            frame = load_parquet_fn(staging_key)
            status["source_keys"].append(staging_key)
        except FileNotFoundError:
            try:
                canonical = standard_read_fn("benchmark_price", trade_date)
                if canonical is not None and not canonical.empty:
                    frame = canonical
                    status["source_keys"].append(build_partition("benchmark_price", trade_date).object_key)
                    status["dq_level"] = "DQ3_STANDARD_CANONICAL"
            except Exception as exc:
                errors.append(f"{trade_date}: canonical read failed: {exc}")
        except Exception as exc:
            errors.append(f"{trade_date}: source read failed: {exc}")
        if frame is None or frame.empty:
            errors.append(f"{trade_date}: benchmark_price source missing")
            missing_indexes[trade_date] = sorted(REQUIRED_BENCHMARK_INDEXES)
            continue
        try:
            normalized = _normalize_benchmark_source(frame, trade_date)
            _validate_benchmark_frame(normalized, trade_date)
            frames[trade_date] = normalized
        except Exception as exc:
            errors.append(f"{trade_date}: {exc}")
            present = set(frame.get("index_code", pd.Series(dtype=str)).astype(str))
            missing_indexes[trade_date] = sorted(REQUIRED_BENCHMARK_INDEXES - present)

    status["row_count"] = sum(len(frame) for frame in frames.values())
    status["coverage"].update(
        {
            "passed": not errors,
            "missing_indexes_by_date": missing_indexes,
            "requested_trade_dates": trade_dates,
        }
    )
    if errors:
        status["blocked_reasons"] = _benchmark_reasons(errors, missing_indexes)
        status["validation"] = {"passed": False, "errors": errors}
    else:
        status["validation"] = {"passed": True, "errors": []}
        status["ready_for_apply"] = True
    return status, frames


def _audit_stock_basic_history(
    *,
    batch_id: str,
    trade_dates: list[str],
    codes: list[str],
    output_keys: dict[str, Any],
    load_parquet_fn: LoadParquetFn,
    reuse_existing_staging: bool,
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    status = _empty_input_status("stock_basic", codes, trade_dates, False)
    status["dq_level"] = "DQ1_VERIFIED_HISTORICAL_SNAPSHOT"
    frames: dict[str, pd.DataFrame] = {}
    errors: list[str] = []
    missing_pairs: list[dict[str, str]] = []
    for trade_date in trade_dates:
        key = output_keys["stock_basic_history_staging"][trade_date]
        try:
            raw = load_parquet_fn(key)
            status["source_keys"].append(key)
        except FileNotFoundError:
            raw = None
        except Exception as exc:
            raw = None
            errors.append(f"{trade_date}: source read failed: {exc}")
        if raw is None or raw.empty:
            errors.append(f"{trade_date}: trusted historical stock_basic source missing")
            missing_pairs.extend({"stock_code": code, "trade_date": trade_date} for code in codes)
            continue
        try:
            frame = _validate_historical_stock_frame(raw, trade_date, codes, load_parquet_fn)
            frames[trade_date] = frame
        except Exception as exc:
            errors.append(f"{trade_date}: {exc}")
            missing_pairs.extend(_missing_pairs("stock_basic", raw, codes, trade_date))

    if errors and reuse_existing_staging:
        current_key = f"candidate/tushare/standard_inputs/stock_basic_staging/batch_id={batch_id}/part.parquet"
        try:
            current = load_parquet_fn(current_key)
            if current is not None and not current.empty:
                status["source_keys"].append(current_key)
                status["dq_level"] = "DQ2_CURRENT_SNAPSHOT_ONLY"
                status["blocked_reasons"].append("CURRENT_SNAPSHOT_NOT_HISTORICAL")
        except FileNotFoundError:
            pass
        except Exception as exc:
            errors.append(f"current snapshot read failed: {exc}")

    status["row_count"] = sum(len(frame) for frame in frames.values())
    status["coverage"] = _coverage_record(codes, trade_dates, missing_pairs)
    if errors or status["blocked_reasons"]:
        if not status["blocked_reasons"]:
            status["blocked_reasons"] = [_stock_reason(errors)]
        status["validation"] = {"passed": False, "errors": errors}
    else:
        status["validation"] = {"passed": True, "errors": []}
        status["ready_for_apply"] = True
    return status, frames


def _audit_st_history(
    *,
    batch_id: str,
    start_date: str,
    end_date: str,
    trade_dates: list[str],
    codes: list[str],
    output_keys: dict[str, Any],
    load_parquet_fn: LoadParquetFn,
    load_json_fn: LoadJsonFn,
    reuse_existing_staging: bool,
) -> tuple[dict[str, Any], dict[str, pd.DataFrame]]:
    status = _empty_input_status("st_history", codes, trade_dates, False)
    status["dq_level"] = "DQ1_VERIFIED_HISTORICAL_INTERVALS"
    key = output_keys["st_history_interval_staging"]
    errors: list[str] = []
    frames: dict[str, pd.DataFrame] = {}
    try:
        raw = load_parquet_fn(key)
        status["source_keys"].append(key)
        coverage_proof = None
        if raw.empty:
            coverage_key = output_keys["st_history_interval_coverage"]
            coverage_proof = load_json_fn(coverage_key)
            status["source_keys"].append(coverage_key)
            status["source_keys"].extend(coverage_proof.get("source_object_keys", []))
        frame = _validate_historical_st_frame(
            raw,
            start_date,
            end_date,
            codes,
            load_parquet_fn,
            coverage_proof=coverage_proof,
        )
        for trade_date in trade_dates:
            validate_dataset_frame("st_history", frame, trade_date)
            frames[trade_date] = frame.copy()
    except FileNotFoundError:
        errors.append("trusted historical st_history source missing")
    except Exception as exc:
        errors.append(str(exc))

    if errors and reuse_existing_staging:
        current_key = f"candidate/tushare/standard_inputs/st_history_staging/batch_id={batch_id}/part.parquet"
        try:
            current = load_parquet_fn(current_key)
            if current is not None and not current.empty:
                status["source_keys"].append(current_key)
                status["dq_level"] = "DQ2_CURRENT_SNAPSHOT_ONLY"
                status["blocked_reasons"].append("ST_STATUS_NOT_HISTORICAL")
        except FileNotFoundError:
            pass
        except Exception as exc:
            errors.append(f"current ST snapshot read failed: {exc}")

    status["row_count"] = len(next(iter(frames.values()))) if frames else 0
    status["coverage"] = {
        "passed": not errors,
        "requested_codes": codes,
        "requested_trade_dates": trade_dates,
        "coverage_start_date": start_date,
        "coverage_end_date": end_date,
    }
    if errors or status["blocked_reasons"]:
        if not status["blocked_reasons"]:
            status["blocked_reasons"] = [_st_reason(errors)]
        status["validation"] = {"passed": False, "errors": errors}
    else:
        status["validation"] = {"passed": True, "errors": []}
        status["ready_for_apply"] = True
    return status, frames


def _apply_promoted_inputs(
    *,
    trade_dates: list[str],
    inputs: dict[str, dict[str, Any]],
    source_frames: dict[str, dict[str, pd.DataFrame]],
    standard_read_fn: StandardReadFn,
    standard_write_fn: StandardWriteFn,
) -> tuple[bool, dict[str, dict[str, int]], list[str]]:
    summaries = {dataset: _zero_summary() for dataset in PROMOTED_INPUTS}
    errors: list[str] = []
    any_write = False
    for dataset in PROMOTED_INPUTS:
        columns = get_schema_contract(dataset).columns
        input_status = inputs[dataset]
        input_status["write"] = {
            "requested": True,
            "status": "PENDING",
            "object_keys": [],
            **_zero_summary(),
        }
        for trade_date in trade_dates:
            incoming = source_frames[dataset][trade_date][columns].copy()
            try:
                existing = standard_read_fn(dataset, trade_date)
                upsert = _upsert_frame(
                    existing=existing,
                    incoming=incoming,
                    key_columns=KEY_COLUMNS[dataset],
                    columns=columns,
                )
                validate_dataset_frame(dataset, upsert["frame"], trade_date)
                standard_write_fn(dataset, trade_date, upsert["frame"])
                object_key = build_partition(dataset, trade_date).object_key
                input_status["write"]["object_keys"].append(object_key)
                _add_summary(summaries[dataset], upsert["summary"])
                any_write = True
            except Exception as exc:
                errors.append(f"{dataset.upper()}_STANDARD_WRITE_FAILED: {trade_date}: {exc}")
                break
        input_status["write"].update(summaries[dataset])
        input_status["write"]["status"] = "WRITTEN" if not errors else "FAILED"
        if errors:
            break
    if errors:
        for dataset in PROMOTED_INPUTS:
            write_status = inputs[dataset]["write"]
            if write_status["requested"] and write_status["status"] == "NOT_REQUESTED":
                write_status["status"] = "SKIPPED_AFTER_WRITE_FAILURE"
    return any_write, summaries, errors


def _verify_all_canonical_inputs(
    *,
    trade_dates: list[str],
    codes: list[str],
    inputs: dict[str, dict[str, Any]],
    expected_frames: dict[str, dict[str, pd.DataFrame]],
    standard_read_fn: StandardReadFn,
    apply_requested: bool,
    ready_for_apply: bool,
    writes_succeeded: bool,
) -> dict[str, Any]:
    should_verify = ready_for_apply and (not apply_requested or writes_succeeded)
    if not should_verify:
        return {"passed": False, "status": "NOT_RUN", "details": []}

    details: list[dict[str, Any]] = []
    all_passed = True
    for dataset in REQUIRED_INPUTS:
        dataset_details = []
        dataset_passed = True
        for trade_date in trade_dates:
            try:
                actual = standard_read_fn(dataset, trade_date)
                actual = _scope_frame(dataset, actual, codes)
                validate_dataset_frame(dataset, actual, trade_date)
                expected = expected_frames[dataset][trade_date]
                _assert_scope_equal(dataset, actual, expected)
                dataset_details.append({"trade_date": trade_date, "passed": True, "row_count": len(actual)})
            except Exception as exc:
                dataset_passed = False
                all_passed = False
                dataset_details.append({"trade_date": trade_date, "passed": False, "error": str(exc)})
        inputs[dataset]["read_back"] = {
            "passed": dataset_passed,
            "status": "PASS" if dataset_passed else "FAILED",
            "details": dataset_details,
        }
        details.append({"dataset": dataset, "passed": dataset_passed, "details": dataset_details})
    return {"passed": all_passed, "status": "PASS" if all_passed else "FAILED", "details": details}


def _validate_historical_stock_frame(
    raw: pd.DataFrame,
    trade_date: str,
    codes: list[str],
    load_parquet_fn: LoadParquetFn,
) -> pd.DataFrame:
    required = {"source_snapshot_date", "source_object_key", "source_semantics"}
    missing = sorted(required - set(raw.columns))
    if missing:
        raise DataValidationError(f"historical stock_basic proof fields missing: {', '.join(missing)}")
    if set(raw["source_semantics"].astype(str)) != {"POINT_IN_TIME_HISTORICAL_SNAPSHOT"}:
        raise DataValidationError("stock_basic source semantics are not point-in-time historical")
    if (raw["source_snapshot_date"].astype(str) != trade_date).any():
        raise DataValidationError("stock_basic source_snapshot_date must equal partition trade_date")
    if raw["source_object_key"].isna().any() or (raw["source_object_key"].astype(str).str.strip() == "").any():
        raise DataValidationError("stock_basic source_object_key must be auditable")
    _reject_untrusted_lineage_keys(raw["source_object_key"])
    columns = get_schema_contract("stock_basic").columns
    frame = raw[columns].copy()
    frame = frame[frame["stock_code"].isin(codes)].reset_index(drop=True)
    validate_dataset_frame("stock_basic", frame, trade_date)
    _require_code_coverage(frame, codes, trade_date)
    if (frame["list_date"].astype(str) > trade_date).any():
        raise DataValidationError("stock_basic contains future list_date")
    delisted = frame["delist_date"].dropna().astype(str)
    if (delisted <= trade_date).any():
        raise DataValidationError("stock_basic contains stocks already delisted on snapshot date")
    for source_key, group in raw[raw["stock_code"].isin(codes)].groupby("source_object_key"):
        evidence = load_parquet_fn(str(source_key))
        evidence = _scope_frame("stock_basic", evidence, list(group["stock_code"].astype(str)))
        _assert_scope_equal("stock_basic", evidence, group[get_schema_contract("stock_basic").columns])
    return frame


def _validate_historical_st_frame(
    raw: pd.DataFrame,
    start_date: str,
    end_date: str,
    codes: list[str],
    load_parquet_fn: LoadParquetFn,
    *,
    coverage_proof: dict[str, Any] | None = None,
) -> pd.DataFrame:
    columns = get_schema_contract("st_history").columns
    missing_columns = sorted(set(columns) - set(raw.columns))
    if missing_columns:
        raise DataValidationError(f"st_history standard columns missing: {', '.join(missing_columns)}")
    if raw.empty:
        _validate_empty_st_coverage_proof(
            coverage_proof,
            start_date=start_date,
            end_date=end_date,
            codes=codes,
            load_parquet_fn=load_parquet_fn,
        )
        return raw[columns].copy()

    required = {
        "source_object_key",
        "source_semantics",
        "coverage_codes",
        "coverage_start_date",
        "coverage_end_date",
        "coverage_complete",
    }
    missing = sorted(required - set(raw.columns))
    if missing:
        raise DataValidationError(f"historical st_history proof fields missing: {', '.join(missing)}")
    if set(raw["source_semantics"].astype(str)) != {"HISTORICAL_INTERVAL_SOURCE"}:
        raise DataValidationError("st_history source semantics are not historical intervals")
    if not raw["coverage_complete"].map(_to_bool).all():
        raise DataValidationError("st_history coverage is incomplete")
    coverage_codes = set()
    for value in raw["coverage_codes"].astype(str):
        coverage_codes.update(item.strip() for item in value.split(",") if item.strip())
    if not set(codes).issubset(coverage_codes):
        raise DataValidationError("st_history code coverage incomplete")
    if (raw["coverage_start_date"].astype(str) > start_date).any():
        raise DataValidationError("st_history start-date coverage incomplete")
    if (raw["coverage_end_date"].astype(str) < end_date).any():
        raise DataValidationError("st_history end-date coverage incomplete")
    if raw["source_object_key"].isna().any() or (raw["source_object_key"].astype(str).str.strip() == "").any():
        raise DataValidationError("st_history source_object_key must be auditable")
    _reject_untrusted_lineage_keys(raw["source_object_key"])
    if raw["source"].astype(str).str.lower().isin(CURRENT_SOURCE_MARKERS).any():
        raise DataValidationError("current ST source cannot prove history")
    if raw["st_type"].astype(str).str.upper().isin(CURRENT_ST_MARKERS).any():
        raise DataValidationError("current ST status cannot prove history")
    frame = raw[columns].copy()
    frame = frame[frame["stock_code"].isin(codes)].reset_index(drop=True)
    if frame.empty:
        raise DataValidationError("st_history has no auditable interval rows for requested codes")
    for _, row in frame.iterrows():
        end_value = row["end_date"]
        if not pd.isna(end_value) and str(end_value) <= str(row["start_date"]):
            raise DataValidationError("st_history intervals must use end_date greater than start_date")
    if frame.duplicated(KEY_COLUMNS["st_history"]).any():
        raise DataValidationError("st_history duplicate interval keys")
    for source_key, group in raw[raw["stock_code"].isin(codes)].groupby("source_object_key"):
        evidence = load_parquet_fn(str(source_key))
        evidence = _scope_frame("st_history", evidence, codes)
        _assert_scope_equal("st_history", evidence, group[get_schema_contract("st_history").columns])
    return frame


def _validate_empty_st_coverage_proof(
    proof: dict[str, Any] | None,
    *,
    start_date: str,
    end_date: str,
    codes: list[str],
    load_parquet_fn: LoadParquetFn,
) -> None:
    if not proof:
        raise DataValidationError("empty st_history requires a separate coverage proof")
    if proof.get("schema_version") != "goal20.st_history_coverage.v1":
        raise DataValidationError("st_history coverage proof schema is invalid")
    if proof.get("source_semantics") != "HISTORICAL_INTERVAL_SOURCE":
        raise DataValidationError("st_history coverage proof is not historical")
    if not _to_bool(proof.get("coverage_complete")):
        raise DataValidationError("st_history coverage proof is incomplete")
    coverage_codes = proof.get("coverage_codes", [])
    if isinstance(coverage_codes, str):
        coverage_codes = [item.strip() for item in coverage_codes.split(",") if item.strip()]
    if not set(codes).issubset(set(coverage_codes)):
        raise DataValidationError("st_history coverage proof misses requested codes")
    proof_start, proof_end = validate_date_range(
        str(proof.get("coverage_start_date", "")),
        str(proof.get("coverage_end_date", "")),
    )
    if proof_start > start_date or proof_end < end_date:
        raise DataValidationError("st_history coverage proof misses requested dates")
    if int(proof.get("interval_row_count", -1)) != 0:
        raise DataValidationError("empty st_history proof must declare interval_row_count=0")
    source_keys = proof.get("source_object_keys", [])
    if not isinstance(source_keys, list) or not source_keys:
        raise DataValidationError("empty st_history proof requires upstream evidence keys")
    _reject_untrusted_lineage_keys(pd.Series(source_keys, dtype=str))
    expected = pd.DataFrame(columns=get_schema_contract("st_history").columns)
    for source_key in source_keys:
        evidence = load_parquet_fn(str(source_key))
        evidence = _scope_frame("st_history", evidence, codes)
        validate_dataset_frame("st_history", evidence, end_date)
        _assert_scope_equal("st_history", evidence, expected)


def _normalize_adj_source(frame: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    columns = get_schema_contract("adj_factor").columns
    if set(columns).issubset(frame.columns):
        result = frame[columns].copy()
        result["trade_date"] = result["trade_date"].astype(str).map(_normalize_date_text)
        result["stock_code"] = result["stock_code"].astype(str).str.upper()
        result["adj_factor"] = pd.to_numeric(result["adj_factor"], errors="coerce")
        return result
    return map_provider_frame("tushare", "adj_factor", frame, trade_date)


def _normalize_benchmark_source(frame: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    columns = get_schema_contract("benchmark_price").columns
    if set(columns).issubset(frame.columns):
        result = frame[columns].copy()
        result["trade_date"] = result["trade_date"].astype(str).map(_normalize_date_text)
        result["index_code"] = result["index_code"].astype(str).str.upper()
        for column in ["open", "high", "low", "close", "pct_chg"]:
            result[column] = pd.to_numeric(result[column], errors="coerce")
        return result
    return map_provider_frame("akshare", "benchmark_price", frame, trade_date)


def _map_or_select_provider_frame(provider: str, dataset: str, frame: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    if dataset == "adj_factor":
        return _normalize_adj_source(frame, trade_date)
    if dataset == "benchmark_price":
        return _normalize_benchmark_source(frame, trade_date)
    return map_provider_frame(provider, dataset, frame, trade_date)


def _validate_adj_frame(frame: pd.DataFrame, trade_date: str, codes: list[str]) -> None:
    if frame["adj_factor"].isna().any() or not frame["adj_factor"].map(lambda value: math.isfinite(float(value))).all():
        raise DataValidationError("adj_factor must be finite")
    if (frame["adj_factor"] <= 0).any():
        raise DataValidationError("adj_factor must be positive")
    validate_dataset_frame("adj_factor", frame, trade_date)
    _require_code_coverage(frame, codes, trade_date)


def _validate_benchmark_frame(frame: pd.DataFrame, trade_date: str) -> None:
    numeric_columns = ["open", "high", "low", "close", "pct_chg"]
    if frame[numeric_columns].isna().any().any():
        raise DataValidationError("benchmark_price numeric values must not be null")
    if not frame[numeric_columns].map(lambda value: math.isfinite(float(value))).all().all():
        raise DataValidationError("benchmark_price numeric values must be finite")
    if frame.duplicated(KEY_COLUMNS["benchmark_price"]).any():
        raise DataValidationError("benchmark_price duplicate index/date keys")
    validate_dataset_frame("benchmark_price", frame, trade_date)
    present = set(frame["index_code"].astype(str))
    if present != set(REQUIRED_BENCHMARK_INDEXES):
        missing = sorted(set(REQUIRED_BENCHMARK_INDEXES) - present)
        extra = sorted(present - set(REQUIRED_BENCHMARK_INDEXES))
        raise DataValidationError(f"benchmark index coverage mismatch; missing={missing}; extra={extra}")


def _validate_scoped_canonical(dataset: str, frame: pd.DataFrame, trade_date: str, codes: list[str]) -> None:
    validate_dataset_frame(dataset, frame, trade_date)
    _require_code_coverage(frame, codes, trade_date)
    if frame.duplicated(KEY_COLUMNS[dataset]).any():
        raise DataValidationError(f"{dataset} duplicate canonical keys")
    if dataset == "financial" and (frame["report_period"].astype(str) > trade_date).any():
        raise DataValidationError("financial report_period must not be in the future")


def _scope_frame(dataset: str, frame: pd.DataFrame | None, codes: list[str]) -> pd.DataFrame:
    if frame is None:
        return pd.DataFrame()
    result = frame.copy()
    if dataset == "benchmark_price":
        return result[result["index_code"].astype(str).isin(REQUIRED_BENCHMARK_INDEXES)].reset_index(drop=True) if "index_code" in result else result
    if "stock_code" in result:
        return result[result["stock_code"].astype(str).isin(codes)].reset_index(drop=True)
    return result


def _require_code_coverage(frame: pd.DataFrame, codes: list[str], trade_date: str) -> None:
    present = set(frame["stock_code"].astype(str)) if "stock_code" in frame else set()
    missing = sorted(set(codes) - present)
    if missing:
        raise DataValidationError(f"code coverage incomplete for {trade_date}: {', '.join(missing)}")


def _reject_untrusted_lineage_keys(values: pd.Series) -> None:
    for key in values.astype(str):
        raw_key = key.strip()
        normalized = raw_key.lower()
        path_parts = raw_key.replace("\\", "/").split("/")
        if not raw_key or raw_key.startswith(("/", "\\")) or "\\" in raw_key or ":" in raw_key or ".." in path_parts:
            raise DataValidationError("historical lineage object key is unsafe")
        if normalized.startswith("smoke/") or normalized.startswith("candidate/real_clean_inputs/"):
            raise DataValidationError("historical lineage must point to an upstream non-smoke evidence object")


def _assert_scope_equal(dataset: str, actual: pd.DataFrame, expected: pd.DataFrame) -> None:
    columns = get_schema_contract(dataset).columns
    keys = KEY_COLUMNS[dataset]
    left = actual[columns].copy().sort_values(keys, na_position="first").reset_index(drop=True)
    right = expected[columns].copy().sort_values(keys, na_position="first").reset_index(drop=True)
    pd.testing.assert_frame_equal(left, right, check_dtype=False, check_like=False)


def _empty_input_status(dataset: str, codes: list[str], trade_dates: list[str], apply_requested: bool) -> dict[str, Any]:
    return {
        "dataset": dataset,
        "source_keys": [],
        "row_count": 0,
        "dq_level": "UNAVAILABLE",
        "coverage": {
            "passed": False,
            "requested_codes": list(codes),
            "requested_trade_dates": list(trade_dates),
            "missing_pairs": [],
        },
        "validation": {"passed": False, "errors": []},
        "write": {
            "requested": bool(apply_requested),
            "status": "NOT_REQUESTED" if not apply_requested else "PENDING",
            "object_keys": [],
            **_zero_summary(),
        },
        "read_back": {"passed": False, "status": "NOT_RUN", "details": []},
        "ready_for_apply": False,
        "ready_for_clean": False,
        "blocked_reasons": [],
    }


def _coverage_record(codes: list[str], trade_dates: list[str], missing_pairs: list[dict[str, str]]) -> dict[str, Any]:
    expected = len(codes) * len(trade_dates)
    missing_pairs = _unique_dicts(missing_pairs)
    return {
        "passed": not missing_pairs,
        "requested_codes": codes,
        "requested_trade_dates": trade_dates,
        "expected_code_date_pairs": expected,
        "covered_code_date_pairs": max(0, expected - len(missing_pairs)),
        "missing_pairs": missing_pairs,
    }


def _missing_pairs(dataset: str, frame: pd.DataFrame, codes: list[str], trade_date: str) -> list[dict[str, str]]:
    _ = dataset
    present = set(frame["stock_code"].astype(str)) if "stock_code" in frame else set()
    return [{"stock_code": code, "trade_date": trade_date} for code in codes if code not in present]


def _append_provider_error(status: dict[str, Any], error: str | None, provider_call_enabled: bool) -> None:
    if not provider_call_enabled or not error:
        return
    status["blocked_reasons"].append(error.split(":", 1)[0])
    status["blocked_reasons"] = _unique(status["blocked_reasons"])
    status["validation"]["passed"] = False
    status["validation"].setdefault("errors", []).append(error)
    status["ready_for_apply"] = False


def _validate_goal13_adj_manifest(
    manifest: dict[str, Any], batch_id: str, codes: list[str], trade_dates: list[str]
) -> list[str]:
    errors = []
    if manifest.get("batch_id") != batch_id:
        errors.append("Goal 13 manifest batch_id mismatch")
    if not set(codes).issubset(set(manifest.get("codes", []))):
        errors.append("Goal 13 manifest code coverage mismatch")
    if not set(trade_dates).issubset(set(manifest.get("trade_dates", []))):
        errors.append("Goal 13 manifest date coverage mismatch")
    if "adj_factor" not in set(manifest.get("interfaces_succeeded", [])):
        errors.append("Goal 13 manifest does not prove adj_factor success")
    return errors


def _canonical_reason(dataset: str, errors: list[str]) -> str:
    text = " ".join(errors).lower()
    if dataset == "financial" and "report_period" in text and "future" in text:
        return "FINANCIAL_FUTURE_REPORT_PERIOD"
    if "duplicate" in text:
        return f"{dataset.upper()}_DUPLICATE_KEYS"
    if "coverage" in text or "empty" in text or "missing" in text:
        return f"{dataset.upper()}_MISSING_OR_INCOMPLETE"
    if dataset == "financial" and "announce_date" in text:
        return "FINANCIAL_AS_OF_INVALID"
    return f"{dataset.upper()}_VALIDATION_FAILED"


def _adj_reasons(errors: list[str]) -> list[str]:
    text = " ".join(errors).lower()
    reasons = []
    if "positive" in text:
        reasons.append("ADJ_FACTOR_NON_POSITIVE")
    if "finite" in text or "numeric" in text:
        reasons.append("ADJ_FACTOR_NUMERIC_INVALID")
    if "duplicate" in text:
        reasons.append("ADJ_FACTOR_DUPLICATE_CODE_DATE")
    if "coverage" in text or "missing" in text or "empty" in text:
        reasons.append("ADJ_FACTOR_COVERAGE_INCOMPLETE")
    return reasons or ["ADJ_FACTOR_VALIDATION_FAILED"]


def _benchmark_reasons(errors: list[str], missing_indexes: dict[str, list[str]]) -> list[str]:
    text = " ".join(errors).lower()
    reasons = []
    if any(missing_indexes.values()) or "coverage" in text or "missing benchmark" in text:
        reasons.append("BENCHMARK_INDEX_COVERAGE_INCOMPLETE")
    if "duplicate" in text:
        reasons.append("BENCHMARK_DUPLICATE_INDEX_DATE")
    if "source missing" in text:
        reasons.append("BENCHMARK_PRICE_SOURCE_MISSING")
    return reasons or ["BENCHMARK_PRICE_VALIDATION_FAILED"]


def _stock_reason(errors: list[str]) -> str:
    text = " ".join(errors).lower()
    if "proof" in text or "historical" in text or "source semantics" in text:
        return "STOCK_BASIC_HISTORICAL_PROOF_MISSING"
    if "coverage" in text or "missing" in text:
        return "STOCK_BASIC_HISTORICAL_COVERAGE_INCOMPLETE"
    if "list_date" in text or "delisted" in text:
        return "STOCK_BASIC_HISTORY_SEMANTICS_INVALID"
    return "STOCK_BASIC_HISTORY_VALIDATION_FAILED"


def _st_reason(errors: list[str]) -> str:
    text = " ".join(errors).lower()
    if "current" in text:
        return "ST_STATUS_NOT_HISTORICAL"
    if "coverage" in text or "missing" in text or "empty" in text:
        return "ST_HISTORY_HISTORICAL_COVERAGE_INCOMPLETE"
    if "interval" in text or "source semantics" in text or "proof" in text:
        return "ST_HISTORY_HISTORICAL_PROOF_INVALID"
    return "ST_HISTORY_VALIDATION_FAILED"


def _collect_blocked_reasons(inputs: dict[str, dict[str, Any]]) -> list[str]:
    reasons = []
    for dataset in REQUIRED_INPUTS:
        reasons.extend(inputs[dataset]["blocked_reasons"])
    return _unique(reasons)


def _normalize_codes(codes: list[str]) -> list[str]:
    normalized = []
    for code in codes:
        value = validate_stock_code(str(code).strip().upper())
        if value not in normalized:
            normalized.append(value)
    if not normalized:
        raise ValueError("codes must not be empty")
    return normalized


def _validate_scope_guards(
    *,
    codes: list[str],
    trade_dates: list[str],
    max_codes: int,
    max_trade_days: int,
    max_rows: int,
    sleep_seconds: float,
) -> None:
    if max_codes <= 0 or len(codes) > max_codes:
        raise ValueError("requested codes exceed max_codes")
    if max_trade_days <= 0 or len(trade_dates) > max_trade_days:
        raise ValueError("requested dates exceed max_trade_days")
    if max_rows <= 0:
        raise ValueError("max_rows must be positive")
    if sleep_seconds < 0:
        raise ValueError("sleep_seconds must be non-negative")


def _trade_dates(start_date: str, end_date: str) -> list[str]:
    current = datetime.fromisoformat(start_date).date()
    end = datetime.fromisoformat(end_date).date()
    values = []
    while current <= end:
        values.append(current.isoformat())
        current += timedelta(days=1)
    return values


def _validate_batch_id(batch_id: str) -> str:
    value = str(batch_id or "").strip()
    if not _BATCH_ID_PATTERN.fullmatch(value):
        raise ValueError("batch_id must use only letters, digits, dot, underscore, or dash")
    return value


def _normalize_date_text(value: Any) -> str:
    text = str(value).strip()
    if re.fullmatch(r"\d{8}", text):
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not pd.isna(value):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _zero_summary() -> dict[str, int]:
    return {"inserted_rows": 0, "updated_rows": 0, "unchanged_rows": 0}


def _add_summary(total: dict[str, int], item: dict[str, Any]) -> None:
    for key in total:
        total[key] += int(item.get(key, 0))


def _unique(values: list[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


def _unique_dicts(values: list[dict[str, str]]) -> list[dict[str, str]]:
    seen = set()
    result = []
    for value in values:
        key = tuple(sorted(value.items()))
        if key not in seen:
            seen.add(key)
            result.append(value)
    return result


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

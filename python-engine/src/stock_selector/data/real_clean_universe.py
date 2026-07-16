from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterable
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
import re
from typing import Any

import pandas as pd

from stock_selector.cleaning.adjust_price import build_adjusted_price
from stock_selector.cleaning.snapshot_builder import build_clean_daily_snapshot, filter_stock_basic_as_of
from stock_selector.data.data_validator import (
    DataValidationError,
    REQUIRED_BENCHMARK_INDEXES,
    validate_dataset_frame,
)
from stock_selector.data.historical_backfill import dataframe_checksum
from stock_selector.providers.schema_contract import get_schema_contract
from stock_selector.universe.universe_builder import build_universe_tables
from stock_selector.utils.date_validator import validate_date_range, validate_trade_date


REQUIRED_INPUTS = (
    "stock_basic",
    "daily_price",
    "adj_factor",
    "daily_basic",
    "financial",
    "st_history",
    "benchmark_price",
)

OUTPUT_DATASETS = (
    "adjusted_price",
    "clean_daily_snapshot",
    "risk_filter",
    "eligible_universe",
    "factor_input_table",
)

INPUT_KEY_COLUMNS = {
    "stock_basic": ["stock_code", "trade_date"],
    "daily_price": ["stock_code", "trade_date"],
    "adj_factor": ["stock_code", "trade_date"],
    "daily_basic": ["stock_code", "trade_date"],
    "financial": ["stock_code", "report_period", "announce_date"],
    "st_history": ["stock_code", "st_type", "start_date", "source"],
    "benchmark_price": ["index_code", "trade_date"],
}

OUTPUT_KEY_COLUMNS = {
    "adjusted_price": ["stock_code", "trade_date"],
    "clean_daily_snapshot": ["stock_code", "trade_date"],
    "risk_filter": ["stock_code", "trade_date"],
    "eligible_universe": ["stock_code", "trade_date"],
    "factor_input_table": ["stock_code", "trade_date"],
}

_DAILY_STOCK_INPUTS = ("daily_price", "adj_factor", "daily_basic")
_RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_DOWNSTREAM_FIREWALLS = {
    "factor_daily": False,
    "selection_result": False,
    "backtest": False,
}


@dataclass(frozen=True)
class InputVersion:
    object_key: str
    row_count: int
    checksum: str


@dataclass(frozen=True)
class InputArtifact:
    frame: pd.DataFrame
    versions: tuple[InputVersion, ...]


InputReadFn = Callable[[str, str], InputArtifact]
ReadJsonFn = Callable[[str], dict[str, Any]]
WriteJsonFn = Callable[[str, dict[str, Any]], str]
ProcessedReadFn = Callable[[str, str], pd.DataFrame]
ProcessedWriteFn = Callable[[str, str, pd.DataFrame], str]


def build_real_clean_universe_output_keys(run_id: str, trade_dates: Iterable[str]) -> dict[str, Any]:
    run_id = _validate_run_id(run_id)
    normalized_dates = _normalize_trade_dates(trade_dates)
    root = f"candidate/real_clean_universe/run_id={run_id}"
    return {
        "range_manifest": f"{root}/manifest.json",
        "daily_reports": {
            trade_date: f"{root}/trade_date={trade_date}/dq_report.json"
            for trade_date in normalized_dates
        },
        "processed": {
            trade_date: {
                dataset: f"processed/{dataset}/trade_date={trade_date}/part.parquet"
                for dataset in OUTPUT_DATASETS
            }
            for trade_date in normalized_dates
        },
    }


def run_real_clean_universe_range(
    *,
    run_id: str,
    start_date: str,
    end_date: str,
    trade_dates: Iterable[str],
    input_read_fn: InputReadFn,
    artifact_read_json_fn: ReadJsonFn,
    artifact_write_json_fn: WriteJsonFn,
    processed_read_fn: ProcessedReadFn | None = None,
    processed_write_fn: ProcessedWriteFn | None = None,
    apply_processed_write: bool = False,
    resume: bool = True,
    force: bool = False,
    trade_date_source: str = "CALLER_PROVIDED",
    generated_at_fn: Callable[[], str] | None = None,
) -> dict[str, Any]:
    run_id = _validate_run_id(run_id)
    start_date, end_date = validate_date_range(start_date, end_date)
    normalized_dates = _normalize_trade_dates(trade_dates)
    if not normalized_dates:
        raise ValueError("trade_dates must not be empty")
    if normalized_dates[0] < start_date or normalized_dates[-1] > end_date:
        raise ValueError("trade_dates must be within start_date and end_date")
    if apply_processed_write and (processed_read_fn is None or processed_write_fn is None):
        raise ValueError("processed read and write functions are required with --apply")
    if trade_date_source not in {
        "CALLER_PROVIDED",
        "EXPLICIT_CLI",
        "STANDARD_MARKET_PARTITION_UNION",
    }:
        raise ValueError("unsupported trade_date_source")

    generated_at_fn = generated_at_fn or _utc_now_iso
    output_keys = build_real_clean_universe_output_keys(run_id, normalized_dates)
    plan = {
        "schema_version": "goal22.real_clean_universe_plan.v1",
        "run_id": run_id,
        "start_date": start_date,
        "end_date": end_date,
        "trade_dates": normalized_dates,
        "trade_date_source": trade_date_source,
        "required_inputs": list(REQUIRED_INPUTS),
        "output_datasets": list(OUTPUT_DATASETS),
    }
    plan_fingerprint = _stable_hash(plan)
    existing_manifest = _read_json_optional(artifact_read_json_fn, output_keys["range_manifest"])
    if existing_manifest is not None and existing_manifest.get("plan_fingerprint") != plan_fingerprint:
        raise ValueError("run_id scope does not match existing manifest")

    mode = "APPLY" if apply_processed_write else "DRY_RUN"
    artifact_write_json_fn(
        output_keys["range_manifest"],
        {
            "schema_version": "goal22.real_clean_universe_manifest.v1",
            "goal": "22",
            "run_id": run_id,
            "generated_at": generated_at_fn(),
            "status": "RUNNING",
            "mode": mode,
            "apply_requested": bool(apply_processed_write),
            "resume": bool(resume),
            "force": bool(force),
            "plan": plan,
            "plan_fingerprint": plan_fingerprint,
            "date_statuses": {},
            "daily_report_keys": output_keys["daily_reports"],
            "downstream_firewalls": deepcopy(_DOWNSTREAM_FIREWALLS),
        },
    )

    daily_reports: dict[str, dict[str, Any]] = {}
    for trade_date in normalized_dates:
        daily_report_key = output_keys["daily_reports"][trade_date]
        previous_report = _read_json_optional(artifact_read_json_fn, daily_report_key) if resume else None
        report = _run_one_trade_date(
            run_id=run_id,
            trade_date=trade_date,
            plan_fingerprint=plan_fingerprint,
            mode=mode,
            apply_processed_write=apply_processed_write,
            force=force,
            previous_report=previous_report,
            planned_output_keys=output_keys["processed"][trade_date],
            input_read_fn=input_read_fn,
            processed_read_fn=processed_read_fn,
            processed_write_fn=processed_write_fn,
            generated_at_fn=generated_at_fn,
        )
        artifact_write_json_fn(daily_report_key, report)
        daily_reports[trade_date] = report

    date_statuses = {trade_date: report["status"] for trade_date, report in daily_reports.items()}
    status = _range_status(date_statuses, apply_processed_write=apply_processed_write)
    status_counts = dict(sorted(Counter(date_statuses.values()).items()))
    manifest = {
        "schema_version": "goal22.real_clean_universe_manifest.v1",
        "goal": "22",
        "run_id": run_id,
        "generated_at": generated_at_fn(),
        "status": status,
        "mode": mode,
        "apply_requested": bool(apply_processed_write),
        "resume": bool(resume),
        "force": bool(force),
        "plan": plan,
        "plan_fingerprint": plan_fingerprint,
        "date_statuses": date_statuses,
        "status_counts": status_counts,
        "daily_report_keys": output_keys["daily_reports"],
        "processed_output_keys": output_keys["processed"],
        "downstream_firewalls": deepcopy(_DOWNSTREAM_FIREWALLS),
    }
    artifact_write_json_fn(output_keys["range_manifest"], manifest)
    return {
        "goal": "22",
        "run_id": run_id,
        "status": status,
        "mode": mode,
        "apply_requested": bool(apply_processed_write),
        "date_statuses": date_statuses,
        "status_counts": status_counts,
        "range_manifest_key": output_keys["range_manifest"],
        "daily_report_keys": output_keys["daily_reports"],
        "processed_output_keys": output_keys["processed"],
        "downstream_firewalls": deepcopy(_DOWNSTREAM_FIREWALLS),
        "manifest": manifest,
    }


def _run_one_trade_date(
    *,
    run_id: str,
    trade_date: str,
    plan_fingerprint: str,
    mode: str,
    apply_processed_write: bool,
    force: bool,
    previous_report: dict[str, Any] | None,
    planned_output_keys: dict[str, str],
    input_read_fn: InputReadFn,
    processed_read_fn: ProcessedReadFn | None,
    processed_write_fn: ProcessedWriteFn | None,
    generated_at_fn: Callable[[], str],
) -> dict[str, Any]:
    attempt = int(previous_report.get("attempt", 0)) + 1 if isinstance(previous_report, dict) else 1
    report = _empty_daily_report(
        run_id=run_id,
        trade_date=trade_date,
        plan_fingerprint=plan_fingerprint,
        mode=mode,
        attempt=attempt,
        generated_at=generated_at_fn(),
        planned_output_keys=planned_output_keys,
    )

    artifacts: dict[str, InputArtifact] = {}
    frames: dict[str, pd.DataFrame] = {}
    blocked_reasons: list[str] = []
    for dataset in REQUIRED_INPUTS:
        try:
            artifact = input_read_fn(dataset, trade_date)
            _validate_input_artifact(dataset, artifact)
            artifacts[dataset] = artifact
            frames[dataset] = artifact.frame.copy(deep=True)
            report["inputs"][dataset] = _input_record(artifact)
        except FileNotFoundError:
            blocked_reasons.append(f"MISSING_INPUT:{dataset}")
            report["inputs"][dataset] = _missing_input_record()
        except Exception as exc:
            blocked_reasons.append(f"INPUT_READ_FAILED:{dataset}:{type(exc).__name__}")
            report["inputs"][dataset] = _missing_input_record(error=_safe_message(exc))

    if blocked_reasons:
        report["status"] = "BLOCKED"
        report["blocked_reasons"] = blocked_reasons
        return report

    try:
        usable_financial, financial_as_of = _prepare_financial_as_of(frames["financial"], trade_date)
        frames["financial"] = usable_financial
        report["financial_as_of"] = financial_as_of

        for dataset in REQUIRED_INPUTS:
            if dataset == "financial":
                validate_dataset_frame(dataset, usable_financial, trade_date)
                _validate_input_semantics(dataset, usable_financial)
            else:
                validate_dataset_frame(dataset, frames[dataset], trade_date)
                _validate_input_semantics(dataset, frames[dataset])

        active_stock, membership_exclusions = filter_stock_basic_as_of(frames["stock_basic"], trade_date)
        report["membership_exclusions"] = membership_exclusions
        if active_stock.empty:
            blocked_reasons.append("NO_ACTIVE_STOCKS")

        active_codes = sorted(set(active_stock["stock_code"].astype(str)))
        stock_master_codes = set(frames["stock_basic"]["stock_code"].astype(str))
        for missing_code in sorted(set(frames["daily_price"]["stock_code"].astype(str)) - stock_master_codes):
            blocked_reasons.append(f"MISSING_STOCK_MASTER_COVERAGE:{missing_code}")
        for dataset in _DAILY_STOCK_INPUTS:
            available_codes = set(frames[dataset]["stock_code"].astype(str))
            for missing_code in sorted(set(active_codes) - available_codes):
                blocked_reasons.append(f"MISSING_CODE_COVERAGE:{dataset}:{missing_code}")
        financial_codes = set(usable_financial["stock_code"].astype(str))
        for missing_code in sorted(set(active_codes) - financial_codes):
            blocked_reasons.append(f"MISSING_CODE_COVERAGE:financial:{missing_code}")

        benchmark_counts = frames["benchmark_price"]["index_code"].astype(str).value_counts()
        if set(benchmark_counts.index) != REQUIRED_BENCHMARK_INDEXES or (benchmark_counts != 1).any():
            blocked_reasons.append("INVALID_BENCHMARK_COVERAGE")
    except Exception as exc:
        blocked_reasons.append(f"INPUT_VALIDATION_FAILED:{_validation_dataset(exc)}:{_safe_message(exc)}")

    if blocked_reasons:
        report["status"] = "BLOCKED"
        report["blocked_reasons"] = _unique(blocked_reasons)
        return report

    try:
        active_code_set = set(active_codes)
        daily_price = frames["daily_price"].loc[
            frames["daily_price"]["stock_code"].astype(str).isin(active_code_set)
        ].copy()
        adj_factor = frames["adj_factor"].loc[
            frames["adj_factor"]["stock_code"].astype(str).isin(active_code_set)
        ].copy()
        daily_basic = frames["daily_basic"].loc[
            frames["daily_basic"]["stock_code"].astype(str).isin(active_code_set)
        ].copy()

        adjusted_price = build_adjusted_price(daily_price, adj_factor, trade_date)
        clean_daily_snapshot = build_clean_daily_snapshot(
            stock_basic=active_stock,
            daily_price=daily_price,
            adj_factor=adj_factor,
            daily_basic=daily_basic,
            financial=frames["financial"],
            st_history=frames["st_history"],
            benchmark_price=frames["benchmark_price"],
            trade_date=trade_date,
            adjusted_price=adjusted_price,
        )
        universe_tables = build_universe_tables(clean_daily_snapshot, trade_date)
        outputs = {
            "adjusted_price": adjusted_price,
            "clean_daily_snapshot": clean_daily_snapshot,
            **universe_tables,
        }
        for dataset in OUTPUT_DATASETS:
            outputs[dataset] = _normalize_output_frame(dataset, outputs[dataset])
            validate_dataset_frame(dataset, outputs[dataset], trade_date)
        report["risk_exclusion_counts"] = _risk_exclusion_counts(outputs["risk_filter"])
        report["missing_rates"] = {
            "inputs": {dataset: _missing_rates(artifacts[dataset].frame) for dataset in REQUIRED_INPUTS},
            "outputs": {dataset: _missing_rates(outputs[dataset]) for dataset in OUTPUT_DATASETS},
        }
        report["outputs"] = _output_records(outputs, planned_output_keys, apply_processed_write)
    except (DataValidationError, ValueError, KeyError, TypeError) as exc:
        report["status"] = "BLOCKED"
        report["blocked_reasons"] = [f"PIPELINE_DQ_FAILED:{type(exc).__name__}:{_safe_message(exc)}"]
        return report
    except Exception as exc:
        report["status"] = "FAILED"
        report["failure"] = _failure_record(exc)
        return report

    if not apply_processed_write:
        report["status"] = "READY_FOR_APPLY"
        return report

    assert processed_read_fn is not None
    assert processed_write_fn is not None
    if not force and _completed_report_matches(
        previous_report,
        report["inputs"],
        outputs,
        processed_read_fn,
        trade_date,
    ):
        report["status"] = "COMPLETED"
        report["resume_action"] = "REUSED_COMPLETED"
        for dataset in OUTPUT_DATASETS:
            report["outputs"][dataset]["write"] = {
                "requested": True,
                "performed": False,
                "status": "UNCHANGED",
            }
            report["outputs"][dataset]["read_back"] = {
                "passed": True,
                "row_count": len(outputs[dataset]),
                "checksum": report["outputs"][dataset]["checksum"],
            }
        return report

    report["resume_action"] = "RECOMPUTED" if previous_report else "NEW"
    current_dataset = None
    try:
        for dataset in OUTPUT_DATASETS:
            current_dataset = dataset
            expected = outputs[dataset]
            expected_checksum = report["outputs"][dataset]["checksum"]
            existing = _read_processed_optional(processed_read_fn, dataset, trade_date)
            if existing is not None and _valid_output_checksum(dataset, existing, trade_date, expected_checksum):
                report["outputs"][dataset]["write"] = {
                    "requested": True,
                    "performed": False,
                    "status": "UNCHANGED",
                }
            else:
                object_key = processed_write_fn(dataset, trade_date, expected.copy(deep=True))
                if object_key != planned_output_keys[dataset]:
                    raise RuntimeError(f"processed writer returned unexpected key for {dataset}")
                report["outputs"][dataset]["write"] = {
                    "requested": True,
                    "performed": True,
                    "status": "WRITTEN",
                }

            read_back = processed_read_fn(dataset, trade_date)
            validate_dataset_frame(dataset, read_back, trade_date)
            read_back_checksum = dataframe_checksum(
                read_back,
                key_columns=OUTPUT_KEY_COLUMNS[dataset],
            )
            if read_back_checksum != expected_checksum:
                raise RuntimeError(f"processed read-back checksum mismatch for {dataset}")
            report["outputs"][dataset]["read_back"] = {
                "passed": True,
                "row_count": len(read_back),
                "checksum": read_back_checksum,
            }
    except Exception as exc:
        report["status"] = "FAILED"
        report["blocked_reasons"] = [f"OUTPUT_APPLY_FAILED:{current_dataset}:{type(exc).__name__}"]
        report["failure"] = _failure_record(exc)
        return report

    report["status"] = "COMPLETED"
    return report


def _empty_daily_report(
    *,
    run_id: str,
    trade_date: str,
    plan_fingerprint: str,
    mode: str,
    attempt: int,
    generated_at: str,
    planned_output_keys: dict[str, str],
) -> dict[str, Any]:
    return {
        "schema_version": "goal22.real_clean_universe_daily_dq.v1",
        "goal": "22",
        "run_id": run_id,
        "trade_date": trade_date,
        "generated_at": generated_at,
        "plan_fingerprint": plan_fingerprint,
        "attempt": attempt,
        "mode": mode,
        "status": "RUNNING",
        "resume_action": "NOT_APPLICABLE",
        "inputs": {},
        "financial_as_of": {
            "source_rows": 0,
            "usable_rows": 0,
            "future_rows_excluded": 0,
            "latest_known_rows": 0,
        },
        "membership_exclusions": {},
        "risk_exclusion_counts": {},
        "missing_rates": {"inputs": {}, "outputs": {}},
        "outputs": {
            dataset: {
                "object_key": planned_output_keys[dataset],
                "row_count": 0,
                "checksum": None,
                "write": {"requested": mode == "APPLY", "performed": False, "status": "NOT_RUN"},
                "read_back": {"passed": False, "status": "NOT_RUN"},
            }
            for dataset in OUTPUT_DATASETS
        },
        "blocked_reasons": [],
        "failure": None,
        "downstream_firewalls": deepcopy(_DOWNSTREAM_FIREWALLS),
    }


def _prepare_financial_as_of(frame: pd.DataFrame, trade_date: str) -> tuple[pd.DataFrame, dict[str, int]]:
    required = get_schema_contract("financial").columns
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise DataValidationError(f"missing financial columns: {', '.join(missing)}")
    normalized = frame[required].copy()
    normalized["announce_date"] = normalized["announce_date"].map(lambda value: validate_trade_date(str(value)))
    normalized["report_period"] = normalized["report_period"].map(lambda value: validate_trade_date(str(value)))
    usable_mask = (normalized["announce_date"] <= trade_date) & (normalized["report_period"] <= trade_date)
    usable = normalized.loc[usable_mask].copy()
    latest_known = (
        usable.sort_values(["stock_code", "announce_date", "report_period"])
        .groupby("stock_code", as_index=False)
        .tail(1)
    )
    return usable, {
        "source_rows": len(normalized),
        "usable_rows": len(usable),
        "future_rows_excluded": int((~usable_mask).sum()),
        "latest_known_rows": len(latest_known),
    }


def _validate_input_semantics(dataset: str, frame: pd.DataFrame) -> None:
    contract = get_schema_contract(dataset)
    non_nullable = [column for column in contract.columns if column not in contract.nullable_columns]
    if frame[non_nullable].isna().any().any():
        raise DataValidationError(f"{dataset} contains null values in non-nullable columns")
    keys = INPUT_KEY_COLUMNS[dataset]
    if frame.duplicated(keys).any():
        raise DataValidationError(f"{dataset} contains duplicate logical keys")
    if dataset == "daily_price":
        if not frame["is_paused"].map(lambda value: type(value).__name__ in {"bool", "bool_"}).all():
            raise DataValidationError("daily_price is_paused must contain explicit booleans")
    elif dataset == "adj_factor":
        if not frame["adj_factor"].map(lambda value: math.isfinite(float(value)) and float(value) > 0).all():
            raise DataValidationError("adj_factor must contain finite positive values")
    elif dataset == "stock_basic":
        for row in frame[["list_date", "delist_date"]].to_dict(orient="records"):
            if not pd.isna(row["delist_date"]) and str(row["delist_date"]) <= str(row["list_date"]):
                raise DataValidationError("stock_basic delist_date must be later than list_date")
    elif dataset == "st_history":
        for row in frame[["start_date", "end_date"]].to_dict(orient="records"):
            if not pd.isna(row["end_date"]) and str(row["end_date"]) <= str(row["start_date"]):
                raise DataValidationError("st_history intervals must satisfy start_date < end_date")


def _output_records(
    outputs: dict[str, pd.DataFrame],
    planned_output_keys: dict[str, str],
    apply_requested: bool,
) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    for dataset in OUTPUT_DATASETS:
        frame = outputs[dataset]
        records[dataset] = {
            "object_key": planned_output_keys[dataset],
            "row_count": len(frame),
            "checksum": dataframe_checksum(frame, key_columns=OUTPUT_KEY_COLUMNS[dataset]),
            "write": {
                "requested": bool(apply_requested),
                "performed": False,
                "status": "NOT_RUN" if apply_requested else "NOT_REQUESTED",
            },
            "read_back": {"passed": False, "status": "NOT_RUN"},
        }
    return records


def _normalize_output_frame(dataset: str, frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy(deep=True)
    contract = get_schema_contract(dataset)
    for column in contract.bool_columns:
        result[column] = result[column].map(bool).astype(bool)
    return result[contract.columns]


def _completed_report_matches(
    previous_report: dict[str, Any] | None,
    input_records: dict[str, dict[str, Any]],
    outputs: dict[str, pd.DataFrame],
    processed_read_fn: ProcessedReadFn,
    trade_date: str,
) -> bool:
    if not isinstance(previous_report, dict) or previous_report.get("status") != "COMPLETED":
        return False
    if previous_report.get("inputs") != input_records:
        return False
    for dataset in OUTPUT_DATASETS:
        expected_checksum = dataframe_checksum(outputs[dataset], key_columns=OUTPUT_KEY_COLUMNS[dataset])
        previous_output = previous_report.get("outputs", {}).get(dataset, {})
        if previous_output.get("checksum") != expected_checksum:
            return False
        current = _read_processed_optional(processed_read_fn, dataset, trade_date)
        if current is None or not _valid_output_checksum(dataset, current, trade_date, expected_checksum):
            return False
    return True


def _valid_output_checksum(dataset: str, frame: pd.DataFrame, trade_date: str, expected_checksum: str) -> bool:
    try:
        validate_dataset_frame(dataset, frame, trade_date)
        return dataframe_checksum(frame, key_columns=OUTPUT_KEY_COLUMNS[dataset]) == expected_checksum
    except (DataValidationError, ValueError, TypeError, KeyError):
        return False


def _validate_input_artifact(dataset: str, artifact: InputArtifact) -> None:
    if not isinstance(artifact, InputArtifact) or not isinstance(artifact.frame, pd.DataFrame):
        raise TypeError("input reader must return InputArtifact")
    if not artifact.versions:
        raise ValueError("input artifact must contain at least one source version")
    seen_keys: set[str] = set()
    for version in artifact.versions:
        if not isinstance(version, InputVersion):
            raise TypeError("input versions must use InputVersion")
        if not version.object_key or version.object_key in seen_keys:
            raise ValueError("input version object keys must be non-empty and unique")
        if version.row_count < 0:
            raise ValueError("input version row_count must be non-negative")
        if re.fullmatch(r"[0-9a-f]{64}", version.checksum) is None:
            raise ValueError("input version checksum must be sha256 hex")
        seen_keys.add(version.object_key)
    if dataset not in REQUIRED_INPUTS:
        raise ValueError(f"unsupported Goal 22 input: {dataset}")


def _input_record(artifact: InputArtifact) -> dict[str, Any]:
    return {
        "source_keys": [version.object_key for version in artifact.versions],
        "versions": [
            {
                "object_key": version.object_key,
                "row_count": version.row_count,
                "checksum": version.checksum,
            }
            for version in artifact.versions
        ],
        "row_count": len(artifact.frame),
        "checksum": dataframe_checksum(artifact.frame),
        "missing_rates": _missing_rates(artifact.frame),
        "read_status": "READ",
    }


def _missing_input_record(error: str | None = None) -> dict[str, Any]:
    return {
        "source_keys": [],
        "versions": [],
        "row_count": 0,
        "checksum": None,
        "missing_rates": {},
        "read_status": "MISSING" if error is None else "FAILED",
        "error": error,
    }


def _risk_exclusion_counts(risk_filter: pd.DataFrame) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for value in risk_filter["exclude_reasons"].fillna("").astype(str):
        for reason in value.split(";"):
            if reason:
                counts[reason] += 1
    return dict(sorted(counts.items()))


def _missing_rates(frame: pd.DataFrame) -> dict[str, float]:
    if frame.empty:
        return {str(column): 0.0 for column in frame.columns}
    return {
        str(column): round(float(frame[column].isna().mean()), 8)
        for column in frame.columns
    }


def _range_status(date_statuses: dict[str, str], *, apply_processed_write: bool) -> str:
    values = list(date_statuses.values())
    if apply_processed_write:
        if values and all(value == "COMPLETED" for value in values):
            return "COMPLETED"
        if any(value == "COMPLETED" for value in values):
            return "PARTIAL"
        return "BLOCKED" if any(value == "BLOCKED" for value in values) else "FAILED"
    if values and all(value == "READY_FOR_APPLY" for value in values):
        return "READY_FOR_APPLY"
    if any(value == "READY_FOR_APPLY" for value in values):
        return "PARTIAL"
    return "BLOCKED" if any(value == "BLOCKED" for value in values) else "FAILED"


def _read_processed_optional(read_fn: ProcessedReadFn, dataset: str, trade_date: str) -> pd.DataFrame | None:
    try:
        return read_fn(dataset, trade_date)
    except FileNotFoundError:
        return None


def _read_json_optional(read_fn: ReadJsonFn, object_key: str) -> dict[str, Any] | None:
    try:
        return read_fn(object_key)
    except FileNotFoundError:
        return None


def _normalize_trade_dates(values: Iterable[str]) -> list[str]:
    return sorted({validate_trade_date(str(value)) for value in values})


def _validate_run_id(run_id: str) -> str:
    if not isinstance(run_id, str) or _RUN_ID_PATTERN.fullmatch(run_id) is None:
        raise ValueError("run_id must use 1-128 safe alphanumeric, dot, underscore or hyphen characters")
    return run_id


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _failure_record(error: BaseException) -> dict[str, str]:
    return {"type": type(error).__name__, "message": _safe_message(error)}


def _safe_message(error: BaseException) -> str:
    text = str(error).replace("\r", " ").replace("\n", " ")[:500]
    return re.sub(
        r"(?i)(token|password|secret|authorization|api[_-]?key)\s*[=:]\s*[^\s,;]+",
        r"\1=<redacted>",
        text,
    )


def _validation_dataset(error: BaseException) -> str:
    text = str(error)
    for dataset in REQUIRED_INPUTS:
        if dataset in text:
            return dataset
    return "required_input"


def _unique(values: Iterable[str]) -> list[str]:
    return list(dict.fromkeys(values))


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

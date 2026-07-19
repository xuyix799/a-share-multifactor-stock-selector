from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Iterable
from copy import deepcopy
from dataclasses import dataclass
from datetime import date as calendar_date
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
import re
from typing import Any

import pandas as pd
from pandas.api.types import is_float_dtype, is_object_dtype, is_string_dtype

from stock_selector.data.data_validator import (
    DataValidationError,
    REQUIRED_BENCHMARK_INDEXES,
    validate_dataset_frame,
)
from stock_selector.data.historical_backfill import dataframe_checksum
from stock_selector.data.real_clean_input_gate import (
    validate_goal22_trusted_input_lineage,
)
from stock_selector.data.real_clean_universe import (
    OUTPUT_DATASETS as GOAL22_OUTPUT_DATASETS,
    OUTPUT_KEY_COLUMNS as GOAL22_OUTPUT_KEY_COLUMNS,
    REQUIRED_INPUTS as GOAL22_REQUIRED_INPUTS,
    build_goal22_processed_commit_key,
    build_goal22_processed_generation_key,
    build_real_clean_universe_output_keys,
)
from stock_selector.factors.factor_builder import build_factor_daily
from stock_selector.factors.factor_validator import (
    FACTOR_DAILY_COLUMNS,
    FACTOR_NUMERIC_COLUMNS,
    FACTOR_SCORE_COLUMNS,
    validate_factor_daily,
)
from stock_selector.factors.valuation_factors import (
    MIN_THREE_YEAR_OBSERVATIONS,
    THREE_YEAR_COVERAGE_TOLERANCE_DAYS,
)
from stock_selector.utils.date_validator import (
    validate_date_range,
    validate_trade_date,
)


GOAL22_CONSUMED_DATASETS = (
    "adjusted_price",
    "clean_daily_snapshot",
    "factor_input_table",
)

FACTOR_BASE_COLUMNS = (
    "stock_code",
    "trade_date",
    "industry",
    "market_type",
)

FACTOR_VALUE_COLUMNS = tuple(
    column
    for column in FACTOR_DAILY_COLUMNS
    if column not in FACTOR_BASE_COLUMNS and column not in FACTOR_SCORE_COLUMNS
)

GOAL23_DOWNSTREAM_FIREWALLS = {
    "selection_result": False,
    "backtest": False,
    "llm": False,
    "provider_call": False,
}

_GOAL22_DOWNSTREAM_FIREWALLS = {
    "factor_daily": False,
    "selection_result": False,
    "backtest": False,
}

_RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_GOAL22_MANIFEST_KEY_PATTERN = re.compile(
    r"candidate/real_clean_universe/run_id=([A-Za-z0-9][A-Za-z0-9._-]{0,127})/"
    r"manifest\.json"
)


class Goal23InputError(ValueError):
    """A trusted-input contract failure that blocks the affected trade date."""


ReadJsonFn = Callable[[str], dict[str, Any]]
WriteJsonFn = Callable[[str, dict[str, Any]], str]
ReadParquetFn = Callable[[str], pd.DataFrame]
ReadGoal22CommitFn = Callable[[str], dict[str, Any]]
ReadFactorObjectFn = Callable[[str], pd.DataFrame]
WriteFactorObjectFn = Callable[[str, str, pd.DataFrame], str]
ReadFactorCommitFn = Callable[[str], dict[str, Any]]
WriteFactorCommitFn = Callable[[str, dict[str, Any]], str]


@dataclass(frozen=True)
class Goal22ManifestRecord:
    object_key: str
    checksum: str
    payload: dict[str, Any]
    trade_dates: tuple[str, ...]
    validation_errors: tuple[str, ...]

    @property
    def valid(self) -> bool:
        return not self.validation_errors


@dataclass(frozen=True)
class Goal22ManifestCatalog:
    records: tuple[Goal22ManifestRecord, ...]

    def plan_summary(self) -> list[dict[str, Any]]:
        return [
            {
                "object_key": record.object_key,
                "checksum": record.checksum,
                "run_id": record.payload.get("run_id"),
                "trade_dates": list(record.trade_dates),
                "valid": record.valid,
                "validation_errors": list(record.validation_errors),
            }
            for record in self.records
        ]


@dataclass(frozen=True)
class Goal22PublishedDate:
    trade_date: str
    adjusted_price: pd.DataFrame
    clean_daily_snapshot: pd.DataFrame
    factor_input_table: pd.DataFrame
    benchmark_price: pd.DataFrame
    lineage: dict[str, Any]


def load_goal22_manifest_catalog(
    *,
    manifest_keys: Iterable[str],
    read_json_fn: ReadJsonFn,
) -> Goal22ManifestCatalog:
    keys = [str(value) for value in manifest_keys]
    if not keys:
        raise ValueError("at least one Goal 22 manifest key is required")
    if len(keys) != len(set(keys)):
        raise ValueError("Goal 22 manifest keys must be unique")

    records: list[Goal22ManifestRecord] = []
    for object_key in keys:
        match = _GOAL22_MANIFEST_KEY_PATTERN.fullmatch(object_key)
        if match is None:
            raise ValueError(f"invalid Goal 22 manifest key: {object_key}")
        try:
            payload = read_json_fn(object_key)
        except FileNotFoundError as exc:
            raise FileNotFoundError(
                f"missing Goal 22 manifest: {object_key}"
            ) from exc
        if not isinstance(payload, dict):
            raise ValueError(f"Goal 22 manifest must be a JSON object: {object_key}")
        trade_dates = _tentative_goal22_trade_dates(payload)
        errors: list[str] = []
        try:
            _validate_goal22_manifest_payload(
                object_key=object_key,
                payload=payload,
                key_run_id=match.group(1),
            )
        except (DataValidationError, ValueError, KeyError, TypeError) as exc:
            errors.append(_safe_message(exc))
        records.append(
            Goal22ManifestRecord(
                object_key=object_key,
                checksum=_stable_hash(payload),
                payload=deepcopy(payload),
                trade_dates=tuple(trade_dates),
                validation_errors=tuple(errors),
            )
        )
    return Goal22ManifestCatalog(records=tuple(records))


def build_real_factor_daily_output_keys(
    run_id: str,
    trade_dates: Iterable[str],
) -> dict[str, Any]:
    run_id = _validate_run_id(run_id)
    normalized_dates = _normalize_trade_dates(trade_dates)
    root = f"candidate/real_factor_daily/run_id={run_id}"
    return {
        "range_manifest": f"{root}/manifest.json",
        "daily_reports": {
            trade_date: f"{root}/trade_date={trade_date}/dq_report.json"
            for trade_date in normalized_dates
        },
        "processed": {
            trade_date: (
                f"processed/factor_daily/trade_date={trade_date}/part.parquet"
            )
            for trade_date in normalized_dates
        },
        "processed_commits": {
            trade_date: build_goal23_factor_commit_key(trade_date)
            for trade_date in normalized_dates
        },
    }


def build_goal23_factor_generation_key(
    trade_date: str,
    generation_id: str,
) -> str:
    trade_date = validate_trade_date(trade_date)
    if re.fullmatch(r"[0-9a-f]{64}", str(generation_id)) is None:
        raise ValueError("generation_id must be sha256 hex")
    return (
        f"processed/factor_daily/trade_date={trade_date}/"
        f"generation={generation_id}/part.parquet"
    )


def build_goal23_factor_commit_key(trade_date: str) -> str:
    trade_date = validate_trade_date(trade_date)
    return (
        f"processed/_goal23_factor_commits/trade_date={trade_date}/commit.json"
    )


def run_real_factor_daily_range(
    *,
    run_id: str,
    start_date: str,
    end_date: str,
    trade_dates: Iterable[str],
    goal22_manifest_catalog: Goal22ManifestCatalog,
    factor_config: dict[str, Any],
    control_read_json_fn: ReadJsonFn,
    control_write_json_fn: WriteJsonFn,
    goal22_processed_object_read_fn: ReadParquetFn,
    canonical_object_read_fn: ReadParquetFn,
    goal22_commit_read_fn: ReadGoal22CommitFn,
    factor_object_read_fn: ReadFactorObjectFn | None = None,
    factor_object_write_fn: WriteFactorObjectFn | None = None,
    factor_commit_read_fn: ReadFactorCommitFn | None = None,
    factor_commit_write_fn: WriteFactorCommitFn | None = None,
    apply_processed_write: bool = False,
    resume: bool = True,
    force: bool = False,
    generated_at_fn: Callable[[], str] | None = None,
) -> dict[str, Any]:
    run_id = _validate_run_id(run_id)
    start_date, end_date = validate_date_range(start_date, end_date)
    normalized_dates = _normalize_trade_dates(trade_dates)
    if not normalized_dates:
        raise ValueError("trade_dates must not be empty")
    if normalized_dates[0] < start_date or normalized_dates[-1] > end_date:
        raise ValueError("trade_dates must be within start_date and end_date")
    if not isinstance(goal22_manifest_catalog, Goal22ManifestCatalog):
        raise TypeError("goal22_manifest_catalog must be a Goal22ManifestCatalog")
    if apply_processed_write and any(
        callback is None
        for callback in (
            factor_object_read_fn,
            factor_object_write_fn,
            factor_commit_read_fn,
            factor_commit_write_fn,
        )
    ):
        raise ValueError("factor generation and commit functions are required with --apply")

    normalized_factor_config = _parse_factor_config(factor_config)
    factor_config_fingerprint = _stable_hash(normalized_factor_config)
    generated_at_fn = generated_at_fn or _utc_now_iso
    output_keys = build_real_factor_daily_output_keys(run_id, normalized_dates)
    plan = {
        "schema_version": "goal23.real_factor_daily_plan.v1",
        "run_id": run_id,
        "start_date": start_date,
        "end_date": end_date,
        "trade_dates": normalized_dates,
        "trade_date_source": "EXPLICIT_CLI_GOAL22_MANIFESTS",
        "goal22_manifests": goal22_manifest_catalog.plan_summary(),
        "factor_config": normalized_factor_config,
        "factor_config_fingerprint": factor_config_fingerprint,
        "factor_daily_columns": list(FACTOR_DAILY_COLUMNS),
        "factor_value_columns": list(FACTOR_VALUE_COLUMNS),
        "history_policy": {
            "only_goal22_committed_dates": True,
            "target_date_or_earlier_only": True,
            "legacy_part_fallback": False,
            "insufficient_history": "KEEP_NULL",
            "valuation_3y_minimum_prior_observations": (
                MIN_THREE_YEAR_OBSERVATIONS
            ),
        },
    }
    plan_fingerprint = _stable_hash(plan)
    existing_manifest = _read_json_optional(
        control_read_json_fn,
        output_keys["range_manifest"],
    )
    if (
        existing_manifest is not None
        and existing_manifest.get("plan_fingerprint") != plan_fingerprint
    ):
        raise ValueError("run_id scope does not match existing manifest")

    previous_attempts = (
        existing_manifest.get("date_attempts", {})
        if isinstance(existing_manifest, dict)
        else {}
    )
    date_attempts = {
        trade_date: int(previous_attempts.get(trade_date, 0)) + 1
        for trade_date in normalized_dates
    }
    mode = "APPLY" if apply_processed_write else "DRY_RUN"
    date_statuses = {trade_date: "PENDING" for trade_date in normalized_dates}

    def write_manifest(status: str) -> dict[str, Any]:
        payload = {
            "schema_version": "goal23.real_factor_daily_manifest.v1",
            "goal": "23",
            "run_id": run_id,
            "generated_at": generated_at_fn(),
            "status": status,
            "mode": mode,
            "apply_requested": bool(apply_processed_write),
            "resume": bool(resume),
            "force": bool(force),
            "plan": plan,
            "plan_fingerprint": plan_fingerprint,
            "date_statuses": dict(date_statuses),
            "date_attempts": dict(date_attempts),
            "status_counts": dict(sorted(Counter(date_statuses.values()).items())),
            "daily_report_keys": output_keys["daily_reports"],
            "processed_output_keys": output_keys["processed"],
            "processed_commit_keys": output_keys["processed_commits"],
            "downstream_firewalls": deepcopy(GOAL23_DOWNSTREAM_FIREWALLS),
        }
        control_write_json_fn(output_keys["range_manifest"], payload)
        return payload

    write_manifest("RUNNING")
    started_reports: dict[str, dict[str, Any]] = {}
    for trade_date in normalized_dates:
        report = _empty_daily_report(
            run_id=run_id,
            trade_date=trade_date,
            mode=mode,
            attempt=date_attempts[trade_date],
            plan_fingerprint=plan_fingerprint,
            factor_config=normalized_factor_config,
            factor_config_fingerprint=factor_config_fingerprint,
            goal22_manifests=goal22_manifest_catalog.plan_summary(),
            logical_output_key=output_keys["processed"][trade_date],
            commit_key=output_keys["processed_commits"][trade_date],
            generated_at=generated_at_fn(),
        )
        report["status"] = "PENDING"
        control_write_json_fn(output_keys["daily_reports"][trade_date], report)
        started_reports[trade_date] = report

    publication_cache: dict[tuple[str, str], Goal22PublishedDate] = {}
    for trade_date in normalized_dates:
        daily_report_key = output_keys["daily_reports"][trade_date]
        started_report = deepcopy(started_reports[trade_date])
        started_report["status"] = "RUNNING"
        control_write_json_fn(daily_report_key, started_report)
        date_statuses[trade_date] = "RUNNING"
        write_manifest("RUNNING")
        try:
            report = _run_one_trade_date(
                run_id=run_id,
                trade_date=trade_date,
                mode=mode,
                plan_fingerprint=plan_fingerprint,
                factor_config=normalized_factor_config,
                factor_config_fingerprint=factor_config_fingerprint,
                goal22_manifest_catalog=goal22_manifest_catalog,
                publication_cache=publication_cache,
                started_report=started_report,
                control_read_json_fn=control_read_json_fn,
                goal22_processed_object_read_fn=goal22_processed_object_read_fn,
                canonical_object_read_fn=canonical_object_read_fn,
                goal22_commit_read_fn=goal22_commit_read_fn,
                factor_object_read_fn=factor_object_read_fn,
                factor_object_write_fn=factor_object_write_fn,
                factor_commit_read_fn=factor_commit_read_fn,
                factor_commit_write_fn=factor_commit_write_fn,
                apply_processed_write=apply_processed_write,
                resume=resume,
                force=force,
                logical_output_key=output_keys["processed"][trade_date],
                commit_key=output_keys["processed_commits"][trade_date],
                generated_at_fn=generated_at_fn,
            )
        except Goal23InputError as exc:
            report = deepcopy(started_report)
            report["status"] = "BLOCKED"
            report["blocked_reasons"] = [f"GOAL22_INPUT_BLOCKED:{_safe_message(exc)}"]
            report["failure"] = _failure_record(exc)
            if apply_processed_write:
                report["commit"]["status"] = "UNCOMMITTED"
        except (DataValidationError, ValueError, KeyError, TypeError) as exc:
            report = deepcopy(started_report)
            report["status"] = "BLOCKED"
            report["blocked_reasons"] = [
                f"FACTOR_DQ_BLOCKED:{type(exc).__name__}:{_safe_message(exc)}"
            ]
            report["failure"] = _failure_record(exc)
            if apply_processed_write:
                report["commit"]["status"] = "UNCOMMITTED"
        except Exception as exc:
            report = deepcopy(started_report)
            report["status"] = "FAILED"
            report["blocked_reasons"] = [
                f"DATE_EXECUTION_FAILED:{type(exc).__name__}"
            ]
            report["failure"] = _failure_record(exc)
            if apply_processed_write:
                report["commit"]["status"] = "UNCOMMITTED"
        control_write_json_fn(daily_report_key, report)
        date_statuses[trade_date] = report["status"]
        write_manifest("RUNNING")

    status = _range_status(
        date_statuses,
        apply_processed_write=apply_processed_write,
    )
    manifest = write_manifest(status)
    return {
        "goal": "23",
        "run_id": run_id,
        "status": status,
        "mode": mode,
        "apply_requested": bool(apply_processed_write),
        "date_statuses": dict(date_statuses),
        "status_counts": manifest["status_counts"],
        "range_manifest_key": output_keys["range_manifest"],
        "daily_report_keys": output_keys["daily_reports"],
        "processed_output_keys": output_keys["processed"],
        "processed_commit_keys": output_keys["processed_commits"],
        "downstream_firewalls": deepcopy(GOAL23_DOWNSTREAM_FIREWALLS),
        "manifest": manifest,
    }


def read_goal23_published_factor_daily(
    *,
    trade_date: str,
    factor_commit_read_fn: ReadFactorCommitFn,
    factor_object_read_fn: ReadFactorObjectFn,
) -> pd.DataFrame:
    trade_date = validate_trade_date(trade_date)
    commit = factor_commit_read_fn(trade_date)
    validate_goal23_factor_commit_payload(commit, trade_date)
    output = commit["output"]
    frame = factor_object_read_fn(output["object_key"])
    frame = normalize_factor_daily_read_back(frame)
    validate_factor_daily(frame, trade_date)
    checksum = dataframe_checksum(frame, key_columns=["stock_code", "trade_date"])
    if len(frame) != output["row_count"] or checksum != output["checksum"]:
        raise DataValidationError(
            f"Goal 23 committed factor_daily checksum mismatch for {trade_date}"
        )
    return frame


def validate_goal23_factor_commit_payload(
    payload: dict[str, Any],
    trade_date: str,
) -> None:
    trade_date = validate_trade_date(trade_date)
    run_id = payload.get("run_id") if isinstance(payload, dict) else None
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != "goal23.factor_date_commit.v1"
        or payload.get("goal") != "23"
        or payload.get("status") != "COMMITTED"
        or not isinstance(run_id, str)
        or _RUN_ID_PATTERN.fullmatch(run_id) is None
        or payload.get("trade_date") != trade_date
        or payload.get("downstream_firewalls") != GOAL23_DOWNSTREAM_FIREWALLS
        or re.fullmatch(
            r"[0-9a-f]{64}",
            str(payload.get("generation_id", "")),
        )
        is None
        or re.fullmatch(
            r"[0-9a-f]{64}",
            str(payload.get("plan_fingerprint", "")),
        )
        is None
        or re.fullmatch(
            r"[0-9a-f]{64}",
            str(payload.get("input_fingerprint", "")),
        )
        is None
        or re.fullmatch(
            r"[0-9a-f]{64}",
            str(payload.get("factor_config_fingerprint", "")),
        )
        is None
    ):
        raise DataValidationError("invalid Goal 23 factor commit payload")
    output = payload.get("output")
    generation_id = payload["generation_id"]
    if (
        not isinstance(output, dict)
        or output.get("dataset") != "factor_daily"
        or output.get("object_key")
        != build_goal23_factor_generation_key(trade_date, generation_id)
        or output.get("logical_key")
        != f"processed/factor_daily/trade_date={trade_date}/part.parquet"
        or isinstance(output.get("row_count"), bool)
        or not isinstance(output.get("row_count"), int)
        or output["row_count"] < 0
        or re.fullmatch(r"[0-9a-f]{64}", str(output.get("checksum", "")))
        is None
    ):
        raise DataValidationError("invalid Goal 23 factor generation mapping")
    expected_generation_id = _stable_hash(
        {
            "run_id": run_id,
            "trade_date": trade_date,
            "plan_fingerprint": payload["plan_fingerprint"],
            "input_fingerprint": payload["input_fingerprint"],
            "factor_config_fingerprint": payload[
                "factor_config_fingerprint"
            ],
            "output_checksum": output["checksum"],
            "row_count": output["row_count"],
        }
    )
    if generation_id != expected_generation_id:
        raise DataValidationError("invalid Goal 23 generation fingerprint")


def normalize_factor_daily_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("factor_daily frame must be a DataFrame")
    actual_columns = list(frame.columns)
    if actual_columns != FACTOR_DAILY_COLUMNS:
        raise DataValidationError(
            "factor_daily columns must exactly match the published schema: "
            f"expected {FACTOR_DAILY_COLUMNS}, got {actual_columns}"
        )
    result = frame.loc[:, FACTOR_DAILY_COLUMNS].copy(deep=True)
    for column in FACTOR_BASE_COLUMNS:
        result[column] = result[column].astype("string")
    for column in FACTOR_DAILY_COLUMNS:
        if column in FACTOR_BASE_COLUMNS:
            continue
        try:
            numeric = pd.to_numeric(result[column], errors="raise")
        except (TypeError, ValueError) as exc:
            raise DataValidationError(
                f"{column} must contain only numeric values or null"
            ) from exc
        result[column] = numeric.astype("Float64")
    return result


def normalize_factor_daily_read_back(frame: pd.DataFrame) -> pd.DataFrame:
    validate_factor_daily_raw_schema(frame)
    return normalize_factor_daily_frame(frame)


def validate_factor_daily_raw_schema(frame: pd.DataFrame) -> None:
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("factor_daily frame must be a DataFrame")
    actual_columns = list(frame.columns)
    if actual_columns != FACTOR_DAILY_COLUMNS:
        raise DataValidationError(
            "factor_daily columns must exactly match the published schema: "
            f"expected {FACTOR_DAILY_COLUMNS}, got {actual_columns}"
        )
    for column in FACTOR_BASE_COLUMNS:
        dtype = frame[column].dtype
        non_null_values = frame[column].dropna()
        if (
            isinstance(dtype, pd.CategoricalDtype)
            or not (is_string_dtype(dtype) or is_object_dtype(dtype))
            or not all(isinstance(value, str) for value in non_null_values)
        ):
            raise DataValidationError(
                f"{column} raw pandas dtype and values must be string, "
                f"got {dtype}"
            )
    for column in FACTOR_NUMERIC_COLUMNS:
        dtype = frame[column].dtype
        if not is_float_dtype(dtype) or getattr(dtype, "itemsize", None) != 8:
            raise DataValidationError(
                f"{column} raw pandas dtype must be float64, "
                f"got {dtype}"
            )


def validate_factor_daily_arrow_schema(schema: Any) -> None:
    import pyarrow as pa

    if not isinstance(schema, pa.Schema):
        raise TypeError("factor_daily Arrow schema must be a pyarrow.Schema")
    actual_columns = list(schema.names)
    if actual_columns != FACTOR_DAILY_COLUMNS:
        raise DataValidationError(
            "factor_daily Parquet/Arrow columns must exactly match the "
            f"published schema: expected {FACTOR_DAILY_COLUMNS}, "
            f"got {actual_columns}"
        )
    for column in FACTOR_BASE_COLUMNS:
        arrow_type = schema.field(column).type
        if arrow_type != pa.string():
            raise DataValidationError(
                f"{column} Parquet/Arrow type must be string, "
                f"got {arrow_type}"
            )
    for column in FACTOR_NUMERIC_COLUMNS:
        arrow_type = schema.field(column).type
        if arrow_type != pa.float64():
            raise DataValidationError(
                f"{column} Parquet/Arrow type must be float64, "
                f"got {arrow_type}"
            )


def audit_factor_contract(frame: pd.DataFrame) -> dict[str, Any]:
    row_count = len(frame)
    factors: dict[str, dict[str, Any]] = {}
    effective: list[str] = []
    all_null: list[str] = []
    for column in FACTOR_VALUE_COLUMNS:
        non_null_count = int(frame[column].notna().sum())
        is_all_null = row_count == 0 or non_null_count == 0
        factors[column] = {
            "non_null_count": non_null_count,
            "missing_rate": (
                None
                if row_count == 0
                else round(float(frame[column].isna().mean()), 8)
            ),
            "all_null": is_all_null,
        }
        if is_all_null:
            all_null.append(column)
        else:
            effective.append(column)
    return {
        "factor_value_columns": list(FACTOR_VALUE_COLUMNS),
        "factor_count": len(FACTOR_VALUE_COLUMNS),
        "effective_factors": effective,
        "effective_factor_count": len(effective),
        "all_null_factors": all_null,
        "known_placeholder_factors": ["quality_cashflow_profit_ratio"],
        "v1_minimum_effective_factors": 15,
        "meets_v1_minimum_effective_factors": len(effective) >= 15,
        "factors": factors,
    }


def _run_one_trade_date(
    *,
    run_id: str,
    trade_date: str,
    mode: str,
    plan_fingerprint: str,
    factor_config: dict[str, Any],
    factor_config_fingerprint: str,
    goal22_manifest_catalog: Goal22ManifestCatalog,
    publication_cache: dict[tuple[str, str], Goal22PublishedDate],
    started_report: dict[str, Any],
    control_read_json_fn: ReadJsonFn,
    goal22_processed_object_read_fn: ReadParquetFn,
    canonical_object_read_fn: ReadParquetFn,
    goal22_commit_read_fn: ReadGoal22CommitFn,
    factor_object_read_fn: ReadFactorObjectFn | None,
    factor_object_write_fn: WriteFactorObjectFn | None,
    factor_commit_read_fn: ReadFactorCommitFn | None,
    factor_commit_write_fn: WriteFactorCommitFn | None,
    apply_processed_write: bool,
    resume: bool,
    force: bool,
    logical_output_key: str,
    commit_key: str,
    generated_at_fn: Callable[[], str],
) -> dict[str, Any]:
    report = deepcopy(started_report)
    history_records = _load_goal22_history_for_target(
        target_trade_date=trade_date,
        catalog=goal22_manifest_catalog,
        cache=publication_cache,
        control_read_json_fn=control_read_json_fn,
        goal22_processed_object_read_fn=goal22_processed_object_read_fn,
        canonical_object_read_fn=canonical_object_read_fn,
        goal22_commit_read_fn=goal22_commit_read_fn,
    )
    target = history_records[trade_date]
    report["goal22_input_lineage"] = {
        date: deepcopy(publication.lineage)
        for date, publication in history_records.items()
    }

    adjusted_history = _concat_history(
        [item.adjusted_price for item in history_records.values()],
        key_columns=["stock_code", "trade_date"],
        target_trade_date=trade_date,
    )
    clean_history = _concat_history(
        [item.clean_daily_snapshot for item in history_records.values()],
        key_columns=["stock_code", "trade_date"],
        target_trade_date=trade_date,
    )
    benchmark_history = _concat_history(
        [item.benchmark_price for item in history_records.values()],
        key_columns=["index_code", "trade_date"],
        target_trade_date=trade_date,
    )
    report["history_coverage"] = _history_coverage(
        factor_input_table=target.factor_input_table,
        adjusted_price_history=adjusted_history,
        clean_snapshot_history=clean_history,
        benchmark_price_history=benchmark_history,
        history_dates=list(history_records),
        target_trade_date=trade_date,
    )
    input_fingerprint = _stable_hash(
        {
            "goal22_input_lineage": report["goal22_input_lineage"],
            "history_coverage": report["history_coverage"],
            "factor_config_fingerprint": factor_config_fingerprint,
        }
    )
    report["input_fingerprint"] = input_fingerprint

    factor_daily = build_factor_daily(
        factor_input_table=target.factor_input_table.copy(deep=True),
        adjusted_price_history=adjusted_history,
        clean_snapshot_history=clean_history,
        benchmark_price_history=benchmark_history,
        trade_date=trade_date,
        factor_weights=factor_config["weights"],
        null_score_policy=factor_config["null_score_policy"],
        neutral_score=factor_config["neutral_score"],
        strict_history_windows=True,
    )
    factor_daily = normalize_factor_daily_frame(factor_daily)
    validate_factor_daily(factor_daily, trade_date)
    report["factor_contract_audit"] = audit_factor_contract(factor_daily)
    report["factor_missing_rates"] = {
        column: detail["missing_rate"]
        for column, detail in report["factor_contract_audit"]["factors"].items()
    }
    output_checksum = dataframe_checksum(
        factor_daily,
        key_columns=["stock_code", "trade_date"],
    )
    report["output"] = {
        "dataset": "factor_daily",
        "logical_key": logical_output_key,
        "object_key": None,
        "row_count": len(factor_daily),
        "checksum": output_checksum,
        "write": {
            "requested": bool(apply_processed_write),
            "performed": False,
            "status": "NOT_RUN" if apply_processed_write else "NOT_REQUESTED",
        },
        "read_back": {"passed": False, "status": "NOT_RUN"},
    }

    if not apply_processed_write:
        report["status"] = "READY_FOR_APPLY"
        return report

    assert factor_object_read_fn is not None
    assert factor_object_write_fn is not None
    assert factor_commit_read_fn is not None
    assert factor_commit_write_fn is not None

    current_phase = "RESUME_COMMIT_READ"
    try:
        existing_commit = (
            _read_factor_commit_optional(factor_commit_read_fn, trade_date)
            if resume and not force
            else None
        )
        if existing_commit is not None and _completed_factor_commit_matches(
            existing_commit,
            run_id=run_id,
            trade_date=trade_date,
            plan_fingerprint=plan_fingerprint,
            input_fingerprint=input_fingerprint,
            factor_config_fingerprint=factor_config_fingerprint,
            output_checksum=output_checksum,
            row_count=len(factor_daily),
            factor_object_read_fn=factor_object_read_fn,
        ):
            report["status"] = "COMPLETED"
            report["resume_action"] = "REUSED_COMPLETED"
            report["commit"] = {
                "object_key": commit_key,
                "status": "COMMITTED",
                "generation_id": existing_commit["generation_id"],
                "reused": True,
            }
            report["output"]["object_key"] = existing_commit["output"][
                "object_key"
            ]
            report["output"]["write"] = {
                "requested": True,
                "performed": False,
                "status": "UNCHANGED",
            }
            report["output"]["read_back"] = {
                "passed": True,
                "row_count": len(factor_daily),
                "checksum": output_checksum,
            }
            return report

        report["resume_action"] = (
            "RECOMPUTED" if existing_commit is not None else "NEW"
        )
        generation_id = _factor_generation_id(
            run_id=run_id,
            trade_date=trade_date,
            plan_fingerprint=plan_fingerprint,
            input_fingerprint=input_fingerprint,
            factor_config_fingerprint=factor_config_fingerprint,
            output_checksum=output_checksum,
            row_count=len(factor_daily),
        )
        generation_key = build_goal23_factor_generation_key(
            trade_date,
            generation_id,
        )
        report["commit"] = {
            "object_key": commit_key,
            "status": "PENDING",
            "generation_id": generation_id,
            "reused": False,
        }
        report["output"]["object_key"] = generation_key

        current_phase = "STAGE:factor_daily"
        existing_object = _read_factor_object_optional(
            factor_object_read_fn,
            generation_key,
        )
        if existing_object is not None:
            if not _valid_factor_output_checksum(
                existing_object,
                trade_date,
                output_checksum,
            ):
                raise RuntimeError(
                    "immutable factor generation already exists with "
                    "mismatched schema or checksum"
                )
            report["output"]["write"] = {
                "requested": True,
                "performed": False,
                "status": "UNCHANGED",
            }
        else:
            written_key = factor_object_write_fn(
                trade_date,
                generation_id,
                factor_daily.copy(deep=True),
            )
            if written_key != generation_key:
                raise RuntimeError(
                    "factor generation writer returned unexpected key"
                )
            report["output"]["write"] = {
                "requested": True,
                "performed": True,
                "status": "WRITTEN",
            }

        staged_read_back = normalize_factor_daily_read_back(
            factor_object_read_fn(generation_key)
        )
        validate_factor_daily(staged_read_back, trade_date)
        staged_checksum = dataframe_checksum(
            staged_read_back,
            key_columns=["stock_code", "trade_date"],
        )
        if (
            len(staged_read_back) != len(factor_daily)
            or staged_checksum != output_checksum
        ):
            raise RuntimeError("factor generation read-back mismatch")
        report["output"]["read_back"] = {
            "passed": True,
            "row_count": len(staged_read_back),
            "checksum": staged_checksum,
        }

        commit_payload = {
            "schema_version": "goal23.factor_date_commit.v1",
            "goal": "23",
            "status": "COMMITTED",
            "run_id": run_id,
            "trade_date": trade_date,
            "generation_id": generation_id,
            "committed_at": generated_at_fn(),
            "plan_fingerprint": plan_fingerprint,
            "input_fingerprint": input_fingerprint,
            "factor_config_fingerprint": factor_config_fingerprint,
            "output": {
                "dataset": "factor_daily",
                "logical_key": logical_output_key,
                "object_key": generation_key,
                "row_count": len(factor_daily),
                "checksum": output_checksum,
            },
            "downstream_firewalls": deepcopy(GOAL23_DOWNSTREAM_FIREWALLS),
        }
        validate_goal23_factor_commit_payload(commit_payload, trade_date)
        current_phase = "DATE_COMMIT"
        written_commit_key = factor_commit_write_fn(
            trade_date,
            commit_payload,
        )
        if written_commit_key != commit_key:
            raise RuntimeError("factor commit writer returned unexpected key")
        report["commit"]["status"] = "COMMITTED"

        current_phase = "COMMITTED_READBACK"
        committed_read_back = read_goal23_published_factor_daily(
            trade_date=trade_date,
            factor_commit_read_fn=factor_commit_read_fn,
            factor_object_read_fn=factor_object_read_fn,
        )
        committed_checksum = dataframe_checksum(
            committed_read_back,
            key_columns=["stock_code", "trade_date"],
        )
        if (
            len(committed_read_back) != len(factor_daily)
            or committed_checksum != output_checksum
        ):
            raise RuntimeError("committed factor_daily read-back mismatch")
        report["output"]["read_back"] = {
            "passed": True,
            "row_count": len(committed_read_back),
            "checksum": committed_checksum,
        }
    except Exception as exc:
        report["status"] = "FAILED"
        report["blocked_reasons"] = [
            f"OUTPUT_APPLY_FAILED:{current_phase}:{type(exc).__name__}"
        ]
        report["failure"] = _failure_record(exc)
        if report["commit"]["status"] != "COMMITTED":
            report["commit"]["status"] = "UNCOMMITTED"
        return report

    report["status"] = "COMPLETED"
    return report


def _load_goal22_history_for_target(
    *,
    target_trade_date: str,
    catalog: Goal22ManifestCatalog,
    cache: dict[tuple[str, str], Goal22PublishedDate],
    control_read_json_fn: ReadJsonFn,
    goal22_processed_object_read_fn: ReadParquetFn,
    canonical_object_read_fn: ReadParquetFn,
    goal22_commit_read_fn: ReadGoal22CommitFn,
) -> dict[str, Goal22PublishedDate]:
    target_trade_date = validate_trade_date(target_trade_date)
    for record in catalog.records:
        affected_dates = [
            date for date in record.trade_dates if date <= target_trade_date
        ]
        if record.validation_errors and affected_dates:
            raise Goal23InputError(
                f"invalid Goal 22 manifest {record.object_key}: "
                f"{'; '.join(record.validation_errors)}"
            )

    completed_coverage: dict[str, list[Goal22ManifestRecord]] = {}
    for record in catalog.records:
        if not record.valid:
            continue
        statuses = record.payload["date_statuses"]
        for date in record.trade_dates:
            if date <= target_trade_date and statuses[date] == "COMPLETED":
                completed_coverage.setdefault(date, []).append(record)

    if target_trade_date not in completed_coverage:
        raise Goal23InputError(
            f"missing completed Goal 22 manifest coverage for {target_trade_date}"
        )

    result: dict[str, Goal22PublishedDate] = {}
    for history_date in sorted(completed_coverage):
        publications: list[Goal22PublishedDate] = []
        for record in completed_coverage[history_date]:
            cache_key = (record.object_key, history_date)
            if cache_key not in cache:
                try:
                    cache[cache_key] = _read_goal22_published_date(
                        record=record,
                        trade_date=history_date,
                        control_read_json_fn=control_read_json_fn,
                        goal22_processed_object_read_fn=(
                            goal22_processed_object_read_fn
                        ),
                        canonical_object_read_fn=canonical_object_read_fn,
                        goal22_commit_read_fn=goal22_commit_read_fn,
                    )
                except Goal23InputError:
                    raise
                except FileNotFoundError as exc:
                    raise Goal23InputError(
                        f"missing Goal 22 published artifact for "
                        f"{history_date}: {_safe_message(exc)}"
                    ) from exc
                except (DataValidationError, ValueError, KeyError, TypeError) as exc:
                    raise Goal23InputError(
                        f"invalid Goal 22 published artifact for "
                        f"{history_date}: {_safe_message(exc)}"
                    ) from exc
            publications.append(cache[cache_key])
        fingerprints = {
            publication.lineage["publication_fingerprint"]
            for publication in publications
        }
        if len(fingerprints) != 1:
            raise Goal23InputError(
                f"ambiguous Goal 22 publication coverage for {history_date}"
            )
        result[history_date] = publications[0]
    return result


def _read_goal22_published_date(
    *,
    record: Goal22ManifestRecord,
    trade_date: str,
    control_read_json_fn: ReadJsonFn,
    goal22_processed_object_read_fn: ReadParquetFn,
    canonical_object_read_fn: ReadParquetFn,
    goal22_commit_read_fn: ReadGoal22CommitFn,
) -> Goal22PublishedDate:
    manifest = record.payload
    daily_report_key = manifest["daily_report_keys"][trade_date]
    daily_report = control_read_json_fn(daily_report_key)
    _validate_goal22_daily_report(
        report=daily_report,
        manifest=manifest,
        trade_date=trade_date,
    )

    commit = goal22_commit_read_fn(trade_date)
    _validate_goal22_commit_for_manifest(
        commit=commit,
        report=daily_report,
        manifest=manifest,
        trade_date=trade_date,
    )

    frames: dict[str, pd.DataFrame] = {}
    consumed_lineage: dict[str, Any] = {}
    for dataset in GOAL22_CONSUMED_DATASETS:
        committed = commit["outputs"][dataset]
        frame = goal22_processed_object_read_fn(committed["object_key"])
        validate_dataset_frame(dataset, frame, trade_date)
        checksum = dataframe_checksum(
            frame,
            key_columns=GOAL22_OUTPUT_KEY_COLUMNS[dataset],
        )
        if (
            len(frame) != committed["row_count"]
            or checksum != committed["checksum"]
        ):
            raise Goal23InputError(
                f"Goal 22 generation checksum mismatch for {dataset} {trade_date}"
            )
        frames[dataset] = frame.copy(deep=True)
        consumed_lineage[dataset] = deepcopy(committed)

    benchmark_version = manifest["plan"]["trusted_input_lineage"][
        "canonical_versions"
    ][trade_date]["benchmark_price"]
    benchmark = canonical_object_read_fn(benchmark_version["object_key"])
    validate_dataset_frame("benchmark_price", benchmark, trade_date)
    benchmark_object_checksum = dataframe_checksum(benchmark)
    if (
        len(benchmark) != benchmark_version["object_row_count"]
        or benchmark_object_checksum != benchmark_version["object_checksum"]
    ):
        raise Goal23InputError(
            f"Goal 22 benchmark canonical version drift for {trade_date}"
        )
    if (
        len(benchmark) != benchmark_version["scope_row_count"]
        or dataframe_checksum(benchmark) != benchmark_version["scope_checksum"]
    ):
        raise Goal23InputError(
            f"Goal 22 benchmark audited scope drift for {trade_date}"
        )
    benchmark_counts = benchmark["index_code"].astype(str).value_counts()
    if (
        set(benchmark_counts.index) != REQUIRED_BENCHMARK_INDEXES
        or (benchmark_counts != 1).any()
    ):
        raise Goal23InputError(
            f"Goal 22 benchmark coverage is not exact for {trade_date}"
        )

    lineage = {
        "goal22_manifest_key": record.object_key,
        "goal22_manifest_checksum": record.checksum,
        "goal22_run_id": manifest["run_id"],
        "goal22_plan_fingerprint": manifest["plan_fingerprint"],
        "goal22_daily_report_key": daily_report_key,
        "goal22_daily_report_checksum": _stable_hash(daily_report),
        "goal22_commit_key": build_goal22_processed_commit_key(trade_date),
        "goal22_commit_checksum": _stable_hash(commit),
        "goal22_generation_id": commit["generation_id"],
        "consumed_outputs": consumed_lineage,
        "benchmark_canonical_version": deepcopy(benchmark_version),
    }
    lineage["publication_fingerprint"] = _stable_hash(lineage)
    return Goal22PublishedDate(
        trade_date=trade_date,
        adjusted_price=frames["adjusted_price"],
        clean_daily_snapshot=frames["clean_daily_snapshot"],
        factor_input_table=frames["factor_input_table"],
        benchmark_price=benchmark.copy(deep=True),
        lineage=lineage,
    )


def _validate_goal22_manifest_payload(
    *,
    object_key: str,
    payload: dict[str, Any],
    key_run_id: str,
) -> None:
    if (
        payload.get("schema_version")
        != "goal22.real_clean_universe_manifest.v2"
        or payload.get("goal") != "22"
        or payload.get("run_id") != key_run_id
        or payload.get("mode") != "APPLY"
        or payload.get("apply_requested") is not True
        or payload.get("downstream_firewalls") != _GOAL22_DOWNSTREAM_FIREWALLS
    ):
        raise Goal23InputError(f"untrusted Goal 22 manifest header: {object_key}")
    plan = payload.get("plan")
    if not isinstance(plan, dict):
        raise Goal23InputError("Goal 22 manifest plan is missing")
    if (
        plan.get("schema_version") != "goal22.real_clean_universe_plan.v2"
        or plan.get("run_id") != key_run_id
        or plan.get("required_inputs") != list(GOAL22_REQUIRED_INPUTS)
        or plan.get("output_datasets") != list(GOAL22_OUTPUT_DATASETS)
    ):
        raise Goal23InputError("Goal 22 manifest plan contract is invalid")
    start_date, end_date = validate_date_range(
        plan.get("start_date"),
        plan.get("end_date"),
    )
    trade_dates = _normalize_trade_dates(plan.get("trade_dates", []))
    if (
        not trade_dates
        or trade_dates != plan.get("trade_dates")
        or trade_dates[0] < start_date
        or trade_dates[-1] > end_date
    ):
        raise Goal23InputError("Goal 22 manifest trade dates are invalid")
    lineage = validate_goal22_trusted_input_lineage(
        plan.get("trusted_input_lineage"),
        expected_trade_dates=trade_dates,
    )
    if lineage != plan.get("trusted_input_lineage"):
        raise Goal23InputError("Goal 22 trusted input lineage is not canonical")
    if plan.get("trusted_input_fingerprint") != _stable_hash(lineage):
        raise Goal23InputError("Goal 22 trusted input fingerprint mismatch")
    if payload.get("plan_fingerprint") != _stable_hash(plan):
        raise Goal23InputError("Goal 22 plan fingerprint mismatch")

    expected_keys = build_real_clean_universe_output_keys(
        key_run_id,
        trade_dates,
    )
    if (
        payload.get("daily_report_keys") != expected_keys["daily_reports"]
        or payload.get("processed_output_keys") != expected_keys["processed"]
        or payload.get("processed_commit_keys")
        != expected_keys["processed_commits"]
    ):
        raise Goal23InputError("Goal 22 manifest output key mapping is invalid")
    statuses = payload.get("date_statuses")
    attempts = payload.get("date_attempts")
    if (
        not isinstance(statuses, dict)
        or list(statuses) != trade_dates
        or not isinstance(attempts, dict)
        or list(attempts) != trade_dates
    ):
        raise Goal23InputError("Goal 22 manifest per-date state is invalid")
    allowed_date_statuses = {"COMPLETED", "BLOCKED", "FAILED"}
    if any(value not in allowed_date_statuses for value in statuses.values()):
        raise Goal23InputError("Goal 22 manifest contains invalid date status")
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 1
        for value in attempts.values()
    ):
        raise Goal23InputError("Goal 22 manifest contains invalid attempt count")
    expected_status = _goal22_range_status(statuses)
    if payload.get("status") != expected_status:
        raise Goal23InputError("Goal 22 range status does not match date states")
    expected_status_counts = dict(
        sorted(Counter(statuses.values()).items())
    )
    if payload.get("status_counts") != expected_status_counts:
        raise Goal23InputError(
            "Goal 22 status counts do not match date states"
        )


def _validate_goal22_daily_report(
    *,
    report: dict[str, Any],
    manifest: dict[str, Any],
    trade_date: str,
) -> None:
    if (
        not isinstance(report, dict)
        or report.get("schema_version")
        != "goal22.real_clean_universe_daily_dq.v2"
        or report.get("goal") != "22"
        or report.get("run_id") != manifest["run_id"]
        or report.get("trade_date") != trade_date
        or report.get("plan_fingerprint") != manifest["plan_fingerprint"]
        or report.get("mode") != "APPLY"
        or report.get("status") != "COMPLETED"
        or report.get("attempt")
        != manifest["date_attempts"][trade_date]
        or report.get("blocked_reasons") != []
        or report.get("failure") is not None
        or report.get("downstream_firewalls") != _GOAL22_DOWNSTREAM_FIREWALLS
    ):
        raise Goal23InputError(
            f"Goal 22 daily DQ is not a completed trusted report for {trade_date}"
        )
    expected_lineage = manifest["plan"]["trusted_input_lineage"]
    report_lineage = report.get("trusted_input_lineage")
    if (
        not isinstance(report_lineage, dict)
        or report_lineage.get("fingerprint") != _stable_hash(expected_lineage)
        or report_lineage.get("codes") != expected_lineage["codes"]
        or report_lineage.get("readiness_report_keys")
        != [
            item["readiness_report_key"]
            for item in expected_lineage["readiness_receipts"]
        ]
        or report_lineage.get("readiness_report_checksums")
        != [
            item["readiness_report_checksum"]
            for item in expected_lineage["readiness_receipts"]
        ]
    ):
        raise Goal23InputError(
            f"Goal 22 daily DQ lineage mismatch for {trade_date}"
        )
    inputs = report.get("inputs")
    if not isinstance(inputs, dict) or set(inputs) != set(GOAL22_REQUIRED_INPUTS):
        raise Goal23InputError(
            f"Goal 22 daily DQ input set mismatch for {trade_date}"
        )
    canonical_versions = expected_lineage["canonical_versions"][trade_date]
    for dataset in GOAL22_REQUIRED_INPUTS:
        item = inputs[dataset]
        expected = canonical_versions[dataset]
        versions = item.get("versions") if isinstance(item, dict) else None
        if (
            not isinstance(versions, list)
            or len(versions) != 1
            or versions[0]
            != {
                "object_key": expected["object_key"],
                "row_count": expected["object_row_count"],
                "checksum": expected["object_checksum"],
            }
            or item.get("source_keys") != [expected["object_key"]]
            or item.get("row_count") != expected["scope_row_count"]
            or item.get("checksum") != expected["scope_checksum"]
            or item.get("read_status") != "READ"
        ):
            raise Goal23InputError(
                f"Goal 22 daily DQ input evidence mismatch for "
                f"{dataset} {trade_date}"
            )
    outputs = report.get("outputs")
    if not isinstance(outputs, dict) or set(outputs) != set(
        GOAL22_OUTPUT_DATASETS
    ):
        raise Goal23InputError(
            f"Goal 22 daily DQ output set mismatch for {trade_date}"
        )
    generation_id = str(report.get("commit", {}).get("generation_id", ""))
    if (
        report.get("commit", {}).get("status") != "COMMITTED"
        or report.get("commit", {}).get("object_key")
        != build_goal22_processed_commit_key(trade_date)
        or re.fullmatch(r"[0-9a-f]{64}", generation_id) is None
    ):
        raise Goal23InputError(
            f"Goal 22 daily DQ commit evidence is invalid for {trade_date}"
        )
    for dataset in GOAL22_OUTPUT_DATASETS:
        item = outputs[dataset]
        if not isinstance(item, dict):
            raise Goal23InputError(
                f"Goal 22 daily DQ output evidence mismatch for "
                f"{dataset} {trade_date}"
            )
        write = item.get("write")
        read_back = item.get("read_back")
        if (
            item.get("logical_key")
            != f"processed/{dataset}/trade_date={trade_date}/part.parquet"
            or item.get("object_key")
            != build_goal22_processed_generation_key(
                dataset,
                trade_date,
                generation_id,
            )
            or isinstance(item.get("row_count"), bool)
            or not isinstance(item.get("row_count"), int)
            or item["row_count"] < 0
            or re.fullmatch(r"[0-9a-f]{64}", str(item.get("checksum", "")))
            is None
            or not isinstance(write, dict)
            or write.get("requested") is not True
            or not isinstance(write.get("performed"), bool)
            or write.get("status") not in {"WRITTEN", "UNCHANGED"}
            or not isinstance(read_back, dict)
            or read_back.get("passed") is not True
            or read_back.get("row_count") != item["row_count"]
            or read_back.get("checksum") != item["checksum"]
        ):
            raise Goal23InputError(
                f"Goal 22 daily DQ output evidence mismatch for "
                f"{dataset} {trade_date}"
            )


def _validate_goal22_commit_for_manifest(
    *,
    commit: dict[str, Any],
    report: dict[str, Any],
    manifest: dict[str, Any],
    trade_date: str,
) -> None:
    if (
        not isinstance(commit, dict)
        or commit.get("schema_version") != "goal22.processed_date_commit.v1"
        or commit.get("goal") != "22"
        or commit.get("status") != "COMMITTED"
        or commit.get("run_id") != manifest["run_id"]
        or commit.get("trade_date") != trade_date
        or commit.get("plan_fingerprint") != manifest["plan_fingerprint"]
        or commit.get("input_fingerprint") != _stable_hash(report["inputs"])
        or commit.get("downstream_firewalls") != _GOAL22_DOWNSTREAM_FIREWALLS
    ):
        raise Goal23InputError(
            f"Goal 22 date commit does not match manifest for {trade_date}"
        )
    generation_id = str(commit.get("generation_id", ""))
    if re.fullmatch(r"[0-9a-f]{64}", generation_id) is None:
        raise Goal23InputError(
            f"Goal 22 generation id is invalid for {trade_date}"
        )
    if generation_id != report["commit"]["generation_id"]:
        raise Goal23InputError(
            f"Goal 22 DQ and date commit generation mismatch for {trade_date}"
        )
    outputs = commit.get("outputs")
    if not isinstance(outputs, dict) or set(outputs) != set(
        GOAL22_OUTPUT_DATASETS
    ):
        raise Goal23InputError(
            f"Goal 22 date commit output set is invalid for {trade_date}"
        )
    for dataset in GOAL22_OUTPUT_DATASETS:
        expected = report["outputs"][dataset]
        committed = outputs[dataset]
        if not isinstance(committed, dict):
            raise Goal23InputError(
                f"Goal 22 date commit mapping mismatch for "
                f"{dataset} {trade_date}"
            )
        if committed != {
            "logical_key": expected["logical_key"],
            "row_count": expected["row_count"],
            "checksum": expected["checksum"],
            "object_key": build_goal22_processed_generation_key(
                dataset,
                trade_date,
                generation_id,
            ),
        }:
            raise Goal23InputError(
                f"Goal 22 date commit mapping mismatch for "
                f"{dataset} {trade_date}"
            )


def _history_coverage(
    *,
    factor_input_table: pd.DataFrame,
    adjusted_price_history: pd.DataFrame,
    clean_snapshot_history: pd.DataFrame,
    benchmark_price_history: pd.DataFrame,
    history_dates: list[str],
    target_trade_date: str,
) -> dict[str, Any]:
    codes = sorted(set(factor_input_table["stock_code"].astype(str)))
    price_counts = {
        code: int(
            (
                adjusted_price_history["stock_code"].astype(str) == code
            ).sum()
        )
        for code in codes
    }
    valuation_counts = {
        code: int(
            (
                clean_snapshot_history["stock_code"].astype(str) == code
            ).sum()
        )
        for code in codes
    }
    valuation_earliest_dates = {
        code: (
            clean_snapshot_history.loc[
                clean_snapshot_history["stock_code"].astype(str) == code,
                "trade_date",
            ]
            .astype(str)
            .min()
        )
        for code in codes
    }
    benchmark_counts = {
        index_code: int(
            (
                benchmark_price_history["index_code"].astype(str)
                == index_code
            ).sum()
        )
        for index_code in sorted(REQUIRED_BENCHMARK_INDEXES)
    }
    minimum_price_observations = min(price_counts.values()) if price_counts else 0
    minimum_valuation_observations = (
        min(valuation_counts.values()) if valuation_counts else 0
    )
    hs300_observations = benchmark_counts.get("000300.SH", 0)
    required_valuation_start = (
        calendar_date.fromisoformat(target_trade_date)
        - timedelta(
            days=365 * 3 - THREE_YEAR_COVERAGE_TOLERANCE_DAYS
        )
    ).isoformat()
    valuation_span_incomplete = any(
        not earliest or earliest > required_valuation_start
        for earliest in valuation_earliest_dates.values()
    )
    return {
        "trusted_trade_dates": list(history_dates),
        "trusted_trade_date_count": len(history_dates),
        "target_stock_count": len(codes),
        "price_observations_by_stock": price_counts,
        "valuation_observations_by_stock": valuation_counts,
        "valuation_earliest_date_by_stock": valuation_earliest_dates,
        "benchmark_observations_by_index": benchmark_counts,
        "minimum_price_observations": minimum_price_observations,
        "minimum_valuation_observations": minimum_valuation_observations,
        "hs300_observations": hs300_observations,
        "valuation_3y_required_start_on_or_before": (
            required_valuation_start
        ),
        "window_requirements": {
            "trend_ret_20d": 21,
            "trend_ma20": 20,
            "trend_ret_60d": 61,
            "trend_ma60": 60,
            "trend_ret_120d": 121,
            "trend_ma120": 120,
            "valuation_percentile_3y": "AVAILABLE_TARGET_OR_PRIOR_3Y",
            "industry_strength_60d": 61,
            "industry_strength_120d": 121,
        },
        "insufficient_windows": {
            "price_20d": minimum_price_observations < 21,
            "price_60d": minimum_price_observations < 61,
            "price_120d": minimum_price_observations < 121,
            "benchmark_60d": hs300_observations < 61,
            "benchmark_120d": hs300_observations < 121,
            "valuation_3y": (
                minimum_valuation_observations - 1
                < MIN_THREE_YEAR_OBSERVATIONS
                or valuation_span_incomplete
            ),
        },
        "insufficient_history_policy": "KEEP_NULL",
        "future_rows_used": False,
    }


def _concat_history(
    frames: list[pd.DataFrame],
    *,
    key_columns: list[str],
    target_trade_date: str,
) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame(columns=key_columns)
    result = pd.concat(
        [frame.copy(deep=True) for frame in frames],
        ignore_index=True,
    )
    if "trade_date" not in result:
        raise Goal23InputError("history frame is missing trade_date")
    result["trade_date"] = result["trade_date"].astype(str)
    if (result["trade_date"] > target_trade_date).any():
        raise Goal23InputError("future rows are forbidden in Goal 23 history")
    if result.duplicated(key_columns).any():
        raise Goal23InputError(
            f"duplicate trusted history keys: {', '.join(key_columns)}"
        )
    return result.sort_values(key_columns).reset_index(drop=True)


def _empty_daily_report(
    *,
    run_id: str,
    trade_date: str,
    mode: str,
    attempt: int,
    plan_fingerprint: str,
    factor_config: dict[str, Any],
    factor_config_fingerprint: str,
    goal22_manifests: list[dict[str, Any]],
    logical_output_key: str,
    commit_key: str,
    generated_at: str,
) -> dict[str, Any]:
    return {
        "schema_version": "goal23.real_factor_daily_daily_dq.v1",
        "goal": "23",
        "run_id": run_id,
        "trade_date": trade_date,
        "generated_at": generated_at,
        "mode": mode,
        "status": "RUNNING",
        "attempt": attempt,
        "resume_action": "NOT_APPLICABLE",
        "plan_fingerprint": plan_fingerprint,
        "factor_config": deepcopy(factor_config),
        "factor_config_fingerprint": factor_config_fingerprint,
        "goal22_manifests": deepcopy(goal22_manifests),
        "goal22_input_lineage": {},
        "history_coverage": {},
        "input_fingerprint": None,
        "factor_contract_audit": {},
        "factor_missing_rates": {},
        "output": {
            "dataset": "factor_daily",
            "logical_key": logical_output_key,
            "object_key": None,
            "row_count": 0,
            "checksum": None,
            "write": {
                "requested": mode == "APPLY",
                "performed": False,
                "status": "NOT_RUN",
            },
            "read_back": {"passed": False, "status": "NOT_RUN"},
        },
        "commit": {
            "object_key": commit_key,
            "status": "PENDING" if mode == "APPLY" else "NOT_REQUESTED",
            "generation_id": None,
            "reused": False,
        },
        "blocked_reasons": [],
        "failure": None,
        "downstream_firewalls": deepcopy(GOAL23_DOWNSTREAM_FIREWALLS),
    }


def _completed_factor_commit_matches(
    commit: dict[str, Any],
    *,
    run_id: str,
    trade_date: str,
    plan_fingerprint: str,
    input_fingerprint: str,
    factor_config_fingerprint: str,
    output_checksum: str,
    row_count: int,
    factor_object_read_fn: ReadFactorObjectFn,
) -> bool:
    try:
        validate_goal23_factor_commit_payload(commit, trade_date)
    except (DataValidationError, ValueError, KeyError, TypeError):
        return False
    if (
        commit.get("run_id") != run_id
        or commit.get("plan_fingerprint") != plan_fingerprint
        or commit.get("input_fingerprint") != input_fingerprint
        or commit.get("factor_config_fingerprint")
        != factor_config_fingerprint
        or commit["output"]["row_count"] != row_count
        or commit["output"]["checksum"] != output_checksum
    ):
        return False
    try:
        frame = factor_object_read_fn(commit["output"]["object_key"])
    except (FileNotFoundError, DataValidationError, ValueError, TypeError):
        return False
    return _valid_factor_output_checksum(
        frame,
        trade_date,
        output_checksum,
    )


def _valid_factor_output_checksum(
    frame: pd.DataFrame,
    trade_date: str,
    expected_checksum: str,
) -> bool:
    try:
        normalized = normalize_factor_daily_read_back(frame)
        validate_factor_daily(normalized, trade_date)
        return (
            dataframe_checksum(
                normalized,
                key_columns=["stock_code", "trade_date"],
            )
            == expected_checksum
        )
    except (DataValidationError, ValueError, TypeError, KeyError):
        return False


def _factor_generation_id(
    *,
    run_id: str,
    trade_date: str,
    plan_fingerprint: str,
    input_fingerprint: str,
    factor_config_fingerprint: str,
    output_checksum: str,
    row_count: int,
) -> str:
    return _stable_hash(
        {
            "run_id": run_id,
            "trade_date": trade_date,
            "plan_fingerprint": plan_fingerprint,
            "input_fingerprint": input_fingerprint,
            "factor_config_fingerprint": factor_config_fingerprint,
            "output_checksum": output_checksum,
            "row_count": row_count,
        }
    )


def _tentative_goal22_trade_dates(payload: dict[str, Any]) -> list[str]:
    try:
        values = payload.get("plan", {}).get("trade_dates", [])
        return _normalize_trade_dates(values)
    except (ValueError, TypeError):
        return []


def _parse_factor_config(raw_config: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(raw_config, dict):
        raise ValueError("factor config must be a mapping")
    weights: dict[str, float] = {}
    for column in FACTOR_SCORE_COLUMNS:
        if column not in raw_config:
            raise ValueError(f"missing weight: {column}")
        weight = float(raw_config[column])
        if not math.isfinite(weight):
            raise ValueError(f"factor weight must be finite: {column}")
        weights[column] = weight
    weight_sum = sum(weights.values())
    if not math.isfinite(weight_sum) or abs(weight_sum - 1.0) > 1e-9:
        raise ValueError(f"factor weight sum must equal 1, got {weight_sum}")
    scoring = raw_config.get("scoring") or {}
    if not isinstance(scoring, dict):
        raise ValueError("scoring config must be a mapping")
    null_score_policy = str(scoring.get("null_score_policy", "neutral"))
    if null_score_policy != "neutral":
        raise ValueError(
            f"unsupported null_score_policy: {null_score_policy}"
        )
    neutral_score = float(scoring.get("neutral_score", 50.0))
    if neutral_score < 0 or neutral_score > 100:
        raise ValueError("neutral_score must be between 0 and 100")
    return {
        "weights": weights,
        "null_score_policy": null_score_policy,
        "neutral_score": neutral_score,
    }


def _goal22_range_status(statuses: dict[str, str]) -> str:
    values = list(statuses.values())
    if values and all(value == "COMPLETED" for value in values):
        return "COMPLETED"
    if any(value == "COMPLETED" for value in values):
        return "PARTIAL"
    return "BLOCKED" if any(value == "BLOCKED" for value in values) else "FAILED"


def _range_status(
    date_statuses: dict[str, str],
    *,
    apply_processed_write: bool,
) -> str:
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


def _read_json_optional(
    read_fn: ReadJsonFn,
    object_key: str,
) -> dict[str, Any] | None:
    try:
        return read_fn(object_key)
    except FileNotFoundError:
        return None


def _read_factor_object_optional(
    read_fn: ReadFactorObjectFn,
    object_key: str,
) -> pd.DataFrame | None:
    try:
        return read_fn(object_key)
    except FileNotFoundError:
        return None


def _read_factor_commit_optional(
    read_fn: ReadFactorCommitFn,
    trade_date: str,
) -> dict[str, Any] | None:
    try:
        return read_fn(trade_date)
    except FileNotFoundError:
        return None


def _normalize_trade_dates(values: Iterable[str]) -> list[str]:
    if isinstance(values, (str, bytes)):
        raise ValueError("trade_dates must be an iterable of dates")
    return sorted({validate_trade_date(str(value)) for value in values})


def _validate_run_id(run_id: str) -> str:
    if not isinstance(run_id, str) or _RUN_ID_PATTERN.fullmatch(run_id) is None:
        raise ValueError(
            "run_id must use 1-128 safe alphanumeric, dot, underscore or "
            "hyphen characters"
        )
    return run_id


def _stable_hash(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _failure_record(error: BaseException) -> dict[str, str]:
    return {"type": type(error).__name__, "message": _safe_message(error)}


def _safe_message(error: BaseException) -> str:
    text = str(error).replace("\r", " ").replace("\n", " ")[:500]
    return re.sub(
        r"(?i)(token|password|secret|authorization|api[_-]?key)"
        r"\s*[=:]\s*[^\s,;]+",
        r"\1=<redacted>",
        text,
    )


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

from __future__ import annotations

import calendar
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import re
from typing import Any, Callable, Iterable

import numpy as np
import pandas as pd

from stock_selector.data.data_validator import validate_stock_code
from stock_selector.data.real_clean_inputs_landing import KEY_COLUMNS, REQUIRED_INPUTS
from stock_selector.providers.schema_contract import REQUIRED_BENCHMARK_INDEXES
from stock_selector.storage.partition import build_partition
from stock_selector.utils.date_validator import validate_date_range
from stock_selector.utils.path_validator import safe_object_key


PLAN_SCHEMA_VERSION = "goal21.history_backfill_plan.v1"
PLANNER_VERSION = "goal21.history_backfill_planner.v1"
IDENTITY_SCHEMA_VERSION = "goal21.history_backfill_identity.v1"
PLAN_SCHEMA_VERSION_V2 = "goal21.history_backfill_plan.v2"
PLANNER_VERSION_V2 = "goal21.history_backfill_planner.v2"
IDENTITY_SCHEMA_VERSION_V2 = "goal21.history_backfill_identity.v2"
CHUNK_MANIFEST_SCHEMA_VERSION = "goal21.chunk_manifest.v1"
SUMMARY_SCHEMA_VERSION = "goal21.chunk_summary.v1"
BENCHMARK_INDEXES = tuple(sorted(REQUIRED_BENCHMARK_INDEXES))

DATASET_STRATEGIES = {
    "stock_basic": "point_in_time_snapshot_by_code_date_window",
    "daily_price": "mixed_price_by_code_date_window",
    "adj_factor": "by_code_date_window",
    "daily_basic": "by_code_date_window",
    "financial": "by_code_report_period_window",
    "st_history": "by_code_interval_window",
    "benchmark_price": "by_index_date_window",
}

DATASET_STRATEGIES_V2 = {
    "stock_basic": "historical_master_by_code",
    "daily_price": "full_market_by_trade_date_window",
    "adj_factor": "full_market_by_trade_date_window",
    "daily_basic": "full_market_by_trade_date_window",
    "financial": "by_code_announce_date_window",
    "st_history": "historical_interval_by_code",
    "benchmark_price": "by_index_date_window",
}

DEFAULT_V2_MAX_CHUNKS = 25_000
DEFAULT_V2_MAX_PLAN_BYTES = 16 * 1024 * 1024
DEFAULT_V2_MAX_PROVIDER_CALLS = 215_000
DEFAULT_V2_MAX_CANONICAL_READS = 40_000
FINANCIAL_SEED_MANIFEST_MAX_BYTES = 64 * 1024

_RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_CHUNK_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
VALID_CHUNK_STATES = (
    "PENDING",
    "RUNNING",
    "STAGED",
    "COMPLETED",
    "FAILED",
    "BLOCKED",
    "INTERRUPTED",
)
FAILURE_CATEGORIES = (
    "EMPTY_RESULT",
    "RATE_LIMITED",
    "PERMISSION_DENIED",
    "SCHEMA_DRIFT",
    "TRANSIENT_PROVIDER_ERROR",
    "CONFIGURATION_ERROR",
    "SEMANTIC_SOURCE_UNAVAILABLE",
    "DQ_FAILED",
    "WRITE_FAILED",
    "READBACK_FAILED",
    "INTERRUPTED",
    "UNKNOWN",
)
_RETRYABLE_FAILURES = {
    "RATE_LIMITED",
    "TRANSIENT_PROVIDER_ERROR",
    "WRITE_FAILED",
    "READBACK_FAILED",
    "UNKNOWN",
}
_CREDENTIAL_PATTERN = re.compile(
    r'''(?ix)
    (?P<prefix>
        ["']?(?:api_token|access_token|token|secret|password|authorization)["']?
        (?:\s*(?:=|:)\s*|\s+)
    )
    (?:(?:bearer|basic|apikey|token)\s+)?
    (?P<value>
        "(?:\\.|[^"\\])*"
        |'(?:\\.|[^'\\])*'
        |[^\s,;&}]+
    )
    ''',
)
_BEARER_PATTERN = re.compile(
    r'''(?ix)\bbearer\s+(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|[^\s,;&]+)'''
)
_CHUNK_MANIFEST_FIELDS = frozenset(
    {
        "schema_version",
        "chunk_id",
        "dataset",
        "chunk",
        "attempt_count",
        "plan_fingerprint",
        "requested_stages",
        "provider_status",
        "row_count",
        "actual_schema",
        "target_schema",
        "dq",
        "coverage",
        "source_key",
        "staging_key",
        "staging_checksum",
        "staging_attempt",
        "canonical_key",
        "canonical_checksum",
        "canonical_keys",
        "canonical_checksums",
        "validation",
        "write_result",
        "read_back_result",
        "failure",
        "state",
    }
)


class BackfillPlanningError(ValueError):
    """Stable configuration error contract for Goal 21 planning."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class BackfillExecutionError(RuntimeError):
    """A classified executor-boundary failure safe for persisted manifests."""

    def __init__(self, failure_category: str, message: str) -> None:
        if failure_category not in FAILURE_CATEGORIES:
            raise ValueError(f"unsupported backfill failure category: {failure_category}")
        super().__init__(_redact_failure_message(message))
        self.failure_category = failure_category


def dataframe_checksum(frame: pd.DataFrame, *, key_columns: Iterable[str] | None = None) -> str:
    """Return a deterministic content-and-schema checksum without mutating frame."""

    if not isinstance(frame, pd.DataFrame):
        raise BackfillPlanningError("INVALID_FRAME", "frame must be a pandas DataFrame")
    columns = list(frame.columns)
    try:
        columns_are_unique = len(columns) == len(set(columns))
    except TypeError as exc:
        raise BackfillPlanningError("INVALID_FRAME", "frame columns must be hashable") from exc
    if not columns_are_unique:
        raise BackfillPlanningError("INVALID_FRAME", "frame columns must be unique")
    ordered_columns = sorted(columns, key=lambda column: _stable_json(_checksum_value(column)))
    if isinstance(key_columns, (str, bytes)):
        raise BackfillPlanningError("INVALID_KEY_COLUMNS", "key_columns must be an iterable of column names")
    requested_keys = ordered_columns if key_columns is None else list(key_columns)
    missing = [column for column in requested_keys if column not in columns]
    if missing:
        raise BackfillPlanningError(
            "MISSING_KEY_COLUMNS",
            f"missing checksum key columns: {', '.join(map(str, missing))}",
        )

    normalized = frame.loc[:, ordered_columns]
    schema = [_checksum_column_schema(column, normalized[column]) for column in ordered_columns]
    key_indexes = [ordered_columns.index(column) for column in requested_keys]
    rows = []
    for raw_row in normalized.itertuples(index=False, name=None):
        row = [_checksum_value(value) for value in raw_row]
        key = [row[index] for index in key_indexes]
        rows.append((_stable_json(key), _stable_json(row), row))
    rows.sort(key=lambda item: (item[0], item[1]))
    payload = {
        "schema": schema,
        "rows": [item[2] for item in rows],
    }
    return _stable_hash(payload)


def persist_historical_raw_landing(
    *,
    provider_name: str,
    run_id: str,
    endpoint: str,
    parameters: dict[str, Any],
    frame: pd.DataFrame,
    read_parquet_fn: Callable[[str], pd.DataFrame],
    write_parquet_fn: Callable[[str, pd.DataFrame], Any],
) -> str:
    """Persist one immutable provider response and verify its exact read-back."""

    run_id = _validate_run_id(run_id)
    provider = str(provider_name).strip().lower()
    endpoint = str(endpoint).strip().lower()
    segment_pattern = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
    if not segment_pattern.fullmatch(provider) or not segment_pattern.fullmatch(endpoint):
        raise BackfillPlanningError("INVALID_RAW_LANDING_SCOPE", "provider or endpoint is unsafe")
    if not isinstance(parameters, dict):
        raise BackfillPlanningError("INVALID_RAW_LANDING_SCOPE", "parameters must be a dictionary")
    if not isinstance(frame, pd.DataFrame):
        raise BackfillExecutionError("SCHEMA_DRIFT", "raw landing frame must be a DataFrame")
    try:
        request_hash = _stable_hash(_checksum_value(parameters))
    except (TypeError, ValueError) as exc:
        raise BackfillPlanningError(
            "INVALID_RAW_LANDING_SCOPE",
            "provider parameters are not deterministically serializable",
        ) from exc
    checksum = dataframe_checksum(frame)
    response_evidence = historical_raw_landing_evidence(endpoint, frame)
    evidence_hash = _stable_hash(_checksum_value(response_evidence))
    object_key = (
        f"raw/provider_landing/provider={provider}/run_id={run_id}/"
        f"endpoint={endpoint}/request={request_hash[:24]}/"
        f"response={checksum[:24]}/evidence={evidence_hash[:24]}/part.parquet"
    )
    try:
        safe_object_key(object_key)
    except ValueError as exc:
        raise BackfillPlanningError("INVALID_RAW_LANDING_SCOPE", "raw landing key is unsafe") from exc

    try:
        persisted = read_parquet_fn(object_key)
    except FileNotFoundError:
        write_parquet_fn(object_key, frame.copy(deep=True))
        try:
            persisted = read_parquet_fn(object_key)
        except FileNotFoundError as exc:
            raise BackfillExecutionError(
                "READBACK_FAILED",
                "raw landing object is missing after atomic write",
            ) from exc
    if not isinstance(persisted, pd.DataFrame):
        raise BackfillExecutionError("READBACK_FAILED", "raw landing read-back is not a DataFrame")
    if dataframe_checksum(persisted) != checksum:
        raise BackfillExecutionError("READBACK_FAILED", "raw landing checksum mismatch")
    if historical_raw_landing_evidence(endpoint, persisted) != response_evidence:
        raise BackfillExecutionError(
            "READBACK_FAILED",
            "raw landing semantic evidence mismatch",
        )
    return object_key


def historical_raw_landing_evidence(
    endpoint: str,
    frame: pd.DataFrame,
) -> dict[str, Any]:
    """Return safety-relevant response metadata bound to raw landing identity."""

    if str(endpoint).strip().lower() != "suspend_d":
        return {}
    attrs = frame.attrs if isinstance(frame, pd.DataFrame) else {}
    pagination = attrs.get("pagination")
    return {
        "full_market_event_set": deepcopy(attrs.get("full_market_event_set")),
        "coverage_complete": deepcopy(attrs.get("coverage_complete")),
        "sample_truncated": deepcopy(attrs.get("sample_truncated")),
        "empty_after_retries": deepcopy(attrs.get("empty_after_retries")),
        "covered_trade_dates": deepcopy(attrs.get("covered_trade_dates")),
        "pagination": deepcopy(pagination) if isinstance(pagination, dict) else None,
    }


def classify_backfill_failure(error: BaseException) -> dict[str, Any]:
    """Classify and sanitize a backfill failure for persisted control records."""

    if isinstance(error, KeyboardInterrupt):
        category = "INTERRUPTED"
    elif isinstance(error, BackfillPlanningError):
        category = "CONFIGURATION_ERROR"
    else:
        explicit = getattr(error, "failure_category", None)
        if explicit in FAILURE_CATEGORIES:
            category = explicit
        else:
            category = _infer_failure_category(error)
    return {
        "category": category,
        "retryable": category in _RETRYABLE_FAILURES,
        "exception_type": _sanitize_exception_type(type(error).__name__),
        "message": _redact_failure_message(str(error)),
    }


def build_chunk_manifest(
    *,
    chunk: dict[str, Any],
    state: str,
    attempt_count: int = 0,
    plan_fingerprint: str | None = None,
    requested_stages: Iterable[str] | None = None,
    provider_status: Any = None,
    row_count: int | None = None,
    actual_schema: Any = None,
    target_schema: Any = None,
    dq: Any = None,
    coverage: Any = None,
    source_key: str | None = None,
    staging_key: str | None = None,
    staging_checksum: str | None = None,
    staging_attempt: int | None = None,
    canonical_key: str | None = None,
    canonical_checksum: str | None = None,
    canonical_keys: Iterable[str] | None = None,
    canonical_checksums: dict[str, str] | None = None,
    validation: Any = None,
    write_result: Any = None,
    read_back_result: Any = None,
    failure: Any = None,
) -> dict[str, Any]:
    """Build and validate one pure chunk manifest state snapshot."""

    if not isinstance(chunk, dict) or "chunk_id" not in chunk or "dataset" not in chunk:
        raise BackfillPlanningError("INVALID_CHUNK", "manifest chunk must expose chunk_id and dataset")
    semantics, expected_chunk_id = _validated_chunk_identity(chunk)
    if chunk["chunk_id"] != expected_chunk_id:
        raise BackfillPlanningError(
            "TAMPERED_CHUNK_ID",
            f"chunk_id does not match chunk semantics: {chunk['chunk_id']}",
        )
    _validate_attempt_count(attempt_count, state)
    normalized_requested_stages = _normalize_requested_stages(requested_stages)
    _validate_optional_attempt("staging_attempt", staging_attempt, attempt_count)
    normalized_canonical_keys = _normalize_canonical_keys(canonical_keys)
    normalized_canonical_checksums = _normalize_canonical_checksums(canonical_checksums)
    _validate_row_count(row_count)
    normalized_failure = _normalize_failure_record(failure)
    _validate_failure_state(state, normalized_failure)
    _validate_manifest_evidence(
        chunk=chunk,
        dataset=chunk["dataset"],
        state=state,
        provider_status=provider_status,
        row_count=row_count,
        actual_schema=actual_schema,
        target_schema=target_schema,
        dq=dq,
        coverage=coverage,
        source_key=source_key,
        staging_key=staging_key,
        staging_checksum=staging_checksum,
        canonical_key=canonical_key,
        canonical_checksum=canonical_checksum,
        canonical_keys=normalized_canonical_keys,
        canonical_checksums=normalized_canonical_checksums,
        validation=validation,
        write_result=write_result,
        read_back_result=read_back_result,
    )
    return {
        "schema_version": CHUNK_MANIFEST_SCHEMA_VERSION,
        "chunk_id": chunk["chunk_id"],
        "dataset": chunk["dataset"],
        "chunk": deepcopy(chunk),
        "attempt_count": attempt_count,
        "plan_fingerprint": plan_fingerprint,
        "requested_stages": normalized_requested_stages,
        "provider_status": deepcopy(provider_status),
        "row_count": row_count,
        "actual_schema": deepcopy(actual_schema),
        "target_schema": deepcopy(target_schema),
        "dq": deepcopy(dq),
        "coverage": deepcopy(coverage),
        "source_key": source_key,
        "staging_key": staging_key,
        "staging_checksum": staging_checksum,
        "staging_attempt": staging_attempt,
        "canonical_key": canonical_key,
        "canonical_checksum": canonical_checksum,
        "canonical_keys": normalized_canonical_keys,
        "canonical_checksums": normalized_canonical_checksums,
        "validation": deepcopy(validation),
        "write_result": deepcopy(write_result),
        "read_back_result": deepcopy(read_back_result),
        "failure": deepcopy(normalized_failure),
        "state": state,
    }


def summarize_chunk_manifests(
    plan: dict[str, Any],
    manifests: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    """Account for every planned chunk exactly once using pure manifest data."""

    planned_chunks = list(plan.get("chunks", [])) if isinstance(plan, dict) else []
    planned_by_id: dict[str, dict[str, Any]] = {}
    for chunk in planned_chunks:
        if not isinstance(chunk, dict) or not chunk.get("chunk_id") or not chunk.get("dataset"):
            raise BackfillPlanningError("INVALID_PLAN", "plan contains an invalid chunk")
        chunk_id = str(chunk["chunk_id"])
        if chunk_id in planned_by_id:
            raise BackfillPlanningError("DUPLICATE_PLANNED_CHUNK", f"duplicate planned chunk: {chunk_id}")
        planned_by_id[chunk_id] = chunk

    supplied: dict[str, dict[str, Any]] = {}
    for manifest in manifests:
        if not isinstance(manifest, dict) or not manifest.get("chunk_id"):
            raise BackfillPlanningError("INVALID_MANIFEST", "manifest must expose chunk_id")
        chunk_id = str(manifest["chunk_id"])
        if chunk_id in supplied:
            raise BackfillPlanningError("DUPLICATE_MANIFEST", f"duplicate manifest: {chunk_id}")
        if chunk_id not in planned_by_id:
            raise BackfillPlanningError("FOREIGN_MANIFEST", f"foreign manifest: {chunk_id}")
        planned_chunk = planned_by_id[chunk_id]
        if manifest.get("dataset") != planned_chunk["dataset"]:
            raise BackfillPlanningError("FOREIGN_MANIFEST", f"manifest dataset mismatch: {chunk_id}")
        normalized_manifest = _validate_persisted_manifest(manifest)
        if normalized_manifest["chunk"] != planned_chunk:
            raise BackfillPlanningError(
                "TAMPERED_MANIFEST_SCOPE",
                f"manifest chunk scope does not match plan: {chunk_id}",
            )
        supplied[chunk_id] = normalized_manifest

    state_counts = {state: 0 for state in VALID_CHUNK_STATES}
    per_dataset: dict[str, dict[str, Any]] = {}
    gaps = []
    for chunk in planned_chunks:
        dataset = chunk["dataset"]
        dataset_summary = per_dataset.setdefault(
            dataset,
            {"total": 0, "state_counts": {state: 0 for state in VALID_CHUNK_STATES}},
        )
        dataset_summary["total"] += 1
        manifest = supplied.get(chunk["chunk_id"])
        state = "PENDING" if manifest is None else manifest["state"]
        state_counts[state] += 1
        dataset_summary["state_counts"][state] += 1
        if state != "COMPLETED":
            failure = manifest.get("failure") if manifest is not None else None
            category = failure.get("category") if isinstance(failure, dict) else None
            if isinstance(failure, dict) and failure.get("message"):
                reason = failure["message"]
            elif manifest is None:
                reason = "manifest missing"
            else:
                reason = f"chunk state is {state}"
            gaps.append(
                {
                    "dataset": dataset,
                    "chunk_id": chunk["chunk_id"],
                    "state": state,
                    "category": category,
                    "reason": reason,
                }
            )

    planned = len(planned_chunks)
    completed = state_counts["COMPLETED"]
    return {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "total": planned,
        "planned": planned,
        "state_counts": state_counts,
        "accounted_count": planned,
        "completion_rate": completed / planned if planned else 0.0,
        "canonical_ready": planned > 0 and completed == planned,
        "per_dataset": per_dataset,
        "gaps": gaps,
    }


def build_history_backfill_plan(
    *,
    run_id: str,
    start_date: str,
    end_date: str,
    codes: Iterable[str] | None = None,
    universe_frame: pd.DataFrame | None = None,
    universe_key: str | None = None,
    code_batch_size: int,
    date_batch_days: int,
    report_period_months: int,
    datasets: Iterable[str] | None = None,
    generated_at_fn: Callable[[], str] | None = None,
) -> dict[str, Any]:
    """Build an immutable, deterministic Goal 21 historical backfill plan."""

    run_id = _validate_run_id(run_id)
    try:
        start_date, end_date = validate_date_range(start_date, end_date)
    except ValueError as exc:
        raise BackfillPlanningError("INVALID_DATE_RANGE", str(exc)) from exc
    _validate_positive_limit("code_batch_size", code_batch_size)
    _validate_positive_limit("date_batch_days", date_batch_days)
    _validate_positive_limit("report_period_months", report_period_months)

    normalized_codes, universe_source, normalized_universe_key = _normalize_universe(
        codes=codes,
        universe_frame=universe_frame,
        universe_key=universe_key,
    )
    selected_datasets = _normalize_datasets(datasets)
    date_windows = _split_date_windows(start_date, end_date, date_batch_days)
    report_windows = _split_report_period_windows(start_date, end_date, report_period_months)
    code_batches = _split_codes(normalized_codes, code_batch_size)

    chunks: list[dict[str, Any]] = []
    for dataset in selected_datasets:
        windows = report_windows if dataset == "financial" else date_windows
        if dataset == "benchmark_price":
            for window_start, window_end in windows:
                chunks.append(
                    _build_chunk(
                        dataset=dataset,
                        codes=[],
                        start_date=window_start,
                        end_date=window_end,
                        index_codes=list(BENCHMARK_INDEXES),
                    )
                )
            continue

        for window_start, window_end in windows:
            for code_batch in code_batches:
                chunks.append(
                    _build_chunk(
                        dataset=dataset,
                        codes=code_batch,
                        start_date=window_start,
                        end_date=window_end,
                    )
                )

    scope = {
        "start_date": start_date,
        "end_date": end_date,
        "date_count": (date.fromisoformat(end_date) - date.fromisoformat(start_date)).days + 1,
        "codes": normalized_codes,
        "code_count": len(normalized_codes),
        "universe_source": universe_source,
        "universe_key": normalized_universe_key,
    }
    limits = {
        "code_batch_size": code_batch_size,
        "date_batch_days": date_batch_days,
        "report_period_months": report_period_months,
    }
    fingerprint_payload = {
        "identity_schema_version": IDENTITY_SCHEMA_VERSION,
        "planner_version": PLANNER_VERSION,
        "scope": scope,
        "datasets": selected_datasets,
        "limits": limits,
        "strategies": {dataset: DATASET_STRATEGIES[dataset] for dataset in selected_datasets},
        "chunks": chunks,
    }
    generated_at_fn = generated_at_fn or _utc_now_iso
    return {
        "schema_version": PLAN_SCHEMA_VERSION,
        "identity_schema_version": IDENTITY_SCHEMA_VERSION,
        "planner_version": PLANNER_VERSION,
        "run_id": run_id,
        "generated_at": generated_at_fn(),
        "plan_fingerprint": _stable_hash(fingerprint_payload),
        "scope": scope,
        "limits": limits,
        "datasets": selected_datasets,
        "strategies": {dataset: DATASET_STRATEGIES[dataset] for dataset in selected_datasets},
        "chunk_count": len(chunks),
        "chunks": chunks,
    }


def estimate_history_backfill_v1_risk(
    *,
    start_date: str,
    end_date: str,
    code_count: int,
    code_batch_size: int,
    date_batch_days: int,
    report_period_months: int,
) -> dict[str, Any]:
    """Estimate the legacy Cartesian planner without materializing its chunks."""

    try:
        start_date, end_date = validate_date_range(start_date, end_date)
    except ValueError as exc:
        raise BackfillPlanningError("INVALID_DATE_RANGE", str(exc)) from exc
    _validate_positive_limit("code_count", code_count)
    _validate_positive_limit("code_batch_size", code_batch_size)
    _validate_positive_limit("date_batch_days", date_batch_days)
    _validate_positive_limit("report_period_months", report_period_months)

    date_count = (date.fromisoformat(end_date) - date.fromisoformat(start_date)).days + 1
    date_window_count = (date_count + date_batch_days - 1) // date_batch_days
    report_window_count = len(
        _split_report_period_windows(start_date, end_date, report_period_months)
    )
    code_batch_count = (code_count + code_batch_size - 1) // code_batch_size
    # Five v1 datasets repeat code batches for every date window, financial
    # repeats them for report-period windows, and benchmark is market-level.
    chunk_count = (
        5 * date_window_count * code_batch_count
        + report_window_count * code_batch_count
        + date_window_count
    )
    open_date_upper_bound = _weekday_count(start_date, end_date)
    provider_calls = (
        3 * code_count * date_window_count
        + 2 * code_batch_count * open_date_upper_bound
        + 1
    )
    financial_chunks = report_window_count * code_batch_count
    canonical_reads = financial_chunks * open_date_upper_bound
    return {
        "planner_version": PLANNER_VERSION,
        "date_count": date_count,
        "date_window_count": date_window_count,
        "report_window_count": report_window_count,
        "code_batch_count": code_batch_count,
        "chunk_count": chunk_count,
        # Derived from the measured compact v1 representation. The estimate
        # deliberately excludes Python deepcopy overhead and pretty-printing.
        "plan_bytes_upper_bound": chunk_count * 343,
        "provider_call_count_upper_bound": provider_calls,
        "canonical_read_count_upper_bound": canonical_reads,
        "financial_canonical_read_count_upper_bound": canonical_reads,
    }


def estimate_history_backfill_plan_v2(
    *,
    start_date: str,
    end_date: str,
    code_count: int,
    code_batch_size: int,
    date_batch_days: int,
    announce_date_batch_days: int,
) -> dict[str, Any]:
    """Return a constant-space upper bound for the natural-axis v2 plan."""

    try:
        start_date, end_date = validate_date_range(start_date, end_date)
    except ValueError as exc:
        raise BackfillPlanningError("INVALID_DATE_RANGE", str(exc)) from exc
    for name, value in (
        ("code_count", code_count),
        ("code_batch_size", code_batch_size),
        ("date_batch_days", date_batch_days),
        ("announce_date_batch_days", announce_date_batch_days),
    ):
        _validate_positive_limit(name, value)

    date_count = (date.fromisoformat(end_date) - date.fromisoformat(start_date)).days + 1
    date_window_count = (date_count + date_batch_days - 1) // date_batch_days
    announce_window_count = (
        date_count + announce_date_batch_days - 1
    ) // announce_date_batch_days
    code_batch_count = (code_count + code_batch_size - 1) // code_batch_size
    open_date_upper_bound = _weekday_count(start_date, end_date)

    # Four price-like/benchmark date axes plus daily_price itself are market
    # windows. Stock/ST use one code cohort each and financial uses bounded
    # announcement-window cohorts that are reduced once per canonical date.
    market_window_chunks = 5 * date_window_count
    historical_semantic_chunks = 2
    financial_source_chunks = announce_window_count
    chunk_count = market_window_chunks + historical_semantic_chunks + financial_source_chunks

    market_provider_calls = 5 * open_date_upper_bound + 3 * date_window_count + 1
    provider_calls = (
        market_provider_calls
        + financial_source_chunks
        + historical_semantic_chunks
    )
    # A successful fresh apply performs one read before write and one exact
    # read-back per materialized partition. Financial may additionally scan a
    # bounded seed window once when resuming an existing history chain.
    canonical_reads = 14 * open_date_upper_bound + 31
    plan_bytes = (
        16_384
        + FINANCIAL_SEED_MANIFEST_MAX_BYTES
        + code_count * 24
        + code_count * 24 * (announce_window_count + 2)
        + chunk_count * 640
    )
    return {
        "planner_version": PLANNER_VERSION_V2,
        "date_count": date_count,
        "date_window_count": date_window_count,
        "announce_window_count": announce_window_count,
        "code_batch_count": code_batch_count,
        "chunk_count": chunk_count,
        "plan_bytes_upper_bound": plan_bytes,
        "provider_call_count_upper_bound": provider_calls,
        "market_level_provider_call_count_upper_bound": market_provider_calls,
        "canonical_read_count_upper_bound": canonical_reads,
        "financial_canonical_read_count_upper_bound": 2 * open_date_upper_bound + 31,
    }


def build_history_backfill_plan_v2(
    *,
    run_id: str,
    start_date: str,
    end_date: str,
    codes: Iterable[str] | None = None,
    universe_frame: pd.DataFrame | None = None,
    universe_key: str | None = None,
    code_batch_size: int,
    date_batch_days: int,
    announce_date_batch_days: int,
    datasets: Iterable[str] | None = None,
    max_chunks: int = DEFAULT_V2_MAX_CHUNKS,
    max_plan_bytes: int = DEFAULT_V2_MAX_PLAN_BYTES,
    max_provider_calls: int = DEFAULT_V2_MAX_PROVIDER_CALLS,
    max_canonical_reads: int = DEFAULT_V2_MAX_CANONICAL_READS,
    financial_seed_manifest: dict[str, Any] | None = None,
    generated_at_fn: Callable[[], str] | None = None,
) -> dict[str, Any]:
    """Build a bounded v2 plan whose chunks follow provider-natural axes."""

    run_id = _validate_run_id(run_id)
    try:
        start_date, end_date = validate_date_range(start_date, end_date)
    except ValueError as exc:
        raise BackfillPlanningError("INVALID_DATE_RANGE", str(exc)) from exc
    for name, value in (
        ("code_batch_size", code_batch_size),
        ("date_batch_days", date_batch_days),
        ("announce_date_batch_days", announce_date_batch_days),
        ("max_chunks", max_chunks),
        ("max_plan_bytes", max_plan_bytes),
        ("max_provider_calls", max_provider_calls),
        ("max_canonical_reads", max_canonical_reads),
    ):
        _validate_positive_limit(name, value)

    normalized_codes, universe_source, normalized_universe_key = _normalize_universe(
        codes=codes,
        universe_frame=universe_frame,
        universe_key=universe_key,
    )
    universe_id = _stable_hash(
        {
            "codes": normalized_codes,
            "universe_source": universe_source,
            "universe_key": normalized_universe_key,
        }
    )
    selected_datasets = _normalize_datasets(datasets)
    normalized_seed_manifest = _normalize_financial_seed_manifest(
        financial_seed_manifest,
        codes=normalized_codes,
        universe_id=universe_id,
        start_date=start_date,
        selected_datasets=selected_datasets,
    )
    estimate = estimate_history_backfill_plan_v2(
        start_date=start_date,
        end_date=end_date,
        code_count=len(normalized_codes),
        code_batch_size=code_batch_size,
        date_batch_days=date_batch_days,
        announce_date_batch_days=announce_date_batch_days,
    )
    if normalized_universe_key is not None:
        estimate["plan_bytes_upper_bound"] += len(
            normalized_universe_key.encode("utf-8")
        )
    exceeded = [
        ("chunk_count", estimate["chunk_count"], max_chunks),
        ("plan_bytes", estimate["plan_bytes_upper_bound"], max_plan_bytes),
        ("provider_calls", estimate["provider_call_count_upper_bound"], max_provider_calls),
        ("canonical_reads", estimate["canonical_read_count_upper_bound"], max_canonical_reads),
    ]
    over_budget = [f"{name}={actual}>{limit}" for name, actual, limit in exceeded if actual > limit]
    if over_budget:
        raise BackfillPlanningError(
            "PLAN_BUDGET_EXCEEDED",
            "v2 preflight budget exceeded: " + ", ".join(over_budget),
        )

    date_windows = _split_date_windows(start_date, end_date, date_batch_days)
    announce_windows = _split_date_windows(
        start_date,
        end_date,
        announce_date_batch_days,
    )
    chunks: list[dict[str, Any]] = []
    for dataset in selected_datasets:
        if dataset == "financial":
            previous_chunk_id: str | None = None
            for window_start, window_end in announce_windows:
                materialization_id = _v2_materialization_id(dataset, window_start, window_end)
                financial_chunk = _build_chunk_v2(
                    dataset=dataset,
                    codes=[],
                    universe_id=universe_id,
                    start_date=window_start,
                    end_date=window_end,
                    axis="announce_date",
                    materialization_id=materialization_id,
                    dependency_keys=[] if previous_chunk_id is None else [previous_chunk_id],
                )
                chunks.append(financial_chunk)
                previous_chunk_id = financial_chunk["chunk_id"]
            continue
        if dataset in {"stock_basic", "st_history"}:
            materialization_id = _v2_materialization_id(dataset, start_date, end_date)
            chunks.append(
                _build_chunk_v2(
                    dataset=dataset,
                    codes=[],
                    universe_id=universe_id,
                    start_date=start_date,
                    end_date=end_date,
                    axis="historical_scope",
                    materialization_id=materialization_id,
                )
            )
            continue
        for window_start, window_end in date_windows:
            chunks.append(
                _build_chunk_v2(
                    dataset=dataset,
                    codes=[],
                    universe_id=universe_id,
                    start_date=window_start,
                    end_date=window_end,
                    axis="trade_date",
                    materialization_id=_v2_materialization_id(dataset, window_start, window_end),
                    index_codes=list(BENCHMARK_INDEXES) if dataset == "benchmark_price" else None,
                )
            )

    scope = {
        "start_date": start_date,
        "end_date": end_date,
        "date_count": (date.fromisoformat(end_date) - date.fromisoformat(start_date)).days + 1,
        "codes": normalized_codes,
        "code_count": len(normalized_codes),
        "universe_source": universe_source,
        "universe_key": normalized_universe_key,
        "universe_id": universe_id,
        "financial_seed": normalized_seed_manifest,
    }
    limits = {
        "code_batch_size": code_batch_size,
        "date_batch_days": date_batch_days,
        "announce_date_batch_days": announce_date_batch_days,
        "max_chunks": max_chunks,
        "max_plan_bytes": max_plan_bytes,
        "max_provider_calls": max_provider_calls,
        "max_canonical_reads": max_canonical_reads,
    }
    strategies = {dataset: DATASET_STRATEGIES_V2[dataset] for dataset in selected_datasets}
    fingerprint_payload = {
        "identity_schema_version": IDENTITY_SCHEMA_VERSION_V2,
        "planner_version": PLANNER_VERSION_V2,
        "scope": scope,
        "datasets": selected_datasets,
        "limits": limits,
        "strategies": strategies,
        "chunks": chunks,
    }
    generated_at_fn = generated_at_fn or _utc_now_iso
    plan = {
        "schema_version": PLAN_SCHEMA_VERSION_V2,
        "identity_schema_version": IDENTITY_SCHEMA_VERSION_V2,
        "planner_version": PLANNER_VERSION_V2,
        "run_id": run_id,
        "generated_at": generated_at_fn(),
        "plan_fingerprint": _stable_hash(fingerprint_payload),
        "scope": scope,
        "limits": limits,
        "datasets": selected_datasets,
        "strategies": strategies,
        "preflight_estimate": estimate,
        "chunk_count": len(chunks),
        "chunks": chunks,
    }
    actual_plan_bytes = len(_stable_json(plan).encode("utf-8"))
    if actual_plan_bytes > max_plan_bytes:
        raise BackfillPlanningError(
            "PLAN_BUDGET_EXCEEDED",
            "v2 actual plan budget exceeded: "
            f"actual_plan_bytes={actual_plan_bytes}>{max_plan_bytes}",
        )
    return plan


def validate_financial_announce_chunk_v2(chunk: dict[str, Any], frame: pd.DataFrame) -> None:
    """Validate a financial source shard on announcement-time semantics."""

    if not isinstance(chunk, dict) or chunk.get("dataset") != "financial":
        raise BackfillExecutionError("DQ_FAILED", "financial v2 chunk is invalid")
    if chunk.get("axis") != "announce_date":
        raise BackfillExecutionError("DQ_FAILED", "financial v2 chunk must use announce_date axis")
    if not isinstance(frame, pd.DataFrame):
        raise BackfillExecutionError("DQ_FAILED", "financial source must be a DataFrame")
    required = {"stock_code", "report_period", "announce_date"}
    if not required.issubset(frame.columns):
        raise BackfillExecutionError("DQ_FAILED", "financial source schema is incomplete")
    if frame.empty:
        return
    start = chunk.get("announce_date_start")
    end = chunk.get("announce_date_end")
    try:
        validate_date_range(start, end)
    except (TypeError, ValueError) as exc:
        raise BackfillExecutionError("DQ_FAILED", "financial announcement window is invalid") from exc
    report_period = frame["report_period"].astype(str)
    announce_date = frame["announce_date"].astype(str)
    if ((announce_date < start) | (announce_date > end)).any():
        raise BackfillExecutionError("DQ_FAILED", "financial announcement is outside the chunk")
    if (report_period > announce_date).any():
        raise BackfillExecutionError("DQ_FAILED", "financial report period is after its announcement")
    codes = set(chunk.get("codes", []))
    if codes and not frame["stock_code"].astype(str).isin(codes).all():
        raise BackfillExecutionError("DQ_FAILED", "financial row is outside the chunk code scope")


def _normalize_financial_seed_manifest(
    manifest: dict[str, Any] | None,
    *,
    codes: list[str],
    universe_id: str,
    start_date: str,
    selected_datasets: list[str],
) -> dict[str, Any] | None:
    if manifest is None:
        return None
    if "financial" not in selected_datasets:
        raise BackfillPlanningError(
            "INVALID_FINANCIAL_SEED",
            "financial seed evidence is invalid when financial is not planned",
        )
    if not isinstance(manifest, dict):
        raise BackfillPlanningError("INVALID_FINANCIAL_SEED", "financial seed manifest must be an object")
    serialized = _stable_json(manifest).encode("utf-8")
    if len(serialized) > FINANCIAL_SEED_MANIFEST_MAX_BYTES:
        raise BackfillPlanningError("INVALID_FINANCIAL_SEED", "financial seed manifest is too large")
    try:
        normalized = _validate_persisted_manifest(deepcopy(manifest))
    except BackfillPlanningError as exc:
        raise BackfillPlanningError(
            "INVALID_FINANCIAL_SEED",
            f"financial seed manifest is invalid: {exc}",
        ) from exc
    if (
        normalized.get("dataset") != "financial"
        or normalized.get("state") != "COMPLETED"
        or normalized.get("failure") is not None
    ):
        raise BackfillPlanningError(
            "INVALID_FINANCIAL_SEED",
            "financial seed must come from a completed financial manifest",
        )
    chunk = normalized.get("chunk")
    if (
        not isinstance(chunk, dict)
        or chunk.get("chunk_schema_version") != "goal21.history_backfill_chunk.v2"
        or chunk.get("axis") != "announce_date"
        or chunk.get("universe_id") != universe_id
        or chunk.get("codes") != []
        or not isinstance(chunk.get("dependency_keys"), list)
    ):
        raise BackfillPlanningError(
            "INVALID_FINANCIAL_SEED",
            "financial seed must be a v2 announce-date reducer for the same immutable universe",
        )
    source_key = normalized.get("source_key")
    if not _valid_financial_upstream_source_key(source_key):
        raise BackfillPlanningError(
            "INVALID_FINANCIAL_SEED",
            "financial seed manifest lacks upstream source lineage",
        )
    coverage = normalized.get("coverage")
    if (
        not isinstance(coverage, dict)
        or coverage.get("complete") is not True
        or coverage.get("requested_codes") != codes
    ):
        raise BackfillPlanningError(
            "INVALID_FINANCIAL_SEED",
            "financial seed manifest does not prove the requested universe",
        )
    validation = normalized.get("validation")
    read_back = normalized.get("read_back_result")
    if (
        not isinstance(validation, dict)
        or validation.get("passed") is not True
        or not isinstance(read_back, dict)
        or read_back.get("success") is not True
    ):
        raise BackfillPlanningError(
            "INVALID_FINANCIAL_SEED",
            "financial seed manifest lacks successful validation and read-back",
        )
    reducer = validation.get("financial_reducer")
    terminal = reducer.get("terminal") if isinstance(reducer, dict) else None
    prior_state_checksum = reducer.get("prior_state_checksum") if isinstance(reducer, dict) else None
    reducer_seed = reducer.get("seed") if isinstance(reducer, dict) else None
    if (
        not isinstance(reducer, dict)
        or reducer.get("dependency_keys") != chunk["dependency_keys"]
        or not isinstance(prior_state_checksum, str)
        or re.fullmatch(r"[0-9a-f]{64}", prior_state_checksum) is None
        or not _valid_financial_seed_summary(reducer_seed, codes=codes)
        or not isinstance(terminal, dict)
        or terminal.get("required") is not True
        or terminal.get("passed") is not True
        or isinstance(terminal.get("pending_row_count"), bool)
        or terminal.get("pending_row_count") != 0
    ):
        raise BackfillPlanningError(
            "INVALID_FINANCIAL_SEED",
            "financial seed manifest lacks a complete final v2 reducer proof",
        )
    terminal_date = terminal.get("last_materialized_trade_date")
    try:
        terminal_date = validate_date_range(str(terminal_date), str(terminal_date))[0]
    except (TypeError, ValueError) as exc:
        raise BackfillPlanningError(
            "INVALID_FINANCIAL_SEED",
            "financial seed terminal materialization date is invalid",
        ) from exc
    terminal_anchor = _normalize_financial_terminal_anchor(terminal.get("anchor"))
    if (
        terminal_anchor is None
        or terminal_anchor["trade_date"] != terminal_date
        or terminal.get("state_checksum") != terminal_anchor["checksum"]
        or terminal_date >= start_date
    ):
        raise BackfillPlanningError(
            "INVALID_FINANCIAL_SEED",
            "financial seed terminal anchor does not prove the predecessor state",
        )
    coverage_dates = coverage.get("canonical_trade_dates")
    try:
        normalized_coverage_dates = [
            validate_date_range(str(value), str(value))[0]
            for value in coverage_dates
        ]
    except (TypeError, ValueError) as exc:
        raise BackfillPlanningError(
            "INVALID_FINANCIAL_SEED",
            "financial seed canonical coverage dates are invalid",
        ) from exc
    if (
        not isinstance(coverage_dates, list)
        or normalized_coverage_dates != coverage_dates
        or normalized_coverage_dates != sorted(set(normalized_coverage_dates))
        or (
            normalized_coverage_dates
            and terminal_date != normalized_coverage_dates[-1]
        )
    ):
        raise BackfillPlanningError(
            "INVALID_FINANCIAL_SEED",
            "financial seed terminal proof does not match canonical coverage",
        )
    canonical_checksums = normalized.get("canonical_checksums")
    canonical_keys = normalized.get("canonical_keys")
    partitions = read_back.get("partitions")
    if (
        not isinstance(canonical_checksums, dict)
        or not isinstance(canonical_keys, list)
        or not isinstance(partitions, list)
    ):
        raise BackfillPlanningError(
            "INVALID_FINANCIAL_SEED",
            "financial seed canonical evidence is incomplete",
        )
    candidates: list[tuple[str, dict[str, Any]]] = []
    prefix = "raw/financial/trade_date="
    suffix = "/part.parquet"
    for record in partitions:
        if not isinstance(record, dict):
            continue
        object_key = record.get("object_key")
        if not isinstance(object_key, str) or not object_key.startswith(prefix) or not object_key.endswith(suffix):
            continue
        trade_date = object_key[len(prefix) : -len(suffix)]
        try:
            trade_date = validate_date_range(trade_date, trade_date)[0]
        except (TypeError, ValueError):
            continue
        checksum = record.get("checksum")
        if (
            trade_date >= start_date
            or object_key != build_partition("financial", trade_date).object_key
            or object_key not in canonical_keys
            or canonical_checksums.get(object_key) != checksum
            or not isinstance(checksum, str)
            or not re.fullmatch(r"[0-9a-f]{64}", checksum)
            or isinstance(record.get("row_count"), bool)
            or not isinstance(record.get("row_count"), int)
            or record["row_count"] < 0
            or record.get("materialized") is not True
            or record.get("exact_read_back_success") is not True
        ):
            continue
        candidates.append((trade_date, deepcopy(record)))
    if normalized_coverage_dates and not candidates:
        raise BackfillPlanningError(
            "INVALID_FINANCIAL_SEED",
            "financial seed manifest has no verified predecessor partition",
        )
    if normalized_coverage_dates:
        seed_trade_date, seed_record = max(candidates, key=lambda value: value[0])
        if (
            seed_trade_date != terminal_date
            or seed_record["object_key"] != terminal_anchor["object_key"]
            or seed_record["checksum"] != terminal_anchor["checksum"]
            or seed_record["row_count"] != terminal_anchor["row_count"]
        ):
            raise BackfillPlanningError(
                "INVALID_FINANCIAL_SEED",
                "financial seed predecessor is not the final verified v2 materialization",
            )
    elif canonical_keys != [] or canonical_checksums != {} or partitions != []:
        raise BackfillPlanningError(
            "INVALID_FINANCIAL_SEED",
            "zero-partition financial seed tail has contradictory canonical evidence",
        )
    return {
        "manifest": normalized,
        "manifest_fingerprint": _stable_hash(normalized),
        "trade_date": terminal_anchor["trade_date"],
        "object_key": terminal_anchor["object_key"],
        "checksum": terminal_anchor["checksum"],
        "row_count": terminal_anchor["row_count"],
        "coverage_codes": list(codes),
        "source_key": source_key,
    }


def _normalize_financial_terminal_anchor(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    trade_date = value.get("trade_date")
    object_key = value.get("object_key")
    checksum = value.get("checksum")
    row_count = value.get("row_count")
    try:
        normalized_date = validate_date_range(str(trade_date), str(trade_date))[0]
    except (TypeError, ValueError):
        return None
    if (
        object_key != build_partition("financial", normalized_date).object_key
        or not isinstance(checksum, str)
        or re.fullmatch(r"[0-9a-f]{64}", checksum) is None
        or isinstance(row_count, bool)
        or not isinstance(row_count, int)
        or row_count < 0
        or value.get("exact_read_back_success") is not True
    ):
        return None
    return {
        "trade_date": normalized_date,
        "object_key": object_key,
        "checksum": checksum,
        "row_count": row_count,
        "exact_read_back_success": True,
    }


def _valid_financial_seed_summary(value: Any, *, codes: list[str]) -> bool:
    if not isinstance(value, dict):
        return False
    trade_date = value.get("trade_date")
    object_key = value.get("object_key")
    checksum = value.get("checksum")
    row_count = value.get("row_count")
    source_key = value.get("source_key")
    fingerprint = value.get("source_manifest_fingerprint")
    try:
        normalized_date = validate_date_range(str(trade_date), str(trade_date))[0]
    except (TypeError, ValueError):
        return False
    return bool(
        value.get("mode") == "COMPLETED_PREDECESSOR_MANIFEST"
        and object_key == build_partition("financial", normalized_date).object_key
        and isinstance(checksum, str)
        and re.fullmatch(r"[0-9a-f]{64}", checksum)
        and not isinstance(row_count, bool)
        and isinstance(row_count, int)
        and row_count >= 0
        and value.get("coverage_codes") == codes
        and _valid_financial_upstream_source_key(source_key)
        and isinstance(fingerprint, str)
        and re.fullmatch(r"[0-9a-f]{64}", fingerprint)
        and value.get("exact_read_back_success") is True
    )


def _valid_financial_upstream_source_key(value: Any) -> bool:
    """Accept only durable upstream lineage for cross-run financial state."""

    if not isinstance(value, str) or not value:
        return False
    try:
        safe_object_key(value)
    except ValueError:
        return False
    normalized = value.casefold()
    return not normalized.startswith(("smoke/", "candidate/"))


def _build_chunk_v2(
    *,
    dataset: str,
    codes: list[str],
    universe_id: str,
    start_date: str,
    end_date: str,
    axis: str,
    materialization_id: str,
    index_codes: list[str] | None = None,
    dependency_keys: list[str] | None = None,
) -> dict[str, Any]:
    chunk: dict[str, Any] = {
        "chunk_schema_version": "goal21.history_backfill_chunk.v2",
        "dataset": dataset,
        "strategy": DATASET_STRATEGIES_V2[dataset],
        "axis": axis,
        "key_columns": list(KEY_COLUMNS[dataset]),
        "codes": list(codes),
        "universe_id": universe_id,
        "start_date": start_date,
        "end_date": end_date,
        "materialization_id": materialization_id,
        "dependency_keys": list(dependency_keys or []),
    }
    if dataset == "financial":
        chunk["announce_date_start"] = start_date
        chunk["announce_date_end"] = end_date
    if dataset == "benchmark_price":
        chunk["index_codes"] = list(index_codes or BENCHMARK_INDEXES)
    chunk["chunk_id"] = _chunk_id_v2(dataset, chunk)
    return chunk


def _chunk_id_v2(dataset: str, semantics: dict[str, Any]) -> str:
    identity = {
        "identity_schema_version": IDENTITY_SCHEMA_VERSION_V2,
        "planner_version": PLANNER_VERSION_V2,
        "chunk": semantics,
    }
    return f"{dataset}-v2-{_stable_hash(identity)[:20]}"


def _v2_materialization_id(dataset: str, start_date: str, end_date: str) -> str:
    return f"{dataset}-materialize-{_stable_hash([dataset, start_date, end_date])[:20]}"


def _weekday_count(start_date: str, end_date: str) -> int:
    start = np.datetime64(start_date, "D")
    # busday_count excludes the upper bound, so add one day for an inclusive
    # conservative weekday estimate. Exchange holidays only lower this value.
    end_exclusive = np.datetime64(end_date, "D") + np.timedelta64(1, "D")
    return int(np.busday_count(start, end_exclusive))


def build_history_backfill_output_keys(run_id: str, chunks: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Build safe Goal 21 control and immutable attempt artifact keys."""

    run_id = _validate_run_id(run_id)
    root_prefix = f"candidate/real_history_backfill/run_id={run_id}/"
    chunk_keys: dict[str, dict[str, str]] = {}
    for chunk in chunks:
        if not isinstance(chunk, dict):
            raise BackfillPlanningError("INVALID_CHUNK", "chunks must contain dictionaries")
        dataset = str(chunk.get("dataset", ""))
        if dataset not in REQUIRED_INPUTS:
            raise BackfillPlanningError("UNSUPPORTED_DATASET", f"unsupported dataset: {dataset}")
        chunk_id = str(chunk.get("chunk_id", ""))
        if not _CHUNK_ID_PATTERN.fullmatch(chunk_id):
            raise BackfillPlanningError("INVALID_CHUNK_ID", f"unsafe chunk_id: {chunk_id}")
        if chunk_id in chunk_keys:
            raise BackfillPlanningError("DUPLICATE_CHUNK_ID", f"duplicate chunk_id: {chunk_id}")
        semantics, expected_chunk_id = _validated_chunk_identity(chunk)
        if chunk_id != expected_chunk_id:
            raise BackfillPlanningError(
                "TAMPERED_CHUNK_ID",
                f"chunk_id does not match chunk semantics: {chunk_id}",
            )
        chunk_prefix = f"{root_prefix}dataset={dataset}/chunk_id={chunk_id}/"
        chunk_keys[chunk_id] = {
            "manifest": f"{chunk_prefix}manifest.json",
            "attempt_report_template": f"{chunk_prefix}attempt={{attempt}}/report.json",
            "staging_template": f"{chunk_prefix}attempt={{attempt}}/part.parquet",
        }
    return {
        "root_prefix": root_prefix,
        "plan": f"{root_prefix}plan.json",
        "root_manifest": f"{root_prefix}manifest.json",
        "chunks": chunk_keys,
    }


def run_real_history_backfill(
    *,
    plan: dict[str, Any],
    artifact_read_json_fn: Callable[[str], dict[str, Any]],
    artifact_write_json_fn: Callable[[str, dict[str, Any]], Any],
    artifact_read_parquet_fn: Callable[[str], pd.DataFrame],
    artifact_write_parquet_fn: Callable[[str, pd.DataFrame], Any],
    fetch_chunk_fn: Callable[[dict[str, Any]], Any] | None = None,
    canonical_read_fn: Callable[[str, str], pd.DataFrame | None] | None = None,
    canonical_write_fn: Callable[[str, str, pd.DataFrame], Any] | None = None,
    provider_call_enabled: bool = False,
    apply_standard_write: bool = False,
    resume: bool = True,
    force: bool = False,
    generated_at_fn: Callable[[], str] | None = None,
) -> dict[str, Any]:
    """Execute an immutable Goal 21 plan through the resumable engine."""

    from stock_selector.data.historical_backfill_executor import execute_history_backfill

    return execute_history_backfill(
        plan=plan,
        artifact_read_json_fn=artifact_read_json_fn,
        artifact_write_json_fn=artifact_write_json_fn,
        artifact_read_parquet_fn=artifact_read_parquet_fn,
        artifact_write_parquet_fn=artifact_write_parquet_fn,
        fetch_chunk_fn=fetch_chunk_fn,
        canonical_read_fn=canonical_read_fn,
        canonical_write_fn=canonical_write_fn,
        provider_call_enabled=provider_call_enabled,
        apply_standard_write=apply_standard_write,
        resume=resume,
        force=force,
        generated_at_fn=generated_at_fn,
    )


def _build_chunk(
    *,
    dataset: str,
    codes: list[str],
    start_date: str,
    end_date: str,
    index_codes: list[str] | None = None,
) -> dict[str, Any]:
    chunk: dict[str, Any] = {
        "dataset": dataset,
        "strategy": DATASET_STRATEGIES[dataset],
        "key_columns": list(KEY_COLUMNS[dataset]),
        "codes": list(codes),
        "start_date": start_date,
        "end_date": end_date,
    }
    if dataset == "financial":
        chunk["report_period_start"] = start_date
        chunk["report_period_end"] = end_date
    if dataset == "benchmark_price":
        chunk["index_codes"] = list(index_codes or BENCHMARK_INDEXES)

    chunk["chunk_id"] = _chunk_id(dataset, chunk)
    return chunk


def _chunk_id(dataset: str, semantics: dict[str, Any]) -> str:
    identity = {
        "identity_schema_version": IDENTITY_SCHEMA_VERSION,
        "planner_version": PLANNER_VERSION,
        "chunk": semantics,
    }
    return f"{dataset}-{_stable_hash(identity)[:20]}"


def _validated_chunk_identity(chunk: dict[str, Any]) -> tuple[dict[str, Any], str]:
    if chunk.get("chunk_schema_version") == "goal21.history_backfill_chunk.v2":
        semantics = _validated_chunk_semantics_v2(chunk)
        return semantics, _chunk_id_v2(str(chunk["dataset"]), semantics)
    semantics = _validated_chunk_semantics(chunk)
    return semantics, _chunk_id(str(chunk["dataset"]), semantics)


def _validated_chunk_semantics_v2(chunk: dict[str, Any]) -> dict[str, Any]:
    dataset = str(chunk.get("dataset", ""))
    if dataset not in REQUIRED_INPUTS:
        raise BackfillPlanningError("UNSUPPORTED_DATASET", f"unsupported dataset: {dataset}")
    required_fields = {
        "chunk_schema_version",
        "dataset",
        "strategy",
        "axis",
        "key_columns",
        "codes",
        "universe_id",
        "start_date",
        "end_date",
        "materialization_id",
        "dependency_keys",
        "chunk_id",
    }
    if dataset == "financial":
        required_fields.update({"announce_date_start", "announce_date_end"})
    if dataset == "benchmark_price":
        required_fields.add("index_codes")
    if set(chunk) != required_fields:
        raise BackfillPlanningError("TAMPERED_CHUNK_ID", "v2 chunk fields do not match its dataset")
    if chunk["strategy"] != DATASET_STRATEGIES_V2[dataset]:
        raise BackfillPlanningError("TAMPERED_CHUNK_ID", "v2 chunk strategy is invalid")
    expected_axes = {
        "financial": "announce_date",
        "stock_basic": "historical_scope",
        "st_history": "historical_scope",
    }
    if chunk["axis"] != expected_axes.get(dataset, "trade_date"):
        raise BackfillPlanningError("TAMPERED_CHUNK_ID", "v2 chunk axis is invalid")
    if chunk["key_columns"] != list(KEY_COLUMNS[dataset]):
        raise BackfillPlanningError("TAMPERED_CHUNK_ID", "v2 chunk key columns are invalid")
    try:
        start_date, end_date = validate_date_range(chunk["start_date"], chunk["end_date"])
    except (TypeError, ValueError) as exc:
        raise BackfillPlanningError("TAMPERED_CHUNK_ID", "v2 chunk date range is invalid") from exc
    if start_date != chunk["start_date"] or end_date != chunk["end_date"]:
        raise BackfillPlanningError("TAMPERED_CHUNK_ID", "v2 chunk dates are not normalized")
    codes = chunk["codes"]
    if not isinstance(codes, list):
        raise BackfillPlanningError("TAMPERED_CHUNK_ID", "v2 chunk codes must be a list")
    try:
        normalized_codes = sorted({validate_stock_code(str(value)) for value in codes})
    except ValueError as exc:
        raise BackfillPlanningError("TAMPERED_CHUNK_ID", "v2 chunk codes are invalid") from exc
    if codes != normalized_codes:
        raise BackfillPlanningError("TAMPERED_CHUNK_ID", "v2 chunk codes are not normalized")
    if codes:
        raise BackfillPlanningError("TAMPERED_CHUNK_ID", "v2 chunks must reference the plan universe")
    universe_id = chunk["universe_id"]
    if not isinstance(universe_id, str) or not re.fullmatch(r"[0-9a-f]{64}", universe_id):
        raise BackfillPlanningError("TAMPERED_CHUNK_ID", "v2 universe identity is invalid")
    materialization_id = chunk["materialization_id"]
    if not isinstance(materialization_id, str) or not _CHUNK_ID_PATTERN.fullmatch(materialization_id):
        raise BackfillPlanningError("TAMPERED_CHUNK_ID", "v2 materialization identity is invalid")
    dependency_keys = chunk["dependency_keys"]
    if not isinstance(dependency_keys, list) or len(dependency_keys) != len(set(dependency_keys)):
        raise BackfillPlanningError("TAMPERED_CHUNK_ID", "v2 dependency keys are not canonical")
    if any(not isinstance(value, str) or not _CHUNK_ID_PATTERN.fullmatch(value) for value in dependency_keys):
        raise BackfillPlanningError("TAMPERED_CHUNK_ID", "v2 dependency key is invalid")
    if dataset == "financial":
        if len(dependency_keys) > 1:
            raise BackfillPlanningError("TAMPERED_CHUNK_ID", "financial chunks have at most one dependency")
    elif dependency_keys:
        raise BackfillPlanningError("TAMPERED_CHUNK_ID", "non-financial dependency keys must be empty")
    if dataset == "financial" and (
        chunk["announce_date_start"] != start_date or chunk["announce_date_end"] != end_date
    ):
        raise BackfillPlanningError("TAMPERED_CHUNK_ID", "financial announcement scope is invalid")
    if dataset == "benchmark_price":
        if chunk["index_codes"] != list(BENCHMARK_INDEXES):
            raise BackfillPlanningError("TAMPERED_CHUNK_ID", "benchmark index coverage is invalid")
    return {key: deepcopy(value) for key, value in chunk.items() if key != "chunk_id"}


def _validated_chunk_semantics(chunk: dict[str, Any]) -> dict[str, Any]:
    dataset = str(chunk["dataset"])
    if dataset not in REQUIRED_INPUTS:
        raise BackfillPlanningError("UNSUPPORTED_DATASET", f"unsupported dataset: {dataset}")
    required_fields = {
        "dataset",
        "strategy",
        "key_columns",
        "codes",
        "start_date",
        "end_date",
        "chunk_id",
    }
    if dataset == "financial":
        required_fields.update({"report_period_start", "report_period_end"})
    if dataset == "benchmark_price":
        required_fields.add("index_codes")
    if set(chunk) != required_fields:
        raise BackfillPlanningError(
            "TAMPERED_CHUNK_ID",
            "chunk fields do not match the dataset identity contract",
        )
    if chunk["strategy"] != DATASET_STRATEGIES[dataset]:
        raise BackfillPlanningError("TAMPERED_CHUNK_ID", "chunk strategy does not match dataset")
    if not isinstance(chunk["key_columns"], list) or chunk["key_columns"] != list(KEY_COLUMNS[dataset]):
        raise BackfillPlanningError("TAMPERED_CHUNK_ID", "chunk key columns do not match dataset")
    try:
        start_date, end_date = validate_date_range(chunk["start_date"], chunk["end_date"])
    except (TypeError, ValueError) as exc:
        raise BackfillPlanningError("TAMPERED_CHUNK_ID", "chunk date range is invalid") from exc
    if start_date != chunk["start_date"] or end_date != chunk["end_date"]:
        raise BackfillPlanningError("TAMPERED_CHUNK_ID", "chunk date range is not normalized")
    codes = chunk["codes"]
    if not isinstance(codes, list):
        raise BackfillPlanningError("TAMPERED_CHUNK_ID", "chunk codes must be a list")
    try:
        normalized_codes = sorted({validate_stock_code(str(code).strip().upper()) for code in codes})
    except ValueError as exc:
        raise BackfillPlanningError("TAMPERED_CHUNK_ID", "chunk codes are invalid") from exc
    if codes != normalized_codes:
        raise BackfillPlanningError("TAMPERED_CHUNK_ID", "chunk codes are not normalized")
    if dataset == "benchmark_price":
        if codes:
            raise BackfillPlanningError("TAMPERED_CHUNK_ID", "benchmark chunks cannot contain stock codes")
        if not isinstance(chunk["index_codes"], list) or chunk["index_codes"] != list(BENCHMARK_INDEXES):
            raise BackfillPlanningError("TAMPERED_CHUNK_ID", "benchmark index coverage is invalid")
    elif not codes:
        raise BackfillPlanningError("TAMPERED_CHUNK_ID", "chunk codes must not be empty")
    if dataset == "financial" and (
        chunk["report_period_start"] != start_date or chunk["report_period_end"] != end_date
    ):
        raise BackfillPlanningError("TAMPERED_CHUNK_ID", "financial report period does not match chunk range")
    return {key: value for key, value in chunk.items() if key != "chunk_id"}


def _normalize_universe(
    *,
    codes: Iterable[str] | None,
    universe_frame: pd.DataFrame | None,
    universe_key: str | None,
) -> tuple[list[str], str, str | None]:
    if (codes is None) == (universe_frame is None):
        raise BackfillPlanningError("INVALID_SCOPE", "exactly one of codes or universe_frame is required")

    if universe_frame is not None:
        if not isinstance(universe_frame, pd.DataFrame):
            raise BackfillPlanningError(
                "INVALID_UNIVERSE_FRAME",
                "universe_frame must be a pandas DataFrame",
            )
        if list(universe_frame.columns).count("stock_code") != 1:
            raise BackfillPlanningError(
                "INVALID_UNIVERSE_FRAME",
                "universe_frame must contain exactly one stock_code column",
            )
        raw_codes: Iterable[str] = universe_frame["stock_code"].tolist()
        normalized_key = _normalize_universe_key(universe_key)
        source = "universe_frame"
    else:
        if universe_key is not None:
            raise BackfillPlanningError("INVALID_SCOPE", "universe_key requires universe_frame")
        raw_codes = codes if codes is not None else []
        normalized_key = None
        source = "codes"

    try:
        normalized = sorted({validate_stock_code(str(code).strip().upper()) for code in raw_codes})
    except (TypeError, ValueError) as exc:
        raise BackfillPlanningError("INVALID_STOCK_CODE", str(exc)) from exc
    if not normalized:
        raise BackfillPlanningError("EMPTY_UNIVERSE", "normalized codes must not be empty")
    return normalized, source, normalized_key


def _normalize_universe_key(universe_key: str | None) -> str | None:
    if universe_key is None:
        return None
    value = str(universe_key).strip()
    segments = value.split("/")
    try:
        safe_object_key(value)
    except ValueError as exc:
        raise BackfillPlanningError("UNSAFE_UNIVERSE_KEY", f"unsafe universe_key: {universe_key}") from exc
    if re.match(r"^[A-Za-z]:", value) or any(segment in {"", ".", ".."} for segment in segments):
        raise BackfillPlanningError("UNSAFE_UNIVERSE_KEY", f"unsafe universe_key: {universe_key}")
    return value


def _normalize_datasets(datasets: Iterable[str] | None) -> list[str]:
    if datasets is None:
        return list(REQUIRED_INPUTS)
    requested = [str(dataset) for dataset in datasets]
    if not requested:
        raise BackfillPlanningError("EMPTY_DATASET_SELECTION", "datasets must not be empty")
    unknown = sorted(set(requested) - set(REQUIRED_INPUTS))
    if unknown:
        raise BackfillPlanningError(
            "UNSUPPORTED_DATASET",
            f"unsupported dataset: {', '.join(unknown)}",
        )
    requested_set = set(requested)
    return [dataset for dataset in REQUIRED_INPUTS if dataset in requested_set]


def _split_codes(codes: list[str], batch_size: int) -> list[list[str]]:
    return [codes[offset : offset + batch_size] for offset in range(0, len(codes), batch_size)]


def _split_date_windows(start_date: str, end_date: str, batch_days: int) -> list[tuple[str, str]]:
    current = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    windows = []
    while current <= end:
        remaining_days = (end - current).days
        window_end = current + timedelta(days=min(batch_days - 1, remaining_days))
        windows.append((current.isoformat(), window_end.isoformat()))
        if window_end == end:
            break
        current = window_end + timedelta(days=1)
    return windows


def _split_report_period_windows(start_date: str, end_date: str, months: int) -> list[tuple[str, str]]:
    anchor = date.fromisoformat(start_date)
    current = anchor
    end = date.fromisoformat(end_date)
    windows = []
    window_index = 1
    end_month_index = end.year * 12 + end.month - 1
    while current <= end:
        month_offset = months * window_index
        next_month_index = anchor.year * 12 + anchor.month - 1 + month_offset
        if next_month_index > end_month_index:
            windows.append((current.isoformat(), end.isoformat()))
            break
        next_start = _add_months(anchor, month_offset)
        if next_start > end:
            windows.append((current.isoformat(), end.isoformat()))
            break
        window_end = min(next_start - timedelta(days=1), end)
        windows.append((current.isoformat(), window_end.isoformat()))
        if window_end == end:
            break
        current = next_start
        window_index += 1
    return windows


def _add_months(value: date, months: int) -> date:
    month_index = value.year * 12 + value.month - 1 + months
    year, zero_based_month = divmod(month_index, 12)
    month = zero_based_month + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _validate_positive_limit(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise BackfillPlanningError("INVALID_LIMIT", f"{name} must be positive")


def _validate_run_id(run_id: str) -> str:
    value = str(run_id or "").strip()
    if not _RUN_ID_PATTERN.fullmatch(value):
        raise BackfillPlanningError("UNSAFE_RUN_ID", f"unsafe run_id: {run_id}")
    return value


def _checksum_column_schema(column: Any, series: pd.Series) -> dict[str, Any]:
    dtype = series.dtype
    schema = {
        "name": _checksum_value(column),
        "dtype": str(dtype),
    }
    if isinstance(dtype, pd.CategoricalDtype):
        schema["categories"] = [_checksum_value(item) for item in dtype.categories.tolist()]
        schema["ordered"] = bool(dtype.ordered)
    return schema


def _checksum_value(value: Any) -> dict[str, Any]:
    if value is None:
        return {"type": "null", "value": None}
    if isinstance(value, dict):
        pairs = [
            {"key": _checksum_value(key), "value": _checksum_value(item)}
            for key, item in value.items()
        ]
        pairs.sort(key=lambda pair: _stable_json(pair["key"]))
        return {"type": "dict", "value": pairs}
    if isinstance(value, (list, tuple)):
        return {"type": type(value).__name__, "value": [_checksum_value(item) for item in value]}
    if isinstance(value, (set, frozenset)):
        items = [_checksum_value(item) for item in value]
        items.sort(key=_stable_json)
        return {"type": type(value).__name__, "value": items}
    if isinstance(value, np.ndarray):
        return {
            "type": "ndarray",
            "dtype": str(value.dtype),
            "shape": list(value.shape),
            "value": _checksum_value(value.tolist()),
        }
    if isinstance(value, pd.Series):
        return {
            "type": "Series",
            "name": _checksum_value(value.name),
            "dtype": str(value.dtype),
            "index": [_checksum_value(item) for item in value.index.tolist()],
            "value": [_checksum_value(item) for item in value.tolist()],
        }
    if isinstance(value, pd.Index):
        return {
            "type": "Index",
            "name": _checksum_value(value.name),
            "dtype": str(value.dtype),
            "value": [_checksum_value(item) for item in value.tolist()],
        }
    try:
        missing = pd.isna(value)
    except (TypeError, ValueError):
        missing = False
    if isinstance(missing, (bool, np.bool_)) and bool(missing):
        return {"type": "missing", "value": None}
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return {"type": "datetime", "value": value.isoformat()}
    if isinstance(value, pd.Timedelta):
        return {"type": "timedelta", "value": value.isoformat()}
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float):
        if value != value:
            return {"type": "float", "value": "NaN"}
        if value == float("inf"):
            return {"type": "float", "value": "Infinity"}
        if value == float("-inf"):
            return {"type": "float", "value": "-Infinity"}
    if isinstance(value, bytes):
        return {"type": "bytes", "value": value.hex()}
    if isinstance(value, (str, int, float, bool)):
        return {"type": type(value).__name__, "value": value}
    raise BackfillPlanningError(
        "UNSUPPORTED_CHECKSUM_VALUE",
        f"unsupported checksum value type: {type(value).__name__}",
    )


def _infer_failure_category(error: BaseException) -> str:
    name = type(error).__name__.lower()
    message = str(error).lower()
    text = f"{name} {message}"
    if (
        "emptyresult" in name
        or "empty_result" in text
        or "empty result" in text
        or "returned no rows" in text
        or "no rows for" in text
    ):
        return "EMPTY_RESULT"
    if (
        "ratelimit" in name
        or "rate_limit" in text
        or "rate limit" in text
        or "frequency limit" in text
        or "429" in text
        or "too many requests" in text
        or "频率" in text
        or "每分钟最多访问" in text
    ):
        return "RATE_LIMITED"
    if (
        isinstance(error, PermissionError)
        or "permission" in text
        or "forbidden" in text
        or "unauthorized" in text
        or re.search(r"\b(?:401|403)\b", text)
        or "权限" in text
        or "没有访问该接口" in text
        or "没有接口" in text
    ):
        return "PERMISSION_DENIED"
    if (
        "schemadrift" in name
        or "schema_drift" in text
        or "schema drift" in text
        or "schema mismatch" in text
        or "missing provider fields" in text
    ):
        return "SCHEMA_DRIFT"
    if "semanticsourceunavailable" in name or "semantic_source_unavailable" in text:
        return "SEMANTIC_SOURCE_UNAVAILABLE"
    if "dqfailed" in name or "dq_failed" in text or "dataquality" in name:
        return "DQ_FAILED"
    if "readback" in text or "read_back" in text:
        return "READBACK_FAILED"
    if "writefailed" in name or "write_failed" in text or "writeerror" in name:
        return "WRITE_FAILED"
    if (
        isinstance(error, (TimeoutError, ConnectionError))
        or "transient" in text
        or "temporarily unavailable" in text
        or "service unavailable" in text
        or re.search(r"\b(?:500|502|503|504)\b", text)
    ):
        return "TRANSIENT_PROVIDER_ERROR"
    return "UNKNOWN"


def _sanitize_exception_type(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9_.-]", "_", str(value))
    return sanitized[:128] or "UnknownError"


def _redact_failure_message(message: str) -> str:
    redacted = _CREDENTIAL_PATTERN.sub(
        lambda match: f"{match.group('prefix')}[REDACTED]",
        str(message),
    )
    return _BEARER_PATTERN.sub("Bearer [REDACTED]", redacted)


def _validate_attempt_count(attempt_count: Any, state: Any) -> None:
    if state not in VALID_CHUNK_STATES:
        raise BackfillPlanningError("INVALID_CHUNK_STATE", f"invalid chunk state: {state}")
    if isinstance(attempt_count, bool) or not isinstance(attempt_count, int) or attempt_count < 0:
        raise BackfillPlanningError("INVALID_ATTEMPT_COUNT", "attempt_count must be a non-negative integer")
    if state != "PENDING" and attempt_count == 0:
        raise BackfillPlanningError("INVALID_ATTEMPT_COUNT", f"{state} requires a positive attempt_count")


def _validate_optional_attempt(name: str, value: Any, attempt_count: int) -> None:
    if value is None:
        return
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise BackfillPlanningError("INVALID_ATTEMPT_COUNT", f"{name} must be a positive integer")
    if value > attempt_count:
        raise BackfillPlanningError("INVALID_ATTEMPT_COUNT", f"{name} cannot exceed attempt_count")


def _normalize_requested_stages(value: Iterable[str] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (str, bytes)):
        raise BackfillPlanningError("INVALID_REQUESTED_STAGES", "requested_stages must be an iterable")
    stages = list(value)
    if any(stage not in {"provider", "apply"} for stage in stages) or len(stages) != len(set(stages)):
        raise BackfillPlanningError("INVALID_REQUESTED_STAGES", "requested_stages are invalid")
    return [stage for stage in ("provider", "apply") if stage in stages]


def _normalize_canonical_keys(value: Iterable[str] | None) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (str, bytes)):
        raise BackfillPlanningError("INVALID_MANIFEST_EVIDENCE", "canonical_keys must be an iterable")
    keys = list(value)
    if any(not isinstance(key, str) or not key for key in keys) or len(keys) != len(set(keys)):
        raise BackfillPlanningError("INVALID_MANIFEST_EVIDENCE", "canonical_keys must be unique object keys")
    for key in keys:
        _validate_evidence_object_key(key, "canonical_keys")
    return sorted(keys)


def _normalize_canonical_checksums(value: dict[str, str] | None) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise BackfillPlanningError("INVALID_MANIFEST_EVIDENCE", "canonical_checksums must be a dictionary")
    if any(not isinstance(key, str) or not key or not isinstance(checksum, str) or not checksum for key, checksum in value.items()):
        raise BackfillPlanningError("INVALID_MANIFEST_EVIDENCE", "canonical_checksums are invalid")
    for key in value:
        _validate_evidence_object_key(key, "canonical_checksums")
    return {key: value[key] for key in sorted(value)}


def _validate_row_count(row_count: Any) -> None:
    if row_count is None:
        return
    if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0:
        raise BackfillPlanningError("INVALID_ROW_COUNT", "row_count must be a non-negative integer")


def _normalize_failure_record(failure: Any) -> Any:
    if failure is None:
        return None
    if isinstance(failure, dict) and not failure:
        return deepcopy(failure)
    if isinstance(failure, BaseException):
        return classify_backfill_failure(failure)
    if not isinstance(failure, dict) or set(failure) != {
        "category",
        "retryable",
        "exception_type",
        "message",
    }:
        raise BackfillPlanningError("INVALID_FAILURE_RECORD", "failure must use the canonical safe record shape")
    category = failure.get("category")
    if category not in FAILURE_CATEGORIES:
        raise BackfillPlanningError("INVALID_FAILURE_RECORD", f"invalid failure category: {category}")
    expected_retryable = category in _RETRYABLE_FAILURES
    if failure.get("retryable") is not expected_retryable:
        raise BackfillPlanningError("INVALID_FAILURE_RECORD", "failure retryability contradicts its category")
    if not isinstance(failure.get("exception_type"), str) or not isinstance(failure.get("message"), str):
        raise BackfillPlanningError("INVALID_FAILURE_RECORD", "failure type and message must be strings")
    return {
        "category": category,
        "retryable": expected_retryable,
        "exception_type": _sanitize_exception_type(failure["exception_type"]),
        "message": _redact_failure_message(failure["message"]),
    }


def _validate_failure_state(state: str, failure: Any) -> None:
    if state in {"PENDING", "RUNNING", "STAGED", "COMPLETED"}:
        if failure not in (None, {}):
            raise BackfillPlanningError("INVALID_FAILURE_STATE", f"{state} cannot persist a failure")
        return
    if not isinstance(failure, dict) or not failure:
        raise BackfillPlanningError("INVALID_FAILURE_STATE", f"{state} requires a failure record")
    category = failure["category"]
    retryable = failure["retryable"]
    if state == "FAILED" and (not retryable or category == "INTERRUPTED"):
        raise BackfillPlanningError("INVALID_FAILURE_STATE", "FAILED requires a retryable failure")
    if state == "BLOCKED" and (retryable or category == "INTERRUPTED"):
        raise BackfillPlanningError("INVALID_FAILURE_STATE", "BLOCKED requires a non-retryable failure")
    if state == "INTERRUPTED" and category != "INTERRUPTED":
        raise BackfillPlanningError("INVALID_FAILURE_STATE", "INTERRUPTED requires interruption evidence")


def _validate_manifest_evidence(
    *,
    chunk: dict[str, Any],
    dataset: str,
    state: Any,
    provider_status: Any,
    row_count: Any,
    actual_schema: Any,
    target_schema: Any,
    dq: Any,
    coverage: Any,
    source_key: Any,
    staging_key: Any,
    staging_checksum: Any,
    canonical_key: Any,
    canonical_checksum: Any,
    canonical_keys: Any,
    canonical_checksums: Any,
    validation: Any,
    write_result: Any,
    read_back_result: Any,
) -> None:
    if state not in VALID_CHUNK_STATES:
        raise BackfillPlanningError("INVALID_CHUNK_STATE", f"invalid chunk state: {state}")
    if state in {"STAGED", "COMPLETED"}:
        provider_kind = _provider_status_kind(provider_status)
        complete_staging_evidence = (
            provider_kind is not None
            and row_count is not None
            and _schema_evidence_valid(actual_schema)
            and _schema_evidence_valid(target_schema)
            and _evidence_succeeded(dq)
            and _coverage_succeeded(coverage)
            and isinstance(source_key, str)
            and bool(source_key)
            and isinstance(staging_key, str)
            and bool(staging_key)
            and isinstance(staging_checksum, str)
            and bool(staging_checksum)
        )
        if not complete_staging_evidence:
            raise BackfillPlanningError(
                "INVALID_MANIFEST_EVIDENCE",
                f"{state} requires complete provider, schema, DQ, coverage, source, and staging evidence",
            )
        _validate_evidence_object_key(source_key, "source_key")
        _validate_evidence_object_key(staging_key, "staging_key")
        if row_count == 0:
            closed_v2_window = (
                chunk.get("chunk_schema_version") == "goal21.history_backfill_chunk.v2"
                and isinstance(coverage, dict)
                and (
                    coverage.get("canonical_trade_dates") == []
                    or coverage.get("financial_announce_window_empty") is True
                )
            )
            if dataset not in {"stock_basic", "st_history"} and not closed_v2_window:
                raise BackfillPlanningError(
                    "INVALID_MANIFEST_EVIDENCE",
                    f"{dataset} cannot use VALID_EMPTY staging evidence",
                )
            if provider_kind != "VALID_EMPTY" or not (
                isinstance(coverage, dict) and coverage.get("valid_empty") is True
            ):
                raise BackfillPlanningError(
                    "INVALID_MANIFEST_EVIDENCE",
                    "zero-row staging requires explicit VALID_EMPTY coverage evidence",
                )
        elif provider_kind == "VALID_EMPTY":
            raise BackfillPlanningError(
                "INVALID_MANIFEST_EVIDENCE",
                "VALID_EMPTY provider status requires zero rows",
            )
    if state == "COMPLETED":
        scalar_canonical_evidence = isinstance(canonical_key, str) and bool(canonical_key) and isinstance(
            canonical_checksum, str
        ) and bool(canonical_checksum)
        plural_canonical_evidence = (
            isinstance(canonical_keys, list)
            and bool(canonical_keys)
            and isinstance(canonical_checksums, dict)
            and set(canonical_checksums) == set(canonical_keys)
        )
        zero_partition_evidence = (
            chunk.get("chunk_schema_version") == "goal21.history_backfill_chunk.v2"
            and isinstance(coverage, dict)
            and coverage.get("canonical_trade_dates") == []
            and canonical_keys == []
            and canonical_checksums == {}
            and _evidence_succeeded(validation)
            and _evidence_succeeded(write_result)
            and _evidence_succeeded(read_back_result)
            and isinstance(write_result.get("partitions"), list)
            and write_result["partitions"] == []
            and isinstance(read_back_result.get("partitions"), list)
            and read_back_result["partitions"] == []
        )
        if scalar_canonical_evidence and plural_canonical_evidence:
            raise BackfillPlanningError(
                "INVALID_MANIFEST_EVIDENCE",
                "canonical scalar and plural evidence are mutually exclusive",
            )
        complete_canonical_evidence = (
            (scalar_canonical_evidence or plural_canonical_evidence or zero_partition_evidence)
            and _evidence_succeeded(validation)
            and _evidence_succeeded(write_result)
            and _evidence_succeeded(read_back_result)
        )
        if not complete_canonical_evidence:
            raise BackfillPlanningError(
                "INVALID_MANIFEST_EVIDENCE",
                "COMPLETED requires successful validation, canonical write, and canonical read-back evidence",
            )
        if scalar_canonical_evidence:
            _validate_evidence_object_key(canonical_key, "canonical_key")
            _validate_canonical_result_consistency(
                canonical_key=canonical_key,
                canonical_checksum=canonical_checksum,
                row_count=row_count,
                write_result=write_result,
                read_back_result=read_back_result,
            )
        if plural_canonical_evidence:
            _validate_plural_canonical_result_consistency(
                canonical_keys=canonical_keys,
                canonical_checksums=canonical_checksums,
                write_result=write_result,
                read_back_result=read_back_result,
            )


def _validate_evidence_object_key(value: str, field: str) -> None:
    try:
        safe_object_key(value)
    except ValueError as exc:
        raise BackfillPlanningError("INVALID_MANIFEST_EVIDENCE", f"unsafe {field}") from exc


def _provider_status_kind(value: Any) -> str | None:
    if isinstance(value, str):
        return value if value in {"FETCHED", "VALID_EMPTY", "SUCCEEDED"} else None
    if not isinstance(value, dict) or not _evidence_succeeded(value):
        return None
    status = value.get("status")
    if status is None:
        return "FETCHED"
    return status if status in {"FETCHED", "VALID_EMPTY", "SUCCEEDED"} else None


def _schema_evidence_valid(value: Any) -> bool:
    return isinstance(value, (list, tuple, dict)) and bool(value)


def _coverage_succeeded(value: Any) -> bool:
    if value is True:
        return True
    if not isinstance(value, dict) or value.get("complete") is not True:
        return False
    return _evidence_flags_consistent(value)


def _validate_canonical_result_consistency(
    *,
    canonical_key: str,
    canonical_checksum: str,
    row_count: int,
    write_result: Any,
    read_back_result: Any,
) -> None:
    for label, result in (("write_result", write_result), ("read_back_result", read_back_result)):
        required = {"success", "object_key", "checksum", "row_count"}
        if not isinstance(result, dict) or not required.issubset(result):
            raise BackfillPlanningError(
                "INVALID_MANIFEST_EVIDENCE",
                f"{label} must contain success, object_key, checksum, and row_count",
            )
        for field in ("object_key", "canonical_key", "key"):
            if field in result and result[field] != canonical_key:
                raise BackfillPlanningError(
                    "INVALID_MANIFEST_EVIDENCE",
                    f"{label}.{field} contradicts canonical_key",
                )
        for field in ("checksum", "canonical_checksum", "read_back_checksum", "expected_checksum"):
            if field in result and result[field] != canonical_checksum:
                raise BackfillPlanningError(
                    "INVALID_MANIFEST_EVIDENCE",
                    f"{label}.{field} contradicts canonical_checksum",
                )
        if "row_count" in result and result["row_count"] != row_count:
            raise BackfillPlanningError(
                "INVALID_MANIFEST_EVIDENCE",
                f"{label}.row_count contradicts row_count",
            )


def _validate_plural_canonical_result_consistency(
    *,
    canonical_keys: list[str],
    canonical_checksums: dict[str, str],
    write_result: Any,
    read_back_result: Any,
) -> None:
    for label, result in (("write_result", write_result), ("read_back_result", read_back_result)):
        if not isinstance(result, dict) or result.get("success") is not True:
            raise BackfillPlanningError("INVALID_MANIFEST_EVIDENCE", f"{label} must be successful")
        partitions = result.get("partitions")
        if not isinstance(partitions, list) or len(partitions) != len(canonical_keys):
            raise BackfillPlanningError(
                "INVALID_MANIFEST_EVIDENCE",
                f"{label}.partitions must cover every canonical key",
            )
        by_key: dict[str, dict[str, Any]] = {}
        for record in partitions:
            if not isinstance(record, dict):
                raise BackfillPlanningError("INVALID_MANIFEST_EVIDENCE", f"{label} partition is invalid")
            object_key = record.get("object_key")
            if object_key in by_key or object_key not in canonical_checksums:
                raise BackfillPlanningError("INVALID_MANIFEST_EVIDENCE", f"{label} partition key is invalid")
            expected = canonical_checksums[object_key]
            observed = record.get("checksum", record.get("canonical_checksum"))
            if observed != expected:
                raise BackfillPlanningError(
                    "INVALID_MANIFEST_EVIDENCE",
                    f"{label} partition checksum contradicts canonical_checksums",
                )
            if record.get("exact_read_back_success") is not True:
                raise BackfillPlanningError(
                    "INVALID_MANIFEST_EVIDENCE",
                    f"{label} partition lacks successful exact read-back evidence",
                )
            row_count = record.get("row_count")
            if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0:
                raise BackfillPlanningError(
                    "INVALID_MANIFEST_EVIDENCE",
                    f"{label} partition row_count is invalid",
                )
            if record.get("materialized") is False and (
                row_count != 0 or record.get("wrote") is not False
            ):
                raise BackfillPlanningError(
                    "INVALID_MANIFEST_EVIDENCE",
                    f"{label} unmaterialized scope evidence is contradictory",
                )
            by_key[object_key] = record
        if set(by_key) != set(canonical_keys):
            raise BackfillPlanningError("INVALID_MANIFEST_EVIDENCE", f"{label} partition coverage is incomplete")


def _validate_persisted_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    if set(manifest) != _CHUNK_MANIFEST_FIELDS:
        raise BackfillPlanningError("INVALID_MANIFEST", "persisted manifest fields do not match schema")
    if manifest.get("schema_version") != CHUNK_MANIFEST_SCHEMA_VERSION:
        raise BackfillPlanningError("INVALID_MANIFEST", "persisted manifest schema version is invalid")
    rebuilt = build_chunk_manifest(
        chunk=manifest["chunk"],
        state=manifest["state"],
        attempt_count=manifest["attempt_count"],
        plan_fingerprint=manifest["plan_fingerprint"],
        requested_stages=manifest["requested_stages"],
        provider_status=manifest["provider_status"],
        row_count=manifest["row_count"],
        actual_schema=manifest["actual_schema"],
        target_schema=manifest["target_schema"],
        dq=manifest["dq"],
        coverage=manifest["coverage"],
        source_key=manifest["source_key"],
        staging_key=manifest["staging_key"],
        staging_checksum=manifest["staging_checksum"],
        staging_attempt=manifest["staging_attempt"],
        canonical_key=manifest["canonical_key"],
        canonical_checksum=manifest["canonical_checksum"],
        canonical_keys=manifest["canonical_keys"],
        canonical_checksums=manifest["canonical_checksums"],
        validation=manifest["validation"],
        write_result=manifest["write_result"],
        read_back_result=manifest["read_back_result"],
        failure=manifest["failure"],
    )
    if rebuilt != manifest:
        raise BackfillPlanningError("INVALID_MANIFEST", "persisted manifest is not canonical or sanitized")
    if manifest["chunk_id"] != manifest["chunk"]["chunk_id"] or manifest["dataset"] != manifest["chunk"]["dataset"]:
        raise BackfillPlanningError("TAMPERED_MANIFEST_SCOPE", "manifest top-level identity contradicts chunk")
    return rebuilt


def _evidence_succeeded(value: Any) -> bool:
    if value is True:
        return True
    if not isinstance(value, dict) or not _evidence_flags_consistent(value):
        return False
    return value.get("success") is True or value.get("passed") is True


def _evidence_flags_consistent(value: dict[str, Any]) -> bool:
    for field in ("success", "passed"):
        if field in value and value[field] is not True:
            return False
    status = str(value.get("status", "")).upper()
    return status not in {"FAILED", "BLOCKED", "ERROR", "INTERRUPTED"}


def _stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _stable_hash(value: Any) -> str:
    return hashlib.sha256(_stable_json(value).encode("utf-8")).hexdigest()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

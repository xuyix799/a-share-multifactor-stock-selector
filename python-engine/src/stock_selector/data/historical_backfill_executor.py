from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Callable, Iterable

import pandas as pd

from stock_selector.data.data_validator import DataValidationError, validate_dataset_frame
from stock_selector.data.historical_backfill import (
    BackfillExecutionError,
    BackfillPlanningError,
    IDENTITY_SCHEMA_VERSION,
    PLAN_SCHEMA_VERSION,
    PLANNER_VERSION,
    build_chunk_manifest,
    build_history_backfill_output_keys,
    classify_backfill_failure,
    dataframe_checksum,
    summarize_chunk_manifests,
    _stable_hash,
    _validate_persisted_manifest,
)
from stock_selector.data.real_clean_inputs_landing import KEY_COLUMNS
from stock_selector.providers.schema_contract import get_schema_contract
from stock_selector.storage.partition import build_partition
from stock_selector.utils.date_validator import validate_trade_date


_MANIFEST_EVIDENCE_FIELDS = (
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
)
_DOWNSTREAM_FIREWALLS = {
    "clean_daily_snapshot": False,
    "factor": False,
    "selection": False,
    "backtest": False,
}
_PARTITION_STRATEGIES = {
    "financial": "FINANCIAL_ANNOUNCE_DATE_AS_OF",
    "st_history": "ST_INTERVAL_HISTORY",
}


def execute_history_backfill(
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
    """Run one immutable plan with durable per-chunk checkpoints."""

    frozen_plan = deepcopy(plan)
    _validate_plan(frozen_plan)
    output_keys = build_history_backfill_output_keys(frozen_plan["run_id"], frozen_plan["chunks"])
    existing_plan = _read_json_optional(artifact_read_json_fn, output_keys["plan"])
    if existing_plan is not None:
        _validate_plan(existing_plan)
        if _plan_identity(existing_plan) != _plan_identity(frozen_plan):
            raise BackfillPlanningError(
                "RUN_PLAN_MISMATCH",
                "the persisted run plan differs from the requested immutable plan",
            )
        # generated_at is observational metadata and intentionally excluded
        # from the stable fingerprint.  Resume from the original persisted
        # plan so a fresh CLI process cannot rewrite that timestamp.
        frozen_plan = deepcopy(existing_plan)
    else:
        artifact_write_json_fn(output_keys["plan"], deepcopy(frozen_plan))

    manifests: dict[str, dict[str, Any]] = {}
    for chunk in frozen_plan["chunks"]:
        key = output_keys["chunks"][chunk["chunk_id"]]["manifest"]
        persisted = _read_json_optional(artifact_read_json_fn, key)
        if persisted is None:
            continue
        normalized = _validate_persisted_manifest(persisted)
        if (
            normalized["chunk_id"] != chunk["chunk_id"]
            or normalized["dataset"] != chunk["dataset"]
            or normalized["chunk"] != chunk
        ):
            raise BackfillPlanningError(
                "TAMPERED_MANIFEST_SCOPE",
                f"manifest does not belong to its planned chunk slot: {chunk['chunk_id']}",
            )
        persisted_fingerprint = normalized.get("plan_fingerprint")
        if persisted_fingerprint not in (None, frozen_plan["plan_fingerprint"]):
            raise BackfillPlanningError(
                "RUN_PLAN_MISMATCH",
                f"chunk manifest belongs to another plan: {chunk['chunk_id']}",
            )
        manifests[chunk["chunk_id"]] = normalized

    attempted: list[str] = []
    skipped: list[str] = []
    reconciled: list[str] = []
    requested_stages = [
        stage
        for stage, enabled in (
            ("provider", provider_call_enabled),
            ("apply", apply_standard_write),
        )
        if enabled
    ]
    now = generated_at_fn or _utc_now_iso

    if provider_call_enabled or apply_standard_write:
        for chunk in frozen_plan["chunks"]:
            chunk_id = chunk["chunk_id"]
            chunk_keys = output_keys["chunks"][chunk_id]
            existing = manifests.get(chunk_id)
            staging_frame = None
            if existing is not None and resume and not force:
                staging_frame = _read_verified_staging(
                    existing,
                    artifact_read_parquet_fn,
                    chunk["dataset"],
                )
                if staging_frame is not None and existing["state"] == "COMPLETED":
                    if not apply_standard_write:
                        skipped.append(chunk_id)
                        continue
                    if _canonical_scope_is_exact(
                        chunk,
                        staging_frame,
                        existing,
                        canonical_read_fn,
                        frozen_plan["scope"],
                    ):
                        skipped.append(chunk_id)
                        continue
                if (
                    staging_frame is not None
                    and existing["state"] == "STAGED"
                    and not apply_standard_write
                ):
                    skipped.append(chunk_id)
                    continue

            attempt = (existing["attempt_count"] if existing is not None else 0) + 1
            do_fetch = bool(provider_call_enabled)
            if (
                do_fetch
                and resume
                and not force
                and staging_frame is not None
                and existing is not None
                and existing["state"] in {"RUNNING", "INTERRUPTED", "STAGED", "COMPLETED"}
            ):
                do_fetch = False

            carried = _manifest_evidence(existing)
            running = _build_manifest(
                chunk=chunk,
                state="RUNNING",
                attempt=attempt,
                plan_fingerprint=frozen_plan["plan_fingerprint"],
                requested_stages=requested_stages,
                evidence=carried,
            )
            # This checkpoint deliberately sits outside the isolation handler:
            # no provider or canonical side effect is allowed without it.
            artifact_write_json_fn(chunk_keys["manifest"], running)
            attempted.append(chunk_id)

            evidence = carried
            source_keys = tuple(
                [carried["source_key"]] if isinstance(carried.get("source_key"), str) and carried["source_key"] else []
            )
            provider_calls: tuple[dict[str, Any], ...] = ()
            report_key = chunk_keys["attempt_report_template"].format(attempt=attempt)
            try:
                _assert_immutable_report_slot(report_key, artifact_read_json_fn)
                if do_fetch:
                    if fetch_chunk_fn is None:
                        raise BackfillExecutionError(
                            "CONFIGURATION_ERROR",
                            "provider-call was enabled without a fetch callback",
                        )
                    result = fetch_chunk_fn(deepcopy(chunk))
                    _validate_fetch_result(result, chunk, frozen_plan["scope"])
                    if result.provider_status in {"FAILED", "BLOCKED"}:
                        failure = result.failure or {
                            "category": "UNKNOWN",
                            "message": "provider returned a failed result",
                        }
                        raise BackfillExecutionError(
                            str(failure.get("category", "UNKNOWN")),
                            str(failure.get("message", "provider returned a failed result")),
                        )
                    staging_frame = result.frame.copy(deep=True)
                    _validate_staging_scope(chunk, staging_frame)
                    source_keys = tuple(result.source_keys)
                    provider_calls = tuple(deepcopy(result.provider_calls))
                    staging_key = chunk_keys["staging_template"].format(attempt=attempt)
                    staging_checksum = dataframe_checksum(
                        staging_frame,
                        key_columns=KEY_COLUMNS[chunk["dataset"]],
                    )
                    artifact_write_parquet_fn(staging_key, staging_frame.copy(deep=True))
                    read_back = artifact_read_parquet_fn(staging_key)
                    _assert_frame_checksum(
                        read_back,
                        staging_checksum,
                        chunk["dataset"],
                        "staging read-back",
                    )
                    evidence = {
                        "provider_status": result.provider_status,
                        "row_count": len(staging_frame),
                        "actual_schema": list(result.actual_schema),
                        "target_schema": list(result.target_schema),
                        "dq": deepcopy(result.dq),
                        "coverage": deepcopy(result.coverage),
                        "source_key": source_keys[0],
                        "staging_key": staging_key,
                        "staging_checksum": staging_checksum,
                        "staging_attempt": attempt,
                        "canonical_key": None,
                        "canonical_checksum": None,
                        "canonical_keys": [],
                        "canonical_checksums": {},
                        "validation": deepcopy(result.validation),
                        "write_result": None,
                        "read_back_result": None,
                    }
                    staging_frame = read_back.copy(deep=True)
                else:
                    if staging_frame is None:
                        staging_frame = _read_verified_staging(
                            existing,
                            artifact_read_parquet_fn,
                            chunk["dataset"],
                        ) if existing is not None else None
                    if staging_frame is None:
                        raise BackfillExecutionError(
                            "CONFIGURATION_ERROR",
                            "verified staging is unavailable and provider-call is disabled",
                        )
                    _validate_staging_scope(chunk, staging_frame)

                if apply_standard_write:
                    if canonical_read_fn is None or canonical_write_fn is None:
                        raise BackfillExecutionError(
                            "CONFIGURATION_ERROR",
                            "apply requires canonical read and write callbacks",
                        )
                    apply_evidence, was_reconciled = _apply_canonical_partitions(
                        chunk=chunk,
                        frame=staging_frame,
                        evidence=evidence,
                        plan_scope=frozen_plan["scope"],
                        canonical_read_fn=canonical_read_fn,
                        canonical_write_fn=canonical_write_fn,
                        always_write=force or not resume,
                    )
                    evidence = dict(evidence, **apply_evidence)
                    final_state = "COMPLETED"
                else:
                    evidence = dict(
                        evidence,
                        canonical_key=None,
                        canonical_checksum=None,
                        canonical_keys=[],
                        canonical_checksums={},
                        write_result=None,
                        read_back_result=None,
                    )
                    final_state = "STAGED"
                    was_reconciled = False

                final_manifest = _build_manifest(
                    chunk=chunk,
                    state=final_state,
                    attempt=attempt,
                    plan_fingerprint=frozen_plan["plan_fingerprint"],
                    requested_stages=requested_stages,
                    evidence=evidence,
                )
                report = _attempt_report(
                    plan=frozen_plan,
                    chunk=chunk,
                    attempt=attempt,
                    requested_stages=requested_stages,
                    state="READY_TO_CHECKPOINT",
                    checkpoint_target_state=final_state,
                    evidence=evidence,
                    source_keys=source_keys,
                    provider_calls=provider_calls,
                    generated_at=now(),
                )
                _write_immutable_report(
                    report_key,
                    report,
                    artifact_read_json_fn,
                    artifact_write_json_fn,
                )
                artifact_write_json_fn(chunk_keys["manifest"], final_manifest)
                manifests[chunk_id] = final_manifest
                if was_reconciled:
                    reconciled.append(chunk_id)
            except KeyboardInterrupt as exc:
                partial_evidence = getattr(exc, "partial_evidence", None)
                if isinstance(partial_evidence, dict):
                    evidence = dict(evidence, **partial_evidence)
                failure = classify_backfill_failure(exc)
                interrupted = _build_manifest(
                    chunk=chunk,
                    state="INTERRUPTED",
                    attempt=attempt,
                    plan_fingerprint=frozen_plan["plan_fingerprint"],
                    requested_stages=requested_stages,
                    evidence=evidence,
                    failure=failure,
                )
                report = _attempt_report(
                    plan=frozen_plan,
                    chunk=chunk,
                    attempt=attempt,
                    requested_stages=requested_stages,
                    state="INTERRUPTED",
                    evidence=evidence,
                    source_keys=source_keys,
                    provider_calls=provider_calls,
                    generated_at=now(),
                    failure=failure,
                )
                try:
                    _write_immutable_report(
                        report_key,
                        report,
                        artifact_read_json_fn,
                        artifact_write_json_fn,
                    )
                except Exception:
                    pass
                artifact_write_json_fn(chunk_keys["manifest"], interrupted)
                manifests[chunk_id] = interrupted
                raise
            except Exception as exc:
                partial_evidence = getattr(exc, "partial_evidence", None)
                if isinstance(partial_evidence, dict):
                    evidence = dict(evidence, **partial_evidence)
                failure = classify_backfill_failure(exc)
                state = "FAILED" if failure["retryable"] else "BLOCKED"
                failed = _build_manifest(
                    chunk=chunk,
                    state=state,
                    attempt=attempt,
                    plan_fingerprint=frozen_plan["plan_fingerprint"],
                    requested_stages=requested_stages,
                    evidence=evidence,
                    failure=failure,
                )
                report = _attempt_report(
                    plan=frozen_plan,
                    chunk=chunk,
                    attempt=attempt,
                    requested_stages=requested_stages,
                    state=state,
                    evidence=evidence,
                    source_keys=source_keys,
                    provider_calls=provider_calls,
                    generated_at=now(),
                    failure=failure,
                )
                try:
                    _write_immutable_report(
                        report_key,
                        report,
                        artifact_read_json_fn,
                        artifact_write_json_fn,
                    )
                except Exception:
                    pass
                artifact_write_json_fn(chunk_keys["manifest"], failed)
                manifests[chunk_id] = failed

    ordered_manifests = [
        manifests[chunk["chunk_id"]]
        for chunk in frozen_plan["chunks"]
        if chunk["chunk_id"] in manifests
    ]
    summary = summarize_chunk_manifests(frozen_plan, ordered_manifests)
    root_manifest = deepcopy(summary)
    artifact_write_json_fn(output_keys["root_manifest"], root_manifest)
    return {
        "goal": "Goal 21 resumable historical backfill",
        "run_id": frozen_plan["run_id"],
        "plan_fingerprint": frozen_plan["plan_fingerprint"],
        "gates": {
            "provider_call_enabled": bool(provider_call_enabled),
            "apply_standard_write": bool(apply_standard_write),
            "resume": bool(resume),
            "force": bool(force),
        },
        "output_keys": deepcopy(output_keys),
        "plan_key": output_keys["plan"],
        "root_manifest_key": output_keys["root_manifest"],
        "root_manifest": root_manifest,
        "summary": summary,
        "attempted_chunk_ids": attempted,
        "skipped_chunk_ids": skipped,
        "reconciled_chunk_ids": reconciled,
        "downstream_firewalls": deepcopy(_DOWNSTREAM_FIREWALLS),
    }


def _validate_plan(plan: dict[str, Any]) -> None:
    if not isinstance(plan, dict) or plan.get("schema_version") != PLAN_SCHEMA_VERSION:
        raise BackfillPlanningError("INVALID_PLAN", "invalid Goal 21 plan schema")
    if plan.get("identity_schema_version") != IDENTITY_SCHEMA_VERSION or plan.get("planner_version") != PLANNER_VERSION:
        raise BackfillPlanningError("INVALID_PLAN", "invalid Goal 21 plan identity version")
    if not isinstance(plan.get("chunks"), list) or plan.get("chunk_count") != len(plan["chunks"]):
        raise BackfillPlanningError("INVALID_PLAN", "plan chunk_count is inconsistent")
    build_history_backfill_output_keys(str(plan.get("run_id", "")), plan["chunks"])
    payload = {
        "identity_schema_version": plan["identity_schema_version"],
        "planner_version": plan["planner_version"],
        "scope": plan.get("scope"),
        "datasets": plan.get("datasets"),
        "limits": plan.get("limits"),
        "strategies": plan.get("strategies"),
        "chunks": plan["chunks"],
    }
    if plan.get("plan_fingerprint") != _stable_hash(payload):
        raise BackfillPlanningError("INVALID_PLAN_FINGERPRINT", "plan fingerprint does not match its contents")


def _plan_identity(plan: dict[str, Any]) -> dict[str, Any]:
    identity = deepcopy(plan)
    identity.pop("generated_at", None)
    return identity


def _read_json_optional(read_fn: Callable[[str], dict[str, Any]], key: str) -> dict[str, Any] | None:
    try:
        value = read_fn(key)
    except FileNotFoundError:
        return None
    if not isinstance(value, dict):
        raise BackfillPlanningError("INVALID_CONTROL_ARTIFACT", f"JSON artifact is not an object: {key}")
    return deepcopy(value)


def _manifest_evidence(manifest: dict[str, Any] | None) -> dict[str, Any]:
    if manifest is None:
        return {}
    return {field: deepcopy(manifest.get(field)) for field in _MANIFEST_EVIDENCE_FIELDS}


def _build_manifest(
    *,
    chunk: dict[str, Any],
    state: str,
    attempt: int,
    plan_fingerprint: str,
    requested_stages: Iterable[str],
    evidence: dict[str, Any],
    failure: Any = None,
) -> dict[str, Any]:
    kwargs = {field: deepcopy(evidence.get(field)) for field in _MANIFEST_EVIDENCE_FIELDS}
    return build_chunk_manifest(
        chunk=chunk,
        state=state,
        attempt_count=attempt,
        plan_fingerprint=plan_fingerprint,
        requested_stages=requested_stages,
        failure=failure,
        **kwargs,
    )


def _read_verified_staging(
    manifest: dict[str, Any],
    read_fn: Callable[[str], pd.DataFrame],
    dataset: str,
) -> pd.DataFrame | None:
    key = manifest.get("staging_key")
    checksum = manifest.get("staging_checksum")
    if not isinstance(key, str) or not key or not isinstance(checksum, str) or not checksum:
        return None
    try:
        frame = read_fn(key)
        _assert_frame_checksum(frame, checksum, dataset, "persisted staging")
    except (FileNotFoundError, BackfillExecutionError, ValueError, TypeError):
        return None
    if manifest.get("row_count") != len(frame):
        return None
    return frame.copy(deep=True)


def _assert_frame_checksum(frame: Any, expected: str, dataset: str, label: str) -> None:
    if not isinstance(frame, pd.DataFrame):
        raise BackfillExecutionError("READBACK_FAILED", f"{label} did not return a DataFrame")
    observed = dataframe_checksum(frame, key_columns=KEY_COLUMNS[dataset])
    if observed != expected:
        raise BackfillExecutionError("READBACK_FAILED", f"{label} checksum mismatch")


def _validate_fetch_result(
    result: Any,
    chunk: dict[str, Any],
    plan_scope: dict[str, Any],
) -> None:
    required = (
        "dataset",
        "chunk_id",
        "frame",
        "provider_status",
        "source_keys",
        "provider_calls",
        "actual_schema",
        "target_schema",
        "dq",
        "coverage",
        "canonical_trade_dates",
        "partition_strategy",
        "validation",
        "failure",
    )
    if any(not hasattr(result, field) for field in required):
        raise BackfillExecutionError("SCHEMA_DRIFT", "fetch result does not implement the historical result contract")
    if result.dataset != chunk["dataset"] or result.chunk_id != chunk["chunk_id"]:
        raise BackfillExecutionError("DQ_FAILED", "fetch result identity does not match the planned chunk")
    if not isinstance(result.frame, pd.DataFrame):
        raise BackfillExecutionError("SCHEMA_DRIFT", "fetch result frame is not a DataFrame")
    if result.provider_status not in {"FETCHED", "VALID_EMPTY", "FAILED", "BLOCKED"}:
        raise BackfillExecutionError("SCHEMA_DRIFT", "fetch result provider status is invalid")
    if result.provider_status in {"FETCHED", "VALID_EMPTY"} and not result.source_keys:
        raise BackfillExecutionError("SEMANTIC_SOURCE_UNAVAILABLE", "successful fetch result has no source lineage")
    expected_strategy = _PARTITION_STRATEGIES.get(chunk["dataset"], "BY_TRADE_DATE_COLUMN")
    if result.partition_strategy != expected_strategy:
        raise BackfillExecutionError("SCHEMA_DRIFT", "fetch result partition strategy is invalid")
    if result.provider_status in {"FETCHED", "VALID_EMPTY"}:
        _validated_canonical_dates(chunk, result.frame, {"coverage": result.coverage}, plan_scope)


def _validate_staging_scope(chunk: dict[str, Any], frame: pd.DataFrame) -> None:
    dataset = chunk["dataset"]
    target = list(get_schema_contract(dataset).columns)
    if list(frame.columns) != target:
        raise BackfillExecutionError("SCHEMA_DRIFT", "staging schema does not match the planned dataset")
    if "stock_code" in frame.columns:
        outside_codes = set(frame["stock_code"].astype(str)) - set(chunk.get("codes", []))
        if outside_codes:
            raise BackfillExecutionError("DQ_FAILED", "staging contains stock codes outside the chunk scope")
    if "index_code" in frame.columns:
        outside_indexes = set(frame["index_code"].astype(str)) - set(chunk.get("index_codes", []))
        if outside_indexes:
            raise BackfillExecutionError("DQ_FAILED", "staging contains indexes outside the chunk scope")
    if "trade_date" in frame.columns and not frame.empty:
        dates = frame["trade_date"].astype(str)
        if ((dates < chunk["start_date"]) | (dates > chunk["end_date"])).any():
            raise BackfillExecutionError("DQ_FAILED", "staging contains trade dates outside the chunk scope")
    if dataset == "financial" and not frame.empty:
        periods = frame["report_period"].astype(str)
        if (
            (periods < chunk["report_period_start"])
            | (periods > chunk["report_period_end"])
        ).any():
            raise BackfillExecutionError("DQ_FAILED", "financial staging contains report periods outside the chunk scope")
    if dataset == "st_history" and not frame.empty:
        starts = frame["start_date"].astype(str)
        ends = frame["end_date"].fillna("9999-12-31").astype(str)
        if ((starts > chunk["end_date"]) | (ends < chunk["start_date"])).any():
            raise BackfillExecutionError("DQ_FAILED", "ST staging contains intervals outside the chunk scope")


def _assert_immutable_report_slot(
    key: str,
    read_fn: Callable[[str], dict[str, Any]],
) -> None:
    existing = _read_json_optional(read_fn, key)
    if existing is not None:
        raise BackfillExecutionError("WRITE_FAILED", "immutable attempt report key already exists")


def _write_immutable_report(
    key: str,
    payload: dict[str, Any],
    read_fn: Callable[[str], dict[str, Any]],
    write_fn: Callable[[str, dict[str, Any]], Any],
    *,
    slot_already_checked: bool = False,
) -> None:
    if not slot_already_checked:
        existing = _read_json_optional(read_fn, key)
        if existing is not None:
            if existing == payload:
                return
            raise BackfillExecutionError("WRITE_FAILED", "immutable attempt report collision")
    write_fn(key, deepcopy(payload))


def _attempt_report(
    *,
    plan: dict[str, Any],
    chunk: dict[str, Any],
    attempt: int,
    requested_stages: Iterable[str],
    state: str,
    evidence: dict[str, Any],
    source_keys: Iterable[str],
    provider_calls: Iterable[dict[str, Any]],
    generated_at: str,
    failure: Any = None,
    checkpoint_target_state: str | None = None,
) -> dict[str, Any]:
    report = {
        "schema_version": "goal21.chunk_attempt_report.v1",
        "run_id": plan["run_id"],
        "plan_fingerprint": plan["plan_fingerprint"],
        "chunk_id": chunk["chunk_id"],
        "dataset": chunk["dataset"],
        "chunk": deepcopy(chunk),
        "attempt": attempt,
        "attempt_count": attempt,
        "requested_stages": list(requested_stages),
        "state": state,
        "checkpoint_target_state": checkpoint_target_state,
        "generated_at": generated_at,
        "source_keys": list(source_keys),
        "provider_calls": deepcopy(list(provider_calls)),
        "failure": deepcopy(failure),
    }
    report.update({field: deepcopy(evidence.get(field)) for field in _MANIFEST_EVIDENCE_FIELDS})
    return report


def _validated_canonical_dates(
    chunk: dict[str, Any],
    frame: pd.DataFrame,
    evidence: dict[str, Any],
    plan_scope: dict[str, Any],
) -> list[str]:
    coverage = evidence.get("coverage")
    if not isinstance(coverage, dict):
        raise BackfillExecutionError("DQ_FAILED", "canonical coverage evidence is missing")
    values = coverage.get("canonical_trade_dates")
    if not isinstance(values, list) or not values:
        raise BackfillExecutionError("DQ_FAILED", "canonical trade-date coverage is missing")
    try:
        dates = [validate_trade_date(str(value)) for value in values]
    except (TypeError, ValueError) as exc:
        raise BackfillExecutionError("DQ_FAILED", "canonical trade-date coverage is invalid") from exc
    if dates != values or dates != sorted(set(dates)):
        raise BackfillExecutionError("DQ_FAILED", "canonical trade dates must be normalized, sorted, and unique")
    scope_start = plan_scope.get("start_date")
    scope_end = plan_scope.get("end_date")
    if not isinstance(scope_start, str) or not isinstance(scope_end, str):
        raise BackfillExecutionError("DQ_FAILED", "immutable plan date scope is invalid")
    if any(value < scope_start or value > scope_end for value in dates):
        raise BackfillExecutionError("DQ_FAILED", "canonical trade dates escape the immutable run scope")

    dataset = chunk["dataset"]
    if _PARTITION_STRATEGIES.get(dataset, "BY_TRADE_DATE_COLUMN") == "BY_TRADE_DATE_COLUMN":
        if frame.empty:
            if dataset not in {"stock_basic"}:
                raise BackfillExecutionError(
                    "DQ_FAILED",
                    f"{dataset} cannot materialize an empty planned canonical partition",
                )
        else:
            observed_dates = sorted(frame["trade_date"].astype(str).unique().tolist())
            if observed_dates != dates:
                raise BackfillExecutionError(
                    "DQ_FAILED",
                    "canonical trade dates do not exactly match staging trade-date coverage",
                )
    return dates


def _project_partitions(
    chunk: dict[str, Any],
    frame: pd.DataFrame,
    evidence: dict[str, Any],
    plan_scope: dict[str, Any],
) -> list[tuple[str, pd.DataFrame]]:
    dataset = chunk["dataset"]
    target = list(get_schema_contract(dataset).columns)
    if list(frame.columns) != target:
        raise BackfillExecutionError("SCHEMA_DRIFT", "staging schema does not match the standard contract")
    partitions = []
    for trade_date in _validated_canonical_dates(chunk, frame, evidence, plan_scope):
        if dataset == "financial":
            projected = frame[
                (frame["report_period"].astype(str) <= trade_date)
                & (frame["announce_date"].astype(str) <= trade_date)
            ]
        elif dataset == "st_history":
            projected = frame
        else:
            projected = frame[frame["trade_date"].astype(str) == trade_date]
        if projected.empty and dataset not in {"financial", "stock_basic", "st_history"}:
            raise BackfillExecutionError(
                "DQ_FAILED",
                f"{dataset} planned canonical partition has no validated rows",
            )
        partitions.append((trade_date, projected.loc[:, target].copy(deep=True).reset_index(drop=True)))
    return partitions


def _canonical_scope_is_exact(
    chunk: dict[str, Any],
    frame: pd.DataFrame,
    evidence: dict[str, Any],
    read_fn: Callable[[str, str], pd.DataFrame | None] | None,
    plan_scope: dict[str, Any],
) -> bool:
    if read_fn is None:
        return False
    dataset = chunk["dataset"]
    keys = KEY_COLUMNS[dataset]
    try:
        for trade_date, incoming in _project_partitions(chunk, frame, evidence, plan_scope):
            _validate_canonical_frame(dataset, incoming, trade_date)
            existing = read_fn(dataset, trade_date)
            if incoming.empty and dataset == "st_history":
                if _st_scope_has_rows(existing, chunk):
                    return False
                continue
            if incoming.duplicated(keys).any():
                return False
            if existing is None:
                if not incoming.empty:
                    return False
                continue
            if not isinstance(existing, pd.DataFrame) or list(existing.columns) != list(incoming.columns):
                return False
            if existing.duplicated(keys).any():
                return False
            _validate_canonical_frame(dataset, existing, trade_date)
            subset = _select_by_keys(existing, incoming, keys)
            if not _frames_equal(subset, incoming, keys):
                return False
    except (FileNotFoundError, ValueError, TypeError, BackfillExecutionError):
        return False
    return True


def _apply_canonical_partitions(
    *,
    chunk: dict[str, Any],
    frame: pd.DataFrame,
    evidence: dict[str, Any],
    plan_scope: dict[str, Any],
    canonical_read_fn: Callable[[str, str], pd.DataFrame | None],
    canonical_write_fn: Callable[[str, str, pd.DataFrame], Any],
    always_write: bool,
) -> tuple[dict[str, Any], bool]:
    dataset = chunk["dataset"]
    keys = KEY_COLUMNS[dataset]
    records: list[dict[str, Any]] = []
    for trade_date, incoming in _project_partitions(chunk, frame, evidence, plan_scope):
        try:
            record = _apply_one_partition(
                chunk=chunk,
                dataset=dataset,
                trade_date=trade_date,
                incoming=incoming,
                keys=keys,
                canonical_read_fn=canonical_read_fn,
                canonical_write_fn=canonical_write_fn,
                always_write=always_write,
            )
        except KeyboardInterrupt as exc:
            record = getattr(exc, "partition_record", None)
            if isinstance(record, dict):
                records.append(deepcopy(record))
            exc.partial_evidence = _partial_apply_evidence(records)
            raise
        except Exception as exc:
            record = getattr(exc, "partition_record", None)
            if isinstance(record, dict):
                records.append(deepcopy(record))
            failure = classify_backfill_failure(exc)
            wrapped = BackfillExecutionError(failure["category"], failure["message"])
            wrapped.partial_evidence = _partial_apply_evidence(records)
            raise wrapped from exc
        records.append(record)

    any_write = any(record.get("wrote") is True for record in records)
    status = "WRITTEN" if any_write else "RECONCILED_EXISTING_WRITE"
    return _successful_apply_evidence(records, status), not any_write


def _apply_one_partition(
    *,
    chunk: dict[str, Any],
    dataset: str,
    trade_date: str,
    incoming: pd.DataFrame,
    keys: list[str],
    canonical_read_fn: Callable[[str, str], pd.DataFrame | None],
    canonical_write_fn: Callable[[str, str, pd.DataFrame], Any],
    always_write: bool,
) -> dict[str, Any]:
    object_key = build_partition(dataset, trade_date).object_key
    incoming = _sorted_frame(incoming, keys)
    incoming_checksum = dataframe_checksum(incoming, key_columns=keys)
    write_attempted = False
    write_confirmed = False
    try:
        if incoming.duplicated(keys).any():
            raise BackfillExecutionError("DQ_FAILED", "incoming canonical keys are duplicated")
        _validate_canonical_frame(dataset, incoming, trade_date)
        existing = canonical_read_fn(dataset, trade_date)

        if incoming.empty and dataset == "st_history":
            if _st_scope_has_rows(existing, chunk):
                raise BackfillExecutionError(
                    "READBACK_FAILED",
                    "proven empty ST scope contradicts existing canonical history",
                )
            return _empty_partition_record(object_key, incoming_checksum)

        if incoming.empty:
            if dataset == "stock_basic" and _stock_scope_has_rows(existing, chunk):
                raise BackfillExecutionError(
                    "READBACK_FAILED",
                    "proven non-member stock scope contradicts existing canonical membership",
                )
            return _empty_partition_record(object_key, incoming_checksum)

        if existing is None:
            existing = pd.DataFrame(columns=incoming.columns)
        if not isinstance(existing, pd.DataFrame) or list(existing.columns) != list(incoming.columns):
            raise BackfillExecutionError("SCHEMA_DRIFT", "canonical schema does not match staging")
        if existing.duplicated(keys).any():
            raise BackfillExecutionError("DQ_FAILED", "existing canonical keys are duplicated")
        existing = _sorted_frame(existing, keys)
        inserted, updated, unchanged = _upsert_counts(existing, incoming, keys)
        merged = _upsert_frame(existing, incoming, keys)
        _validate_canonical_frame(dataset, merged, trade_date)
        current_subset = _select_by_keys(existing, incoming, keys)
        exact_before_write = _frames_equal(current_subset, incoming, keys)
        wrote = always_write or not exact_before_write
        if wrote:
            write_attempted = True
            canonical_write_fn(dataset, trade_date, merged.copy(deep=True))
            write_confirmed = True
            read_back = canonical_read_fn(dataset, trade_date)
            if read_back is None or not isinstance(read_back, pd.DataFrame):
                raise BackfillExecutionError("READBACK_FAILED", "canonical read-back is missing")
            if list(read_back.columns) != list(merged.columns) or not _frames_equal(read_back, merged, keys):
                raise BackfillExecutionError("READBACK_FAILED", "canonical read-back checksum mismatch")
            _validate_canonical_frame(dataset, read_back, trade_date)
            exact_subset = _frames_equal(_select_by_keys(read_back, incoming, keys), incoming, keys)
            if not exact_subset:
                raise BackfillExecutionError("READBACK_FAILED", "canonical chunk subset read-back mismatch")
        else:
            exact_subset = True
        return {
            "object_key": object_key,
            "checksum": incoming_checksum,
            "row_count": len(incoming),
            "inserted_rows": inserted,
            "updated_rows": updated,
            "unchanged_rows": unchanged,
            "write_attempted": write_attempted,
            "write_confirmed": write_confirmed,
            "wrote": wrote,
            "materialized": True,
            "exact_read_back_success": exact_subset,
        }
    except (Exception, KeyboardInterrupt, SystemExit) as exc:
        failure = classify_backfill_failure(exc)
        exc.partition_record = {
            "object_key": object_key,
            "checksum": incoming_checksum,
            "row_count": len(incoming),
            "inserted_rows": None,
            "updated_rows": None,
            "unchanged_rows": None,
            "write_attempted": write_attempted,
            "write_confirmed": write_confirmed,
            "wrote": write_confirmed,
            "materialized": write_confirmed,
            "exact_read_back_success": False,
            "status": "FAILED",
            "failure": failure,
        }
        raise


def _empty_partition_record(object_key: str, checksum: str) -> dict[str, Any]:
    return {
        "object_key": object_key,
        "checksum": checksum,
        "row_count": 0,
        "inserted_rows": 0,
        "updated_rows": 0,
        "unchanged_rows": 0,
        "write_attempted": False,
        "write_confirmed": False,
        "wrote": False,
        "materialized": False,
        "exact_read_back_success": True,
    }


def _successful_apply_evidence(records: list[dict[str, Any]], status: str) -> dict[str, Any]:
    canonical_keys = sorted(record["object_key"] for record in records)
    canonical_checksums = {record["object_key"]: record["checksum"] for record in records}
    return {
        "canonical_key": None,
        "canonical_checksum": None,
        "canonical_keys": canonical_keys,
        "canonical_checksums": canonical_checksums,
        "validation": {"success": True, "passed": True, "status": "PASSED"},
        "write_result": {"success": True, "status": status, "partitions": deepcopy(records)},
        "read_back_result": {"success": True, "status": "VERIFIED", "partitions": deepcopy(records)},
    }


def _partial_apply_evidence(records: list[dict[str, Any]]) -> dict[str, Any]:
    canonical_keys = sorted(record["object_key"] for record in records)
    canonical_checksums = {record["object_key"]: record["checksum"] for record in records}
    return {
        "canonical_key": None,
        "canonical_checksum": None,
        "canonical_keys": canonical_keys,
        "canonical_checksums": canonical_checksums,
        "validation": {"success": False, "passed": False, "status": "FAILED"},
        "write_result": {"success": False, "status": "PARTIAL_FAILED", "partitions": deepcopy(records)},
        "read_back_result": {"success": False, "status": "PARTIAL_FAILED", "partitions": deepcopy(records)},
    }


def _upsert_counts(existing: pd.DataFrame, incoming: pd.DataFrame, keys: list[str]) -> tuple[int, int, int]:
    existing_by_key = {_row_key(row, keys): row for _, row in existing.iterrows()}
    inserted = updated = unchanged = 0
    for _, row in incoming.iterrows():
        previous = existing_by_key.get(_row_key(row, keys))
        if previous is None:
            inserted += 1
        elif _series_equal(previous, row, list(incoming.columns)):
            unchanged += 1
        else:
            updated += 1
    return inserted, updated, unchanged


def _upsert_frame(existing: pd.DataFrame, incoming: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    if incoming.empty:
        return existing.copy(deep=True)
    if existing.empty:
        return _sorted_frame(incoming.copy(deep=True), keys)
    incoming_keys = {_row_key(row, keys) for _, row in incoming.iterrows()}
    keep = [
        _row_key(row, keys) not in incoming_keys
        for _, row in existing.iterrows()
    ]
    remainder = existing.loc[keep].copy(deep=True)
    if remainder.empty:
        return _sorted_frame(incoming.copy(deep=True), keys)
    return _sorted_frame(pd.concat([remainder, incoming], ignore_index=True), keys)


def _select_by_keys(existing: pd.DataFrame, incoming: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    if incoming.empty:
        return existing.iloc[0:0].copy(deep=True)
    wanted = {_row_key(row, keys) for _, row in incoming.iterrows()}
    selected = existing.loc[
        [_row_key(row, keys) in wanted for _, row in existing.iterrows()]
    ].copy(deep=True)
    return _sorted_frame(selected, keys)


def _row_key(row: pd.Series, keys: list[str]) -> tuple[Any, ...]:
    return tuple(_comparable_value(row[key]) for key in keys)


def _comparable_value(value: Any) -> Any:
    try:
        if pd.isna(value):
            return ("<NA>",)
    except (TypeError, ValueError):
        pass
    return value


def _series_equal(left: pd.Series, right: pd.Series, columns: list[str]) -> bool:
    for column in columns:
        left_value = left[column]
        right_value = right[column]
        try:
            if pd.isna(left_value) and pd.isna(right_value):
                continue
        except (TypeError, ValueError):
            pass
        if left_value != right_value:
            return False
    return True


def _sorted_frame(frame: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    if frame.empty:
        return frame.copy(deep=True).reset_index(drop=True)
    return frame.sort_values(keys, kind="mergesort").reset_index(drop=True)


def _frames_equal(left: pd.DataFrame, right: pd.DataFrame, keys: list[str]) -> bool:
    if list(left.columns) != list(right.columns) or len(left) != len(right):
        return False
    return dataframe_checksum(left, key_columns=keys) == dataframe_checksum(right, key_columns=keys)


def _st_scope_has_rows(existing: pd.DataFrame | None, chunk: dict[str, Any]) -> bool:
    if existing is None or existing.empty:
        return False
    if not isinstance(existing, pd.DataFrame):
        raise BackfillExecutionError("READBACK_FAILED", "ST canonical read-back is not a DataFrame")
    required = {"stock_code", "start_date", "end_date"}
    if not required.issubset(existing.columns):
        raise BackfillExecutionError("READBACK_FAILED", "ST canonical read-back schema is invalid")
    scoped = existing[existing["stock_code"].astype(str).isin(chunk.get("codes", []))]
    if scoped.empty:
        return False
    starts = scoped["start_date"].astype(str)
    ends = scoped["end_date"].fillna("9999-12-31").astype(str)
    return bool(((starts <= chunk["end_date"]) & (ends >= chunk["start_date"])).any())


def _stock_scope_has_rows(existing: pd.DataFrame | None, chunk: dict[str, Any]) -> bool:
    if existing is None or existing.empty:
        return False
    if not isinstance(existing, pd.DataFrame) or "stock_code" not in existing.columns:
        raise BackfillExecutionError("READBACK_FAILED", "stock_basic canonical read-back schema is invalid")
    return bool(existing["stock_code"].astype(str).isin(chunk.get("codes", [])).any())


def _validate_canonical_frame(dataset: str, frame: pd.DataFrame, trade_date: str) -> None:
    # An empty point-in-time projection is auditable absence, not a fabricated
    # standard row.  Non-empty objects (and ST's explicit empty contract) still
    # pass through the shared canonical validator.
    if frame.empty and dataset != "st_history":
        return
    try:
        validate_dataset_frame(dataset, frame, trade_date)
    except (DataValidationError, ValueError, TypeError) as exc:
        raise BackfillExecutionError("DQ_FAILED", f"canonical validation failed: {exc}") from exc


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

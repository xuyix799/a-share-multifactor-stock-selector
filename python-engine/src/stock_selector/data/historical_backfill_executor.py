from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Callable, Iterable

import numpy as np
import pandas as pd

from stock_selector.data.data_validator import DataValidationError, validate_dataset_frame
from stock_selector.data.historical_backfill import (
    BackfillExecutionError,
    BackfillPlanningError,
    IDENTITY_SCHEMA_VERSION,
    IDENTITY_SCHEMA_VERSION_V2,
    PLAN_SCHEMA_VERSION,
    PLAN_SCHEMA_VERSION_V2,
    PLANNER_VERSION,
    PLANNER_VERSION_V2,
    build_chunk_manifest,
    build_history_backfill_output_keys,
    classify_backfill_failure,
    dataframe_checksum,
    summarize_chunk_manifests,
    validate_financial_announce_chunk_v2,
    _stable_hash,
    _validate_persisted_manifest,
)
from stock_selector.data.real_clean_inputs_landing import (
    CURRENT_SOURCE_MARKERS,
    CURRENT_ST_MARKERS,
    KEY_COLUMNS,
)
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


class _StructuredProviderResultFailure(Exception):
    """Control-flow marker preserving a provider's canonical failure record."""

    def __init__(self, state: str, failure: dict[str, Any]) -> None:
        super().__init__(str(failure.get("message", "provider returned a failed result")))
        self.state = state
        self.failure = deepcopy(failure)


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
    financial_carry_state = pd.DataFrame(columns=get_schema_contract("financial").columns)
    financial_source_chain_complete = True
    financial_seed_loaded = False
    financial_seed_evidence: dict[str, Any] | None = None
    financial_seed_error: BackfillExecutionError | None = None
    financial_last_materialized_trade_date: str | None = None
    financial_last_materialized_anchor: dict[str, Any] | None = None
    financial_chunk_ids = [
        chunk["chunk_id"]
        for chunk in frozen_plan["chunks"]
        if _is_v2_financial_chunk(chunk)
    ]
    final_financial_chunk_id = financial_chunk_ids[-1] if financial_chunk_ids else None

    if provider_call_enabled or apply_standard_write:
        for chunk in frozen_plan["chunks"]:
            chunk_id = chunk["chunk_id"]
            chunk_keys = output_keys["chunks"][chunk_id]
            existing = manifests.get(chunk_id)
            v2_financial = _is_v2_financial_chunk(chunk)
            financial_dependencies_ready = financial_source_chain_complete
            financial_prior_state = financial_carry_state.copy(deep=True) if v2_financial else None
            financial_source_verified = False
            is_final_financial_chunk = v2_financial and chunk_id == final_financial_chunk_id
            recovered_staging: pd.DataFrame | None = None
            if existing is not None and resume and not force:
                recovery = None
                if (
                    not v2_financial
                    or (financial_dependencies_ready and financial_seed_error is None)
                ):
                    try:
                        recovery = _recover_ready_checkpoint(
                            plan=frozen_plan,
                            chunk=chunk,
                            manifest=existing,
                            chunk_keys=chunk_keys,
                            artifact_read_json_fn=artifact_read_json_fn,
                            artifact_read_parquet_fn=artifact_read_parquet_fn,
                            canonical_read_fn=canonical_read_fn,
                            financial_prior_frame=financial_prior_state,
                            financial_seed_loaded=financial_seed_loaded,
                            financial_seed_evidence=financial_seed_evidence,
                            financial_last_materialized_trade_date=(
                                financial_last_materialized_trade_date
                            ),
                            financial_last_materialized_anchor=(
                                financial_last_materialized_anchor
                            ),
                            is_final_financial_chunk=is_final_financial_chunk,
                        )
                    except BackfillExecutionError as exc:
                        if not v2_financial:
                            raise
                        if financial_seed_error is None:
                            financial_seed_error = exc
                if recovery is not None:
                    recovered = recovery["manifest"]
                    artifact_write_json_fn(chunk_keys["manifest"], recovered)
                    manifests[chunk_id] = recovered
                    existing = recovered
                    recovered_staging = recovery["staging_frame"]
                    reconciled.append(chunk_id)
                    if v2_financial and recovered["state"] == "COMPLETED":
                        financial_prior_state = recovery["financial_prior_frame"]
                        financial_seed_loaded = recovery["financial_seed_loaded"]
                        financial_seed_evidence = recovery["financial_seed_evidence"]
                        financial_carry_state = recovery["financial_carry_frame"]
                        financial_last_materialized_trade_date = recovery[
                            "financial_last_materialized_trade_date"
                        ]
                        financial_last_materialized_anchor = recovery[
                            "financial_last_materialized_anchor"
                        ]
                        financial_source_verified = True
                    if recovered["state"] == "COMPLETED":
                        continue
            staging_frame = recovered_staging
            if existing is not None and resume and not force:
                if staging_frame is None:
                    staging_frame = _read_verified_staging(
                        existing,
                        artifact_read_parquet_fn,
                        chunk["dataset"],
                    )
                if (
                    v2_financial
                    and apply_standard_write
                    and staging_frame is not None
                    and financial_seed_error is None
                ):
                    try:
                        (
                            financial_prior_state,
                            financial_seed_loaded,
                            financial_seed_evidence,
                        ) = _ensure_financial_seed(
                            prior=financial_prior_state,
                            seed_loaded=financial_seed_loaded,
                            seed_evidence=financial_seed_evidence,
                            evidence=existing,
                            canonical_read_fn=canonical_read_fn,
                            plan_scope=frozen_plan["scope"],
                        )
                        financial_last_materialized_trade_date = (
                            _latest_financial_materialization_date(
                                financial_last_materialized_trade_date,
                                financial_seed_evidence,
                            )
                        )
                        financial_last_materialized_anchor = _latest_financial_anchor(
                            financial_last_materialized_anchor,
                            financial_seed_evidence,
                        )
                    except BackfillExecutionError as exc:
                        financial_seed_error = exc
                if staging_frame is not None and existing["state"] == "COMPLETED":
                    if not apply_standard_write:
                        if v2_financial:
                            financial_carry_state = _merge_financial_source_state(
                                financial_prior_state,
                                staging_frame,
                            )
                        skipped.append(chunk_id)
                        continue
                    financial_candidate_carry = (
                        _merge_financial_source_state(financial_prior_state, staging_frame)
                        if v2_financial
                        else None
                    )
                    financial_candidate_latest = (
                        _latest_financial_materialization_date(
                            financial_last_materialized_trade_date,
                            existing,
                        )
                        if v2_financial
                        else None
                    )
                    financial_candidate_anchor = (
                        _latest_financial_anchor(
                            financial_last_materialized_anchor,
                            existing,
                        )
                        if v2_financial
                        else None
                    )
                    financial_terminal_evidence = (
                        _financial_terminal_evidence(
                            financial_candidate_carry,
                            financial_candidate_latest,
                            financial_candidate_anchor,
                            required=is_final_financial_chunk,
                        )
                        if v2_financial
                        else None
                    )
                    if (
                        financial_dependencies_ready
                        and financial_seed_error is None
                        and (
                            not v2_financial
                            or _financial_reducer_evidence_matches(
                                existing,
                                chunk=chunk,
                                financial_prior_frame=financial_prior_state,
                                financial_seed_evidence=financial_seed_evidence,
                                financial_terminal_evidence=financial_terminal_evidence,
                            )
                        )
                        and (
                            not is_final_financial_chunk
                            or financial_terminal_evidence["passed"] is True
                        )
                        and _canonical_evidence_matches_staging(
                            chunk=chunk,
                            staging_frame=staging_frame,
                            evidence=existing,
                            plan_scope=frozen_plan["scope"],
                            financial_prior_frame=financial_prior_state,
                        )
                        and _canonical_scope_is_exact(
                            chunk,
                            staging_frame,
                            existing,
                            canonical_read_fn,
                            frozen_plan["scope"],
                            financial_prior_frame=financial_prior_state,
                        )
                    ):
                        if v2_financial:
                            financial_carry_state = financial_candidate_carry
                            financial_last_materialized_trade_date = financial_candidate_latest
                            financial_last_materialized_anchor = financial_candidate_anchor
                            financial_source_verified = True
                        skipped.append(chunk_id)
                        continue
                if (
                    staging_frame is not None
                    and existing["state"] == "STAGED"
                    and not apply_standard_write
                ):
                    if v2_financial:
                        financial_carry_state = _merge_financial_source_state(
                            financial_prior_state,
                            staging_frame,
                        )
                        financial_source_verified = True
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
            # A fresh provider attempt owns a fresh audit envelope. Evidence
            # from an earlier FAILED/BLOCKED attempt is historical provenance,
            # not evidence about a callback that failed before returning a
            # structured result. Reused verified staging deliberately retains
            # its originating attempt evidence below.
            attempt_evidence = {} if do_fetch else carried
            source_keys = tuple(
                [attempt_evidence["source_key"]]
                if isinstance(attempt_evidence.get("source_key"), str)
                and attempt_evidence["source_key"]
                else []
            )
            provider_calls: tuple[dict[str, Any], ...] = ()
            running = _build_manifest(
                chunk=chunk,
                state="RUNNING",
                attempt=attempt,
                plan_fingerprint=frozen_plan["plan_fingerprint"],
                requested_stages=requested_stages,
                evidence=attempt_evidence,
            )
            # This checkpoint deliberately sits outside the isolation handler:
            # no provider or canonical side effect is allowed without it.
            artifact_write_json_fn(chunk_keys["manifest"], running)
            attempted.append(chunk_id)

            evidence = attempt_evidence
            report_key = chunk_keys["attempt_report_template"].format(attempt=attempt)
            try:
                _assert_immutable_report_slot(report_key, artifact_read_json_fn)
                if not do_fetch and staging_frame is not None and existing is not None:
                    (
                        source_keys,
                        provider_calls,
                        staging_provenance,
                        provenance_contradictions,
                    ) = _read_staging_provenance(
                        plan=frozen_plan,
                        chunk=chunk,
                        manifest=existing,
                        chunk_keys=chunk_keys,
                        artifact_read_json_fn=artifact_read_json_fn,
                    )
                    evidence = dict(evidence, **staging_provenance)
                    if provenance_contradictions:
                        raise BackfillExecutionError(
                            "DQ_FAILED",
                            "manifest contradicts immutable staging provenance: "
                            + ", ".join(provenance_contradictions),
                        )
                if do_fetch:
                    if fetch_chunk_fn is None:
                        raise BackfillExecutionError(
                            "CONFIGURATION_ERROR",
                            "provider-call was enabled without a fetch callback",
                        )
                    result = fetch_chunk_fn(deepcopy(chunk))
                    _validate_fetch_result(result, chunk, frozen_plan["scope"])
                    source_keys = tuple(result.source_keys)
                    provider_calls = tuple(deepcopy(result.provider_calls))
                    evidence = _provider_result_evidence(result, source_keys)
                    if result.provider_status in {"FAILED", "BLOCKED"}:
                        failure = deepcopy(result.failure) if result.failure is not None else {
                            "category": "UNKNOWN",
                            "retryable": False,
                            "exception_type": "HistoricalProviderError",
                            "message": "provider returned a failed result",
                        }
                        raise _StructuredProviderResultFailure(
                            "FAILED" if result.provider_status == "FAILED" else "BLOCKED",
                            failure,
                        )
                    staging_frame = result.frame.copy(deep=True)
                    _validate_staging_scope(chunk, staging_frame, frozen_plan["scope"])
                    staging_key = chunk_keys["staging_template"].format(attempt=attempt)
                    staging_checksum = dataframe_checksum(
                        staging_frame,
                        key_columns=KEY_COLUMNS[chunk["dataset"]],
                    )
                    artifact_write_parquet_fn(staging_key, staging_frame.copy(deep=True))
                    evidence = dict(
                        evidence,
                        staging_key=staging_key,
                        staging_checksum=staging_checksum,
                        staging_attempt=attempt,
                        canonical_key=None,
                        canonical_checksum=None,
                        canonical_keys=[],
                        canonical_checksums={},
                        write_result=None,
                        read_back_result=None,
                    )
                    read_back = artifact_read_parquet_fn(staging_key)
                    _assert_frame_checksum(
                        read_back,
                        staging_checksum,
                        chunk["dataset"],
                        "staging read-back",
                    )
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
                    _validate_staging_scope(chunk, staging_frame, frozen_plan["scope"])

                if v2_financial:
                    if financial_seed_error is not None:
                        raise financial_seed_error
                    (
                        financial_prior_state,
                        financial_seed_loaded,
                        financial_seed_evidence,
                    ) = _ensure_financial_seed(
                        prior=financial_prior_state,
                        seed_loaded=financial_seed_loaded,
                        seed_evidence=financial_seed_evidence,
                        evidence=evidence,
                        canonical_read_fn=canonical_read_fn if apply_standard_write else None,
                        plan_scope=frozen_plan["scope"],
                    )
                    financial_last_materialized_trade_date = (
                        _latest_financial_materialization_date(
                            financial_last_materialized_trade_date,
                            financial_seed_evidence,
                        )
                    )
                    financial_last_materialized_anchor = _latest_financial_anchor(
                        financial_last_materialized_anchor,
                        financial_seed_evidence,
                    )
                    financial_carry_state = _merge_financial_source_state(
                        financial_prior_state,
                        staging_frame,
                    )
                    financial_source_verified = True

                if apply_standard_write:
                    if canonical_read_fn is None or canonical_write_fn is None:
                        raise BackfillExecutionError(
                            "CONFIGURATION_ERROR",
                            "apply requires canonical read and write callbacks",
                        )
                    if v2_financial and not financial_dependencies_ready:
                        raise BackfillExecutionError(
                            "CONFIGURATION_ERROR",
                            "financial source dependency chain is incomplete",
                        )
                    financial_candidate_latest = (
                        _latest_financial_materialization_date(
                            financial_last_materialized_trade_date,
                            evidence,
                        )
                        if v2_financial
                        else None
                    )
                    financial_pending_evidence = (
                        _financial_terminal_evidence(
                            financial_carry_state,
                            financial_candidate_latest,
                            financial_last_materialized_anchor,
                            required=False,
                        )
                        if v2_financial
                        else None
                    )
                    if (
                        is_final_financial_chunk
                        and financial_pending_evidence["pending_row_count"] > 0
                    ):
                        raise BackfillExecutionError(
                            "SEMANTIC_SOURCE_UNAVAILABLE",
                            "financial announcements remain pending after the final materialized trade date",
                        )
                    apply_evidence, was_reconciled = _apply_canonical_partitions(
                        chunk=chunk,
                        frame=staging_frame,
                        evidence=evidence,
                        plan_scope=frozen_plan["scope"],
                        canonical_read_fn=canonical_read_fn,
                        canonical_write_fn=canonical_write_fn,
                        always_write=force or not resume,
                        financial_prior_frame=financial_prior_state,
                        financial_seed_evidence=financial_seed_evidence,
                        financial_terminal_evidence=None,
                    )
                    financial_candidate_anchor = (
                        _latest_financial_anchor(
                            financial_last_materialized_anchor,
                            apply_evidence,
                        )
                        if v2_financial
                        else None
                    )
                    if v2_financial:
                        financial_terminal_evidence = _financial_terminal_evidence(
                            financial_carry_state,
                            financial_candidate_latest,
                            financial_candidate_anchor,
                            required=is_final_financial_chunk,
                        )
                        apply_evidence["validation"]["financial_reducer"][
                            "terminal"
                        ] = financial_terminal_evidence
                        evidence = dict(evidence, **apply_evidence)
                        if (
                            is_final_financial_chunk
                            and financial_terminal_evidence["passed"] is not True
                        ):
                            raise BackfillExecutionError(
                                "READBACK_FAILED",
                                "financial final reducer anchor does not match canonical state",
                            )
                    else:
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
                if v2_financial and final_state == "COMPLETED":
                    financial_last_materialized_trade_date = financial_candidate_latest
                    financial_last_materialized_anchor = financial_candidate_anchor
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
            except _StructuredProviderResultFailure as exc:
                failure = deepcopy(exc.failure)
                failed = _build_manifest(
                    chunk=chunk,
                    state=exc.state,
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
                    state=exc.state,
                    evidence=evidence,
                    source_keys=source_keys,
                    provider_calls=provider_calls,
                    generated_at=now(),
                    failure=failure,
                )
                _write_immutable_report(
                    report_key,
                    report,
                    artifact_read_json_fn,
                    artifact_write_json_fn,
                )
                artifact_write_json_fn(chunk_keys["manifest"], failed)
                manifests[chunk_id] = failed
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
            finally:
                if v2_financial and not financial_source_verified:
                    financial_source_chain_complete = False

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
    if not isinstance(plan, dict):
        raise BackfillPlanningError("INVALID_PLAN", "invalid Goal 21 plan schema")
    versions = (
        plan.get("schema_version"),
        plan.get("identity_schema_version"),
        plan.get("planner_version"),
    )
    if versions not in {
        (PLAN_SCHEMA_VERSION, IDENTITY_SCHEMA_VERSION, PLANNER_VERSION),
        (PLAN_SCHEMA_VERSION_V2, IDENTITY_SCHEMA_VERSION_V2, PLANNER_VERSION_V2),
    }:
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
    if versions == (PLAN_SCHEMA_VERSION_V2, IDENTITY_SCHEMA_VERSION_V2, PLANNER_VERSION_V2):
        previous_financial_chunk_id: str | None = None
        for chunk in plan["chunks"]:
            dependencies = chunk.get("dependency_keys")
            if chunk.get("dataset") == "financial":
                expected = [] if previous_financial_chunk_id is None else [previous_financial_chunk_id]
                if dependencies != expected:
                    raise BackfillPlanningError(
                        "INVALID_PLAN",
                        "financial dependency chain is not canonical",
                    )
                previous_financial_chunk_id = chunk["chunk_id"]
            elif dependencies != []:
                raise BackfillPlanningError("INVALID_PLAN", "non-financial dependencies are invalid")


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


def _read_staging_provenance(
    *,
    plan: dict[str, Any],
    chunk: dict[str, Any],
    manifest: dict[str, Any],
    chunk_keys: dict[str, str],
    artifact_read_json_fn: Callable[[str], dict[str, Any]],
) -> tuple[
    tuple[str, ...],
    tuple[dict[str, Any], ...],
    dict[str, Any],
    list[str],
]:
    """Recover complete immutable provider lineage for a reused staging blob."""

    def invalid(message: str) -> BackfillPlanningError:
        return BackfillPlanningError("INVALID_STAGING_PROVENANCE", message)

    staging_attempt = manifest.get("staging_attempt")
    attempt_count = manifest.get("attempt_count")
    if (
        isinstance(staging_attempt, bool)
        or not isinstance(staging_attempt, int)
        or staging_attempt <= 0
        or isinstance(attempt_count, bool)
        or not isinstance(attempt_count, int)
        or staging_attempt > attempt_count
    ):
        raise invalid("manifest staging attempt is invalid")
    report_key = chunk_keys["attempt_report_template"].format(
        attempt=staging_attempt
    )
    report = _read_json_optional(artifact_read_json_fn, report_key)
    if report is None:
        raise invalid("immutable staging attempt report is missing")
    expected_identity = {
        "schema_version": "goal21.chunk_attempt_report.v1",
        "run_id": plan["run_id"],
        "plan_fingerprint": plan["plan_fingerprint"],
        "chunk_id": chunk["chunk_id"],
        "dataset": chunk["dataset"],
        "chunk": chunk,
        "attempt": staging_attempt,
        "attempt_count": staging_attempt,
    }
    if any(report.get(field) != expected for field, expected in expected_identity.items()):
        raise invalid("immutable staging attempt report identity is invalid")
    if report.get("state") not in {
        "READY_TO_CHECKPOINT",
        "INTERRUPTED",
        "FAILED",
        "BLOCKED",
    }:
        raise invalid("immutable staging attempt report state is invalid")
    requested_stages = report.get("requested_stages")
    if not isinstance(requested_stages, list) or "provider" not in requested_stages:
        raise invalid("staging attempt report does not prove a provider stage")
    source_keys = report.get("source_keys")
    provider_calls = report.get("provider_calls")
    if (
        not isinstance(source_keys, list)
        or not source_keys
        or any(not isinstance(value, str) or not value for value in source_keys)
        or not isinstance(provider_calls, list)
        or any(not isinstance(value, dict) for value in provider_calls)
    ):
        raise invalid("staging attempt provider provenance is invalid")
    provenance_fields = (
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
    )
    contradictions = [
        field
        for field in provenance_fields
        if report.get(field) != manifest.get(field)
    ]
    if report.get("source_key") != source_keys[0]:
        raise invalid("scalar and plural staging source lineage disagree")
    provenance = {
        field: deepcopy(report.get(field))
        for field in provenance_fields
    }
    return (
        tuple(source_keys),
        tuple(deepcopy(provider_calls)),
        provenance,
        contradictions,
    )


def _provider_result_evidence(result: Any, source_keys: tuple[str, ...]) -> dict[str, Any]:
    """Snapshot a validated provider result before status-dependent control flow."""

    return {
        "provider_status": result.provider_status,
        "row_count": len(result.frame),
        "actual_schema": list(result.actual_schema),
        "target_schema": list(result.target_schema),
        "dq": deepcopy(result.dq),
        "coverage": deepcopy(result.coverage),
        "source_key": source_keys[0] if source_keys else None,
        "staging_key": None,
        "staging_checksum": None,
        "staging_attempt": None,
        "canonical_key": None,
        "canonical_checksum": None,
        "canonical_keys": [],
        "canonical_checksums": {},
        "validation": deepcopy(result.validation),
        "write_result": None,
        "read_back_result": None,
    }


def _recover_ready_checkpoint(
    *,
    plan: dict[str, Any],
    chunk: dict[str, Any],
    manifest: dict[str, Any],
    chunk_keys: dict[str, str],
    artifact_read_json_fn: Callable[[str], dict[str, Any]],
    artifact_read_parquet_fn: Callable[[str], pd.DataFrame],
    canonical_read_fn: Callable[[str, str], pd.DataFrame | None] | None,
    financial_prior_frame: pd.DataFrame | None = None,
    financial_seed_loaded: bool = False,
    financial_seed_evidence: dict[str, Any] | None = None,
    financial_last_materialized_trade_date: str | None = None,
    financial_last_materialized_anchor: dict[str, Any] | None = None,
    is_final_financial_chunk: bool = False,
) -> dict[str, Any] | None:
    if manifest.get("state") in {"STAGED", "COMPLETED"}:
        return None
    attempt = manifest.get("attempt_count")
    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt <= 0:
        return None
    report_key = chunk_keys["attempt_report_template"].format(attempt=attempt)
    report = _read_json_optional(artifact_read_json_fn, report_key)
    if report is None or report.get("state") != "READY_TO_CHECKPOINT":
        return None

    def invalid(message: str) -> BackfillPlanningError:
        return BackfillPlanningError("INVALID_READY_REPORT", message)

    expected_identity = {
        "schema_version": "goal21.chunk_attempt_report.v1",
        "run_id": plan["run_id"],
        "plan_fingerprint": plan["plan_fingerprint"],
        "chunk_id": chunk["chunk_id"],
        "dataset": chunk["dataset"],
        "chunk": chunk,
        "attempt": attempt,
        "attempt_count": attempt,
    }
    for field, expected in expected_identity.items():
        if report.get(field) != expected:
            raise invalid(f"READY report {field} does not match the immutable run")
    if report.get("failure") is not None:
        raise invalid("READY report cannot carry a failure")
    requested_stages = report.get("requested_stages")
    if not isinstance(requested_stages, list) or requested_stages not in (
        ["provider"],
        ["apply"],
        ["provider", "apply"],
    ):
        raise invalid("READY report requested stages are invalid")
    if manifest.get("requested_stages") != requested_stages:
        raise invalid("READY report requested stages do not match the mutable manifest")
    target_state = report.get("checkpoint_target_state")
    if target_state not in {"STAGED", "COMPLETED"}:
        raise invalid("READY report checkpoint target is invalid")
    if target_state == "STAGED" and "provider" not in requested_stages:
        raise invalid("STAGED READY report lacks a provider stage")
    if target_state == "COMPLETED" and "apply" not in requested_stages:
        raise invalid("COMPLETED READY report lacks an apply stage")

    source_keys = report.get("source_keys")
    provider_calls = report.get("provider_calls")
    if (
        not isinstance(source_keys, list)
        or not source_keys
        or any(not isinstance(value, str) or not value for value in source_keys)
        or not isinstance(provider_calls, list)
        or any(not isinstance(value, dict) for value in provider_calls)
    ):
        raise invalid("READY report provider provenance is invalid")
    evidence = {field: deepcopy(report.get(field)) for field in _MANIFEST_EVIDENCE_FIELDS}
    target_schema = evidence.get("target_schema")
    actual_schema = evidence.get("actual_schema")
    if target_schema != list(get_schema_contract(chunk["dataset"]).columns):
        raise invalid("READY report target schema is invalid")
    if (
        not isinstance(actual_schema, list)
        or not actual_schema
        or any(not isinstance(value, str) or not value for value in actual_schema)
        or len(actual_schema) != len(set(actual_schema))
    ):
        raise invalid("READY report actual schema is invalid")
    if evidence.get("provider_status") not in {"FETCHED", "VALID_EMPTY"}:
        raise invalid("READY report provider status is invalid")
    if evidence.get("source_key") != source_keys[0]:
        raise invalid("READY report scalar and plural source lineage disagree")
    staging_attempt = evidence.get("staging_attempt")
    if (
        isinstance(staging_attempt, bool)
        or not isinstance(staging_attempt, int)
        or staging_attempt <= 0
        or staging_attempt > attempt
        or ("provider" in requested_stages and staging_attempt != attempt)
        or ("provider" not in requested_stages and staging_attempt >= attempt)
    ):
        raise invalid("READY report staging attempt is invalid")
    if not isinstance(evidence.get("staging_key"), str) or not isinstance(
        evidence.get("staging_checksum"), str
    ):
        raise invalid("READY report staging evidence is incomplete")

    if target_state == "COMPLETED":
        validation = evidence.get("validation")
        write_result = evidence.get("write_result")
        read_back_result = evidence.get("read_back_result")
        if not isinstance(validation, dict) or validation.get("passed") is not True:
            raise invalid("READY report canonical validation did not pass")
        if not isinstance(write_result, dict) or write_result.get("success") is not True:
            raise invalid("READY report canonical write evidence is invalid")
        if not isinstance(read_back_result, dict) or read_back_result.get("success") is not True:
            raise invalid("READY report canonical read-back evidence is invalid")
        partitions = read_back_result.get("partitions")
        if not isinstance(partitions, list) or any(
            not isinstance(record, dict)
            or record.get("exact_read_back_success") is not True
            for record in partitions
        ):
            raise invalid("READY report canonical partition read-back is not exact")

    recovered = _build_manifest(
        chunk=chunk,
        state=target_state,
        attempt=attempt,
        plan_fingerprint=plan["plan_fingerprint"],
        requested_stages=requested_stages,
        evidence=evidence,
    )
    staging_frame = _read_verified_staging(
        recovered,
        artifact_read_parquet_fn,
        chunk["dataset"],
    )
    if staging_frame is None:
        raise invalid("READY report staging checksum or read-back is invalid")
    recovered_financial_prior = financial_prior_frame
    recovered_seed_loaded = financial_seed_loaded
    recovered_seed_evidence = deepcopy(financial_seed_evidence)
    recovered_financial_carry = financial_prior_frame
    recovered_financial_latest = financial_last_materialized_trade_date
    recovered_financial_anchor = deepcopy(financial_last_materialized_anchor)
    if target_state == "COMPLETED":
        if canonical_read_fn is None:
            raise invalid("READY completion recovery requires canonical read access")
        if _is_v2_financial_chunk(chunk):
            (
                recovered_financial_prior,
                recovered_seed_loaded,
                recovered_seed_evidence,
            ) = _ensure_financial_seed(
                prior=financial_prior_frame,
                seed_loaded=financial_seed_loaded,
                seed_evidence=financial_seed_evidence,
                evidence=report,
                canonical_read_fn=canonical_read_fn,
                plan_scope=plan["scope"],
            )
            recovered_financial_latest = _latest_financial_materialization_date(
                financial_last_materialized_trade_date,
                recovered_seed_evidence,
            )
            recovered_financial_anchor = _latest_financial_anchor(
                financial_last_materialized_anchor,
                recovered_seed_evidence,
            )
            recovered_financial_carry = _merge_financial_source_state(
                recovered_financial_prior,
                staging_frame,
            )
            recovered_financial_latest = _latest_financial_materialization_date(
                recovered_financial_latest,
                report,
            )
            recovered_financial_anchor = _latest_financial_anchor(
                recovered_financial_anchor,
                report,
            )
            terminal_evidence = _financial_terminal_evidence(
                recovered_financial_carry,
                recovered_financial_latest,
                recovered_financial_anchor,
                required=is_final_financial_chunk,
            )
            if not _financial_reducer_evidence_matches(
                report,
                chunk=chunk,
                financial_prior_frame=recovered_financial_prior,
                financial_seed_evidence=recovered_seed_evidence,
                financial_terminal_evidence=terminal_evidence,
            ):
                raise invalid("READY report financial reducer evidence is invalid")
            if is_final_financial_chunk and terminal_evidence["passed"] is not True:
                raise invalid("READY report leaves financial announcements unmaterialized")
        if not _canonical_evidence_matches_staging(
            chunk=chunk,
            staging_frame=staging_frame,
            evidence=report,
            plan_scope=plan["scope"],
            financial_prior_frame=recovered_financial_prior,
        ):
            raise invalid("READY report canonical checksum evidence is invalid")
        if not _canonical_scope_is_exact(
            chunk,
            staging_frame,
            recovered,
            canonical_read_fn,
            plan["scope"],
            financial_prior_frame=recovered_financial_prior,
        ):
            raise invalid("READY report canonical scope is not exact")
    return {
        "manifest": recovered,
        "staging_frame": staging_frame,
        "financial_prior_frame": recovered_financial_prior,
        "financial_seed_loaded": recovered_seed_loaded,
        "financial_seed_evidence": recovered_seed_evidence,
        "financial_carry_frame": recovered_financial_carry,
        "financial_last_materialized_trade_date": recovered_financial_latest,
        "financial_last_materialized_anchor": recovered_financial_anchor,
    }


def _canonical_evidence_matches_staging(
    *,
    chunk: dict[str, Any],
    staging_frame: pd.DataFrame,
    evidence: dict[str, Any],
    plan_scope: dict[str, Any],
    financial_prior_frame: pd.DataFrame | None,
) -> bool:
    try:
        expected_partitions = []
        for trade_date, incoming in _project_partitions(
            chunk,
            staging_frame,
            evidence,
            plan_scope,
            financial_prior_frame=financial_prior_frame,
        ):
            _validate_canonical_frame(chunk["dataset"], incoming, trade_date)
            expected_partitions.append(
                {
                    "object_key": build_partition(chunk["dataset"], trade_date).object_key,
                    "checksum": dataframe_checksum(
                        incoming,
                        key_columns=KEY_COLUMNS[chunk["dataset"]],
                    ),
                    "row_count": len(incoming),
                }
            )
    except (BackfillExecutionError, TypeError, ValueError):
        return False

    expected_by_key = {
        record["object_key"]: record
        for record in expected_partitions
    }
    expected_keys = sorted(expected_by_key)
    expected_checksums = {
        key: expected_by_key[key]["checksum"]
        for key in expected_keys
    }
    if (
        evidence.get("canonical_key") is not None
        or evidence.get("canonical_checksum") is not None
        or evidence.get("canonical_keys") != expected_keys
        or evidence.get("canonical_checksums") != expected_checksums
    ):
        return False

    if not _successful_partition_audit_is_consistent(evidence):
        return False

    for field in ("write_result", "read_back_result"):
        container = evidence.get(field)
        records = container.get("partitions") if isinstance(container, dict) else None
        if not isinstance(records, list) or len(records) != len(expected_keys):
            return False
        observed_by_key: dict[str, dict[str, Any]] = {}
        for record in records:
            if not isinstance(record, dict):
                return False
            object_key = record.get("object_key")
            if not isinstance(object_key, str) or object_key in observed_by_key:
                return False
            observed_by_key[object_key] = record
        if set(observed_by_key) != set(expected_keys):
            return False
        for object_key, expected in expected_by_key.items():
            observed = observed_by_key[object_key]
            if (
                observed.get("checksum") != expected["checksum"]
                or observed.get("row_count") != expected["row_count"]
                or observed.get("exact_read_back_success") is not True
            ):
                return False
    return True


def _successful_partition_audit_is_consistent(evidence: dict[str, Any]) -> bool:
    write_result = evidence.get("write_result")
    read_back_result = evidence.get("read_back_result")
    write_records = (
        write_result.get("partitions") if isinstance(write_result, dict) else None
    )
    read_records = (
        read_back_result.get("partitions")
        if isinstance(read_back_result, dict)
        else None
    )
    if not isinstance(write_records, list) or write_records != read_records:
        return False
    for record in write_records:
        if not isinstance(record, dict):
            return False
        row_count = record.get("row_count")
        counters = [
            record.get("inserted_rows"),
            record.get("updated_rows"),
            record.get("unchanged_rows"),
        ]
        if (
            isinstance(row_count, bool)
            or not isinstance(row_count, int)
            or row_count < 0
            or any(
                isinstance(value, bool) or not isinstance(value, int) or value < 0
                for value in counters
            )
            or sum(counters) != row_count
        ):
            return False
        flags = {
            field: record.get(field)
            for field in (
                "write_attempted",
                "write_confirmed",
                "wrote",
                "materialized",
                "exact_read_back_success",
            )
        }
        if any(type(value) is not bool for value in flags.values()):
            return False
        if (
            flags["write_attempted"] != flags["write_confirmed"]
            or flags["write_confirmed"] != flags["wrote"]
            or flags["materialized"] != (row_count > 0)
            or flags["exact_read_back_success"] is not True
        ):
            return False
    return True


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


def _validate_staging_scope(
    chunk: dict[str, Any],
    frame: pd.DataFrame,
    plan_scope: dict[str, Any] | None = None,
) -> None:
    dataset = chunk["dataset"]
    target = list(get_schema_contract(dataset).columns)
    if list(frame.columns) != target:
        raise BackfillExecutionError("SCHEMA_DRIFT", "staging schema does not match the planned dataset")
    if "stock_code" in frame.columns:
        requested_codes = list(chunk.get("codes", []))
        if (
            not requested_codes
            and chunk.get("chunk_schema_version") == "goal21.history_backfill_chunk.v2"
            and isinstance(plan_scope, dict)
        ):
            requested_codes = list(plan_scope.get("codes", []))
        outside_codes = set(frame["stock_code"].astype(str)) - set(requested_codes)
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
        if chunk.get("chunk_schema_version") == "goal21.history_backfill_chunk.v2":
            validate_financial_announce_chunk_v2(chunk, frame)
        else:
            periods = frame["report_period"].astype(str)
            if (
                (periods < chunk["report_period_start"])
                | (periods > chunk["report_period_end"])
            ).any():
                raise BackfillExecutionError("DQ_FAILED", "financial staging contains report periods outside the chunk scope")
    if dataset == "st_history" and not frame.empty:
        starts = frame["start_date"].astype(str)
        ends = frame["end_date"].fillna("9999-12-31").astype(str)
        if ((starts > chunk["end_date"]) | (ends <= chunk["start_date"])).any():
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
    if not isinstance(values, list):
        raise BackfillExecutionError("DQ_FAILED", "canonical trade-date coverage is missing")
    if not values:
        if chunk.get("chunk_schema_version") == "goal21.history_backfill_chunk.v2":
            return []
        raise BackfillExecutionError("DQ_FAILED", "canonical trade-date coverage is empty")
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
    *,
    financial_prior_frame: pd.DataFrame | None = None,
) -> list[tuple[str, pd.DataFrame]]:
    dataset = chunk["dataset"]
    target = list(get_schema_contract(dataset).columns)
    if list(frame.columns) != target:
        raise BackfillExecutionError("SCHEMA_DRIFT", "staging schema does not match the standard contract")
    partitions = []
    for trade_date in _validated_canonical_dates(chunk, frame, evidence, plan_scope):
        if dataset == "financial":
            current = frame[
                (frame["report_period"].astype(str) <= trade_date)
                & (frame["announce_date"].astype(str) <= trade_date)
            ]
            if _is_v2_financial_chunk(chunk):
                projected = _merge_financial_source_state(financial_prior_frame, current)
            else:
                projected = current
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


def _chunk_scope_codes(
    chunk: dict[str, Any],
    plan_scope: dict[str, Any],
) -> list[str]:
    if chunk.get("dataset") == "benchmark_price":
        return []
    if chunk.get("chunk_schema_version") == "goal21.history_backfill_chunk.v2":
        codes = plan_scope.get("codes")
    else:
        codes = chunk.get("codes")
    if not isinstance(codes, list) or not codes:
        raise BackfillExecutionError(
            "CONFIGURATION_ERROR",
            "canonical replacement scope has no stock universe",
        )
    return list(codes)


def _canonical_scope_mask(
    dataset: str,
    frame: pd.DataFrame,
    chunk: dict[str, Any],
    scope_codes: list[str] | None,
) -> pd.Series:
    if dataset == "benchmark_price":
        indexes = chunk.get("index_codes")
        if not isinstance(indexes, list) or not indexes:
            raise BackfillExecutionError(
                "CONFIGURATION_ERROR",
                "benchmark replacement scope is invalid",
            )
        return frame["index_code"].astype(str).isin(indexes)
    if not isinstance(scope_codes, list) or not scope_codes:
        raise BackfillExecutionError(
            "CONFIGURATION_ERROR",
            f"{dataset} replacement scope is invalid",
        )
    mask = frame["stock_code"].astype(str).isin(scope_codes)
    if dataset == "st_history":
        starts = frame["start_date"].astype(str)
        ends = frame["end_date"].fillna("9999-12-31").astype(str)
        mask &= (starts <= chunk["end_date"]) & (ends > chunk["start_date"])
    return mask


def _canonical_scope_frame(
    dataset: str,
    frame: pd.DataFrame | None,
    chunk: dict[str, Any],
    scope_codes: list[str] | None,
) -> pd.DataFrame:
    columns = list(get_schema_contract(dataset).columns)
    if frame is None:
        return pd.DataFrame(columns=columns)
    if not isinstance(frame, pd.DataFrame) or list(frame.columns) != columns:
        raise BackfillExecutionError(
            "READBACK_FAILED",
            f"{dataset} canonical replacement schema is invalid",
        )
    mask = _canonical_scope_mask(dataset, frame, chunk, scope_codes)
    scoped = frame.loc[mask, columns].copy(deep=True)
    return _sorted_frame(scoped, KEY_COLUMNS[dataset])


def _canonical_outside_scope_frame(
    dataset: str,
    frame: pd.DataFrame,
    chunk: dict[str, Any],
    scope_codes: list[str] | None,
) -> pd.DataFrame:
    mask = _canonical_scope_mask(dataset, frame, chunk, scope_codes)
    outside = frame.loc[~mask, list(frame.columns)].copy(deep=True)
    return _sorted_frame(outside, KEY_COLUMNS[dataset])


def _validate_existing_canonical_if_present(
    dataset: str,
    existing: pd.DataFrame | None,
    incoming: pd.DataFrame,
    trade_date: str,
) -> None:
    if existing is None:
        return
    if not isinstance(existing, pd.DataFrame) or list(existing.columns) != list(incoming.columns):
        raise BackfillExecutionError(
            "SCHEMA_DRIFT",
            "canonical schema does not match staging",
        )
    if existing.duplicated(KEY_COLUMNS[dataset]).any():
        raise BackfillExecutionError("DQ_FAILED", "existing canonical keys are duplicated")
    _validate_canonical_frame(dataset, existing, trade_date)


def _canonical_scope_is_exact(
    chunk: dict[str, Any],
    frame: pd.DataFrame,
    evidence: dict[str, Any],
    read_fn: Callable[[str, str], pd.DataFrame | None] | None,
    plan_scope: dict[str, Any],
    *,
    financial_prior_frame: pd.DataFrame | None = None,
) -> bool:
    if read_fn is None:
        return False
    dataset = chunk["dataset"]
    keys = KEY_COLUMNS[dataset]
    scope_codes = _chunk_scope_codes(chunk, plan_scope)
    try:
        for trade_date, incoming in _project_partitions(
            chunk,
            frame,
            evidence,
            plan_scope,
            financial_prior_frame=financial_prior_frame,
        ):
            _validate_canonical_frame(dataset, incoming, trade_date)
            existing = read_fn(dataset, trade_date)
            if incoming.empty and dataset == "st_history":
                if _st_scope_has_rows(existing, chunk, scope_codes):
                    return False
                _validate_existing_canonical_if_present(dataset, existing, incoming, trade_date)
                continue
            if incoming.empty and dataset == "stock_basic":
                if _stock_scope_has_rows(existing, chunk, scope_codes):
                    return False
                _validate_existing_canonical_if_present(dataset, existing, incoming, trade_date)
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
            existing_scope = _canonical_scope_frame(
                dataset,
                existing,
                chunk,
                scope_codes,
            )
            if not _frames_equal(existing_scope, incoming, keys):
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
    financial_prior_frame: pd.DataFrame | None = None,
    financial_seed_evidence: dict[str, Any] | None = None,
    financial_terminal_evidence: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], bool]:
    dataset = chunk["dataset"]
    keys = KEY_COLUMNS[dataset]
    scope_codes = _chunk_scope_codes(chunk, plan_scope)
    records: list[dict[str, Any]] = []
    for trade_date, incoming in _project_partitions(
        chunk,
        frame,
        evidence,
        plan_scope,
        financial_prior_frame=financial_prior_frame,
    ):
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
                scope_codes=scope_codes,
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
    successful = _successful_apply_evidence(records, status)
    if _is_v2_financial_chunk(chunk):
        successful["validation"]["financial_reducer"] = {
            "dependency_keys": list(chunk.get("dependency_keys", [])),
            "prior_state_checksum": dataframe_checksum(
                financial_prior_frame
                if financial_prior_frame is not None
                else pd.DataFrame(columns=get_schema_contract("financial").columns),
                key_columns=KEY_COLUMNS["financial"],
            ),
            "seed": deepcopy(financial_seed_evidence),
            "terminal": deepcopy(financial_terminal_evidence),
        }
    return successful, not any_write


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
    scope_codes: list[str] | None = None,
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
            if _st_scope_has_rows(existing, chunk, scope_codes):
                raise BackfillExecutionError(
                    "READBACK_FAILED",
                    "proven empty ST scope contradicts existing canonical history",
                )
            _validate_existing_canonical_if_present(dataset, existing, incoming, trade_date)
            return _empty_partition_record(object_key, incoming_checksum)

        if incoming.empty:
            if dataset == "financial" and not _canonical_scope_frame(
                dataset,
                existing,
                chunk,
                scope_codes,
            ).empty:
                raise BackfillExecutionError(
                    "READBACK_FAILED",
                    "proven empty financial scope contradicts existing canonical state",
                )
            if dataset == "stock_basic" and _stock_scope_has_rows(existing, chunk, scope_codes):
                raise BackfillExecutionError(
                    "READBACK_FAILED",
                    "proven non-member stock scope contradicts existing canonical membership",
                )
            _validate_existing_canonical_if_present(dataset, existing, incoming, trade_date)
            return _empty_partition_record(object_key, incoming_checksum)

        if existing is None:
            existing = pd.DataFrame(columns=incoming.columns)
        if not isinstance(existing, pd.DataFrame) or list(existing.columns) != list(incoming.columns):
            raise BackfillExecutionError("SCHEMA_DRIFT", "canonical schema does not match staging")
        if existing.duplicated(keys).any():
            raise BackfillExecutionError("DQ_FAILED", "existing canonical keys are duplicated")
        existing = _sorted_frame(existing, keys)
        comparison_existing = _canonical_scope_frame(
            dataset,
            existing,
            chunk,
            scope_codes,
        )
        outside_scope = _canonical_outside_scope_frame(
            dataset,
            existing,
            chunk,
            scope_codes,
        )
        if outside_scope.empty:
            merged = _sorted_frame(incoming.copy(deep=True), keys)
        else:
            merged = _sorted_frame(
                pd.concat([outside_scope, incoming], ignore_index=True),
                keys,
            )
        inserted, updated, unchanged = _upsert_counts(comparison_existing, incoming, keys)
        _validate_canonical_frame(dataset, merged, trade_date)
        exact_before_write = _frames_equal(comparison_existing, incoming, keys)
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
            exact_subset = _frames_equal(
                _canonical_scope_frame(
                    dataset,
                    read_back,
                    chunk,
                    scope_codes,
                ),
                incoming,
                keys,
            )
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


def _is_v2_financial_chunk(chunk: dict[str, Any]) -> bool:
    return (
        chunk.get("dataset") == "financial"
        and chunk.get("chunk_schema_version") == "goal21.history_backfill_chunk.v2"
        and chunk.get("axis") == "announce_date"
    )


def _merge_financial_source_state(
    prior: pd.DataFrame | None,
    current: pd.DataFrame,
) -> pd.DataFrame:
    columns = list(get_schema_contract("financial").columns)
    if prior is None:
        prior = pd.DataFrame(columns=columns)
    if not isinstance(prior, pd.DataFrame) or not isinstance(current, pd.DataFrame):
        raise BackfillExecutionError("SCHEMA_DRIFT", "financial carry state must be a DataFrame")
    if list(prior.columns) != columns or list(current.columns) != columns:
        raise BackfillExecutionError("SCHEMA_DRIFT", "financial carry state schema is invalid")
    _validate_financial_source_state(prior)
    _validate_financial_source_state(current)
    keys = KEY_COLUMNS["financial"]
    if prior.duplicated(keys).any() or current.duplicated(keys).any():
        raise BackfillExecutionError("DQ_FAILED", "financial carry state contains duplicate keys")
    return _upsert_frame(prior, current, keys)


def _latest_financial_materialization_date(
    current: str | None,
    evidence: dict[str, Any] | None,
) -> str | None:
    candidates: list[str] = []
    if current is not None:
        try:
            candidates.append(validate_trade_date(current))
        except (TypeError, ValueError) as exc:
            raise BackfillExecutionError(
                "DQ_FAILED",
                "financial materialization cursor is invalid",
            ) from exc
    if isinstance(evidence, dict):
        seed_date = evidence.get("trade_date")
        if seed_date is not None:
            try:
                candidates.append(validate_trade_date(str(seed_date)))
            except (TypeError, ValueError) as exc:
                raise BackfillExecutionError("DQ_FAILED", "financial seed date is invalid") from exc
        coverage = evidence.get("coverage")
        dates = coverage.get("canonical_trade_dates") if isinstance(coverage, dict) else None
        if isinstance(dates, list):
            try:
                candidates.extend(validate_trade_date(str(value)) for value in dates)
            except (TypeError, ValueError) as exc:
                raise BackfillExecutionError(
                    "DQ_FAILED",
                    "financial canonical materialization dates are invalid",
                ) from exc
    return max(candidates) if candidates else None


def _financial_terminal_evidence(
    carry: pd.DataFrame | None,
    last_materialized_trade_date: str | None,
    last_materialized_anchor: dict[str, Any] | None,
    *,
    required: bool,
) -> dict[str, Any]:
    columns = list(get_schema_contract("financial").columns)
    state = carry if carry is not None else pd.DataFrame(columns=columns)
    if not isinstance(state, pd.DataFrame) or list(state.columns) != columns:
        raise BackfillExecutionError("SCHEMA_DRIFT", "financial terminal state schema is invalid")
    _validate_financial_source_state(state)
    normalized_last = None
    if last_materialized_trade_date is not None:
        try:
            normalized_last = validate_trade_date(last_materialized_trade_date)
        except (TypeError, ValueError) as exc:
            raise BackfillExecutionError(
                "DQ_FAILED",
                "financial final materialization date is invalid",
            ) from exc
    if state.empty:
        pending = state
        pending_dates: list[str] = []
    else:
        announce_dates = state["announce_date"].astype(str)
        pending = state if normalized_last is None else state.loc[announce_dates > normalized_last]
        pending_dates = sorted(pending["announce_date"].astype(str).unique().tolist())
    state_checksum = dataframe_checksum(state, key_columns=KEY_COLUMNS["financial"])
    anchor = _normalized_financial_anchor(last_materialized_anchor)
    anchor_matches_state = bool(
        anchor is not None
        and anchor["trade_date"] == normalized_last
        and anchor["checksum"] == state_checksum
        and anchor["row_count"] == len(state)
    )
    return {
        "required": bool(required),
        "last_materialized_trade_date": normalized_last,
        "state_checksum": state_checksum,
        "anchor": deepcopy(anchor),
        "pending_row_count": int(len(pending)),
        "pending_announce_date_min": pending_dates[0] if pending_dates else None,
        "pending_announce_date_max": pending_dates[-1] if pending_dates else None,
        "passed": bool(not required or (not pending_dates and anchor_matches_state)),
    }


def _latest_financial_anchor(
    current: dict[str, Any] | None,
    evidence: dict[str, Any] | None,
) -> dict[str, Any] | None:
    candidates: list[dict[str, Any]] = []
    normalized_current = _normalized_financial_anchor(current)
    if normalized_current is not None:
        candidates.append(normalized_current)
    if isinstance(evidence, dict) and evidence.get("mode") == "COMPLETED_PREDECESSOR_MANIFEST":
        seed_anchor = _normalized_financial_anchor(evidence)
        if seed_anchor is not None:
            candidates.append(seed_anchor)
    read_back = evidence.get("read_back_result") if isinstance(evidence, dict) else None
    partitions = read_back.get("partitions") if isinstance(read_back, dict) else None
    if isinstance(partitions, list):
        for record in partitions:
            normalized = _normalized_financial_anchor(record)
            if normalized is not None and record.get("materialized") is True:
                candidates.append(normalized)
    if not candidates:
        return None
    return deepcopy(max(candidates, key=lambda value: value["trade_date"]))


def _normalized_financial_anchor(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    object_key = value.get("object_key")
    checksum = value.get("checksum")
    row_count = value.get("row_count")
    trade_date = value.get("trade_date")
    if trade_date is None and isinstance(object_key, str):
        prefix = "raw/financial/trade_date="
        suffix = "/part.parquet"
        if object_key.startswith(prefix) and object_key.endswith(suffix):
            trade_date = object_key[len(prefix) : -len(suffix)]
    try:
        normalized_date = validate_trade_date(str(trade_date))
    except (TypeError, ValueError):
        return None
    if (
        object_key != build_partition("financial", normalized_date).object_key
        or not isinstance(checksum, str)
        or len(checksum) != 64
        or any(character not in "0123456789abcdef" for character in checksum)
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


def _financial_reducer_evidence_matches(
    evidence: dict[str, Any],
    *,
    chunk: dict[str, Any],
    financial_prior_frame: pd.DataFrame | None,
    financial_seed_evidence: dict[str, Any] | None,
    financial_terminal_evidence: dict[str, Any] | None,
) -> bool:
    validation = evidence.get("validation")
    reducer = validation.get("financial_reducer") if isinstance(validation, dict) else None
    prior = (
        financial_prior_frame
        if financial_prior_frame is not None
        else pd.DataFrame(columns=get_schema_contract("financial").columns)
    )
    expected = {
        "dependency_keys": list(chunk.get("dependency_keys", [])),
        "prior_state_checksum": dataframe_checksum(
            prior,
            key_columns=KEY_COLUMNS["financial"],
        ),
        "seed": deepcopy(financial_seed_evidence),
        "terminal": deepcopy(financial_terminal_evidence),
    }
    return reducer == expected


def _ensure_financial_seed(
    *,
    prior: pd.DataFrame | None,
    seed_loaded: bool,
    seed_evidence: dict[str, Any] | None,
    evidence: dict[str, Any],
    canonical_read_fn: Callable[[str, str], pd.DataFrame | None] | None,
    plan_scope: dict[str, Any],
) -> tuple[pd.DataFrame, bool, dict[str, Any] | None]:
    current = _merge_financial_source_state(
        pd.DataFrame(columns=get_schema_contract("financial").columns),
        prior
        if prior is not None
        else pd.DataFrame(columns=get_schema_contract("financial").columns),
    )
    if seed_loaded or canonical_read_fn is None:
        return current, seed_loaded, deepcopy(seed_evidence)
    coverage = evidence.get("coverage")
    dates = coverage.get("canonical_trade_dates") if isinstance(coverage, dict) else None
    if not isinstance(dates, list):
        raise BackfillExecutionError("DQ_FAILED", "financial canonical date coverage is missing")
    if not dates:
        return current, False, None
    try:
        first_trade_date = validate_trade_date(str(dates[0]))
    except (TypeError, ValueError) as exc:
        raise BackfillExecutionError("DQ_FAILED", "financial canonical date coverage is invalid") from exc
    seed_proof = plan_scope.get("financial_seed")
    if not isinstance(seed_proof, dict):
        raise BackfillExecutionError(
            "SEMANTIC_SOURCE_UNAVAILABLE",
            "financial materialization requires a completed predecessor seed manifest",
        )
    predecessor = coverage.get("predecessor_trade_date")
    if predecessor != seed_proof.get("trade_date") or not isinstance(predecessor, str):
        raise BackfillExecutionError(
            "SEMANTIC_SOURCE_UNAVAILABLE",
            "financial seed is not the proven predecessor trading partition",
        )
    if predecessor >= first_trade_date:
        raise BackfillExecutionError("DQ_FAILED", "financial predecessor date is not before materialization")
    object_key = build_partition("financial", predecessor).object_key
    if object_key != seed_proof.get("object_key"):
        raise BackfillExecutionError("SEMANTIC_SOURCE_UNAVAILABLE", "financial seed object identity is invalid")
    if set(seed_proof.get("coverage_codes", [])) != set(plan_scope.get("codes", [])):
        raise BackfillExecutionError("SEMANTIC_SOURCE_UNAVAILABLE", "financial seed universe is incomplete")
    try:
        seed = canonical_read_fn("financial", predecessor)
    except FileNotFoundError as exc:
        raise BackfillExecutionError("SEMANTIC_SOURCE_UNAVAILABLE", "financial seed object is missing") from exc
    except Exception as exc:
        raise BackfillExecutionError("READBACK_FAILED", "financial seed read failed") from exc
    if not isinstance(seed, pd.DataFrame):
        raise BackfillExecutionError("READBACK_FAILED", "financial seed is not a DataFrame")
    columns = list(get_schema_contract("financial").columns)
    if list(seed.columns) != columns:
        raise BackfillExecutionError("READBACK_FAILED", "financial seed schema is invalid")
    _validate_canonical_frame("financial", seed, predecessor)
    scoped_seed = _financial_scope_frame(seed, plan_scope)
    checksum = dataframe_checksum(scoped_seed, key_columns=KEY_COLUMNS["financial"])
    if checksum != seed_proof.get("checksum") or len(scoped_seed) != seed_proof.get("row_count"):
        raise BackfillExecutionError("READBACK_FAILED", "financial seed manifest does not match canonical scope")
    merged = _merge_financial_source_state(scoped_seed, current)
    return (
        merged,
        True,
        {
            "mode": "COMPLETED_PREDECESSOR_MANIFEST",
            "trade_date": predecessor,
            "object_key": object_key,
            "checksum": checksum,
            "row_count": len(scoped_seed),
            "coverage_codes": list(plan_scope.get("codes", [])),
            "source_key": seed_proof.get("source_key"),
            "source_manifest_fingerprint": seed_proof.get("manifest_fingerprint"),
            "exact_read_back_success": True,
        },
    )


def _financial_scope_frame(
    frame: pd.DataFrame | None,
    plan_scope: dict[str, Any],
) -> pd.DataFrame:
    columns = list(get_schema_contract("financial").columns)
    if frame is None:
        return pd.DataFrame(columns=columns)
    if not isinstance(frame, pd.DataFrame) or list(frame.columns) != columns:
        raise BackfillExecutionError("READBACK_FAILED", "financial canonical schema is invalid")
    codes = plan_scope.get("codes")
    if not isinstance(codes, list) or not codes:
        raise BackfillExecutionError("CONFIGURATION_ERROR", "financial plan universe is invalid")
    scoped = frame.loc[frame["stock_code"].astype(str).isin(codes), columns].copy(deep=True)
    return _sorted_frame(scoped, KEY_COLUMNS["financial"])


def _st_scope_has_rows(
    existing: pd.DataFrame | None,
    chunk: dict[str, Any],
    scope_codes: Any = None,
) -> bool:
    if existing is None or existing.empty:
        return False
    if not isinstance(existing, pd.DataFrame):
        raise BackfillExecutionError("READBACK_FAILED", "ST canonical read-back is not a DataFrame")
    required = {"stock_code", "start_date", "end_date"}
    if not required.issubset(existing.columns):
        raise BackfillExecutionError("READBACK_FAILED", "ST canonical read-back schema is invalid")
    codes = scope_codes if isinstance(scope_codes, list) and scope_codes else chunk.get("codes", [])
    scoped = existing[existing["stock_code"].astype(str).isin(codes)]
    if scoped.empty:
        return False
    starts = scoped["start_date"].astype(str)
    ends = scoped["end_date"].fillna("9999-12-31").astype(str)
    return bool(((starts <= chunk["end_date"]) & (ends > chunk["start_date"])).any())


def _stock_scope_has_rows(
    existing: pd.DataFrame | None,
    chunk: dict[str, Any],
    scope_codes: Any = None,
) -> bool:
    if existing is None or existing.empty:
        return False
    if not isinstance(existing, pd.DataFrame) or "stock_code" not in existing.columns:
        raise BackfillExecutionError("READBACK_FAILED", "stock_basic canonical read-back schema is invalid")
    codes = scope_codes if isinstance(scope_codes, list) and scope_codes else chunk.get("codes", [])
    return bool(existing["stock_code"].astype(str).isin(codes).any())


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
    if dataset == "stock_basic":
        try:
            normalized_trade_date = validate_trade_date(trade_date)
            list_dates = frame["list_date"].map(
                lambda value: validate_trade_date(str(value))
            )
            delist_dates = frame["delist_date"].dropna().map(
                lambda value: validate_trade_date(str(value))
            )
        except (TypeError, ValueError) as exc:
            raise BackfillExecutionError(
                "DQ_FAILED",
                "stock_basic membership dates are invalid",
            ) from exc
        if (list_dates > normalized_trade_date).any():
            raise BackfillExecutionError(
                "DQ_FAILED",
                "stock_basic list_date is after canonical date",
            )
        if (delist_dates <= normalized_trade_date).any():
            raise BackfillExecutionError(
                "DQ_FAILED",
                "stock_basic delist_date does not include canonical date",
            )
    elif dataset == "st_history":
        if (
            {value.strip().upper() for value in frame["st_type"].astype(str)}
            & {value.upper() for value in CURRENT_ST_MARKERS}
            or {value.strip().lower() for value in frame["source"].astype(str)}
            & {value.lower() for value in CURRENT_SOURCE_MARKERS}
        ):
            raise BackfillExecutionError(
                "DQ_FAILED",
                "current ST snapshot markers cannot enter canonical history",
            )
    elif dataset == "financial":
        _validate_financial_state(frame, trade_date)


def _validate_financial_state(frame: pd.DataFrame, as_of_date: str) -> None:
    _validate_financial_source_state(frame)
    try:
        announce_dates = frame["announce_date"].map(lambda value: validate_trade_date(str(value)))
        normalized_as_of = validate_trade_date(as_of_date)
    except (TypeError, ValueError) as exc:
        raise BackfillExecutionError("DQ_FAILED", "financial point-in-time dates are invalid") from exc
    if (announce_dates > normalized_as_of).any():
        raise BackfillExecutionError("DQ_FAILED", "financial announcement is after canonical date")


def _validate_financial_source_state(frame: pd.DataFrame) -> None:
    keys = KEY_COLUMNS["financial"]
    if frame.duplicated(keys).any():
        raise BackfillExecutionError("DQ_FAILED", "financial source keys are duplicated")
    contract = get_schema_contract("financial")
    for column in contract.numeric_columns:
        values = pd.to_numeric(frame[column], errors="coerce")
        if values.isna().any() or not np.isfinite(values.to_numpy(dtype=float)).all():
            raise BackfillExecutionError("DQ_FAILED", f"financial.{column} must be finite")
    try:
        report_periods = frame["report_period"].map(lambda value: validate_trade_date(str(value)))
        announce_dates = frame["announce_date"].map(lambda value: validate_trade_date(str(value)))
    except (TypeError, ValueError) as exc:
        raise BackfillExecutionError("DQ_FAILED", "financial source dates are invalid") from exc
    if (report_periods > announce_dates).any():
        raise BackfillExecutionError("DQ_FAILED", "financial report period is after announcement")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

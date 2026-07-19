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

import numpy as np
import pandas as pd
from pandas.api.types import (
    is_float_dtype,
    is_integer_dtype,
    is_object_dtype,
    is_string_dtype,
)

from stock_selector.data.data_validator import (
    DataValidationError,
    validate_dataset_frame,
)
from stock_selector.data.historical_backfill import dataframe_checksum
from stock_selector.data.real_clean_universe import (
    OUTPUT_KEY_COLUMNS as GOAL22_OUTPUT_KEY_COLUMNS,
    build_goal22_processed_commit_key,
)
from stock_selector.factors.factor_validator import (
    FACTOR_DAILY_COLUMNS,
    validate_factor_daily,
)
from stock_selector.factors.real_factor_daily import (
    GOAL23_DOWNSTREAM_FIREWALLS,
    Goal22ManifestRecord,
    _validate_goal22_commit_for_manifest,
    _validate_goal22_daily_report,
    audit_factor_contract,
    build_goal23_factor_commit_key,
    build_goal23_factor_generation_key,
    build_real_factor_daily_output_keys,
    load_goal22_manifest_catalog,
    normalize_factor_daily_read_back,
    validate_goal23_factor_commit_payload,
)
from stock_selector.providers.schema_contract import get_schema_contract
from stock_selector.scoring.risk_level import determine_risk_level
from stock_selector.scoring.rule_explainer import (
    build_reason,
    build_suggestion,
)
from stock_selector.scoring.score_engine import (
    SCORE_WEIGHT_COLUMNS,
    ScoringConfig,
    parse_scoring_config,
)
from stock_selector.scoring.selection_builder import build_selection_result
from stock_selector.scoring.selection_snapshot_repo import (
    summarize_selection_result,
)
from stock_selector.scoring.selection_validator import (
    SELECTION_RESULT_COLUMNS,
    SELECTION_SCORE_COLUMNS,
    validate_selection_result,
)
from stock_selector.utils.date_validator import (
    validate_date_range,
    validate_trade_date,
)


EXPECTED_V1_WEIGHTS = {
    "quality_score": 0.30,
    "growth_score": 0.25,
    "valuation_score": 0.20,
    "industry_score": 0.15,
    "trend_score": 0.10,
}

REAL_SELECTION_TOP_N = 50

GOAL22_SELECTION_INPUT_DATASETS = (
    "risk_filter",
    "eligible_universe",
    "factor_input_table",
)

GOAL22_ARROW_SCHEMA_EVIDENCE_ATTR = (
    "_goal24_goal22_arrow_schema_evidence"
)

SELECTION_TEXT_COLUMNS = (
    "stock_code",
    "trade_date",
    "industry",
    "market_type",
    "risk_level",
    "suggestion",
    "reason",
    "exclude_reasons",
    "risk_flags",
)

SELECTION_FLOAT_COLUMNS = tuple(SELECTION_SCORE_COLUMNS)

GOAL24_FIREWALLS = {
    "provider_call": False,
    "backtest": False,
    "llm": False,
    "api_page_scheduler": False,
    "auto_trading": False,
}

_RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_GOAL23_MANIFEST_KEY_PATTERN = re.compile(
    r"candidate/real_factor_daily/"
    r"run_id=([A-Za-z0-9][A-Za-z0-9._-]{0,127})/manifest\.json"
)


class Goal24InputError(ValueError):
    """A trusted-input failure that blocks one Goal 24 selection date."""


ReadJsonFn = Callable[[str], dict[str, Any]]
WriteJsonFn = Callable[[str, dict[str, Any]], str]
ReadParquetFn = Callable[[str], pd.DataFrame]
ReadGoal23CommitFn = Callable[[str], dict[str, Any]]
ReadGoal22CommitFn = Callable[[str], dict[str, Any]]
ReadSelectionObjectFn = Callable[[str], pd.DataFrame]
WriteSelectionObjectFn = Callable[[str, str, str, pd.DataFrame], str]
ReadSelectionCommitFn = Callable[[str, str], dict[str, Any]]
WriteSelectionCommitFn = Callable[[str, str, dict[str, Any]], str]
ReadSnapshotFn = Callable[[str, str], dict[str, Any] | None]
UpsertSnapshotFn = Callable[[dict[str, Any]], None]


@dataclass(frozen=True)
class Goal23ManifestRecord:
    object_key: str
    checksum: str
    payload: dict[str, Any]
    trade_dates: tuple[str, ...]
    validation_errors: tuple[str, ...]

    @property
    def valid(self) -> bool:
        return not self.validation_errors


@dataclass(frozen=True)
class Goal23ManifestCatalog:
    records: tuple[Goal23ManifestRecord, ...]

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
class Goal22SelectionInputs:
    risk_filter: pd.DataFrame
    eligible_universe: pd.DataFrame
    factor_input_table: pd.DataFrame
    lineage: dict[str, Any]


@dataclass(frozen=True)
class Goal24TrustedInputs:
    factor_daily: pd.DataFrame
    risk_filter: pd.DataFrame
    eligible_universe: pd.DataFrame
    factor_input_table: pd.DataFrame
    goal23_lineage: dict[str, Any]
    goal22_lineage: dict[str, Any]
    factor_contract_audit: dict[str, Any]


def load_goal23_manifest_catalog(
    *,
    manifest_keys: Iterable[str],
    read_json_fn: ReadJsonFn,
) -> Goal23ManifestCatalog:
    keys = [str(value) for value in manifest_keys]
    if not keys:
        raise ValueError("at least one Goal 23 manifest key is required")
    if len(keys) != len(set(keys)):
        raise ValueError("Goal 23 manifest keys must be unique")

    records: list[Goal23ManifestRecord] = []
    for object_key in keys:
        match = _GOAL23_MANIFEST_KEY_PATTERN.fullmatch(object_key)
        if match is None:
            raise ValueError(f"invalid Goal 23 manifest key: {object_key}")
        try:
            payload = read_json_fn(object_key)
        except FileNotFoundError as exc:
            raise FileNotFoundError(
                f"missing Goal 23 manifest: {object_key}"
            ) from exc
        if not isinstance(payload, dict):
            raise ValueError(
                f"Goal 23 manifest must be a JSON object: {object_key}"
            )
        trade_dates = _tentative_goal23_trade_dates(payload)
        errors: list[str] = []
        try:
            _validate_goal23_manifest_payload(
                object_key=object_key,
                payload=payload,
                key_run_id=match.group(1),
            )
        except (DataValidationError, ValueError, KeyError, TypeError) as exc:
            errors.append(_safe_message(exc))
        records.append(
            Goal23ManifestRecord(
                object_key=object_key,
                checksum=_stable_hash(payload),
                payload=deepcopy(payload),
                trade_dates=tuple(trade_dates),
                validation_errors=tuple(errors),
            )
        )
    return Goal23ManifestCatalog(records=tuple(records))


def freeze_real_selection_config(
    raw_config: dict[str, Any],
) -> dict[str, Any]:
    if not isinstance(raw_config, dict):
        raise ValueError("selection config must be a mapping")
    scoring = raw_config.get("scoring")
    if (
        not isinstance(scoring, dict)
        or type(scoring.get("top_n")) is not int
        or scoring["top_n"] != REAL_SELECTION_TOP_N
    ):
        raise ValueError(
            f"real selection top_n must be the integer "
            f"{REAL_SELECTION_TOP_N}"
        )
    parsed = parse_scoring_config(raw_config)
    for column, expected in EXPECTED_V1_WEIGHTS.items():
        actual = parsed.weights[column]
        if actual != expected:
            raise ValueError(
                "selection weights must match the first-version contract: "
                f"{column} expected {expected}, got {actual}"
            )
    return {
        "weights": {
            column: parsed.weights[column]
            for column in SCORE_WEIGHT_COLUMNS
        },
        "null_score_policy": parsed.null_score_policy,
        "neutral_score": parsed.neutral_score,
        "top_n": parsed.top_n,
    }


def build_real_selection_output_keys(
    run_id: str,
    selection_dates: Iterable[str],
    rebalance_mode: str,
) -> dict[str, Any]:
    run_id = _validate_run_id(run_id)
    dates = _normalize_trade_dates(selection_dates)
    mode = _validate_rebalance_mode(rebalance_mode)
    root = f"candidate/real_selection_result/run_id={run_id}"
    return {
        "range_manifest": f"{root}/manifest.json",
        "daily_reports": {
            trade_date: (
                f"{root}/trade_date={trade_date}/"
                f"rebalance_mode={mode}/dq_report.json"
            )
            for trade_date in dates
        },
        "processed": {
            trade_date: (
                f"processed/selection_result/trade_date={trade_date}/"
                f"rebalance_mode={mode}/part.parquet"
            )
            for trade_date in dates
        },
        "processed_commits": {
            trade_date: build_goal24_selection_commit_key(
                trade_date,
                mode,
            )
            for trade_date in dates
        },
    }


def build_goal24_selection_generation_key(
    trade_date: str,
    rebalance_mode: str,
    generation_id: str,
) -> str:
    trade_date = validate_trade_date(trade_date)
    mode = _validate_rebalance_mode(rebalance_mode)
    if re.fullmatch(r"[0-9a-f]{64}", str(generation_id)) is None:
        raise ValueError("generation_id must be sha256 hex")
    return (
        f"processed/selection_result/trade_date={trade_date}/"
        f"rebalance_mode={mode}/generation={generation_id}/part.parquet"
    )


def build_goal24_selection_commit_key(
    trade_date: str,
    rebalance_mode: str,
) -> str:
    trade_date = validate_trade_date(trade_date)
    mode = _validate_rebalance_mode(rebalance_mode)
    return (
        f"processed/_goal24_selection_commits/trade_date={trade_date}/"
        f"rebalance_mode={mode}/commit.json"
    )


def run_real_selection_result_range(
    *,
    run_id: str,
    start_date: str,
    end_date: str,
    selection_dates: Iterable[str],
    rebalance_mode: str,
    goal23_manifest_catalog: Goal23ManifestCatalog,
    selection_config: dict[str, Any],
    control_read_json_fn: ReadJsonFn,
    control_write_json_fn: WriteJsonFn | None,
    goal23_factor_object_read_fn: ReadParquetFn,
    goal23_commit_read_fn: ReadGoal23CommitFn,
    goal22_processed_object_read_fn: ReadParquetFn,
    goal22_commit_read_fn: ReadGoal22CommitFn,
    selection_object_read_fn: ReadSelectionObjectFn | None = None,
    selection_object_write_fn: WriteSelectionObjectFn | None = None,
    selection_commit_read_fn: ReadSelectionCommitFn | None = None,
    selection_commit_write_fn: WriteSelectionCommitFn | None = None,
    snapshot_read_fn: ReadSnapshotFn | None = None,
    snapshot_upsert_fn: UpsertSnapshotFn | None = None,
    apply_processed_write: bool = False,
    resume: bool = True,
    force: bool = False,
    generated_at_fn: Callable[[], str] | None = None,
) -> dict[str, Any]:
    run_id = _validate_run_id(run_id)
    start_date, end_date = validate_date_range(start_date, end_date)
    dates = _normalize_trade_dates(selection_dates)
    mode = _validate_rebalance_mode(rebalance_mode)
    if not dates:
        raise ValueError("selection_dates must not be empty")
    if dates[0] < start_date or dates[-1] > end_date:
        raise ValueError(
            "selection_dates must be within start_date and end_date"
        )
    if not isinstance(goal23_manifest_catalog, Goal23ManifestCatalog):
        raise TypeError(
            "goal23_manifest_catalog must be a Goal23ManifestCatalog"
        )
    apply_callbacks = (
        control_write_json_fn,
        selection_object_read_fn,
        selection_object_write_fn,
        selection_commit_read_fn,
        selection_commit_write_fn,
        snapshot_read_fn,
        snapshot_upsert_fn,
    )
    if apply_processed_write and any(
        callback is None for callback in apply_callbacks
    ):
        raise ValueError(
            "selection generation, commit, control and snapshot functions "
            "are required with --apply"
        )

    frozen_config = freeze_real_selection_config(selection_config)
    config_fingerprint = _stable_hash(frozen_config)
    generated_at_fn = generated_at_fn or _utc_now_iso
    output_keys = build_real_selection_output_keys(run_id, dates, mode)
    schema_contract = selection_result_physical_schema_contract()
    plan = {
        "schema_version": "goal24.real_selection_result_plan.v1",
        "run_id": run_id,
        "start_date": start_date,
        "end_date": end_date,
        "selection_dates": dates,
        "selection_date_source": "EXPLICIT_CLI_GOAL23_MANIFESTS",
        "rebalance_mode": mode,
        "goal23_manifests": goal23_manifest_catalog.plan_summary(),
        "selection_config": frozen_config,
        "selection_config_fingerprint": config_fingerprint,
        "selection_result_columns": list(SELECTION_RESULT_COLUMNS),
        "physical_schema": schema_contract,
        "physical_schema_fingerprint": _stable_hash(schema_contract),
        "top_n": REAL_SELECTION_TOP_N,
        "lineage_policy": {
            "goal23_committed_generation_only": True,
            "goal22_lineage_required": True,
            "legacy_part_fallback": False,
            "minimum_effective_factor_count": 15,
            "factor_input_table_required": True,
            "risk_filter_missing_row_policy": "BLOCK",
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
        for trade_date in dates
    }
    date_statuses = {trade_date: "PENDING" for trade_date in dates}
    mode_name = "APPLY" if apply_processed_write else "DRY_RUN"
    date_results: dict[str, dict[str, Any]] = {}

    def manifest_payload(status: str) -> dict[str, Any]:
        return {
            "schema_version": "goal24.real_selection_result_manifest.v1",
            "goal": "24",
            "run_id": run_id,
            "generated_at": generated_at_fn(),
            "status": status,
            "mode": mode_name,
            "apply_requested": bool(apply_processed_write),
            "resume": bool(resume),
            "force": bool(force),
            "plan": plan,
            "plan_fingerprint": plan_fingerprint,
            "date_statuses": dict(date_statuses),
            "date_attempts": dict(date_attempts),
            "status_counts": dict(
                sorted(Counter(date_statuses.values()).items())
            ),
            "daily_report_keys": output_keys["daily_reports"],
            "processed_output_keys": output_keys["processed"],
            "processed_commit_keys": output_keys["processed_commits"],
            "firewalls": deepcopy(GOAL24_FIREWALLS),
        }

    if apply_processed_write:
        assert control_write_json_fn is not None
        control_write_json_fn(
            output_keys["range_manifest"],
            manifest_payload("RUNNING"),
        )

    for trade_date in dates:
        report = _empty_daily_report(
            run_id=run_id,
            trade_date=trade_date,
            rebalance_mode=mode,
            mode=mode_name,
            attempt=date_attempts[trade_date],
            plan_fingerprint=plan_fingerprint,
            selection_config=frozen_config,
            config_fingerprint=config_fingerprint,
            goal23_manifests=goal23_manifest_catalog.plan_summary(),
            logical_output_key=output_keys["processed"][trade_date],
            commit_key=output_keys["processed_commits"][trade_date],
            generated_at=generated_at_fn(),
        )
        if apply_processed_write:
            assert control_write_json_fn is not None
            control_write_json_fn(
                output_keys["daily_reports"][trade_date],
                report,
            )
            date_statuses[trade_date] = "RUNNING"
            control_write_json_fn(
                output_keys["range_manifest"],
                manifest_payload("RUNNING"),
            )
        try:
            report = _run_one_selection_date(
                run_id=run_id,
                trade_date=trade_date,
                rebalance_mode=mode,
                plan_fingerprint=plan_fingerprint,
                selection_config=frozen_config,
                config_fingerprint=config_fingerprint,
                goal23_manifest_catalog=goal23_manifest_catalog,
                started_report=report,
                control_read_json_fn=control_read_json_fn,
                goal23_factor_object_read_fn=goal23_factor_object_read_fn,
                goal23_commit_read_fn=goal23_commit_read_fn,
                goal22_processed_object_read_fn=(
                    goal22_processed_object_read_fn
                ),
                goal22_commit_read_fn=goal22_commit_read_fn,
                selection_object_read_fn=selection_object_read_fn,
                selection_object_write_fn=selection_object_write_fn,
                selection_commit_read_fn=selection_commit_read_fn,
                selection_commit_write_fn=selection_commit_write_fn,
                snapshot_read_fn=snapshot_read_fn,
                snapshot_upsert_fn=snapshot_upsert_fn,
                apply_processed_write=apply_processed_write,
                resume=resume,
                force=force,
                logical_output_key=output_keys["processed"][trade_date],
                commit_key=output_keys["processed_commits"][trade_date],
                generated_at_fn=generated_at_fn,
            )
        except Goal24InputError as exc:
            report["status"] = "BLOCKED"
            report["dq_status"] = "BLOCKED"
            report["blocked_reasons"] = [
                f"TRUSTED_INPUT_BLOCKED:{_safe_message(exc)}"
            ]
            report["failure"] = _failure_record(exc)
            if apply_processed_write:
                report["commit"]["status"] = "UNCOMMITTED"
        except (
            DataValidationError,
            ValueError,
            KeyError,
            TypeError,
        ) as exc:
            report["status"] = "BLOCKED"
            report["dq_status"] = "BLOCKED"
            report["blocked_reasons"] = [
                f"SELECTION_DQ_BLOCKED:{type(exc).__name__}:"
                f"{_safe_message(exc)}"
            ]
            report["failure"] = _failure_record(exc)
            if apply_processed_write:
                report["commit"]["status"] = "UNCOMMITTED"
        except Exception as exc:
            report["status"] = "FAILED"
            report["dq_status"] = "BLOCKED"
            report["blocked_reasons"] = [
                f"DATE_EXECUTION_FAILED:{type(exc).__name__}"
            ]
            report["failure"] = _failure_record(exc)
            if apply_processed_write:
                report["commit"]["status"] = "UNCOMMITTED"

        date_results[trade_date] = deepcopy(report)
        date_statuses[trade_date] = report["status"]
        if apply_processed_write:
            assert control_write_json_fn is not None
            control_write_json_fn(
                output_keys["daily_reports"][trade_date],
                report,
            )
            control_write_json_fn(
                output_keys["range_manifest"],
                manifest_payload("RUNNING"),
            )

    status = _range_status(
        date_statuses,
        apply_processed_write=apply_processed_write,
    )
    manifest = manifest_payload(status)
    if apply_processed_write:
        assert control_write_json_fn is not None
        control_write_json_fn(output_keys["range_manifest"], manifest)
    return {
        "goal": "24",
        "run_id": run_id,
        "status": status,
        "mode": mode_name,
        "rebalance_mode": mode,
        "apply_requested": bool(apply_processed_write),
        "execution_plan": deepcopy(plan),
        "date_statuses": dict(date_statuses),
        "status_counts": manifest["status_counts"],
        "date_results": date_results,
        "range_manifest_key": (
            output_keys["range_manifest"] if apply_processed_write else None
        ),
        "daily_report_keys": (
            output_keys["daily_reports"] if apply_processed_write else {}
        ),
        "processed_output_keys": output_keys["processed"],
        "processed_commit_keys": output_keys["processed_commits"],
        "firewalls": deepcopy(GOAL24_FIREWALLS),
        "manifest_persisted": bool(apply_processed_write),
        "manifest": manifest if apply_processed_write else None,
    }


def read_goal24_published_selection_result(
    *,
    trade_date: str,
    rebalance_mode: str,
    selection_commit_read_fn: ReadSelectionCommitFn,
    selection_object_read_fn: ReadSelectionObjectFn,
) -> pd.DataFrame:
    trade_date = validate_trade_date(trade_date)
    mode = _validate_rebalance_mode(rebalance_mode)
    commit = selection_commit_read_fn(trade_date, mode)
    validate_goal24_selection_commit_payload(commit, trade_date, mode)
    output = commit["output"]
    frame = normalize_selection_result_read_back(
        selection_object_read_fn(output["object_key"])
    )
    config = _scoring_config_from_frozen(commit["selection_config"])
    validate_real_selection_result(
        frame,
        trade_date,
        scoring_config=config,
    )
    checksum = dataframe_checksum(
        frame,
        key_columns=["stock_code", "trade_date"],
    )
    if len(frame) != output["row_count"] or checksum != output["checksum"]:
        raise DataValidationError(
            "Goal 24 committed selection_result checksum mismatch for "
            f"{trade_date} {mode}"
        )
    return frame


def validate_goal24_selection_commit_payload(
    payload: dict[str, Any],
    trade_date: str,
    rebalance_mode: str,
) -> None:
    trade_date = validate_trade_date(trade_date)
    mode = _validate_rebalance_mode(rebalance_mode)
    run_id = payload.get("run_id") if isinstance(payload, dict) else None
    config = payload.get("selection_config") if isinstance(payload, dict) else None
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version")
        != "goal24.selection_date_commit.v1"
        or payload.get("goal") != "24"
        or payload.get("status") != "COMMITTED"
        or not isinstance(run_id, str)
        or _RUN_ID_PATTERN.fullmatch(run_id) is None
        or payload.get("trade_date") != trade_date
        or payload.get("rebalance_mode") != mode
        or payload.get("firewalls") != GOAL24_FIREWALLS
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
            str(payload.get("selection_config_fingerprint", "")),
        )
        is None
        or not isinstance(config, dict)
    ):
        raise DataValidationError("invalid Goal 24 selection commit payload")
    frozen_config = freeze_real_selection_config(_raw_config(config))
    if (
        frozen_config != config
        or _stable_hash(config)
        != payload["selection_config_fingerprint"]
    ):
        raise DataValidationError(
            "invalid Goal 24 selection config fingerprint"
        )
    output = payload.get("output")
    generation_id = payload["generation_id"]
    schema_contract = selection_result_physical_schema_contract()
    schema_fingerprint = _stable_hash(schema_contract)
    if (
        not isinstance(output, dict)
        or output.get("dataset") != "selection_result"
        or output.get("object_key")
        != build_goal24_selection_generation_key(
            trade_date,
            mode,
            generation_id,
        )
        or output.get("logical_key")
        != (
            f"processed/selection_result/trade_date={trade_date}/"
            f"rebalance_mode={mode}/part.parquet"
        )
        or isinstance(output.get("row_count"), bool)
        or not isinstance(output.get("row_count"), int)
        or output["row_count"] <= 0
        or output["row_count"] > REAL_SELECTION_TOP_N
        or re.fullmatch(
            r"[0-9a-f]{64}",
            str(output.get("checksum", "")),
        )
        is None
        or output.get("physical_schema_fingerprint")
        != schema_fingerprint
        or output.get("dq_status") != "PASS"
    ):
        raise DataValidationError(
            "invalid Goal 24 selection generation mapping"
        )
    expected_generation_id = _selection_generation_id(
        trade_date=trade_date,
        rebalance_mode=mode,
        input_fingerprint=payload["input_fingerprint"],
        config_fingerprint=payload["selection_config_fingerprint"],
        output_checksum=output["checksum"],
        row_count=output["row_count"],
        physical_schema_fingerprint=schema_fingerprint,
    )
    if generation_id != expected_generation_id:
        raise DataValidationError("invalid Goal 24 generation fingerprint")


def selection_result_physical_schema_contract() -> dict[str, str]:
    return {
        column: (
            "string"
            if column in SELECTION_TEXT_COLUMNS
            else "int64"
            if column == "rank"
            else "float64"
        )
        for column in SELECTION_RESULT_COLUMNS
    }


def validate_goal22_selection_input_arrow_schema(
    dataset: str,
    schema: Any,
) -> dict[str, str]:
    import pyarrow as pa

    if dataset not in GOAL22_SELECTION_INPUT_DATASETS:
        raise ValueError(
            f"unsupported Goal 22 selection input dataset: {dataset}"
        )
    if not isinstance(schema, pa.Schema):
        raise TypeError(
            "Goal 22 selection input Arrow schema must be a pyarrow.Schema"
        )
    contract = get_schema_contract(dataset)
    actual_columns = list(schema.names)
    if actual_columns != contract.columns:
        raise DataValidationError(
            f"{dataset} Parquet/Arrow columns must exactly match the "
            f"published schema: expected {contract.columns}, "
            f"got {actual_columns}"
        )
    physical_schema: dict[str, str] = {}
    for column in contract.columns:
        arrow_type = schema.field(column).type
        if column in contract.bool_columns:
            if arrow_type != pa.bool_():
                raise DataValidationError(
                    f"{column} Parquet/Arrow type must be bool, "
                    f"got {arrow_type}"
                )
        elif column in contract.numeric_columns:
            if arrow_type not in {pa.float64(), pa.int64()}:
                raise DataValidationError(
                    f"{column} Parquet/Arrow type must be a 64-bit "
                    f"numeric type, got {arrow_type}"
                )
        elif arrow_type != pa.string():
            raise DataValidationError(
                f"{column} Parquet/Arrow type must be string, "
                f"got {arrow_type}"
            )
        physical_schema[column] = str(arrow_type)
    return physical_schema


def normalize_selection_result_frame(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("selection_result frame must be a DataFrame")
    actual_columns = list(frame.columns)
    if actual_columns != SELECTION_RESULT_COLUMNS:
        raise DataValidationError(
            "selection_result columns must exactly match the published "
            f"schema: expected {SELECTION_RESULT_COLUMNS}, "
            f"got {actual_columns}"
        )
    result = frame.loc[:, SELECTION_RESULT_COLUMNS].copy(deep=True)
    for column in SELECTION_TEXT_COLUMNS:
        result[column] = result[column].astype("string")
    for column in SELECTION_FLOAT_COLUMNS:
        try:
            numeric = pd.to_numeric(result[column], errors="raise")
        except (TypeError, ValueError) as exc:
            raise DataValidationError(
                f"{column} must contain only numeric values"
            ) from exc
        result[column] = numeric.astype("Float64")
    try:
        rank_numeric = pd.to_numeric(result["rank"], errors="raise")
    except (TypeError, ValueError) as exc:
        raise DataValidationError("rank must contain only integer values") from exc
    if not rank_numeric.empty and (
        rank_numeric.isna().any()
        or (rank_numeric.astype(float) % 1 != 0).any()
    ):
        raise DataValidationError("rank must contain only integer values")
    result["rank"] = rank_numeric.astype("Int64")
    return result


def normalize_selection_result_read_back(
    frame: pd.DataFrame,
) -> pd.DataFrame:
    validate_selection_result_raw_schema(frame)
    return normalize_selection_result_frame(frame)


def validate_selection_result_raw_schema(frame: pd.DataFrame) -> None:
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("selection_result frame must be a DataFrame")
    if list(frame.columns) != SELECTION_RESULT_COLUMNS:
        raise DataValidationError(
            "selection_result columns must exactly match the published schema"
        )
    for column in SELECTION_TEXT_COLUMNS:
        dtype = frame[column].dtype
        values = frame[column].dropna()
        if (
            isinstance(dtype, pd.CategoricalDtype)
            or not (is_string_dtype(dtype) or is_object_dtype(dtype))
            or not all(isinstance(value, str) for value in values)
        ):
            raise DataValidationError(
                f"{column} raw pandas dtype and values must be string, "
                f"got {dtype}"
            )
    for column in SELECTION_FLOAT_COLUMNS:
        dtype = frame[column].dtype
        if not is_float_dtype(dtype) or getattr(dtype, "itemsize", None) != 8:
            raise DataValidationError(
                f"{column} raw pandas dtype must be float64, got {dtype}"
            )
    rank_dtype = frame["rank"].dtype
    if (
        not is_integer_dtype(rank_dtype)
        or getattr(rank_dtype, "itemsize", None) != 8
    ):
        raise DataValidationError(
            f"rank raw pandas dtype must be int64, got {rank_dtype}"
        )


def validate_selection_result_arrow_schema(schema: Any) -> None:
    import pyarrow as pa

    if not isinstance(schema, pa.Schema):
        raise TypeError(
            "selection_result Arrow schema must be a pyarrow.Schema"
        )
    if list(schema.names) != SELECTION_RESULT_COLUMNS:
        raise DataValidationError(
            "selection_result Parquet/Arrow columns must exactly match "
            "the published schema"
        )
    for column in SELECTION_TEXT_COLUMNS:
        arrow_type = schema.field(column).type
        if arrow_type != pa.string():
            raise DataValidationError(
                f"{column} Parquet/Arrow type must be string, "
                f"got {arrow_type}"
            )
    for column in SELECTION_FLOAT_COLUMNS:
        arrow_type = schema.field(column).type
        if arrow_type != pa.float64():
            raise DataValidationError(
                f"{column} Parquet/Arrow type must be float64, "
                f"got {arrow_type}"
            )
    rank_type = schema.field("rank").type
    if rank_type != pa.int64():
        raise DataValidationError(
            f"rank Parquet/Arrow type must be int64, got {rank_type}"
        )


def validate_real_selection_result(
    frame: pd.DataFrame,
    trade_date: str,
    *,
    scoring_config: ScoringConfig,
) -> None:
    trade_date = validate_trade_date(trade_date)
    validate_selection_result(frame, trade_date)
    if scoring_config.top_n != REAL_SELECTION_TOP_N:
        raise DataValidationError(
            f"real selection top_n must equal {REAL_SELECTION_TOP_N}"
        )
    if len(frame) > REAL_SELECTION_TOP_N:
        raise DataValidationError(
            f"selection_result must contain at most {REAL_SELECTION_TOP_N} rows"
        )
    ordered = frame.sort_values(
        ["total_score", "stock_code"],
        ascending=[False, True],
        kind="mergesort",
    ).reset_index(drop=True)
    actual = frame.reset_index(drop=True)
    if actual["stock_code"].astype(str).tolist() != ordered[
        "stock_code"
    ].astype(str).tolist():
        raise DataValidationError(
            "selection_result must use stock_code ascending as score tie-break"
        )
    for row in actual.to_dict(orient="records"):
        expected_score = round(
            sum(
                float(row[column]) * scoring_config.weights[column]
                for column in SCORE_WEIGHT_COLUMNS
            ),
            6,
        )
        if not math.isclose(
            float(row["total_score"]),
            expected_score,
            rel_tol=0.0,
            abs_tol=5e-7,
        ):
            raise DataValidationError(
                "total_score cannot be recomputed from frozen weights"
            )
        expected_risk = determine_risk_level(
            total_score=row["total_score"],
            quality_score=row["quality_score"],
            growth_score=row["growth_score"],
            risk_flags=row["risk_flags"],
        )
        if str(row["risk_level"]) != expected_risk:
            raise DataValidationError("risk_level is not deterministic")
        expected_reason = build_reason(row)
        expected_suggestion = build_suggestion(row)
        if str(row["reason"]) != expected_reason:
            raise DataValidationError("reason is not deterministic")
        if str(row["suggestion"]) != expected_suggestion:
            raise DataValidationError("suggestion is not deterministic")


def _run_one_selection_date(
    *,
    run_id: str,
    trade_date: str,
    rebalance_mode: str,
    plan_fingerprint: str,
    selection_config: dict[str, Any],
    config_fingerprint: str,
    goal23_manifest_catalog: Goal23ManifestCatalog,
    started_report: dict[str, Any],
    control_read_json_fn: ReadJsonFn,
    goal23_factor_object_read_fn: ReadParquetFn,
    goal23_commit_read_fn: ReadGoal23CommitFn,
    goal22_processed_object_read_fn: ReadParquetFn,
    goal22_commit_read_fn: ReadGoal22CommitFn,
    selection_object_read_fn: ReadSelectionObjectFn | None,
    selection_object_write_fn: WriteSelectionObjectFn | None,
    selection_commit_read_fn: ReadSelectionCommitFn | None,
    selection_commit_write_fn: WriteSelectionCommitFn | None,
    snapshot_read_fn: ReadSnapshotFn | None,
    snapshot_upsert_fn: UpsertSnapshotFn | None,
    apply_processed_write: bool,
    resume: bool,
    force: bool,
    logical_output_key: str,
    commit_key: str,
    generated_at_fn: Callable[[], str],
) -> dict[str, Any]:
    report = deepcopy(started_report)
    trusted = _load_trusted_inputs_for_date(
        trade_date=trade_date,
        catalog=goal23_manifest_catalog,
        control_read_json_fn=control_read_json_fn,
        goal23_factor_object_read_fn=goal23_factor_object_read_fn,
        goal23_commit_read_fn=goal23_commit_read_fn,
        goal22_processed_object_read_fn=goal22_processed_object_read_fn,
        goal22_commit_read_fn=goal22_commit_read_fn,
    )
    report["goal23_lineage"] = deepcopy(trusted.goal23_lineage)
    report["goal22_lineage"] = deepcopy(trusted.goal22_lineage)
    report["factor_contract_audit"] = deepcopy(
        trusted.factor_contract_audit
    )
    goal23_factor_config = trusted.goal23_lineage[
        "goal23_factor_config"
    ]
    if (
        goal23_factor_config.get("weights")
        != selection_config["weights"]
        or goal23_factor_config.get("null_score_policy")
        != selection_config["null_score_policy"]
        or goal23_factor_config.get("neutral_score")
        != selection_config["neutral_score"]
    ):
        raise Goal24InputError(
            "Goal 23 factor config does not match the frozen Goal 24 "
            "selection config"
        )
    effective_count = trusted.factor_contract_audit[
        "effective_factor_count"
    ]
    if (
        isinstance(effective_count, bool)
        or not isinstance(effective_count, int)
        or effective_count < 15
        or trusted.factor_contract_audit.get(
            "meets_v1_minimum_effective_factors"
        )
        is not True
    ):
        raise Goal24InputError(
            "Goal 23 effective factor count is below the required minimum "
            f"of 15 for {trade_date}"
        )

    key_audit = _audit_input_key_sets(
        factor_daily=trusted.factor_daily,
        risk_filter=trusted.risk_filter,
        eligible_universe=trusted.eligible_universe,
        factor_input_table=trusted.factor_input_table,
        trade_date=trade_date,
    )
    report["input_key_audit"] = key_audit
    input_fingerprint = _stable_hash(
        {
            "goal23_lineage": report["goal23_lineage"],
            "goal22_lineage": report["goal22_lineage"],
            "input_key_audit": key_audit,
            "selection_config_fingerprint": config_fingerprint,
            "rebalance_mode": rebalance_mode,
        }
    )
    report["input_fingerprint"] = input_fingerprint
    scoring_config = _scoring_config_from_frozen(selection_config)
    result = build_selection_result(
        factor_daily=trusted.factor_daily.copy(deep=True),
        risk_filter=trusted.risk_filter.copy(deep=True),
        eligible_universe=trusted.eligible_universe.copy(deep=True),
        factor_input_table=trusted.factor_input_table.copy(deep=True),
        trade_date=trade_date,
        scoring_config=scoring_config,
    )
    result = normalize_selection_result_frame(result)
    validate_real_selection_result(
        result,
        trade_date,
        scoring_config=scoring_config,
    )
    output_checksum = dataframe_checksum(
        result,
        key_columns=["stock_code", "trade_date"],
    )
    dq_status = "PASS"
    report["dq_status"] = dq_status
    report["counts"] = {
        "factor_row_count": len(trusted.factor_daily),
        "eligible_count": len(trusted.eligible_universe),
        "risk_filter_count": len(trusted.risk_filter),
        "factor_input_count": len(trusted.factor_input_table),
        "pre_score_count": len(trusted.factor_daily),
        "post_filter_count": len(trusted.eligible_universe),
        "final_output_count": len(result),
        "top_n": REAL_SELECTION_TOP_N,
    }
    report["score_statistics"] = _score_statistics(result)
    report["determinism_checks"] = {
        "score_recomputed": True,
        "risk_level_recomputed": True,
        "reason_recomputed": True,
        "suggestion_recomputed": True,
        "sort_total_score_desc": True,
        "tie_break_stock_code_asc": True,
        "rank_continuous": True,
    }
    report["output"] = {
        "dataset": "selection_result",
        "logical_key": logical_output_key,
        "object_key": None,
        "row_count": len(result),
        "checksum": output_checksum,
        "physical_schema": selection_result_physical_schema_contract(),
        "physical_schema_fingerprint": _stable_hash(
            selection_result_physical_schema_contract()
        ),
        "dq_status": dq_status,
        "write": {
            "requested": bool(apply_processed_write),
            "performed": False,
            "status": (
                "NOT_RUN" if apply_processed_write else "NOT_REQUESTED"
            ),
        },
        "read_back": {"passed": False, "status": "NOT_RUN"},
    }
    schema_fingerprint = report["output"][
        "physical_schema_fingerprint"
    ]
    generation_id = _selection_generation_id(
        trade_date=trade_date,
        rebalance_mode=rebalance_mode,
        input_fingerprint=input_fingerprint,
        config_fingerprint=config_fingerprint,
        output_checksum=output_checksum,
        row_count=len(result),
        physical_schema_fingerprint=schema_fingerprint,
    )
    generation_key = build_goal24_selection_generation_key(
        trade_date,
        rebalance_mode,
        generation_id,
    )
    report["commit"]["generation_id"] = generation_id
    report["output"]["object_key"] = generation_key

    if not apply_processed_write:
        report["status"] = "READY_FOR_APPLY"
        return report

    assert selection_object_read_fn is not None
    assert selection_object_write_fn is not None
    assert selection_commit_read_fn is not None
    assert selection_commit_write_fn is not None
    assert snapshot_read_fn is not None
    assert snapshot_upsert_fn is not None

    existing_commit = _read_selection_commit_optional(
        selection_commit_read_fn,
        trade_date,
        rebalance_mode,
    )
    compatible_commit = None
    if existing_commit is not None:
        try:
            validate_goal24_selection_commit_payload(
                existing_commit,
                trade_date,
                rebalance_mode,
            )
        except (
            DataValidationError,
            ValueError,
            KeyError,
            TypeError,
        ) as exc:
            raise Goal24InputError(
                "existing canonical commit is invalid and cannot be "
                f"replaced for {trade_date} {rebalance_mode}"
            ) from exc
        if not _completed_selection_commit_matches(
            existing_commit,
            trade_date=trade_date,
            rebalance_mode=rebalance_mode,
            input_fingerprint=input_fingerprint,
            config_fingerprint=config_fingerprint,
            output_checksum=output_checksum,
            row_count=len(result),
            selection_object_read_fn=selection_object_read_fn,
        ):
            raise Goal24InputError(
                "canonical commit collision: an existing committed "
                "publication has incompatible lineage or content for "
                f"{trade_date} {rebalance_mode}"
            )
        compatible_commit = existing_commit

    current_phase = "RESUME_COMMIT_READ"
    commit_published = False
    try:
        if compatible_commit is not None and resume and not force:
            commit_published = True
            report["resume_action"] = "REUSED_COMMITTED"
            report["commit"] = {
                "object_key": commit_key,
                "status": "COMMITTED",
                "generation_id": compatible_commit["generation_id"],
                "reused": True,
            }
            report["output"]["object_key"] = compatible_commit["output"][
                "object_key"
            ]
            report["output"]["write"] = {
                "requested": True,
                "performed": False,
                "status": "UNCHANGED",
            }
            report["output"]["read_back"] = {
                "passed": True,
                "row_count": len(result),
                "checksum": output_checksum,
            }
            return _complete_snapshot_phase(
                report=report,
                result=result,
                trade_date=trade_date,
                rebalance_mode=rebalance_mode,
                object_key=report["output"]["object_key"],
                snapshot_read_fn=snapshot_read_fn,
                snapshot_upsert_fn=snapshot_upsert_fn,
                reused_commit=True,
            )

        report["resume_action"] = (
            "FORCE_RECOMPUTED"
            if force
            else "RECOMPUTED"
            if existing_commit is not None
            else "NEW"
        )
        report["commit"] = {
            "object_key": commit_key,
            "status": "PENDING",
            "generation_id": generation_id,
            "reused": False,
        }
        report["output"]["object_key"] = generation_key

        current_phase = "STAGE:selection_result"
        existing_object = _read_selection_object_optional(
            selection_object_read_fn,
            generation_key,
        )
        if existing_object is not None:
            if not _valid_selection_output_checksum(
                existing_object,
                trade_date,
                output_checksum,
                scoring_config,
            ):
                raise RuntimeError(
                    "immutable selection generation already exists with "
                    "mismatched schema or checksum"
                )
            report["output"]["write"] = {
                "requested": True,
                "performed": False,
                "status": "UNCHANGED",
            }
        else:
            written_key = selection_object_write_fn(
                trade_date,
                rebalance_mode,
                generation_id,
                result.copy(deep=True),
            )
            if written_key != generation_key:
                raise RuntimeError(
                    "selection generation writer returned unexpected key"
                )
            report["output"]["write"] = {
                "requested": True,
                "performed": True,
                "status": "WRITTEN",
            }

        staged = normalize_selection_result_read_back(
            selection_object_read_fn(generation_key)
        )
        validate_real_selection_result(
            staged,
            trade_date,
            scoring_config=scoring_config,
        )
        staged_checksum = dataframe_checksum(
            staged,
            key_columns=["stock_code", "trade_date"],
        )
        if len(staged) != len(result) or staged_checksum != output_checksum:
            raise RuntimeError("selection generation read-back mismatch")
        report["output"]["read_back"] = {
            "passed": True,
            "row_count": len(staged),
            "checksum": staged_checksum,
            "exact_arrow_schema": True,
            "deterministic_recalculation": True,
        }

        commit_payload = {
            "schema_version": "goal24.selection_date_commit.v1",
            "goal": "24",
            "status": "COMMITTED",
            "run_id": run_id,
            "trade_date": trade_date,
            "rebalance_mode": rebalance_mode,
            "generation_id": generation_id,
            "committed_at": generated_at_fn(),
            "plan_fingerprint": plan_fingerprint,
            "input_fingerprint": input_fingerprint,
            "selection_config": deepcopy(selection_config),
            "selection_config_fingerprint": config_fingerprint,
            "output": {
                "dataset": "selection_result",
                "logical_key": logical_output_key,
                "object_key": generation_key,
                "row_count": len(result),
                "checksum": output_checksum,
                "physical_schema_fingerprint": schema_fingerprint,
                "dq_status": dq_status,
            },
            "firewalls": deepcopy(GOAL24_FIREWALLS),
        }
        validate_goal24_selection_commit_payload(
            commit_payload,
            trade_date,
            rebalance_mode,
        )
        if compatible_commit is None:
            current_phase = "DATE_COMMIT"
            concurrent_commit = _read_selection_commit_optional(
                selection_commit_read_fn,
                trade_date,
                rebalance_mode,
            )
            if concurrent_commit is not None:
                try:
                    validate_goal24_selection_commit_payload(
                        concurrent_commit,
                        trade_date,
                        rebalance_mode,
                    )
                except (
                    DataValidationError,
                    ValueError,
                    KeyError,
                    TypeError,
                ) as exc:
                    raise Goal24InputError(
                        "canonical commit collision: an invalid "
                        "publication appeared before commit"
                    ) from exc
                if not _completed_selection_commit_matches(
                    concurrent_commit,
                    trade_date=trade_date,
                    rebalance_mode=rebalance_mode,
                    input_fingerprint=input_fingerprint,
                    config_fingerprint=config_fingerprint,
                    output_checksum=output_checksum,
                    row_count=len(result),
                    selection_object_read_fn=(
                        selection_object_read_fn
                    ),
                ):
                    raise Goal24InputError(
                        "canonical commit collision: an incompatible "
                        "publication appeared before commit"
                    )
                compatible_commit = concurrent_commit
                report["resume_action"] = (
                    "CONCURRENT_PUBLICATION_REUSED"
                )
                report["commit"]["reused"] = True
            else:
                try:
                    written_commit_key = selection_commit_write_fn(
                        trade_date,
                        rebalance_mode,
                        commit_payload,
                    )
                except FileExistsError as exc:
                    try:
                        winning_commit = selection_commit_read_fn(
                            trade_date,
                            rebalance_mode,
                        )
                        validate_goal24_selection_commit_payload(
                            winning_commit,
                            trade_date,
                            rebalance_mode,
                        )
                    except (
                        DataValidationError,
                        FileNotFoundError,
                        ValueError,
                        KeyError,
                        TypeError,
                    ) as validation_exc:
                        raise Goal24InputError(
                            "canonical commit collision: create-only "
                            "publication lost to an invalid commit"
                        ) from validation_exc
                    if not _completed_selection_commit_matches(
                        winning_commit,
                        trade_date=trade_date,
                        rebalance_mode=rebalance_mode,
                        input_fingerprint=input_fingerprint,
                        config_fingerprint=config_fingerprint,
                        output_checksum=output_checksum,
                        row_count=len(result),
                        selection_object_read_fn=(
                            selection_object_read_fn
                        ),
                    ):
                        raise Goal24InputError(
                            "canonical commit collision: create-only "
                            "publication lost to an incompatible commit"
                        ) from exc
                    compatible_commit = winning_commit
                    written_commit_key = commit_key
                    report["resume_action"] = (
                        "CONCURRENT_PUBLICATION_REUSED"
                    )
                    report["commit"]["reused"] = True
                if written_commit_key != commit_key:
                    raise RuntimeError(
                        "selection commit writer returned unexpected key"
                    )
        else:
            report["resume_action"] = (
                "FORCE_RECOMPUTED_REUSED_PUBLICATION"
                if force
                else "RECOMPUTED_REUSED_PUBLICATION"
            )
            report["commit"]["reused"] = True
        commit_published = True
        report["commit"]["status"] = "COMMITTED"

        current_phase = "COMMITTED_READBACK"
        committed = read_goal24_published_selection_result(
            trade_date=trade_date,
            rebalance_mode=rebalance_mode,
            selection_commit_read_fn=selection_commit_read_fn,
            selection_object_read_fn=selection_object_read_fn,
        )
        committed_checksum = dataframe_checksum(
            committed,
            key_columns=["stock_code", "trade_date"],
        )
        if (
            len(committed) != len(result)
            or committed_checksum != output_checksum
        ):
            raise RuntimeError(
                "committed selection_result read-back mismatch"
            )
        report["output"]["read_back"] = {
            "passed": True,
            "row_count": len(committed),
            "checksum": committed_checksum,
            "exact_arrow_schema": True,
            "deterministic_recalculation": True,
        }
    except Goal24InputError:
        raise
    except Exception as exc:
        report["status"] = "FAILED"
        report["dq_status"] = "BLOCKED"
        report["blocked_reasons"] = [
            f"OUTPUT_APPLY_FAILED:{current_phase}:{type(exc).__name__}"
        ]
        report["failure"] = _failure_record(exc)
        if not commit_published:
            report["commit"]["status"] = "UNCOMMITTED"
        return report

    return _complete_snapshot_phase(
        report=report,
        result=result,
        trade_date=trade_date,
        rebalance_mode=rebalance_mode,
        object_key=report["output"]["object_key"],
        snapshot_read_fn=snapshot_read_fn,
        snapshot_upsert_fn=snapshot_upsert_fn,
        reused_commit=False,
    )


def _complete_snapshot_phase(
    *,
    report: dict[str, Any],
    result: pd.DataFrame,
    trade_date: str,
    rebalance_mode: str,
    object_key: str,
    snapshot_read_fn: ReadSnapshotFn,
    snapshot_upsert_fn: UpsertSnapshotFn,
    reused_commit: bool,
) -> dict[str, Any]:
    summary = summarize_selection_result(
        result,
        trade_date=trade_date,
        top_n=REAL_SELECTION_TOP_N,
        object_key=object_key,
        rebalance_mode=rebalance_mode,
    )
    report["snapshot"] = {
        "identity": {
            "trade_date": trade_date,
            "rebalance_mode": rebalance_mode,
        },
        "status": "PENDING",
        "repair_only": bool(reused_commit),
        "summary": deepcopy(summary),
    }
    try:
        current = snapshot_read_fn(trade_date, rebalance_mode)
        if _snapshot_matches(current, summary):
            report["snapshot"]["status"] = "UNCHANGED"
            report["resume_action"] = (
                "REUSED_COMPLETED"
                if reused_commit
                else report["resume_action"]
            )
        else:
            snapshot_upsert_fn(deepcopy(summary))
            verified = snapshot_read_fn(trade_date, rebalance_mode)
            if not _snapshot_matches(verified, summary):
                raise RuntimeError(
                    "selection snapshot read-back mismatch after upsert"
                )
            report["snapshot"]["status"] = (
                "REPAIRED"
                if reused_commit
                else "WRITTEN"
            )
            if reused_commit:
                report["resume_action"] = "REPAIRED_DATABASE_SUMMARY"
    except Exception as exc:
        report["status"] = "DATABASE_PENDING"
        report["snapshot"]["status"] = "FAILED_PENDING_REPAIR"
        report["snapshot"]["failure"] = _failure_record(exc)
        report["failure"] = _failure_record(exc)
        report["blocked_reasons"] = [
            f"POSTGRES_SNAPSHOT_PENDING:{type(exc).__name__}"
        ]
        return report
    report["status"] = "COMPLETED"
    return report


def _load_trusted_inputs_for_date(
    *,
    trade_date: str,
    catalog: Goal23ManifestCatalog,
    control_read_json_fn: ReadJsonFn,
    goal23_factor_object_read_fn: ReadParquetFn,
    goal23_commit_read_fn: ReadGoal23CommitFn,
    goal22_processed_object_read_fn: ReadParquetFn,
    goal22_commit_read_fn: ReadGoal22CommitFn,
) -> Goal24TrustedInputs:
    trade_date = validate_trade_date(trade_date)
    for record in catalog.records:
        if trade_date in record.trade_dates and record.validation_errors:
            raise Goal24InputError(
                f"invalid Goal 23 manifest {record.object_key}: "
                f"{'; '.join(record.validation_errors)}"
            )
    coverage = [
        record
        for record in catalog.records
        if record.valid
        and trade_date in record.trade_dates
        and record.payload["date_statuses"][trade_date] == "COMPLETED"
    ]
    if not coverage:
        raise Goal24InputError(
            f"missing completed Goal 23 manifest coverage for {trade_date}"
        )
    publications = [
        _read_goal23_publication(
            record=record,
            trade_date=trade_date,
            control_read_json_fn=control_read_json_fn,
            goal23_factor_object_read_fn=goal23_factor_object_read_fn,
            goal23_commit_read_fn=goal23_commit_read_fn,
            goal22_processed_object_read_fn=(
                goal22_processed_object_read_fn
            ),
            goal22_commit_read_fn=goal22_commit_read_fn,
        )
        for record in coverage
    ]
    fingerprints = {
        item.goal23_lineage["publication_fingerprint"]
        for item in publications
    }
    if len(fingerprints) != 1:
        raise Goal24InputError(
            f"ambiguous Goal 23 publication coverage for {trade_date}"
        )
    return publications[0]


def _read_goal23_publication(
    *,
    record: Goal23ManifestRecord,
    trade_date: str,
    control_read_json_fn: ReadJsonFn,
    goal23_factor_object_read_fn: ReadParquetFn,
    goal23_commit_read_fn: ReadGoal23CommitFn,
    goal22_processed_object_read_fn: ReadParquetFn,
    goal22_commit_read_fn: ReadGoal22CommitFn,
) -> Goal24TrustedInputs:
    manifest = record.payload
    report_key = manifest["daily_report_keys"][trade_date]
    try:
        report = control_read_json_fn(report_key)
    except FileNotFoundError as exc:
        raise Goal24InputError(
            f"missing Goal 23 daily DQ report: {report_key}"
        ) from exc
    _validate_goal23_daily_report(
        report=report,
        manifest=manifest,
        trade_date=trade_date,
    )
    try:
        commit = goal23_commit_read_fn(trade_date)
    except FileNotFoundError as exc:
        raise Goal24InputError(
            f"missing Goal 23 commit for {trade_date}"
        ) from exc
    validate_goal23_factor_commit_payload(commit, trade_date)
    if (
        commit.get("run_id") != manifest["run_id"]
        or commit.get("plan_fingerprint") != manifest["plan_fingerprint"]
        or commit.get("input_fingerprint") != report["input_fingerprint"]
        or commit.get("factor_config_fingerprint")
        != report["factor_config_fingerprint"]
        or commit.get("generation_id")
        != report.get("commit", {}).get("generation_id")
        or commit.get("output", {}).get("object_key")
        != report.get("output", {}).get("object_key")
        or commit.get("output", {}).get("checksum")
        != report.get("output", {}).get("checksum")
        or commit.get("output", {}).get("row_count")
        != report.get("output", {}).get("row_count")
    ):
        raise Goal24InputError(
            f"Goal 23 commit, manifest and DQ mismatch for {trade_date}"
        )
    try:
        factor_daily = normalize_factor_daily_read_back(
            goal23_factor_object_read_fn(commit["output"]["object_key"])
        )
    except FileNotFoundError as exc:
        raise Goal24InputError(
            f"missing Goal 23 factor generation for {trade_date}"
        ) from exc
    validate_factor_daily(factor_daily, trade_date)
    factor_checksum = dataframe_checksum(
        factor_daily,
        key_columns=["stock_code", "trade_date"],
    )
    if (
        len(factor_daily) != commit["output"]["row_count"]
        or factor_checksum != commit["output"]["checksum"]
    ):
        raise Goal24InputError(
            f"Goal 23 factor generation checksum mismatch for {trade_date}"
        )
    actual_audit = audit_factor_contract(factor_daily)
    if actual_audit != report["factor_contract_audit"]:
        raise Goal24InputError(
            f"Goal 23 factor contract audit drift for {trade_date}"
        )
    goal22_ref = report["goal22_input_lineage"].get(trade_date)
    if not isinstance(goal22_ref, dict):
        raise Goal24InputError(
            f"Goal 23 target Goal 22 lineage is missing for {trade_date}"
        )
    goal22_inputs = _read_goal22_selection_inputs(
        trade_date=trade_date,
        goal22_ref=goal22_ref,
        control_read_json_fn=control_read_json_fn,
        goal22_processed_object_read_fn=(
            goal22_processed_object_read_fn
        ),
        goal22_commit_read_fn=goal22_commit_read_fn,
    )
    goal23_lineage = {
        "goal23_manifest_key": record.object_key,
        "goal23_manifest_checksum": record.checksum,
        "goal23_run_id": manifest["run_id"],
        "goal23_plan_fingerprint": manifest["plan_fingerprint"],
        "goal23_daily_report_key": report_key,
        "goal23_daily_report_checksum": _stable_hash(report),
        "goal23_commit_key": build_goal23_factor_commit_key(trade_date),
        "goal23_commit_checksum": _stable_hash(commit),
        "goal23_generation_id": commit["generation_id"],
        "goal23_generation_key": commit["output"]["object_key"],
        "goal23_generation_checksum": commit["output"]["checksum"],
        "goal23_generation_row_count": commit["output"]["row_count"],
        "goal23_factor_config": deepcopy(report["factor_config"]),
        "goal23_factor_config_fingerprint": report[
            "factor_config_fingerprint"
        ],
        "goal23_effective_factor_count": actual_audit[
            "effective_factor_count"
        ],
        "physical_schema_fingerprint": _stable_hash(
            {
                column: (
                    "string"
                    if column
                    in {"stock_code", "trade_date", "industry", "market_type"}
                    else "float64"
                )
                for column in FACTOR_DAILY_COLUMNS
            }
        ),
    }
    goal23_lineage["publication_fingerprint"] = _stable_hash(
        {
            "goal23": goal23_lineage,
            "goal22": goal22_inputs.lineage,
        }
    )
    return Goal24TrustedInputs(
        factor_daily=factor_daily,
        risk_filter=goal22_inputs.risk_filter,
        eligible_universe=goal22_inputs.eligible_universe,
        factor_input_table=goal22_inputs.factor_input_table,
        goal23_lineage=goal23_lineage,
        goal22_lineage=goal22_inputs.lineage,
        factor_contract_audit=actual_audit,
    )


def _read_goal22_selection_inputs(
    *,
    trade_date: str,
    goal22_ref: dict[str, Any],
    control_read_json_fn: ReadJsonFn,
    goal22_processed_object_read_fn: ReadParquetFn,
    goal22_commit_read_fn: ReadGoal22CommitFn,
) -> Goal22SelectionInputs:
    manifest_key = goal22_ref.get("goal22_manifest_key")
    if not isinstance(manifest_key, str):
        raise Goal24InputError("Goal 22 manifest lineage key is missing")
    try:
        catalog = load_goal22_manifest_catalog(
            manifest_keys=[manifest_key],
            read_json_fn=control_read_json_fn,
        )
    except FileNotFoundError as exc:
        raise Goal24InputError(
            f"missing Goal 22 manifest: {manifest_key}"
        ) from exc
    record: Goal22ManifestRecord = catalog.records[0]
    if not record.valid:
        raise Goal24InputError(
            f"invalid Goal 22 manifest {manifest_key}: "
            f"{'; '.join(record.validation_errors)}"
        )
    manifest = record.payload
    if (
        record.checksum != goal22_ref.get("goal22_manifest_checksum")
        or manifest.get("run_id") != goal22_ref.get("goal22_run_id")
        or manifest.get("plan_fingerprint")
        != goal22_ref.get("goal22_plan_fingerprint")
        or manifest.get("date_statuses", {}).get(trade_date)
        != "COMPLETED"
    ):
        raise Goal24InputError(
            f"Goal 22 manifest lineage drift for {trade_date}"
        )
    report_key = manifest["daily_report_keys"][trade_date]
    if report_key != goal22_ref.get("goal22_daily_report_key"):
        raise Goal24InputError(
            f"Goal 22 daily report key drift for {trade_date}"
        )
    try:
        report = control_read_json_fn(report_key)
    except FileNotFoundError as exc:
        raise Goal24InputError(
            f"missing Goal 22 daily DQ report: {report_key}"
        ) from exc
    _validate_goal22_daily_report(
        report=report,
        manifest=manifest,
        trade_date=trade_date,
    )
    if _stable_hash(report) != goal22_ref.get(
        "goal22_daily_report_checksum"
    ):
        raise Goal24InputError(
            f"Goal 22 daily DQ checksum drift for {trade_date}"
        )
    try:
        commit = goal22_commit_read_fn(trade_date)
    except FileNotFoundError as exc:
        raise Goal24InputError(
            f"missing Goal 22 commit for {trade_date}"
        ) from exc
    _validate_goal22_commit_for_manifest(
        commit=commit,
        report=report,
        manifest=manifest,
        trade_date=trade_date,
    )
    if (
        goal22_ref.get("goal22_commit_key")
        != build_goal22_processed_commit_key(trade_date)
        or _stable_hash(commit)
        != goal22_ref.get("goal22_commit_checksum")
        or commit.get("generation_id")
        != goal22_ref.get("goal22_generation_id")
    ):
        raise Goal24InputError(
            f"Goal 22 commit lineage drift for {trade_date}"
        )
    consumed_factor_input = goal22_ref.get("consumed_outputs", {}).get(
        "factor_input_table"
    )
    if consumed_factor_input != commit["outputs"]["factor_input_table"]:
        raise Goal24InputError(
            f"Goal 23 and Goal 22 factor_input_table lineage mismatch "
            f"for {trade_date}"
        )

    frames: dict[str, pd.DataFrame] = {}
    records: dict[str, Any] = {}
    for dataset in (
        "risk_filter",
        "eligible_universe",
        "factor_input_table",
    ):
        committed = commit["outputs"][dataset]
        try:
            frame = goal22_processed_object_read_fn(
                committed["object_key"]
            )
        except FileNotFoundError as exc:
            raise Goal24InputError(
                f"missing Goal 22 {dataset} generation for {trade_date}"
            ) from exc
        arrow_evidence = frame.attrs.get(
            GOAL22_ARROW_SCHEMA_EVIDENCE_ATTR
        )
        if (
            not isinstance(arrow_evidence, dict)
            or arrow_evidence.get("dataset") != dataset
            or not isinstance(
                arrow_evidence.get("physical_schema"),
                dict,
            )
        ):
            raise Goal24InputError(
                f"missing Goal 22 physical Arrow schema evidence for "
                f"{dataset} {trade_date}"
            )
        physical_schema = arrow_evidence["physical_schema"]
        contract = get_schema_contract(dataset)
        if list(physical_schema) != contract.columns:
            raise Goal24InputError(
                f"invalid Goal 22 physical Arrow schema evidence for "
                f"{dataset} {trade_date}"
            )
        validate_dataset_frame(dataset, frame, trade_date)
        checksum = dataframe_checksum(
            frame,
            key_columns=GOAL22_OUTPUT_KEY_COLUMNS[dataset],
        )
        if (
            len(frame) != committed["row_count"]
            or checksum != committed["checksum"]
        ):
            raise Goal24InputError(
                f"Goal 22 generation checksum mismatch for "
                f"{dataset} {trade_date}"
            )
        frames[dataset] = frame.copy(deep=True)
        records[dataset] = deepcopy(committed)
        records[dataset]["physical_schema"] = deepcopy(
            physical_schema
        )
        records[dataset]["physical_schema_fingerprint"] = _stable_hash(
            physical_schema
        )
    lineage = {
        "goal22_manifest_key": manifest_key,
        "goal22_manifest_checksum": record.checksum,
        "goal22_run_id": manifest["run_id"],
        "goal22_plan_fingerprint": manifest["plan_fingerprint"],
        "goal22_daily_report_key": report_key,
        "goal22_daily_report_checksum": _stable_hash(report),
        "goal22_commit_key": build_goal22_processed_commit_key(trade_date),
        "goal22_commit_checksum": _stable_hash(commit),
        "goal22_generation_id": commit["generation_id"],
        "selection_inputs": records,
    }
    lineage["publication_fingerprint"] = _stable_hash(lineage)
    return Goal22SelectionInputs(
        risk_filter=frames["risk_filter"],
        eligible_universe=frames["eligible_universe"],
        factor_input_table=frames["factor_input_table"],
        lineage=lineage,
    )


def _validate_goal23_manifest_payload(
    *,
    object_key: str,
    payload: dict[str, Any],
    key_run_id: str,
) -> None:
    if (
        payload.get("schema_version")
        != "goal23.real_factor_daily_manifest.v1"
        or payload.get("goal") != "23"
        or payload.get("run_id") != key_run_id
        or payload.get("mode") != "APPLY"
        or payload.get("apply_requested") is not True
        or payload.get("downstream_firewalls")
        != GOAL23_DOWNSTREAM_FIREWALLS
    ):
        raise Goal24InputError(
            f"untrusted Goal 23 manifest header: {object_key}"
        )
    plan = payload.get("plan")
    if (
        not isinstance(plan, dict)
        or plan.get("schema_version")
        != "goal23.real_factor_daily_plan.v1"
        or plan.get("run_id") != key_run_id
        or plan.get("factor_daily_columns")
        != list(FACTOR_DAILY_COLUMNS)
    ):
        raise Goal24InputError("Goal 23 manifest plan contract is invalid")
    start_date, end_date = validate_date_range(
        plan.get("start_date"),
        plan.get("end_date"),
    )
    dates = _normalize_trade_dates(plan.get("trade_dates", []))
    if (
        not dates
        or dates != plan.get("trade_dates")
        or dates[0] < start_date
        or dates[-1] > end_date
    ):
        raise Goal24InputError("Goal 23 manifest trade dates are invalid")
    frozen_factor_config = _freeze_goal23_factor_config(
        plan.get("factor_config")
    )
    if (
        frozen_factor_config != plan.get("factor_config")
        or _stable_hash(frozen_factor_config)
        != plan.get("factor_config_fingerprint")
        or payload.get("plan_fingerprint") != _stable_hash(plan)
    ):
        raise Goal24InputError(
            "Goal 23 factor config or plan fingerprint mismatch"
        )
    expected_keys = build_real_factor_daily_output_keys(key_run_id, dates)
    if (
        payload.get("daily_report_keys") != expected_keys["daily_reports"]
        or payload.get("processed_output_keys") != expected_keys["processed"]
        or payload.get("processed_commit_keys")
        != expected_keys["processed_commits"]
    ):
        raise Goal24InputError(
            "Goal 23 manifest output key mapping is invalid"
        )
    statuses = payload.get("date_statuses")
    attempts = payload.get("date_attempts")
    if (
        not isinstance(statuses, dict)
        or list(statuses) != dates
        or not isinstance(attempts, dict)
        or list(attempts) != dates
    ):
        raise Goal24InputError(
            "Goal 23 manifest per-date state is invalid"
        )
    if any(
        value not in {"COMPLETED", "BLOCKED", "FAILED"}
        for value in statuses.values()
    ):
        raise Goal24InputError(
            "Goal 23 manifest contains invalid date status"
        )
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 1
        for value in attempts.values()
    ):
        raise Goal24InputError(
            "Goal 23 manifest contains invalid attempt count"
        )
    if payload.get("status") != _goal23_range_status(statuses):
        raise Goal24InputError(
            "Goal 23 range status does not match date states"
        )
    expected_counts = dict(sorted(Counter(statuses.values()).items()))
    if payload.get("status_counts") != expected_counts:
        raise Goal24InputError(
            "Goal 23 status counts do not match date states"
        )


def _validate_goal23_daily_report(
    *,
    report: dict[str, Any],
    manifest: dict[str, Any],
    trade_date: str,
) -> None:
    if (
        not isinstance(report, dict)
        or report.get("schema_version")
        != "goal23.real_factor_daily_daily_dq.v1"
        or report.get("goal") != "23"
        or report.get("run_id") != manifest["run_id"]
        or report.get("trade_date") != trade_date
        or report.get("plan_fingerprint") != manifest["plan_fingerprint"]
        or report.get("mode") != "APPLY"
        or report.get("status") != "COMPLETED"
        or report.get("attempt")
        != manifest["date_attempts"][trade_date]
        or report.get("blocked_reasons") != []
        or report.get("failure") is not None
        or report.get("downstream_firewalls")
        != GOAL23_DOWNSTREAM_FIREWALLS
    ):
        raise Goal24InputError(
            f"Goal 23 daily DQ is not completed for {trade_date}"
        )
    plan = manifest["plan"]
    if (
        report.get("factor_config") != plan["factor_config"]
        or report.get("factor_config_fingerprint")
        != plan["factor_config_fingerprint"]
        or report.get("goal22_manifests") != plan["goal22_manifests"]
        or not isinstance(report.get("goal22_input_lineage"), dict)
        or trade_date not in report["goal22_input_lineage"]
        or report.get("input_fingerprint") is None
    ):
        raise Goal24InputError(
            f"Goal 23 daily DQ lineage mismatch for {trade_date}"
        )
    output = report.get("output")
    commit = report.get("commit")
    if (
        not isinstance(output, dict)
        or output.get("dataset") != "factor_daily"
        or output.get("logical_key")
        != f"processed/factor_daily/trade_date={trade_date}/part.parquet"
        or isinstance(output.get("row_count"), bool)
        or not isinstance(output.get("row_count"), int)
        or output["row_count"] < 0
        or re.fullmatch(
            r"[0-9a-f]{64}",
            str(output.get("checksum", "")),
        )
        is None
        or output.get("write", {}).get("requested") is not True
        or output.get("write", {}).get("status")
        not in {"WRITTEN", "UNCHANGED"}
        or output.get("read_back", {}).get("passed") is not True
        or output.get("read_back", {}).get("row_count")
        != output["row_count"]
        or output.get("read_back", {}).get("checksum")
        != output["checksum"]
        or not isinstance(commit, dict)
        or commit.get("status") != "COMMITTED"
        or commit.get("object_key")
        != build_goal23_factor_commit_key(trade_date)
        or re.fullmatch(
            r"[0-9a-f]{64}",
            str(commit.get("generation_id", "")),
        )
        is None
        or output.get("object_key")
        != build_goal23_factor_generation_key(
            trade_date,
            commit["generation_id"],
        )
    ):
        raise Goal24InputError(
            f"Goal 23 output evidence is invalid for {trade_date}"
        )
    audit = report.get("factor_contract_audit")
    if (
        not isinstance(audit, dict)
        or isinstance(audit.get("effective_factor_count"), bool)
        or not isinstance(audit.get("effective_factor_count"), int)
        or audit.get("effective_factor_count")
        != len(audit.get("effective_factors", []))
        or audit.get("meets_v1_minimum_effective_factors")
        != (audit.get("effective_factor_count", 0) >= 15)
    ):
        raise Goal24InputError(
            f"Goal 23 factor audit is invalid for {trade_date}"
        )


def _audit_input_key_sets(
    *,
    factor_daily: pd.DataFrame,
    risk_filter: pd.DataFrame,
    eligible_universe: pd.DataFrame,
    factor_input_table: pd.DataFrame,
    trade_date: str,
) -> dict[str, Any]:
    trade_date = validate_trade_date(trade_date)
    frames = {
        "factor_daily": factor_daily,
        "risk_filter": risk_filter,
        "eligible_universe": eligible_universe,
        "factor_input_table": factor_input_table,
    }
    key_sets: dict[str, set[tuple[str, str]]] = {}
    for name, frame in frames.items():
        if not isinstance(frame, pd.DataFrame):
            raise Goal24InputError(f"{name} must be a DataFrame")
        if frame.duplicated(["stock_code", "trade_date"]).any():
            raise Goal24InputError(f"{name} contains duplicate stock/date keys")
        dates = set(frame["trade_date"].astype(str))
        if dates and dates != {trade_date}:
            raise Goal24InputError(
                f"{name} trade_date does not match {trade_date}"
            )
        key_sets[name] = set(
            zip(
                frame["stock_code"].astype(str),
                frame["trade_date"].astype(str),
                strict=True,
            )
        )
    factor_keys = key_sets["factor_daily"]
    eligible_keys = key_sets["eligible_universe"]
    factor_input_keys = key_sets["factor_input_table"]
    risk_keys = key_sets["risk_filter"]
    if factor_keys != eligible_keys or factor_keys != factor_input_keys:
        raise Goal24InputError(
            "factor_daily, eligible_universe and factor_input_table "
            "stock/date key sets must match exactly"
        )
    missing_risk = eligible_keys - risk_keys
    if missing_risk:
        raise Goal24InputError(
            "eligible stocks are missing risk_filter rows: "
            f"{sorted(code for code, _ in missing_risk)[:10]}"
        )
    if eligible_keys:
        eligible_risk = risk_filter.merge(
            eligible_universe[["stock_code", "trade_date"]],
            on=["stock_code", "trade_date"],
            how="inner",
        )
        if not eligible_risk["is_eligible"].map(
            lambda value: isinstance(value, (bool, np.bool_))
            and bool(value)
        ).all():
            raise Goal24InputError(
                "eligible_universe contains stocks not explicitly allowed "
                "by risk_filter"
            )
    coverage = (
        1.0 if not eligible_keys else len(eligible_keys & risk_keys) / len(eligible_keys)
    )
    return {
        "trade_date": trade_date,
        "factor_key_count": len(factor_keys),
        "eligible_key_count": len(eligible_keys),
        "factor_input_key_count": len(factor_input_keys),
        "risk_filter_key_count": len(risk_keys),
        "risk_filter_covered_eligible_count": len(eligible_keys & risk_keys),
        "risk_filter_coverage_rate": coverage,
        "factor_eligible_factor_input_exact_match": True,
        "eligible_risk_filter_coverage_complete": True,
        "missing_risk_filter_keys": [],
    }


def _empty_daily_report(
    *,
    run_id: str,
    trade_date: str,
    rebalance_mode: str,
    mode: str,
    attempt: int,
    plan_fingerprint: str,
    selection_config: dict[str, Any],
    config_fingerprint: str,
    goal23_manifests: list[dict[str, Any]],
    logical_output_key: str,
    commit_key: str,
    generated_at: str,
) -> dict[str, Any]:
    return {
        "schema_version": "goal24.real_selection_result_daily_dq.v1",
        "goal": "24",
        "run_id": run_id,
        "trade_date": trade_date,
        "rebalance_mode": rebalance_mode,
        "generated_at": generated_at,
        "mode": mode,
        "status": "RUNNING",
        "dq_status": "BLOCKED",
        "attempt": attempt,
        "resume_action": "NOT_APPLICABLE",
        "plan_fingerprint": plan_fingerprint,
        "selection_config": deepcopy(selection_config),
        "selection_config_fingerprint": config_fingerprint,
        "goal23_manifests": deepcopy(goal23_manifests),
        "goal23_lineage": {},
        "goal22_lineage": {},
        "factor_contract_audit": {},
        "input_key_audit": {},
        "input_fingerprint": None,
        "counts": {
            "factor_row_count": 0,
            "eligible_count": 0,
            "risk_filter_count": 0,
            "factor_input_count": 0,
            "pre_score_count": 0,
            "post_filter_count": 0,
            "final_output_count": 0,
            "top_n": REAL_SELECTION_TOP_N,
        },
        "score_statistics": {},
        "determinism_checks": {},
        "output": {
            "dataset": "selection_result",
            "logical_key": logical_output_key,
            "object_key": None,
            "row_count": 0,
            "checksum": None,
            "physical_schema": selection_result_physical_schema_contract(),
            "physical_schema_fingerprint": _stable_hash(
                selection_result_physical_schema_contract()
            ),
            "dq_status": "BLOCKED",
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
        "snapshot": {
            "identity": {
                "trade_date": trade_date,
                "rebalance_mode": rebalance_mode,
            },
            "status": "NOT_RUN",
        },
        "blocked_reasons": [],
        "failure": None,
        "firewalls": deepcopy(GOAL24_FIREWALLS),
    }


def _completed_selection_commit_matches(
    commit: dict[str, Any],
    *,
    trade_date: str,
    rebalance_mode: str,
    input_fingerprint: str,
    config_fingerprint: str,
    output_checksum: str,
    row_count: int,
    selection_object_read_fn: ReadSelectionObjectFn,
) -> bool:
    try:
        validate_goal24_selection_commit_payload(
            commit,
            trade_date,
            rebalance_mode,
        )
    except (DataValidationError, ValueError, KeyError, TypeError):
        return False
    if (
        commit.get("input_fingerprint") != input_fingerprint
        or commit.get("selection_config_fingerprint")
        != config_fingerprint
        or commit["output"]["row_count"] != row_count
        or commit["output"]["checksum"] != output_checksum
    ):
        return False
    try:
        frame = selection_object_read_fn(commit["output"]["object_key"])
    except (FileNotFoundError, DataValidationError, ValueError, TypeError):
        return False
    return _valid_selection_output_checksum(
        frame,
        trade_date,
        output_checksum,
        _scoring_config_from_frozen(commit["selection_config"]),
    )


def _valid_selection_output_checksum(
    frame: pd.DataFrame,
    trade_date: str,
    expected_checksum: str,
    scoring_config: ScoringConfig,
) -> bool:
    try:
        normalized = normalize_selection_result_read_back(frame)
        validate_real_selection_result(
            normalized,
            trade_date,
            scoring_config=scoring_config,
        )
        return (
            dataframe_checksum(
                normalized,
                key_columns=["stock_code", "trade_date"],
            )
            == expected_checksum
        )
    except (DataValidationError, ValueError, TypeError, KeyError):
        return False


def _selection_generation_id(
    *,
    trade_date: str,
    rebalance_mode: str,
    input_fingerprint: str,
    config_fingerprint: str,
    output_checksum: str,
    row_count: int,
    physical_schema_fingerprint: str,
) -> str:
    return _stable_hash(
        {
            "trade_date": trade_date,
            "rebalance_mode": rebalance_mode,
            "input_fingerprint": input_fingerprint,
            "selection_config_fingerprint": config_fingerprint,
            "output_checksum": output_checksum,
            "row_count": row_count,
            "physical_schema_fingerprint": physical_schema_fingerprint,
        }
    )


def _score_statistics(frame: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for column in SELECTION_SCORE_COLUMNS:
        values = pd.to_numeric(frame[column], errors="coerce")
        result[column] = {
            "count": int(values.notna().sum()),
            "min": None if values.empty else float(values.min()),
            "max": None if values.empty else float(values.max()),
            "mean": None if values.empty else float(values.mean()),
        }
    return result


def _snapshot_matches(
    actual: dict[str, Any] | None,
    expected: dict[str, Any],
) -> bool:
    if not isinstance(actual, dict):
        return False
    comparable_keys = (
        "trade_date",
        "rebalance_mode",
        "top_n",
        "stock_count",
        "avg_total_score",
        "max_total_score",
        "min_total_score",
        "top_stocks",
        "object_key",
    )
    return all(actual.get(key) == expected.get(key) for key in comparable_keys)


def _scoring_config_from_frozen(
    config: dict[str, Any],
) -> ScoringConfig:
    return parse_scoring_config(_raw_config(config))


def _raw_config(config: dict[str, Any]) -> dict[str, Any]:
    weights = config.get("weights") if isinstance(config, dict) else None
    if not isinstance(weights, dict):
        raise ValueError("frozen selection config weights are missing")
    return {
        **weights,
        "scoring": {
            "null_score_policy": config.get("null_score_policy"),
            "neutral_score": config.get("neutral_score"),
            "top_n": config.get("top_n"),
        },
    }


def _freeze_goal23_factor_config(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise Goal24InputError("Goal 23 factor config is missing")
    weights = value.get("weights")
    if not isinstance(weights, dict) or set(weights) != set(
        SCORE_WEIGHT_COLUMNS
    ):
        raise Goal24InputError("Goal 23 factor weights are invalid")
    normalized_weights: dict[str, float] = {}
    for column in SCORE_WEIGHT_COLUMNS:
        weight = float(weights[column])
        if not math.isfinite(weight) or weight < 0:
            raise Goal24InputError(
                f"Goal 23 factor weight is invalid: {column}"
            )
        normalized_weights[column] = weight
    if not math.isclose(
        sum(normalized_weights.values()),
        1.0,
        rel_tol=0.0,
        abs_tol=1e-9,
    ):
        raise Goal24InputError("Goal 23 factor weight sum must equal 1")
    neutral = float(value.get("neutral_score"))
    if (
        value.get("null_score_policy") != "neutral"
        or not math.isfinite(neutral)
        or neutral < 0
        or neutral > 100
    ):
        raise Goal24InputError("Goal 23 null score policy is invalid")
    return {
        "weights": normalized_weights,
        "null_score_policy": "neutral",
        "neutral_score": neutral,
    }


def _tentative_goal23_trade_dates(
    payload: dict[str, Any],
) -> list[str]:
    try:
        return _normalize_trade_dates(
            payload.get("plan", {}).get("trade_dates", [])
        )
    except (ValueError, TypeError):
        return []


def _goal23_range_status(statuses: dict[str, str]) -> str:
    values = list(statuses.values())
    if values and all(value == "COMPLETED" for value in values):
        return "COMPLETED"
    if any(value == "COMPLETED" for value in values):
        return "PARTIAL"
    return "BLOCKED" if any(value == "BLOCKED" for value in values) else "FAILED"


def _range_status(
    statuses: dict[str, str],
    *,
    apply_processed_write: bool,
) -> str:
    values = list(statuses.values())
    if apply_processed_write:
        if values and all(value == "COMPLETED" for value in values):
            return "COMPLETED"
        if values and all(value == "DATABASE_PENDING" for value in values):
            return "DATABASE_PENDING"
        if any(
            value in {"COMPLETED", "DATABASE_PENDING"} for value in values
        ):
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


def _read_selection_object_optional(
    read_fn: ReadSelectionObjectFn,
    object_key: str,
) -> pd.DataFrame | None:
    try:
        return read_fn(object_key)
    except FileNotFoundError:
        return None


def _read_selection_commit_optional(
    read_fn: ReadSelectionCommitFn,
    trade_date: str,
    rebalance_mode: str,
) -> dict[str, Any] | None:
    try:
        return read_fn(trade_date, rebalance_mode)
    except FileNotFoundError:
        return None


def _normalize_trade_dates(values: Iterable[str]) -> list[str]:
    if isinstance(values, (str, bytes)):
        raise ValueError("dates must be an iterable of date strings")
    return sorted({validate_trade_date(str(value)) for value in values})


def _validate_run_id(run_id: str) -> str:
    if not isinstance(run_id, str) or _RUN_ID_PATTERN.fullmatch(run_id) is None:
        raise ValueError(
            "run_id must use 1-128 safe alphanumeric, dot, underscore or "
            "hyphen characters"
        )
    return run_id


def _validate_rebalance_mode(rebalance_mode: str) -> str:
    mode = str(rebalance_mode)
    if mode not in {"monthly", "quarterly"}:
        raise ValueError(
            "rebalance_mode must be monthly or quarterly"
        )
    return mode


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

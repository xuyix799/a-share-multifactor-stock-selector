"""Prospective RED tests for Goal 21 Task 4.

This draft is intentionally outside the repository.  It is designed to be
copied into ``python-engine/tests/test_goal21_historical_backfill.py`` after
Task 3 lands.  The executor is looked up lazily so pytest can collect this
module while ``run_real_history_backfill`` is still absent.

Minimal first slice: gates and base flow (matrix cases 1-6).
Remaining matrix: resume/recovery, checkpointing, isolation, canonical
reconciliation, and root-manifest rebuilding (cases 7-30 below).
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
import importlib
from typing import Any, Callable

import pandas as pd
import pytest

from stock_selector.data import historical_backfill as backfill
from stock_selector.data.real_clean_inputs_landing import KEY_COLUMNS
from stock_selector.providers.schema_contract import get_schema_contract
from stock_selector.storage.partition import build_partition


DATE_1 = "2024-01-02"
DATE_2 = "2024-01-03"
CODE_1 = "000001.SZ"
CODE_2 = "600519.SH"
FIXED_NOW = "2026-07-12T00:00:00Z"


class InjectedFailure(RuntimeError):
    """A classified failure used only at storage/provider boundaries."""

    def __init__(self, message: str, *, failure_category: str = "UNKNOWN") -> None:
        super().__init__(message)
        self.failure_category = failure_category


@dataclass
class MemoryHarness:
    """Storage-agnostic in-memory artifact/canonical test harness.

    Callback events are appended before control returns to the executor.  The
    harness copies every payload/frame at the boundary, so tests exercise the
    executor's durable evidence instead of sharing mutable test objects.
    """

    json_objects: dict[str, dict[str, Any]] = field(default_factory=dict)
    parquet_objects: dict[str, pd.DataFrame] = field(default_factory=dict)
    canonical_objects: dict[tuple[str, str], pd.DataFrame] = field(default_factory=dict)
    events: list[tuple[Any, ...]] = field(default_factory=list)
    fetch_factory: Callable[[dict[str, Any]], Any] | None = None
    forbidden: set[str] = field(default_factory=set)
    corrupt_next_parquet_write: bool = False
    corrupt_next_parquet_read: bool = False
    corrupt_next_canonical_write: bool = False
    corrupt_next_canonical_read: bool = False
    json_write_hook: Callable[[str, dict[str, Any]], None] | None = None
    parquet_write_hook: Callable[[str, pd.DataFrame], None] | None = None
    canonical_read_hook: Callable[[str, str, pd.DataFrame | None], None] | None = None
    canonical_write_hook: Callable[[str, str, pd.DataFrame], None] | None = None
    canonical_after_write_hook: Callable[[str, str, pd.DataFrame], None] | None = None

    def forbid(self, *boundaries: str) -> None:
        self.forbidden.update(boundaries)

    def allow_all(self) -> None:
        self.forbidden.clear()

    def reset_events(self) -> None:
        self.events.clear()

    def _guard(self, boundary: str) -> None:
        if boundary in self.forbidden:
            raise AssertionError(f"disabled boundary was called: {boundary}")

    def artifact_read_json(self, key: str) -> dict[str, Any]:
        self._guard("artifact_json_read")
        self.events.append(("artifact_json_read", key))
        if key not in self.json_objects:
            raise FileNotFoundError(key)
        return deepcopy(self.json_objects[key])

    def artifact_write_json(self, key: str, payload: dict[str, Any]) -> str:
        self._guard("artifact_json_write")
        snapshot = deepcopy(payload)
        event_kind = "artifact_json_write"
        if "/chunk_id=" in key and key.endswith("/manifest.json"):
            event_kind = "chunk_manifest_write"
        elif "/attempt=" in key and key.endswith("/report.json"):
            event_kind = "attempt_report_write"
        elif key.endswith("/plan.json"):
            event_kind = "plan_write"
        elif key.endswith("/manifest.json"):
            event_kind = "root_manifest_write"
        self.events.append((event_kind, key, snapshot.get("state")))
        if self.json_write_hook is not None:
            self.json_write_hook(key, snapshot)
        self.json_objects[key] = snapshot
        return f"memory-json://{key}"

    def artifact_read_parquet(self, key: str) -> pd.DataFrame:
        self._guard("artifact_parquet_read")
        self.events.append(("artifact_parquet_read", key))
        if key not in self.parquet_objects:
            raise FileNotFoundError(key)
        frame = self.parquet_objects[key].copy(deep=True)
        if self.corrupt_next_parquet_read:
            self.corrupt_next_parquet_read = False
            frame = _corrupt_frame(frame)
        return frame

    def artifact_write_parquet(self, key: str, frame: pd.DataFrame) -> str:
        self._guard("artifact_parquet_write")
        snapshot = frame.copy(deep=True)
        self.events.append(("artifact_parquet_write", key, len(snapshot)))
        if self.parquet_write_hook is not None:
            self.parquet_write_hook(key, snapshot)
        if self.corrupt_next_parquet_write:
            self.corrupt_next_parquet_write = False
            snapshot = _corrupt_frame(snapshot)
        self.parquet_objects[key] = snapshot
        return f"memory-parquet://{key}"

    def fetch_chunk(self, chunk: dict[str, Any]) -> Any:
        self._guard("fetch")
        self.events.append(("fetch", chunk["chunk_id"]))
        if self.fetch_factory is None:
            raise AssertionError("fetch_factory was not configured")
        return self.fetch_factory(deepcopy(chunk))

    def canonical_read(self, dataset: str, trade_date: str) -> pd.DataFrame | None:
        self._guard("canonical_read")
        self.events.append(("canonical_read", dataset, trade_date))
        frame = self.canonical_objects.get((dataset, trade_date))
        if self.canonical_read_hook is not None:
            self.canonical_read_hook(
                dataset,
                trade_date,
                None if frame is None else frame.copy(deep=True),
            )
        if frame is None:
            return None
        result = frame.copy(deep=True)
        if self.corrupt_next_canonical_read:
            self.corrupt_next_canonical_read = False
            result = _corrupt_frame(result)
        return result

    def canonical_write(self, dataset: str, trade_date: str, frame: pd.DataFrame) -> str:
        self._guard("canonical_write")
        snapshot = frame.copy(deep=True)
        self.events.append(("canonical_write", dataset, trade_date, len(snapshot)))
        if self.canonical_write_hook is not None:
            self.canonical_write_hook(dataset, trade_date, snapshot)
        if self.corrupt_next_canonical_write:
            self.corrupt_next_canonical_write = False
            snapshot = _corrupt_frame(snapshot)
        self.canonical_objects[(dataset, trade_date)] = snapshot
        if self.canonical_after_write_hook is not None:
            self.canonical_after_write_hook(dataset, trade_date, snapshot.copy(deep=True))
        # Deliberately not the logical raw/... object key.  The executor must
        # derive canonical identity with build_partition, not trust this locator.
        return f"C:/memory-canonical/{dataset}/{trade_date}/part.parquet"

    def manifest(self, plan: dict[str, Any], chunk: dict[str, Any] | None = None) -> dict[str, Any]:
        chunk = chunk or plan["chunks"][0]
        key = _keys(plan)["chunks"][chunk["chunk_id"]]["manifest"]
        return deepcopy(self.json_objects[key])

    def attempt_report(self, plan: dict[str, Any], chunk: dict[str, Any], attempt: int) -> dict[str, Any]:
        template = _keys(plan)["chunks"][chunk["chunk_id"]]["attempt_report_template"]
        return deepcopy(self.json_objects[template.format(attempt=attempt)])


def _corrupt_frame(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy(deep=True)
    if result.empty:
        result.attrs["corrupt"] = True
        return result
    for column in result.columns:
        if column not in {"stock_code", "index_code", "trade_date", "report_period", "announce_date"}:
            value = result.iloc[0][column]
            if isinstance(value, bool):
                result.loc[result.index[0], column] = not value
            elif isinstance(value, (int, float)):
                result.loc[result.index[0], column] = value + 1
            else:
                result.loc[result.index[0], column] = f"{value}-corrupt"
            return result
    result = pd.concat([result, result.iloc[[0]]], ignore_index=True)
    return result


def _plan(
    *,
    run_id: str = "goal21-task4-test",
    dataset: str = "adj_factor",
    codes: list[str] | None = None,
    start_date: str = DATE_1,
    end_date: str = DATE_1,
    code_batch_size: int = 10,
    date_batch_days: int = 31,
) -> dict[str, Any]:
    return backfill.build_history_backfill_plan(
        run_id=run_id,
        start_date=start_date,
        end_date=end_date,
        codes=list(codes or [CODE_1]),
        code_batch_size=code_batch_size,
        date_batch_days=date_batch_days,
        report_period_months=3,
        datasets=[dataset],
        generated_at_fn=lambda: FIXED_NOW,
    )


def _keys(plan: dict[str, Any]) -> dict[str, Any]:
    return backfill.build_history_backfill_output_keys(plan["run_id"], plan["chunks"])


def _runner() -> Callable[..., dict[str, Any]]:
    runner = getattr(backfill, "run_real_history_backfill", None)
    assert callable(runner), "Task 4 executor run_real_history_backfill is missing"
    return runner


def _run(
    harness: MemoryHarness,
    plan: dict[str, Any],
    *,
    provider: bool,
    apply: bool,
    resume: bool = True,
    force: bool = False,
    fetch_chunk_fn: Any = ...,
    canonical_read_fn: Any = ...,
    canonical_write_fn: Any = ...,
) -> dict[str, Any]:
    return _runner()(
        plan=deepcopy(plan),
        artifact_read_json_fn=harness.artifact_read_json,
        artifact_write_json_fn=harness.artifact_write_json,
        artifact_read_parquet_fn=harness.artifact_read_parquet,
        artifact_write_parquet_fn=harness.artifact_write_parquet,
        fetch_chunk_fn=harness.fetch_chunk if fetch_chunk_fn is ... else fetch_chunk_fn,
        canonical_read_fn=harness.canonical_read if canonical_read_fn is ... else canonical_read_fn,
        canonical_write_fn=harness.canonical_write if canonical_write_fn is ... else canonical_write_fn,
        provider_call_enabled=provider,
        apply_standard_write=apply,
        resume=resume,
        force=force,
        generated_at_fn=lambda: FIXED_NOW,
    )


def _provider_module():
    try:
        return importlib.import_module("stock_selector.providers.historical_provider")
    except ModuleNotFoundError as exc:
        pytest.fail(f"Task 3 historical provider module is missing: {exc}")


def _adj_frame(
    *,
    codes: list[str] | tuple[str, ...] = (CODE_1,),
    trade_dates: list[str] | tuple[str, ...] = (DATE_1,),
    offset: float = 0.0,
) -> pd.DataFrame:
    rows = []
    for code_index, code in enumerate(codes):
        for date_index, trade_date in enumerate(trade_dates):
            rows.append(
                {
                    "stock_code": code,
                    "trade_date": trade_date,
                    "adj_factor": 1.0 + offset + code_index * 0.1 + date_index * 0.01,
                }
            )
    return pd.DataFrame(rows, columns=get_schema_contract("adj_factor").columns)


def _daily_price_frame(
    *,
    codes: list[str] | tuple[str, ...] = (CODE_1,),
    trade_dates: list[str] | tuple[str, ...] = (DATE_1,),
    close_offset: float = 0.0,
) -> pd.DataFrame:
    rows = []
    for code_index, code in enumerate(codes):
        for date_index, trade_date in enumerate(trade_dates):
            close = 10.0 + close_offset + code_index + date_index * 0.1
            rows.append(
                {
                    "stock_code": code,
                    "trade_date": trade_date,
                    "open": close - 0.1,
                    "high": close + 0.5,
                    "low": close - 0.5,
                    "close": close,
                    "pre_close": close - 0.2,
                    "volume": 1000.0 + code_index,
                    "amount": 10000.0 + date_index,
                    "pct_chg": 1.0,
                    "is_paused": False,
                    "limit_up": close + 1.0,
                    "limit_down": close - 1.0,
                }
            )
    return pd.DataFrame(rows, columns=get_schema_contract("daily_price").columns)


def _financial_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "stock_code": CODE_1,
                "report_period": DATE_1,
                "announce_date": DATE_1,
                "revenue_yoy": 10.0,
                "net_profit_yoy": 8.0,
                "roe": 0.12,
                "gross_margin": 0.30,
                "debt_ratio": 0.40,
                "operating_cashflow": 100.0,
            },
            {
                "stock_code": CODE_1,
                "report_period": DATE_1,
                "announce_date": DATE_2,
                "revenue_yoy": 11.0,
                "net_profit_yoy": 9.0,
                "roe": 0.13,
                "gross_margin": 0.31,
                "debt_ratio": 0.41,
                "operating_cashflow": 110.0,
            },
        ],
        columns=get_schema_contract("financial").columns,
    )


def _st_frame(
    *,
    code: str = CODE_1,
    start_date: str = DATE_1,
    end_date: str | None = DATE_2,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "stock_code": code,
                "st_type": "ST",
                "start_date": start_date,
                "end_date": end_date,
                "source": "fixture_history",
            }
        ],
        columns=get_schema_contract("st_history").columns,
    )


def _stock_basic_frame(*, list_date: str = "1991-04-03") -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "stock_code": CODE_1,
                "stock_name": "Fixture Bank",
                "exchange": "SZSE",
                "list_date": list_date,
                "delist_date": None,
                "industry": "Bank",
                "market_type": "Main Board",
                "is_st": False,
                "trade_date": DATE_1,
            }
        ],
        columns=get_schema_contract("stock_basic").columns,
    )


def _fetch_result(
    chunk: dict[str, Any],
    *,
    frame: pd.DataFrame | None = None,
    canonical_trade_dates: tuple[str, ...] | None = None,
    provider_status: str = "FETCHED",
    valid_empty: bool = False,
) -> Any:
    provider = _provider_module()
    dataset = chunk["dataset"]
    frame = frame.copy(deep=True) if frame is not None else _adj_frame(
        codes=tuple(chunk.get("codes", [])),
        trade_dates=(chunk["start_date"],),
    )
    dates = canonical_trade_dates or tuple(
        sorted(frame["trade_date"].astype(str).unique()) if "trade_date" in frame else [chunk["start_date"]]
    )
    complete = provider_status in {"FETCHED", "VALID_EMPTY"}
    axis_by_dataset = {
        "financial": "report_period_x_announce_date",
        "st_history": "stock_code_x_interval",
        "benchmark_price": "index_code_x_open_trade_date",
    }
    partition_strategy = {
        "financial": "FINANCIAL_ANNOUNCE_DATE_AS_OF",
        "st_history": "ST_INTERVAL_HISTORY",
    }.get(dataset, "BY_TRADE_DATE_COLUMN")
    source_semantics = {
        "financial": "TRUSTED_STANDARD_FINANCIAL_SOURCE",
        "st_history": "HISTORICAL_INTERVAL_SOURCE",
    }.get(dataset, "HISTORICAL_RANGE_SOURCE")
    coverage = {
        "axis": axis_by_dataset.get(dataset, "stock_code_x_open_trade_date"),
        "complete": complete,
        "expected_count": 0 if valid_empty else len(frame),
        "covered_count": len(frame),
        "missing": [],
        "requested_codes": list(chunk.get("codes", [])),
        "requested_indexes": list(chunk.get("index_codes", [])),
        "requested_start_date": chunk["start_date"],
        "requested_end_date": chunk["end_date"],
        "canonical_trade_dates": list(dates),
        "valid_empty": valid_empty,
    }
    return provider.HistoricalChunkFetchResult(
        dataset=dataset,
        chunk_id=chunk["chunk_id"],
        frame=frame,
        provider_status=provider_status,
        provider_name="fixture",
        source_keys=(f"archive/{dataset}/{chunk['chunk_id']}.parquet",),
        source_semantics=source_semantics,
        provider_calls=(),
        actual_schema=tuple(frame.columns),
        target_schema=tuple(get_schema_contract(dataset).columns),
        dq={"passed": complete, "level": "STRICT", "blocked_reasons": []},
        coverage=coverage,
        canonical_trade_dates=tuple(dates),
        partition_strategy=partition_strategy,
        valid_empty=valid_empty,
        validation={"passed": complete, "errors": []},
        failure=None,
    )


def _configure_adj_fetch(harness: MemoryHarness, plan: dict[str, Any]) -> None:
    global_frame = _adj_frame(codes=tuple(plan["scope"]["codes"]))
    frames = {
        chunk["chunk_id"]: global_frame[
            global_frame["stock_code"].isin(chunk["codes"])
        ].copy(deep=True).reset_index(drop=True)
        for chunk in plan["chunks"]
    }
    harness.fetch_factory = lambda chunk: _fetch_result(chunk, frame=frames[chunk["chunk_id"]])


def _stage(harness: MemoryHarness, plan: dict[str, Any]) -> dict[str, Any]:
    _configure_adj_fetch(harness, plan)
    result = _run(
        harness,
        plan,
        provider=True,
        apply=False,
        canonical_read_fn=None,
        canonical_write_fn=None,
    )
    assert all(harness.manifest(plan, chunk)["state"] == "STAGED" for chunk in plan["chunks"])
    return result


def _summary(result: dict[str, Any]) -> dict[str, Any]:
    assert "summary" in result, "executor result must expose the final pure summary"
    return result["summary"]


def _root_summary(root_manifest: dict[str, Any]) -> dict[str, Any]:
    # The brief does not fix whether manifest.json wraps or directly extends
    # the pure summary.  Accept both without weakening any summary assertion.
    return root_manifest.get("summary", root_manifest)


def _returned_control_key(result: dict[str, Any], name: str) -> str:
    direct = result.get(f"{name}_key")
    if isinstance(direct, str):
        return direct
    output_keys = result.get("output_keys")
    assert isinstance(output_keys, dict), f"executor result must expose {name} control key"
    return output_keys[name]


def _event_count(harness: MemoryHarness, kind: str) -> int:
    return sum(event[0] == kind for event in harness.events)


# ---------------------------------------------------------------------------
# Minimal first slice: gates and base flow (Task 4 RED matrix 1-6)
# ---------------------------------------------------------------------------


def test_executor_dry_run_writes_only_plan_and_pending_root():
    plan = _plan(run_id="goal21-task4-dry-run")
    harness = MemoryHarness()
    harness.forbid(
        "fetch",
        "artifact_parquet_read",
        "artifact_parquet_write",
        "canonical_read",
        "canonical_write",
    )

    result = _run(harness, plan, provider=False, apply=False)

    keys = _keys(plan)
    assert set(harness.json_objects) == {keys["plan"], keys["root_manifest"]}
    assert harness.parquet_objects == {}
    assert _summary(result)["state_counts"]["PENDING"] == plan["chunk_count"]
    assert _summary(result)["accounted_count"] == plan["chunk_count"]
    assert _root_summary(harness.json_objects[keys["root_manifest"]])["state_counts"]["PENDING"] == plan["chunk_count"]
    assert result["attempted_chunk_ids"] == []
    assert result["skipped_chunk_ids"] == []
    assert result["reconciled_chunk_ids"] == []
    assert result["goal"]
    assert result["run_id"] == plan["run_id"]
    assert result["plan_fingerprint"] == plan["plan_fingerprint"]
    assert result["gates"]["provider_call_enabled"] is False
    assert result["gates"]["apply_standard_write"] is False
    assert _returned_control_key(result, "plan") == keys["plan"]
    assert _returned_control_key(result, "root_manifest") == keys["root_manifest"]
    assert result["root_manifest"] == harness.json_objects[keys["root_manifest"]]
    assert all(value is False for value in result["downstream_firewalls"].values())
    assert all("/attempt=" not in key and "/chunk_id=" not in key for key in harness.json_objects)


def test_executor_provider_only_stages_and_never_reads_or_writes_canonical():
    plan = _plan(run_id="goal21-task4-provider-only")
    harness = MemoryHarness()
    _configure_adj_fetch(harness, plan)
    harness.forbid("canonical_read", "canonical_write")

    result = _run(
        harness,
        plan,
        provider=True,
        apply=False,
        canonical_read_fn=None,
        canonical_write_fn=None,
    )

    chunk = plan["chunks"][0]
    manifest = harness.manifest(plan, chunk)
    assert manifest["state"] == "STAGED"
    assert manifest["attempt_count"] == 1
    assert manifest["staging_attempt"] == 1
    assert manifest["staging_key"] in harness.parquet_objects
    read_back = harness.parquet_objects[manifest["staging_key"]]
    assert manifest["staging_checksum"] == backfill.dataframe_checksum(
        read_back,
        key_columns=KEY_COLUMNS[chunk["dataset"]],
    )
    assert _event_count(harness, "fetch") == 1
    assert _event_count(harness, "artifact_parquet_write") == 1
    assert _event_count(harness, "artifact_parquet_read") >= 1
    assert _summary(result)["state_counts"]["STAGED"] == 1


def test_executor_apply_only_reuses_verified_staging_without_fetch():
    plan = _plan(run_id="goal21-task4-apply-only")
    harness = MemoryHarness()
    _stage(harness, plan)
    old_parquet = {key: frame.copy(deep=True) for key, frame in harness.parquet_objects.items()}
    harness.reset_events()
    harness.forbid("fetch", "artifact_parquet_write")

    result = _run(harness, plan, provider=False, apply=True, fetch_chunk_fn=None)

    chunk = plan["chunks"][0]
    manifest = harness.manifest(plan, chunk)
    assert manifest["state"] == "COMPLETED"
    assert manifest["attempt_count"] == 2
    assert manifest["staging_attempt"] == 1
    assert manifest["canonical_keys"] == [build_partition("adj_factor", DATE_1).object_key]
    pd.testing.assert_frame_equal(harness.canonical_objects[("adj_factor", DATE_1)], _adj_frame())
    assert set(harness.parquet_objects) == set(old_parquet)
    for key, expected in old_parquet.items():
        pd.testing.assert_frame_equal(harness.parquet_objects[key], expected)
    assert _event_count(harness, "canonical_write") == 1
    assert _event_count(harness, "canonical_read") >= 2
    assert _summary(result)["state_counts"]["COMPLETED"] == 1


def test_executor_combined_fetch_apply_completes_after_readback():
    plan = _plan(run_id="goal21-task4-combined")
    harness = MemoryHarness()
    _configure_adj_fetch(harness, plan)

    result = _run(harness, plan, provider=True, apply=True)

    chunk = plan["chunks"][0]
    manifest = harness.manifest(plan, chunk)
    assert manifest["state"] == "COMPLETED"
    assert manifest["attempt_count"] == 1
    assert manifest["canonical_keys"] == [build_partition("adj_factor", DATE_1).object_key]
    assert manifest["read_back_result"]["success"] is True
    assert manifest["read_back_result"]["partitions"][0]["exact_read_back_success"] is True
    assert _event_count(harness, "fetch") == 1
    assert _event_count(harness, "artifact_parquet_write") == 1
    assert _event_count(harness, "canonical_write") == 1
    assert _event_count(harness, "canonical_read") >= 2
    assert _summary(result)["canonical_ready"] is True


@pytest.mark.parametrize(
    ("provider_enabled", "apply_enabled", "expected_state"),
    [
        (False, False, "STAGED"),
        (True, False, "STAGED"),
        (False, True, "COMPLETED"),
        (True, True, "COMPLETED"),
    ],
)
def test_force_never_escalates_disabled_gates(provider_enabled, apply_enabled, expected_state):
    plan = _plan(run_id=f"goal21-task4-force-{int(provider_enabled)}-{int(apply_enabled)}")
    harness = MemoryHarness()
    _stage(harness, plan)
    before_attempt = harness.manifest(plan)["attempt_count"]
    harness.reset_events()

    _run(
        harness,
        plan,
        provider=provider_enabled,
        apply=apply_enabled,
        force=True,
    )

    assert _event_count(harness, "fetch") == (1 if provider_enabled else 0)
    assert _event_count(harness, "canonical_write") == (1 if apply_enabled else 0)
    manifest = harness.manifest(plan)
    assert manifest["state"] == expected_state
    expected_attempt = before_attempt if not provider_enabled and not apply_enabled else before_attempt + 1
    assert manifest["attempt_count"] == expected_attempt


def test_existing_plan_identity_mismatch_stops_before_side_effects():
    plan = _plan(run_id="goal21-task4-plan-mismatch", codes=[CODE_1])
    different = _plan(run_id=plan["run_id"], codes=[CODE_1, CODE_2])
    harness = MemoryHarness()
    keys = _keys(plan)
    harness.json_objects[keys["plan"]] = deepcopy(different)
    original = deepcopy(harness.json_objects)
    harness.forbid(
        "fetch",
        "artifact_parquet_read",
        "artifact_parquet_write",
        "canonical_read",
        "canonical_write",
    )

    with pytest.raises(backfill.BackfillPlanningError) as exc_info:
        _run(harness, plan, provider=True, apply=True)

    assert exc_info.value.code == "RUN_PLAN_MISMATCH"
    assert harness.json_objects == original
    assert _event_count(harness, "plan_write") == 0
    assert _event_count(harness, "root_manifest_write") == 0


def test_same_run_identity_resumes_when_rebuilt_plan_has_a_new_generated_timestamp():
    plan = _plan(run_id="goal21-task4-cross-process-resume")
    harness = MemoryHarness()
    _stage(harness, plan)
    persisted_plan = deepcopy(harness.json_objects[_keys(plan)["plan"]])
    rebuilt = deepcopy(plan)
    rebuilt["generated_at"] = "2030-01-01T00:00:00Z"
    harness.reset_events()
    harness.forbid("fetch", "artifact_parquet_write", "canonical_read", "canonical_write")

    result = _run(harness, rebuilt, provider=True, apply=False)

    assert result["skipped_chunk_ids"] == [plan["chunks"][0]["chunk_id"]]
    assert harness.json_objects[_keys(plan)["plan"]] == persisted_plan
    assert _event_count(harness, "plan_write") == 0


def test_executor_rejects_tampered_plan_fingerprint_before_control_writes():
    plan = _plan(run_id="goal21-task4-plan-fingerprint")
    plan["scope"]["end_date"] = DATE_2
    harness = MemoryHarness()
    harness.forbid(
        "fetch",
        "artifact_parquet_read",
        "artifact_parquet_write",
        "canonical_read",
        "canonical_write",
    )

    with pytest.raises(backfill.BackfillPlanningError) as exc_info:
        _run(harness, plan, provider=True, apply=True)

    assert exc_info.value.code == "INVALID_PLAN_FINGERPRINT"
    assert harness.json_objects == {}


def test_manifest_slot_cannot_reuse_another_same_plan_chunk_staging():
    plan = _plan(
        run_id="goal21-task4-foreign-manifest-slot",
        codes=[CODE_1, CODE_2],
        code_batch_size=1,
    )
    harness = MemoryHarness()
    _stage(harness, plan)
    first, second = plan["chunks"]
    harness.json_objects[_manifest_key(plan, first)] = harness.manifest(plan, second)
    harness.reset_events()

    with pytest.raises(backfill.BackfillPlanningError) as exc_info:
        _run(harness, plan, provider=False, apply=True, fetch_chunk_fn=None)

    assert exc_info.value.code == "TAMPERED_MANIFEST_SCOPE"
    assert _event_count(harness, "canonical_read") == 0
    assert _event_count(harness, "canonical_write") == 0


def _complete(harness: MemoryHarness, plan: dict[str, Any]) -> dict[str, Any]:
    _configure_adj_fetch(harness, plan)
    result = _run(harness, plan, provider=True, apply=True)
    assert all(harness.manifest(plan, chunk)["state"] == "COMPLETED" for chunk in plan["chunks"])
    return result


def _manifest_key(plan: dict[str, Any], chunk: dict[str, Any]) -> str:
    return _keys(plan)["chunks"][chunk["chunk_id"]]["manifest"]


def _attempt_report_key(plan: dict[str, Any], chunk: dict[str, Any], attempt: int) -> str:
    template = _keys(plan)["chunks"][chunk["chunk_id"]]["attempt_report_template"]
    return template.format(attempt=attempt)


def _staging_key(plan: dict[str, Any], chunk: dict[str, Any], attempt: int) -> str:
    template = _keys(plan)["chunks"][chunk["chunk_id"]]["staging_template"]
    return template.format(attempt=attempt)


def _mark_stale_running(harness: MemoryHarness, plan: dict[str, Any], chunk: dict[str, Any]) -> int:
    key = _manifest_key(plan, chunk)
    manifest = deepcopy(harness.json_objects[key])
    manifest["state"] = "RUNNING"
    manifest["attempt_count"] += 1
    manifest["failure"] = None
    harness.json_objects[key] = manifest
    return manifest["attempt_count"]


def _replace_scalar(value: Any, old: Any, new: Any) -> Any:
    if value == old and not isinstance(value, (dict, list, tuple)):
        return deepcopy(new)
    if isinstance(value, dict):
        return {key: _replace_scalar(item, old, new) for key, item in value.items()}
    if isinstance(value, list):
        return [_replace_scalar(item, old, new) for item in value]
    if isinstance(value, tuple):
        return tuple(_replace_scalar(item, old, new) for item in value)
    return deepcopy(value)


def _replace_named_scalar(value: Any, names: set[str], new: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: deepcopy(new) if key in names else _replace_named_scalar(item, names, new)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_replace_named_scalar(item, names, new) for item in value]
    if isinstance(value, tuple):
        return tuple(_replace_named_scalar(item, names, new) for item in value)
    return deepcopy(value)


def _replace_verified_staging(
    harness: MemoryHarness,
    plan: dict[str, Any],
    chunk: dict[str, Any],
    frame: pd.DataFrame,
) -> None:
    manifest_key = _manifest_key(plan, chunk)
    manifest = deepcopy(harness.json_objects[manifest_key])
    staging_key = manifest["staging_key"]
    old_checksum = manifest["staging_checksum"]
    new_checksum = backfill.dataframe_checksum(frame, key_columns=KEY_COLUMNS[chunk["dataset"]])
    harness.parquet_objects[staging_key] = frame.copy(deep=True)
    updated_manifest = _replace_scalar(manifest, old_checksum, new_checksum)
    updated_manifest["row_count"] = len(frame)
    harness.json_objects[manifest_key] = updated_manifest
    report_key = _attempt_report_key(plan, chunk, manifest["staging_attempt"])
    if report_key in harness.json_objects:
        updated_report = _replace_scalar(
            harness.json_objects[report_key],
            old_checksum,
            new_checksum,
        )
        harness.json_objects[report_key] = _replace_named_scalar(
            updated_report,
            {"row_count", "staging_row_count"},
            len(frame),
        )


def _report_attempt(report: dict[str, Any]) -> int | None:
    return report.get("attempt", report.get("attempt_count"))


def _partition_records(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    records = manifest["write_result"].get("partitions")
    assert isinstance(records, list), "write_result must preserve per-partition details"
    return records


# ---------------------------------------------------------------------------
# Resume, force, and stale RUNNING (Task 4 RED matrix 7-15)
# ---------------------------------------------------------------------------


def test_completed_resume_skip_requires_staging_and_exact_canonical_reconciliation():
    plan = _plan(run_id="goal21-task4-completed-skip")
    harness = MemoryHarness()
    _complete(harness, plan)
    chunk = plan["chunks"][0]
    before = harness.manifest(plan, chunk)
    harness.reset_events()

    result = _run(harness, plan, provider=True, apply=True, resume=True)

    after = harness.manifest(plan, chunk)
    assert after["attempt_count"] == before["attempt_count"]
    assert after["state"] == "COMPLETED"
    assert _event_count(harness, "fetch") == 0
    assert _event_count(harness, "artifact_parquet_write") == 0
    assert _event_count(harness, "artifact_parquet_read") >= 1
    assert _event_count(harness, "canonical_read") >= 1
    assert _event_count(harness, "canonical_write") == 0
    assert result["skipped_chunk_ids"] == [chunk["chunk_id"]]


@pytest.mark.parametrize("damage", ["missing", "corrupt"])
@pytest.mark.parametrize("provider_enabled", [True, False])
def test_completed_manifest_with_missing_or_corrupt_staging_is_not_trusted(damage, provider_enabled):
    plan = _plan(run_id=f"goal21-task4-untrusted-{damage}-{int(provider_enabled)}")
    harness = MemoryHarness()
    _complete(harness, plan)
    chunk = plan["chunks"][0]
    manifest = harness.manifest(plan, chunk)
    if damage == "missing":
        harness.parquet_objects.pop(manifest["staging_key"])
    else:
        harness.parquet_objects[manifest["staging_key"]] = _corrupt_frame(
            harness.parquet_objects[manifest["staging_key"]]
        )
    harness.reset_events()

    _run(
        harness,
        plan,
        provider=provider_enabled,
        apply=True,
        resume=True,
        fetch_chunk_fn=harness.fetch_chunk if provider_enabled else None,
    )

    final = harness.manifest(plan, chunk)
    if provider_enabled:
        assert _event_count(harness, "fetch") == 1
        assert final["state"] == "COMPLETED"
        assert final["attempt_count"] == 2
    else:
        assert _event_count(harness, "fetch") == 0
        assert _event_count(harness, "canonical_write") == 0
        assert final["state"] == "BLOCKED"
        assert final["failure"]["category"] in {"CONFIGURATION_ERROR", "READBACK_FAILED"}


def test_staged_apply_resume_uses_source_attempt_and_new_attempt_report():
    plan = _plan(run_id="goal21-task4-staged-apply")
    harness = MemoryHarness()
    _stage(harness, plan)
    chunk = plan["chunks"][0]
    report_1_key = _attempt_report_key(plan, chunk, 1)
    staging_1_key = _staging_key(plan, chunk, 1)
    report_1 = deepcopy(harness.json_objects[report_1_key])
    staging_1 = harness.parquet_objects[staging_1_key].copy(deep=True)
    harness.reset_events()

    _run(harness, plan, provider=False, apply=True, fetch_chunk_fn=None)

    final = harness.manifest(plan, chunk)
    report_2 = harness.attempt_report(plan, chunk, 2)
    assert final["state"] == "COMPLETED"
    assert final["attempt_count"] == 2
    assert final["staging_attempt"] == 1
    assert _report_attempt(report_2) == 2
    assert report_2["staging_attempt"] == 1
    assert harness.json_objects[report_1_key] == report_1
    pd.testing.assert_frame_equal(harness.parquet_objects[staging_1_key], staging_1)
    assert _staging_key(plan, chunk, 2) not in harness.parquet_objects


def test_force_provider_creates_new_attempt_staging_without_overwrite():
    plan = _plan(run_id="goal21-task4-force-provider")
    harness = MemoryHarness()
    _stage(harness, plan)
    chunk = plan["chunks"][0]
    old_staging = harness.parquet_objects[_staging_key(plan, chunk, 1)].copy(deep=True)
    old_report = deepcopy(harness.json_objects[_attempt_report_key(plan, chunk, 1)])
    harness.reset_events()

    _run(harness, plan, provider=True, apply=False, force=True)

    final = harness.manifest(plan, chunk)
    assert final["state"] == "STAGED"
    assert final["attempt_count"] == 2
    assert final["staging_attempt"] == 2
    assert _staging_key(plan, chunk, 1) in harness.parquet_objects
    assert _staging_key(plan, chunk, 2) in harness.parquet_objects
    pd.testing.assert_frame_equal(harness.parquet_objects[_staging_key(plan, chunk, 1)], old_staging)
    assert harness.json_objects[_attempt_report_key(plan, chunk, 1)] == old_report


@pytest.mark.parametrize(
    ("provider_enabled", "apply_enabled", "expected_state"),
    [
        (True, False, "STAGED"),
        (False, True, "COMPLETED"),
        (True, True, "COMPLETED"),
    ],
)
def test_no_resume_reruns_only_enabled_stages_with_monotonic_attempt(
    provider_enabled,
    apply_enabled,
    expected_state,
):
    plan = _plan(run_id=f"goal21-task4-no-resume-{int(provider_enabled)}-{int(apply_enabled)}")
    harness = MemoryHarness()
    _complete(harness, plan)
    before_attempt = harness.manifest(plan)["attempt_count"]
    harness.reset_events()

    _run(
        harness,
        plan,
        provider=provider_enabled,
        apply=apply_enabled,
        resume=False,
        fetch_chunk_fn=harness.fetch_chunk if provider_enabled else None,
    )

    final = harness.manifest(plan)
    assert final["state"] == expected_state
    assert final["attempt_count"] == before_attempt + 1
    assert _event_count(harness, "fetch") == (1 if provider_enabled else 0)
    assert _event_count(harness, "canonical_write") == (1 if apply_enabled else 0)


def test_stale_running_with_verified_staging_reconciles_to_staged_without_refetch():
    plan = _plan(run_id="goal21-task4-stale-running-staging")
    harness = MemoryHarness()
    _stage(harness, plan)
    chunk = plan["chunks"][0]
    stale_attempt = _mark_stale_running(harness, plan, chunk)
    harness.reset_events()

    _run(harness, plan, provider=True, apply=False, resume=True)

    final = harness.manifest(plan, chunk)
    assert _event_count(harness, "fetch") == 0
    assert final["state"] == "STAGED"
    assert final["attempt_count"] == stale_attempt + 1
    assert final["staging_attempt"] == 1


def test_canonical_written_before_checkpoint_is_reconciled_without_second_write():
    plan = _plan(run_id="goal21-task4-reconcile-existing-write")
    harness = MemoryHarness()
    _configure_adj_fetch(harness, plan)
    interrupted = {"done": False}

    def interrupt_completed_checkpoint(key, payload):
        if (
            not interrupted["done"]
            and "/chunk_id=" in key
            and payload.get("state") == "COMPLETED"
        ):
            interrupted["done"] = True
            raise KeyboardInterrupt()

    harness.json_write_hook = interrupt_completed_checkpoint
    with pytest.raises(KeyboardInterrupt):
        _run(harness, plan, provider=True, apply=True)
    assert harness.manifest(plan)["state"] == "INTERRUPTED"
    assert _event_count(harness, "canonical_write") == 1
    harness.json_write_hook = None
    harness.reset_events()

    result = _run(harness, plan, provider=False, apply=True, fetch_chunk_fn=None)

    final = harness.manifest(plan)
    assert final["state"] == "COMPLETED"
    assert _event_count(harness, "canonical_write") == 0
    assert result["reconciled_chunk_ids"] == [plan["chunks"][0]["chunk_id"]]
    assert final["write_result"]["status"] == "RECONCILED_EXISTING_WRITE"


def test_stale_running_partial_canonical_is_idempotently_repaired_when_apply_enabled():
    plan = _plan(
        run_id="goal21-task4-partial-canonical",
        codes=[CODE_1, CODE_2],
        code_batch_size=2,
    )
    harness = MemoryHarness()
    _stage(harness, plan)
    chunk = plan["chunks"][0]
    _mark_stale_running(harness, plan, chunk)
    expected = _adj_frame(codes=(CODE_1, CODE_2))
    harness.canonical_objects[("adj_factor", DATE_1)] = expected.iloc[[0]].copy(deep=True)
    harness.reset_events()

    _run(harness, plan, provider=False, apply=True, fetch_chunk_fn=None)

    final = harness.manifest(plan, chunk)
    assert final["state"] == "COMPLETED"
    assert _event_count(harness, "canonical_write") == 1
    pd.testing.assert_frame_equal(
        harness.canonical_objects[("adj_factor", DATE_1)].reset_index(drop=True),
        expected.sort_values(KEY_COLUMNS["adj_factor"]).reset_index(drop=True),
    )


def test_resume_allows_later_partition_additions_but_rejects_changed_chunk_values():
    plan = _plan(run_id="goal21-task4-subset-reconciliation")
    harness = MemoryHarness()
    _complete(harness, plan)
    chunk = plan["chunks"][0]
    later = _adj_frame(codes=(CODE_2,), offset=4.0)
    harness.canonical_objects[("adj_factor", DATE_1)] = pd.concat(
        [harness.canonical_objects[("adj_factor", DATE_1)], later],
        ignore_index=True,
    ).sort_values(KEY_COLUMNS["adj_factor"]).reset_index(drop=True)
    harness.reset_events()

    first_resume = _run(harness, plan, provider=False, apply=True, fetch_chunk_fn=None)

    assert _event_count(harness, "canonical_write") == 0
    assert first_resume["skipped_chunk_ids"] == [chunk["chunk_id"]]
    canonical = harness.canonical_objects[("adj_factor", DATE_1)]
    canonical.loc[canonical["stock_code"] == CODE_1, "adj_factor"] = 99.0
    harness.reset_events()

    _run(harness, plan, provider=False, apply=True, fetch_chunk_fn=None)

    repaired = harness.canonical_objects[("adj_factor", DATE_1)]
    assert _event_count(harness, "canonical_write") == 1
    assert repaired.loc[repaired["stock_code"] == CODE_1, "adj_factor"].item() == pytest.approx(1.0)
    assert repaired.loc[repaired["stock_code"] == CODE_2, "adj_factor"].item() == later["adj_factor"].item()


def test_completed_resume_never_skips_semantically_invalid_staging_and_canonical():
    plan = _plan(run_id="goal21-task4-invalid-completed-skip")
    harness = MemoryHarness()
    _complete(harness, plan)
    chunk = plan["chunks"][0]
    invalid = _adj_frame()
    invalid.loc[:, "adj_factor"] = 0.0
    _replace_verified_staging(harness, plan, chunk, invalid)
    harness.canonical_objects[("adj_factor", DATE_1)] = invalid.copy(deep=True)
    harness.reset_events()

    result = _run(harness, plan, provider=False, apply=True, fetch_chunk_fn=None)

    final = harness.manifest(plan, chunk)
    assert final["state"] == "BLOCKED"
    assert final["failure"]["category"] == "DQ_FAILED"
    assert result["skipped_chunk_ids"] == []
    assert result["summary"]["canonical_ready"] is False
    assert _event_count(harness, "canonical_write") == 0


# ---------------------------------------------------------------------------
# Attempt/checkpoint order and interruption (Task 4 RED matrix 16-20)
# ---------------------------------------------------------------------------


def test_success_checkpoint_is_last_after_staging_write_read_canonical_write_read_and_attempt_report():
    plan = _plan(run_id="goal21-task4-checkpoint-order")
    harness = MemoryHarness()
    _configure_adj_fetch(harness, plan)

    _run(harness, plan, provider=True, apply=True)

    lifecycle = []
    running_seen = False
    for event in harness.events:
        kind = event[0]
        if kind == "chunk_manifest_write" and event[2] == "RUNNING":
            running_seen = True
            lifecycle.append("manifest:RUNNING")
        elif not running_seen:
            continue
        elif kind == "fetch":
            lifecycle.append("fetch")
        elif kind == "artifact_parquet_write":
            lifecycle.append("staging:write")
        elif kind == "artifact_parquet_read":
            lifecycle.append("staging:read")
        elif kind == "canonical_read":
            lifecycle.append("canonical:read")
        elif kind == "canonical_write":
            lifecycle.append("canonical:write")
        elif kind == "attempt_report_write":
            lifecycle.append("report:write")
        elif kind == "chunk_manifest_write":
            lifecycle.append(f"manifest:{event[2]}")
        elif kind == "root_manifest_write":
            lifecycle.append("root:write")

    assert lifecycle == [
        "manifest:RUNNING",
        "fetch",
        "staging:write",
        "staging:read",
        "canonical:read",
        "canonical:write",
        "canonical:read",
        "report:write",
        "manifest:COMPLETED",
        "root:write",
    ]


def test_attempt_report_collision_never_overwrites_different_immutable_payload():
    plan = _plan(run_id="goal21-task4-report-collision")
    harness = MemoryHarness()
    _stage(harness, plan)
    chunk = plan["chunks"][0]
    report_2_key = _attempt_report_key(plan, chunk, 2)
    collision = {
        "schema_version": "foreign.immutable.report.v1",
        "attempt": 2,
        "proof": "must survive byte-for-byte",
    }
    harness.json_objects[report_2_key] = deepcopy(collision)
    harness.reset_events()

    _run(harness, plan, provider=True, apply=False, force=True)

    final = harness.manifest(plan, chunk)
    assert harness.json_objects[report_2_key] == collision
    assert final["state"] in {"FAILED", "BLOCKED"}
    assert final["failure"]["category"]
    assert all(
        not (event[0] == "attempt_report_write" and event[1] == report_2_key)
        for event in harness.events
    )


def test_attempt_report_collision_created_during_fetch_is_rechecked_before_write():
    plan = _plan(run_id="goal21-task4-report-race")
    harness = MemoryHarness()
    chunk = plan["chunks"][0]
    report_key = _attempt_report_key(plan, chunk, 1)
    collision = {"foreign": "must-not-overwrite"}

    def fetch(planned_chunk):
        harness.json_objects[report_key] = deepcopy(collision)
        return _fetch_result(planned_chunk)

    harness.fetch_factory = fetch
    _run(harness, plan, provider=True, apply=False)

    assert harness.json_objects[report_key] == collision
    final = harness.manifest(plan, chunk)
    assert final["state"] == "FAILED"
    assert final["failure"]["category"] == "WRITE_FAILED"
    assert all(
        not (event[0] == "attempt_report_write" and event[1] == report_key)
        for event in harness.events
    )


@pytest.mark.parametrize(
    "interrupt_at",
    ["fetch", "staging_write", "canonical_write", "final_checkpoint"],
)
def test_keyboard_interrupt_at_fetch_staging_write_canonical_write_and_final_checkpoint_is_persisted_then_reraised(
    interrupt_at,
):
    plan = _plan(
        run_id=f"goal21-task4-interrupt-{interrupt_at}",
        codes=[CODE_1, CODE_2],
        code_batch_size=1,
    )
    harness = MemoryHarness()
    _configure_adj_fetch(harness, plan)
    first, second = plan["chunks"]
    original_fetch = harness.fetch_factory
    fired = {"done": False}

    if interrupt_at == "fetch":
        def fetch_with_interrupt(chunk):
            if not fired["done"]:
                fired["done"] = True
                raise KeyboardInterrupt()
            return original_fetch(chunk)

        harness.fetch_factory = fetch_with_interrupt
    elif interrupt_at == "staging_write":
        def interrupt_staging(_key, _frame):
            if not fired["done"]:
                fired["done"] = True
                raise KeyboardInterrupt()

        harness.parquet_write_hook = interrupt_staging
    elif interrupt_at == "canonical_write":
        def interrupt_canonical(_dataset, _trade_date, _frame):
            if not fired["done"]:
                fired["done"] = True
                raise KeyboardInterrupt()

        harness.canonical_write_hook = interrupt_canonical
    else:
        def interrupt_checkpoint(key, payload):
            if (
                not fired["done"]
                and "/chunk_id=" in key
                and payload.get("state") == "COMPLETED"
            ):
                fired["done"] = True
                raise KeyboardInterrupt()

        harness.json_write_hook = interrupt_checkpoint

    with pytest.raises(KeyboardInterrupt):
        _run(harness, plan, provider=True, apply=True)

    assert fired["done"] is True
    assert harness.manifest(plan, first)["state"] == "INTERRUPTED"
    assert _attempt_report_key(plan, first, 1) in harness.json_objects
    assert _manifest_key(plan, second) not in harness.json_objects
    assert all(
        not (event[0] == "fetch" and event[1] == second["chunk_id"])
        for event in harness.events
    )


def test_resume_after_write_then_keyboard_interrupt_uses_readback_and_avoids_duplicate_write():
    plan = _plan(run_id="goal21-task4-interrupt-after-write")
    harness = MemoryHarness()
    _configure_adj_fetch(harness, plan)
    read_count = {"value": 0}

    def interrupt_first_readback(_dataset, _trade_date, _frame):
        read_count["value"] += 1
        if read_count["value"] == 2:
            raise KeyboardInterrupt()

    harness.canonical_read_hook = interrupt_first_readback
    with pytest.raises(KeyboardInterrupt):
        _run(harness, plan, provider=True, apply=True)
    assert harness.manifest(plan)["state"] == "INTERRUPTED"
    assert ("adj_factor", DATE_1) in harness.canonical_objects
    harness.canonical_read_hook = None
    harness.reset_events()

    result = _run(harness, plan, provider=False, apply=True, fetch_chunk_fn=None)

    assert harness.manifest(plan)["state"] == "COMPLETED"
    assert _event_count(harness, "canonical_write") == 0
    assert result["reconciled_chunk_ids"] == [plan["chunks"][0]["chunk_id"]]


def test_running_checkpoint_failure_aborts_before_provider_or_canonical_side_effect():
    plan = _plan(run_id="goal21-task4-running-checkpoint-failure")
    harness = MemoryHarness()
    _configure_adj_fetch(harness, plan)

    def fail_running_checkpoint(key, payload):
        if "/chunk_id=" in key and payload.get("state") == "RUNNING":
            raise InjectedFailure("RUNNING checkpoint unavailable", failure_category="WRITE_FAILED")

    harness.json_write_hook = fail_running_checkpoint
    with pytest.raises(InjectedFailure, match="RUNNING checkpoint unavailable"):
        _run(harness, plan, provider=True, apply=True)

    assert _event_count(harness, "fetch") == 0
    assert _event_count(harness, "artifact_parquet_write") == 0
    assert _event_count(harness, "canonical_read") == 0
    assert _event_count(harness, "canonical_write") == 0
    assert _manifest_key(plan, plan["chunks"][0]) not in harness.json_objects


def test_final_manifest_checkpoint_failure_leaves_truthful_ready_attempt_report():
    plan = _plan(run_id="goal21-task4-final-checkpoint-failure")
    harness = MemoryHarness()
    _configure_adj_fetch(harness, plan)

    def fail_completed_manifest(key, payload):
        if key.endswith("/manifest.json") and "/chunk_id=" in key and payload.get("state") == "COMPLETED":
            raise InjectedFailure("final checkpoint unavailable", failure_category="WRITE_FAILED")

    harness.json_write_hook = fail_completed_manifest
    result = _run(harness, plan, provider=True, apply=True)

    chunk = plan["chunks"][0]
    final = harness.manifest(plan, chunk)
    report = harness.attempt_report(plan, chunk, 1)
    assert final["state"] == "FAILED"
    assert final["failure"]["category"] == "WRITE_FAILED"
    assert report["state"] == "READY_TO_CHECKPOINT"
    assert report["checkpoint_target_state"] == "COMPLETED"
    assert report["failure"] is None
    assert result["summary"]["canonical_ready"] is False


# ---------------------------------------------------------------------------
# Isolation, checksum, and read-back (Task 4 RED matrix 21-23)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("failure_category", "expected_state"),
    [("TRANSIENT_PROVIDER_ERROR", "FAILED"), ("DQ_FAILED", "BLOCKED")],
)
def test_ordinary_chunk_failure_is_checkpointed_and_next_independent_chunk_continues(
    failure_category,
    expected_state,
):
    plan = _plan(
        run_id=f"goal21-task4-isolation-{failure_category.lower()}",
        codes=[CODE_1, CODE_2],
        code_batch_size=1,
    )
    harness = MemoryHarness()
    first, second = plan["chunks"]

    def fetch(chunk):
        if chunk["chunk_id"] == first["chunk_id"]:
            raise InjectedFailure("isolated provider failure", failure_category=failure_category)
        return _fetch_result(chunk, frame=_adj_frame(codes=tuple(chunk["codes"])))

    harness.fetch_factory = fetch
    result = _run(harness, plan, provider=True, apply=False)

    first_manifest = harness.manifest(plan, first)
    second_manifest = harness.manifest(plan, second)
    assert first_manifest["state"] == expected_state
    assert first_manifest["failure"]["category"] == failure_category
    assert second_manifest["state"] == "STAGED"
    assert _summary(result)["accounted_count"] == 2
    assert _summary(result)["state_counts"][expected_state] == 1
    assert _summary(result)["state_counts"]["STAGED"] == 1


def test_corrupt_staging_writer_is_detected_by_immediate_checksum_and_blocks_canonical_write():
    plan = _plan(run_id="goal21-task4-corrupt-staging")
    harness = MemoryHarness(corrupt_next_parquet_write=True)
    _configure_adj_fetch(harness, plan)

    _run(harness, plan, provider=True, apply=True)

    final = harness.manifest(plan)
    assert final["state"] == "FAILED"
    assert final["failure"]["category"] == "READBACK_FAILED"
    assert _event_count(harness, "artifact_parquet_read") == 1
    assert _event_count(harness, "canonical_write") == 0


@pytest.mark.parametrize("corruption", ["writer", "readback"])
def test_corrupt_canonical_writer_or_readback_is_never_completed(corruption):
    plan = _plan(run_id=f"goal21-task4-corrupt-canonical-{corruption}")
    harness = MemoryHarness()
    _configure_adj_fetch(harness, plan)
    if corruption == "writer":
        harness.corrupt_next_canonical_write = True
    else:
        harness.corrupt_next_canonical_read = True

    _run(harness, plan, provider=True, apply=True)

    final = harness.manifest(plan)
    assert final["state"] == "FAILED"
    assert final["failure"]["category"] == "READBACK_FAILED"
    assert _event_count(harness, "canonical_write") == 1
    assert _summary({"summary": backfill.summarize_chunk_manifests(plan, [final])})["canonical_ready"] is False


# ---------------------------------------------------------------------------
# Idempotency and dataset-specific reconciliation (Task 4 RED matrix 24-30)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("duplicate_source", ["incoming", "existing"])
def test_duplicate_incoming_or_existing_canonical_keys_are_dq_failed_before_upsert_write(
    duplicate_source,
):
    plan = _plan(run_id=f"goal21-task4-duplicates-{duplicate_source}")
    harness = MemoryHarness()
    _stage(harness, plan)
    chunk = plan["chunks"][0]
    if duplicate_source == "incoming":
        incoming = harness.parquet_objects[harness.manifest(plan)["staging_key"]]
        duplicate = pd.concat([incoming, incoming.iloc[[0]]], ignore_index=True)
        _replace_verified_staging(harness, plan, chunk, duplicate)
    else:
        existing = _adj_frame()
        harness.canonical_objects[("adj_factor", DATE_1)] = pd.concat(
            [existing, existing.iloc[[0]]],
            ignore_index=True,
        )
    harness.reset_events()

    _run(harness, plan, provider=False, apply=True, fetch_chunk_fn=None)

    final = harness.manifest(plan, chunk)
    assert final["state"] == "BLOCKED"
    assert final["failure"]["category"] == "DQ_FAILED"
    assert _event_count(harness, "canonical_write") == 0


def test_apply_only_revalidates_verified_staging_before_canonical_write():
    plan = _plan(run_id="goal21-task4-invalid-staging-dq")
    harness = MemoryHarness()
    _stage(harness, plan)
    chunk = plan["chunks"][0]
    invalid = harness.parquet_objects[harness.manifest(plan)["staging_key"]].copy(deep=True)
    invalid.loc[:, "adj_factor"] = 0.0
    _replace_verified_staging(harness, plan, chunk, invalid)
    harness.reset_events()

    _run(harness, plan, provider=False, apply=True, fetch_chunk_fn=None)

    final = harness.manifest(plan, chunk)
    assert final["state"] == "BLOCKED"
    assert final["failure"]["category"] == "DQ_FAILED"
    assert _event_count(harness, "canonical_write") == 0


def test_two_code_shards_upsert_same_partition_without_lost_rows():
    plan = _plan(
        run_id="goal21-task4-shared-partition",
        codes=[CODE_1, CODE_2],
        code_batch_size=1,
    )
    harness = MemoryHarness()
    _configure_adj_fetch(harness, plan)

    _run(harness, plan, provider=True, apply=True)

    canonical = harness.canonical_objects[("adj_factor", DATE_1)]
    expected = _adj_frame(codes=(CODE_1, CODE_2))
    pd.testing.assert_frame_equal(
        canonical.sort_values(KEY_COLUMNS["adj_factor"]).reset_index(drop=True),
        expected.sort_values(KEY_COLUMNS["adj_factor"]).reset_index(drop=True),
    )
    assert not canonical.duplicated(KEY_COLUMNS["adj_factor"]).any()
    assert _event_count(harness, "canonical_write") == 2
    assert all(harness.manifest(plan, chunk)["state"] == "COMPLETED" for chunk in plan["chunks"])


def test_forced_duplicate_apply_is_idempotent():
    plan = _plan(run_id="goal21-task4-forced-idempotent")
    harness = MemoryHarness()
    _complete(harness, plan)
    before = harness.canonical_objects[("adj_factor", DATE_1)].copy(deep=True)
    harness.reset_events()

    _run(
        harness,
        plan,
        provider=False,
        apply=True,
        force=True,
        fetch_chunk_fn=None,
    )

    after = harness.canonical_objects[("adj_factor", DATE_1)]
    final = harness.manifest(plan)
    record = _partition_records(final)[0]
    assert _event_count(harness, "canonical_write") == 1
    assert record["inserted_rows"] == 0
    assert record["updated_rows"] == 0
    assert record["unchanged_rows"] == len(before)
    assert not after.duplicated(KEY_COLUMNS["adj_factor"]).any()
    pd.testing.assert_frame_equal(after, before)


def test_financial_partitions_enforce_announce_date_asof_and_exact_readback():
    plan = _plan(
        run_id="goal21-task4-financial-asof",
        dataset="financial",
        start_date=DATE_1,
        end_date=DATE_2,
    )
    harness = MemoryHarness()
    frame = _financial_frame()
    harness.fetch_factory = lambda chunk: _fetch_result(
        chunk,
        frame=frame,
        canonical_trade_dates=(DATE_1, DATE_2),
    )

    _run(harness, plan, provider=True, apply=True)

    day_1 = harness.canonical_objects[("financial", DATE_1)]
    day_2 = harness.canonical_objects[("financial", DATE_2)]
    assert day_1["announce_date"].tolist() == [DATE_1]
    assert day_2["announce_date"].tolist() == [DATE_1, DATE_2]
    assert (day_1["report_period"] <= DATE_1).all()
    assert (day_2["report_period"] <= DATE_2).all()
    final = harness.manifest(plan)
    expected_keys = sorted(
        [
            build_partition("financial", DATE_1).object_key,
            build_partition("financial", DATE_2).object_key,
        ]
    )
    assert final["canonical_keys"] == expected_keys
    assert set(final["canonical_checksums"]) == set(expected_keys)
    assert all(record["exact_read_back_success"] is True for record in _partition_records(final))


def test_financial_pre_announcement_empty_partition_is_audited_without_phantom_write():
    plan = _plan(
        run_id="goal21-task4-financial-pre-announce",
        dataset="financial",
        start_date=DATE_1,
        end_date=DATE_2,
    )
    harness = MemoryHarness()
    frame = _financial_frame().iloc[[1]].copy(deep=True).reset_index(drop=True)
    harness.fetch_factory = lambda chunk: _fetch_result(
        chunk,
        frame=frame,
        canonical_trade_dates=(DATE_1, DATE_2),
    )

    _run(harness, plan, provider=True, apply=True)

    final = harness.manifest(plan)
    assert final["state"] == "COMPLETED"
    assert ("financial", DATE_1) not in harness.canonical_objects
    assert harness.canonical_objects[("financial", DATE_2)]["announce_date"].tolist() == [DATE_2]
    records = _partition_records(final)
    assert records[0]["object_key"] == build_partition("financial", DATE_1).object_key
    assert records[0]["materialized"] is False
    assert records[0]["wrote"] is False
    assert records[0]["exact_read_back_success"] is True


@pytest.mark.parametrize("stale_overlap", [False, True])
def test_proven_empty_st_history_requires_negative_scope_readback(stale_overlap):
    plan = _plan(
        run_id=f"goal21-task4-empty-st-{int(stale_overlap)}",
        dataset="st_history",
        start_date=DATE_1,
        end_date=DATE_2,
    )
    harness = MemoryHarness()
    empty = pd.DataFrame(columns=get_schema_contract("st_history").columns)
    harness.fetch_factory = lambda chunk: _fetch_result(
        chunk,
        frame=empty,
        canonical_trade_dates=(DATE_1, DATE_2),
        provider_status="VALID_EMPTY",
        valid_empty=True,
    )
    if stale_overlap:
        harness.canonical_objects[("st_history", DATE_1)] = _st_frame()

    _run(harness, plan, provider=True, apply=True)

    final = harness.manifest(plan)
    if stale_overlap:
        assert final["state"] == "FAILED"
        assert final["failure"]["category"] == "READBACK_FAILED"
    else:
        assert final["state"] == "COMPLETED"
        assert final["row_count"] == 0
    assert _event_count(harness, "canonical_write") == 0


def _legacy_scalar_completed_manifest(chunk: dict[str, Any]) -> dict[str, Any]:
    frame = _adj_frame()
    staging_checksum = backfill.dataframe_checksum(frame, key_columns=KEY_COLUMNS["adj_factor"])
    canonical_key = build_partition("adj_factor", DATE_1).object_key
    canonical_checksum = staging_checksum
    return backfill.build_chunk_manifest(
        chunk=chunk,
        state="COMPLETED",
        attempt_count=1,
        provider_status={"success": True, "provider": "fixture"},
        row_count=len(frame),
        actual_schema=list(frame.columns),
        target_schema=list(frame.columns),
        dq={"success": True, "duplicate_count": 0},
        coverage={
            "start_date": DATE_1,
            "end_date": DATE_1,
            "complete": True,
        },
        source_key="candidate/source/adj-factor.parquet",
        staging_key="candidate/staging/adj-factor.parquet",
        staging_checksum=staging_checksum,
        canonical_key=canonical_key,
        canonical_checksum=canonical_checksum,
        validation={"success": True},
        write_result={
            "success": True,
            "object_key": canonical_key,
            "checksum": canonical_checksum,
            "row_count": len(frame),
        },
        read_back_result={
            "success": True,
            "object_key": canonical_key,
            "checksum": canonical_checksum,
            "row_count": len(frame),
        },
    )


def test_multi_partition_manifest_uses_plural_keys_and_checksums_and_remains_task2_compatible():
    plan = _plan(
        run_id="goal21-task4-multi-partition",
        dataset="daily_price",
        start_date=DATE_1,
        end_date=DATE_2,
    )
    harness = MemoryHarness()
    frame = _daily_price_frame(trade_dates=(DATE_1, DATE_2))
    harness.fetch_factory = lambda chunk: _fetch_result(
        chunk,
        frame=frame,
        canonical_trade_dates=(DATE_1, DATE_2),
    )

    _run(harness, plan, provider=True, apply=True)

    final = harness.manifest(plan)
    expected_keys = sorted(
        [
            build_partition("daily_price", DATE_1).object_key,
            build_partition("daily_price", DATE_2).object_key,
        ]
    )
    assert final["canonical_keys"] == expected_keys
    assert set(final["canonical_checksums"]) == set(expected_keys)
    assert final["canonical_key"] is None
    assert final["canonical_checksum"] is None
    assert len(_partition_records(final)) == 2

    legacy_plan = _plan(run_id="goal21-task4-legacy-scalar")
    legacy = _legacy_scalar_completed_manifest(legacy_plan["chunks"][0])
    assert legacy["state"] == "COMPLETED"
    assert legacy["canonical_key"] == build_partition("adj_factor", DATE_1).object_key
    assert legacy["canonical_checksum"]


def test_completed_plural_manifest_rejects_failed_partition_readback_flag():
    plan = _plan(run_id="goal21-task4-partition-readback-flag")
    harness = MemoryHarness()
    _complete(harness, plan)
    manifest = harness.manifest(plan)
    manifest["read_back_result"]["partitions"][0]["exact_read_back_success"] = False

    with pytest.raises(backfill.BackfillPlanningError) as exc_info:
        backfill.summarize_chunk_manifests(plan, [manifest])

    assert exc_info.value.code == "INVALID_MANIFEST_EVIDENCE"


def test_completed_manifest_rejects_contradictory_scalar_and_plural_canonical_evidence():
    plan = _plan(run_id="goal21-task4-canonical-evidence-xor")
    harness = MemoryHarness()
    _complete(harness, plan)
    manifest = harness.manifest(plan)
    conflicting_key = build_partition("adj_factor", DATE_2).object_key
    conflicting_checksum = "f" * 64
    manifest["canonical_key"] = conflicting_key
    manifest["canonical_checksum"] = conflicting_checksum
    for field in ("write_result", "read_back_result"):
        manifest[field].update(
            {
                "object_key": conflicting_key,
                "checksum": conflicting_checksum,
                "row_count": manifest["row_count"],
            }
        )

    with pytest.raises(backfill.BackfillPlanningError) as exc_info:
        backfill.summarize_chunk_manifests(plan, [manifest])

    assert exc_info.value.code == "INVALID_MANIFEST_EVIDENCE"


@pytest.mark.parametrize("stage_first", [False, True])
def test_financial_canonical_dates_cannot_escape_immutable_run_scope(stage_first):
    plan = _plan(
        run_id=f"goal21-task4-financial-future-{int(stage_first)}",
        dataset="financial",
    )
    harness = MemoryHarness()
    future_date = "2099-01-01"
    if stage_first:
        harness.fetch_factory = lambda chunk: _fetch_result(
            chunk,
            frame=_financial_frame().iloc[[0]].copy(deep=True),
            canonical_trade_dates=(DATE_1,),
        )
        staged = _run(
            harness,
            plan,
            provider=True,
            apply=False,
            canonical_read_fn=None,
            canonical_write_fn=None,
        )
        assert _summary(staged)["state_counts"]["STAGED"] == 1
        manifest_key = _manifest_key(plan, plan["chunks"][0])
        persisted = harness.json_objects[manifest_key]
        persisted["coverage"]["canonical_trade_dates"] = [future_date]
        harness.reset_events()
    else:
        harness.fetch_factory = lambda chunk: _fetch_result(
            chunk,
            frame=_financial_frame().iloc[[0]].copy(deep=True),
            canonical_trade_dates=(future_date,),
        )

    _run(
        harness,
        plan,
        provider=not stage_first,
        apply=True,
        fetch_chunk_fn=None if stage_first else ...,
    )

    final = harness.manifest(plan)
    assert final["state"] == "BLOCKED"
    assert final["failure"]["category"] == "DQ_FAILED"
    assert ("financial", future_date) not in harness.canonical_objects
    assert _event_count(harness, "canonical_write") == 0


def test_financial_report_period_must_stay_inside_its_planned_window():
    plan = _plan(run_id="goal21-task4-financial-old-period", dataset="financial")
    harness = MemoryHarness()
    outside = _financial_frame().iloc[[0]].copy(deep=True)
    outside.loc[:, "report_period"] = "2023-12-31"
    harness.fetch_factory = lambda chunk: _fetch_result(chunk, frame=outside)

    _run(harness, plan, provider=True, apply=True)

    final = harness.manifest(plan)
    assert final["state"] == "BLOCKED"
    assert final["failure"]["category"] == "DQ_FAILED"
    assert _event_count(harness, "artifact_parquet_write") == 0
    assert _event_count(harness, "canonical_write") == 0


def test_nonempty_daily_staging_cannot_claim_an_extra_empty_canonical_partition():
    plan = _plan(
        run_id="goal21-task4-extra-empty-date",
        dataset="daily_price",
        end_date=DATE_2,
    )
    harness = MemoryHarness()
    harness.fetch_factory = lambda chunk: _fetch_result(
        chunk,
        frame=_daily_price_frame(trade_dates=(DATE_1,)),
        canonical_trade_dates=(DATE_1, DATE_2),
    )

    _run(harness, plan, provider=True, apply=True)

    final = harness.manifest(plan)
    assert final["state"] == "BLOCKED"
    assert final["failure"]["category"] == "DQ_FAILED"
    assert _event_count(harness, "canonical_write") == 0


def test_partial_multi_partition_write_is_durably_exposed_in_manifest_and_attempt_report():
    plan = _plan(
        run_id="goal21-task4-partial-write-evidence",
        dataset="daily_price",
        end_date=DATE_2,
    )
    harness = MemoryHarness()
    harness.fetch_factory = lambda chunk: _fetch_result(
        chunk,
        frame=_daily_price_frame(trade_dates=(DATE_1, DATE_2)),
        canonical_trade_dates=(DATE_1, DATE_2),
    )

    def fail_second_partition(dataset: str, trade_date: str, frame: pd.DataFrame) -> None:
        _ = dataset, frame
        if trade_date == DATE_2:
            raise InjectedFailure("second partition write failed", failure_category="WRITE_FAILED")

    harness.canonical_write_hook = fail_second_partition
    _run(harness, plan, provider=True, apply=True)

    final = harness.manifest(plan)
    report = harness.attempt_report(plan, plan["chunks"][0], 1)
    assert final["state"] == "FAILED"
    assert final["failure"]["category"] == "WRITE_FAILED"
    assert ("daily_price", DATE_1) in harness.canonical_objects
    assert ("daily_price", DATE_2) not in harness.canonical_objects
    expected_keys = sorted(
        [
            build_partition("daily_price", DATE_1).object_key,
            build_partition("daily_price", DATE_2).object_key,
        ]
    )
    for evidence in (final, report):
        assert evidence["canonical_keys"] == expected_keys
        assert set(evidence["canonical_checksums"]) == set(expected_keys)
        assert evidence["validation"]["success"] is False
        assert evidence["write_result"]["success"] is False
        assert evidence["write_result"]["status"] == "PARTIAL_FAILED"
        records = evidence["write_result"]["partitions"]
        assert records[0]["object_key"] == build_partition("daily_price", DATE_1).object_key
        assert records[0]["write_confirmed"] is True
        assert records[0]["exact_read_back_success"] is True
        assert records[1]["object_key"] == build_partition("daily_price", DATE_2).object_key
        assert records[1]["write_attempted"] is True
        assert records[1]["write_confirmed"] is False
        assert records[1]["exact_read_back_success"] is False


def test_canonical_validator_value_error_is_classified_as_nonretryable_dq_failure():
    plan = _plan(run_id="goal21-task4-invalid-stock-date", dataset="stock_basic")
    harness = MemoryHarness()
    harness.fetch_factory = lambda chunk: _fetch_result(
        chunk,
        frame=_stock_basic_frame(list_date="not-a-date"),
    )

    _run(harness, plan, provider=True, apply=True)

    final = harness.manifest(plan)
    assert final["state"] == "BLOCKED"
    assert final["failure"]["category"] == "DQ_FAILED"
    assert _event_count(harness, "canonical_write") == 0


def test_keyboard_interrupt_during_canonical_checksum_is_not_swallowed(monkeypatch):
    executor = importlib.import_module("stock_selector.data.historical_backfill_executor")
    plan = _plan(run_id="goal21-task4-checksum-interrupt")
    harness = MemoryHarness()
    _configure_adj_fetch(harness, plan)
    original = executor.dataframe_checksum
    calls = 0

    def interrupt_third_checksum(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 3:
            raise KeyboardInterrupt()
        return original(*args, **kwargs)

    monkeypatch.setattr(executor, "dataframe_checksum", interrupt_third_checksum)
    with pytest.raises(KeyboardInterrupt):
        _run(harness, plan, provider=True, apply=True)

    final = harness.manifest(plan)
    assert final["state"] == "INTERRUPTED"
    assert final["failure"]["category"] == "INTERRUPTED"
    assert final["failure"]["exception_type"] == "KeyboardInterrupt"


def test_root_manifest_rebuilds_from_chunk_checkpoints_after_stale_root():
    plan = _plan(
        run_id="goal21-task4-stale-root",
        codes=[CODE_1, CODE_2],
        code_batch_size=1,
    )
    harness = MemoryHarness()
    _complete(harness, plan)
    keys = _keys(plan)
    durable_manifests = {
        _manifest_key(plan, chunk): deepcopy(harness.json_objects[_manifest_key(plan, chunk)])
        for chunk in plan["chunks"]
    }
    harness.json_objects[keys["root_manifest"]] = {
        "schema_version": "stale.root.v0",
        "summary": {"state_counts": {"PENDING": 999}},
    }
    harness.reset_events()
    harness.forbid(
        "fetch",
        "artifact_parquet_read",
        "artifact_parquet_write",
        "canonical_read",
        "canonical_write",
    )

    result = _run(harness, plan, provider=False, apply=False)

    summary = _summary(result)
    root_summary = _root_summary(harness.json_objects[keys["root_manifest"]])
    assert summary["state_counts"]["COMPLETED"] == plan["chunk_count"]
    assert summary["gaps"] == []
    assert summary["completion_rate"] == 1.0
    assert summary["canonical_ready"] is True
    assert root_summary == summary
    assert all(harness.json_objects[key] == value for key, value in durable_manifests.items())
    assert all(value is False for value in result["downstream_firewalls"].values())


# Contract notes discovered while translating the brief into executable tests:
#
# 1. HistoricalChunkFetchResult exposes plural ``source_keys``, but the Task 2
#    compatibility extension list adds no manifest-level ``source_keys`` field.
#    The draft therefore requires full source lineage in attempt reports while
#    retaining the existing scalar ``source_key`` in chunk manifests.
# 2. The brief says every financial canonical trade date is materialized, yet
#    validate_dataset_frame rejects empty financial frames.  These tests choose
#    canonical dates on/after the first disclosure; an earlier proven open date
#    needs an explicit decision: skip the empty object or extend validation.
# 3. The brief requires a root manifest and a separate pure summary but does not
#    specify whether manifest.json wraps or directly extends that summary.  The
#    helper accepts both layouts while asserting the exact summary contents.
# 4. "30 RED tests" means 30 named test functions.  Required parameterizations
#    (gates, corruption, interruption, duplicate sources, and ST negative scope)
#    intentionally produce more than 30 collected pytest cases.
# 5. Artifact collision is required to be non-overwriting, but the brief does
#    not assign it a stable failure taxonomy/state.  The collision test therefore
#    accepts FAILED or BLOCKED while requiring durable non-success evidence.

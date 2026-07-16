"""RED regressions for the Goal 21 executor review findings.

These tests intentionally exercise only in-memory artifact and canonical
boundaries.  They specify durable audit/recovery semantics without requiring a
provider token, MinIO, or a network connection.
"""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Callable

import pandas as pd
import pytest

from stock_selector.data import historical_backfill as backfill
from stock_selector.providers.historical_provider import HistoricalChunkFetchResult
from stock_selector.providers.schema_contract import get_schema_contract
from stock_selector.storage.partition import build_partition


DATE_1 = "2024-01-02"
DATE_2 = "2024-01-03"
SEED_DATE = "2023-12-29"
CODE_1 = "000001.SZ"
CODE_2 = "600519.SH"
FIXED_NOW = "2026-07-12T00:00:00Z"


class InjectedFailure(RuntimeError):
    """A classified storage-boundary failure used by the crash-window test."""

    def __init__(self, message: str, *, failure_category: str) -> None:
        super().__init__(message)
        self.failure_category = failure_category


@dataclass
class MemoryHarness:
    """Small storage harness that records every observable side effect."""

    json_objects: dict[str, dict[str, Any]] = field(default_factory=dict)
    parquet_objects: dict[str, pd.DataFrame] = field(default_factory=dict)
    canonical_objects: dict[tuple[str, str], pd.DataFrame] = field(default_factory=dict)
    events: list[tuple[Any, ...]] = field(default_factory=list)
    fetch_factory: Callable[[dict[str, Any]], HistoricalChunkFetchResult] | None = None
    json_write_hook: Callable[[str, dict[str, Any]], None] | None = None

    def reset_events(self) -> None:
        self.events.clear()

    def artifact_read_json(self, key: str) -> dict[str, Any]:
        self.events.append(("artifact_json_read", key))
        if key not in self.json_objects:
            raise FileNotFoundError(key)
        return deepcopy(self.json_objects[key])

    def artifact_write_json(self, key: str, payload: dict[str, Any]) -> str:
        snapshot = deepcopy(payload)
        kind = "artifact_json_write"
        if "/chunk_id=" in key and key.endswith("/manifest.json"):
            kind = "chunk_manifest_write"
        elif "/attempt=" in key and key.endswith("/report.json"):
            kind = "attempt_report_write"
        self.events.append((kind, key, snapshot.get("state")))
        if self.json_write_hook is not None:
            self.json_write_hook(key, snapshot)
        self.json_objects[key] = snapshot
        return f"memory-json://{key}"

    def artifact_read_parquet(self, key: str) -> pd.DataFrame:
        self.events.append(("artifact_parquet_read", key))
        if key not in self.parquet_objects:
            raise FileNotFoundError(key)
        return self.parquet_objects[key].copy(deep=True)

    def artifact_write_parquet(self, key: str, frame: pd.DataFrame) -> str:
        snapshot = frame.copy(deep=True)
        self.events.append(("artifact_parquet_write", key, len(snapshot)))
        self.parquet_objects[key] = snapshot
        return f"memory-parquet://{key}"

    def fetch_chunk(self, chunk: dict[str, Any]) -> HistoricalChunkFetchResult:
        self.events.append(("fetch", chunk["chunk_id"]))
        if self.fetch_factory is None:
            raise AssertionError("fetch_factory was not configured")
        return self.fetch_factory(deepcopy(chunk))

    def canonical_read(self, dataset: str, trade_date: str) -> pd.DataFrame | None:
        self.events.append(("canonical_read", dataset, trade_date))
        frame = self.canonical_objects.get((dataset, trade_date))
        return None if frame is None else frame.copy(deep=True)

    def canonical_write(self, dataset: str, trade_date: str, frame: pd.DataFrame) -> str:
        snapshot = frame.copy(deep=True)
        self.events.append(("canonical_write", dataset, trade_date, len(snapshot)))
        self.canonical_objects[(dataset, trade_date)] = snapshot
        return f"memory-canonical://{dataset}/{trade_date}"

    def manifest(self, plan: dict[str, Any]) -> dict[str, Any]:
        chunk = plan["chunks"][0]
        key = _keys(plan)["chunks"][chunk["chunk_id"]]["manifest"]
        return deepcopy(self.json_objects[key])

    def attempt_report(self, plan: dict[str, Any], attempt: int = 1) -> dict[str, Any]:
        chunk = plan["chunks"][0]
        template = _keys(plan)["chunks"][chunk["chunk_id"]]["attempt_report_template"]
        return deepcopy(self.json_objects[template.format(attempt=attempt)])


def _plan(
    *,
    run_id: str,
    dataset: str = "adj_factor",
    codes: list[str] | None = None,
) -> dict[str, Any]:
    return backfill.build_history_backfill_plan(
        run_id=run_id,
        start_date=DATE_1,
        end_date=DATE_1,
        codes=list(codes or [CODE_1]),
        code_batch_size=10,
        date_batch_days=31,
        report_period_months=3,
        datasets=[dataset],
        generated_at_fn=lambda: FIXED_NOW,
    )


def _v2_financial_plan(*, run_id: str, with_seed_manifest: bool = True) -> dict[str, Any]:
    return backfill.build_history_backfill_plan_v2(
        run_id=run_id,
        start_date=DATE_1,
        end_date=DATE_2,
        codes=[CODE_1],
        code_batch_size=250,
        date_batch_days=31,
        announce_date_batch_days=1,
        datasets=["financial"],
        financial_seed_manifest=(
            _financial_seed_manifest() if with_seed_manifest else None
        ),
        generated_at_fn=lambda: FIXED_NOW,
    )


def _keys(plan: dict[str, Any]) -> dict[str, Any]:
    return backfill.build_history_backfill_output_keys(plan["run_id"], plan["chunks"])


def _attempt_report_key(plan: dict[str, Any], attempt: int) -> str:
    chunk = plan["chunks"][0]
    template = _keys(plan)["chunks"][chunk["chunk_id"]]["attempt_report_template"]
    return template.format(attempt=attempt)


def _run(
    harness: MemoryHarness,
    plan: dict[str, Any],
    *,
    provider: bool,
    apply: bool,
) -> dict[str, Any]:
    return backfill.run_real_history_backfill(
        plan=deepcopy(plan),
        artifact_read_json_fn=harness.artifact_read_json,
        artifact_write_json_fn=harness.artifact_write_json,
        artifact_read_parquet_fn=harness.artifact_read_parquet,
        artifact_write_parquet_fn=harness.artifact_write_parquet,
        fetch_chunk_fn=harness.fetch_chunk if provider else None,
        canonical_read_fn=harness.canonical_read if apply else None,
        canonical_write_fn=harness.canonical_write if apply else None,
        provider_call_enabled=provider,
        apply_standard_write=apply,
        resume=True,
        force=False,
        generated_at_fn=lambda: FIXED_NOW,
    )


def _coverage(
    chunk: dict[str, Any],
    *,
    axis: str = "stock_code_x_open_trade_date",
    complete: bool,
    valid_empty: bool,
    covered_count: int,
) -> dict[str, Any]:
    return {
        "axis": axis,
        "complete": complete,
        "expected_count": covered_count if complete else 1,
        "covered_count": covered_count,
        "missing": [] if complete else [{"stock_code": CODE_1, "trade_date": DATE_1}],
        "requested_codes": list(chunk.get("codes", [])),
        "requested_indexes": list(chunk.get("index_codes", [])),
        "requested_start_date": chunk["start_date"],
        "requested_end_date": chunk["end_date"],
        "canonical_trade_dates": [DATE_1],
        "valid_empty": valid_empty,
    }


def _provider_call(*, status: str, row_count: int) -> dict[str, Any]:
    return {
        "endpoint": "adj_factor",
        "strategy": "by_code_range",
        "parameters": {
            "ts_code": CODE_1,
            "start_date": "20240102",
            "end_date": "20240102",
        },
        "row_count": row_count,
        "status": status,
    }


def _adj_result(
    chunk: dict[str, Any],
    *,
    source_keys: tuple[str, ...] = (
        "raw/provider/tushare/adj_factor/response-a.parquet",
    ),
) -> HistoricalChunkFetchResult:
    frame = pd.DataFrame(
        [{"stock_code": CODE_1, "trade_date": DATE_1, "adj_factor": 1.25}],
        columns=get_schema_contract("adj_factor").columns,
    )
    return HistoricalChunkFetchResult(
        dataset="adj_factor",
        chunk_id=chunk["chunk_id"],
        frame=frame,
        provider_status="FETCHED",
        provider_name="fixture",
        source_keys=source_keys,
        source_semantics="HISTORICAL_RANGE_SOURCE",
        provider_calls=(_provider_call(status="FETCHED", row_count=1),),
        actual_schema=("ts_code", "trade_date", "adj_factor"),
        target_schema=tuple(get_schema_contract("adj_factor").columns),
        dq={"passed": True, "level": "STRICT", "blocked_reasons": []},
        coverage=_coverage(chunk, complete=True, valid_empty=False, covered_count=1),
        canonical_trade_dates=(DATE_1,),
        partition_strategy="BY_TRADE_DATE_COLUMN",
        valid_empty=False,
        validation={"passed": True, "errors": []},
        failure=None,
    )


def _failed_result(chunk: dict[str, Any], status: str) -> HistoricalChunkFetchResult:
    failure = {
        "category": "RATE_LIMITED" if status == "FAILED" else "PERMISSION_DENIED",
        "retryable": status == "FAILED",
        "exception_type": "ProviderRateLimit" if status == "FAILED" else "ProviderPermissionDenied",
        "message": "provider supplied original canonical failure",
    }
    return HistoricalChunkFetchResult(
        dataset="adj_factor",
        chunk_id=chunk["chunk_id"],
        frame=pd.DataFrame(columns=get_schema_contract("adj_factor").columns),
        provider_status=status,
        provider_name="tushare",
        source_keys=(
            "raw/provider/tushare/adj_factor/request.json",
            "raw/provider/tushare/adj_factor/error.json",
        ),
        source_semantics="HISTORICAL_RANGE_SOURCE",
        provider_calls=(_provider_call(status="FAILED", row_count=0),),
        actual_schema=("ts_code", "trade_date"),
        target_schema=tuple(get_schema_contract("adj_factor").columns),
        dq={
            "passed": False,
            "level": "BLOCKED",
            "blocked_reasons": ["provider supplied original canonical failure"],
        },
        coverage=_coverage(chunk, complete=False, valid_empty=False, covered_count=0),
        canonical_trade_dates=(DATE_1,),
        partition_strategy="BY_TRADE_DATE_COLUMN",
        valid_empty=False,
        validation={
            "passed": False,
            "errors": ["provider supplied original canonical failure"],
        },
        failure=failure,
    )


def _empty_result(
    chunk: dict[str, Any],
    *,
    dataset: str,
    frame: pd.DataFrame | None = None,
) -> HistoricalChunkFetchResult:
    empty = (
        frame.copy(deep=True)
        if frame is not None
        else pd.DataFrame(columns=get_schema_contract(dataset).columns)
    )
    semantics = {
        "stock_basic": "POINT_IN_TIME_HISTORICAL_SNAPSHOT",
        "st_history": "HISTORICAL_INTERVAL_SOURCE",
    }[dataset]
    strategy = "ST_INTERVAL_HISTORY" if dataset == "st_history" else "BY_TRADE_DATE_COLUMN"
    axis = "stock_code_x_interval" if dataset == "st_history" else "stock_code_x_open_trade_date"
    return HistoricalChunkFetchResult(
        dataset=dataset,
        chunk_id=chunk["chunk_id"],
        frame=empty,
        provider_status="VALID_EMPTY",
        provider_name="fixture",
        source_keys=(f"archive/{dataset}/proven-empty.parquet",),
        source_semantics=semantics,
        provider_calls=(),
        actual_schema=tuple(empty.columns),
        target_schema=tuple(get_schema_contract(dataset).columns),
        dq={"passed": True, "level": "STRICT", "blocked_reasons": []},
        coverage=_coverage(
            chunk,
            axis=axis,
            complete=True,
            valid_empty=True,
            covered_count=0,
        ),
        canonical_trade_dates=(DATE_1,),
        partition_strategy=strategy,
        valid_empty=True,
        validation={"passed": True, "errors": []},
        failure=None,
    )


def _st_result(chunk: dict[str, Any], frame: pd.DataFrame) -> HistoricalChunkFetchResult:
    return HistoricalChunkFetchResult(
        dataset="st_history",
        chunk_id=chunk["chunk_id"],
        frame=frame,
        provider_status="FETCHED",
        provider_name="fixture",
        source_keys=("archive/st_history/intervals.parquet",),
        source_semantics="HISTORICAL_INTERVAL_SOURCE",
        provider_calls=(),
        actual_schema=tuple(frame.columns),
        target_schema=tuple(get_schema_contract("st_history").columns),
        dq={"passed": True, "level": "STRICT", "blocked_reasons": []},
        coverage=_coverage(
            chunk,
            axis="stock_code_x_interval",
            complete=True,
            valid_empty=False,
            covered_count=len(frame),
        ),
        canonical_trade_dates=(DATE_1,),
        partition_strategy="ST_INTERVAL_HISTORY",
        valid_empty=False,
        validation={"passed": True, "errors": []},
        failure=None,
    )


def _stock_result(
    chunk: dict[str, Any],
    frame: pd.DataFrame,
) -> HistoricalChunkFetchResult:
    return HistoricalChunkFetchResult(
        dataset="stock_basic",
        chunk_id=chunk["chunk_id"],
        frame=frame,
        provider_status="FETCHED",
        provider_name="fixture",
        source_keys=("archive/stock_basic/historical-snapshot.parquet",),
        source_semantics="POINT_IN_TIME_HISTORICAL_SNAPSHOT",
        provider_calls=(),
        actual_schema=tuple(frame.columns),
        target_schema=tuple(get_schema_contract("stock_basic").columns),
        dq={"passed": True, "level": "STRICT", "blocked_reasons": []},
        coverage=_coverage(
            chunk,
            axis="stock_code_x_open_trade_date",
            complete=True,
            valid_empty=False,
            covered_count=len(frame),
        ),
        canonical_trade_dates=(DATE_1,),
        partition_strategy="BY_TRADE_DATE_COLUMN",
        valid_empty=False,
        validation={"passed": True, "errors": []},
        failure=None,
    )


def _daily_price_frame(code: str, *, is_paused: bool = False) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "stock_code": code,
                "trade_date": DATE_1,
                "open": 10.0,
                "high": 10.5,
                "low": 9.8,
                "close": 10.2,
                "pre_close": 10.0,
                "volume": 1000.0,
                "amount": 10_200.0,
                "pct_chg": 2.0,
                "is_paused": is_paused,
                "limit_up": 11.0,
                "limit_down": 9.0,
            }
        ],
        columns=get_schema_contract("daily_price").columns,
    )


def _daily_price_result(
    chunk: dict[str, Any],
    frame: pd.DataFrame,
) -> HistoricalChunkFetchResult:
    coverage = _coverage(
        chunk,
        complete=True,
        valid_empty=False,
        covered_count=len(frame),
    )
    coverage.update(
        expected_count=len(frame),
        paused_codes_proven_absent=[CODE_2],
        suspension_full_market_event_set=True,
    )
    return HistoricalChunkFetchResult(
        dataset="daily_price",
        chunk_id=chunk["chunk_id"],
        frame=frame,
        provider_status="FETCHED",
        provider_name="fixture",
        source_keys=(
            "raw/provider/tushare/daily.parquet",
            "raw/provider/tushare/suspend_d.parquet",
        ),
        source_semantics="HISTORICAL_DAILY_PRICE_SOURCE",
        provider_calls=(),
        actual_schema=tuple(frame.columns),
        target_schema=tuple(get_schema_contract("daily_price").columns),
        dq={"passed": True, "level": "STRICT", "blocked_reasons": []},
        coverage=coverage,
        canonical_trade_dates=(DATE_1,),
        partition_strategy="BY_TRADE_DATE_COLUMN",
        valid_empty=False,
        validation={"passed": True, "errors": []},
        failure=None,
    )


def _financial_result(
    chunk: dict[str, Any],
    *,
    announce_date: str,
    revenue_yoy: float,
    canonical_dates: tuple[str, ...] | None = None,
    predecessor_trade_date: str = SEED_DATE,
) -> HistoricalChunkFetchResult:
    frame = pd.DataFrame(
        [
            {
                "stock_code": CODE_1,
                "report_period": "2023-12-31",
                "announce_date": announce_date,
                "revenue_yoy": revenue_yoy,
                "net_profit_yoy": 8.0,
                "roe": 0.12,
                "gross_margin": 0.30,
                "debt_ratio": 0.40,
                "operating_cashflow": 1_000_000.0,
            }
        ],
        columns=get_schema_contract("financial").columns,
    )
    dates = (announce_date,) if canonical_dates is None else canonical_dates
    return HistoricalChunkFetchResult(
        dataset="financial",
        chunk_id=chunk["chunk_id"],
        frame=frame,
        provider_status="FETCHED",
        provider_name="fixture",
        source_keys=(f"archive/financial/{announce_date}.parquet",),
        source_semantics="TRUSTED_STANDARD_FINANCIAL_SOURCE",
        provider_calls=(),
        actual_schema=tuple(frame.columns),
        target_schema=tuple(get_schema_contract("financial").columns),
        dq={"passed": True, "level": "STRICT", "blocked_reasons": []},
        coverage={
            "axis": "by_code_announce_date_window",
            "complete": True,
            "expected_count": len(frame),
            "covered_count": len(frame),
            "missing": [],
            "requested_codes": [CODE_1],
            "requested_indexes": [],
            "requested_start_date": chunk["start_date"],
            "requested_end_date": chunk["end_date"],
            "canonical_trade_dates": list(dates),
            "predecessor_trade_date": predecessor_trade_date,
            "valid_empty": False,
        },
        canonical_trade_dates=dates,
        partition_strategy="FINANCIAL_ANNOUNCE_DATE_AS_OF",
        valid_empty=False,
        validation={"passed": True, "errors": []},
        failure=None,
    )


def _empty_financial_result(
    chunk: dict[str, Any],
    *,
    canonical_dates: tuple[str, ...] = (),
    predecessor_trade_date: str,
) -> HistoricalChunkFetchResult:
    frame = pd.DataFrame(columns=get_schema_contract("financial").columns)
    return HistoricalChunkFetchResult(
        dataset="financial",
        chunk_id=chunk["chunk_id"],
        frame=frame,
        provider_status="VALID_EMPTY",
        provider_name="fixture",
        source_keys=(f"archive/financial/empty-{chunk['start_date']}.parquet",),
        source_semantics="TRUSTED_STANDARD_FINANCIAL_SOURCE",
        provider_calls=(),
        actual_schema=tuple(frame.columns),
        target_schema=tuple(frame.columns),
        dq={"passed": True, "level": "STRICT", "blocked_reasons": []},
        coverage={
            "axis": "by_code_announce_date_window",
            "complete": True,
            "expected_count": 0,
            "covered_count": 0,
            "missing": [],
            "requested_codes": [CODE_1],
            "requested_indexes": [],
            "requested_start_date": chunk["start_date"],
            "requested_end_date": chunk["end_date"],
            "canonical_trade_dates": list(canonical_dates),
            "predecessor_trade_date": predecessor_trade_date,
            "financial_announce_window_empty": True,
            "valid_empty": True,
        },
        canonical_trade_dates=canonical_dates,
        partition_strategy="FINANCIAL_ANNOUNCE_DATE_AS_OF",
        valid_empty=True,
        validation={"passed": True, "errors": []},
        failure=None,
    )


def _financial_seed_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "stock_code": CODE_1,
                "report_period": "2023-09-30",
                "announce_date": SEED_DATE,
                "revenue_yoy": 9.0,
                "net_profit_yoy": 7.0,
                "roe": 0.11,
                "gross_margin": 0.29,
                "debt_ratio": 0.39,
                "operating_cashflow": 900_000.0,
            }
        ],
        columns=get_schema_contract("financial").columns,
    )


def _financial_seed_manifest(
    *,
    frame: pd.DataFrame | None = None,
    trade_date: str = SEED_DATE,
) -> dict[str, Any]:
    seed = _financial_seed_frame() if frame is None else frame.copy(deep=True)
    seed_plan = backfill.build_history_backfill_plan_v2(
        run_id=f"goal21-review-financial-seed-{trade_date}",
        start_date=trade_date,
        end_date=trade_date,
        codes=[CODE_1],
        code_batch_size=250,
        date_batch_days=31,
        announce_date_batch_days=31,
        datasets=["financial"],
        generated_at_fn=lambda: FIXED_NOW,
    )
    chunk = seed_plan["chunks"][0]
    checksum = backfill.dataframe_checksum(
        seed,
        key_columns=backfill.KEY_COLUMNS["financial"],
    )
    object_key = build_partition("financial", trade_date).object_key
    record = {
        "object_key": object_key,
        "checksum": checksum,
        "row_count": len(seed),
        "inserted_rows": len(seed),
        "updated_rows": 0,
        "unchanged_rows": 0,
        "write_attempted": True,
        "write_confirmed": True,
        "wrote": True,
        "materialized": True,
        "exact_read_back_success": True,
    }
    return backfill.build_chunk_manifest(
        chunk=chunk,
        state="COMPLETED",
        attempt_count=1,
        plan_fingerprint=seed_plan["plan_fingerprint"],
        requested_stages=["provider", "apply"],
        provider_status="FETCHED",
        row_count=len(seed),
        actual_schema=list(seed.columns),
        target_schema=list(seed.columns),
        dq={"passed": True, "level": "STRICT", "blocked_reasons": []},
        coverage={
            "complete": True,
            "canonical_trade_dates": [trade_date],
            "requested_codes": [CODE_1],
        },
        source_key="archive/financial/verified-seed-source.parquet",
        staging_key="candidate/financial/verified-seed-staging.parquet",
        staging_checksum=checksum,
        staging_attempt=1,
        canonical_key=None,
        canonical_checksum=None,
        canonical_keys=[object_key],
        canonical_checksums={object_key: checksum},
        validation={
            "success": True,
            "passed": True,
            "status": "PASSED",
            "financial_reducer": {
                "dependency_keys": list(chunk["dependency_keys"]),
                "prior_state_checksum": "2" * 64,
                "seed": {
                    "mode": "COMPLETED_PREDECESSOR_MANIFEST",
                    "trade_date": "2023-09-29",
                    "object_key": build_partition(
                        "financial",
                        "2023-09-29",
                    ).object_key,
                    "checksum": "3" * 64,
                    "row_count": 0,
                    "coverage_codes": [CODE_1],
                    "source_key": "archive/financial/root-seed-source.parquet",
                    "source_manifest_fingerprint": "4" * 64,
                    "exact_read_back_success": True,
                },
                "terminal": {
                    "required": True,
                    "last_materialized_trade_date": trade_date,
                    "state_checksum": checksum,
                    "anchor": {
                        "trade_date": trade_date,
                        "object_key": object_key,
                        "checksum": checksum,
                        "row_count": len(seed),
                        "exact_read_back_success": True,
                    },
                    "pending_row_count": 0,
                    "pending_announce_date_min": None,
                    "pending_announce_date_max": None,
                    "passed": True,
                },
            },
        },
        write_result={"success": True, "status": "WRITTEN", "partitions": [record]},
        read_back_result={"success": True, "status": "VERIFIED", "partitions": [record]},
    )


@pytest.mark.parametrize(
    "defect",
    ["v1_manifest", "missing_reducer", "nonfinal_terminal", "foreign_universe"],
)
def test_v2_financial_plan_rejects_seed_without_final_same_universe_reducer_proof(
    defect: str,
):
    manifest = _financial_seed_manifest()
    if defect == "v1_manifest":
        v1_plan = backfill.build_history_backfill_plan(
            run_id="goal21-review-financial-v1-seed",
            start_date=SEED_DATE,
            end_date=SEED_DATE,
            codes=[CODE_1],
            code_batch_size=250,
            date_batch_days=31,
            report_period_months=3,
            datasets=["financial"],
            generated_at_fn=lambda: FIXED_NOW,
        )
        manifest["chunk"] = deepcopy(v1_plan["chunks"][0])
        manifest["chunk_id"] = v1_plan["chunks"][0]["chunk_id"]
        manifest["plan_fingerprint"] = v1_plan["plan_fingerprint"]
    elif defect == "missing_reducer":
        manifest["validation"].pop("financial_reducer")
    elif defect == "nonfinal_terminal":
        manifest["validation"]["financial_reducer"]["terminal"]["required"] = False
    else:
        manifest["chunk"]["universe_id"] = "0" * 64

    with pytest.raises(backfill.BackfillPlanningError) as exc_info:
        backfill.build_history_backfill_plan_v2(
            run_id=f"goal21-review-financial-reject-seed-{defect}",
            start_date=DATE_1,
            end_date=DATE_2,
            codes=[CODE_1],
            code_batch_size=250,
            date_batch_days=31,
            announce_date_batch_days=1,
            datasets=["financial"],
            financial_seed_manifest=manifest,
            generated_at_fn=lambda: FIXED_NOW,
        )

    assert exc_info.value.code == "INVALID_FINANCIAL_SEED"


@pytest.mark.parametrize(
    ("location", "source_key"),
    [
        ("manifest", "smoke/tushare/financial/part.parquet"),
        ("manifest", "candidate/financial/unverified-source.parquet"),
        ("reducer", "smoke/tushare/financial/root-seed.parquet"),
        ("reducer", "../outside/financial/root-seed.parquet"),
    ],
)
def test_v2_financial_plan_rejects_smoke_candidate_or_unsafe_seed_lineage(
    location: str,
    source_key: str,
):
    manifest = _financial_seed_manifest()
    if location == "manifest":
        manifest["source_key"] = source_key
    else:
        manifest["validation"]["financial_reducer"]["seed"]["source_key"] = source_key

    with pytest.raises(backfill.BackfillPlanningError) as exc_info:
        backfill.build_history_backfill_plan_v2(
            run_id=(
                "goal21-review-financial-reject-lineage-"
                f"{location}-{source_key.split('/')[0].replace('.', 'dot')}"
            ),
            start_date=DATE_1,
            end_date=DATE_2,
            codes=[CODE_1],
            code_batch_size=250,
            date_batch_days=31,
            announce_date_batch_days=1,
            datasets=["financial"],
            financial_seed_manifest=manifest,
            generated_at_fn=lambda: FIXED_NOW,
        )

    assert exc_info.value.code == "INVALID_FINANCIAL_SEED"


def _event_count(harness: MemoryHarness, kind: str) -> int:
    return sum(event[0] == kind for event in harness.events)


def test_v2_financial_materialization_carries_prior_announcement_state_across_windows():
    plan = _v2_financial_plan(run_id="goal21-review-financial-carry")
    harness = MemoryHarness()
    harness.canonical_objects[("financial", SEED_DATE)] = _financial_seed_frame()
    results = {
        DATE_1: (10.0, DATE_1),
        DATE_2: (11.0, DATE_2),
    }
    harness.fetch_factory = lambda chunk: _financial_result(
        chunk,
        announce_date=results[chunk["start_date"]][1],
        revenue_yoy=results[chunk["start_date"]][0],
    )

    result = _run(harness, plan, provider=True, apply=True)

    assert result["summary"]["canonical_ready"] is True
    assert harness.canonical_objects[("financial", DATE_1)]["announce_date"].tolist() == [
        SEED_DATE,
        DATE_1,
    ]
    assert harness.canonical_objects[("financial", DATE_2)]["announce_date"].tolist() == [
        SEED_DATE,
        DATE_1,
        DATE_2,
    ]
    partition_reads = [
        event
        for event in harness.events
        if event[:2] == ("canonical_read", "financial") and event[2] in {DATE_1, DATE_2}
    ]
    assert len(partition_reads) == 4
    assert _event_count(harness, "canonical_write") == 2


def test_v2_financial_completed_resume_detects_and_repairs_missing_carried_state():
    plan = _v2_financial_plan(run_id="goal21-review-financial-carry-resume")
    harness = MemoryHarness()
    harness.canonical_objects[("financial", SEED_DATE)] = _financial_seed_frame()
    harness.fetch_factory = lambda chunk: _financial_result(
        chunk,
        announce_date=chunk["start_date"],
        revenue_yoy=10.0 if chunk["start_date"] == DATE_1 else 11.0,
    )
    _run(harness, plan, provider=True, apply=True)
    day_2 = harness.canonical_objects[("financial", DATE_2)]
    harness.canonical_objects[("financial", DATE_2)] = day_2[
        day_2["announce_date"] == DATE_2
    ].copy(deep=True).reset_index(drop=True)
    harness.reset_events()

    result = _run(harness, plan, provider=False, apply=True)

    assert plan["chunks"][0]["chunk_id"] in result["skipped_chunk_ids"]
    assert plan["chunks"][1]["chunk_id"] not in result["skipped_chunk_ids"]
    assert harness.canonical_objects[("financial", DATE_2)]["announce_date"].tolist() == [
        SEED_DATE,
        DATE_1,
        DATE_2,
    ]
    assert _event_count(harness, "fetch") == 0
    assert _event_count(harness, "canonical_write") == 1


def test_v2_financial_apply_without_verified_prior_seed_is_blocked_without_canonical_write():
    plan = _v2_financial_plan(
        run_id="goal21-review-financial-missing-seed",
        with_seed_manifest=False,
    )
    harness = MemoryHarness()
    harness.fetch_factory = lambda chunk: _financial_result(
        chunk,
        announce_date=chunk["start_date"],
        revenue_yoy=10.0,
    )

    result = _run(harness, plan, provider=True, apply=True)

    assert result["summary"]["canonical_ready"] is False
    assert result["summary"]["state_counts"]["BLOCKED"] == 2
    assert all(
        gap["category"] == "SEMANTIC_SOURCE_UNAVAILABLE"
        for gap in result["summary"]["gaps"]
    )
    assert _event_count(harness, "canonical_write") == 0
    canonical_reads = _event_count(harness, "canonical_read")
    assert canonical_reads == 0
    assert canonical_reads <= plan["preflight_estimate"][
        "financial_canonical_read_count_upper_bound"
    ]


def test_v2_financial_existing_canonical_without_completed_seed_manifest_is_not_trusted():
    plan = _v2_financial_plan(
        run_id="goal21-review-financial-unproven-seed",
        with_seed_manifest=False,
    )
    harness = MemoryHarness()
    harness.canonical_objects[("financial", SEED_DATE)] = _financial_seed_frame()
    harness.fetch_factory = lambda chunk: _financial_result(
        chunk,
        announce_date=chunk["start_date"],
        revenue_yoy=10.0,
    )

    result = _run(harness, plan, provider=True, apply=True)

    assert result["summary"]["canonical_ready"] is False
    assert result["summary"]["state_counts"]["BLOCKED"] == 2
    assert all(
        gap["category"] == "SEMANTIC_SOURCE_UNAVAILABLE"
        for gap in result["summary"]["gaps"]
    )
    assert _event_count(harness, "canonical_write") == 0


@pytest.mark.parametrize("defect", ["future_report_period", "non_finite_numeric"])
def test_v2_financial_seed_reuses_full_point_in_time_dq_and_blocks_pollution(defect: str):
    seed = _financial_seed_frame().copy(deep=True)
    if defect == "future_report_period":
        seed.loc[0, "report_period"] = "2024-03-31"
    else:
        seed.loc[0, "revenue_yoy"] = float("nan")
    plan = backfill.build_history_backfill_plan_v2(
        run_id=f"goal21-review-financial-invalid-seed-{defect}",
        start_date=DATE_1,
        end_date=DATE_2,
        codes=[CODE_1],
        code_batch_size=250,
        date_batch_days=31,
        announce_date_batch_days=1,
        datasets=["financial"],
        financial_seed_manifest=_financial_seed_manifest(frame=seed),
        generated_at_fn=lambda: FIXED_NOW,
    )
    harness = MemoryHarness()
    harness.canonical_objects[("financial", SEED_DATE)] = seed
    harness.fetch_factory = lambda chunk: _financial_result(
        chunk,
        announce_date=chunk["start_date"],
        revenue_yoy=10.0,
    )

    result = _run(harness, plan, provider=True, apply=True)

    assert result["summary"]["canonical_ready"] is False
    assert result["summary"]["state_counts"]["BLOCKED"] == 2
    assert _event_count(harness, "canonical_write") == 0


def test_v2_financial_closed_day_sources_carry_into_next_open_date_on_apply_only_resume():
    friday = "2024-01-05"
    saturday = "2024-01-06"
    sunday = "2024-01-07"
    monday = "2024-01-08"
    seed = _financial_seed_frame().copy(deep=True)
    seed["announce_date"] = friday
    plan = backfill.build_history_backfill_plan_v2(
        run_id="goal21-review-financial-weekend-carry",
        start_date=saturday,
        end_date=monday,
        codes=[CODE_1],
        code_batch_size=250,
        date_batch_days=31,
        announce_date_batch_days=1,
        datasets=["financial"],
        financial_seed_manifest=_financial_seed_manifest(
            frame=seed,
            trade_date=friday,
        ),
        generated_at_fn=lambda: FIXED_NOW,
    )
    harness = MemoryHarness()
    harness.canonical_objects[("financial", friday)] = seed
    harness.fetch_factory = lambda chunk: _financial_result(
        chunk,
        announce_date=chunk["start_date"],
        revenue_yoy={saturday: 10.0, sunday: 11.0, monday: 12.0}[chunk["start_date"]],
        canonical_dates=(monday,) if chunk["start_date"] == monday else (),
        predecessor_trade_date=friday,
    )

    staged = _run(harness, plan, provider=True, apply=False)
    assert staged["summary"]["state_counts"]["STAGED"] == 3
    assert _event_count(harness, "canonical_write") == 0
    harness.reset_events()

    completed = _run(harness, plan, provider=False, apply=True)

    assert completed["summary"]["canonical_ready"] is True
    assert completed["summary"]["state_counts"]["COMPLETED"] == 3
    assert harness.canonical_objects[("financial", monday)]["announce_date"].tolist() == [
        friday,
        saturday,
        sunday,
        monday,
    ]
    assert _event_count(harness, "fetch") == 0
    assert _event_count(harness, "canonical_write") == 1


def test_v2_financial_ready_hard_crash_recovers_carry_without_refetch_or_duplicate_write():
    plan = _v2_financial_plan(run_id="goal21-review-financial-ready-hard-crash")
    harness = MemoryHarness()
    harness.canonical_objects[("financial", SEED_DATE)] = _financial_seed_frame()
    harness.fetch_factory = lambda chunk: _financial_result(
        chunk,
        announce_date=chunk["start_date"],
        revenue_yoy=10.0 if chunk["start_date"] == DATE_1 else 11.0,
    )
    _run(harness, plan, provider=True, apply=False)
    first_chunk_id = plan["chunks"][0]["chunk_id"]

    def hard_crash_after_first_ready(key: str, payload: dict[str, Any]) -> None:
        if (
            f"chunk_id={first_chunk_id}/manifest.json" in key
            and payload.get("state") == "COMPLETED"
        ):
            raise SystemExit(91)

    harness.json_write_hook = hard_crash_after_first_ready
    with pytest.raises(SystemExit, match="91"):
        _run(harness, plan, provider=False, apply=True)
    assert harness.manifest(plan)["state"] == "RUNNING"
    assert harness.attempt_report(plan, attempt=2)["state"] == "READY_TO_CHECKPOINT"
    first_before = harness.canonical_objects[("financial", DATE_1)].copy(deep=True)

    harness.json_write_hook = None
    harness.reset_events()
    result = _run(harness, plan, provider=False, apply=True)

    assert result["summary"]["canonical_ready"] is True
    assert result["summary"]["state_counts"]["COMPLETED"] == 2
    assert first_chunk_id in result["reconciled_chunk_ids"]
    assert _event_count(harness, "fetch") == 0
    assert sum(
        event[:3] == ("canonical_write", "financial", DATE_1)
        for event in harness.events
    ) == 0
    assert sum(
        event[:3] == ("canonical_read", "financial", DATE_1)
        for event in harness.events
    ) == 1
    pd.testing.assert_frame_equal(
        harness.canonical_objects[("financial", DATE_1)],
        first_before,
    )
    assert harness.canonical_objects[("financial", DATE_2)]["announce_date"].tolist() == [
        SEED_DATE,
        DATE_1,
        DATE_2,
    ]


@pytest.mark.parametrize(
    "defect",
    [
        "dependency_keys",
        "prior_state_checksum",
        "seed_trade_date",
        "seed_object_key",
        "seed_checksum",
        "terminal_cursor",
    ],
)
def test_v2_financial_ready_recovery_rejects_tampered_reducer_evidence(defect: str):
    plan = _v2_financial_plan(run_id=f"goal21-review-financial-ready-tamper-{defect}")
    harness = MemoryHarness()
    harness.canonical_objects[("financial", SEED_DATE)] = _financial_seed_frame()
    harness.fetch_factory = lambda chunk: _financial_result(
        chunk,
        announce_date=chunk["start_date"],
        revenue_yoy=10.0 if chunk["start_date"] == DATE_1 else 11.0,
    )
    _run(harness, plan, provider=True, apply=False)
    first_chunk_id = plan["chunks"][0]["chunk_id"]

    def hard_crash_after_ready(key: str, payload: dict[str, Any]) -> None:
        if (
            f"chunk_id={first_chunk_id}/manifest.json" in key
            and payload.get("state") == "COMPLETED"
        ):
            raise SystemExit(92)

    harness.json_write_hook = hard_crash_after_ready
    with pytest.raises(SystemExit, match="92"):
        _run(harness, plan, provider=False, apply=True)
    harness.json_write_hook = None
    report_key = _attempt_report_key(plan, 2)
    tampered = deepcopy(harness.json_objects[report_key])
    reducer = tampered["validation"]["financial_reducer"]
    if defect == "dependency_keys":
        reducer["dependency_keys"] = ["foreign-chunk"]
    elif defect == "prior_state_checksum":
        reducer["prior_state_checksum"] = "0" * 64
    elif defect == "seed_trade_date":
        reducer["seed"]["trade_date"] = "2023-12-28"
    elif defect == "seed_object_key":
        reducer["seed"]["object_key"] = "raw/financial/trade_date=2023-12-28/data.parquet"
    elif defect == "seed_checksum":
        reducer["seed"]["checksum"] = "0" * 64
    else:
        reducer["terminal"]["last_materialized_trade_date"] = "2023-12-31"
    harness.json_objects[report_key] = tampered
    harness.reset_events()

    with pytest.raises(backfill.BackfillPlanningError) as exc_info:
        _run(harness, plan, provider=False, apply=True)

    assert exc_info.value.code == "INVALID_READY_REPORT"
    assert _event_count(harness, "fetch") == 0
    assert _event_count(harness, "canonical_write") == 0


def test_v2_financial_final_closed_tail_with_unmaterialized_announcements_is_blocked():
    friday = "2024-01-05"
    saturday = "2024-01-06"
    sunday = "2024-01-07"
    seed = _financial_seed_frame().copy(deep=True)
    seed["announce_date"] = friday
    plan = backfill.build_history_backfill_plan_v2(
        run_id="goal21-review-financial-final-closed-tail",
        start_date=saturday,
        end_date=sunday,
        codes=[CODE_1],
        code_batch_size=250,
        date_batch_days=31,
        announce_date_batch_days=1,
        datasets=["financial"],
        financial_seed_manifest=_financial_seed_manifest(
            frame=seed,
            trade_date=friday,
        ),
        generated_at_fn=lambda: FIXED_NOW,
    )
    harness = MemoryHarness()
    harness.canonical_objects[("financial", friday)] = seed
    harness.fetch_factory = lambda chunk: _financial_result(
        chunk,
        announce_date=chunk["start_date"],
        revenue_yoy=10.0 if chunk["start_date"] == saturday else 11.0,
        canonical_dates=(),
        predecessor_trade_date=friday,
    )

    result = _run(harness, plan, provider=True, apply=True)

    assert result["summary"]["canonical_ready"] is False
    assert result["summary"]["state_counts"]["BLOCKED"] >= 1
    assert any(
        gap["category"] == "SEMANTIC_SOURCE_UNAVAILABLE"
        for gap in result["summary"]["gaps"]
    )
    assert _event_count(harness, "canonical_write") == 0


def test_v2_financial_open_window_with_later_closed_tail_cannot_claim_ready():
    thursday = "2024-01-04"
    friday = "2024-01-05"
    saturday = "2024-01-06"
    sunday = "2024-01-07"
    seed = _financial_seed_frame().copy(deep=True)
    seed["announce_date"] = thursday
    plan = backfill.build_history_backfill_plan_v2(
        run_id="goal21-review-financial-open-plus-closed-tail",
        start_date=friday,
        end_date=sunday,
        codes=[CODE_1],
        code_batch_size=250,
        date_batch_days=31,
        announce_date_batch_days=1,
        datasets=["financial"],
        financial_seed_manifest=_financial_seed_manifest(
            frame=seed,
            trade_date=thursday,
        ),
        generated_at_fn=lambda: FIXED_NOW,
    )
    harness = MemoryHarness()
    harness.canonical_objects[("financial", thursday)] = seed
    harness.fetch_factory = lambda chunk: _financial_result(
        chunk,
        announce_date=chunk["start_date"],
        revenue_yoy={friday: 10.0, saturday: 11.0, sunday: 12.0}[chunk["start_date"]],
        canonical_dates=(friday,) if chunk["start_date"] == friday else (),
        predecessor_trade_date=thursday,
    )

    result = _run(harness, plan, provider=True, apply=True)

    assert result["summary"]["canonical_ready"] is False
    assert result["summary"]["state_counts"]["BLOCKED"] >= 1
    assert any(
        gap["category"] == "SEMANTIC_SOURCE_UNAVAILABLE"
        for gap in result["summary"]["gaps"]
    )
    assert _event_count(harness, "canonical_write") == 1
    assert ("financial", friday) in harness.canonical_objects


def test_v2_financial_empty_closed_tail_carries_final_anchor_into_next_run_seed():
    thursday = "2024-01-04"
    friday = "2024-01-05"
    saturday = "2024-01-06"
    sunday = "2024-01-07"
    monday = "2024-01-08"
    seed = _financial_seed_frame().copy(deep=True)
    seed["announce_date"] = thursday
    plan = backfill.build_history_backfill_plan_v2(
        run_id="goal21-review-financial-empty-closed-tail-anchor",
        start_date=friday,
        end_date=sunday,
        codes=[CODE_1],
        code_batch_size=250,
        date_batch_days=31,
        announce_date_batch_days=1,
        datasets=["financial"],
        financial_seed_manifest=_financial_seed_manifest(
            frame=seed,
            trade_date=thursday,
        ),
        generated_at_fn=lambda: FIXED_NOW,
    )
    harness = MemoryHarness()
    harness.canonical_objects[("financial", thursday)] = seed

    def fetch(chunk: dict[str, Any]) -> HistoricalChunkFetchResult:
        if chunk["start_date"] == friday:
            return _financial_result(
                chunk,
                announce_date=friday,
                revenue_yoy=10.0,
                canonical_dates=(friday,),
                predecessor_trade_date=thursday,
            )
        return _empty_financial_result(
            chunk,
            predecessor_trade_date=thursday,
        )

    harness.fetch_factory = fetch
    result = _run(harness, plan, provider=True, apply=True)
    final_chunk = plan["chunks"][-1]
    final_manifest_key = _keys(plan)["chunks"][final_chunk["chunk_id"]]["manifest"]
    final_manifest = deepcopy(harness.json_objects[final_manifest_key])

    assert result["summary"]["canonical_ready"] is True
    assert final_manifest["state"] == "COMPLETED"
    assert final_manifest["coverage"]["canonical_trade_dates"] == []
    terminal = final_manifest["validation"]["financial_reducer"]["terminal"]
    assert terminal["passed"] is True
    assert terminal["last_materialized_trade_date"] == friday
    assert terminal["anchor"]["object_key"] == build_partition(
        "financial",
        friday,
    ).object_key

    next_plan = backfill.build_history_backfill_plan_v2(
        run_id="goal21-review-financial-next-run-from-closed-tail",
        start_date=monday,
        end_date=monday,
        codes=[CODE_1],
        code_batch_size=250,
        date_batch_days=31,
        announce_date_batch_days=1,
        datasets=["financial"],
        financial_seed_manifest=final_manifest,
        generated_at_fn=lambda: FIXED_NOW,
    )

    assert next_plan["scope"]["financial_seed"]["trade_date"] == friday
    assert next_plan["scope"]["financial_seed"]["checksum"] == terminal["state_checksum"]


@pytest.mark.parametrize(
    ("provider_status", "expected_state"),
    [("FAILED", "FAILED"), ("BLOCKED", "BLOCKED")],
)
def test_structured_provider_failure_is_preserved_before_executor_classification(
    provider_status: str,
    expected_state: str,
):
    plan = _plan(run_id=f"goal21-review-provider-audit-{provider_status.lower()}")
    harness = MemoryHarness()
    supplied: dict[str, HistoricalChunkFetchResult] = {}

    def fetch(chunk: dict[str, Any]) -> HistoricalChunkFetchResult:
        result = _failed_result(chunk, provider_status)
        supplied["result"] = result
        return result

    harness.fetch_factory = fetch
    _run(harness, plan, provider=True, apply=False)

    result = supplied["result"]
    manifest = harness.manifest(plan)
    report = harness.attempt_report(plan)
    assert manifest["state"] == expected_state
    assert manifest["provider_status"] == result.provider_status
    assert manifest["row_count"] == 0
    assert manifest["actual_schema"] == list(result.actual_schema)
    assert manifest["target_schema"] == list(result.target_schema)
    assert manifest["dq"] == result.dq
    assert manifest["coverage"] == result.coverage
    assert manifest["validation"] == result.validation
    assert manifest["source_key"] == result.source_keys[0]
    assert manifest["failure"] == result.failure

    assert report["state"] == expected_state
    assert report["provider_status"] == result.provider_status
    assert report["source_keys"] == list(result.source_keys)
    assert report["provider_calls"] == list(result.provider_calls)
    assert report["actual_schema"] == list(result.actual_schema)
    assert report["target_schema"] == list(result.target_schema)
    assert report["dq"] == result.dq
    assert report["coverage"] == result.coverage
    assert report["validation"] == result.validation
    assert report["failure"] == result.failure
    assert report["failure"]["exception_type"] == result.failure["exception_type"]

    assert harness.parquet_objects == {}
    assert harness.canonical_objects == {}
    assert _event_count(harness, "artifact_parquet_write") == 0
    assert _event_count(harness, "canonical_write") == 0


def test_new_provider_attempt_does_not_inherit_failed_audit_evidence_from_prior_attempt():
    plan = _plan(run_id="goal21-review-provider-audit-attempt-isolation")
    harness = MemoryHarness()
    harness.fetch_factory = lambda chunk: _failed_result(chunk, "BLOCKED")

    _run(harness, plan, provider=True, apply=False)
    first = harness.attempt_report(plan, attempt=1)
    assert first["provider_status"] == "BLOCKED"
    assert first["source_keys"]
    assert first["actual_schema"]

    def raise_transient(_chunk: dict[str, Any]) -> HistoricalChunkFetchResult:
        raise InjectedFailure(
            "new provider attempt timed out before returning a result",
            failure_category="TRANSIENT_PROVIDER_ERROR",
        )

    harness.fetch_factory = raise_transient
    _run(harness, plan, provider=True, apply=False)

    manifest = harness.manifest(plan)
    second = harness.attempt_report(plan, attempt=2)
    assert manifest["state"] == second["state"] == "FAILED"
    assert manifest["attempt_count"] == second["attempt"] == 2
    assert manifest["failure"]["category"] == "TRANSIENT_PROVIDER_ERROR"
    assert second["failure"] == manifest["failure"]
    assert second["provider_status"] is None
    assert second["source_keys"] == []
    assert second["provider_calls"] == []
    assert second["row_count"] is None
    assert second["actual_schema"] is None
    assert second["target_schema"] is None
    assert second["dq"] is None
    assert second["coverage"] is None
    assert second["validation"] is None
    assert second["source_key"] is None


def test_staging_write_is_auditable_when_immediate_readback_fails():
    plan = _plan(run_id="goal21-review-orphan-staging-audit")
    harness = MemoryHarness()
    harness.fetch_factory = _adj_result
    original_read = harness.artifact_read_parquet

    def fail_staging_readback(key: str) -> pd.DataFrame:
        if "/attempt=1/" in key and key.endswith("/part.parquet"):
            raise InjectedFailure(
                "staging read-back failed after atomic write",
                failure_category="READBACK_FAILED",
            )
        return original_read(key)

    harness.artifact_read_parquet = fail_staging_readback  # type: ignore[method-assign]
    _run(harness, plan, provider=True, apply=False)

    manifest = harness.manifest(plan)
    report = harness.attempt_report(plan)
    assert manifest["state"] == report["state"] == "FAILED"
    assert manifest["failure"]["category"] == "READBACK_FAILED"
    assert report["provider_status"] == "FETCHED"
    assert report["source_keys"]
    assert isinstance(report["staging_key"], str)
    assert report["staging_key"] in harness.parquet_objects
    assert isinstance(report["staging_checksum"], str)
    assert len(report["staging_checksum"]) == 64
    assert report["staging_attempt"] == 1
    assert manifest["staging_key"] == report["staging_key"]
    assert manifest["staging_checksum"] == report["staging_checksum"]
    assert manifest["staging_attempt"] == report["staging_attempt"]
    assert _event_count(harness, "canonical_write") == 0


def _leave_ready_report_without_final_checkpoint(
    plan: dict[str, Any],
) -> tuple[MemoryHarness, dict[str, Any]]:
    harness = MemoryHarness()
    harness.fetch_factory = _adj_result

    def fail_completed_checkpoint(key: str, payload: dict[str, Any]) -> None:
        if (
            "/chunk_id=" in key
            and key.endswith("/manifest.json")
            and payload.get("state") == "COMPLETED"
        ):
            raise InjectedFailure(
                "final checkpoint unavailable",
                failure_category="WRITE_FAILED",
            )

    harness.json_write_hook = fail_completed_checkpoint
    _run(harness, plan, provider=True, apply=True)
    ready = harness.attempt_report(plan)
    assert ready["state"] == "READY_TO_CHECKPOINT"
    assert ready["checkpoint_target_state"] == "COMPLETED"
    assert harness.manifest(plan)["state"] == "FAILED"
    harness.json_write_hook = None
    return harness, ready


def _leave_provider_only_ready_report_without_final_checkpoint(
    plan: dict[str, Any],
) -> tuple[MemoryHarness, dict[str, Any]]:
    harness = MemoryHarness()
    harness.fetch_factory = _adj_result

    def fail_staged_checkpoint(key: str, payload: dict[str, Any]) -> None:
        if (
            "/chunk_id=" in key
            and key.endswith("/manifest.json")
            and payload.get("state") == "STAGED"
        ):
            raise InjectedFailure(
                "final provider checkpoint unavailable",
                failure_category="WRITE_FAILED",
            )

    harness.json_write_hook = fail_staged_checkpoint
    _run(harness, plan, provider=True, apply=False)
    ready = harness.attempt_report(plan)
    assert ready["state"] == "READY_TO_CHECKPOINT"
    assert ready["checkpoint_target_state"] == "STAGED"
    assert ready["requested_stages"] == ["provider"]
    assert harness.manifest(plan)["state"] == "FAILED"
    harness.json_write_hook = None
    return harness, ready


def _leave_apply_only_ready_report_without_final_checkpoint(
    plan: dict[str, Any],
) -> tuple[MemoryHarness, dict[str, Any]]:
    harness = MemoryHarness()
    harness.fetch_factory = _adj_result
    _run(harness, plan, provider=True, apply=False)

    def fail_completed_checkpoint(key: str, payload: dict[str, Any]) -> None:
        if (
            "/chunk_id=" in key
            and key.endswith("/manifest.json")
            and payload.get("state") == "COMPLETED"
        ):
            raise InjectedFailure(
                "final apply checkpoint unavailable",
                failure_category="WRITE_FAILED",
            )

    harness.json_write_hook = fail_completed_checkpoint
    _run(harness, plan, provider=False, apply=True)
    ready = harness.attempt_report(plan, attempt=2)
    assert ready["state"] == "READY_TO_CHECKPOINT"
    assert ready["checkpoint_target_state"] == "COMPLETED"
    assert ready["requested_stages"] == ["apply"]
    assert harness.manifest(plan)["state"] == "FAILED"
    harness.json_write_hook = None
    return harness, ready


@pytest.mark.parametrize(
    ("report_mode", "manifest_stages"),
    [
        ("combined", ["apply"]),
        ("combined", ["provider"]),
        ("apply_only", ["provider", "apply"]),
        ("apply_only", ["provider"]),
    ],
)
def test_ready_recovery_rejects_legal_but_conflicting_manifest_and_report_stages(
    report_mode: str,
    manifest_stages: list[str],
):
    plan = _plan(
        run_id=(
            "goal21-final-ready-stage-conflict-"
            f"{report_mode}-{'-'.join(manifest_stages)}"
        )
    )
    if report_mode == "combined":
        harness, ready = _leave_ready_report_without_final_checkpoint(plan)
    else:
        harness, ready = _leave_apply_only_ready_report_without_final_checkpoint(plan)

    manifest_key = _keys(plan)["chunks"][plan["chunks"][0]["chunk_id"]]["manifest"]
    tampered_manifest = deepcopy(harness.json_objects[manifest_key])
    tampered_manifest["requested_stages"] = list(manifest_stages)
    harness.json_objects[manifest_key] = tampered_manifest
    manifest_before = deepcopy(tampered_manifest)
    report_before = deepcopy(ready)
    harness.reset_events()

    with pytest.raises(backfill.BackfillPlanningError) as exc_info:
        _run(harness, plan, provider=False, apply=True)

    assert exc_info.value.code == "INVALID_READY_REPORT"
    assert harness.json_objects[manifest_key] == manifest_before
    assert harness.attempt_report(plan, attempt=ready["attempt"]) == report_before
    assert _event_count(harness, "fetch") == 0
    assert _event_count(harness, "artifact_parquet_write") == 0
    assert _event_count(harness, "canonical_write") == 0
    assert not any(
        event[0] == "chunk_manifest_write" and event[2] in {"STAGED", "COMPLETED"}
        for event in harness.events
    )


def test_provider_only_ready_recovery_still_reconstructs_staged_without_refetch():
    plan = _plan(run_id="goal21-final-provider-only-ready-recovery")
    harness, ready = _leave_provider_only_ready_report_without_final_checkpoint(plan)
    harness.reset_events()

    _run(harness, plan, provider=True, apply=False)

    manifest = harness.manifest(plan)
    assert manifest["state"] == "STAGED"
    assert manifest["requested_stages"] == ready["requested_stages"] == ["provider"]
    assert manifest["attempt_count"] == ready["attempt"] == 1
    assert _event_count(harness, "fetch") == 0
    assert _event_count(harness, "artifact_parquet_write") == 0
    assert _event_count(harness, "canonical_write") == 0


def test_apply_only_ready_recovery_still_reconstructs_completed_without_rewrite():
    plan = _plan(run_id="goal21-final-apply-only-ready-recovery")
    harness, ready = _leave_apply_only_ready_report_without_final_checkpoint(plan)
    canonical_before = harness.canonical_objects[("adj_factor", DATE_1)].copy(deep=True)
    harness.reset_events()

    _run(harness, plan, provider=False, apply=True)

    manifest = harness.manifest(plan)
    assert manifest["state"] == "COMPLETED"
    assert manifest["requested_stages"] == ready["requested_stages"] == ["apply"]
    assert manifest["attempt_count"] == ready["attempt"] == 2
    assert _event_count(harness, "fetch") == 0
    assert _event_count(harness, "artifact_parquet_write") == 0
    assert _event_count(harness, "canonical_write") == 0
    pd.testing.assert_frame_equal(
        harness.canonical_objects[("adj_factor", DATE_1)],
        canonical_before,
    )


def test_apply_only_recovers_valid_ready_report_without_refetch_or_duplicate_write():
    plan = _plan(run_id="goal21-review-ready-recovery")
    harness, ready = _leave_ready_report_without_final_checkpoint(plan)
    persisted_ready = deepcopy(ready)
    canonical_before = harness.canonical_objects[("adj_factor", DATE_1)].copy(deep=True)
    harness.reset_events()

    _run(harness, plan, provider=False, apply=True)

    manifest = harness.manifest(plan)
    assert manifest["state"] == "COMPLETED"
    assert manifest["attempt_count"] == ready["attempt"] == 1
    assert manifest["plan_fingerprint"] == ready["plan_fingerprint"] == plan["plan_fingerprint"]
    assert ready["run_id"] == plan["run_id"]
    assert ready["chunk"] == plan["chunks"][0]
    assert ready["requested_stages"] == ["provider", "apply"]
    for field in (
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
        "canonical_keys",
        "canonical_checksums",
        "validation",
        "write_result",
        "read_back_result",
    ):
        assert manifest[field] == ready[field]
    assert harness.attempt_report(plan) == persisted_ready
    assert _attempt_report_key(plan, 2) not in harness.json_objects
    assert _event_count(harness, "fetch") == 0
    assert _event_count(harness, "artifact_parquet_write") == 0
    assert _event_count(harness, "canonical_write") == 0
    assert _event_count(harness, "canonical_read") >= 1
    pd.testing.assert_frame_equal(
        harness.canonical_objects[("adj_factor", DATE_1)],
        canonical_before,
    )


def test_apply_only_carries_complete_plural_provider_lineage_from_staging_attempt():
    plan = _plan(run_id="goal21-review-apply-only-plural-lineage")
    harness = MemoryHarness()
    expected_source_keys = (
        "raw/provider/tushare/adj_factor/response-a.parquet",
        "raw/provider/tushare/adj_factor/response-b.parquet",
    )
    harness.fetch_factory = lambda chunk: _adj_result(
        chunk,
        source_keys=expected_source_keys,
    )

    _run(harness, plan, provider=True, apply=False)
    first_report = harness.attempt_report(plan, attempt=1)
    _run(harness, plan, provider=False, apply=True)
    second_report = harness.attempt_report(plan, attempt=2)

    assert first_report["source_keys"] == list(expected_source_keys)
    assert second_report["source_keys"] == list(expected_source_keys)
    assert second_report["provider_calls"] == first_report["provider_calls"]
    assert second_report["staging_attempt"] == 1
    assert harness.manifest(plan)["state"] == "COMPLETED"


def test_completed_resume_recomputes_and_repairs_coherently_tampered_canonical_evidence():
    plan = _plan(run_id="goal21-review-completed-canonical-evidence-tamper")
    harness = MemoryHarness()
    harness.fetch_factory = _adj_result
    _run(harness, plan, provider=True, apply=True)
    original = harness.manifest(plan)
    object_key = original["canonical_keys"][0]
    expected_checksum = original["canonical_checksums"][object_key]
    tampered = deepcopy(original)
    tampered["canonical_checksums"][object_key] = "0" * 64
    tampered["write_result"]["partitions"][0]["checksum"] = "0" * 64
    tampered["read_back_result"]["partitions"][0]["checksum"] = "0" * 64
    manifest_key = _keys(plan)["chunks"][plan["chunks"][0]["chunk_id"]]["manifest"]
    harness.json_objects[manifest_key] = tampered
    harness.reset_events()

    _run(harness, plan, provider=False, apply=True)

    repaired = harness.manifest(plan)
    assert repaired["state"] == "COMPLETED"
    assert repaired["attempt_count"] == 2
    assert repaired["canonical_checksums"][object_key] == expected_checksum
    assert repaired["write_result"]["partitions"][0]["checksum"] == expected_checksum
    assert repaired["read_back_result"]["partitions"][0]["checksum"] == expected_checksum
    assert _event_count(harness, "fetch") == 0
    assert _event_count(harness, "canonical_write") == 0


@pytest.mark.parametrize(
    "defect",
    [
        "identity",
        "staging_checksum",
        "read_back",
        "canonical_checksum_bundle",
        "canonical_row_count_bundle",
        "provider_call_type",
        "target_schema",
        "staging_row_count",
        "partition_counters",
    ],
)
def test_ready_report_recovery_rejects_tampered_identity_checksum_or_readback(defect: str):
    plan = _plan(run_id=f"goal21-review-ready-tampered-{defect}")
    harness, _ready = _leave_ready_report_without_final_checkpoint(plan)
    report_key = _attempt_report_key(plan, 1)
    tampered = deepcopy(harness.json_objects[report_key])
    if defect == "identity":
        tampered["plan_fingerprint"] = "0" * 64
    elif defect == "staging_checksum":
        tampered["staging_checksum"] = "0" * 64
    elif defect == "read_back":
        tampered["read_back_result"]["partitions"][0]["exact_read_back_success"] = False
    elif defect == "canonical_checksum_bundle":
        object_key = tampered["canonical_keys"][0]
        tampered["canonical_checksums"][object_key] = "0" * 64
        tampered["write_result"]["partitions"][0]["checksum"] = "0" * 64
        tampered["read_back_result"]["partitions"][0]["checksum"] = "0" * 64
    elif defect == "canonical_row_count_bundle":
        tampered["write_result"]["partitions"][0]["row_count"] += 1
        tampered["read_back_result"]["partitions"][0]["row_count"] += 1
    elif defect == "provider_call_type":
        tampered["provider_calls"].append("not-a-provider-call")
    elif defect == "target_schema":
        tampered["target_schema"] = list(reversed(tampered["target_schema"]))
    elif defect == "staging_row_count":
        tampered["row_count"] += 1
    else:
        for field in ("write_result", "read_back_result"):
            record = tampered[field]["partitions"][0]
            record["inserted_rows"] = record["row_count"] + 1
            record["unchanged_rows"] = 0
    harness.json_objects[report_key] = tampered
    harness.reset_events()

    with pytest.raises(backfill.BackfillPlanningError):
        _run(harness, plan, provider=False, apply=True)

    assert _event_count(harness, "fetch") == 0
    assert _event_count(harness, "artifact_parquet_write") == 0
    assert _event_count(harness, "canonical_write") == 0


def _typed_stock_frames(dtype_mode: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    columns = get_schema_contract("stock_basic").columns
    row = {
        "stock_code": CODE_1,
        "stock_name": "Fixture Bank",
        "exchange": "SZSE",
        "list_date": "1991-04-03",
        "delist_date": None,
        "industry": "Bank",
        "market_type": "Main Board",
        "is_st": False,
        "trade_date": DATE_1,
    }
    if dtype_mode == "object":
        incoming = pd.DataFrame(columns=columns, dtype=object)
        existing = pd.DataFrame([row], columns=columns, dtype=object)
        return incoming, existing

    dtypes = {
        "stock_code": "string",
        "stock_name": "string",
        "exchange": "string",
        "list_date": "string",
        "delist_date": "string",
        "industry": "string",
        "market_type": "string",
        "is_st": "boolean",
        "trade_date": "string",
    }
    incoming = pd.DataFrame({column: pd.Series(dtype=dtypes[column]) for column in columns})
    existing = pd.DataFrame([row], columns=columns).astype(dtypes)
    return incoming, existing


@pytest.mark.parametrize("dtype_mode", ["object", "nullable"])
def test_empty_stock_basic_completed_resume_never_skips_contaminated_member_scope(dtype_mode: str):
    plan = _plan(run_id=f"goal21-review-empty-stock-{dtype_mode}", dataset="stock_basic")
    harness = MemoryHarness()
    incoming, contaminated = _typed_stock_frames(dtype_mode)
    harness.fetch_factory = lambda chunk: _empty_result(
        chunk,
        dataset="stock_basic",
        frame=incoming,
    )
    _run(harness, plan, provider=True, apply=True)
    assert harness.manifest(plan)["state"] == "COMPLETED"
    harness.canonical_objects[("stock_basic", DATE_1)] = contaminated.copy(deep=True)
    harness.reset_events()

    result = _run(harness, plan, provider=False, apply=True)

    manifest = harness.manifest(plan)
    assert result["skipped_chunk_ids"] == []
    assert manifest["state"] == "FAILED"
    assert manifest["failure"]["category"] == "READBACK_FAILED"
    assert _event_count(harness, "canonical_read") >= 1
    assert _event_count(harness, "canonical_write") == 0
    pd.testing.assert_frame_equal(
        harness.canonical_objects[("stock_basic", DATE_1)],
        contaminated,
    )


def test_stock_basic_nonempty_apply_replaces_the_complete_requested_membership_scope():
    plan = _plan(
        run_id="goal21-review-stock-membership-scope-replacement",
        dataset="stock_basic",
        codes=[CODE_1, CODE_2],
    )
    columns = get_schema_contract("stock_basic").columns
    member = pd.DataFrame(
        [
            {
                "stock_code": CODE_1,
                "stock_name": "Fixture Bank",
                "exchange": "SZSE",
                "list_date": "1991-04-03",
                "delist_date": None,
                "industry": "Bank",
                "market_type": "Main Board",
                "is_st": False,
                "trade_date": DATE_1,
            }
        ],
        columns=columns,
    )
    stale_nonmember = member.copy(deep=True)
    stale_nonmember.loc[0, "stock_code"] = CODE_2
    stale_nonmember.loc[0, "stock_name"] = "Future Member"
    stale_nonmember.loc[0, "list_date"] = "2025-01-01"
    harness = MemoryHarness()
    harness.canonical_objects[("stock_basic", DATE_1)] = stale_nonmember
    harness.fetch_factory = lambda chunk: _stock_result(chunk, member)

    _run(harness, plan, provider=True, apply=True)

    canonical = harness.canonical_objects[("stock_basic", DATE_1)]
    assert canonical["stock_code"].tolist() == [CODE_1]
    assert harness.manifest(plan)["state"] == "COMPLETED"
    harness.reset_events()

    resumed = _run(harness, plan, provider=False, apply=True)

    assert resumed["skipped_chunk_ids"] == [plan["chunks"][0]["chunk_id"]]
    assert _event_count(harness, "canonical_write") == 0
    assert harness.canonical_objects[("stock_basic", DATE_1)]["stock_code"].tolist() == [CODE_1]


def test_stock_basic_apply_rejects_future_list_date_pollution_outside_its_write_scope():
    plan = _plan(
        run_id="goal21-review-stock-future-list-date-canonical-dq",
        dataset="stock_basic",
    )
    columns = get_schema_contract("stock_basic").columns
    incoming = pd.DataFrame(
        [
            {
                "stock_code": CODE_1,
                "stock_name": "Fixture Bank",
                "exchange": "SZSE",
                "list_date": "1991-04-03",
                "delist_date": None,
                "industry": "Bank",
                "market_type": "Main Board",
                "is_st": False,
                "trade_date": DATE_1,
            }
        ],
        columns=columns,
    )
    future_outside_scope = incoming.copy(deep=True)
    future_outside_scope.loc[0, "stock_code"] = CODE_2
    future_outside_scope.loc[0, "list_date"] = "2025-01-01"
    harness = MemoryHarness()
    harness.canonical_objects[("stock_basic", DATE_1)] = future_outside_scope
    harness.fetch_factory = lambda chunk: _stock_result(chunk, incoming)

    _run(harness, plan, provider=True, apply=True)

    manifest = harness.manifest(plan)
    assert manifest["state"] == "BLOCKED"
    assert manifest["failure"]["category"] == "DQ_FAILED"
    assert _event_count(harness, "canonical_write") == 0


def test_daily_price_apply_replaces_stale_rows_proven_absent_by_suspension_coverage():
    plan = _plan(
        run_id="goal21-review-daily-price-paused-scope-replacement",
        dataset="daily_price",
        codes=[CODE_1, CODE_2],
    )
    incoming = _daily_price_frame(CODE_1)
    stale_paused_code = _daily_price_frame(CODE_2, is_paused=False)
    harness = MemoryHarness()
    harness.canonical_objects[("daily_price", DATE_1)] = stale_paused_code
    harness.fetch_factory = lambda chunk: _daily_price_result(chunk, incoming)

    _run(harness, plan, provider=True, apply=True)

    canonical = harness.canonical_objects[("daily_price", DATE_1)]
    assert canonical["stock_code"].tolist() == [CODE_1]
    assert harness.manifest(plan)["state"] == "COMPLETED"
    harness.reset_events()

    resumed = _run(harness, plan, provider=False, apply=True)

    assert resumed["skipped_chunk_ids"] == [plan["chunks"][0]["chunk_id"]]
    assert _event_count(harness, "canonical_write") == 0
    assert harness.canonical_objects[("daily_price", DATE_1)]["stock_code"].tolist() == [CODE_1]


def _boundary_st_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "stock_code": CODE_1,
                "st_type": "ST",
                "start_date": "2023-12-15",
                "end_date": DATE_1,
                "source": "fixture_history",
            }
        ],
        columns=get_schema_contract("st_history").columns,
    )


def test_st_interval_ending_at_chunk_start_is_rejected_from_staging_as_non_overlapping():
    plan = _plan(run_id="goal21-review-st-staging-boundary", dataset="st_history")
    harness = MemoryHarness()
    harness.fetch_factory = lambda chunk: _st_result(chunk, _boundary_st_frame())

    _run(harness, plan, provider=True, apply=False)

    manifest = harness.manifest(plan)
    assert manifest["state"] == "BLOCKED"
    assert manifest["failure"]["category"] == "DQ_FAILED"
    assert _event_count(harness, "artifact_parquet_write") == 0
    assert _event_count(harness, "canonical_write") == 0


def test_st_interval_ending_at_chunk_start_does_not_contradict_valid_empty_canonical_scope():
    plan = _plan(run_id="goal21-review-st-canonical-boundary", dataset="st_history")
    harness = MemoryHarness()
    harness.fetch_factory = lambda chunk: _empty_result(chunk, dataset="st_history")
    boundary = _boundary_st_frame()
    harness.canonical_objects[("st_history", DATE_1)] = boundary.copy(deep=True)

    _run(harness, plan, provider=True, apply=True)

    manifest = harness.manifest(plan)
    assert manifest["state"] == "COMPLETED"
    assert manifest["failure"] is None
    assert _event_count(harness, "canonical_write") == 0
    pd.testing.assert_frame_equal(
        harness.canonical_objects[("st_history", DATE_1)],
        boundary,
    )


def test_st_history_nonempty_apply_replaces_overlapping_requested_interval_scope():
    plan = _plan(
        run_id="goal21-review-st-history-scope-replacement",
        dataset="st_history",
    )
    columns = get_schema_contract("st_history").columns
    trusted = pd.DataFrame(
        [
            {
                "stock_code": CODE_1,
                "st_type": "ST",
                "start_date": "2023-12-15",
                "end_date": None,
                "source": "verified_history",
            }
        ],
        columns=columns,
    )
    current_snapshot = pd.DataFrame(
        [
            {
                "stock_code": CODE_1,
                "st_type": "CURRENT_ST_SNAPSHOT",
                "start_date": DATE_1,
                "end_date": None,
                "source": "current_st_snapshot",
            }
        ],
        columns=columns,
    )
    outside_scope = pd.DataFrame(
        [
            {
                "stock_code": CODE_2,
                "st_type": "ST",
                "start_date": "2023-12-01",
                "end_date": None,
                "source": "other_verified_history",
            }
        ],
        columns=columns,
    )
    harness = MemoryHarness()
    harness.canonical_objects[("st_history", DATE_1)] = pd.concat(
        [current_snapshot, outside_scope],
        ignore_index=True,
    )
    harness.fetch_factory = lambda chunk: _st_result(chunk, trusted)

    _run(harness, plan, provider=True, apply=True)

    canonical = harness.canonical_objects[("st_history", DATE_1)]
    code_1 = canonical.loc[canonical["stock_code"] == CODE_1]
    assert code_1["source"].tolist() == ["verified_history"]
    assert set(canonical["stock_code"]) == {CODE_1, CODE_2}
    assert harness.manifest(plan)["state"] == "COMPLETED"
    harness.reset_events()

    resumed = _run(harness, plan, provider=False, apply=True)

    assert resumed["skipped_chunk_ids"] == [plan["chunks"][0]["chunk_id"]]
    assert _event_count(harness, "canonical_write") == 0
    code_1 = harness.canonical_objects[("st_history", DATE_1)].loc[
        lambda frame: frame["stock_code"] == CODE_1
    ]
    assert code_1["source"].tolist() == ["verified_history"]


def test_st_history_apply_rejects_case_variant_current_snapshot_outside_write_scope():
    plan = _plan(
        run_id="goal21-review-st-current-marker-canonical-dq",
        dataset="st_history",
    )
    columns = get_schema_contract("st_history").columns
    trusted = pd.DataFrame(
        [
            {
                "stock_code": CODE_1,
                "st_type": "ST",
                "start_date": "2023-12-15",
                "end_date": None,
                "source": "verified_history",
            }
        ],
        columns=columns,
    )
    outside_current_snapshot = pd.DataFrame(
        [
            {
                "stock_code": CODE_2,
                "st_type": "Current_ST_Snapshot",
                "start_date": DATE_1,
                "end_date": None,
                "source": "Current_St_Snapshot",
            }
        ],
        columns=columns,
    )
    harness = MemoryHarness()
    harness.canonical_objects[("st_history", DATE_1)] = outside_current_snapshot
    harness.fetch_factory = lambda chunk: _st_result(chunk, trusted)

    _run(harness, plan, provider=True, apply=True)

    manifest = harness.manifest(plan)
    assert manifest["state"] == "BLOCKED"
    assert manifest["failure"]["category"] == "DQ_FAILED"
    assert _event_count(harness, "canonical_write") == 0

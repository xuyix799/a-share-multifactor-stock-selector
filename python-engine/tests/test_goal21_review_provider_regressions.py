from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
import hashlib
import json
from typing import Any
from types import SimpleNamespace

import pandas as pd
import pytest

from stock_selector import cli
from stock_selector.data import historical_backfill as backfill
from stock_selector.providers.historical_provider import HistoricalProviderRouter
from stock_selector.providers import tushare_provider as tushare_provider_module
from stock_selector.providers.base import ProviderFetchError
from stock_selector.providers.tushare_provider import TushareProvider


TRADE_DATE = "2024-01-02"
COMPACT_DATE = "20240102"
CODES = ["000001.SZ", "600519.SH"]


@dataclass
class _MemoryArtifacts:
    json_objects: dict[str, dict[str, Any]] = field(default_factory=dict)
    parquet_objects: dict[str, pd.DataFrame] = field(default_factory=dict)

    def read_json(self, key: str) -> dict[str, Any]:
        if key not in self.json_objects:
            raise FileNotFoundError(key)
        return deepcopy(self.json_objects[key])

    def write_json(self, key: str, payload: dict[str, Any]) -> str:
        self.json_objects[key] = deepcopy(payload)
        return key

    def read_parquet(self, key: str) -> pd.DataFrame:
        if key not in self.parquet_objects:
            raise FileNotFoundError(key)
        return self.parquet_objects[key].copy(deep=True)

    def write_parquet(self, key: str, frame: pd.DataFrame) -> str:
        self.parquet_objects[key] = frame.copy(deep=True)
        return key


@dataclass
class _VerifiedRawLanding:
    """A fake raw landing boundary that really performs checksum read-back."""

    objects: dict[str, pd.DataFrame] = field(default_factory=dict)
    endpoint_keys: dict[str, list[str]] = field(default_factory=dict)

    def __call__(self, endpoint: str, parameters: dict[str, Any], frame: pd.DataFrame) -> str:
        request_hash = hashlib.sha256(
            json.dumps(parameters, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()[:16]
        response_checksum = backfill.dataframe_checksum(frame, key_columns=_raw_key_columns(frame))
        key = (
            f"raw/provider=tushare/endpoint={endpoint}/request={request_hash}/"
            f"response={response_checksum}.parquet"
        )
        self.objects[key] = frame.copy(deep=True)
        read_back = self.objects[key].copy(deep=True)
        assert backfill.dataframe_checksum(
            read_back, key_columns=_raw_key_columns(read_back)
        ) == response_checksum
        self.endpoint_keys.setdefault(endpoint, []).append(key)
        return key


def _raw_key_columns(frame: pd.DataFrame) -> list[str]:
    preferred = [name for name in ("ts_code", "trade_date") if name in frame.columns]
    return preferred or list(frame.columns)


def test_real_raw_landing_is_immutable_idempotent_and_read_back_verified():
    artifacts = _MemoryArtifacts()
    frame = _live_raw_frame(
        "adj_factor",
        {"ts_code": CODES[0], "start_date": COMPACT_DATE, "end_date": COMPACT_DATE},
    )
    persist = getattr(backfill, "persist_historical_raw_landing", None)
    assert callable(persist), "Goal 21 review requires a real raw landing helper"
    kwargs = {
        "provider_name": "tushare",
        "run_id": "goal21-review-raw-landing",
        "endpoint": "adj_factor",
        "parameters": {"ts_code": CODES[0], "trade_date": COMPACT_DATE},
        "frame": frame,
        "read_parquet_fn": artifacts.read_parquet,
        "write_parquet_fn": artifacts.write_parquet,
    }

    key = persist(**kwargs)
    assert key.startswith(
        "raw/provider_landing/provider=tushare/run_id=goal21-review-raw-landing/"
    )
    assert set(artifacts.parquet_objects) == {key}
    assert persist(**kwargs) == key

    artifacts.parquet_objects[key].loc[0, "adj_factor"] = 9.9
    with pytest.raises(backfill.BackfillExecutionError) as exc_info:
        persist(**kwargs)
    assert exc_info.value.failure_category == "READBACK_FAILED"


def test_suspend_raw_landing_identity_and_readback_bind_terminal_coverage_evidence():
    artifacts = _MemoryArtifacts()
    rows = pd.DataFrame(
        {
            "ts_code": [CODES[0]],
            "trade_date": [COMPACT_DATE],
            "suspend_type": ["S"],
            "suspend_timing": ["09:30:00"],
        }
    )
    incomplete = rows.copy(deep=True)
    incomplete.attrs.update(
        full_market_event_set=False,
        coverage_complete=False,
        sample_truncated=True,
        empty_after_retries=False,
        covered_trade_dates=[COMPACT_DATE],
        pagination={
            "page_size": 5000,
            "page_count": 1,
            "row_counts": [1],
            "terminal_page": False,
        },
    )
    complete = rows.copy(deep=True)
    complete.attrs.update(
        full_market_event_set=True,
        coverage_complete=True,
        sample_truncated=False,
        empty_after_retries=False,
        covered_trade_dates=[COMPACT_DATE],
        pagination={
            "page_size": 5000,
            "page_count": 1,
            "row_counts": [1],
            "terminal_page": True,
        },
    )
    common = {
        "provider_name": "tushare",
        "run_id": "goal21-review-suspend-raw-evidence",
        "endpoint": "suspend_d",
        "parameters": {"trade_date": COMPACT_DATE},
        "read_parquet_fn": artifacts.read_parquet,
        "write_parquet_fn": artifacts.write_parquet,
    }

    incomplete_key = backfill.persist_historical_raw_landing(
        frame=incomplete,
        **common,
    )
    complete_key = backfill.persist_historical_raw_landing(
        frame=complete,
        **common,
    )

    assert complete_key != incomplete_key
    assert artifacts.parquet_objects[complete_key].attrs["coverage_complete"] is True
    assert artifacts.parquet_objects[complete_key].attrs["pagination"]["terminal_page"] is True

    artifacts.parquet_objects[complete_key].attrs["sample_truncated"] = True
    with pytest.raises(backfill.BackfillExecutionError) as exc_info:
        backfill.persist_historical_raw_landing(frame=complete, **common)
    assert exc_info.value.failure_category == "READBACK_FAILED"


@pytest.mark.parametrize("upstream_truncated", [False, True])
def test_tushare_suspend_boundary_emits_terminal_page_proof_without_erasing_negative_evidence(
    upstream_truncated: bool,
):
    class _Pro:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def suspend_d(self, **parameters: Any) -> pd.DataFrame:
            self.calls.append(deepcopy(parameters))
            if upstream_truncated:
                frame = pd.DataFrame(
                    {
                        "ts_code": [CODES[0]],
                        "trade_date": [COMPACT_DATE],
                        "suspend_type": ["S"],
                        "suspend_timing": ["09:30:00"],
                    }
                )
                frame.attrs["sample_truncated"] = True
                return frame
            return pd.DataFrame(
                columns=["ts_code", "trade_date", "suspend_type", "suspend_timing"]
            )

    provider = TushareProvider.__new__(TushareProvider)
    provider._pro = _Pro()
    provider._retry = SimpleNamespace(max_attempts=1, backoff_seconds=0.0)

    frame = provider.fetch_raw_endpoint_allow_empty(
        "suspend_d",
        trade_date=COMPACT_DATE,
        fields="ts_code,trade_date,suspend_timing,suspend_type",
    )

    assert provider._pro.calls[0]["offset"] == 0
    assert provider._pro.calls[0]["limit"] > 0
    assert frame.attrs["full_market_event_set"] is (not upstream_truncated)
    assert frame.attrs["covered_trade_dates"] == [COMPACT_DATE]
    assert frame.attrs["empty_after_retries"] is False
    assert frame.attrs["sample_truncated"] is upstream_truncated
    assert frame.attrs["coverage_complete"] is (not upstream_truncated)
    assert frame.attrs["pagination"]["terminal_page"] is True


@pytest.mark.parametrize(
    ("negative_attr", "negative_value"),
    [("coverage_complete", False), ("empty_after_retries", True)],
)
def test_tushare_suspend_boundary_keeps_all_upstream_incompleteness_sticky(
    negative_attr: str,
    negative_value: bool,
):
    class _Pro:
        def suspend_d(self, **_parameters: Any) -> pd.DataFrame:
            frame = pd.DataFrame(
                {
                    "ts_code": [CODES[0]],
                    "trade_date": [COMPACT_DATE],
                    "suspend_type": ["S"],
                    "suspend_timing": ["09:30:00"],
                }
            )
            frame.attrs[negative_attr] = negative_value
            return frame

    provider = TushareProvider.__new__(TushareProvider)
    provider._pro = _Pro()
    provider._retry = SimpleNamespace(max_attempts=1, backoff_seconds=0.0)

    frame = provider.fetch_raw_endpoint_allow_empty(
        "suspend_d",
        trade_date=COMPACT_DATE,
        fields="ts_code,trade_date,suspend_timing,suspend_type",
    )

    assert frame.attrs[negative_attr] is negative_value
    assert frame.attrs["coverage_complete"] is False
    assert frame.attrs["full_market_event_set"] is False


def test_tushare_suspend_boundary_rejects_cross_page_event_overlap(monkeypatch):
    monkeypatch.setattr(tushare_provider_module, "TUSHARE_SUSPEND_PAGE_SIZE", 2)

    class _Pro:
        def suspend_d(self, **parameters: Any) -> pd.DataFrame:
            rows = {
                0: [CODES[0], CODES[1]],
                2: [CODES[1]],
            }[parameters["offset"]]
            return pd.DataFrame(
                {
                    "ts_code": rows,
                    "trade_date": [COMPACT_DATE] * len(rows),
                    "suspend_type": ["S"] * len(rows),
                    "suspend_timing": ["09:30:00"] * len(rows),
                }
            )

    provider = TushareProvider.__new__(TushareProvider)
    provider._pro = _Pro()
    provider._retry = SimpleNamespace(max_attempts=1, backoff_seconds=0.0)

    with pytest.raises(ProviderFetchError, match="overlapping event keys"):
        provider.fetch_raw_endpoint_allow_empty(
            "suspend_d",
            trade_date=COMPACT_DATE,
            fields="ts_code,trade_date,suspend_timing,suspend_type",
        )


def _plan(
    dataset: str,
    *,
    codes: list[str] | None = None,
    code_batch_size: int = 10,
    run_id: str | None = None,
) -> dict[str, Any]:
    return backfill.build_history_backfill_plan(
        run_id=run_id or f"goal21-review-{dataset}",
        start_date=TRADE_DATE,
        end_date=TRADE_DATE,
        codes=list(codes or [CODES[0]]),
        code_batch_size=code_batch_size,
        date_batch_days=31,
        report_period_months=3,
        datasets=[dataset],
        generated_at_fn=lambda: "2026-07-12T00:00:00Z",
    )


def _calendar(_start_date: str, _end_date: str) -> pd.DataFrame:
    return pd.DataFrame({"cal_date": [COMPACT_DATE], "is_open": [1]})


def _full_calendar(start_date: str, end_date: str) -> pd.DataFrame:
    dates = pd.date_range(start_date, end_date, freq="D")
    return pd.DataFrame(
        {
            "cal_date": dates.strftime("%Y%m%d"),
            "is_open": [int(value.weekday() < 5) for value in dates],
        }
    )


def _live_raw_frame(endpoint: str, parameters: dict[str, Any]) -> pd.DataFrame:
    requested_code = parameters.get("ts_code", CODES[0])
    if endpoint == "adj_factor":
        frame = pd.DataFrame(
            {"ts_code": [requested_code], "trade_date": [COMPACT_DATE], "adj_factor": [1.0]}
        )
    elif endpoint == "daily_basic":
        frame = pd.DataFrame(
            {
                "ts_code": [requested_code],
                "trade_date": [COMPACT_DATE],
                "pe_ttm": [8.0],
                "pb": [1.0],
                "ps_ttm": [1.5],
                "total_mv": [1000.0],
                "circ_mv": [800.0],
                "turnover_rate": [0.5],
            }
        )
    elif endpoint == "daily":
        frame = pd.DataFrame(
            {
                "ts_code": [requested_code],
                "trade_date": [COMPACT_DATE],
                "open": [10.0],
                "high": [10.5],
                "low": [9.8],
                "close": [10.2],
                "pre_close": [10.0],
                "vol": [100.0],
                "amount": [1020.0],
            }
        )
    elif endpoint == "stk_limit":
        frame = pd.DataFrame(
            {
                "ts_code": CODES,
                "trade_date": [COMPACT_DATE, COMPACT_DATE],
                "up_limit": [11.0, 11.0],
                "down_limit": [9.0, 9.0],
            }
        )
    elif endpoint == "suspend_d":
        frame = pd.DataFrame(
            columns=["ts_code", "trade_date", "suspend_type", "suspend_timing"]
        )
        frame.attrs.update(
            full_market_event_set=True,
            coverage_complete=True,
            sample_truncated=False,
            empty_after_retries=False,
            covered_trade_dates=[COMPACT_DATE],
        )
    else:
        raise AssertionError(f"unexpected endpoint: {endpoint}")

    if endpoint in {"adj_factor", "daily_basic"}:
        frame.attrs.update(
            source_semantics="HISTORICAL_RANGE_SOURCE",
            coverage_complete=True,
            sample_truncated=False,
            requested_codes=[requested_code],
            coverage_start_date=TRADE_DATE,
            coverage_end_date=TRADE_DATE,
        )
    return frame


@pytest.mark.parametrize("dataset", ["daily_price", "adj_factor", "daily_basic"])
def test_live_router_to_executor_stages_only_after_verified_raw_landing(dataset: str):
    plan = _plan(dataset)
    unlanded_artifacts = _MemoryArtifacts()
    unlanded_router = HistoricalProviderRouter(
        plan=plan,
        provider_name="tushare",
        raw_fetch_fn=lambda endpoint, parameters: _live_raw_frame(endpoint, parameters),
        trading_calendar_fn=_calendar,
    )
    unlanded = backfill.run_real_history_backfill(
        plan=plan,
        artifact_read_json_fn=unlanded_artifacts.read_json,
        artifact_write_json_fn=unlanded_artifacts.write_json,
        artifact_read_parquet_fn=unlanded_artifacts.read_parquet,
        artifact_write_parquet_fn=unlanded_artifacts.write_parquet,
        fetch_chunk_fn=unlanded_router.fetch_chunk,
        provider_call_enabled=True,
        apply_standard_write=False,
        generated_at_fn=lambda: "2026-07-12T00:00:00Z",
    )

    assert unlanded["summary"]["state_counts"]["BLOCKED"] == 1
    assert unlanded["summary"]["gaps"][0]["category"] == "SEMANTIC_SOURCE_UNAVAILABLE"
    assert unlanded_artifacts.parquet_objects == {}

    artifacts = _MemoryArtifacts()
    raw_landing = _VerifiedRawLanding()
    router = HistoricalProviderRouter(
        plan=plan,
        provider_name="tushare",
        raw_fetch_fn=lambda endpoint, parameters: _live_raw_frame(endpoint, parameters),
        trading_calendar_fn=_calendar,
        raw_landing_fn=raw_landing,
    )
    observed_results = []

    def fetch(chunk: dict[str, Any]):
        result = router.fetch_chunk(chunk)
        observed_results.append(result)
        return result

    result = backfill.run_real_history_backfill(
        plan=plan,
        artifact_read_json_fn=artifacts.read_json,
        artifact_write_json_fn=artifacts.write_json,
        artifact_read_parquet_fn=artifacts.read_parquet,
        artifact_write_parquet_fn=artifacts.write_parquet,
        fetch_chunk_fn=fetch,
        provider_call_enabled=True,
        apply_standard_write=False,
        generated_at_fn=lambda: "2026-07-12T00:00:01Z",
    )

    assert result["summary"]["state_counts"]["STAGED"] == 1
    assert result["summary"]["canonical_ready"] is False
    assert result["summary"]["gaps"] == [
        {
            "dataset": dataset,
            "chunk_id": plan["chunks"][0]["chunk_id"],
            "state": "STAGED",
            "category": None,
            "reason": "chunk state is STAGED",
        }
    ]
    assert len(observed_results) == 1
    fetch_result = observed_results[0]
    assert fetch_result.source_keys
    assert set(fetch_result.source_keys) == set(raw_landing.objects)
    assert raw_landing.endpoint_keys["trade_cal"]
    assert fetch_result.source_keys[0] not in raw_landing.endpoint_keys["trade_cal"]
    assert any(call["endpoint"] == "trade_cal" for call in fetch_result.provider_calls)
    assert all(key.startswith("raw/provider=tushare/") for key in fetch_result.source_keys)
    landed_calls = [call for call in fetch_result.provider_calls if call.get("source_keys")]
    assert landed_calls
    for call in landed_calls:
        checksum = call["raw_checksum"]
        assert len(checksum) == 64
        int(checksum, 16)
        assert call["raw_read_back_verified"] is True
    reports = [
        payload
        for key, payload in artifacts.json_objects.items()
        if "/attempt=" in key and key.endswith("/report.json")
    ]
    assert len(reports) == 1
    assert reports[0]["source_keys"] == list(fetch_result.source_keys)


def test_market_sidecar_cache_is_bounded_to_the_active_date_window():
    plan = backfill.build_history_backfill_plan_v2(
        run_id="goal21-review-bounded-market-sidecar-cache",
        start_date="2024-01-02",
        end_date="2024-01-12",
        codes=[CODES[0]],
        code_batch_size=250,
        date_batch_days=1,
        announce_date_batch_days=31,
        datasets=["daily_price"],
        generated_at_fn=lambda: "2026-07-12T00:00:00Z",
    )

    def raw_fetch(endpoint: str, parameters: dict[str, Any]) -> pd.DataFrame:
        frame = _live_raw_frame(endpoint, parameters)
        compact = str(
            parameters.get("trade_date")
            or parameters.get("start_date")
            or COMPACT_DATE
        ).replace("-", "")
        if "trade_date" in frame.columns and not frame.empty:
            frame.loc[:, "trade_date"] = compact
        if endpoint == "suspend_d":
            frame.attrs["covered_trade_dates"] = [compact]
        return frame

    router = HistoricalProviderRouter(
        plan=plan,
        provider_name="tushare",
        raw_fetch_fn=raw_fetch,
        trading_calendar_fn=_full_calendar,
        raw_landing_fn=_VerifiedRawLanding(),
    )

    for chunk in plan["chunks"]:
        router.fetch_chunk(chunk)

    assert len(router._market_sidecar_cache) <= 2  # noqa: SLF001
    assert {
        endpoint for endpoint, _trade_date in router._market_sidecar_cache  # noqa: SLF001
    }.issubset({"stk_limit", "suspend_d"})


@pytest.mark.parametrize("dataset", ["adj_factor", "daily_basic"])
def test_v2_tushare_simple_mapping_failure_preserves_landed_call_schema_and_lineage(
    dataset: str,
):
    plan = backfill.build_history_backfill_plan_v2(
        run_id=f"goal21-review-malformed-{dataset}",
        start_date=TRADE_DATE,
        end_date=TRADE_DATE,
        codes=[CODES[0]],
        code_batch_size=250,
        date_batch_days=31,
        announce_date_batch_days=31,
        datasets=[dataset],
        generated_at_fn=lambda: "2026-07-12T00:00:00Z",
    )
    malformed = _live_raw_frame(dataset, {"trade_date": COMPACT_DATE})
    malformed.loc[0, "ts_code"] = "BAD-CODE"
    raw_landing = _VerifiedRawLanding()
    router = HistoricalProviderRouter(
        plan=plan,
        provider_name="tushare",
        raw_fetch_fn=lambda _endpoint, _parameters: malformed.copy(deep=True),
        trading_calendar_fn=_calendar,
        raw_landing_fn=raw_landing,
    )

    result = router.fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] == "SCHEMA_DRIFT"
    assert result.actual_schema == tuple(malformed.columns)
    assert any(call["endpoint"] == dataset for call in result.provider_calls)
    assert raw_landing.endpoint_keys[dataset]
    assert set(raw_landing.endpoint_keys[dataset]).issubset(result.source_keys)


def test_capability_blocked_chunk_does_not_inherit_previous_calendar_audit():
    plan = backfill.build_history_backfill_plan_v2(
        run_id="goal21-review-capability-audit-isolation",
        start_date=TRADE_DATE,
        end_date=TRADE_DATE,
        codes=[CODES[0]],
        code_batch_size=250,
        date_batch_days=31,
        announce_date_batch_days=31,
        datasets=["adj_factor", "financial"],
        generated_at_fn=lambda: "2026-07-12T00:00:00Z",
    )
    raw_landing = _VerifiedRawLanding()
    router = HistoricalProviderRouter(
        plan=plan,
        provider_name="tushare",
        raw_fetch_fn=lambda endpoint, parameters: _live_raw_frame(endpoint, parameters),
        trading_calendar_fn=_full_calendar,
        raw_landing_fn=raw_landing,
    )
    by_dataset = {chunk["dataset"]: chunk for chunk in plan["chunks"]}

    successful = router.fetch_chunk(by_dataset["adj_factor"])
    blocked = router.fetch_chunk(by_dataset["financial"])

    assert successful.provider_status == "FETCHED"
    assert any(call["endpoint"] == "trade_cal" for call in successful.provider_calls)
    assert blocked.provider_status == "BLOCKED"
    assert blocked.failure["category"] == "SEMANTIC_SOURCE_UNAVAILABLE"
    assert blocked.provider_calls == ()
    assert blocked.source_keys == ()


def test_akshare_benchmark_uses_verified_raw_landing_lineage_including_calendar():
    plan = _plan("benchmark_price", run_id="goal21-review-akshare-benchmark-lineage")
    raw_landing = _VerifiedRawLanding()

    def raw_fetch(endpoint: str, parameters: dict[str, Any]) -> pd.DataFrame:
        assert endpoint == "benchmark_price"
        close = 10.2
        pre_close = 10.0
        return pd.DataFrame(
            {
                "index_code": [parameters["index_code"]],
                "trade_date": [TRADE_DATE],
                "open": [10.0],
                "high": [10.5],
                "low": [9.8],
                "close": [close],
                "pre_close": [pre_close],
                "pct_chg": [((close / pre_close) - 1.0) * 100.0],
            }
        )

    router = HistoricalProviderRouter(
        plan=plan,
        provider_name="akshare",
        raw_fetch_fn=raw_fetch,
        trading_calendar_fn=_calendar,
        raw_landing_fn=raw_landing,
    )

    result = router.fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "FETCHED"
    assert result.source_keys
    assert set(result.source_keys) == set(raw_landing.objects)
    assert raw_landing.endpoint_keys["trade_cal"]
    assert len(raw_landing.endpoint_keys["benchmark_price"]) == 3
    assert all(
        call.get("raw_read_back_verified") is True
        and len(call.get("raw_checksum", "")) == 64
        for call in result.provider_calls
        if call.get("source_keys")
    )


def test_v2_trusted_empty_financial_announce_window_is_valid_empty_source_evidence():
    plan = backfill.build_history_backfill_plan_v2(
        run_id="goal21-review-empty-financial-announcements",
        start_date=TRADE_DATE,
        end_date=TRADE_DATE,
        codes=[CODES[0]],
        code_batch_size=250,
        date_batch_days=31,
        announce_date_batch_days=31,
        datasets=["financial"],
        generated_at_fn=lambda: "2026-07-12T00:00:00Z",
    )
    raw = pd.DataFrame(
        columns=[
            "ts_code",
            "end_date",
            "ann_date",
            "revenue_yoy",
            "net_profit_yoy",
            "roe",
            "gross_margin",
            "debt_ratio",
            "operating_cashflow",
        ]
    )
    raw.attrs.update(
        source_keys=["archive/financial/verified-empty-window.parquet"],
        source_semantics="TRUSTED_STANDARD_FINANCIAL_SOURCE",
        source_coverage_codes=[CODES[0]],
        requested_codes=[CODES[0]],
        coverage_complete=True,
        sample_truncated=False,
        coverage_start_date=TRADE_DATE,
        coverage_end_date=TRADE_DATE,
    )
    router = HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda _endpoint, _parameters: raw.copy(deep=True),
        trading_calendar_fn=_full_calendar,
    )

    result = router.fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "VALID_EMPTY"
    assert result.valid_empty is True
    assert result.coverage["complete"] is True
    assert result.coverage["financial_announce_window_empty"] is True
    assert result.source_keys == ("archive/financial/verified-empty-window.parquet",)


def test_v2_closed_market_window_completes_from_verified_calendar_without_dataset_call():
    saturday = "2024-01-06"
    compact_saturday = "20240106"
    plan = backfill.build_history_backfill_plan_v2(
        run_id="goal21-review-closed-market-window",
        start_date=saturday,
        end_date=saturday,
        codes=[CODES[0]],
        code_batch_size=250,
        date_batch_days=31,
        announce_date_batch_days=31,
        datasets=["adj_factor"],
        generated_at_fn=lambda: "2026-07-12T00:00:00Z",
    )
    artifacts = _MemoryArtifacts()
    raw_landing = _VerifiedRawLanding()

    def closed_calendar(_start: str, _end: str) -> pd.DataFrame:
        return pd.DataFrame({"cal_date": [compact_saturday], "is_open": [0]})

    router = HistoricalProviderRouter(
        plan=plan,
        provider_name="tushare",
        raw_fetch_fn=lambda *_args, **_kwargs: pytest.fail("closed window called dataset endpoint"),
        trading_calendar_fn=closed_calendar,
        raw_landing_fn=raw_landing,
    )

    result = backfill.run_real_history_backfill(
        plan=plan,
        artifact_read_json_fn=artifacts.read_json,
        artifact_write_json_fn=artifacts.write_json,
        artifact_read_parquet_fn=artifacts.read_parquet,
        artifact_write_parquet_fn=artifacts.write_parquet,
        fetch_chunk_fn=router.fetch_chunk,
        canonical_read_fn=lambda *_args: pytest.fail("closed window read canonical"),
        canonical_write_fn=lambda *_args: pytest.fail("closed window wrote canonical"),
        provider_call_enabled=True,
        apply_standard_write=True,
        generated_at_fn=lambda: "2026-07-12T00:00:01Z",
    )

    assert result["summary"]["canonical_ready"] is True
    assert result["summary"]["state_counts"]["COMPLETED"] == 1
    assert raw_landing.endpoint_keys["trade_cal"]


@pytest.mark.parametrize(
    ("suspend_mode", "expected_status", "expected_failure"),
    [
        ("truncated_subset", "BLOCKED", "SEMANTIC_SOURCE_UNAVAILABLE"),
        ("trusted_complete_empty", "FETCHED", None),
    ],
)
def test_cli_preserves_upstream_suspend_completeness_evidence(
    monkeypatch: pytest.MonkeyPatch,
    suspend_mode: str,
    expected_status: str,
    expected_failure: str | None,
):
    plan = _plan(
        "daily_price",
        codes=CODES,
        run_id=f"goal21-review-suspend-{suspend_mode}",
    )
    provider = _CliFakeProvider(suspend_mode)
    monkeypatch.setattr(cli, "load_settings", lambda: {"offline": True})
    monkeypatch.setattr(cli, "TushareProvider", lambda settings: provider)

    result = cli._build_history_fetch_chunk_fn(plan)(plan["chunks"][0])

    assert result.provider_status == expected_status
    assert (result.failure or {}).get("category") == expected_failure
    if suspend_mode == "truncated_subset":
        assert result.coverage["complete"] is False
        assert result.dq["passed"] is False
    else:
        assert result.coverage["complete"] is True
        assert result.frame["is_paused"].eq(False).all()


def test_market_sidecars_are_fetched_and_landed_once_per_trade_date_across_code_chunks():
    plan = _plan(
        "daily_price",
        codes=CODES,
        code_batch_size=1,
        run_id="goal21-review-sidecar-cache",
    )
    raw_landing = _VerifiedRawLanding()
    endpoint_calls: list[tuple[str, dict[str, Any]]] = []

    def raw_fetch(endpoint: str, parameters: dict[str, Any]) -> pd.DataFrame:
        endpoint_calls.append((endpoint, deepcopy(parameters)))
        return _live_raw_frame(endpoint, parameters)

    router = HistoricalProviderRouter(
        plan=plan,
        provider_name="tushare",
        raw_fetch_fn=raw_fetch,
        trading_calendar_fn=_calendar,
        raw_landing_fn=raw_landing,
    )

    results = [router.fetch_chunk(chunk) for chunk in plan["chunks"]]

    assert len(results) == 2
    assert all(result.provider_status == "FETCHED" for result in results)
    assert [endpoint for endpoint, _ in endpoint_calls].count("daily") == 2
    assert [endpoint for endpoint, _ in endpoint_calls].count("stk_limit") == 1
    assert [endpoint for endpoint, _ in endpoint_calls].count("suspend_d") == 1
    sidecar_keys = set(raw_landing.endpoint_keys["stk_limit"] + raw_landing.endpoint_keys["suspend_d"])
    assert sidecar_keys
    shared_keys = sidecar_keys | set(raw_landing.endpoint_keys["trade_cal"])
    assert set(results[0].source_keys) & set(results[1].source_keys) == shared_keys


class _CliFakeProvider:
    def __init__(self, suspend_mode: str) -> None:
        self.suspend_mode = suspend_mode

    def fetch_raw_endpoint_allow_empty(self, endpoint: str, **parameters: Any) -> pd.DataFrame:
        if endpoint == "trade_cal":
            return _calendar(TRADE_DATE, TRADE_DATE)
        if endpoint != "suspend_d":
            return _live_raw_frame(endpoint, parameters)
        if self.suspend_mode == "truncated_subset":
            frame = pd.DataFrame(
                {
                    "ts_code": [CODES[0]],
                    "trade_date": [COMPACT_DATE],
                    "suspend_type": ["S"],
                    "suspend_timing": ["09:30:00"],
                }
            )
            frame.attrs.update(
                full_market_event_set=True,
                coverage_complete=False,
                sample_truncated=True,
                empty_after_retries=False,
                covered_trade_dates=[COMPACT_DATE],
            )
            return frame
        assert self.suspend_mode == "trusted_complete_empty"
        return _live_raw_frame("suspend_d", parameters)

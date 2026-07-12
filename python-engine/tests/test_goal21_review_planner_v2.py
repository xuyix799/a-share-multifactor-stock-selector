from __future__ import annotations

from collections.abc import Callable
import json

import pandas as pd
import pytest

from stock_selector.data import historical_backfill as backfill
from stock_selector.providers.schema_contract import get_schema_contract


START_DATE = "2015-01-01"
END_DATE = "2024-12-31"
ONE_DAY = "2024-06-03"
SMALL_CODES = ["000001.SZ", "600519.SH"]


def test_v2_financial_plan_uses_announce_date_axis_instead_of_report_period_axis():
    plan = _build_v2_plan(
        run_id="goal21-review-v2-financial-one-day",
        start_date=ONE_DAY,
        end_date=ONE_DAY,
        datasets=["financial"],
    )

    assert plan["schema_version"] == backfill.PLAN_SCHEMA_VERSION_V2
    assert plan["planner_version"] == backfill.PLANNER_VERSION_V2
    assert plan["identity_schema_version"] == backfill.IDENTITY_SCHEMA_VERSION_V2
    assert plan["planner_version"] != backfill.PLANNER_VERSION
    assert plan["identity_schema_version"] != backfill.IDENTITY_SCHEMA_VERSION

    assert plan["chunk_count"] == 1
    chunk = plan["chunks"][0]
    assert chunk["dataset"] == "financial"
    assert chunk["strategy"] == "by_code_announce_date_window"
    assert chunk["axis"] == "announce_date"
    assert chunk["announce_date_start"] == ONE_DAY
    assert chunk["announce_date_end"] == ONE_DAY
    assert chunk["codes"] == []
    assert "report_period_start" not in chunk
    assert "report_period_end" not in chunk


def test_financial_one_day_increment_accepts_prior_quarter_announced_on_that_day():
    chunk = _financial_one_day_chunk()
    frame = _financial_frame(report_period="2024-03-31", announce_date=ONE_DAY)

    _api("validate_financial_announce_chunk_v2")(chunk, frame)


@pytest.mark.parametrize(
    ("report_period", "announce_date"),
    [
        ("2024-06-30", ONE_DAY),
        ("2024-03-31", "2024-06-04"),
    ],
    ids=["report-period-after-announce-date", "announce-date-outside-chunk"],
)
def test_financial_announce_chunk_rejects_future_period_and_out_of_scope_announcement(
    report_period,
    announce_date,
):
    chunk = _financial_one_day_chunk()
    frame = _financial_frame(report_period=report_period, announce_date=announce_date)

    with pytest.raises(backfill.BackfillExecutionError) as exc_info:
        _api("validate_financial_announce_chunk_v2")(chunk, frame)

    assert exc_info.value.failure_category == "DQ_FAILED"


def test_v1_scale_risk_estimate_is_constant_space_and_reproduces_the_known_explosion(monkeypatch):
    def refuse_chunk_materialization(**_kwargs):
        raise AssertionError("risk estimation must not materialize the 315,118-chunk v1 plan")

    monkeypatch.setattr(backfill, "_build_chunk", refuse_chunk_materialization)

    estimate = _api("estimate_history_backfill_v1_risk")(
        start_date=START_DATE,
        end_date=END_DATE,
        code_count=5000,
        code_batch_size=10,
        date_batch_days=31,
        report_period_months=3,
    )

    assert "chunks" not in estimate
    assert estimate["planner_version"] == backfill.PLANNER_VERSION
    assert estimate["chunk_count"] == 315_118
    assert estimate["plan_bytes_upper_bound"] >= 100_000_000
    assert 4_000_000 <= estimate["provider_call_count_upper_bound"] <= 6_000_000
    assert estimate["canonical_read_count_upper_bound"] >= 48_000_000


def test_v2_scale_estimate_meets_bounds_and_market_calls_do_not_multiply_by_code_batches():
    one_code_batch = _estimate_v2(code_count=250)
    full_market = _estimate_v2(code_count=5000)

    assert one_code_batch["code_batch_count"] == 1
    assert full_market["code_batch_count"] == 20
    assert full_market["market_level_provider_call_count_upper_bound"] == (
        one_code_batch["market_level_provider_call_count_upper_bound"]
    )

    assert full_market["planner_version"] == backfill.PLANNER_VERSION_V2
    assert full_market["chunk_count"] <= 25_000
    assert full_market["plan_bytes_upper_bound"] <= 16 * 1024 * 1024
    assert full_market["provider_call_count_upper_bound"] <= 215_000
    assert full_market["canonical_read_count_upper_bound"] <= backfill.DEFAULT_V2_MAX_CANONICAL_READS
    assert full_market["financial_canonical_read_count_upper_bound"] <= (
        2 * full_market["date_count"] + 31
    )


def test_v1_and_v2_plan_identity_are_disjoint_and_v2_uses_a_new_run_id():
    common = {
        "start_date": ONE_DAY,
        "end_date": ONE_DAY,
        "codes": SMALL_CODES,
        "datasets": ["financial"],
        "generated_at_fn": lambda: "2026-07-12T00:00:00Z",
    }
    v1 = backfill.build_history_backfill_plan(
        run_id="goal21-existing-v1-run",
        code_batch_size=250,
        date_batch_days=31,
        report_period_months=3,
        **common,
    )
    v2 = _build_v2_plan(run_id="goal21-review-v2-new-run", **common)

    assert v2["run_id"] != v1["run_id"]
    assert v2["schema_version"] != v1["schema_version"]
    assert v2["planner_version"] != v1["planner_version"]
    assert v2["identity_schema_version"] != v1["identity_schema_version"]
    assert v2["plan_fingerprint"] != v1["plan_fingerprint"]
    assert {chunk["chunk_id"] for chunk in v2["chunks"]}.isdisjoint(
        chunk["chunk_id"] for chunk in v1["chunks"]
    )


def test_v2_plan_executes_as_default_dry_run_without_provider_or_canonical_callbacks():
    plan = _build_v2_plan(
        run_id="goal21-review-v2-dry-run",
        datasets=["daily_price", "adj_factor", "daily_basic", "financial"],
    )
    objects = {}

    def read_json(key):
        if key not in objects:
            raise FileNotFoundError(key)
        return objects[key]

    def write_json(key, value):
        objects[key] = value

    def forbidden(*_args, **_kwargs):
        raise AssertionError("v2 dry-run must not access Parquet, provider, or canonical storage")

    result = backfill.run_real_history_backfill(
        plan=plan,
        artifact_read_json_fn=read_json,
        artifact_write_json_fn=write_json,
        artifact_read_parquet_fn=forbidden,
        artifact_write_parquet_fn=forbidden,
        provider_call_enabled=False,
        apply_standard_write=False,
    )

    assert result["summary"]["planned"] == plan["chunk_count"]
    assert result["summary"]["state_counts"]["PENDING"] == plan["chunk_count"]
    assert result["downstream_firewalls"] == {
        "clean_daily_snapshot": False,
        "factor": False,
        "selection": False,
        "backtest": False,
    }


def test_v2_financial_dependencies_are_ordered_and_universe_is_stored_once():
    plan = _build_v2_plan(
        run_id="goal21-review-v2-dependencies",
        start_date="2024-06-03",
        end_date="2024-06-04",
        announce_date_batch_days=1,
        datasets=["financial", "stock_basic", "st_history"],
    )
    financial = [chunk for chunk in plan["chunks"] if chunk["dataset"] == "financial"]

    assert plan["scope"]["codes"] == SMALL_CODES
    assert all(chunk["codes"] == [] for chunk in plan["chunks"])
    assert financial[0]["dependency_keys"] == []
    assert financial[1]["dependency_keys"] == [financial[0]["chunk_id"]]


def test_v2_preflight_plan_byte_bound_covers_pretty_serialized_full_market_plan():
    codes = [f"{value:06d}.SZ" for value in range(1, 5_001)]
    plan = _build_v2_plan(
        run_id="goal21-review-v2-plan-byte-bound",
        start_date=START_DATE,
        end_date=END_DATE,
        codes=codes,
        datasets=["daily_price", "adj_factor", "daily_basic", "financial"],
    )
    pretty_bytes = len(json.dumps(plan, ensure_ascii=False, indent=2).encode("utf-8"))

    assert pretty_bytes <= plan["preflight_estimate"]["plan_bytes_upper_bound"]
    assert pretty_bytes <= backfill.DEFAULT_V2_MAX_PLAN_BYTES


def test_v2_preflight_plan_byte_budget_includes_universe_key_bytes():
    universe = pd.DataFrame({"stock_code": ["000001.SZ"]})
    oversized_key = "raw/universe/" + "a" * 200_000 + ".parquet"

    with pytest.raises(backfill.BackfillPlanningError) as exc_info:
        _build_v2_plan(
            run_id="goal21-review-v2-oversized-universe-key",
            codes=None,
            universe_frame=universe,
            universe_key=oversized_key,
            max_plan_bytes=100_000,
        )

    assert exc_info.value.code == "PLAN_BUDGET_EXCEEDED"
    assert "plan_bytes" in str(exc_info.value)


def test_v2_plan_byte_budget_rechecks_the_actual_serialized_plan():
    with pytest.raises(backfill.BackfillPlanningError) as exc_info:
        _build_v2_plan(
            run_id="goal21-review-v2-actual-plan-byte-gate",
            max_plan_bytes=100_000,
            generated_at_fn=lambda: "x" * 200_000,
        )

    assert exc_info.value.code == "PLAN_BUDGET_EXCEEDED"
    assert "actual_plan_bytes" in str(exc_info.value)


def _api(name: str) -> Callable:
    value = getattr(backfill, name, None)
    assert callable(value), f"Goal 21 review requires public API historical_backfill.{name}"
    return value


def _build_v2_plan(**overrides):
    parameters = {
        "run_id": "goal21-review-v2",
        "start_date": "2024-06-01",
        "end_date": "2024-06-30",
        "codes": SMALL_CODES,
        "code_batch_size": 250,
        "date_batch_days": 31,
        "announce_date_batch_days": 31,
        "datasets": ["financial"],
        "generated_at_fn": lambda: "2026-07-12T00:00:00Z",
    }
    parameters.update(overrides)
    return _api("build_history_backfill_plan_v2")(**parameters)


def _estimate_v2(*, code_count: int):
    return _api("estimate_history_backfill_plan_v2")(
        start_date=START_DATE,
        end_date=END_DATE,
        code_count=code_count,
        code_batch_size=250,
        date_batch_days=31,
        announce_date_batch_days=31,
    )


def _financial_one_day_chunk():
    plan = _build_v2_plan(
        run_id="goal21-review-v2-financial-validation",
        start_date=ONE_DAY,
        end_date=ONE_DAY,
        datasets=["financial"],
    )
    assert plan["chunk_count"] == 1
    return plan["chunks"][0]


def _financial_frame(*, report_period: str, announce_date: str) -> pd.DataFrame:
    row = {
        "stock_code": "000001.SZ",
        "report_period": report_period,
        "announce_date": announce_date,
        "revenue_yoy": 10.0,
        "net_profit_yoy": 8.0,
        "roe": 12.0,
        "gross_margin": 30.0,
        "debt_ratio": 40.0,
        "operating_cashflow": 1_000_000.0,
    }
    return pd.DataFrame([row], columns=get_schema_contract("financial").columns)

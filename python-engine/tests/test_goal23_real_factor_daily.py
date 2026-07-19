from copy import deepcopy
from datetime import date, timedelta

import pandas as pd
import pytest

from factor_test_helpers import (
    adjusted_price_history,
    benchmark_price_history,
    clean_snapshot_history,
    factor_input_frame,
)
from goal23_test_helpers import FACTOR_CONFIG, Goal23MemoryStores
from stock_selector.data.data_validator import DataValidationError
from stock_selector.factors.factor_builder import build_factor_daily
from stock_selector.factors.factor_validator import FACTOR_DAILY_COLUMNS
import stock_selector.factors.real_factor_daily as real_factor_daily_module
from stock_selector.factors.real_factor_daily import (
    FACTOR_VALUE_COLUMNS,
    GOAL23_DOWNSTREAM_FIREWALLS,
    audit_factor_contract,
    build_goal23_factor_commit_key,
    read_goal23_published_factor_daily,
)


TRADE_DATE = "2026-06-19"


def _strict_three_year_clean_history(
    *,
    observations: int,
    earliest_offset_days: int,
) -> pd.DataFrame:
    target = date.fromisoformat(TRADE_DATE)
    earliest = target - timedelta(days=earliest_offset_days)
    latest = target - timedelta(days=1)
    span_days = (latest - earliest).days
    offsets = [
        round(index * span_days / (observations - 1))
        for index in range(observations)
    ]
    assert len(set(offsets)) == observations
    templates = clean_snapshot_history(TRADE_DATE, days=1).to_dict(
        orient="records"
    )
    rows = []
    for observation_index, offset in enumerate(offsets):
        history_date = (earliest + timedelta(days=offset)).isoformat()
        for stock_index, template in enumerate(templates):
            row = dict(template)
            row["trade_date"] = history_date
            row["pe_ttm"] = 8.0 + observation_index / 100 + stock_index
            row["pb"] = 0.8 + observation_index / 1000 + stock_index / 10
            rows.append(row)
    return pd.DataFrame(rows)


def test_goal23_dry_run_calculates_without_processed_writes():
    stores = Goal23MemoryStores([TRADE_DATE])

    result = stores.run_goal23(apply=False)

    assert result["status"] == "READY_FOR_APPLY"
    assert result["mode"] == "DRY_RUN"
    assert stores.factor_objects == {}
    assert stores.factor_commits == {}
    report = stores.control_json[result["daily_report_keys"][TRADE_DATE]]
    assert report["output"]["row_count"] > 0
    assert report["output"]["write"]["status"] == "NOT_REQUESTED"
    assert report["downstream_firewalls"] == GOAL23_DOWNSTREAM_FIREWALLS
    assert report["history_coverage"]["future_rows_used"] is False


def test_goal23_force_does_not_enable_apply():
    stores = Goal23MemoryStores([TRADE_DATE])

    result = stores.run_goal23(apply=False, force=True)

    assert result["status"] == "READY_FOR_APPLY"
    assert result["apply_requested"] is False
    assert result["manifest"]["force"] is True
    assert stores.factor_objects == {}
    assert stores.factor_commits == {}


def test_goal23_apply_stages_reads_back_then_commits_and_reader_uses_commit():
    stores = Goal23MemoryStores([TRADE_DATE])

    result = stores.run_goal23(apply=True)

    assert result["status"] == "COMPLETED"
    write_index = next(
        index
        for index, event in enumerate(stores.factor_events)
        if event[0] == "write_object"
    )
    commit_index = next(
        index
        for index, event in enumerate(stores.factor_events)
        if event[0] == "write_commit"
    )
    assert write_index < commit_index
    assert any(
        event[0] == "read_object"
        for event in stores.factor_events[write_index + 1 : commit_index]
    )
    committed = read_goal23_published_factor_daily(
        trade_date=TRADE_DATE,
        factor_commit_read_fn=stores.read_factor_commit,
        factor_object_read_fn=stores.read_factor_object,
    )
    assert list(committed.columns) == FACTOR_DAILY_COLUMNS
    assert "total_score" not in committed.columns
    assert build_goal23_factor_commit_key(TRADE_DATE) == (
        f"processed/_goal23_factor_commits/trade_date={TRADE_DATE}/commit.json"
    )


def test_goal23_generation_failure_does_not_publish_commit():
    stores = Goal23MemoryStores([TRADE_DATE])
    stores.fail_factor_write_once = TRADE_DATE

    result = stores.run_goal23(apply=True)

    assert result["status"] == "FAILED"
    assert TRADE_DATE not in stores.factor_commits
    report = stores.control_json[result["daily_report_keys"][TRADE_DATE]]
    assert report["commit"]["status"] == "UNCOMMITTED"
    assert report["blocked_reasons"] == [
        "OUTPUT_APPLY_FAILED:STAGE:factor_daily:RuntimeError"
    ]


def test_goal23_resume_is_idempotent_and_does_not_rewrite_generation():
    stores = Goal23MemoryStores([TRADE_DATE])
    first = stores.run_goal23(apply=True)
    first_object_writes = list(stores.factor_object_writes)
    first_commit_writes = list(stores.factor_commit_writes)

    second = stores.run_goal23(apply=True)

    assert first["status"] == second["status"] == "COMPLETED"
    assert stores.factor_object_writes == first_object_writes
    assert stores.factor_commit_writes == first_commit_writes
    report = stores.control_json[second["daily_report_keys"][TRADE_DATE]]
    assert report["resume_action"] == "REUSED_COMPLETED"
    assert report["output"]["write"]["status"] == "UNCHANGED"


def test_goal23_corrupt_existing_generation_is_not_overwritten():
    stores = Goal23MemoryStores([TRADE_DATE])
    first = stores.run_goal23(apply=True)
    assert first["status"] == "COMPLETED"
    generation_key = stores.factor_commits[TRADE_DATE]["output"]["object_key"]
    corrupted = stores.factor_objects[generation_key].copy(deep=True)
    corrupted.loc[corrupted.index[0], "quality_roe"] += 1.0
    stores.factor_objects[generation_key] = corrupted
    first_object_writes = list(stores.factor_object_writes)
    first_commit_writes = list(stores.factor_commit_writes)

    second = stores.run_goal23(apply=True)

    assert second["status"] == "FAILED"
    assert stores.factor_object_writes == first_object_writes
    assert stores.factor_commit_writes == first_commit_writes
    pd.testing.assert_frame_equal(
        stores.factor_objects[generation_key],
        corrupted,
    )
    report = stores.control_json[second["daily_report_keys"][TRADE_DATE]]
    assert report["blocked_reasons"] == [
        "OUTPUT_APPLY_FAILED:STAGE:factor_daily:RuntimeError"
    ]


def test_goal23_reader_rejects_extra_physical_total_score_column():
    stores = Goal23MemoryStores([TRADE_DATE])
    result = stores.run_goal23(apply=True)
    assert result["status"] == "COMPLETED"
    generation_key = stores.factor_commits[TRADE_DATE]["output"]["object_key"]
    stores.factor_objects[generation_key]["total_score"] = 88.0

    with pytest.raises(DataValidationError, match="exactly match"):
        read_goal23_published_factor_daily(
            trade_date=TRADE_DATE,
            factor_commit_read_fn=stores.read_factor_commit,
            factor_object_read_fn=stores.read_factor_object,
        )


@pytest.mark.parametrize(
    ("raw_dtype", "tampered_value"),
    [
        pytest.param("string", "1.25", id="string"),
        pytest.param("object", "1.25", id="object"),
        pytest.param("bool", True, id="bool"),
    ],
)
def test_goal23_reader_rejects_non_numeric_generation_dtype_before_normalizing(
    monkeypatch,
    raw_dtype,
    tampered_value,
):
    stores = Goal23MemoryStores([TRADE_DATE])
    result = stores.run_goal23(apply=True)
    assert result["status"] == "COMPLETED"
    generation_key = stores.factor_commits[TRADE_DATE]["output"]["object_key"]
    corrupted = stores.factor_objects[generation_key].copy(deep=True)
    assert corrupted["trend_ret_20d"].isna().all()
    corrupted["trend_ret_20d"] = pd.Series(
        [tampered_value] * len(corrupted),
        index=corrupted.index,
        dtype=raw_dtype,
    )
    stores.factor_objects[generation_key] = corrupted
    monkeypatch.setattr(
        real_factor_daily_module.pd,
        "to_numeric",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("pd.to_numeric ran before raw schema validation")
        ),
    )

    with pytest.raises(DataValidationError, match="trend_ret_20d.*raw pandas dtype"):
        read_goal23_published_factor_daily(
            trade_date=TRADE_DATE,
            factor_commit_read_fn=stores.read_factor_commit,
            factor_object_read_fn=stores.read_factor_object,
        )


def test_goal23_non_finite_base_factor_blocks_publication(monkeypatch):
    stores = Goal23MemoryStores([TRADE_DATE])
    original_builder = real_factor_daily_module.build_factor_daily

    def build_with_infinite_pe(**kwargs):
        factor_input = kwargs["factor_input_table"].copy(deep=True)
        factor_input.loc[factor_input.index[0], "pe_ttm"] = float("inf")
        return original_builder(
            **{
                **kwargs,
                "factor_input_table": factor_input,
            }
        )

    monkeypatch.setattr(
        real_factor_daily_module,
        "build_factor_daily",
        build_with_infinite_pe,
    )

    result = stores.run_goal23(
        run_id="goal23-non-finite-factor",
        apply=True,
    )

    assert result["status"] == "BLOCKED"
    assert stores.factor_objects == {}
    assert stores.factor_commits == {}
    report = stores.control_json[result["daily_report_keys"][TRADE_DATE]]
    assert report["commit"]["status"] == "UNCOMMITTED"
    assert "valuation_pe_ttm must contain only finite values" in report[
        "failure"
    ]["message"]


def test_goal23_one_date_failure_does_not_stop_later_date():
    dates = ["2026-06-18", "2026-06-19"]
    stores = Goal23MemoryStores(dates)
    stores.fail_factor_write_once = dates[0]

    result = stores.run_goal23(target_dates=dates, apply=True)

    assert result["status"] == "PARTIAL"
    assert result["date_statuses"] == {
        dates[0]: "FAILED",
        dates[1]: "COMPLETED",
    }
    assert dates[0] not in stores.factor_commits
    assert dates[1] in stores.factor_commits


def test_goal23_run_id_rejects_changed_date_or_manifest_scope():
    dates = ["2026-06-18", "2026-06-19"]
    stores = Goal23MemoryStores(dates)
    stores.run_goal23(
        run_id="goal23-immutable",
        target_dates=[dates[0]],
        apply=False,
    )

    with pytest.raises(ValueError, match="run_id scope"):
        stores.run_goal23(
            run_id="goal23-immutable",
            target_dates=[dates[1]],
            apply=False,
        )


def test_goal23_run_id_rejects_factor_config_drift_and_weight_sum_must_be_one():
    stores = Goal23MemoryStores([TRADE_DATE])
    stores.run_goal23(
        run_id="goal23-config-immutable",
        apply=False,
    )
    changed = deepcopy(FACTOR_CONFIG)
    changed["scoring"]["neutral_score"] = 45

    with pytest.raises(ValueError, match="run_id scope"):
        stores.run_goal23(
            run_id="goal23-config-immutable",
            apply=False,
            factor_config=changed,
        )

    invalid = deepcopy(FACTOR_CONFIG)
    invalid["quality_score"] = 0.31
    with pytest.raises(ValueError, match="sum must equal 1"):
        stores.run_goal23(
            run_id="goal23-invalid-weights",
            apply=False,
            factor_config=invalid,
        )


@pytest.mark.parametrize(
    "invalid_weight",
    [float("nan"), float("inf"), float("-inf")],
)
def test_goal23_factor_weights_must_be_finite(invalid_weight):
    stores = Goal23MemoryStores([TRADE_DATE])
    invalid = deepcopy(FACTOR_CONFIG)
    invalid["quality_score"] = invalid_weight

    with pytest.raises(ValueError, match="weight must be finite"):
        stores.run_goal23(
            run_id="goal23-non-finite-weights",
            apply=False,
            factor_config=invalid,
        )


@pytest.mark.parametrize("drift_dataset", ["adjusted_price", "factor_input_table"])
def test_goal23_goal22_generation_checksum_drift_blocks_target(drift_dataset):
    stores = Goal23MemoryStores([TRADE_DATE])
    commit = stores.goal22_commits[TRADE_DATE]
    object_key = commit["outputs"][drift_dataset]["object_key"]
    drifted = stores.goal22_processed_objects[object_key].copy()
    numeric_column = (
        "adj_close" if drift_dataset == "adjusted_price" else "amount"
    )
    drifted.loc[drifted.index[0], numeric_column] += 1.0
    stores.goal22_processed_objects[object_key] = drifted

    result = stores.run_goal23(apply=False)

    assert result["status"] == "BLOCKED"
    report = stores.control_json[result["daily_report_keys"][TRADE_DATE]]
    assert "generation checksum mismatch" in report["failure"]["message"]
    assert stores.factor_objects == {}


def test_goal23_input_drift_never_reuses_existing_factor_commit():
    stores = Goal23MemoryStores([TRADE_DATE])
    first = stores.run_goal23(apply=True)
    assert first["status"] == "COMPLETED"
    old_commit = deepcopy(stores.factor_commits[TRADE_DATE])
    goal22_key = stores.goal22_commits[TRADE_DATE]["outputs"][
        "adjusted_price"
    ]["object_key"]
    drifted = stores.goal22_processed_objects[goal22_key].copy()
    drifted.loc[drifted.index[0], "adj_close"] += 1.0
    stores.goal22_processed_objects[goal22_key] = drifted

    second = stores.run_goal23(apply=True)

    assert second["status"] == "BLOCKED"
    assert stores.factor_commits[TRADE_DATE] == old_commit
    report = stores.control_json[second["daily_report_keys"][TRADE_DATE]]
    assert report["resume_action"] != "REUSED_COMPLETED"


def test_goal23_unpublished_goal22_date_is_blocked():
    stores = Goal23MemoryStores([TRADE_DATE])
    del stores.goal22_commits[TRADE_DATE]

    result = stores.run_goal23(apply=False)

    assert result["status"] == "BLOCKED"
    report = stores.control_json[result["daily_report_keys"][TRADE_DATE]]
    assert "missing Goal 22 published artifact" in report["failure"]["message"]


def test_goal23_reader_rejects_uncommitted_generation_object():
    stores = Goal23MemoryStores([TRADE_DATE])
    result = stores.run_goal23(apply=True)
    assert result["status"] == "COMPLETED"
    del stores.factor_commits[TRADE_DATE]

    with pytest.raises(FileNotFoundError):
        read_goal23_published_factor_daily(
            trade_date=TRADE_DATE,
            factor_commit_read_fn=stores.read_factor_commit,
            factor_object_read_fn=stores.read_factor_object,
        )


def test_goal23_missing_benchmark_or_untrusted_lineage_blocks_target():
    stores = Goal23MemoryStores([TRADE_DATE])
    benchmark_key = f"raw/benchmark_price/trade_date={TRADE_DATE}/part.parquet"
    del stores.canonical_objects[benchmark_key]

    result = stores.run_goal23(apply=False)

    assert result["status"] == "BLOCKED"
    report = stores.control_json[result["daily_report_keys"][TRADE_DATE]]
    assert benchmark_key in report["failure"]["message"]


def test_goal23_benchmark_canonical_version_drift_blocks_target():
    stores = Goal23MemoryStores([TRADE_DATE])
    benchmark_key = f"raw/benchmark_price/trade_date={TRADE_DATE}/part.parquet"
    drifted = stores.canonical_objects[benchmark_key].copy()
    drifted.loc[drifted.index[0], "close"] += 1.0
    stores.canonical_objects[benchmark_key] = drifted

    result = stores.run_goal23(apply=False)

    assert result["status"] == "BLOCKED"
    report = stores.control_json[result["daily_report_keys"][TRADE_DATE]]
    assert "benchmark canonical version drift" in report["failure"]["message"]


def test_goal23_future_goal22_data_does_not_change_target_output():
    dates = ["2026-06-18", "2026-06-19"]
    target = dates[0]
    stores = Goal23MemoryStores(dates)
    before = stores.run_goal23(
        run_id="goal23-before-future-change",
        target_dates=[target],
        apply=False,
    )
    before_report = stores.control_json[before["daily_report_keys"][target]]
    future_commit = stores.goal22_commits[dates[1]]
    future_adjusted_key = future_commit["outputs"]["adjusted_price"]["object_key"]
    future_adjusted = stores.goal22_processed_objects[future_adjusted_key].copy()
    future_adjusted["adj_close"] = 999999.0
    stores.goal22_processed_objects[future_adjusted_key] = future_adjusted
    future_factor_key = future_commit["outputs"]["factor_input_table"][
        "object_key"
    ]
    future_factor_input = stores.goal22_processed_objects[
        future_factor_key
    ].copy()
    future_factor_input["roe"] = 999999.0
    future_factor_input["revenue_yoy"] = 999999.0
    stores.goal22_processed_objects[future_factor_key] = future_factor_input
    future_clean_key = future_commit["outputs"]["clean_daily_snapshot"][
        "object_key"
    ]
    future_clean = stores.goal22_processed_objects[future_clean_key].copy()
    future_clean["roe"] = 999999.0
    future_clean["net_profit_yoy"] = 999999.0
    stores.goal22_processed_objects[future_clean_key] = future_clean
    future_benchmark_key = (
        f"raw/benchmark_price/trade_date={dates[1]}/part.parquet"
    )
    future_benchmark = stores.canonical_objects[future_benchmark_key].copy()
    future_benchmark["close"] = 999999.0
    stores.canonical_objects[future_benchmark_key] = future_benchmark

    after = stores.run_goal23(
        run_id="goal23-after-future-change",
        target_dates=[target],
        apply=False,
    )
    after_report = stores.control_json[after["daily_report_keys"][target]]

    assert before["status"] == after["status"] == "READY_FOR_APPLY"
    assert before_report["output"]["checksum"] == after_report["output"]["checksum"]
    assert list(after_report["goal22_input_lineage"]) == [target]


def test_goal23_short_history_keeps_window_factors_null_and_scores_neutral():
    stores = Goal23MemoryStores([TRADE_DATE])

    result = stores.run_goal23(apply=True)

    committed = read_goal23_published_factor_daily(
        trade_date=TRADE_DATE,
        factor_commit_read_fn=stores.read_factor_commit,
        factor_object_read_fn=stores.read_factor_object,
    )
    assert committed["trend_ret_20d"].isna().all()
    assert committed["trend_ma20"].isna().all()
    assert committed["industry_strength_60d"].isna().all()
    assert committed["valuation_pe_percentile_3y"].isna().all()
    assert committed["valuation_pb_percentile_3y"].isna().all()
    assert committed["trend_score"].eq(50.0).all()
    report = stores.control_json[result["daily_report_keys"][TRADE_DATE]]
    assert report["history_coverage"]["insufficient_windows"]["price_20d"] is True
    assert report["history_coverage"]["insufficient_windows"]["valuation_3y"] is True
    assert report["factor_contract_audit"]["effective_factor_count"] < 15


def test_goal23_schema_complete_empty_factor_input_publishes_empty_factor_daily():
    stores = Goal23MemoryStores([TRADE_DATE], empty_universe=True)

    result = stores.run_goal23(apply=True)

    assert result["status"] == "COMPLETED"
    committed = read_goal23_published_factor_daily(
        trade_date=TRADE_DATE,
        factor_commit_read_fn=stores.read_factor_commit,
        factor_object_read_fn=stores.read_factor_object,
    )
    assert committed.empty
    assert list(committed.columns) == FACTOR_DAILY_COLUMNS
    report = stores.control_json[result["daily_report_keys"][TRADE_DATE]]
    audit = report["factor_contract_audit"]
    assert audit["effective_factor_count"] == 0
    assert set(audit["all_null_factors"]) == set(FACTOR_VALUE_COLUMNS)
    assert "total_score" not in committed.columns


def test_goal23_all_null_factors_are_not_counted_toward_v1_minimum():
    factor_daily = build_factor_daily(
        factor_input_table=factor_input_frame(TRADE_DATE),
        adjusted_price_history=adjusted_price_history(TRADE_DATE, days=130),
        clean_snapshot_history=clean_snapshot_history(TRADE_DATE, days=5),
        benchmark_price_history=benchmark_price_history(TRADE_DATE, days=130),
        trade_date=TRADE_DATE,
        factor_weights={},
    )
    factor_daily = factor_daily.copy()
    factor_daily["quality_cashflow_profit_ratio"] = pd.NA
    audit = audit_factor_contract(factor_daily)

    assert "quality_cashflow_profit_ratio" in audit["all_null_factors"]
    assert "quality_cashflow_profit_ratio" not in audit["effective_factors"]
    assert audit["effective_factor_count"] == len(FACTOR_VALUE_COLUMNS) - 1
    assert audit["effective_factor_count"] >= 15
    assert audit["meets_v1_minimum_effective_factors"] is True


def test_goal23_strict_three_year_history_accepts_exactly_720_prior_observations_at_boundary():
    factor_daily = build_factor_daily(
        factor_input_table=factor_input_frame(TRADE_DATE),
        adjusted_price_history=adjusted_price_history(TRADE_DATE, days=130),
        clean_snapshot_history=_strict_three_year_clean_history(
            observations=720,
            earliest_offset_days=365 * 3 - 31,
        ),
        benchmark_price_history=benchmark_price_history(
            TRADE_DATE,
            days=130,
        ),
        trade_date=TRADE_DATE,
        factor_weights={},
        strict_history_windows=True,
    )

    assert factor_daily["valuation_pe_percentile_3y"].notna().all()
    assert factor_daily["valuation_pb_percentile_3y"].notna().all()
    audit = audit_factor_contract(factor_daily)
    assert audit["effective_factor_count"] == len(FACTOR_VALUE_COLUMNS) - 1
    assert audit["effective_factor_count"] == 23
    assert audit["meets_v1_minimum_effective_factors"] is True


@pytest.mark.parametrize(
    ("observations", "earliest_offset_days"),
    [
        (719, 365 * 3 - 31),
        (720, 365 * 3 - 32),
    ],
)
def test_goal23_strict_three_year_history_rejects_count_or_boundary_shortfall(
    observations,
    earliest_offset_days,
):
    factor_daily = build_factor_daily(
        factor_input_table=factor_input_frame(TRADE_DATE),
        adjusted_price_history=adjusted_price_history(TRADE_DATE, days=130),
        clean_snapshot_history=_strict_three_year_clean_history(
            observations=observations,
            earliest_offset_days=earliest_offset_days,
        ),
        benchmark_price_history=benchmark_price_history(
            TRADE_DATE,
            days=130,
        ),
        trade_date=TRADE_DATE,
        factor_weights={},
        strict_history_windows=True,
    )

    assert factor_daily["valuation_pe_percentile_3y"].isna().all()
    assert factor_daily["valuation_pb_percentile_3y"].isna().all()


def test_goal23_manifest_tampering_is_not_accepted_as_trusted_coverage():
    stores = Goal23MemoryStores([TRADE_DATE])
    tampered = deepcopy(stores.control_json[stores.goal22_manifest_key])
    tampered["plan_fingerprint"] = "0" * 64
    stores.control_json[stores.goal22_manifest_key] = tampered
    catalog = stores.catalog()

    result = stores.run_goal23(apply=False, catalog=catalog)

    assert result["status"] == "BLOCKED"
    report = stores.control_json[result["daily_report_keys"][TRADE_DATE]]
    assert "plan fingerprint mismatch" in report["failure"]["message"]


def test_goal23_manifest_status_counts_must_match_date_states():
    stores = Goal23MemoryStores([TRADE_DATE])
    tampered = deepcopy(stores.control_json[stores.goal22_manifest_key])
    tampered["status_counts"] = {"BLOCKED": 1}
    stores.control_json[stores.goal22_manifest_key] = tampered

    result = stores.run_goal23(apply=False, catalog=stores.catalog())

    assert result["status"] == "BLOCKED"
    report = stores.control_json[result["daily_report_keys"][TRADE_DATE]]
    assert "status counts" in report["failure"]["message"]


def test_goal23_goal22_daily_attempt_and_commit_generation_must_agree():
    stores = Goal23MemoryStores([TRADE_DATE])
    manifest = stores.control_json[stores.goal22_manifest_key]
    daily_report_key = manifest["daily_report_keys"][TRADE_DATE]
    stores.control_json[daily_report_key]["attempt"] += 1

    attempt_result = stores.run_goal23(
        run_id="goal23-attempt-mismatch",
        apply=False,
    )

    assert attempt_result["status"] == "BLOCKED"
    attempt_report = stores.control_json[
        attempt_result["daily_report_keys"][TRADE_DATE]
    ]
    assert "not a completed trusted report" in attempt_report["failure"][
        "message"
    ]

    stores.control_json[daily_report_key]["attempt"] -= 1
    stores.goal22_commits[TRADE_DATE]["generation_id"] = "f" * 64
    generation_result = stores.run_goal23(
        run_id="goal23-generation-mismatch",
        apply=False,
    )

    assert generation_result["status"] == "BLOCKED"
    generation_report = stores.control_json[
        generation_result["daily_report_keys"][TRADE_DATE]
    ]
    assert "generation mismatch" in generation_report["failure"]["message"]

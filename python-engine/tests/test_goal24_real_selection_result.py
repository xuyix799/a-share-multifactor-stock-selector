from copy import deepcopy

import pandas as pd
import pytest

from goal23_test_helpers import FACTOR_CONFIG, Goal23MemoryStores
from goal24_test_helpers import (
    END_DATE,
    SELECTION_DATES,
    fresh_goal24_stores,
    with_goal22_arrow_schema_evidence,
)
from stock_selector.data.data_validator import DataValidationError
from stock_selector.scoring.real_selection_result import (
    GOAL24_FIREWALLS,
    REAL_SELECTION_TOP_N,
    _audit_input_key_sets,
    _selection_generation_id,
    freeze_real_selection_config,
    build_goal24_selection_generation_key,
    load_goal23_manifest_catalog,
    normalize_selection_result_frame,
    read_goal24_published_selection_result,
    run_real_selection_result_range,
    validate_goal24_selection_commit_payload,
    validate_real_selection_result,
)
from stock_selector.scoring.score_engine import parse_scoring_config
from stock_selector.scoring.selection_builder import build_selection_result
from stock_selector.scoring.selection_validator import (
    SELECTION_RESULT_COLUMNS,
)


@pytest.fixture
def stores():
    return fresh_goal24_stores()


def _target_report(stores, trade_date=END_DATE):
    manifest = stores.control_json[stores.goal23_manifest_key]
    return stores.control_json[manifest["daily_report_keys"][trade_date]]


def _target_frames(stores, trade_date=END_DATE):
    goal23_commit = stores.goal23_commits[trade_date]
    factor = stores.goal23_objects[
        goal23_commit["output"]["object_key"]
    ].copy(deep=True)
    goal22_commit = stores.goal22_commits[trade_date]
    frames = {
        dataset: stores.goal22_objects[
            goal22_commit["outputs"][dataset]["object_key"]
        ].copy(deep=True)
        for dataset in (
            "risk_filter",
            "eligible_universe",
            "factor_input_table",
        )
    }
    return factor, frames


def _scoring_config():
    return parse_scoring_config(FACTOR_CONFIG)


def _install_valid_different_lineage_commit(
    source_stores,
    *,
    target_stores=None,
):
    identity = (END_DATE, "monthly")
    target = target_stores or source_stores
    conflicting = deepcopy(source_stores.selection_commits[identity])
    original_frame = source_stores.selection_objects[
        conflicting["output"]["object_key"]
    ].copy(deep=True)
    conflicting["run_id"] = "goal24-other-lineage"
    conflicting["plan_fingerprint"] = "a" * 64
    conflicting["input_fingerprint"] = "b" * 64
    generation_id = _selection_generation_id(
        trade_date=END_DATE,
        rebalance_mode="monthly",
        input_fingerprint=conflicting["input_fingerprint"],
        config_fingerprint=conflicting[
            "selection_config_fingerprint"
        ],
        output_checksum=conflicting["output"]["checksum"],
        row_count=conflicting["output"]["row_count"],
        physical_schema_fingerprint=conflicting["output"][
            "physical_schema_fingerprint"
        ],
    )
    object_key = build_goal24_selection_generation_key(
        END_DATE,
        "monthly",
        generation_id,
    )
    conflicting["generation_id"] = generation_id
    conflicting["output"]["object_key"] = object_key
    validate_goal24_selection_commit_payload(
        conflicting,
        END_DATE,
        "monthly",
    )
    target.selection_objects[object_key] = original_frame
    return conflicting


def test_goal24_dry_run_is_plan_only_with_zero_persistence(stores):
    control_before = deepcopy(stores.control_json)

    result = stores.run_goal24(apply=False, force=True, resume=False)

    assert result["status"] == "READY_FOR_APPLY"
    assert result["mode"] == "DRY_RUN"
    assert result["manifest_persisted"] is False
    assert result["range_manifest_key"] is None
    assert result["daily_report_keys"] == {}
    assert stores.control_json == control_before
    assert stores.selection_objects == {}
    assert stores.selection_commits == {}
    assert stores.snapshots == {}
    plan = result["execution_plan"]
    assert plan["selection_dates"] == [END_DATE]
    assert plan["rebalance_mode"] == "monthly"
    assert plan["top_n"] == REAL_SELECTION_TOP_N
    assert result["firewalls"] == GOAL24_FIREWALLS
    report = result["date_results"][END_DATE]
    assert report["commit"]["status"] == "NOT_REQUESTED"
    assert len(report["commit"]["generation_id"]) == 64
    assert report["output"]["object_key"].endswith(
        f"generation={report['commit']['generation_id']}/part.parquet"
    )


def test_goal24_apply_reads_back_before_commit_and_snapshot(stores):
    result = stores.run_goal24(apply=True)

    assert result["status"] == "COMPLETED"
    write_index = next(
        index
        for index, event in enumerate(stores.events)
        if event[0] == "write_object"
    )
    commit_index = next(
        index
        for index, event in enumerate(stores.events)
        if event[0] == "write_commit"
    )
    snapshot_index = next(
        index
        for index, event in enumerate(stores.events)
        if event[0] == "write_snapshot"
    )
    assert write_index < commit_index < snapshot_index
    assert any(
        event[0] == "read_object"
        for event in stores.events[write_index + 1 : commit_index]
    )
    report = result["date_results"][END_DATE]
    assert report["dq_status"] == "PASS"
    assert report["output"]["read_back"]["exact_arrow_schema"] is True
    for dataset in (
        "risk_filter",
        "eligible_universe",
        "factor_input_table",
    ):
        input_record = report["goal22_lineage"]["selection_inputs"][
            dataset
        ]
        assert input_record["physical_schema"]
        assert len(input_record["physical_schema_fingerprint"]) == 64
    assert report["determinism_checks"] == {
        "score_recomputed": True,
        "risk_level_recomputed": True,
        "reason_recomputed": True,
        "suggestion_recomputed": True,
        "sort_total_score_desc": True,
        "tie_break_stock_code_asc": True,
        "rank_continuous": True,
    }
    assert report["snapshot"]["status"] == "WRITTEN"


def test_goal24_reader_recomputes_committed_rules_and_checksum(stores):
    stores.run_goal24(apply=True)

    published = read_goal24_published_selection_result(
        trade_date=END_DATE,
        rebalance_mode="monthly",
        selection_commit_read_fn=stores.read_selection_commit,
        selection_object_read_fn=stores.read_selection_object,
    )

    assert list(published.columns) == SELECTION_RESULT_COLUMNS
    assert published["rank"].tolist() == list(
        range(1, len(published) + 1)
    )
    assert len(published) <= REAL_SELECTION_TOP_N


@pytest.mark.parametrize(
    "missing_artifact",
    ["daily_report", "commit", "generation"],
)
def test_goal24_missing_goal23_artifact_blocks_without_output(
    stores,
    missing_artifact,
):
    report = _target_report(stores)
    if missing_artifact == "daily_report":
        manifest = stores.control_json[stores.goal23_manifest_key]
        del stores.control_json[manifest["daily_report_keys"][END_DATE]]
    elif missing_artifact == "commit":
        del stores.goal23_commits[END_DATE]
    else:
        del stores.goal23_objects[report["output"]["object_key"]]

    result = stores.run_goal24(apply=True)

    assert result["status"] == "BLOCKED"
    assert stores.selection_objects == {}
    assert stores.selection_commits == {}
    assert stores.snapshots == {}


def test_goal24_goal23_checksum_tampering_blocks(stores):
    commit = stores.goal23_commits[END_DATE]
    object_key = commit["output"]["object_key"]
    drifted = stores.goal23_objects[object_key].copy(deep=True)
    drifted.loc[drifted.index[0], "quality_score"] += 1.0
    stores.goal23_objects[object_key] = drifted

    result = stores.run_goal24()

    assert result["status"] == "BLOCKED"
    assert "checksum mismatch" in result["date_results"][END_DATE][
        "failure"
    ]["message"]


def test_goal24_goal23_schema_and_readback_evidence_tampering_blocks(stores):
    report = _target_report(stores)
    report["output"]["read_back"]["passed"] = False

    result = stores.run_goal24()

    assert result["status"] == "BLOCKED"
    assert "output evidence" in result["date_results"][END_DATE][
        "failure"
    ]["message"]


def test_goal24_missing_goal22_lineage_blocks(stores):
    report = _target_report(stores)
    del report["goal22_input_lineage"][END_DATE]

    result = stores.run_goal24()

    assert result["status"] == "BLOCKED"
    assert stores.selection_objects == {}


def test_goal24_missing_goal22_manifest_blocks(stores):
    goal22_ref = _target_report(stores)["goal22_input_lineage"][END_DATE]
    del stores.control_json[goal22_ref["goal22_manifest_key"]]

    result = stores.run_goal24()

    assert result["status"] == "BLOCKED"
    assert "missing Goal 22 manifest" in result["date_results"][
        END_DATE
    ]["failure"]["message"]


@pytest.mark.parametrize(
    "missing_artifact",
    ["daily_report", "commit", "factor_input_generation"],
)
def test_goal24_missing_goal22_artifact_blocks(
    stores,
    missing_artifact,
):
    goal22_ref = _target_report(stores)["goal22_input_lineage"][END_DATE]
    if missing_artifact == "daily_report":
        del stores.control_json[goal22_ref["goal22_daily_report_key"]]
    elif missing_artifact == "commit":
        del stores.goal22_commits[END_DATE]
    else:
        object_key = stores.goal22_commits[END_DATE]["outputs"][
            "factor_input_table"
        ]["object_key"]
        del stores.goal22_objects[object_key]

    result = stores.run_goal24()

    assert result["status"] == "BLOCKED"


def test_goal24_goal22_generation_checksum_tampering_blocks(stores):
    committed = stores.goal22_commits[END_DATE]["outputs"]["risk_filter"]
    drifted = stores.goal22_objects[committed["object_key"]].copy(deep=True)
    drifted.loc[drifted.index[0], "amount"] += 1.0
    stores.goal22_objects[committed["object_key"]] = drifted

    result = stores.run_goal24()

    assert result["status"] == "BLOCKED"
    assert "checksum mismatch" in result["date_results"][END_DATE][
        "failure"
    ]["message"]


def test_goal24_effective_factor_count_below_15_is_blocked():
    upstream = Goal23MemoryStores([END_DATE])
    goal23 = upstream.run_goal23(apply=True)
    catalog = load_goal23_manifest_catalog(
        manifest_keys=[goal23["range_manifest_key"]],
        read_json_fn=upstream.read_control_json,
    )

    result = run_real_selection_result_range(
        run_id="goal24-low-factor-count",
        start_date=END_DATE,
        end_date=END_DATE,
        selection_dates=[END_DATE],
        rebalance_mode="monthly",
        goal23_manifest_catalog=catalog,
        selection_config=FACTOR_CONFIG,
        control_read_json_fn=upstream.read_control_json,
        control_write_json_fn=None,
        goal23_factor_object_read_fn=upstream.read_factor_object,
        goal23_commit_read_fn=upstream.read_factor_commit,
        goal22_processed_object_read_fn=(
            lambda object_key: with_goal22_arrow_schema_evidence(
                object_key,
                upstream.read_goal22_processed_object(object_key),
            )
        ),
        goal22_commit_read_fn=upstream.read_goal22_commit,
    )

    assert result["status"] == "BLOCKED"
    assert "below the required minimum" in result["date_results"][
        END_DATE
    ]["failure"]["message"]


def test_goal24_empty_trusted_chain_blocks_without_pass_empty_publication(
    stores,
):
    upstream = Goal23MemoryStores([END_DATE], empty_universe=True)
    goal23 = upstream.run_goal23(
        run_id="goal23-empty-goal24-fixture",
        apply=True,
    )
    assert goal23["status"] == "COMPLETED"
    stores.upstream = upstream
    stores.goal23_manifest_key = goal23["range_manifest_key"]

    result = stores.run_goal24(
        run_id="goal24-empty-is-blocked",
        apply=True,
    )

    assert result["status"] == "BLOCKED"
    report = result["date_results"][END_DATE]
    assert report["dq_status"] == "BLOCKED"
    assert (
        "below the required minimum" in report["failure"]["message"]
        or "Parquet/Arrow type must be" in report["failure"]["message"]
    )
    assert stores.selection_objects == {}
    assert stores.selection_commits == {}
    assert stores.snapshots == {}


def test_goal24_key_set_date_and_missing_risk_fail_closed(stores):
    factor, frames = _target_frames(stores)
    risk_missing = frames["risk_filter"].iloc[1:].reset_index(drop=True)

    with pytest.raises(ValueError, match="missing risk_filter"):
        _audit_input_key_sets(
            factor_daily=factor,
            risk_filter=risk_missing,
            eligible_universe=frames["eligible_universe"],
            factor_input_table=frames["factor_input_table"],
            trade_date=END_DATE,
        )

    explicitly_ineligible = frames["risk_filter"].copy(deep=True)
    eligible_code = frames["eligible_universe"].iloc[0]["stock_code"]
    explicitly_ineligible.loc[
        explicitly_ineligible["stock_code"] == eligible_code,
        "is_eligible",
    ] = False
    with pytest.raises(ValueError, match="not explicitly allowed"):
        _audit_input_key_sets(
            factor_daily=factor,
            risk_filter=explicitly_ineligible,
            eligible_universe=frames["eligible_universe"],
            factor_input_table=frames["factor_input_table"],
            trade_date=END_DATE,
        )

    wrong_date = frames["factor_input_table"].copy(deep=True)
    wrong_date["trade_date"] = "2026-06-18"
    with pytest.raises(ValueError, match="trade_date"):
        _audit_input_key_sets(
            factor_daily=factor,
            risk_filter=frames["risk_filter"],
            eligible_universe=frames["eligible_universe"],
            factor_input_table=wrong_date,
            trade_date=END_DATE,
        )


def test_goal24_factor_eligible_and_factor_input_keys_must_match(stores):
    factor, frames = _target_frames(stores)
    factor_input = frames["factor_input_table"].iloc[1:].reset_index(
        drop=True
    )

    with pytest.raises(ValueError, match="key sets must match"):
        _audit_input_key_sets(
            factor_daily=factor,
            risk_filter=frames["risk_filter"],
            eligible_universe=frames["eligible_universe"],
            factor_input_table=factor_input,
            trade_date=END_DATE,
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("quality_score", float("nan"), "finite"),
        ("quality_score", float("inf"), "finite"),
        ("quality_score", -0.01, "non-negative"),
        ("quality_score", 0.31, "sum must equal 1"),
        ("trend_score", 0.11, "sum must equal 1"),
    ],
)
def test_goal24_weights_are_finite_nonnegative_exact_and_sum_one(
    field,
    value,
    message,
):
    config = deepcopy(FACTOR_CONFIG)
    config[field] = value

    with pytest.raises(ValueError, match=message):
        freeze_real_selection_config(config)


def test_goal24_weights_must_match_first_version_even_when_sum_is_one():
    config = deepcopy(FACTOR_CONFIG)
    config["quality_score"] = 0.29
    config["growth_score"] = 0.26

    with pytest.raises(ValueError, match="first-version contract"):
        freeze_real_selection_config(config)


def test_goal24_weights_reject_even_microscopic_first_version_drift():
    config = deepcopy(FACTOR_CONFIG)
    config["quality_score"] += 1e-13
    config["growth_score"] -= 1e-13

    with pytest.raises(ValueError, match="first-version contract"):
        freeze_real_selection_config(config)


@pytest.mark.parametrize("top_n", [50.9, 50.0, "50", True])
def test_goal24_top_n_must_be_explicit_integer_50(top_n):
    config = deepcopy(FACTOR_CONFIG)
    config["scoring"]["top_n"] = top_n

    with pytest.raises(ValueError, match="top_n must be the integer 50"):
        freeze_real_selection_config(config)


def test_goal24_top_n_is_fixed_at_50():
    config = deepcopy(FACTOR_CONFIG)
    config["scoring"]["top_n"] = 49

    with pytest.raises(ValueError, match="top_n must be the integer 50"):
        freeze_real_selection_config(config)


def test_goal24_deterministic_top50_tie_break_and_continuous_rank(stores):
    factor, frames = _target_frames(stores)
    factor_template = factor.iloc[0].to_dict()
    eligible_template = frames["eligible_universe"].iloc[0].to_dict()
    risk_template = frames["risk_filter"].loc[
        frames["risk_filter"]["is_eligible"].map(bool)
    ].iloc[0].to_dict()
    input_template = frames["factor_input_table"].iloc[0].to_dict()
    factor_rows = []
    eligible_rows = []
    risk_rows = []
    input_rows = []
    for index in range(60, 0, -1):
        code = f"{index:06d}.SZ"
        factor_row = dict(factor_template)
        factor_row["stock_code"] = code
        for column in (
            "quality_score",
            "growth_score",
            "valuation_score",
            "industry_score",
            "trend_score",
        ):
            factor_row[column] = 80.0
        factor_rows.append(factor_row)
        eligible_rows.append(
            {**eligible_template, "stock_code": code}
        )
        risk_rows.append(
            {
                **risk_template,
                "stock_code": code,
                "is_eligible": True,
                "exclude_reasons": "",
                "risk_flags": "",
            }
        )
        input_rows.append({**input_template, "stock_code": code})

    result = build_selection_result(
        factor_daily=pd.DataFrame(factor_rows),
        risk_filter=pd.DataFrame(risk_rows),
        eligible_universe=pd.DataFrame(eligible_rows),
        factor_input_table=pd.DataFrame(input_rows),
        trade_date=END_DATE,
        scoring_config=_scoring_config(),
        allow_empty=True,
    )
    result = normalize_selection_result_frame(result)
    validate_real_selection_result(
        result,
        END_DATE,
        scoring_config=_scoring_config(),
    )

    assert len(result) == 50
    assert result["stock_code"].tolist() == [
        f"{index:06d}.SZ" for index in range(1, 51)
    ]
    assert result["rank"].tolist() == list(range(1, 51))


def test_goal24_outputs_actual_count_when_fewer_than_50(stores):
    result = stores.run_goal24()
    report = result["date_results"][END_DATE]

    assert 0 < report["counts"]["final_output_count"] < 50
    assert report["counts"]["top_n"] == 50


def test_goal24_empty_result_is_rejected_as_unreachable():
    empty = normalize_selection_result_frame(
        pd.DataFrame(columns=SELECTION_RESULT_COLUMNS)
    )

    with pytest.raises(DataValidationError, match="empty"):
        validate_real_selection_result(
            empty,
            END_DATE,
            scoring_config=_scoring_config(),
        )


def test_goal24_monthly_and_quarterly_same_date_coexist(stores):
    monthly = stores.run_goal24(
        run_id="goal24-monthly",
        rebalance_mode="monthly",
        apply=True,
    )
    quarterly = stores.run_goal24(
        run_id="goal24-quarterly",
        rebalance_mode="quarterly",
        apply=True,
    )

    assert monthly["status"] == quarterly["status"] == "COMPLETED"
    assert (END_DATE, "monthly") in stores.selection_commits
    assert (END_DATE, "quarterly") in stores.selection_commits
    assert (END_DATE, "monthly") in stores.snapshots
    assert (END_DATE, "quarterly") in stores.snapshots
    monthly_key = stores.selection_commits[
        (END_DATE, "monthly")
    ]["output"]["object_key"]
    quarterly_key = stores.selection_commits[
        (END_DATE, "quarterly")
    ]["output"]["object_key"]
    assert monthly_key != quarterly_key
    assert "rebalance_mode=monthly" in monthly_key
    assert "rebalance_mode=quarterly" in quarterly_key


def test_goal24_database_failure_resume_repairs_without_rewriting(stores):
    stores.fail_snapshot_once = (END_DATE, "monthly")

    first = stores.run_goal24(apply=True)
    object_writes = list(stores.selection_object_writes)
    commit_writes = list(stores.selection_commit_writes)
    second = stores.run_goal24(apply=True)

    assert first["status"] == "DATABASE_PENDING"
    assert (END_DATE, "monthly") in stores.selection_commits
    assert second["status"] == "COMPLETED"
    assert stores.selection_object_writes == object_writes
    assert stores.selection_commit_writes == commit_writes

    recomputed = stores.run_goal24(
        run_id="goal24-deterministic-third",
        apply=True,
        resume=False,
    )
    assert recomputed["date_results"][END_DATE][
        "resume_action"
    ] == "RECOMPUTED_REUSED_PUBLICATION"
    assert stores.selection_object_writes == object_writes
    assert stores.selection_commit_writes == commit_writes
    report = second["date_results"][END_DATE]
    assert report["resume_action"] == "REPAIRED_DATABASE_SUMMARY"
    assert report["snapshot"]["status"] == "REPAIRED"


def test_goal24_generation_failure_never_commits_or_updates_database(stores):
    stores.tamper_written_generation_once = END_DATE

    result = stores.run_goal24(apply=True)

    assert result["status"] == "FAILED"
    assert stores.selection_commits == {}
    assert stores.snapshots == {}
    report = result["date_results"][END_DATE]
    assert report["commit"]["status"] == "UNCOMMITTED"
    assert report["blocked_reasons"] == [
        "OUTPUT_APPLY_FAILED:STAGE:selection_result:DataValidationError"
    ]


def test_goal24_immutable_generation_collision_is_not_overwritten(stores):
    first = stores.run_goal24(apply=True)
    assert first["status"] == "COMPLETED"
    commit = stores.selection_commits[(END_DATE, "monthly")]
    object_key = commit["output"]["object_key"]
    corrupted = stores.selection_objects[object_key].copy(deep=True)
    corrupted.loc[corrupted.index[0], "reason"] = "被篡改"
    stores.selection_objects[object_key] = corrupted
    object_writes = list(stores.selection_object_writes)
    commit_writes = list(stores.selection_commit_writes)

    second = stores.run_goal24(apply=True, force=True)

    assert second["status"] == "BLOCKED"
    assert "canonical commit collision" in second["date_results"][
        END_DATE
    ]["failure"]["message"]
    assert stores.selection_object_writes == object_writes
    assert stores.selection_commit_writes == commit_writes
    pd.testing.assert_frame_equal(
        stores.selection_objects[object_key],
        corrupted,
    )


def test_goal24_incompatible_valid_canonical_commit_is_not_replaced(stores):
    first = stores.run_goal24(
        run_id="goal24-original-publication",
        apply=True,
    )
    assert first["status"] == "COMPLETED"
    identity = (END_DATE, "monthly")
    conflicting = _install_valid_different_lineage_commit(stores)
    stores.selection_commits[identity] = deepcopy(conflicting)
    object_writes = list(stores.selection_object_writes)
    commit_writes = list(stores.selection_commit_writes)
    snapshot_writes = list(stores.snapshot_writes)

    second = stores.run_goal24(
        run_id="goal24-collision-attempt",
        apply=True,
        force=True,
    )

    assert second["status"] == "BLOCKED"
    assert "canonical commit collision" in second["date_results"][
        END_DATE
    ]["failure"]["message"]
    assert stores.selection_commits[identity] == conflicting
    assert stores.selection_object_writes == object_writes
    assert stores.selection_commit_writes == commit_writes
    assert stores.snapshot_writes == snapshot_writes


def test_goal24_write_time_canonical_collision_is_not_overwritten(stores):
    seed = fresh_goal24_stores()
    seeded = seed.run_goal24(
        run_id="goal24-write-race-seed",
        apply=True,
    )
    assert seeded["status"] == "COMPLETED"
    conflicting = _install_valid_different_lineage_commit(
        seed,
        target_stores=stores,
    )
    identity = (END_DATE, "monthly")

    def competing_create_only_writer(
        trade_date,
        rebalance_mode,
        _payload,
    ):
        stores.selection_commits[
            (trade_date, rebalance_mode)
        ] = deepcopy(conflicting)
        raise FileExistsError("competing canonical commit won")

    stores.write_selection_commit = competing_create_only_writer

    result = stores.run_goal24(
        run_id="goal24-write-race-attempt",
        apply=True,
    )

    assert result["status"] == "BLOCKED"
    assert "create-only publication lost" in result["date_results"][
        END_DATE
    ]["failure"]["message"]
    assert stores.selection_commits[identity] == conflicting
    assert stores.selection_commit_writes == []
    assert stores.snapshots == {}


def test_goal24_write_time_compatible_winner_is_reused(stores):
    identity = (END_DATE, "monthly")

    def competing_create_only_writer(
        trade_date,
        rebalance_mode,
        payload,
    ):
        stores.selection_commits[
            (trade_date, rebalance_mode)
        ] = deepcopy(payload)
        raise FileExistsError("compatible canonical commit won")

    stores.write_selection_commit = competing_create_only_writer

    result = stores.run_goal24(
        run_id="goal24-compatible-write-race",
        apply=True,
    )

    assert result["status"] == "COMPLETED"
    report = result["date_results"][END_DATE]
    assert report["resume_action"] == "CONCURRENT_PUBLICATION_REUSED"
    assert report["commit"]["reused"] is True
    assert stores.selection_commits[identity]["run_id"] == (
        "goal24-compatible-write-race"
    )
    assert stores.selection_commit_writes == []
    assert identity in stores.snapshots


def test_goal24_same_inputs_across_runs_reuse_generation_and_commit(stores):
    first = stores.run_goal24(
        run_id="goal24-deterministic-first",
        apply=True,
    )
    assert first["status"] == "COMPLETED"
    original_commit = deepcopy(
        stores.selection_commits[(END_DATE, "monthly")]
    )
    object_writes = list(stores.selection_object_writes)
    commit_writes = list(stores.selection_commit_writes)

    second = stores.run_goal24(
        run_id="goal24-deterministic-second",
        apply=True,
    )

    assert second["status"] == "COMPLETED"
    report = second["date_results"][END_DATE]
    assert report["resume_action"] == "REUSED_COMPLETED"
    assert report["commit"]["generation_id"] == original_commit[
        "generation_id"
    ]
    assert stores.selection_commits[
        (END_DATE, "monthly")
    ] == original_commit
    assert stores.selection_object_writes == object_writes
    assert stores.selection_commit_writes == commit_writes


def test_goal24_run_id_parameter_drift_is_rejected(stores):
    stores.run_goal24(
        run_id="goal24-immutable-run",
        target_dates=[SELECTION_DATES[0]],
        apply=True,
    )

    with pytest.raises(ValueError, match="run_id scope"):
        stores.run_goal24(
            run_id="goal24-immutable-run",
            target_dates=[SELECTION_DATES[1]],
            apply=False,
        )


def test_goal24_one_date_failure_does_not_stop_other_date(stores):
    stores.fail_selection_write_once = SELECTION_DATES[0]

    result = stores.run_goal24(
        target_dates=SELECTION_DATES,
        apply=True,
    )

    assert result["status"] == "PARTIAL"
    assert result["date_statuses"] == {
        SELECTION_DATES[0]: "FAILED",
        SELECTION_DATES[1]: "COMPLETED",
    }
    assert (SELECTION_DATES[0], "monthly") not in stores.selection_commits
    assert (SELECTION_DATES[1], "monthly") in stores.selection_commits


@pytest.mark.parametrize(
    ("column", "value", "message"),
    [
        ("total_score", 1.0, "total_score"),
        ("risk_level", "high", "risk_level"),
        ("rank", 2, "rank"),
        ("reason", "任意文本", "reason"),
        ("suggestion", "任意文本", "suggestion"),
    ],
)
def test_goal24_readback_rejects_non_recomputable_fields(
    stores,
    column,
    value,
    message,
):
    stores.run_goal24(apply=True)
    commit = stores.selection_commits[(END_DATE, "monthly")]
    object_key = commit["output"]["object_key"]
    tampered = stores.selection_objects[object_key].copy(deep=True)
    tampered.loc[tampered.index[0], column] = value
    stores.selection_objects[object_key] = tampered

    with pytest.raises(DataValidationError, match=message):
        read_goal24_published_selection_result(
            trade_date=END_DATE,
            rebalance_mode="monthly",
            selection_commit_read_fn=stores.read_selection_commit,
            selection_object_read_fn=stores.read_selection_object,
        )

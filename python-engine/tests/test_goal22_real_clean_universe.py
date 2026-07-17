from __future__ import annotations

from copy import deepcopy

import pandas as pd
import pytest

from stock_selector.data.historical_backfill import dataframe_checksum
from stock_selector.data.mock_data import generate_mock_dataset
from stock_selector.data.real_clean_universe import (
    InputArtifact,
    InputVersion,
    OUTPUT_DATASETS,
    REQUIRED_INPUTS,
    build_goal22_processed_commit_key,
    build_goal22_processed_generation_key,
    build_real_clean_universe_output_keys,
    run_real_clean_universe_range,
)
from stock_selector.storage.atomic_writer import AtomicObjectWriter


TRADE_DATE = "2026-06-19"
CODES = ["000001.SZ", "000002.SZ", "600000.SH", "600519.SH", "300750.SZ"]


class MemoryStores:
    def __init__(self, inputs: dict[tuple[str, str], InputArtifact]):
        self.inputs = inputs
        trade_dates = sorted({trade_date for _dataset, trade_date in inputs})
        trusted_inputs = _valid_inputs(trade_dates)
        trusted_inputs.update(inputs)
        self.trusted_input_lineage = _trusted_input_lineage(
            trusted_inputs,
            trade_dates,
        )
        self.json: dict[str, dict] = {}
        self.processed_objects: dict[str, pd.DataFrame] = {}
        self.commits: dict[str, dict] = {}
        self.processed: dict[tuple[str, str], pd.DataFrame] = {}
        self.processed_writes: list[tuple[str, str]] = []
        self.commit_writes: list[str] = []
        self.fail_once: tuple[str, str] | None = None
        self.commit_read_fail_once: str | None = None
        self.commit_interrupt_once: str | None = None

    def read_input(self, dataset: str, trade_date: str) -> InputArtifact:
        try:
            artifact = self.inputs[(dataset, trade_date)]
        except KeyError as exc:
            raise FileNotFoundError(f"missing {dataset} for {trade_date}") from exc
        return InputArtifact(frame=artifact.frame.copy(deep=True), versions=artifact.versions)

    def read_json(self, object_key: str) -> dict:
        if object_key not in self.json:
            raise FileNotFoundError(object_key)
        return deepcopy(self.json[object_key])

    def write_json(self, object_key: str, payload: dict) -> str:
        self.json[object_key] = deepcopy(payload)
        return object_key

    def read_processed(self, dataset: str, trade_date: str) -> pd.DataFrame:
        commit = self.read_commit(trade_date)
        object_key = commit["outputs"][dataset]["object_key"]
        return self.read_processed_object(object_key)

    def read_processed_object(self, object_key: str) -> pd.DataFrame:
        try:
            return self.processed_objects[object_key].copy(deep=True)
        except KeyError as exc:
            raise FileNotFoundError(object_key) from exc

    def write_processed_object(
        self,
        dataset: str,
        trade_date: str,
        generation_id: str,
        frame: pd.DataFrame,
    ) -> str:
        if self.fail_once == (dataset, trade_date):
            self.fail_once = None
            raise RuntimeError(f"injected write failure for {dataset} {trade_date}")
        object_key = build_goal22_processed_generation_key(
            dataset,
            trade_date,
            generation_id,
        )
        self.processed_objects[object_key] = frame.copy(deep=True)
        self.processed_writes.append((dataset, trade_date))
        return object_key

    def read_commit(self, trade_date: str) -> dict:
        if self.commit_read_fail_once == trade_date:
            self.commit_read_fail_once = None
            raise OSError(f"injected commit read failure for {trade_date}")
        try:
            return deepcopy(self.commits[trade_date])
        except KeyError as exc:
            raise FileNotFoundError(build_goal22_processed_commit_key(trade_date)) from exc

    def write_commit(self, trade_date: str, payload: dict) -> str:
        if self.commit_interrupt_once == trade_date:
            self.commit_interrupt_once = None
            raise KeyboardInterrupt(f"injected commit interruption for {trade_date}")
        self.commits[trade_date] = deepcopy(payload)
        self.commit_writes.append(trade_date)
        for dataset, record in payload["outputs"].items():
            self.processed[(dataset, trade_date)] = self.read_processed_object(
                record["object_key"]
            )
        return build_goal22_processed_commit_key(trade_date)


def test_goal22_historical_membership_asof_st_pause_and_risk_filters_are_reused():
    inputs = _valid_inputs([TRADE_DATE])
    stock = inputs[("stock_basic", TRADE_DATE)].frame.copy()
    stock.loc[stock["stock_code"] == "000002.SZ", "list_date"] = "2026-06-20"
    stock.loc[stock["stock_code"] == "600000.SH", "delist_date"] = TRADE_DATE
    stock.loc[stock["stock_code"] == "600519.SH", "list_date"] = "2026-06-01"
    inputs[("stock_basic", TRADE_DATE)] = _artifact("stock_basic", TRADE_DATE, stock)

    daily = inputs[("daily_price", TRADE_DATE)].frame.copy()
    daily.loc[daily["stock_code"] == "300750.SZ", "is_paused"] = True
    daily.loc[daily["stock_code"] == "300750.SZ", "amount"] = 1_000_000
    inputs[("daily_price", TRADE_DATE)] = _artifact("daily_price", TRADE_DATE, daily)

    st_history = pd.DataFrame(
        [
            {
                "stock_code": "600519.SH",
                "st_type": "ST",
                "start_date": "2026-06-10",
                "end_date": "2026-06-20",
                "source": "historical-status",
            }
        ]
    )
    inputs[("st_history", TRADE_DATE)] = _artifact("st_history", TRADE_DATE, st_history)

    financial = inputs[("financial", TRADE_DATE)].frame.copy()
    old = financial.loc[financial["stock_code"] == "000001.SZ"].iloc[0].copy()
    financial.loc[financial["stock_code"] == "000001.SZ", "announce_date"] = "2026-05-31"
    financial.loc[financial["stock_code"] == "000001.SZ", "roe"] = 0.11
    future = old.copy()
    future["report_period"] = "2026-06-30"
    future["announce_date"] = "2026-06-21"
    future["roe"] = 0.99
    financial = pd.concat([financial, pd.DataFrame([future])], ignore_index=True)
    inputs[("financial", TRADE_DATE)] = _artifact("financial", TRADE_DATE, financial)

    stores = MemoryStores(inputs)
    result = _run(stores, trade_dates=[TRADE_DATE], apply=True)

    assert result["status"] == "COMPLETED"
    snapshot = stores.processed[("clean_daily_snapshot", TRADE_DATE)]
    assert set(snapshot["stock_code"]) == {"000001.SZ", "600519.SH", "300750.SZ"}
    assert snapshot.loc[snapshot["stock_code"] == "000001.SZ", "roe"].item() == pytest.approx(0.11)
    assert snapshot["announce_date"].dropna().max() <= TRADE_DATE

    risk_filter = stores.processed[("risk_filter", TRADE_DATE)].set_index("stock_code")
    assert "ST" in risk_filter.loc["600519.SH", "exclude_reasons"]
    assert "LISTED_DAYS_LT_MIN" in risk_filter.loc["600519.SH", "exclude_reasons"]
    assert "PAUSED" in risk_filter.loc["300750.SZ", "exclude_reasons"]
    assert "AMOUNT_LT_MIN" in risk_filter.loc["300750.SZ", "exclude_reasons"]
    assert set(stores.processed[("eligible_universe", TRADE_DATE)]["stock_code"]) == {"000001.SZ"}
    assert set(stores.processed[("factor_input_table", TRADE_DATE)]["stock_code"]) == {"000001.SZ"}
    assert set(stores.processed[("adjusted_price", TRADE_DATE)]["stock_code"]) == {
        "000001.SZ",
        "600519.SH",
        "300750.SZ",
    }

    dq = stores.json[result["daily_report_keys"][TRADE_DATE]]
    assert dq["financial_as_of"]["future_rows_excluded"] == 1
    assert dq["membership_exclusions"] == {"DELISTED": 1, "NOT_YET_LISTED": 1}
    assert dq["risk_exclusion_counts"]["ST"] == 1
    assert dq["risk_exclusion_counts"]["PAUSED"] == 1
    assert dq["downstream_firewalls"] == {
        "factor_daily": False,
        "selection_result": False,
        "backtest": False,
    }


@pytest.mark.parametrize("missing", REQUIRED_INPUTS)
def test_goal22_missing_required_input_blocks_only_that_date(missing: str):
    inputs = _valid_inputs([TRADE_DATE])
    del inputs[(missing, TRADE_DATE)]
    stores = MemoryStores(inputs)

    result = _run(stores, trade_dates=[TRADE_DATE], apply=True)

    assert result["status"] == "BLOCKED"
    assert stores.processed_writes == []
    dq = stores.json[result["daily_report_keys"][TRADE_DATE]]
    assert dq["status"] == "BLOCKED"
    assert f"MISSING_INPUT:{missing}" in dq["blocked_reasons"]


@pytest.mark.parametrize("bad_value", [0.0, -1.0])
def test_goal22_non_positive_adj_factor_blocks_date(bad_value: float):
    inputs = _valid_inputs([TRADE_DATE])
    adj = inputs[("adj_factor", TRADE_DATE)].frame.copy()
    adj.loc[adj["stock_code"] == CODES[0], "adj_factor"] = bad_value
    inputs[("adj_factor", TRADE_DATE)] = _artifact("adj_factor", TRADE_DATE, adj)
    stores = MemoryStores(inputs)

    result = _run(stores, trade_dates=[TRADE_DATE], apply=True)

    assert result["status"] == "BLOCKED"
    dq = stores.json[result["daily_report_keys"][TRADE_DATE]]
    assert any("adj_factor" in reason for reason in dq["blocked_reasons"])
    assert stores.processed_writes == []


def test_goal22_missing_adj_factor_code_coverage_blocks_date():
    inputs = _valid_inputs([TRADE_DATE])
    adj = inputs[("adj_factor", TRADE_DATE)].frame.iloc[1:].reset_index(drop=True)
    inputs[("adj_factor", TRADE_DATE)] = _artifact("adj_factor", TRADE_DATE, adj)
    stores = MemoryStores(inputs)

    result = _run(stores, trade_dates=[TRADE_DATE], apply=True)

    assert result["status"] == "BLOCKED"
    dq = stores.json[result["daily_report_keys"][TRADE_DATE]]
    assert "MISSING_CODE_COVERAGE:adj_factor:000001.SZ" in dq["blocked_reasons"]


def test_goal22_missing_financial_asof_code_coverage_blocks_date():
    inputs = _valid_inputs([TRADE_DATE])
    financial = inputs[("financial", TRADE_DATE)].frame
    financial = financial[financial["stock_code"] != CODES[0]].reset_index(drop=True)
    inputs[("financial", TRADE_DATE)] = _artifact("financial", TRADE_DATE, financial)
    stores = MemoryStores(inputs)

    result = _run(stores, trade_dates=[TRADE_DATE], apply=True)

    assert result["status"] == "BLOCKED"
    dq = stores.json[result["daily_report_keys"][TRADE_DATE]]
    assert "MISSING_CODE_COVERAGE:financial:000001.SZ" in dq["blocked_reasons"]


def test_goal22_missing_adj_factor_value_blocks_date():
    inputs = _valid_inputs([TRADE_DATE])
    adj = inputs[("adj_factor", TRADE_DATE)].frame.copy()
    adj.loc[adj["stock_code"] == CODES[0], "adj_factor"] = float("nan")
    inputs[("adj_factor", TRADE_DATE)] = _artifact("adj_factor", TRADE_DATE, adj)
    stores = MemoryStores(inputs)

    result = _run(stores, trade_dates=[TRADE_DATE], apply=True)

    assert result["status"] == "BLOCKED"
    dq = stores.json[result["daily_report_keys"][TRADE_DATE]]
    assert any("adj_factor" in reason for reason in dq["blocked_reasons"])


def test_goal22_pause_status_requires_an_explicit_historical_boolean():
    inputs = _valid_inputs([TRADE_DATE])
    daily = inputs[("daily_price", TRADE_DATE)].frame.copy()
    daily["is_paused"] = daily["is_paused"].astype(object)
    daily.loc[daily["stock_code"] == CODES[0], "is_paused"] = "False"
    inputs[("daily_price", TRADE_DATE)] = _artifact("daily_price", TRADE_DATE, daily)
    stores = MemoryStores(inputs)

    result = _run(stores, trade_dates=[TRADE_DATE], apply=True)

    assert result["status"] == "BLOCKED"
    dq = stores.json[result["daily_report_keys"][TRADE_DATE]]
    assert any("is_paused" in reason for reason in dq["blocked_reasons"])


def test_goal22_dry_run_apply_readback_and_idempotent_resume():
    stores = MemoryStores(_valid_inputs([TRADE_DATE]))

    dry_run = _run(stores, trade_dates=[TRADE_DATE], apply=False)

    assert dry_run["status"] == "READY_FOR_APPLY"
    assert dry_run["mode"] == "DRY_RUN"
    assert stores.processed_writes == []
    dry_dq = stores.json[dry_run["daily_report_keys"][TRADE_DATE]]
    assert all(item["write"]["status"] == "NOT_REQUESTED" for item in dry_dq["outputs"].values())

    applied = _run(stores, trade_dates=[TRADE_DATE], apply=True)

    assert applied["status"] == "COMPLETED"
    assert len(stores.processed_writes) == len(OUTPUT_DATASETS)
    applied_dq = stores.json[applied["daily_report_keys"][TRADE_DATE]]
    assert all(item["read_back"]["passed"] for item in applied_dq["outputs"].values())
    assert all(item["object_key"].startswith("processed/") for item in applied_dq["outputs"].values())
    first_write_count = len(stores.processed_writes)

    rerun = _run(stores, trade_dates=[TRADE_DATE], apply=True)

    assert rerun["status"] == "COMPLETED"
    assert len(stores.processed_writes) == first_write_count
    rerun_dq = stores.json[rerun["daily_report_keys"][TRADE_DATE]]
    assert rerun_dq["resume_action"] == "REUSED_COMPLETED"
    assert all(item["write"]["status"] == "UNCHANGED" for item in rerun_dq["outputs"].values())


def test_goal22_single_day_failure_isolated_and_recoverable():
    trade_dates = ["2026-06-17", "2026-06-18", "2026-06-19"]
    stores = MemoryStores(_valid_inputs(trade_dates))
    stores.fail_once = ("clean_daily_snapshot", trade_dates[1])

    first = _run(stores, trade_dates=trade_dates, apply=True)

    assert first["status"] == "PARTIAL"
    assert first["date_statuses"] == {
        trade_dates[0]: "COMPLETED",
        trade_dates[1]: "FAILED",
        trade_dates[2]: "COMPLETED",
    }
    for dataset in OUTPUT_DATASETS:
        assert (dataset, trade_dates[0]) in stores.processed
        assert (dataset, trade_dates[1]) not in stores.processed
        assert (dataset, trade_dates[2]) in stores.processed
    assert trade_dates[1] not in stores.commits
    writes_after_first = list(stores.processed_writes)

    recovered = _run(stores, trade_dates=trade_dates, apply=True)

    assert recovered["status"] == "COMPLETED"
    assert recovered["date_statuses"] == {trade_date: "COMPLETED" for trade_date in trade_dates}
    new_writes = stores.processed_writes[len(writes_after_first) :]
    assert new_writes
    assert {trade_date for _dataset, trade_date in new_writes} == {trade_dates[1]}
    for dataset in OUTPUT_DATASETS:
        assert (dataset, trade_dates[1]) in stores.processed


def test_goal22_resume_commit_read_failure_is_isolated_to_one_date():
    trade_dates = ["2026-06-17", "2026-06-18", "2026-06-19"]
    stores = MemoryStores(_valid_inputs(trade_dates))
    assert _run(stores, trade_dates=trade_dates, apply=True)["status"] == "COMPLETED"
    stores.commit_read_fail_once = trade_dates[1]

    result = _run(stores, trade_dates=trade_dates, apply=True)

    assert result["status"] == "PARTIAL"
    assert result["date_statuses"] == {
        trade_dates[0]: "COMPLETED",
        trade_dates[1]: "FAILED",
        trade_dates[2]: "COMPLETED",
    }
    failed = stores.json[result["daily_report_keys"][trade_dates[1]]]
    assert failed["failure"]["type"] == "OSError"
    assert failed["commit"]["status"] == "UNCOMMITTED"


def test_goal22_hard_interrupt_replaces_stale_completed_daily_report_with_running():
    stores = MemoryStores(_valid_inputs([TRADE_DATE]))
    completed = _run(stores, trade_dates=[TRADE_DATE], apply=True)
    daily_key = completed["daily_report_keys"][TRADE_DATE]
    assert stores.json[daily_key]["status"] == "COMPLETED"
    stores.commit_interrupt_once = TRADE_DATE

    with pytest.raises(KeyboardInterrupt):
        run_real_clean_universe_range(
            run_id="goal22-test-run",
            start_date=TRADE_DATE,
            end_date=TRADE_DATE,
            trade_dates=[TRADE_DATE],
            trusted_input_lineage=_slice_trusted_input_lineage(
                stores.trusted_input_lineage,
                [TRADE_DATE],
            ),
            input_read_fn=stores.read_input,
            artifact_read_json_fn=stores.read_json,
            artifact_write_json_fn=stores.write_json,
            processed_read_fn=stores.read_processed,
            processed_object_read_fn=stores.read_processed_object,
            processed_object_write_fn=stores.write_processed_object,
            processed_commit_read_fn=stores.read_commit,
            processed_commit_write_fn=stores.write_commit,
            apply_processed_write=True,
            resume=True,
            force=True,
            generated_at_fn=lambda: "2026-06-20T00:00:00+00:00",
        )

    assert stores.json[daily_key]["status"] == "RUNNING"


def test_goal22_new_attempt_invalidates_later_stale_daily_reports_before_apply():
    trade_dates = ["2026-06-17", "2026-06-18", "2026-06-19"]
    stores = MemoryStores(_valid_inputs(trade_dates))
    completed = _run(stores, trade_dates=trade_dates, apply=True)
    assert all(
        stores.json[completed["daily_report_keys"][trade_date]]["status"]
        == "COMPLETED"
        for trade_date in trade_dates
    )
    stores.commit_interrupt_once = trade_dates[0]

    with pytest.raises(KeyboardInterrupt):
        run_real_clean_universe_range(
            run_id="goal22-test-run",
            start_date=trade_dates[0],
            end_date=trade_dates[-1],
            trade_dates=trade_dates,
            trusted_input_lineage=_slice_trusted_input_lineage(
                stores.trusted_input_lineage,
                trade_dates,
            ),
            input_read_fn=stores.read_input,
            artifact_read_json_fn=stores.read_json,
            artifact_write_json_fn=stores.write_json,
            processed_read_fn=stores.read_processed,
            processed_object_read_fn=stores.read_processed_object,
            processed_object_write_fn=stores.write_processed_object,
            processed_commit_read_fn=stores.read_commit,
            processed_commit_write_fn=stores.write_commit,
            apply_processed_write=True,
            resume=True,
            force=True,
            generated_at_fn=lambda: "2026-06-20T00:00:00+00:00",
        )

    assert (
        stores.json[completed["daily_report_keys"][trade_dates[0]]]["status"]
        == "RUNNING"
    )
    for trade_date in trade_dates[1:]:
        assert (
            stores.json[completed["daily_report_keys"][trade_date]]["status"]
            == "PENDING"
        )


def test_goal22_schema_complete_empty_eligible_universe_is_valid():
    inputs = _valid_inputs([TRADE_DATE])
    daily = inputs[("daily_price", TRADE_DATE)].frame.copy()
    daily["amount"] = 1.0
    inputs[("daily_price", TRADE_DATE)] = _artifact(
        "daily_price",
        TRADE_DATE,
        daily,
    )
    stores = MemoryStores(inputs)

    result = _run(stores, trade_dates=[TRADE_DATE], apply=True)

    assert result["status"] == "COMPLETED"
    assert stores.processed[("risk_filter", TRADE_DATE)]["is_eligible"].eq(False).all()
    assert stores.processed[("eligible_universe", TRADE_DATE)].empty
    assert stores.processed[("factor_input_table", TRADE_DATE)].empty
    dq = stores.json[result["daily_report_keys"][TRADE_DATE]]
    assert dq["outputs"]["eligible_universe"]["row_count"] == 0
    assert dq["outputs"]["factor_input_table"]["row_count"] == 0
    assert dq["outputs"]["eligible_universe"]["read_back"]["passed"] is True
    assert dq["outputs"]["factor_input_table"]["read_back"]["passed"] is True


def test_goal22_run_id_scope_is_immutable():
    stores = MemoryStores(_valid_inputs(["2026-06-18", "2026-06-19"]))
    _run(stores, trade_dates=["2026-06-18"], apply=False)

    with pytest.raises(ValueError, match="run_id scope"):
        _run(stores, trade_dates=["2026-06-19"], apply=False)


def _run(stores: MemoryStores, *, trade_dates: list[str], apply: bool) -> dict:
    return run_real_clean_universe_range(
        run_id="goal22-test-run",
        start_date=trade_dates[0],
        end_date=trade_dates[-1],
        trade_dates=trade_dates,
        trusted_input_lineage=_slice_trusted_input_lineage(
            stores.trusted_input_lineage,
            trade_dates,
        ),
        input_read_fn=stores.read_input,
        artifact_read_json_fn=stores.read_json,
        artifact_write_json_fn=stores.write_json,
        processed_read_fn=stores.read_processed if apply else None,
        processed_object_read_fn=stores.read_processed_object if apply else None,
        processed_object_write_fn=stores.write_processed_object if apply else None,
        processed_commit_read_fn=stores.read_commit if apply else None,
        processed_commit_write_fn=stores.write_commit if apply else None,
        apply_processed_write=apply,
        resume=True,
        generated_at_fn=lambda: "2026-06-20T00:00:00+00:00",
    )


def _valid_inputs(trade_dates: list[str]) -> dict[tuple[str, str], InputArtifact]:
    result: dict[tuple[str, str], InputArtifact] = {}
    for trade_date in trade_dates:
        for dataset in REQUIRED_INPUTS:
            if dataset == "st_history":
                frame = pd.DataFrame(columns=["stock_code", "st_type", "start_date", "end_date", "source"])
            else:
                frame = generate_mock_dataset(dataset, trade_date)
                if "stock_code" in frame.columns:
                    frame = frame[frame["stock_code"].isin(CODES)].reset_index(drop=True)
            if dataset == "daily_price":
                frame["amount"] = 100_000_000
            if dataset == "financial":
                frame["roe"] = 0.10
                frame["debt_ratio"] = 0.40
            result[(dataset, trade_date)] = _artifact(dataset, trade_date, frame)
    return result


def _artifact(dataset: str, trade_date: str, frame: pd.DataFrame) -> InputArtifact:
    object_key = f"raw/{dataset}/trade_date={trade_date}/part.parquet"
    version = InputVersion(
        object_key=object_key,
        row_count=len(frame),
        checksum=dataframe_checksum(frame),
    )
    return InputArtifact(frame=frame.copy(deep=True), versions=(version,))


def _trusted_input_lineage(
    inputs: dict[tuple[str, str], InputArtifact],
    trade_dates: list[str],
) -> dict:
    codes = sorted(
        set(inputs[("stock_basic", trade_dates[0])].frame["stock_code"].astype(str))
    )
    canonical_versions = {}
    for trade_date in trade_dates:
        canonical_versions[trade_date] = {}
        for dataset in REQUIRED_INPUTS:
            artifact = inputs[(dataset, trade_date)]
            version = artifact.versions[0]
            canonical_versions[trade_date][dataset] = {
                "object_key": version.object_key,
                "object_row_count": version.row_count,
                "object_checksum": version.checksum,
                "scope_row_count": len(artifact.frame),
                "scope_checksum": dataframe_checksum(artifact.frame),
            }
    return {
        "schema_version": "goal22.trusted_input_lineage.v1",
        "trade_dates": list(trade_dates),
        "codes": codes,
        "readiness_receipts": [
            {
                "batch_id": "goal22-test-receipt",
                "readiness_report_key": (
                    "candidate/real_clean_inputs/readiness_report/"
                    "batch_id=goal22-test-receipt/report.json"
                ),
                "readiness_report_checksum": "a" * 64,
                "manifest_key": (
                    "candidate/real_clean_inputs/manifest/"
                    "batch_id=goal22-test-receipt/manifest.json"
                ),
                "manifest_checksum": "b" * 64,
            }
        ],
        "canonical_versions": canonical_versions,
    }


def _slice_trusted_input_lineage(lineage: dict, trade_dates: list[str]) -> dict:
    result = deepcopy(lineage)
    result["trade_dates"] = list(trade_dates)
    result["canonical_versions"] = {
        trade_date: deepcopy(lineage["canonical_versions"][trade_date])
        for trade_date in trade_dates
    }
    return result


def test_goal22_output_key_contract_is_stable():
    keys = build_real_clean_universe_output_keys("run-22", [TRADE_DATE])

    assert keys["range_manifest"] == "candidate/real_clean_universe/run_id=run-22/manifest.json"
    assert keys["daily_reports"][TRADE_DATE].endswith(f"trade_date={TRADE_DATE}/dq_report.json")
    assert keys["processed"][TRADE_DATE]["factor_input_table"] == (
        f"processed/factor_input_table/trade_date={TRADE_DATE}/part.parquet"
    )
    assert keys["processed_commits"][TRADE_DATE] == (
        f"processed/_goal22_commits/trade_date={TRADE_DATE}/commit.json"
    )
    generation_key = build_goal22_processed_generation_key(
        "factor_input_table",
        TRADE_DATE,
        "a" * 64,
    )
    assert generation_key == (
        f"processed/factor_input_table/trade_date={TRADE_DATE}/"
        f"generation={'a' * 64}/part.parquet"
    )
    assert AtomicObjectWriter._temp_key_for(
        generation_key
    ).startswith(f"_processed_tmp/factor_input_table/trade_date={TRADE_DATE}/")

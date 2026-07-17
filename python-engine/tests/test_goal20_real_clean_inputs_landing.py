import json

import pandas as pd
import pytest

from stock_selector.cli import main
from stock_selector.data.mock_data import generate_mock_dataset
from stock_selector.data.real_clean_input_gate import (
    load_goal22_trusted_input_lineage,
    readiness_payload_checksum,
)


REQUIRED_INPUTS = {
    "stock_basic",
    "daily_price",
    "adj_factor",
    "daily_basic",
    "financial",
    "st_history",
    "benchmark_price",
}
TRADE_DATES = ["2024-06-03", "2024-06-04"]
CODES = ["000001.SZ", "600519.SH"]
BATCH_ID = "goal20-test-batch"


def test_goal20_default_is_dry_run_and_reports_all_required_inputs(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))

    exit_code = main(_goal20_args())

    assert exit_code == 1
    output = json.loads(capsys.readouterr().out)
    assert output["goal"] == "20"
    assert output["mode"] == "DRY_RUN"
    assert output["provider_call_requested"] is False
    assert output["apply_requested"] is False
    assert output["ready_for_clean"] is False
    assert (tmp_path / output["readiness_report_key"]).exists()

    report = json.loads((tmp_path / output["readiness_report_key"]).read_text(encoding="utf-8"))
    assert set(report["inputs"]) == REQUIRED_INPUTS
    for status in report["inputs"].values():
        assert {
            "source_keys",
            "row_count",
            "dq_level",
            "coverage",
            "validation",
            "write",
            "read_back",
            "blocked_reasons",
        } <= set(status)


def test_goal20_default_does_not_construct_external_providers(monkeypatch, tmp_path, capsys):
    import stock_selector.cli as cli_module

    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(cli_module, "TushareProvider", _provider_must_not_be_constructed)
    monkeypatch.setattr(cli_module, "AKShareProvider", _provider_must_not_be_constructed)

    assert main(_goal20_args()) == 1

    output = json.loads(capsys.readouterr().out)
    assert output["provider_call_requested"] is False
    assert output["status"] == "BLOCKED"


def test_goal20_cli_defaults_do_not_exceed_the_default_code_limit(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))

    exit_code = main(
        [
            "run-real-clean-inputs-small-batch",
            "--batch-id",
            BATCH_ID,
            "--start-date",
            TRADE_DATES[0],
            "--end-date",
            TRADE_DATES[-1],
            "--sleep-seconds",
            "0",
        ]
    )

    assert exit_code == 1
    output = json.loads(capsys.readouterr().out)
    assert output["goal"] == "20"
    assert output["status"] == "BLOCKED"


def test_goal20_valid_dry_run_never_writes_canonical_data(monkeypatch, tmp_path, capsys):
    import stock_selector.cli as cli_module

    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_valid_goal20_sources(tmp_path)
    monkeypatch.setattr(cli_module, "_write_dataset", _canonical_write_must_not_be_called)

    assert main(_goal20_args()) == 0

    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "READY_FOR_APPLY"
    assert output["apply_requested"] is False


def test_goal20_provider_call_fetches_only_after_explicit_opt_in(monkeypatch, tmp_path, capsys):
    import stock_selector.cli as cli_module

    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_valid_goal20_sources(tmp_path, missing="adj_factor")
    for trade_date in TRADE_DATES:
        (tmp_path / _goal20_staging_key("benchmark_price", trade_date)).unlink()
    calls = {"adj_factor": [], "benchmark_price": []}
    monkeypatch.setattr(cli_module, "TushareProvider", lambda settings=None: _FakeAdjProvider(calls))
    monkeypatch.setattr(cli_module, "AKShareProvider", lambda settings=None: _FakeBenchmarkProvider(calls))

    assert main(_goal20_args("--provider-call")) == 0

    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "READY_FOR_APPLY"
    assert output["provider_call_requested"] is True
    assert calls == {"adj_factor": TRADE_DATES, "benchmark_price": TRADE_DATES}
    for trade_date in TRADE_DATES:
        assert (tmp_path / _goal20_staging_key("adj_factor", trade_date)).exists()
        assert (tmp_path / _goal20_staging_key("benchmark_price", trade_date)).exists()
    assert not (tmp_path / "raw" / "adj_factor").exists()
    assert not (tmp_path / "raw" / "benchmark_price").exists()


@pytest.mark.parametrize("missing", sorted(REQUIRED_INPUTS))
def test_goal20_any_missing_required_input_keeps_readiness_false(monkeypatch, tmp_path, capsys, missing):
    import stock_selector.cli as cli_module

    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_valid_goal20_sources(tmp_path, missing=missing)
    monkeypatch.setattr(cli_module, "_write_dataset", _canonical_write_must_not_be_called)

    assert main(_goal20_args("--apply")) == 1

    output = json.loads(capsys.readouterr().out)
    report = _read_report(tmp_path, output["readiness_report_key"])
    assert output["ready_for_clean"] is False
    assert report["ready_for_apply"] is False
    assert report["inputs"][missing]["validation"]["passed"] is False
    assert report["inputs"][missing]["blocked_reasons"]


@pytest.mark.parametrize("bad_value", [0.0, -1.0])
def test_goal20_non_positive_adj_factor_blocks_apply(monkeypatch, tmp_path, capsys, bad_value):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_valid_goal20_sources(tmp_path, adj_factor=bad_value)

    assert main(_goal20_args("--apply")) == 1

    output = json.loads(capsys.readouterr().out)
    report = _read_report(tmp_path, output["readiness_report_key"])
    assert "ADJ_FACTOR_NON_POSITIVE" in report["inputs"]["adj_factor"]["blocked_reasons"]
    assert output["ready_for_clean"] is False
    assert not (tmp_path / "raw" / "adj_factor").exists()


def test_goal20_benchmark_requires_all_three_indexes(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_valid_goal20_sources(tmp_path, missing_benchmark="000906.SH")

    assert main(_goal20_args("--apply")) == 1

    output = json.loads(capsys.readouterr().out)
    report = _read_report(tmp_path, output["readiness_report_key"])
    benchmark = report["inputs"]["benchmark_price"]
    assert "BENCHMARK_INDEX_COVERAGE_INCOMPLETE" in benchmark["blocked_reasons"]
    assert benchmark["coverage"]["required_indexes"] == ["000300.SH", "000905.SH", "000906.SH"]
    assert not (tmp_path / "raw" / "benchmark_price").exists()


def test_goal20_smoke_only_benchmark_is_not_accepted_as_history(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_valid_goal20_sources(tmp_path, missing="benchmark_price")
    for trade_date in TRADE_DATES:
        _write_frame(
            tmp_path,
            f"smoke/akshare/benchmark_price/trade_date={trade_date}/part.parquet",
            generate_mock_dataset("benchmark_price", trade_date),
        )

    assert main(_goal20_args("--apply")) == 1

    output = json.loads(capsys.readouterr().out)
    report = _read_report(tmp_path, output["readiness_report_key"])
    benchmark = report["inputs"]["benchmark_price"]
    assert benchmark["ready_for_apply"] is False
    assert all(not key.startswith("smoke/") for key in benchmark["source_keys"])
    assert not (tmp_path / "raw" / "benchmark_price").exists()


def test_goal20_current_goal18_stock_and_st_snapshots_cannot_be_historical(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_valid_goal20_sources(tmp_path, missing="stock_basic")
    _remove_goal20_staging(tmp_path, "st_history")
    _write_goal18_current_snapshot_staging(tmp_path)

    assert main(_goal20_args("--apply", "--reuse-existing-staging")) == 1

    output = json.loads(capsys.readouterr().out)
    report = _read_report(tmp_path, output["readiness_report_key"])
    stock = report["inputs"]["stock_basic"]
    st = report["inputs"]["st_history"]
    assert stock["dq_level"] == "DQ2_CURRENT_SNAPSHOT_ONLY"
    assert "CURRENT_SNAPSHOT_NOT_HISTORICAL" in stock["blocked_reasons"]
    assert st["dq_level"] == "DQ2_CURRENT_SNAPSHOT_ONLY"
    assert "ST_STATUS_NOT_HISTORICAL" in st["blocked_reasons"]
    assert not (tmp_path / "raw" / "stock_basic").exists()
    assert not (tmp_path / "raw" / "st_history").exists()


def test_goal20_future_financial_report_period_blocks_all_goal20_writes(monkeypatch, tmp_path, capsys):
    import stock_selector.cli as cli_module

    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_valid_goal20_sources(tmp_path)
    for trade_date in TRADE_DATES:
        key = _raw_key("financial", trade_date)
        frame = pd.read_parquet(tmp_path / key)
        frame["report_period"] = "2025-03-31"
        _write_frame(tmp_path, key, frame)
    monkeypatch.setattr(cli_module, "_write_dataset", _canonical_write_must_not_be_called)

    assert main(_goal20_args("--apply")) == 1

    output = json.loads(capsys.readouterr().out)
    report = _read_report(tmp_path, output["readiness_report_key"])
    assert "FINANCIAL_FUTURE_REPORT_PERIOD" in report["inputs"]["financial"]["blocked_reasons"]


def test_goal20_unreadable_stock_history_evidence_is_blocked(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_valid_goal20_sources(tmp_path)
    (tmp_path / f"evidence/vendor/stock_basic/as_of={TRADE_DATES[0]}/part.parquet").unlink()

    assert main(_goal20_args("--apply")) == 1

    output = json.loads(capsys.readouterr().out)
    report = _read_report(tmp_path, output["readiness_report_key"])
    assert report["inputs"]["stock_basic"]["validation"]["passed"] is False
    assert report["inputs"]["stock_basic"]["blocked_reasons"]
    assert not (tmp_path / "raw" / "stock_basic").exists()


def test_goal20_verified_empty_st_history_means_all_clear_without_fake_rows(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_valid_goal20_sources(tmp_path)
    evidence_key = "evidence/vendor/st_history/all-clear-2023-01-01_2024-06-04.parquet"
    empty_history = pd.DataFrame(columns=["stock_code", "st_type", "start_date", "end_date", "source"])
    _write_frame(tmp_path, evidence_key, empty_history)
    _write_frame(tmp_path, _goal20_staging_key("st_history"), empty_history)
    _write_json(
        tmp_path,
        _goal20_st_coverage_key(),
        {
            "schema_version": "goal20.st_history_coverage.v1",
            "source_semantics": "HISTORICAL_INTERVAL_SOURCE",
            "source_object_keys": [evidence_key],
            "coverage_codes": CODES,
            "coverage_start_date": "2023-01-01",
            "coverage_end_date": TRADE_DATES[-1],
            "coverage_complete": True,
            "interval_row_count": 0,
        },
    )

    assert main(_goal20_args("--apply")) == 0

    output = json.loads(capsys.readouterr().out)
    report = _read_report(tmp_path, output["readiness_report_key"])
    status = report["inputs"]["st_history"]
    assert status["row_count"] == 0
    assert status["validation"]["passed"] is True
    assert status["read_back"]["passed"] is True
    assert status["source_keys"] == [
        _goal20_staging_key("st_history"),
        _goal20_st_coverage_key(),
        evidence_key,
    ]
    assert report["ready_for_clean"] is True
    for trade_date in TRADE_DATES:
        stored = pd.read_parquet(tmp_path / _raw_key("st_history", trade_date))
        assert stored.empty
    lineage = load_goal22_trusted_input_lineage(
        readiness_report_keys=[output["readiness_report_key"]],
        start_date=TRADE_DATES[0],
        end_date=TRADE_DATES[-1],
        trade_dates=TRADE_DATES,
        read_json_fn=lambda key: _read_report(tmp_path, key),
        read_parquet_fn=lambda key: pd.read_parquet(tmp_path / key),
    )
    assert all(
        lineage["canonical_versions"][trade_date]["st_history"][
            "scope_row_count"
        ]
        == 0
        for trade_date in TRADE_DATES
    )


def test_goal20_valid_apply_writes_and_reads_back(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_valid_goal20_sources(tmp_path)

    assert main(_goal20_args("--apply")) == 0

    output = json.loads(capsys.readouterr().out)
    report = _read_report(tmp_path, output["readiness_report_key"])
    assert output["status"] == "READY"
    assert output["ready_for_clean"] is True
    assert report["ready_for_apply"] is True
    assert report["ready_for_clean"] is True
    assert report["read_back_verification"]["passed"] is True
    manifest = _read_report(tmp_path, output["manifest_key"])
    assert manifest["readiness_report_checksum"] == readiness_payload_checksum(report)
    for dataset in REQUIRED_INPUTS:
        assert report["inputs"][dataset]["validation"]["passed"] is True
        assert report["inputs"][dataset]["read_back"]["passed"] is True
        assert all(
            detail["object_checksum"] and detail["scope_checksum"]
            for detail in report["inputs"][dataset]["read_back"]["details"]
        )
    for trade_date in TRADE_DATES:
        for dataset in ["stock_basic", "adj_factor", "st_history", "benchmark_price"]:
            assert (tmp_path / "raw" / dataset / f"trade_date={trade_date}" / "part.parquet").exists()
        stock = pd.read_parquet(tmp_path / _raw_key("stock_basic", trade_date))
        assert set(stock["list_date"].astype(str)) == {"2010-01-01"}
        assert stock.loc[stock["stock_code"] == "600519.SH", "delist_date"].iloc[0] == "2025-01-01"

    lineage = load_goal22_trusted_input_lineage(
        readiness_report_keys=[output["readiness_report_key"]],
        start_date=TRADE_DATES[0],
        end_date=TRADE_DATES[-1],
        trade_dates=TRADE_DATES,
        read_json_fn=lambda key: _read_report(tmp_path, key),
        read_parquet_fn=lambda key: pd.read_parquet(tmp_path / key),
    )
    assert lineage["trade_dates"] == TRADE_DATES
    assert lineage["codes"] == sorted(CODES)


def test_goal20_verified_receipt_closes_goal22_cli_trust_gate(
    monkeypatch,
    tmp_path,
    capsys,
):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_valid_goal20_sources(tmp_path)
    assert main(_goal20_args("--apply")) == 0
    goal20_output = json.loads(capsys.readouterr().out)

    assert main(
        [
            "run-real-clean-universe-range",
            "--run-id",
            "goal20-to-goal22-integration",
            "--start-date",
            TRADE_DATES[0],
            "--end-date",
            TRADE_DATES[-1],
            "--trade-dates",
            ",".join(TRADE_DATES),
            "--readiness-report-key",
            goal20_output["readiness_report_key"],
        ]
    ) == 0

    goal22_output = json.loads(capsys.readouterr().out)
    assert goal22_output["status"] == "READY_FOR_APPLY"
    assert goal22_output["date_statuses"] == {
        trade_date: "READY_FOR_APPLY" for trade_date in TRADE_DATES
    }


def test_goal20_goal13_adj_reuse_requires_and_records_manifest(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_valid_goal20_sources(tmp_path, missing="adj_factor")
    manifest_key = _write_goal13_adj_sources(tmp_path)

    assert main(_goal20_args("--reuse-existing-staging")) == 0

    output = json.loads(capsys.readouterr().out)
    report = _read_report(tmp_path, output["readiness_report_key"])
    adj = report["inputs"]["adj_factor"]
    assert adj["ready_for_apply"] is True
    assert adj["lineage"]["goal13_manifest_key"] == manifest_key
    assert manifest_key in adj["source_keys"]


@pytest.mark.parametrize(
    "manifest_override",
    [
        {"batch_id": "wrong-batch"},
        {"codes": [CODES[0]]},
        {"trade_dates": [TRADE_DATES[0]]},
        {"interfaces_succeeded": []},
    ],
)
def test_goal20_rejects_goal13_adj_staging_with_invalid_manifest(
    monkeypatch, tmp_path, capsys, manifest_override
):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_valid_goal20_sources(tmp_path, missing="adj_factor")
    _write_goal13_adj_sources(tmp_path, manifest_override=manifest_override)

    assert main(_goal20_args("--reuse-existing-staging", "--apply")) == 1

    output = json.loads(capsys.readouterr().out)
    report = _read_report(tmp_path, output["readiness_report_key"])
    assert "GOAL13_ADJ_FACTOR_MANIFEST_INVALID" in report["inputs"]["adj_factor"]["blocked_reasons"]
    assert not (tmp_path / "raw" / "stock_basic").exists()


def test_goal20_rejects_goal13_adj_staging_without_manifest(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_valid_goal20_sources(tmp_path, missing="adj_factor")
    _write_goal13_adj_sources(tmp_path, write_manifest=False)

    assert main(_goal20_args("--reuse-existing-staging", "--apply")) == 1

    output = json.loads(capsys.readouterr().out)
    report = _read_report(tmp_path, output["readiness_report_key"])
    assert "GOAL13_ADJ_FACTOR_MANIFEST_MISSING" in report["inputs"]["adj_factor"]["blocked_reasons"]
    assert not (tmp_path / "raw" / "stock_basic").exists()


def test_goal20_ignores_unselected_stale_goal13_manifest(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_valid_goal20_sources(tmp_path)
    _write_goal13_adj_sources(tmp_path, manifest_override={"batch_id": "stale-wrong-batch"})

    assert main(_goal20_args("--reuse-existing-staging")) == 0

    output = json.loads(capsys.readouterr().out)
    report = _read_report(tmp_path, output["readiness_report_key"])
    adj = report["inputs"]["adj_factor"]
    assert adj["ready_for_apply"] is True
    assert "lineage" not in adj


def test_goal20_does_not_read_unselected_malformed_goal13_manifest(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_valid_goal20_sources(tmp_path)
    manifest_path = tmp_path / f"candidate/tushare/batch_manifest/batch_id={BATCH_ID}/manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text("{malformed-json", encoding="utf-8")

    assert main(_goal20_args("--reuse-existing-staging")) == 0

    output = json.loads(capsys.readouterr().out)
    report = _read_report(tmp_path, output["readiness_report_key"])
    assert report["inputs"]["adj_factor"]["ready_for_apply"] is True
    assert "GOAL13_MANIFEST_READ_FAILED" not in report["inputs"]["adj_factor"]["blocked_reasons"]


def test_goal20_repeated_apply_is_idempotent(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_valid_goal20_sources(tmp_path)

    assert main(_goal20_args("--apply")) == 0
    capsys.readouterr()
    assert main(_goal20_args("--apply")) == 0

    output = json.loads(capsys.readouterr().out)
    for dataset in ["stock_basic", "adj_factor", "st_history", "benchmark_price"]:
        summary = output["upsert_summary"][dataset]
        assert summary["inserted_rows"] == 0
        assert summary["updated_rows"] == 0
        assert summary["unchanged_rows"] > 0


def test_goal20_failed_readback_keeps_readiness_false(monkeypatch, tmp_path, capsys):
    import stock_selector.cli as cli_module

    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_valid_goal20_sources(tmp_path)
    original_write = cli_module._write_dataset

    def corrupt_benchmark_write(dataset, trade_date, frame):
        if dataset == "benchmark_price":
            frame = frame[frame["index_code"] != "000906.SH"].reset_index(drop=True)
        return original_write(dataset, trade_date, frame)

    monkeypatch.setattr(cli_module, "_write_dataset", corrupt_benchmark_write)

    assert main(_goal20_args("--apply")) == 1

    output = json.loads(capsys.readouterr().out)
    report = _read_report(tmp_path, output["readiness_report_key"])
    assert output["status"] == "BLOCKED"
    assert output["ready_for_clean"] is False
    assert report["read_back_verification"]["passed"] is False
    assert "READ_BACK_VERIFICATION_FAILED" in report["blocked_reasons"]


def test_goal20_partial_write_failure_is_reported_truthfully(monkeypatch, tmp_path, capsys):
    import stock_selector.cli as cli_module

    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_valid_goal20_sources(tmp_path)
    original_write = cli_module._write_dataset

    def fail_second_stock_partition(dataset, trade_date, frame):
        if dataset == "stock_basic" and trade_date == TRADE_DATES[1]:
            raise RuntimeError("injected second-partition write failure")
        return original_write(dataset, trade_date, frame)

    monkeypatch.setattr(cli_module, "_write_dataset", fail_second_stock_partition)

    assert main(_goal20_args("--apply")) == 1

    output = json.loads(capsys.readouterr().out)
    report = _read_report(tmp_path, output["readiness_report_key"])
    assert output["standard_writes_performed"] is True
    assert report["standard_writes_performed"] is True
    assert report["inputs"]["stock_basic"]["write"]["status"] == "FAILED"
    assert report["inputs"]["adj_factor"]["write"]["status"] == "SKIPPED_AFTER_WRITE_FAILURE"
    assert report["read_back_verification"]["status"] == "NOT_RUN"
    assert output["ready_for_clean"] is False
    assert (tmp_path / _raw_key("stock_basic", TRADE_DATES[0])).exists()


def test_goal20_downstream_firewalls_remain_closed(monkeypatch, tmp_path, capsys):
    import stock_selector.cli as cli_module

    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    for name in [
        "build_adjusted_price_for_date",
        "build_clean_snapshot_for_date",
        "build_universe_inputs_for_date",
        "build_factor_daily_for_date",
        "build_selection_for_date",
        "run_backtest",
    ]:
        monkeypatch.setattr(cli_module, name, _downstream_must_not_be_called)
    _write_valid_goal20_sources(tmp_path)

    assert main(_goal20_args("--apply")) == 0

    output = json.loads(capsys.readouterr().out)
    report = _read_report(tmp_path, output["readiness_report_key"])
    assert report["downstream_firewalls"] == {
        "adjusted_price_entered": False,
        "clean_daily_snapshot_entered": False,
        "universe_entered": False,
        "factor_entered": False,
        "selection_entered": False,
        "backtest_entered": False,
    }
    assert output["clean_factor_selection_backtest_entered"] is False


def _goal20_args(*extra: str) -> list[str]:
    args = [
        "run-real-clean-inputs-small-batch",
        "--batch-id",
        BATCH_ID,
        "--codes",
        ",".join(CODES),
        "--start-date",
        "2024-06-03",
        "--end-date",
        "2024-06-04",
        "--max-codes",
        "5",
        "--max-trade-days",
        "5",
        "--max-rows",
        "100",
        "--sleep-seconds",
        "0",
    ]
    args.extend(extra)
    return args


def _write_valid_goal20_sources(root, *, missing=None, adj_factor=1.0, missing_benchmark=None):
    for trade_date in TRADE_DATES:
        for dataset in ["daily_price", "daily_basic", "financial"]:
            if missing == dataset:
                continue
            frame = generate_mock_dataset(dataset, trade_date)
            frame = frame[frame["stock_code"].isin(CODES)].reset_index(drop=True)
            if dataset == "financial":
                frame["report_period"] = "2024-03-31"
            _write_frame(root, _raw_key(dataset, trade_date), frame)

        if missing != "adj_factor":
            adj = generate_mock_dataset("adj_factor", trade_date)
            adj = adj[adj["stock_code"].isin(CODES)].reset_index(drop=True)
            adj["adj_factor"] = adj_factor
            _write_frame(root, _goal20_staging_key("adj_factor", trade_date), adj)

        if missing != "benchmark_price":
            benchmark = generate_mock_dataset("benchmark_price", trade_date)
            if missing_benchmark:
                benchmark = benchmark[benchmark["index_code"] != missing_benchmark].reset_index(drop=True)
            _write_frame(root, _goal20_staging_key("benchmark_price", trade_date), benchmark)

        if missing != "stock_basic":
            stock = generate_mock_dataset("stock_basic", trade_date)
            stock = stock[stock["stock_code"].isin(CODES)].reset_index(drop=True)
            stock.loc[stock["stock_code"] == "600519.SH", "delist_date"] = "2025-01-01"
            stock_evidence_key = f"evidence/vendor/stock_basic/as_of={trade_date}/part.parquet"
            _write_frame(root, stock_evidence_key, stock.copy())
            stock["source_snapshot_date"] = trade_date
            stock["source_object_key"] = stock_evidence_key
            stock["source_semantics"] = "POINT_IN_TIME_HISTORICAL_SNAPSHOT"
            _write_frame(root, _goal20_staging_key("stock_basic", trade_date), stock)

    if missing != "st_history":
        st_source_key = "evidence/vendor/st_history/2023-01-01_2024-06-04.parquet"
        st = pd.DataFrame(
            [
                {
                    "stock_code": CODES[0],
                    "st_type": "ST",
                    "start_date": "2023-05-01",
                    "end_date": "2024-01-15",
                    "source": "exchange_status_history",
                    "source_object_key": st_source_key,
                    "source_semantics": "HISTORICAL_INTERVAL_SOURCE",
                    "coverage_codes": ",".join(CODES),
                    "coverage_start_date": "2023-01-01",
                    "coverage_end_date": TRADE_DATES[-1],
                    "coverage_complete": True,
                }
            ]
        )
        _write_frame(root, st_source_key, st[["stock_code", "st_type", "start_date", "end_date", "source"]].copy())
        _write_frame(root, _goal20_staging_key("st_history"), st)


def _write_goal18_current_snapshot_staging(root):
    stock = pd.DataFrame(
        [
            {
                "ts_code": code,
                "name": f"current-{code}",
                "exchange": code[-2:],
                "industry": "current",
                "market": "current",
                "list_date": "20100101",
                "delist_date": None,
                "is_st": code == CODES[0],
            }
            for code in CODES
        ]
    )
    st = pd.DataFrame(
        [
            {
                "ts_code": CODES[0],
                "st_type": "CURRENT_NAME_SNAPSHOT",
                "start_date": TRADE_DATES[0],
                "end_date": None,
                "source": "current_stock_basic_snapshot",
            }
        ]
    )
    _write_frame(
        root,
        f"candidate/tushare/standard_inputs/stock_basic_staging/batch_id={BATCH_ID}/part.parquet",
        stock,
    )
    _write_frame(
        root,
        f"candidate/tushare/standard_inputs/st_history_staging/batch_id={BATCH_ID}/part.parquet",
        st,
    )


def _write_goal13_adj_sources(root, *, manifest_override=None, write_manifest=True):
    manifest_key = f"candidate/tushare/batch_manifest/batch_id={BATCH_ID}/manifest.json"
    manifest = {
        "provider": "tushare",
        "goal": "13C",
        "batch_id": BATCH_ID,
        "codes": CODES,
        "trade_dates": TRADE_DATES,
        "interfaces_succeeded": ["adj_factor"],
        "blocked_reasons": [],
    }
    manifest.update(manifest_override or {})
    if write_manifest:
        _write_json(root, manifest_key, manifest)
    for trade_date in TRADE_DATES:
        _write_frame(
            root,
            f"candidate/tushare/adj_factor_staging/batch_id={BATCH_ID}/trade_date={trade_date}/part.parquet",
            pd.DataFrame(
                [
                    {"ts_code": code, "trade_date": trade_date.replace("-", ""), "adj_factor": 1.25}
                    for code in CODES
                ]
            ),
        )
    return manifest_key


def _remove_goal20_staging(root, dataset):
    path = root / _goal20_staging_key(dataset)
    if path.exists():
        path.unlink()


def _goal20_staging_key(dataset, trade_date=None):
    names = {
        "adj_factor": "adj_factor_staging",
        "benchmark_price": "benchmark_price_staging",
        "stock_basic": "stock_basic_history_staging",
        "st_history": "st_history_interval_staging",
    }
    base = f"candidate/real_clean_inputs/{names[dataset]}/batch_id={BATCH_ID}"
    if trade_date:
        return f"{base}/trade_date={trade_date}/part.parquet"
    return f"{base}/part.parquet"


def _goal20_st_coverage_key():
    return f"candidate/real_clean_inputs/st_history_interval_staging/batch_id={BATCH_ID}/coverage.json"


def _raw_key(dataset, trade_date):
    return f"raw/{dataset}/trade_date={trade_date}/part.parquet"


def _write_frame(root, object_key, frame):
    path = root / object_key
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _write_json(root, object_key, payload):
    path = root / object_key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _read_report(root, object_key):
    return json.loads((root / object_key).read_text(encoding="utf-8"))


def _provider_must_not_be_constructed(*_args, **_kwargs):
    raise AssertionError("provider must not be constructed without --provider-call")


def _downstream_must_not_be_called(*_args, **_kwargs):
    raise AssertionError("Goal 20 must not call downstream workflows")


def _canonical_write_must_not_be_called(*_args, **_kwargs):
    raise AssertionError("canonical writes require --apply and a complete seven-input preflight")


class _FakeAdjProvider:
    def __init__(self, calls):
        self.calls = calls

    def fetch_adj_factor(self, trade_date):
        self.calls["adj_factor"].append(trade_date)
        return pd.DataFrame(
            [
                {"ts_code": code, "trade_date": trade_date.replace("-", ""), "adj_factor": 1.1}
                for code in CODES
            ]
        )


class _FakeBenchmarkProvider:
    def __init__(self, calls):
        self.calls = calls

    def fetch_benchmark_price(self, trade_date):
        self.calls["benchmark_price"].append(trade_date)
        return generate_mock_dataset("benchmark_price", trade_date)

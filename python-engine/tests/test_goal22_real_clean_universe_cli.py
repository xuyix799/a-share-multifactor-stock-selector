import json

import pandas as pd
import pytest

from stock_selector.cli import _read_goal22_processed, build_parser, main
from stock_selector.data.data_validator import DataValidationError
from stock_selector.data.historical_backfill import dataframe_checksum
from stock_selector.data.mock_data import generate_mock_dataset
from stock_selector.data.real_clean_input_gate import readiness_payload_checksum
from stock_selector.data.real_clean_universe import OUTPUT_DATASETS, REQUIRED_INPUTS


TRADE_DATE = "2026-06-19"
RUN_ID = "goal22-cli-test"
READINESS_KEY = (
    "candidate/real_clean_inputs/readiness_report/"
    "batch_id=goal22-cli-receipt/report.json"
)
READINESS_MANIFEST_KEY = (
    "candidate/real_clean_inputs/manifest/"
    "batch_id=goal22-cli-receipt/manifest.json"
)


def test_goal22_parser_defaults_to_dry_run_and_resume():
    args = build_parser().parse_args(_args())

    assert args.command == "run-real-clean-universe-range"
    assert args.apply is False
    assert args.resume is True
    assert args.force is False


def test_goal22_parser_requires_explicit_trade_dates_and_readiness_receipt():
    parser = build_parser()
    base = [
        "run-real-clean-universe-range",
        "--run-id",
        RUN_ID,
        "--start-date",
        TRADE_DATE,
        "--end-date",
        TRADE_DATE,
    ]

    with pytest.raises(SystemExit):
        parser.parse_args([*base, "--readiness-report-key", READINESS_KEY])
    with pytest.raises(SystemExit):
        parser.parse_args([*base, "--trade-dates", TRADE_DATE])


def test_goal22_cli_rejects_arbitrary_raw_inputs_without_goal20_receipt(
    monkeypatch,
    tmp_path,
    capsys,
):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_inputs(tmp_path, write_receipt=False)

    assert main(_args()) == 2

    assert "readiness" in capsys.readouterr().err.lower()
    assert not (tmp_path / "candidate" / "real_clean_universe").exists()
    assert not (tmp_path / "processed").exists()


def test_goal22_cli_rejects_receipt_when_ready_for_clean_is_false(
    monkeypatch,
    tmp_path,
    capsys,
):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_inputs(tmp_path)
    report = _read_json(tmp_path, READINESS_KEY)
    report["ready_for_clean"] = False
    _write_json(tmp_path, READINESS_KEY, report)
    manifest = _read_json(tmp_path, READINESS_MANIFEST_KEY)
    manifest["readiness_report_checksum"] = readiness_payload_checksum(report)
    _write_json(tmp_path, READINESS_MANIFEST_KEY, manifest)

    assert main(_args()) == 2

    assert "ready_for_clean" in capsys.readouterr().err
    assert not (tmp_path / "candidate" / "real_clean_universe").exists()


def test_goal22_cli_rejects_trade_date_outside_goal20_audit_scope(
    monkeypatch,
    tmp_path,
    capsys,
):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_inputs(tmp_path)
    requested_dates = ["2026-06-18", TRADE_DATE]

    assert main(
        _args(
            run_id="goal22-outside-receipt",
            trade_dates=requested_dates,
            start_date=requested_dates[0],
            end_date=requested_dates[-1],
        )
    ) == 2

    assert "do not cover trade_date 2026-06-18" in capsys.readouterr().err
    assert not (tmp_path / "candidate" / "real_clean_universe").exists()


def test_goal22_cli_rejects_manifest_source_lineage_mismatch(
    monkeypatch,
    tmp_path,
    capsys,
):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_inputs(tmp_path)
    manifest = _read_json(tmp_path, READINESS_MANIFEST_KEY)
    manifest["source_keys"]["daily_price"] = []
    _write_json(tmp_path, READINESS_MANIFEST_KEY, manifest)

    assert main(_args(run_id="goal22-lineage-mismatch")) == 2

    assert "source lineage differs for daily_price" in capsys.readouterr().err
    assert not (tmp_path / "candidate" / "real_clean_universe").exists()


def test_goal22_cli_dry_run_apply_and_processed_readback(monkeypatch, tmp_path, capsys):
    import stock_selector.cli as cli_module

    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_inputs(tmp_path)
    monkeypatch.setattr(cli_module, "build_factor_daily_for_date", _downstream_must_not_run)
    monkeypatch.setattr(cli_module, "build_selection_for_date", _downstream_must_not_run)
    monkeypatch.setattr(cli_module, "run_backtest", _downstream_must_not_run)

    assert main(_args()) == 0

    dry_output = json.loads(capsys.readouterr().out)
    assert dry_output["status"] == "READY_FOR_APPLY"
    assert dry_output["mode"] == "DRY_RUN"
    assert dry_output["apply_requested"] is False
    assert not (tmp_path / "processed").exists()
    dq = _read_json(tmp_path, dry_output["daily_report_keys"][TRADE_DATE])
    assert dq["trusted_input_lineage"]["readiness_report_keys"] == [READINESS_KEY]
    assert set(dq["inputs"]) == set(REQUIRED_INPUTS)
    assert all(item["versions"] for item in dq["inputs"].values())
    assert all(item["checksum"] for item in dq["inputs"].values())

    assert main(_args("--apply")) == 0

    apply_output = json.loads(capsys.readouterr().out)
    assert apply_output["status"] == "COMPLETED"
    assert apply_output["mode"] == "APPLY"
    for dataset in OUTPUT_DATASETS:
        legacy_direct_path = (
            tmp_path / "processed" / dataset / f"trade_date={TRADE_DATE}" / "part.parquet"
        )
        assert not legacy_direct_path.exists()
        assert not _read_goal22_processed(dataset, TRADE_DATE).empty
    assert (
        tmp_path
        / "processed"
        / "_goal22_commits"
        / f"trade_date={TRADE_DATE}"
        / "commit.json"
    ).exists()
    manifest = _read_json(tmp_path, apply_output["range_manifest_key"])
    assert manifest["status"] == "COMPLETED"
    assert manifest["downstream_firewalls"] == {
        "factor_daily": False,
        "selection_result": False,
        "backtest": False,
    }


def test_goal22_processed_reader_rejects_partial_commit_marker(
    monkeypatch,
    tmp_path,
    capsys,
):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_inputs(tmp_path)
    assert main(_args("--apply", run_id="goal22-partial-commit")) == 0
    capsys.readouterr()
    commit_key = (
        f"processed/_goal22_commits/trade_date={TRADE_DATE}/commit.json"
    )
    commit = _read_json(tmp_path, commit_key)
    del commit["outputs"]["factor_input_table"]
    _write_json(tmp_path, commit_key, commit)

    with pytest.raises(DataValidationError, match="processed commit payload"):
        _read_goal22_processed("adjusted_price", TRADE_DATE)


def test_goal22_cli_stale_receipt_checksum_blocks_changed_canonical_input(
    monkeypatch,
    tmp_path,
    capsys,
):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_inputs(tmp_path)
    daily_path = tmp_path / _raw_key("daily_price", TRADE_DATE)
    daily = pd.read_parquet(daily_path)
    daily["amount"] = daily["amount"] + 1.0
    daily.to_parquet(daily_path, index=False)

    assert main(_args()) == 1

    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "BLOCKED"
    dq = _read_json(tmp_path, output["daily_report_keys"][TRADE_DATE])
    assert any(
        reason.startswith("INPUT_READ_FAILED:daily_price:")
        for reason in dq["blocked_reasons"]
    )
    assert not (tmp_path / "processed").exists()


def test_goal22_explicit_trusted_plan_keeps_fully_missing_market_date_in_dq(
    monkeypatch,
    tmp_path,
    capsys,
):
    trade_dates = ["2026-06-17", "2026-06-18", "2026-06-19"]
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_inputs(tmp_path, trade_dates=trade_dates)
    for dataset in ("daily_price", "adj_factor", "daily_basic", "benchmark_price"):
        (tmp_path / _raw_key(dataset, trade_dates[1])).unlink()

    assert main(
        _args(
            run_id="goal22-missing-date",
            trade_dates=trade_dates,
            start_date=trade_dates[0],
            end_date=trade_dates[-1],
        )
    ) == 1

    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "PARTIAL"
    assert list(output["date_statuses"]) == trade_dates
    assert output["date_statuses"][trade_dates[1]] == "BLOCKED"
    dq = _read_json(tmp_path, output["daily_report_keys"][trade_dates[1]])
    for dataset in ("daily_price", "adj_factor", "daily_basic", "benchmark_price"):
        assert f"MISSING_INPUT:{dataset}" in dq["blocked_reasons"]


def _args(
    *extra: str,
    run_id: str = RUN_ID,
    trade_dates: list[str] | None = None,
    start_date: str = TRADE_DATE,
    end_date: str = TRADE_DATE,
) -> list[str]:
    trade_dates = trade_dates or [TRADE_DATE]
    args = [
        "run-real-clean-universe-range",
        "--run-id",
        run_id,
        "--start-date",
        start_date,
        "--end-date",
        end_date,
        "--trade-dates",
        ",".join(trade_dates),
        "--readiness-report-key",
        READINESS_KEY,
    ]
    args.extend(extra)
    return args


def _write_inputs(
    root,
    *,
    trade_dates: list[str] | None = None,
    write_receipt: bool = True,
) -> None:
    trade_dates = trade_dates or [TRADE_DATE]
    for trade_date in trade_dates:
        for dataset in REQUIRED_INPUTS:
            if dataset == "st_history":
                frame = pd.DataFrame(
                    columns=["stock_code", "st_type", "start_date", "end_date", "source"]
                )
            else:
                frame = generate_mock_dataset(dataset, trade_date)
            if dataset == "daily_price":
                frame["amount"] = 100_000_000
            if dataset == "financial":
                frame["roe"] = 0.10
                frame["debt_ratio"] = 0.40
            path = root / _raw_key(dataset, trade_date)
            path.parent.mkdir(parents=True, exist_ok=True)
            frame.to_parquet(path, index=False)
    if write_receipt:
        _write_goal20_receipt(root, trade_dates)


def _write_goal20_receipt(root, trade_dates: list[str]) -> None:
    stock = pd.read_parquet(root / _raw_key("stock_basic", trade_dates[0]))
    codes = sorted(set(stock["stock_code"].astype(str)))
    inputs = {}
    readback_details = []
    source_keys = {}
    for dataset in REQUIRED_INPUTS:
        details = []
        keys = []
        total_rows = 0
        for trade_date in trade_dates:
            object_key = _raw_key(dataset, trade_date)
            raw = pd.read_parquet(root / object_key)
            scoped = (
                raw
                if dataset == "benchmark_price"
                else raw.loc[raw["stock_code"].astype(str).isin(codes)].reset_index(drop=True)
            )
            detail = {
                "trade_date": trade_date,
                "passed": True,
                "object_key": object_key,
                "object_row_count": len(raw),
                "object_checksum": dataframe_checksum(raw),
                "scope_row_count": len(scoped),
                "scope_checksum": dataframe_checksum(scoped),
            }
            details.append(detail)
            keys.append(object_key)
            total_rows += len(scoped)
        source_keys[dataset] = keys
        coverage = {
            "passed": True,
            "requested_codes": codes,
            "requested_trade_dates": trade_dates,
            "missing_pairs": [],
        }
        if dataset == "benchmark_price":
            coverage["required_indexes"] = [
                "000300.SH",
                "000905.SH",
                "000906.SH",
            ]
        inputs[dataset] = {
            "dataset": dataset,
            "source_keys": keys,
            "row_count": total_rows,
            "dq_level": "DQ3_STANDARD_CANONICAL",
            "coverage": coverage,
            "validation": {"passed": True, "errors": []},
            "write": {"requested": True, "status": "WRITTEN", "object_keys": keys},
            "read_back": {"passed": True, "status": "PASS", "details": details},
            "ready_for_apply": True,
            "ready_for_clean": True,
            "blocked_reasons": [],
        }
        readback_details.append(
            {"dataset": dataset, "passed": True, "details": details}
        )
    requested_scope = {
        "codes": codes,
        "start_date": trade_dates[0],
        "end_date": trade_dates[-1],
        "trade_dates": trade_dates,
        "max_codes": len(codes),
        "max_trade_days": len(trade_dates),
        "max_rows": 10000,
    }
    report = {
        "schema_version": "goal20.real_clean_input_readiness.v1",
        "goal": "20",
        "batch_id": "goal22-cli-receipt",
        "generated_at": "2026-06-20T00:00:00+00:00",
        "status": "READY",
        "mode": "APPLY",
        "requested_scope": requested_scope,
        "apply_requested": True,
        "standard_writes_performed": True,
        "ready_for_apply": True,
        "ready_for_clean": True,
        "inputs": inputs,
        "read_back_verification": {
            "passed": True,
            "status": "PASS",
            "details": readback_details,
        },
        "blocked_reasons": [],
        "downstream_firewalls": {
            "adjusted_price_entered": False,
            "clean_daily_snapshot_entered": False,
            "universe_entered": False,
            "factor_entered": False,
            "selection_entered": False,
            "backtest_entered": False,
        },
    }
    _write_json(root, READINESS_KEY, report)
    manifest = {
        "schema_version": "goal20.real_clean_input_manifest.v1",
        "goal": "20",
        "batch_id": "goal22-cli-receipt",
        "status": "COMPLETED",
        "readiness_status": "READY",
        "ready_for_apply": True,
        "ready_for_clean": True,
        "requested_scope": requested_scope,
        "readiness_report_key": READINESS_KEY,
        "readiness_report_checksum": readiness_payload_checksum(report),
        "source_keys": source_keys,
        "blocked_reasons": [],
        "downstream_firewalls": {
            "adjusted_price_entered": False,
            "clean_daily_snapshot_entered": False,
            "universe_entered": False,
            "factor_entered": False,
            "selection_entered": False,
            "backtest_entered": False,
        },
    }
    _write_json(root, READINESS_MANIFEST_KEY, manifest)


def _raw_key(dataset: str, trade_date: str) -> str:
    return f"raw/{dataset}/trade_date={trade_date}/part.parquet"


def _read_json(root, object_key: str) -> dict:
    return json.loads((root / object_key).read_text(encoding="utf-8"))


def _write_json(root, object_key: str, payload: dict) -> None:
    path = root / object_key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _downstream_must_not_run(*_args, **_kwargs):
    raise AssertionError("Goal 22 must not trigger factor_daily, selection_result or backtest")

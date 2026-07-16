import json

import pandas as pd

from stock_selector.cli import build_parser, main
from stock_selector.data.mock_data import generate_mock_dataset
from stock_selector.data.real_clean_universe import OUTPUT_DATASETS, REQUIRED_INPUTS


TRADE_DATE = "2026-06-19"
RUN_ID = "goal22-cli-test"


def test_goal22_parser_defaults_to_dry_run_and_resume():
    args = build_parser().parse_args(_args())

    assert args.command == "run-real-clean-universe-range"
    assert args.apply is False
    assert args.resume is True
    assert args.force is False


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
    assert set(dq["inputs"]) == set(REQUIRED_INPUTS)
    assert all(item["versions"] for item in dq["inputs"].values())
    assert all(item["checksum"] for item in dq["inputs"].values())

    assert main(_args("--apply")) == 0

    apply_output = json.loads(capsys.readouterr().out)
    assert apply_output["status"] == "COMPLETED"
    assert apply_output["mode"] == "APPLY"
    for dataset in OUTPUT_DATASETS:
        path = tmp_path / "processed" / dataset / f"trade_date={TRADE_DATE}" / "part.parquet"
        assert path.exists()
        assert not pd.read_parquet(path).empty
    manifest = _read_json(tmp_path, apply_output["range_manifest_key"])
    assert manifest["status"] == "COMPLETED"
    assert manifest["downstream_firewalls"] == {
        "factor_daily": False,
        "selection_result": False,
        "backtest": False,
    }


def test_goal22_cli_partition_discovery_still_blocks_a_missing_market_input(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_inputs(tmp_path, missing="benchmark_price")

    args = _args(include_trade_dates=False)
    assert main(args) == 1

    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "BLOCKED"
    dq = _read_json(tmp_path, output["daily_report_keys"][TRADE_DATE])
    assert "MISSING_INPUT:benchmark_price" in dq["blocked_reasons"]
    assert not (tmp_path / "processed").exists()


def _args(*extra: str, include_trade_dates: bool = True) -> list[str]:
    args = [
        "run-real-clean-universe-range",
        "--run-id",
        RUN_ID,
        "--start-date",
        TRADE_DATE,
        "--end-date",
        TRADE_DATE,
    ]
    if include_trade_dates:
        args.extend(["--trade-dates", TRADE_DATE])
    args.extend(extra)
    return args


def _write_inputs(root, *, missing: str | None = None) -> None:
    for dataset in REQUIRED_INPUTS:
        if dataset == missing:
            continue
        if dataset == "st_history":
            frame = pd.DataFrame(columns=["stock_code", "st_type", "start_date", "end_date", "source"])
        else:
            frame = generate_mock_dataset(dataset, TRADE_DATE)
        if dataset == "daily_price":
            frame["amount"] = 100_000_000
        if dataset == "financial":
            frame["roe"] = 0.10
            frame["debt_ratio"] = 0.40
        path = root / "raw" / dataset / f"trade_date={TRADE_DATE}" / "part.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(path, index=False)


def _read_json(root, object_key: str) -> dict:
    return json.loads((root / object_key).read_text(encoding="utf-8"))


def _downstream_must_not_run(*_args, **_kwargs):
    raise AssertionError("Goal 22 must not trigger factor_daily, selection_result or backtest")

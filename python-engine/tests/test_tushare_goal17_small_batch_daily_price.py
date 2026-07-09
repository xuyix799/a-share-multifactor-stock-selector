import json

import pandas as pd

from stock_selector.cli import main
from test_tushare_goal13_candidate_staging_batch import (
    END_DATE as GOAL13_END_DATE,
    START_DATE as GOAL13_START_DATE,
    _FakeTushareBatchProvider,
    _ProviderShouldNotBeConstructed,
)
from test_tushare_goal14_daily_price_promotion_validator import (
    BATCH_ID,
    CODES,
    TRADE_DATES,
    _candidate_frame,
    _write_goal13c_artifacts,
)


START_DATE = "2024-06-01"
END_DATE = "2024-06-05"


def test_goal17_default_dry_run_writes_reports_only_and_no_canonical_daily_price(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_goal13c_artifacts(tmp_path, BATCH_ID)

    exit_code = main(_small_batch_args())

    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["goal"] == "17"
    assert output["status"] == "VALIDATOR_PASS"
    assert output["mode"] == "DRY_RUN"
    assert output["provider_call_requested"] is False
    assert output["apply_requested"] is False
    assert output["standard_daily_price_write_performed"] is False
    assert (tmp_path / output["small_batch_run_report_key"]).exists()
    assert (tmp_path / output["daily_price_promotion_validator_report_key"]).exists()
    assert (tmp_path / output["standard_daily_price_promotion_dry_run_report_key"]).exists()
    assert output["standard_daily_price_promotion_apply_report_key"] is None
    assert not (tmp_path / "raw" / "daily_price").exists()

    report = _read_report(tmp_path, output["small_batch_run_report_key"])
    assert report["schema_version"] == "goal17.tushare_daily_price_small_batch_run_report.v1"
    assert report["requested_scope"] == {
        "codes": CODES,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "max_codes": 5,
        "max_trade_days": 10,
        "max_rows": 50,
    }
    assert report["source_artifact_keys"]["daily_price_candidate_batch"] == (
        f"candidate/tushare/daily_price_candidate_batch/batch_id={BATCH_ID}/part.parquet"
    )
    assert report["candidate_preflight_status"] == "READY_FOR_PROMOTION_VALIDATOR"
    assert report["promotion_validator_status"] == "VALIDATOR_PASS"
    assert report["apply"]["requested"] is False
    assert report["apply"]["performed"] is False
    assert report["downstream_firewalls"] == _expected_firewalls()


def test_goal17_apply_routes_to_canonical_daily_price_and_records_readback(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_goal13c_artifacts(tmp_path, BATCH_ID)

    exit_code = main(_small_batch_args("--apply"))

    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["mode"] == "APPLY"
    assert output["standard_daily_price_write_performed"] is True
    assert output["read_back_verification"]["passed"] is True
    assert output["standard_daily_price_promotion_apply_report_key"] == (
        f"candidate/tushare/standard_daily_price_promotion_apply_report/batch_id={BATCH_ID}/report.json"
    )
    for trade_date in TRADE_DATES:
        canonical = tmp_path / "raw" / "daily_price" / f"trade_date={trade_date}" / "part.parquet"
        assert canonical.exists()
        frame = pd.read_parquet(canonical)
        assert len(frame) == 2
        assert len(frame.drop_duplicates(["stock_code", "trade_date"])) == 2

    report = _read_report(tmp_path, output["small_batch_run_report_key"])
    assert report["apply"]["requested"] is True
    assert report["apply"]["performed"] is True
    assert report["read_back_verification"]["passed"] is True
    assert report["downstream_firewalls"] == _expected_firewalls()


def test_goal17_no_provider_call_reuse_existing_staging_runs_from_local_staging(monkeypatch, tmp_path, capsys):
    import stock_selector.cli as cli_module

    batch_id = "goal17-reuse-staging"
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("STOCK_TUSHARE_ENABLED", "true")
    monkeypatch.setenv("TUSHARE_TOKEN", "fake-token")
    monkeypatch.setattr(
        cli_module,
        "TushareProvider",
        lambda settings=None: _FakeTushareBatchProvider(complete_limit_coverage=True, empty_suspend_d=True),
    )
    assert main(
        [
            "build-tushare-candidate-staging-batch",
            "--start-date",
            GOAL13_START_DATE,
            "--end-date",
            GOAL13_END_DATE,
            "--codes",
            ",".join(CODES),
            "--batch-id",
            batch_id,
            "--sleep-seconds",
            "0",
            "--coverage-expansion",
            "--fetch-semantics-audit",
            "--goal13c-preflight",
        ]
    ) == 0
    _ = capsys.readouterr()

    monkeypatch.setattr(cli_module, "TushareProvider", lambda settings=None: _ProviderShouldNotBeConstructed())
    exit_code = main(_small_batch_args("--batch-id", batch_id, "--no-provider-call", "--reuse-existing-staging"))

    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "VALIDATOR_PASS"
    assert output["provider_call_requested"] is False
    assert output["reused_existing_staging"] is True
    report = _read_report(tmp_path, output["small_batch_run_report_key"])
    assert report["provider"]["enabled"] is False
    assert report["provider"]["reuse_existing_staging"] is True
    assert report["staging_status"] == "REUSED_EXISTING_CANDIDATE_ARTIFACTS"


def test_goal17_provider_call_blocks_when_provider_disabled_or_token_missing(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    monkeypatch.delenv("STOCK_TUSHARE_ENABLED", raising=False)
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)

    assert main(_small_batch_args("--provider-call")) == 1
    disabled = json.loads(capsys.readouterr().out)
    assert disabled["status"] == "BLOCKED_BY_PROVIDER_DISABLED"
    assert disabled["provider_call_requested"] is True
    assert not (tmp_path / "raw" / "daily_price").exists()

    monkeypatch.setenv("STOCK_TUSHARE_ENABLED", "true")
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    assert main(_small_batch_args("--provider-call")) == 1
    missing_token = json.loads(capsys.readouterr().out)
    assert missing_token["status"] == "BLOCKED_BY_MISSING_TUSHARE_TOKEN"
    assert "TUSHARE_TOKEN" in missing_token["blocked_reasons"][0]
    assert "fake-token" not in json.dumps(_read_report(tmp_path, missing_token["small_batch_run_report_key"]))


def test_goal17_invalid_candidate_rows_are_rejected_without_polluting_canonical(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_goal13c_artifacts(tmp_path, BATCH_ID)
    invalid = _candidate_frame()
    invalid.loc[0, "close"] = None
    invalid.to_parquet(
        tmp_path / f"candidate/tushare/daily_price_candidate_batch/batch_id={BATCH_ID}/part.parquet",
        index=False,
    )

    exit_code = main(_small_batch_args("--apply"))

    assert exit_code == 1
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "BLOCKED"
    assert "MISSING_REQUIRED_DAILY_PRICE_FIELD" in output["blocked_reasons"]
    assert output["standard_daily_price_write_performed"] is False
    assert not (tmp_path / "raw" / "daily_price").exists()
    report = _read_report(tmp_path, output["small_batch_run_report_key"])
    assert report["apply"]["requested"] is True
    assert report["apply"]["performed"] is False


def test_goal17_duplicate_apply_rerun_is_idempotent(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_goal13c_artifacts(tmp_path, BATCH_ID)

    assert main(_small_batch_args("--apply")) == 0
    _ = capsys.readouterr()
    assert main(_small_batch_args("--apply")) == 0

    output = json.loads(capsys.readouterr().out)
    assert output["upsert_summary"] == {"inserted_rows": 0, "updated_rows": 0, "unchanged_rows": 4}
    for trade_date in TRADE_DATES:
        frame = pd.read_parquet(tmp_path / "raw" / "daily_price" / f"trade_date={trade_date}" / "part.parquet")
        assert len(frame) == len(frame.drop_duplicates(["stock_code", "trade_date"]))


def test_goal17_downstream_firewalls_remain_false(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_goal13c_artifacts(tmp_path, BATCH_ID)

    assert main(_small_batch_args("--apply")) == 0

    output = json.loads(capsys.readouterr().out)
    report = _read_report(tmp_path, output["small_batch_run_report_key"])
    assert report["downstream_firewalls"] == _expected_firewalls()
    assert report["standard_suspension_status_write_performed"] is False
    assert report["clean_factor_selection_backtest_entered"] is False
    assert "clean_daily_snapshot" not in json.dumps(output)
    assert "factor_daily" not in json.dumps(output)
    assert "selection_result" not in json.dumps(output)
    assert output["real_backtest_performed"] is False


def _small_batch_args(*extra):
    args = [
        "run-tushare-daily-price-small-batch",
        "--batch-id",
        BATCH_ID,
        "--codes",
        ",".join(CODES),
        "--start-date",
        START_DATE,
        "--end-date",
        END_DATE,
        "--sleep-seconds",
        "0",
    ]
    args.extend(extra)
    return args


def _read_report(root, object_key):
    return json.loads((root / object_key).read_text(encoding="utf-8"))


def _expected_firewalls():
    return {
        "standard_suspension_status_write_performed": False,
        "clean_daily_snapshot_entered": False,
        "factor_input_table_entered": False,
        "factor_daily_entered": False,
        "selection_result_entered": False,
        "backtest_entered": False,
    }

import json

import pandas as pd

from stock_selector.cli import main


BATCH_ID = "goal18-test-batch"
CODES = ["000001.SZ", "600519.SH"]
START_DATE = "2024-06-03"
END_DATE = "2024-06-04"
TRADE_DATES = ["2024-06-03", "2024-06-04"]


def test_goal18_default_dry_run_writes_report_only_and_no_standard_layers(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_goal18_staging(tmp_path)

    exit_code = main(_goal18_args())

    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["goal"] == "18"
    assert output["status"] == "VALIDATION_PASS"
    assert output["mode"] == "DRY_RUN"
    assert output["provider_call_requested"] is False
    assert output["apply_requested"] is False
    assert output["standard_writes_performed"] is False
    assert (tmp_path / output["standard_inputs_run_report_key"]).exists()
    assert not (tmp_path / "raw" / "daily_basic").exists()
    assert not (tmp_path / "raw" / "financial").exists()

    report = _read_report(tmp_path, output["standard_inputs_run_report_key"])
    assert report["schema_version"] == "goal18.tushare_standard_inputs_run_report.v1"
    assert report["requested_scope"]["codes"] == CODES
    assert report["requested_scope"]["start_date"] == START_DATE
    assert report["requested_scope"]["end_date"] == END_DATE
    assert report["dataset_statuses"]["daily_basic"]["standard_write_allowed"] is True
    assert report["dataset_statuses"]["financial"]["standard_write_allowed"] is True
    assert report["dataset_statuses"]["stock_basic"]["standard_write_allowed"] is False
    assert report["dataset_statuses"]["st_history"]["standard_write_allowed"] is False
    assert report["downstream_firewalls"] == _expected_firewalls()


def test_goal18_default_does_not_construct_provider_without_provider_call(monkeypatch, tmp_path, capsys):
    import stock_selector.cli as cli_module

    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_goal18_staging(tmp_path)
    monkeypatch.setattr(cli_module, "TushareProvider", lambda settings=None: _ProviderShouldNotBeConstructed())

    assert main(_goal18_args()) == 0

    output = json.loads(capsys.readouterr().out)
    assert output["provider_call_requested"] is False
    assert output["status"] == "VALIDATION_PASS"


def test_goal18_provider_call_blocks_when_provider_disabled_or_token_missing(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    monkeypatch.delenv("STOCK_TUSHARE_ENABLED", raising=False)
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)

    assert main(_goal18_args("--provider-call")) == 1
    disabled = json.loads(capsys.readouterr().out)
    assert disabled["status"] == "BLOCKED_BY_PROVIDER_DISABLED"
    assert disabled["provider_call_requested"] is True
    assert (tmp_path / disabled["standard_inputs_run_report_key"]).exists()
    assert not (tmp_path / "raw").exists()

    monkeypatch.setenv("STOCK_TUSHARE_ENABLED", "true")
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    assert main(_goal18_args("--provider-call")) == 1
    missing_token = json.loads(capsys.readouterr().out)
    assert missing_token["status"] == "BLOCKED_BY_MISSING_TUSHARE_TOKEN"
    assert "TUSHARE_TOKEN" in missing_token["blocked_reasons"][0]
    assert "fake-token" not in json.dumps(_read_report(tmp_path, missing_token["standard_inputs_run_report_key"]))


def test_goal18_no_provider_call_reuse_existing_staging_rebuilds_report(monkeypatch, tmp_path, capsys):
    import stock_selector.cli as cli_module

    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_goal18_staging(tmp_path)
    monkeypatch.setattr(cli_module, "TushareProvider", lambda settings=None: _ProviderShouldNotBeConstructed())

    exit_code = main(_goal18_args("--no-provider-call", "--reuse-existing-staging"))

    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["reused_existing_staging"] is True
    report = _read_report(tmp_path, output["standard_inputs_run_report_key"])
    assert report["provider"]["enabled"] is False
    assert report["provider"]["reuse_existing_staging"] is True
    assert report["dataset_statuses"]["daily_basic"]["staging_row_count"] == 4
    assert report["dataset_statuses"]["financial"]["staging_row_count"] == 2


def test_goal18_apply_writes_standard_and_candidate_layers_with_readback(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_goal18_staging(tmp_path)

    exit_code = main(_goal18_args("--apply"))

    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["mode"] == "APPLY"
    assert output["standard_writes_performed"] is True
    assert output["read_back_verification"]["passed"] is True
    for trade_date in TRADE_DATES:
        daily_basic = pd.read_parquet(tmp_path / "raw" / "daily_basic" / f"trade_date={trade_date}" / "part.parquet")
        financial = pd.read_parquet(tmp_path / "raw" / "financial" / f"trade_date={trade_date}" / "part.parquet")
        assert len(daily_basic) == 2
        assert len(daily_basic.drop_duplicates(["stock_code", "trade_date"])) == 2
        assert len(financial) == 2
        assert len(financial.drop_duplicates(["stock_code", "report_period", "announce_date"])) == 2

    report = _read_report(tmp_path, output["standard_inputs_run_report_key"])
    assert report["dataset_statuses"]["daily_basic"]["write_status"] == "WRITTEN"
    assert report["dataset_statuses"]["financial"]["write_status"] == "WRITTEN"
    assert report["dataset_statuses"]["stock_basic"]["write_status"] == "CANDIDATE_WRITTEN"
    assert report["dataset_statuses"]["st_history"]["write_status"] == "CANDIDATE_WRITTEN"
    assert (tmp_path / report["dataset_statuses"]["stock_basic"]["candidate_object_key"]).exists()
    assert (tmp_path / report["dataset_statuses"]["st_history"]["candidate_object_key"]).exists()


def test_goal18_invalid_row_is_blocked_without_polluting_standard_layers(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_goal18_staging(tmp_path, invalid_daily_basic=True)

    exit_code = main(_goal18_args("--apply"))

    assert exit_code == 1
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "BLOCKED"
    assert "DAILY_BASIC_NUMERIC_INVALID" in output["blocked_reasons"]
    assert output["standard_writes_performed"] is False
    assert not (tmp_path / "raw" / "daily_basic").exists()
    assert not (tmp_path / "raw" / "financial").exists()


def test_goal18_duplicate_apply_rerun_is_idempotent(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_goal18_staging(tmp_path)

    assert main(_goal18_args("--apply")) == 0
    _ = capsys.readouterr()
    assert main(_goal18_args("--apply")) == 0

    output = json.loads(capsys.readouterr().out)
    assert output["upsert_summary"]["daily_basic"] == {
        "inserted_rows": 0,
        "updated_rows": 0,
        "unchanged_rows": 4,
    }
    assert output["upsert_summary"]["financial"] == {
        "inserted_rows": 0,
        "updated_rows": 0,
        "unchanged_rows": 4,
    }
    for trade_date in TRADE_DATES:
        daily_basic = pd.read_parquet(tmp_path / "raw" / "daily_basic" / f"trade_date={trade_date}" / "part.parquet")
        assert len(daily_basic) == len(daily_basic.drop_duplicates(["stock_code", "trade_date"]))


def test_goal18_financial_announce_date_asof_blocks_future_disclosure(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_goal18_staging(tmp_path, future_financial=True)

    assert main(_goal18_args("--apply")) == 1

    output = json.loads(capsys.readouterr().out)
    assert "FINANCIAL_ANNOUNCE_DATE_AFTER_SCOPE_START" in output["blocked_reasons"]
    assert not (tmp_path / "raw" / "financial").exists()
    report = _read_report(tmp_path, output["standard_inputs_run_report_key"])
    assert report["dataset_statuses"]["financial"]["as_of_check"]["passed"] is False


def test_goal18_current_snapshot_stock_basic_and_st_are_not_declared_historical(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_goal18_staging(tmp_path)

    assert main(_goal18_args("--apply")) == 0

    output = json.loads(capsys.readouterr().out)
    report = _read_report(tmp_path, output["standard_inputs_run_report_key"])
    stock_basic = report["dataset_statuses"]["stock_basic"]
    st_history = report["dataset_statuses"]["st_history"]
    assert stock_basic["dq_level"] == "DQ2_CURRENT_SNAPSHOT_ONLY"
    assert stock_basic["standard_write_allowed"] is False
    assert "CURRENT_SNAPSHOT_NOT_HISTORICAL" in stock_basic["blocked_reasons"]
    assert st_history["dq_level"] == "DQ2_CURRENT_SNAPSHOT_ONLY"
    assert st_history["standard_write_allowed"] is False
    assert "ST_STATUS_NOT_HISTORICAL" in st_history["blocked_reasons"]
    assert not (tmp_path / "raw" / "stock_basic").exists()
    assert not (tmp_path / "raw" / "st_history").exists()


def test_goal18_downstream_firewalls_remain_false(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_goal18_staging(tmp_path)

    assert main(_goal18_args("--apply")) == 0

    output = json.loads(capsys.readouterr().out)
    report = _read_report(tmp_path, output["standard_inputs_run_report_key"])
    assert report["downstream_firewalls"] == _expected_firewalls()
    assert output["clean_factor_selection_backtest_entered"] is False
    assert output["real_backtest_performed"] is False


def _goal18_args(*extra):
    args = [
        "run-tushare-standard-inputs-small-batch",
        "--batch-id",
        BATCH_ID,
        "--codes",
        ",".join(CODES),
        "--start-date",
        START_DATE,
        "--end-date",
        END_DATE,
        "--max-codes",
        "5",
        "--max-trade-days",
        "5",
        "--max-rows",
        "50",
        "--sleep-seconds",
        "0",
    ]
    args.extend(extra)
    return args


def _write_goal18_staging(root, *, invalid_daily_basic=False, future_financial=False):
    stock_basic = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "name": "平安银行",
                "exchange": "SZSE",
                "industry": "银行",
                "market": "主板",
                "list_date": "19910403",
                "is_st": False,
            },
            {
                "ts_code": "600519.SH",
                "name": "贵州茅台",
                "exchange": "SSE",
                "industry": "食品饮料",
                "market": "主板",
                "list_date": "20010827",
                "is_st": False,
            },
        ]
    )
    st_history = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "st_type": "CURRENT_NAME_SNAPSHOT",
                "start_date": START_DATE,
                "end_date": None,
                "source": "current_stock_basic_snapshot",
            }
        ]
    )
    financial = pd.DataFrame(
        [
            {
                "stock_code": "000001.SZ",
                "report_period": "2024-03-31",
                "announce_date": "2024-06-05" if future_financial else "2024-05-30",
                "revenue_yoy": 0.08,
                "net_profit_yoy": 0.06,
                "roe": 0.11,
                "gross_margin": 0.34,
                "debt_ratio": 0.42,
                "operating_cashflow": 1000000000.0,
            },
            {
                "stock_code": "600519.SH",
                "report_period": "2024-03-31",
                "announce_date": "2024-05-31",
                "revenue_yoy": 0.12,
                "net_profit_yoy": 0.10,
                "roe": 0.18,
                "gross_margin": 0.61,
                "debt_ratio": 0.21,
                "operating_cashflow": 2000000000.0,
            },
        ]
    )

    _write_parquet(root, _stock_basic_staging_key(), stock_basic)
    _write_parquet(root, _financial_staging_key(), financial)
    _write_parquet(root, _st_history_staging_key(), st_history)
    for trade_date in TRADE_DATES:
        daily_basic = pd.DataFrame(
            [
                _daily_basic_row("000001.SZ", trade_date, pb=-0.1 if invalid_daily_basic else 0.8),
                _daily_basic_row("600519.SH", trade_date, pb=11.5),
            ]
        )
        _write_parquet(root, _daily_basic_staging_key(trade_date), daily_basic)


def _daily_basic_row(code, trade_date, *, pb):
    return {
        "ts_code": code,
        "trade_date": trade_date.replace("-", ""),
        "pe_ttm": 8.5 if code == "000001.SZ" else 28.0,
        "pb": pb,
        "ps_ttm": 1.3 if code == "000001.SZ" else 14.0,
        "total_mv": 100000.0,
        "circ_mv": 90000.0,
        "turnover_rate": 0.8,
    }


def _write_parquet(root, object_key, frame):
    path = root / object_key
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False)


def _read_report(root, object_key):
    return json.loads((root / object_key).read_text(encoding="utf-8"))


def _stock_basic_staging_key():
    return f"candidate/tushare/standard_inputs/stock_basic_staging/batch_id={BATCH_ID}/part.parquet"


def _daily_basic_staging_key(trade_date):
    return f"candidate/tushare/standard_inputs/daily_basic_staging/batch_id={BATCH_ID}/trade_date={trade_date}/part.parquet"


def _financial_staging_key():
    return f"candidate/tushare/standard_inputs/financial_staging/batch_id={BATCH_ID}/part.parquet"


def _st_history_staging_key():
    return f"candidate/tushare/standard_inputs/st_history_staging/batch_id={BATCH_ID}/part.parquet"


def _expected_firewalls():
    return {
        "clean_daily_snapshot_entered": False,
        "factor_input_table_entered": False,
        "factor_daily_entered": False,
        "selection_result_entered": False,
        "backtest_entered": False,
    }


class _ProviderShouldNotBeConstructed:
    def __init__(self):
        raise AssertionError("TushareProvider should not be constructed without --provider-call")

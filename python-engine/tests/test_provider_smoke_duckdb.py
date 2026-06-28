import json

import pandas as pd

from stock_selector.cli import main
from stock_selector.data.mock_data import generate_mock_dataset
from stock_selector.storage.atomic_writer import write_parquet_local_atomic
from stock_selector.storage.partition import build_provider_smoke_partition


def test_query_parquet_reads_isolated_provider_smoke_dataset(tmp_path, monkeypatch, capsys):
    trade_date = "2026-06-19"
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    partition = build_provider_smoke_partition("tushare", "daily_basic", trade_date, local_root=tmp_path)
    write_parquet_local_atomic(generate_mock_dataset("daily_basic", trade_date), partition.local_path)

    exit_code = main(["query-parquet", "--dataset", "daily_basic", "--trade-date", trade_date, "--smoke-provider", "tushare"])
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["dataset"] == "daily_basic"
    assert output["provider"] == "tushare"
    assert output["smoke"] is True
    assert output["row_count"] > 0


def test_query_parquet_reads_provider_raw_smoke_dataset_only_from_smoke_namespace(tmp_path, monkeypatch, capsys):
    trade_date = "2026-06-19"
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    partition = build_provider_smoke_partition("akshare", "daily_price_raw_smoke", trade_date, local_root=tmp_path)
    write_parquet_local_atomic(
        pd.DataFrame(
            [
                {
                    "stock_code": "000001.SZ",
                    "trade_date": trade_date,
                    "open": 10.0,
                    "close": 10.2,
                }
            ]
        ),
        partition.local_path,
    )

    exit_code = main(["query-parquet", "--dataset", "daily_price_raw_smoke", "--trade-date", trade_date, "--smoke-provider", "akshare"])
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["dataset"] == "daily_price_raw_smoke"
    assert output["provider"] == "akshare"
    assert output["smoke"] is True
    assert output["row_count"] == 1


def test_query_parquet_reads_goal12b_tushare_smoke_candidates(tmp_path, monkeypatch, capsys):
    trade_date = "2026-06-19"
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    trade_cal_partition = build_provider_smoke_partition("tushare", "trade_cal", trade_date, local_root=tmp_path)
    suspend_d_partition = build_provider_smoke_partition("tushare", "suspend_d", trade_date, local_root=tmp_path)
    write_parquet_local_atomic(
        pd.DataFrame(
            [
                {
                    "exchange": "SSE",
                    "cal_date": "20260619",
                    "is_open": 1,
                    "pretrade_date": "20260618",
                }
            ]
        ),
        trade_cal_partition.local_path,
    )
    write_parquet_local_atomic(
        pd.DataFrame(columns=["ts_code", "trade_date", "suspend_timing", "suspend_type"]),
        suspend_d_partition.local_path,
    )

    trade_cal_exit = main(["query-parquet", "--dataset", "trade_cal", "--trade-date", trade_date, "--smoke-provider", "tushare"])
    trade_cal_output = json.loads(capsys.readouterr().out)
    suspend_exit = main(["query-parquet", "--dataset", "suspend_d", "--trade-date", trade_date, "--smoke-provider", "tushare"])
    suspend_output = json.loads(capsys.readouterr().out)

    assert trade_cal_exit == 0
    assert trade_cal_output["dataset"] == "trade_cal"
    assert trade_cal_output["provider"] == "tushare"
    assert trade_cal_output["smoke"] is True
    assert trade_cal_output["row_count"] == 1
    assert suspend_exit == 0
    assert suspend_output["dataset"] == "suspend_d"
    assert suspend_output["provider"] == "tushare"
    assert suspend_output["smoke"] is True
    assert suspend_output["row_count"] == 0


def test_query_parquet_rejects_raw_smoke_dataset_without_smoke_provider(capsys):
    exit_code = main(["query-parquet", "--dataset", "daily_price_raw_smoke", "--trade-date", "2026-06-19"])

    assert exit_code == 2
    assert "unsupported dataset: daily_price_raw_smoke" in capsys.readouterr().err

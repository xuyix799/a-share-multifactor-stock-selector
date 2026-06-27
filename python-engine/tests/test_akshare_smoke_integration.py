import os
from pathlib import Path

import pytest

from stock_selector.cli import main
from stock_selector.storage.duckdb_query import query_dataset_file
from stock_selector.storage.minio_client import create_minio_client


def test_optional_real_akshare_benchmark_smoke_writes_isolated_minio_parquet_and_duckdb_reads_it(tmp_path, capsys):
    if os.getenv("RUN_AKSHARE_SMOKE") != "1":
        pytest.skip("set RUN_AKSHARE_SMOKE=1 to run optional real AKShare smoke")
    trade_date = os.getenv("AKSHARE_SMOKE_TRADE_DATE")
    if not trade_date:
        pytest.skip("set AKSHARE_SMOKE_TRADE_DATE=YYYY-MM-DD to run optional real AKShare smoke")
    if not os.getenv("STOCK_MINIO_ACCESS_KEY") or not os.getenv("STOCK_MINIO_SECRET_KEY"):
        pytest.skip("set MinIO credentials to run optional real AKShare smoke")

    exit_code = main(
        [
            "update-provider-data",
            "--provider",
            "akshare",
            "--trade-date",
            trade_date,
            "--dataset",
            "benchmark_price",
            "--smoke",
            "--force",
        ]
    )
    assert exit_code == 0
    output = __import__("json").loads(capsys.readouterr().out)
    result = output["results"][0]
    assert result["dataset"] == "benchmark_price"
    assert result["object_key"] == f"smoke/akshare/benchmark_price/trade_date={trade_date}/part.parquet"

    settings = {
        "storage": {
            "minio_endpoint": os.getenv("STOCK_MINIO_ENDPOINT", "stock-minio:9000"),
            "minio_bucket_raw": os.getenv("STOCK_MINIO_BUCKET_RAW", "stock-raw"),
        }
    }
    minio_client = create_minio_client(settings)
    bucket = settings["storage"]["minio_bucket_raw"]
    target = Path(tmp_path) / result["object_key"]
    target.parent.mkdir(parents=True, exist_ok=True)
    minio_client.fget_object(bucket, result["object_key"], str(target))
    rows = query_dataset_file(target, limit=10)
    assert {row["index_code"] for row in rows} == {"000300.SH", "000905.SH", "000906.SH"}


def test_optional_real_akshare_daily_raw_smoke_writes_isolated_minio_parquet_and_duckdb_reads_it(tmp_path, capsys):
    if os.getenv("RUN_AKSHARE_SMOKE") != "1":
        pytest.skip("set RUN_AKSHARE_SMOKE=1 to run optional real AKShare smoke")
    trade_date = os.getenv("AKSHARE_SMOKE_TRADE_DATE")
    if not trade_date:
        pytest.skip("set AKSHARE_SMOKE_TRADE_DATE=YYYY-MM-DD to run optional real AKShare smoke")
    if not os.getenv("STOCK_MINIO_ACCESS_KEY") or not os.getenv("STOCK_MINIO_SECRET_KEY"):
        pytest.skip("set MinIO credentials to run optional real AKShare smoke")

    exit_code = main(
        [
            "update-provider-data",
            "--provider",
            "akshare",
            "--trade-date",
            trade_date,
            "--dataset",
            "daily_price_raw_smoke",
            "--smoke",
            "--force",
        ]
    )
    assert exit_code == 0
    output = __import__("json").loads(capsys.readouterr().out)
    result = output["results"][0]
    assert result["dataset"] == "daily_price_raw_smoke"
    assert result["object_key"] == f"smoke/akshare/daily_price_raw_smoke/trade_date={trade_date}/part.parquet"

    settings = {
        "storage": {
            "minio_endpoint": os.getenv("STOCK_MINIO_ENDPOINT", "stock-minio:9000"),
            "minio_bucket_raw": os.getenv("STOCK_MINIO_BUCKET_RAW", "stock-raw"),
        }
    }
    minio_client = create_minio_client(settings)
    bucket = settings["storage"]["minio_bucket_raw"]
    target = Path(tmp_path) / result["object_key"]
    target.parent.mkdir(parents=True, exist_ok=True)
    minio_client.fget_object(bucket, result["object_key"], str(target))
    rows = query_dataset_file(target, limit=10)
    assert rows[0]["stock_code"] == "000001.SZ"
    assert {"limit_up", "limit_down", "is_paused"}.isdisjoint(rows[0])

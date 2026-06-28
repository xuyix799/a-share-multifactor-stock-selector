import os
from pathlib import Path

import pytest

from stock_selector.cli import main
from stock_selector.storage.duckdb_query import query_dataset_file
from stock_selector.storage.minio_client import create_minio_client


def test_optional_real_tushare_smoke_writes_isolated_minio_parquet_and_duckdb_reads_it(tmp_path, capsys):
    if os.getenv("RUN_TUSHARE_SMOKE") != "1":
        pytest.skip("set RUN_TUSHARE_SMOKE=1 to run optional real Tushare smoke")
    if not os.getenv("TUSHARE_TOKEN"):
        pytest.skip("set TUSHARE_TOKEN to run optional real Tushare smoke")
    trade_date = os.getenv("TUSHARE_SMOKE_TRADE_DATE")
    if not trade_date:
        pytest.skip("set TUSHARE_SMOKE_TRADE_DATE=YYYY-MM-DD to run optional real Tushare smoke")
    if not os.getenv("STOCK_MINIO_ACCESS_KEY") or not os.getenv("STOCK_MINIO_SECRET_KEY"):
        pytest.skip("set MinIO credentials to run optional real Tushare smoke")

    interfaces = ["stock_basic", "daily", "stk_limit", "adj_factor", "daily_basic", "index_daily", "fina_indicator"]

    exit_code = main(
        [
            "probe-tushare-goal10r",
            "--trade-date",
            trade_date,
            "--sample-limit",
            "3",
            "--sleep-seconds",
            os.getenv("TUSHARE_SMOKE_SLEEP_SECONDS", "12"),
        ]
    )
    assert exit_code == 0
    output = __import__("json").loads(capsys.readouterr().out)
    results = output["interfaces"]

    settings = {
        "storage": {
            "minio_endpoint": os.getenv("STOCK_MINIO_ENDPOINT", "stock-minio:9000"),
            "minio_bucket_raw": os.getenv("STOCK_MINIO_BUCKET_RAW", "stock-raw"),
        }
    }
    minio_client = create_minio_client(settings)
    bucket = settings["storage"]["minio_bucket_raw"]
    for result in results:
        assert result["interface"] in interfaces
        if not result["available"]:
            continue
        assert result["object_key"].startswith(f"smoke/tushare/{result['interface']}/trade_date={trade_date}/")
        target = Path(tmp_path) / result["object_key"]
        target.parent.mkdir(parents=True, exist_ok=True)
        minio_client.fget_object(bucket, result["object_key"], str(target))
        rows = query_dataset_file(target, limit=5)
        assert rows, result["interface"]

    composition = output["daily_price_composition"]
    assert composition["standard_daily_price_possible"] is False
    assert composition["suspension_status_available"] is False
    assert "is_paused" in composition["missing_for_dq3"]

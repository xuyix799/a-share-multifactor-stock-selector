import pandas as pd

from stock_selector.backtesting.storage import write_backtest_detail


def test_write_backtest_detail_writes_local_parquet_with_run_key_partition(tmp_path, monkeypatch):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    detail = pd.DataFrame(
        [
            {
                "run_key": "abc123",
                "strategy_name": "goal8-core",
                "record_type": "portfolio",
                "trade_date": "2026-03-02",
                "total_asset": 101000.0,
            }
        ]
    )

    object_key = write_backtest_detail("abc123", detail)

    expected_path = tmp_path / "backtest" / "detail" / "run_key=abc123" / "part.parquet"
    assert object_key == expected_path.as_posix()
    written = pd.read_parquet(expected_path)
    assert written.iloc[0]["run_key"] == "abc123"
    assert written.iloc[0]["record_type"] == "portfolio"

from stock_selector.cleaning.adjust_price import build_adjusted_price
from stock_selector.cleaning.snapshot_builder import build_clean_daily_snapshot
from stock_selector.data.mock_data import generate_mock_dataset
from stock_selector.storage.duckdb_query import query_dataset_file


def test_duckdb_can_query_adjusted_price(tmp_path):
    trade_date = "2026-06-19"
    adjusted = build_adjusted_price(
        generate_mock_dataset("daily_price", trade_date),
        generate_mock_dataset("adj_factor", trade_date),
        trade_date,
    )
    parquet_path = tmp_path / "adjusted.parquet"
    adjusted.to_parquet(parquet_path, index=False)

    rows = query_dataset_file(parquet_path)

    assert len(rows) == 5
    assert "adj_close" in rows[0]


def test_duckdb_can_query_clean_daily_snapshot(tmp_path):
    trade_date = "2026-06-19"
    snapshot = build_clean_daily_snapshot(
        stock_basic=generate_mock_dataset("stock_basic", trade_date),
        daily_price=generate_mock_dataset("daily_price", trade_date),
        adj_factor=generate_mock_dataset("adj_factor", trade_date),
        daily_basic=generate_mock_dataset("daily_basic", trade_date),
        financial=generate_mock_dataset("financial", trade_date),
        st_history=generate_mock_dataset("st_history", trade_date),
        benchmark_price=generate_mock_dataset("benchmark_price", trade_date),
        trade_date=trade_date,
    )
    parquet_path = tmp_path / "snapshot.parquet"
    snapshot.to_parquet(parquet_path, index=False)

    rows = query_dataset_file(parquet_path)

    assert len(rows) == 5
    assert "is_st_on_date" in rows[0]

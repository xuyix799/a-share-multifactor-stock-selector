from factor_test_helpers import adjusted_price_history, benchmark_price_history, clean_snapshot_history, factor_input_frame
from stock_selector.factors.factor_builder import build_factor_daily
from stock_selector.storage.duckdb_query import query_dataset_file


def test_duckdb_can_query_factor_daily(tmp_path):
    trade_date = "2026-06-19"
    factor_daily = build_factor_daily(
        factor_input_table=factor_input_frame(trade_date),
        adjusted_price_history=adjusted_price_history(trade_date, days=130),
        clean_snapshot_history=clean_snapshot_history(trade_date, days=5),
        benchmark_price_history=benchmark_price_history(trade_date, days=130),
        trade_date=trade_date,
        factor_weights={},
    )
    parquet_path = tmp_path / "factor_daily.parquet"
    factor_daily.to_parquet(parquet_path, index=False)

    rows = query_dataset_file(parquet_path)

    assert rows
    assert "quality_score" in rows[0]
    assert "total_score" not in rows[0]

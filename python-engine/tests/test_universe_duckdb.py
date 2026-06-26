from stock_selector.cleaning.snapshot_builder import build_clean_daily_snapshot
from stock_selector.data.mock_data import generate_mock_dataset
from stock_selector.storage.duckdb_query import query_dataset_file
from stock_selector.universe.risk_filter import build_risk_filter
from stock_selector.universe.universe_builder import build_eligible_universe, build_factor_input_table


def _clean_snapshot(trade_date: str):
    return build_clean_daily_snapshot(
        stock_basic=generate_mock_dataset("stock_basic", trade_date),
        daily_price=generate_mock_dataset("daily_price", trade_date),
        adj_factor=generate_mock_dataset("adj_factor", trade_date),
        daily_basic=generate_mock_dataset("daily_basic", trade_date),
        financial=generate_mock_dataset("financial", trade_date),
        st_history=generate_mock_dataset("st_history", trade_date),
        benchmark_price=generate_mock_dataset("benchmark_price", trade_date),
        trade_date=trade_date,
    )


def test_duckdb_can_query_goal5_universe_tables(tmp_path):
    trade_date = "2026-06-19"
    snapshot = _clean_snapshot(trade_date)
    risk_filter = build_risk_filter(snapshot, trade_date)
    eligible = build_eligible_universe(snapshot, risk_filter, trade_date)
    factor_input = build_factor_input_table(snapshot, eligible, trade_date)

    tables = {
        "risk_filter": risk_filter,
        "eligible_universe": eligible,
        "factor_input_table": factor_input,
    }
    for dataset, df in tables.items():
        parquet_path = tmp_path / f"{dataset}.parquet"
        df.to_parquet(parquet_path, index=False)
        rows = query_dataset_file(parquet_path)
        assert rows
        assert rows[0]["trade_date"] == trade_date

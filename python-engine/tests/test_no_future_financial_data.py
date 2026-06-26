from stock_selector.cleaning.snapshot_builder import build_clean_daily_snapshot
from stock_selector.data.mock_data import generate_mock_dataset


def test_clean_snapshot_never_joins_future_announced_financial_data():
    trade_date = "2026-06-19"
    financial = generate_mock_dataset("financial", trade_date)
    financial["announce_date"] = "2026-06-20"
    snapshot = build_clean_daily_snapshot(
        stock_basic=generate_mock_dataset("stock_basic", trade_date),
        daily_price=generate_mock_dataset("daily_price", trade_date),
        adj_factor=generate_mock_dataset("adj_factor", trade_date),
        daily_basic=generate_mock_dataset("daily_basic", trade_date),
        financial=financial,
        st_history=generate_mock_dataset("st_history", trade_date),
        benchmark_price=generate_mock_dataset("benchmark_price", trade_date),
        trade_date=trade_date,
    )

    assert snapshot["announce_date"].isna().all()

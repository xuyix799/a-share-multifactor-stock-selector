from datetime import date

from stock_selector.cleaning.snapshot_builder import CLEAN_DAILY_SNAPSHOT_COLUMNS, build_clean_daily_snapshot
from stock_selector.cleaning.snapshot_validator import validate_clean_daily_snapshot
from stock_selector.data.mock_data import generate_mock_dataset


def _mock_inputs(trade_date: str) -> dict[str, object]:
    return {
        "stock_basic": generate_mock_dataset("stock_basic", trade_date),
        "daily_price": generate_mock_dataset("daily_price", trade_date),
        "adj_factor": generate_mock_dataset("adj_factor", trade_date),
        "daily_basic": generate_mock_dataset("daily_basic", trade_date),
        "financial": generate_mock_dataset("financial", trade_date),
        "st_history": generate_mock_dataset("st_history", trade_date),
        "benchmark_price": generate_mock_dataset("benchmark_price", trade_date),
    }


def test_clean_daily_snapshot_contains_expected_fields_and_valid_asof_data():
    trade_date = "2026-06-19"
    snapshot = build_clean_daily_snapshot(trade_date=trade_date, **_mock_inputs(trade_date))

    assert list(snapshot.columns) == CLEAN_DAILY_SNAPSHOT_COLUMNS
    assert len(snapshot) == 5
    assert (snapshot["announce_date"] <= trade_date).all()
    assert snapshot.set_index("stock_code").loc["000002.SZ", "is_st_on_date"] is True
    assert snapshot.set_index("stock_code").loc["000001.SZ", "listed_days"] == (date.fromisoformat(trade_date) - date.fromisoformat("2010-01-01")).days
    validate_clean_daily_snapshot(snapshot, trade_date)


def test_clean_daily_snapshot_does_not_use_stock_basic_current_is_st_snapshot():
    trade_date = "2026-06-19"
    inputs = _mock_inputs(trade_date)
    stock_basic = inputs["stock_basic"].copy()
    stock_basic.loc[stock_basic["stock_code"] == "000001.SZ", "is_st"] = True
    inputs["stock_basic"] = stock_basic

    snapshot = build_clean_daily_snapshot(trade_date=trade_date, **inputs)

    assert snapshot.set_index("stock_code").loc["000001.SZ", "is_st_on_date"] is False

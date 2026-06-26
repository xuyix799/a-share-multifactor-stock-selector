from stock_selector.cleaning.adjust_price import build_adjusted_price
from stock_selector.data.mock_data import generate_mock_dataset


def test_adjusted_price_calculates_ohlc_with_adj_factor():
    daily_price = generate_mock_dataset("daily_price", "2026-06-19")
    adj_factor = generate_mock_dataset("adj_factor", "2026-06-19")

    result = build_adjusted_price(daily_price, adj_factor, "2026-06-19")
    row = result[result["stock_code"] == "600000.SH"].iloc[0]
    source = daily_price[daily_price["stock_code"] == "600000.SH"].iloc[0]
    factor = adj_factor[adj_factor["stock_code"] == "600000.SH"].iloc[0]["adj_factor"]

    assert row["adj_close"] == source["close"] * factor
    assert row["adj_open"] == source["open"] * factor
    assert row["volume"] == source["volume"]
    assert row["limit_up"] == source["limit_up"]


def test_adjusted_price_drops_stock_without_adj_factor():
    daily_price = generate_mock_dataset("daily_price", "2026-06-19")
    adj_factor = generate_mock_dataset("adj_factor", "2026-06-19")
    adj_factor = adj_factor[adj_factor["stock_code"] != "600000.SH"]

    result = build_adjusted_price(daily_price, adj_factor, "2026-06-19")

    assert "600000.SH" not in set(result["stock_code"])
    assert len(result) == len(daily_price) - 1

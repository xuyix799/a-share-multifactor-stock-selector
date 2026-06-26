import pandas as pd

from stock_selector.cleaning.st_filter import mark_st_status


def test_st_status_uses_historical_range_and_exclusive_end_date():
    base = pd.DataFrame(
        [
            {"stock_code": "000001.SZ", "trade_date": "2026-06-19"},
            {"stock_code": "000002.SZ", "trade_date": "2026-06-19"},
            {"stock_code": "600000.SH", "trade_date": "2026-06-19"},
        ]
    )
    st_history = pd.DataFrame(
        [
            {"stock_code": "000001.SZ", "st_type": "ST", "start_date": "2026-01-01", "end_date": "2026-06-19", "source": "unit"},
            {"stock_code": "000002.SZ", "st_type": "ST", "start_date": "2026-01-01", "end_date": None, "source": "unit"},
        ]
    )

    result = mark_st_status(base, st_history, "2026-06-19")
    by_code = result.set_index("stock_code")["is_st_on_date"].to_dict()

    assert by_code["000001.SZ"] is False
    assert by_code["000002.SZ"] is True
    assert by_code["600000.SH"] is False

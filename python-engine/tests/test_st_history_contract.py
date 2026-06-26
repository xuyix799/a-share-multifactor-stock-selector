from stock_selector.providers.mock_provider import MockProvider
from stock_selector.providers.schema_contract import is_st_on_date
from stock_selector.providers.schema_mapper import map_provider_frame


def test_st_history_contract_can_answer_historical_st_state():
    raw = MockProvider().fetch_st_history("2026-06-19")
    mapped = map_provider_frame("mock", "st_history", raw, "2026-06-19")

    assert is_st_on_date(mapped, "000002.SZ", "2026-06-19") is True
    assert is_st_on_date(mapped, "000001.SZ", "2026-06-19") is False
    assert is_st_on_date(mapped, "000002.SZ", "2025-12-31") is False


def test_st_history_end_date_is_exclusive_for_backtest_date():
    import pandas as pd

    st_history = pd.DataFrame(
        [
            {
                "stock_code": "000001.SZ",
                "st_type": "ST",
                "start_date": "2026-01-01",
                "end_date": "2026-06-19",
                "source": "unit",
            }
        ]
    )

    assert is_st_on_date(st_history, "000001.SZ", "2026-06-18") is True
    assert is_st_on_date(st_history, "000001.SZ", "2026-06-19") is False


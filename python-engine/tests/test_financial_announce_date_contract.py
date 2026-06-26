import pytest

from stock_selector.data.data_validator import DataValidationError
from stock_selector.providers.mock_provider import MockProvider
from stock_selector.providers.schema_mapper import map_provider_frame


def test_financial_mapping_rejects_announce_date_after_requested_trade_date():
    raw = MockProvider().fetch_financial("2026-06-19")
    raw.loc[0, "ann_date"] = "20260620"

    with pytest.raises(DataValidationError, match="announce_date"):
        map_provider_frame("mock", "financial", raw, "2026-06-19")


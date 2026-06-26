import pytest

from stock_selector.data.data_validator import DataValidationError
from stock_selector.providers.mock_provider import MockProvider
from stock_selector.providers.schema_contract import REQUIRED_BENCHMARK_INDEXES, validate_benchmark_contract
from stock_selector.providers.schema_mapper import map_provider_frame


def test_benchmark_price_contract_contains_required_indexes():
    raw = MockProvider().fetch_benchmark_price("2026-06-19")
    mapped = map_provider_frame("mock", "benchmark_price", raw, "2026-06-19")

    assert REQUIRED_BENCHMARK_INDEXES.issubset(set(mapped["index_code"]))
    validate_benchmark_contract(mapped)


def test_benchmark_price_contract_rejects_missing_required_index():
    raw = MockProvider().fetch_benchmark_price("2026-06-19")
    raw = raw[raw["index_code"] != "000906.SH"]

    with pytest.raises(DataValidationError, match="missing benchmark indexes"):
        map_provider_frame("mock", "benchmark_price", raw, "2026-06-19")


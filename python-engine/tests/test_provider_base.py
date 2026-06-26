from stock_selector.providers.base import MarketDataProvider, PROVIDER_DATASETS
from stock_selector.storage.partition import RAW_DATASETS


def test_provider_base_declares_fetch_method_for_each_goal2_dataset():
    assert PROVIDER_DATASETS == RAW_DATASETS
    for dataset in PROVIDER_DATASETS:
        assert hasattr(MarketDataProvider, f"fetch_{dataset}")


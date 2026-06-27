import pytest

from stock_selector.providers.base import PROVIDER_DATASETS, ProviderConfigurationError
from stock_selector.providers.mock_provider import MockProvider
from stock_selector.providers.provider_factory import create_provider, list_providers
from stock_selector.providers.tushare_provider import TushareProvider


def test_mock_provider_returns_raw_frames_for_all_datasets():
    provider = MockProvider()

    for dataset in PROVIDER_DATASETS:
        df = getattr(provider, f"fetch_{dataset}")("2026-06-19")
        assert not df.empty, dataset


def test_mock_provider_returns_provider_raw_field_names_before_mapping():
    df = MockProvider().fetch_daily_price("2026-06-19")

    assert "ts_code" in df.columns
    assert "vol" in df.columns
    assert "volume" not in df.columns


def test_provider_factory_lists_mock_and_disabled_real_provider_skeletons():
    providers = list_providers()

    assert providers["mock"]["enabled"] is True
    assert providers["tushare"]["enabled"] is False
    assert providers["akshare"]["enabled"] is False
    assert providers["baostock"]["enabled"] is False
    assert create_provider("mock").name == "mock"


def test_provider_factory_can_enable_tushare_with_explicit_env_flag(monkeypatch):
    monkeypatch.setenv("STOCK_TUSHARE_ENABLED", "1")

    providers = list_providers()

    assert providers["tushare"]["enabled"] is True


def test_provider_factory_can_enable_akshare_with_explicit_env_flag(monkeypatch):
    monkeypatch.setenv("STOCK_AKSHARE_ENABLED", "1")

    providers = list_providers()

    assert providers["akshare"]["enabled"] is True


def test_real_provider_without_token_has_clear_error_but_does_not_affect_mock():
    assert create_provider("mock").name == "mock"

    with pytest.raises(ProviderConfigurationError, match="TUSHARE_TOKEN"):
        TushareProvider(settings={"provider": {"tushare": {"enabled": True, "token_env": "TUSHARE_TOKEN"}}})


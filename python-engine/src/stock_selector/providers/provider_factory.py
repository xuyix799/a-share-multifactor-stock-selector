from stock_selector.config.config_loader import load_settings
from stock_selector.providers.akshare_provider import AKShareProvider
from stock_selector.providers.baostock_provider import BaostockProvider
from stock_selector.providers.base import MarketDataProvider, ProviderConfigurationError
from stock_selector.providers.mock_provider import MockProvider
from stock_selector.providers.tushare_provider import TushareProvider


def list_providers(settings: dict | None = None) -> dict[str, dict[str, object]]:
    settings = settings or load_settings()
    provider_settings = settings.get("provider", {})
    return {
        "mock": {"enabled": True, "requires_token": False},
        "tushare": {
            "enabled": bool(provider_settings.get("tushare", {}).get("enabled", False)),
            "requires_token": True,
            "token_env": provider_settings.get("tushare", {}).get("token_env", "TUSHARE_TOKEN"),
        },
        "akshare": {"enabled": bool(provider_settings.get("akshare", {}).get("enabled", False)), "requires_token": False},
        "baostock": {"enabled": bool(provider_settings.get("baostock", {}).get("enabled", False)), "requires_token": False},
    }


def create_provider(provider_name: str | None = None, settings: dict | None = None) -> MarketDataProvider:
    settings = settings or load_settings()
    provider_name = provider_name or settings.get("provider", {}).get("default", "mock")
    provider_name = provider_name.lower()
    if provider_name == "mock":
        return MockProvider()
    if provider_name == "tushare":
        return TushareProvider(settings=settings)
    if provider_name == "akshare":
        return AKShareProvider(settings=settings)
    if provider_name == "baostock":
        return BaostockProvider(settings=settings)
    raise ProviderConfigurationError(f"unsupported provider: {provider_name}")


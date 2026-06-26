from stock_selector.config.config_loader import load_settings
from stock_selector.providers.base import ExternalProviderSkeleton, ProviderConfigurationError


class BaostockProvider(ExternalProviderSkeleton):
    name = "baostock"

    def __init__(self, settings: dict | None = None):
        settings = settings or load_settings()
        if not settings.get("provider", {}).get("baostock", {}).get("enabled", False):
            raise ProviderConfigurationError("baostock provider is disabled in settings")


from stock_selector.config.config_loader import load_settings
from stock_selector.providers.base import ExternalProviderSkeleton, ProviderConfigurationError


class AKShareProvider(ExternalProviderSkeleton):
    name = "akshare"

    def __init__(self, settings: dict | None = None):
        settings = settings or load_settings()
        if not settings.get("provider", {}).get("akshare", {}).get("enabled", False):
            raise ProviderConfigurationError("akshare provider is disabled in settings")


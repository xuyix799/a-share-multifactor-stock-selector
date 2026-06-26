import os

from stock_selector.config.config_loader import load_settings
from stock_selector.providers.base import ExternalProviderSkeleton, ProviderConfigurationError


class TushareProvider(ExternalProviderSkeleton):
    name = "tushare"

    def __init__(self, settings: dict | None = None):
        settings = settings or load_settings()
        config = settings.get("provider", {}).get("tushare", {})
        if not config.get("enabled", False):
            raise ProviderConfigurationError("tushare provider is disabled in settings")
        token_env = config.get("token_env", "TUSHARE_TOKEN")
        if not os.getenv(token_env):
            raise ProviderConfigurationError(f"missing {token_env} for tushare provider")


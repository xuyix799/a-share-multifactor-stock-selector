from abc import ABC, abstractmethod

import pandas as pd

from stock_selector.storage.partition import SUPPORTED_DATASETS, validate_dataset
from stock_selector.utils.date_validator import validate_trade_date


PROVIDER_DATASETS = SUPPORTED_DATASETS


class ProviderConfigurationError(RuntimeError):
    pass


class ProviderFetchError(RuntimeError):
    pass


class MarketDataProvider(ABC):
    name: str

    def fetch_dataset(self, dataset: str, trade_date: str) -> pd.DataFrame:
        dataset = validate_dataset(dataset)
        trade_date = validate_trade_date(trade_date)
        return getattr(self, f"fetch_{dataset}")(trade_date)

    @abstractmethod
    def fetch_stock_basic(self, trade_date: str) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def fetch_daily_price(self, trade_date: str) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def fetch_adj_factor(self, trade_date: str) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def fetch_daily_basic(self, trade_date: str) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def fetch_financial(self, trade_date: str) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def fetch_st_history(self, trade_date: str) -> pd.DataFrame:
        raise NotImplementedError

    @abstractmethod
    def fetch_benchmark_price(self, trade_date: str) -> pd.DataFrame:
        raise NotImplementedError


class ExternalProviderSkeleton(MarketDataProvider):
    def _not_implemented(self) -> pd.DataFrame:
        raise ProviderFetchError(f"{self.name} provider fetch is not implemented in Goal 3")

    def fetch_stock_basic(self, trade_date: str) -> pd.DataFrame:
        _ = validate_trade_date(trade_date)
        return self._not_implemented()

    def fetch_daily_price(self, trade_date: str) -> pd.DataFrame:
        _ = validate_trade_date(trade_date)
        return self._not_implemented()

    def fetch_adj_factor(self, trade_date: str) -> pd.DataFrame:
        _ = validate_trade_date(trade_date)
        return self._not_implemented()

    def fetch_daily_basic(self, trade_date: str) -> pd.DataFrame:
        _ = validate_trade_date(trade_date)
        return self._not_implemented()

    def fetch_financial(self, trade_date: str) -> pd.DataFrame:
        _ = validate_trade_date(trade_date)
        return self._not_implemented()

    def fetch_st_history(self, trade_date: str) -> pd.DataFrame:
        _ = validate_trade_date(trade_date)
        return self._not_implemented()

    def fetch_benchmark_price(self, trade_date: str) -> pd.DataFrame:
        _ = validate_trade_date(trade_date)
        return self._not_implemented()


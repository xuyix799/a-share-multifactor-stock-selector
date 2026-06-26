import pandas as pd

from stock_selector.data.mock_data import generate_mock_dataset
from stock_selector.providers.base import MarketDataProvider
from stock_selector.utils.date_validator import validate_trade_date


class MockProvider(MarketDataProvider):
    name = "mock"

    def fetch_stock_basic(self, trade_date: str) -> pd.DataFrame:
        df = generate_mock_dataset("stock_basic", validate_trade_date(trade_date))
        return _compact_dates(
            df.rename(columns={"stock_code": "ts_code", "stock_name": "name"}),
            ["list_date", "delist_date", "trade_date"],
        )

    def fetch_daily_price(self, trade_date: str) -> pd.DataFrame:
        df = generate_mock_dataset("daily_price", validate_trade_date(trade_date))
        return _compact_dates(
            df.rename(columns={"stock_code": "ts_code", "volume": "vol", "limit_up": "up_limit", "limit_down": "down_limit"}),
            ["trade_date"],
        )

    def fetch_adj_factor(self, trade_date: str) -> pd.DataFrame:
        df = generate_mock_dataset("adj_factor", validate_trade_date(trade_date))
        return _compact_dates(df.rename(columns={"stock_code": "ts_code"}), ["trade_date"])

    def fetch_daily_basic(self, trade_date: str) -> pd.DataFrame:
        df = generate_mock_dataset("daily_basic", validate_trade_date(trade_date))
        return _compact_dates(df.rename(columns={"stock_code": "ts_code"}), ["trade_date"])

    def fetch_financial(self, trade_date: str) -> pd.DataFrame:
        df = generate_mock_dataset("financial", validate_trade_date(trade_date))
        return _compact_dates(
            df.rename(columns={"stock_code": "ts_code", "report_period": "end_date", "announce_date": "ann_date"}),
            ["end_date", "ann_date"],
        )

    def fetch_st_history(self, trade_date: str) -> pd.DataFrame:
        df = generate_mock_dataset("st_history", validate_trade_date(trade_date))
        return _compact_dates(df.rename(columns={"stock_code": "ts_code"}), ["start_date", "end_date"])

    def fetch_benchmark_price(self, trade_date: str) -> pd.DataFrame:
        df = generate_mock_dataset("benchmark_price", validate_trade_date(trade_date))
        return _compact_dates(df, ["trade_date"])


def _compact_dates(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    result = df.copy()
    for column in columns:
        if column in result.columns:
            result[column] = result[column].map(lambda value: None if pd.isna(value) else str(value).replace("-", ""))
    return result


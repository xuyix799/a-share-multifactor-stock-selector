import pandas as pd
import pytest

from stock_selector.providers.akshare_provider import AKShareProvider
from stock_selector.providers.base import ProviderFetchError
from stock_selector.providers.schema_mapper import map_provider_frame


class FakeAKShare:
    def __init__(self):
        self.calls = []
        self.hist_calls = []

    def stock_zh_index_daily(self, symbol):
        self.calls.append(symbol)
        return pd.DataFrame(
            [
                {"date": "2024-06-18", "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 10},
                {"date": "2024-06-19", "open": 100.0, "high": 103.0, "low": 99.0, "close": 102.0, "volume": 20},
            ]
        )

    def stock_zh_a_hist(self, symbol, period, start_date, end_date, adjust):
        self.hist_calls.append(
            {
                "symbol": symbol,
                "period": period,
                "start_date": start_date,
                "end_date": end_date,
                "adjust": adjust,
            }
        )
        return pd.DataFrame(
            [
                {
                    "日期": "2024-06-19",
                    "股票代码": symbol,
                    "开盘": 10.0,
                    "收盘": 10.2,
                    "最高": 10.3,
                    "最低": 9.9,
                    "成交量": 1000,
                    "成交额": 10200.0,
                    "振幅": 4.0,
                    "涨跌幅": 2.0,
                    "涨跌额": 0.2,
                    "换手率": 1.1,
                }
            ]
        )


def _provider(monkeypatch, ak_module):
    monkeypatch.setenv("STOCK_AKSHARE_ENABLED", "1")
    return AKShareProvider(settings={"provider": {"akshare": {"enabled": False}}}, ak_module=ak_module)


def test_akshare_provider_fetches_benchmark_price_and_maps_to_standard_schema(monkeypatch):
    fake = FakeAKShare()
    provider = _provider(monkeypatch, fake)

    raw = provider.fetch_benchmark_price("2024-06-19")
    mapped = map_provider_frame("akshare", "benchmark_price", raw, "2024-06-19")

    assert set(mapped["index_code"]) == {"000300.SH", "000905.SH", "000906.SH"}
    assert set(fake.calls) == {"sh000300", "sh000905", "sh000906"}
    assert mapped["trade_date"].unique().tolist() == ["2024-06-19"]
    assert mapped.loc[mapped["index_code"] == "000300.SH", "pct_chg"].iloc[0] == pytest.approx(2.0)


def test_akshare_stock_basic_reports_capability_gap(monkeypatch):
    provider = _provider(monkeypatch, FakeAKShare())

    with pytest.raises(ProviderFetchError, match="provider capability insufficient.*stock_basic.*industry.*market_type"):
        provider.fetch_stock_basic("2024-06-19")


def test_akshare_daily_price_reports_limit_price_capability_gap(monkeypatch):
    provider = _provider(monkeypatch, FakeAKShare())

    with pytest.raises(ProviderFetchError, match="provider capability insufficient.*daily_price.*limit_up.*limit_down"):
        provider.fetch_daily_price("2024-06-19")


def test_akshare_provider_fetches_daily_price_raw_smoke_without_faking_standard_fields(monkeypatch):
    fake = FakeAKShare()
    provider = _provider(monkeypatch, fake)

    raw = provider.fetch_dataset("daily_price_raw_smoke", "2024-06-19")

    assert fake.hist_calls == [
        {
            "symbol": "000001",
            "period": "daily",
            "start_date": "20240619",
            "end_date": "20240619",
            "adjust": "",
        }
    ]
    assert raw.to_dict(orient="records") == [
        {
            "stock_code": "000001.SZ",
            "trade_date": "2024-06-19",
            "open": 10.0,
            "high": 10.3,
            "low": 9.9,
            "close": 10.2,
            "volume": 1000,
            "amount": 10200.0,
            "pct_chg": 2.0,
            "source_symbol": "000001",
        }
    ]
    assert {"limit_up", "limit_down", "is_paused"}.isdisjoint(raw.columns)

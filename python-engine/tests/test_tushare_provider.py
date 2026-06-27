import pandas as pd
import pytest

from stock_selector.providers.base import ProviderFetchError
from stock_selector.providers.schema_mapper import map_provider_frame
from stock_selector.providers.tushare_provider import TushareProvider


class FakeTusharePro:
    def __init__(self):
        self.calls = []

    def stock_basic(self, **kwargs):
        self.calls.append(("stock_basic", kwargs))
        return pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "name": "平安银行",
                    "exchange": "SZSE",
                    "industry": "银行",
                    "market": "主板",
                    "list_date": "19910403",
                }
            ]
        )

    def daily(self, **kwargs):
        self.calls.append(("daily", kwargs))
        return pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20260619",
                    "open": 10.0,
                    "high": 10.5,
                    "low": 9.9,
                    "close": 10.2,
                    "pre_close": 10.0,
                    "pct_chg": 2.0,
                    "vol": 1000.0,
                    "amount": 10200.0,
                }
            ]
        )

    def stk_limit(self, **kwargs):
        self.calls.append(("stk_limit", kwargs))
        return pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20260619",
                    "up_limit": 11.0,
                    "down_limit": 9.0,
                }
            ]
        )

    def adj_factor(self, **kwargs):
        self.calls.append(("adj_factor", kwargs))
        return pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20260619", "adj_factor": 1.23}])

    def daily_basic(self, **kwargs):
        self.calls.append(("daily_basic", kwargs))
        return pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20260619",
                    "pe_ttm": 6.5,
                    "pb": 0.7,
                    "ps_ttm": 1.2,
                    "total_mv": 100000.0,
                    "circ_mv": 90000.0,
                    "turnover_rate": 0.8,
                }
            ]
        )


class EmptyTusharePro:
    def stock_basic(self, **kwargs):
        return pd.DataFrame()


class RateLimitedTusharePro:
    def __init__(self):
        self.calls = 0

    def stock_basic(self, **kwargs):
        self.calls += 1
        raise Exception("抱歉，您访问接口(stock_basic)频率超限(1次/分钟)，具体频次详情：https://tushare.pro/document/1?doc_id=108。")


def _provider(monkeypatch, pro_client):
    monkeypatch.setenv("TUSHARE_TOKEN", "fake-token")
    settings = {
        "provider": {
            "retry": {"max_attempts": 1, "backoff_seconds": 0},
            "tushare": {"enabled": True, "token_env": "TUSHARE_TOKEN"},
        }
    }
    return TushareProvider(settings=settings, pro_client=pro_client)


@pytest.mark.parametrize("dataset", ["stock_basic", "daily_price", "adj_factor", "daily_basic"])
def test_tushare_provider_fetches_goal10_smoke_datasets_and_maps_to_standard_schema(monkeypatch, dataset):
    provider = _provider(monkeypatch, FakeTusharePro())

    raw = provider.fetch_dataset(dataset, "2026-06-19")
    mapped = map_provider_frame("tushare", dataset, raw, "2026-06-19")

    assert not mapped.empty
    assert mapped.iloc[0]["stock_code"] == "000001.SZ"
    if "trade_date" in mapped.columns:
        assert mapped.iloc[0]["trade_date"] == "2026-06-19"


def test_tushare_daily_price_uses_trade_date_and_limit_endpoint(monkeypatch):
    fake = FakeTusharePro()
    provider = _provider(monkeypatch, fake)

    raw = provider.fetch_daily_price("2026-06-19")

    assert raw.iloc[0]["up_limit"] == 11.0
    assert raw.iloc[0]["down_limit"] == 9.0
    assert ("daily", {"trade_date": "20260619"}) in [(name, {"trade_date": call["trade_date"]}) for name, call in fake.calls if "trade_date" in call]
    assert any(name == "stk_limit" and call["trade_date"] == "20260619" for name, call in fake.calls)


def test_tushare_provider_rejects_non_smoke_datasets(monkeypatch):
    provider = _provider(monkeypatch, FakeTusharePro())

    with pytest.raises(ProviderFetchError, match="not supported by Goal 10 Tushare smoke"):
        provider.fetch_financial("2026-06-19")


def test_tushare_empty_endpoint_error_mentions_permission_and_date_checks(monkeypatch):
    provider = _provider(monkeypatch, EmptyTusharePro())

    with pytest.raises(ProviderFetchError, match="check Tushare token permission and trade_date"):
        provider.fetch_stock_basic("2026-06-19")


def test_tushare_rate_limit_errors_are_not_retried(monkeypatch):
    fake = RateLimitedTusharePro()
    monkeypatch.setenv("TUSHARE_TOKEN", "fake-token")
    provider = TushareProvider(
        settings={
            "provider": {
                "retry": {"max_attempts": 3, "backoff_seconds": 0},
                "tushare": {"enabled": True, "token_env": "TUSHARE_TOKEN"},
            }
        },
        pro_client=fake,
    )

    with pytest.raises(ProviderFetchError, match="频率超限"):
        provider.fetch_stock_basic("2026-06-19")
    assert fake.calls == 1

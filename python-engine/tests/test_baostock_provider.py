import pytest

from stock_selector.providers.baostock_provider import BaostockProvider
from stock_selector.providers.base import ProviderFetchError


class FakeLoginResult:
    error_code = "0"
    error_msg = ""


class FakeQueryResult:
    fields = ["date", "code", "open", "high", "low", "close", "preclose", "volume", "amount", "pctChg"]

    def __init__(self):
        self.error_code = "0"
        self.error_msg = ""
        self._rows = [
            ["2024-06-19", "sz.000001", "10.0", "10.3", "9.9", "10.2", "10.0", "1000", "10200.0", "2.0"]
        ]
        self._index = -1

    def next(self):
        self._index += 1
        return self._index < len(self._rows)

    def get_row_data(self):
        return self._rows[self._index]


class FakeBaostock:
    def __init__(self):
        self.queries = []
        self.logged_out = False

    def login(self):
        return FakeLoginResult()

    def logout(self):
        self.logged_out = True

    def query_history_k_data_plus(self, code, fields, start_date, end_date, frequency, adjustflag):
        self.queries.append(
            {
                "code": code,
                "fields": fields,
                "start_date": start_date,
                "end_date": end_date,
                "frequency": frequency,
                "adjustflag": adjustflag,
            }
        )
        return FakeQueryResult()


def _provider(monkeypatch, bs_module):
    monkeypatch.setenv("STOCK_BAOSTOCK_ENABLED", "1")
    return BaostockProvider(settings={"provider": {"baostock": {"enabled": False}}}, bs_module=bs_module)


def test_baostock_daily_price_reports_limit_and_pause_capability_gap(monkeypatch):
    provider = _provider(monkeypatch, FakeBaostock())

    with pytest.raises(ProviderFetchError, match="provider capability insufficient.*daily_price.*limit_up.*limit_down.*is_paused"):
        provider.fetch_daily_price("2024-06-19")


def test_baostock_provider_fetches_daily_price_raw_smoke_without_faking_standard_fields(monkeypatch):
    fake = FakeBaostock()
    provider = _provider(monkeypatch, fake)

    raw = provider.fetch_dataset("daily_price_raw_smoke", "2024-06-19")

    assert fake.queries == [
        {
            "code": "sz.000001",
            "fields": "date,code,open,high,low,close,preclose,volume,amount,pctChg",
            "start_date": "2024-06-19",
            "end_date": "2024-06-19",
            "frequency": "d",
            "adjustflag": "3",
        }
    ]
    assert fake.logged_out is True
    assert raw.to_dict(orient="records") == [
        {
            "stock_code": "000001.SZ",
            "trade_date": "2024-06-19",
            "open": 10.0,
            "high": 10.3,
            "low": 9.9,
            "close": 10.2,
            "volume": 1000.0,
            "amount": 10200.0,
            "pct_chg": 2.0,
            "source_symbol": "sz.000001",
        }
    ]
    assert {"limit_up", "limit_down", "is_paused"}.isdisjoint(raw.columns)

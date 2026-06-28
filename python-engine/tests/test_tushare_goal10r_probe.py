import pandas as pd

from stock_selector.providers.tushare_goal10r_probe import GOAL10R_INTERFACES, probe_tushare_goal10r
from stock_selector.providers.tushare_provider import TushareProvider


class FakeGoal10RTusharePro:
    def __init__(self):
        self.calls = []

    def stock_basic(self, **kwargs):
        self.calls.append(("stock_basic", kwargs))
        return pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "name": "平安银行",
                    "industry": "银行",
                    "market": "主板",
                    "list_date": "19910403",
                },
                {
                    "ts_code": "600000.SH",
                    "name": "浦发银行",
                    "industry": "银行",
                    "market": "主板",
                    "list_date": "19991110",
                },
            ]
        )

    def daily(self, **kwargs):
        self.calls.append(("daily", kwargs))
        return pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20240619",
                    "open": 10.1,
                    "high": 10.21,
                    "low": 10.08,
                    "close": 10.15,
                    "pre_close": 10.08,
                    "pct_chg": 0.69,
                    "vol": 796184.0,
                    "amount": 807676.1922,
                }
            ]
        )

    def stk_limit(self, **kwargs):
        self.calls.append(("stk_limit", kwargs))
        return pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20240619",
                    "up_limit": 11.09,
                    "down_limit": 9.07,
                }
            ]
        )

    def adj_factor(self, **kwargs):
        self.calls.append(("adj_factor", kwargs))
        return pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20240619", "adj_factor": 125.5}])

    def daily_basic(self, **kwargs):
        self.calls.append(("daily_basic", kwargs))
        return pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20240619",
                    "pe_ttm": 4.8,
                    "pb": 0.52,
                    "ps_ttm": 1.1,
                    "total_mv": 19600000.0,
                    "circ_mv": 19600000.0,
                    "turnover_rate": 0.41,
                }
            ]
        )

    def index_daily(self, **kwargs):
        self.calls.append(("index_daily", kwargs))
        return pd.DataFrame(
            [
                {
                    "ts_code": "000300.SH",
                    "trade_date": "20240619",
                    "open": 3543.36,
                    "high": 3543.36,
                    "low": 3524.845,
                    "close": 3528.749,
                    "pre_close": 3545.589,
                    "pct_chg": -0.474984,
                }
            ]
        )

    def fina_indicator(self, **kwargs):
        self.calls.append(("fina_indicator", kwargs))
        return pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "ann_date": "20240420",
                    "end_date": "20240331",
                    "or_yoy": 1.2,
                    "netprofit_yoy": 2.3,
                    "roe": 3.4,
                    "grossprofit_margin": 4.5,
                    "debt_to_assets": 90.1,
                    "ocfps": 0.8,
                }
            ]
        )


class RateLimitedDailyPro(FakeGoal10RTusharePro):
    def __init__(self):
        super().__init__()
        self.daily_calls = 0

    def daily(self, **kwargs):
        self.calls.append(("daily", kwargs))
        self.daily_calls += 1
        raise Exception("抱歉，您访问接口(daily)频率超限(1次/分钟)。")


def _provider(monkeypatch, pro_client):
    monkeypatch.setenv("TUSHARE_TOKEN", "fake-token")
    return TushareProvider(
        settings={
            "provider": {
                "retry": {"max_attempts": 3, "backoff_seconds": 0},
                "tushare": {"enabled": True, "token_env": "TUSHARE_TOKEN"},
            }
        },
        pro_client=pro_client,
    )


def test_goal10r_probe_writes_all_interfaces_to_tushare_smoke_and_reports_dq2_not_dq3(monkeypatch):
    provider = _provider(monkeypatch, FakeGoal10RTusharePro())
    writes = []
    sleeps = []

    def writer(dataset, trade_date, df):
        writes.append((dataset, trade_date, len(df), tuple(df.columns)))
        return f"smoke/tushare/{dataset}/trade_date={trade_date}/part.parquet"

    result = probe_tushare_goal10r(
        provider,
        "2024-06-19",
        write_dataset_fn=writer,
        sample_limit=1,
        sleep_seconds=0.5,
        sleeper=lambda seconds: sleeps.append(seconds),
    )

    assert [item["interface"] for item in result["interfaces"]] == list(GOAL10R_INTERFACES)
    assert [item[0] for item in writes] == list(GOAL10R_INTERFACES)
    assert all(item[1] == "2024-06-19" for item in writes)
    assert all(item[2] == 1 for item in writes)
    assert sleeps == [0.5] * (len(GOAL10R_INTERFACES) - 1)

    daily = _by_interface(result, "daily")
    assert daily["available"] is True
    assert daily["schema_satisfied"] is False
    assert daily["missing_for_current_schema"] == ["is_paused", "limit_down", "limit_up"]
    assert daily["can_enter_dq0_smoke"] is True
    assert daily["can_enter_dq1"] is True
    assert daily["can_enter_dq2"] is False
    assert daily["can_enter_dq3"] is False

    composition = result["daily_price_composition"]
    assert composition["price_fields_complete"] is True
    assert composition["limit_fields_complete"] is True
    assert composition["adj_factor_available"] is True
    assert composition["daily_basic_available"] is True
    assert composition["suspension_status_available"] is False
    assert composition["standard_daily_price_possible"] is False
    assert composition["max_dq_level"] == "DQ2"
    assert composition["missing_for_dq3"] == ["is_paused"]


def test_goal10r_probe_does_not_retry_frequency_errors(monkeypatch):
    fake = RateLimitedDailyPro()
    provider = _provider(monkeypatch, fake)

    result = probe_tushare_goal10r(
        provider,
        "2024-06-19",
        write_dataset_fn=lambda dataset, trade_date, df: f"smoke/tushare/{dataset}/trade_date={trade_date}/part.parquet",
        sample_limit=1,
        sleep_seconds=0,
        sleeper=lambda seconds: None,
    )

    daily = _by_interface(result, "daily")
    assert daily["available"] is False
    assert daily["error_class"] == "ProviderFetchError"
    assert "频率超限" in daily["error"]
    assert fake.daily_calls == 1


def _by_interface(result, interface):
    return next(item for item in result["interfaces"] if item["interface"] == interface)

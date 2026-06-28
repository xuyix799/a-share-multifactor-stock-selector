import pandas as pd

from stock_selector.data.quality_contract import (
    DataQualityLevel,
    classify_tushare_suspension_status_candidate,
)
from stock_selector.providers.tushare_goal12b_probe import (
    GOAL12B_INTERFACES,
    ProbeStatus,
    probe_tushare_goal12b,
)
from stock_selector.providers.tushare_provider import TushareProvider


class FakeGoal12BTusharePro:
    def __init__(self, suspend_rows=None):
        self.calls = []
        self.suspend_rows = suspend_rows

    def trade_cal(self, **kwargs):
        self.calls.append(("trade_cal", kwargs))
        return pd.DataFrame(
            [
                {
                    "exchange": "SSE",
                    "cal_date": "20240619",
                    "is_open": 1,
                    "pretrade_date": "20240618",
                }
            ]
        )

    def suspend_d(self, **kwargs):
        self.calls.append(("suspend_d", kwargs))
        if self.suspend_rows is not None:
            return pd.DataFrame(self.suspend_rows, columns=["ts_code", "trade_date", "suspend_timing", "suspend_type"])
        return pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20240619",
                    "suspend_timing": "09:30:00",
                    "suspend_type": "S",
                }
            ]
        )


class EmptySuspendGoal12BTusharePro(FakeGoal12BTusharePro):
    def suspend_d(self, **kwargs):
        self.calls.append(("suspend_d", kwargs))
        return pd.DataFrame(columns=["ts_code", "trade_date", "suspend_timing", "suspend_type"])


class SchemaMismatchGoal12BTusharePro(FakeGoal12BTusharePro):
    def suspend_d(self, **kwargs):
        self.calls.append(("suspend_d", kwargs))
        return pd.DataFrame([{"ts_code": "000001.SZ", "trade_date": "20240619"}])


class BlockedGoal12BTusharePro(FakeGoal12BTusharePro):
    def suspend_d(self, **kwargs):
        self.calls.append(("suspend_d", kwargs))
        raise Exception("抱歉，您没有接口(suspend_d)访问权限")


class ApiErrorGoal12BTusharePro(FakeGoal12BTusharePro):
    def suspend_d(self, **kwargs):
        self.calls.append(("suspend_d", kwargs))
        raise RuntimeError("temporary upstream timeout")


def _provider(monkeypatch, pro_client):
    monkeypatch.setenv("TUSHARE_TOKEN", "fake-token")
    return TushareProvider(
        settings={
            "provider": {
                "retry": {"max_attempts": 1, "backoff_seconds": 0},
                "tushare": {"enabled": True, "token_env": "TUSHARE_TOKEN"},
            }
        },
        pro_client=pro_client,
    )


def test_goal12b_probe_writes_trade_cal_and_suspend_d_smoke_and_marks_candidate_not_dq3(monkeypatch):
    provider = _provider(monkeypatch, FakeGoal12BTusharePro())
    writes = []
    sleeps = []

    def writer(dataset, trade_date, df):
        writes.append((dataset, trade_date, len(df), tuple(df.columns)))
        return f"smoke/tushare/{dataset}/trade_date={trade_date}/part.parquet"

    result = probe_tushare_goal12b(
        provider,
        "2024-06-19",
        write_dataset_fn=writer,
        sample_limit=1,
        sleep_seconds=0.25,
        sleeper=lambda seconds: sleeps.append(seconds),
    )

    assert [item["interface"] for item in result["interfaces"]] == list(GOAL12B_INTERFACES)
    assert [item[0] for item in writes] == ["trade_cal", "suspend_d"]
    assert all(item[1] == "2024-06-19" for item in writes)
    assert sleeps == [0.25]

    trade_cal = _by_interface(result, "trade_cal")
    assert trade_cal["status"] == ProbeStatus.PASS_WITH_ROWS.value
    assert trade_cal["object_key"] == "smoke/tushare/trade_cal/trade_date=2024-06-19/part.parquet"
    assert trade_cal["contract_role"] == "trading_calendar_candidate"

    suspend_d = _by_interface(result, "suspend_d")
    assert suspend_d["status"] == ProbeStatus.PASS_WITH_ROWS.value
    assert suspend_d["object_key"] == "smoke/tushare/suspend_d/trade_date=2024-06-19/part.parquet"
    assert suspend_d["contract_role"] == "suspension_status_candidate"
    assert suspend_d["hit_means_is_paused_true_candidate"] is True
    assert suspend_d["miss_means_is_paused_false_candidate"] is False

    contract = result["suspension_status_candidate_contract"]
    assert contract["provider"] == "tushare"
    assert contract["source_interface"] == "suspend_d"
    assert contract["candidate_dataset"] == "suspension_status_candidate"
    assert contract["event_source_candidate_available"] is True
    assert contract["dq_level"] == "DQ1"
    assert contract["standard_suspension_status_ready"] is False
    assert contract["standard_daily_price_ready"] is False
    assert contract["standard_daily_price_written"] is False
    assert contract["real_backtest_allowed"] is False
    assert "staging" in contract["required_future_gates"]
    assert "join_dry_run" in contract["required_future_gates"]
    assert "coverage_audit" in contract["required_future_gates"]
    assert "validator_verification" in contract["required_future_gates"]


def test_goal12b_probe_treats_empty_suspend_d_as_pass_empty_without_false_pause_inference(monkeypatch):
    provider = _provider(monkeypatch, EmptySuspendGoal12BTusharePro())
    writes = []

    result = probe_tushare_goal12b(
        provider,
        "2024-06-19",
        write_dataset_fn=lambda dataset, trade_date, df: writes.append((dataset, len(df))) or f"smoke/tushare/{dataset}/trade_date={trade_date}/part.parquet",
        sample_limit=1,
        sleep_seconds=0,
        sleeper=lambda seconds: None,
    )

    suspend_d = _by_interface(result, "suspend_d")
    assert suspend_d["status"] == ProbeStatus.PASS_EMPTY.value
    assert suspend_d["row_count"] == 0
    assert suspend_d["object_key"] == "smoke/tushare/suspend_d/trade_date=2024-06-19/part.parquet"
    assert ("suspend_d", 0) in writes
    assert suspend_d["hit_means_is_paused_true_candidate"] is False
    assert suspend_d["miss_means_is_paused_false_candidate"] is False
    assert result["suspension_status_candidate_contract"]["event_source_candidate_available"] is True
    assert result["suspension_status_candidate_contract"]["standard_daily_price_ready"] is False


def test_goal12b_probe_distinguishes_schema_mismatch_blocked_and_api_error(monkeypatch):
    schema_result = probe_tushare_goal12b(
        _provider(monkeypatch, SchemaMismatchGoal12BTusharePro()),
        "2024-06-19",
        write_dataset_fn=lambda dataset, trade_date, df: "unexpected",
        sample_limit=1,
        sleep_seconds=0,
        sleeper=lambda seconds: None,
    )
    assert _by_interface(schema_result, "suspend_d")["status"] == ProbeStatus.SCHEMA_MISMATCH.value
    assert _by_interface(schema_result, "suspend_d")["object_key"] is None

    blocked_result = probe_tushare_goal12b(
        _provider(monkeypatch, BlockedGoal12BTusharePro()),
        "2024-06-19",
        write_dataset_fn=lambda dataset, trade_date, df: "unexpected",
        sample_limit=1,
        sleep_seconds=0,
        sleeper=lambda seconds: None,
    )
    assert _by_interface(blocked_result, "suspend_d")["status"] == ProbeStatus.BLOCKED.value

    api_error_result = probe_tushare_goal12b(
        _provider(monkeypatch, ApiErrorGoal12BTusharePro()),
        "2024-06-19",
        write_dataset_fn=lambda dataset, trade_date, df: "unexpected",
        sample_limit=1,
        sleep_seconds=0,
        sleeper=lambda seconds: None,
    )
    assert _by_interface(api_error_result, "suspend_d")["status"] == ProbeStatus.API_ERROR.value


def test_tushare_suspension_status_candidate_contract_never_promotes_to_false_or_daily_price():
    contract = classify_tushare_suspension_status_candidate(
        smoke_status=ProbeStatus.PASS_WITH_ROWS.value,
        fields={"ts_code", "trade_date", "suspend_timing", "suspend_type"},
    )

    assert contract.dq_level == DataQualityLevel.DQ1
    assert contract.event_source_candidate_available is True
    assert contract.hit_means_is_paused_true_candidate is True
    assert contract.miss_means_is_paused_false_candidate is False
    assert contract.standard_suspension_status_ready is False
    assert contract.standard_daily_price_ready is False
    assert contract.real_backtest_allowed is False


def _by_interface(result, interface):
    return next(item for item in result["interfaces"] if item["interface"] == interface)

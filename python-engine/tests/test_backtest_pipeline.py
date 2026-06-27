import pandas as pd

from stock_selector.backtesting.backtest_pipeline import BacktestConfig, run_backtest


class FakeSummaryRepository:
    def __init__(self):
        self.rows = {}
        self.upserts = []

    def find_done(self, run_key):
        return self.rows.get(run_key)

    def upsert_done(self, summary):
        self.rows[summary["run_key"]] = dict(summary)
        self.upserts.append(dict(summary))


def _selection_history():
    return pd.DataFrame(
        [
            {
                "stock_code": "000001.SZ",
                "trade_date": "2026-01-30",
                "industry": "bank",
                "market_type": "main",
                "quality_score": 80.0,
                "growth_score": 70.0,
                "valuation_score": 60.0,
                "trend_score": 65.0,
                "industry_score": 55.0,
                "total_score": 72.0,
                "risk_level": "LOW",
                "rank": 1,
                "suggestion": "watch",
                "reason": "mock",
                "exclude_reasons": "",
                "risk_flags": "",
            },
            {
                "stock_code": "600519.SH",
                "trade_date": "2026-02-27",
                "industry": "consumer",
                "market_type": "main",
                "quality_score": 90.0,
                "growth_score": 80.0,
                "valuation_score": 70.0,
                "trend_score": 75.0,
                "industry_score": 65.0,
                "total_score": 82.0,
                "risk_level": "LOW",
                "rank": 1,
                "suggestion": "watch",
                "reason": "mock",
                "exclude_reasons": "",
                "risk_flags": "",
            },
        ]
    )


def _price_history():
    rows = []
    for trade_date, one_open, one_close, mao_open, mao_close in [
        ("2026-01-30", 10.0, 10.0, 100.0, 100.0),
        ("2026-02-02", 10.0, 11.0, 100.0, 100.0),
        ("2026-02-27", 11.0, 12.0, 101.0, 102.0),
        ("2026-03-02", 12.0, 12.5, 102.0, 103.0),
    ]:
        rows.extend(
            [
                {
                    "stock_code": "000001.SZ",
                    "trade_date": trade_date,
                    "adj_open": one_open,
                    "adj_high": one_open + 1,
                    "adj_low": one_open - 1,
                    "adj_close": one_close,
                    "volume": 1000000,
                    "amount": 10000000,
                    "pct_chg": 1.0,
                    "is_paused": False,
                    "limit_up": one_open * 1.1,
                    "limit_down": one_open * 0.9,
                },
                {
                    "stock_code": "600519.SH",
                    "trade_date": trade_date,
                    "adj_open": mao_open,
                    "adj_high": mao_open + 1,
                    "adj_low": mao_open - 1,
                    "adj_close": mao_close,
                    "volume": 1000000,
                    "amount": 100000000,
                    "pct_chg": 1.0,
                    "is_paused": False,
                    "limit_up": mao_open * 1.1,
                    "limit_down": mao_open * 0.9,
                },
            ]
        )
    return pd.DataFrame(rows)


def _daily_price_history():
    rows = []
    for trade_date, one_open, mao_open in [
        ("2026-01-30", 10.0, 100.0),
        ("2026-02-02", 10.0, 100.0),
        ("2026-02-27", 11.0, 101.0),
        ("2026-03-02", 12.0, 102.0),
    ]:
        rows.extend(
            [
                {
                    "stock_code": "000001.SZ",
                    "trade_date": trade_date,
                    "open": one_open,
                    "limit_up": one_open * 1.1,
                    "limit_down": one_open * 0.9,
                },
                {
                    "stock_code": "600519.SH",
                    "trade_date": trade_date,
                    "open": mao_open,
                    "limit_up": mao_open * 1.1,
                    "limit_down": mao_open * 0.9,
                },
            ]
        )
    return pd.DataFrame(rows)


def _benchmark_history():
    rows = []
    for index_code, start_close, end_close in [
        ("000300.SH", 4000.0, 4040.0),
        ("000905.SH", 5000.0, 5100.0),
        ("000906.SH", 6000.0, 6180.0),
    ]:
        rows.extend(
            [
                {"index_code": index_code, "trade_date": "2026-01-30", "open": start_close, "high": start_close, "low": start_close, "close": start_close, "pct_chg": 0.0},
                {"index_code": index_code, "trade_date": "2026-03-02", "open": end_close, "high": end_close, "low": end_close, "close": end_close, "pct_chg": 0.0},
            ]
        )
    return pd.DataFrame(rows)


def test_backtest_pipeline_writes_detail_summary_and_skips_done_unless_force():
    summary_repo = FakeSummaryRepository()
    writes = []
    history = {
        "selection_result": _selection_history(),
        "adjusted_price": _price_history(),
        "daily_price": _daily_price_history(),
        "benchmark_price": _benchmark_history(),
    }

    def read_history(dataset, start_date, end_date):
        return history[dataset]

    def write_detail(run_key, detail):
        writes.append((run_key, detail.copy()))
        return f"backtest_detail/run_key={run_key}/part.parquet"

    config = BacktestConfig(
        strategy_name="goal8-core",
        start_date="2026-01-01",
        end_date="2026-03-02",
        rebalance_mode="monthly",
        initial_cash=100000.0,
        commission_rate=0.001,
        slippage_bps=0.0,
        stamp_tax_rate=0.001,
        top_n=50,
        execution_rule="next_open",
    )

    first = run_backtest(config, read_history_fn=read_history, write_detail_fn=write_detail, summary_repo=summary_repo)
    skipped = run_backtest(config, read_history_fn=read_history, write_detail_fn=write_detail, summary_repo=summary_repo)
    forced = run_backtest(config, force=True, read_history_fn=read_history, write_detail_fn=write_detail, summary_repo=summary_repo)

    assert first["status"] == "done"
    assert skipped["status"] == "skipped"
    assert forced["status"] == "done"
    assert len(writes) == 2
    assert len(summary_repo.upserts) == 2
    assert first["detail_object_key"].startswith("backtest_detail/run_key=")
    assert first["run_key"] == skipped["run_key"] == forced["run_key"]
    assert {"total_return", "max_drawdown", "benchmark_returns", "excess_returns", "cost_total", "trade_count"}.issubset(first["metrics"])

    trade_detail = writes[0][1][writes[0][1]["record_type"] == "trade"]
    assert set(trade_detail["signal_date"]) == {"2026-01-30", "2026-02-27"}
    assert set(trade_detail["execution_date"]) == {"2026-02-02", "2026-03-02"}
    assert set(first["metrics"]["benchmark_returns"]) == {"000300.SH", "000905.SH", "000906.SH"}


def test_run_key_changes_when_top_n_or_execution_rule_changes():
    from stock_selector.backtesting.backtest_pipeline import build_run_key

    base = BacktestConfig(
        strategy_name="goal8-core",
        start_date="2026-01-01",
        end_date="2026-03-02",
        rebalance_mode="monthly",
        initial_cash=100000.0,
        commission_rate=0.001,
        slippage_bps=0.0,
        stamp_tax_rate=0.001,
        top_n=50,
        execution_rule="next_open",
    )
    different_top_n = BacktestConfig(**{**base.normalized(), "top_n": 10})
    different_execution = BacktestConfig(**{**base.normalized(), "execution_rule": "next_open_v2"})

    assert build_run_key(base) != build_run_key(different_top_n)
    assert build_run_key(base) != build_run_key(different_execution)

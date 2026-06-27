import pandas as pd
import pytest

from stock_selector.backtesting.metrics import calculate_backtest_metrics


def test_metrics_include_portfolio_return_drawdown_costs_and_required_benchmarks():
    portfolio = pd.DataFrame(
        [
            {"record_type": "portfolio", "trade_date": "2026-01-02", "total_asset": 100000.0},
            {"record_type": "portfolio", "trade_date": "2026-01-03", "total_asset": 90000.0},
            {"record_type": "portfolio", "trade_date": "2026-01-04", "total_asset": 110000.0},
        ]
    )
    trades = pd.DataFrame(
        [
            {"record_type": "trade", "commission": 10.0, "stamp_tax": 0.0},
            {"record_type": "trade", "commission": 8.0, "stamp_tax": 3.0},
        ]
    )
    benchmark = pd.DataFrame(
        [
            {"index_code": "000300.SH", "trade_date": "2026-01-02", "close": 100.0},
            {"index_code": "000300.SH", "trade_date": "2026-01-04", "close": 105.0},
            {"index_code": "000905.SH", "trade_date": "2026-01-02", "close": 200.0},
            {"index_code": "000905.SH", "trade_date": "2026-01-04", "close": 210.0},
            {"index_code": "000906.SH", "trade_date": "2026-01-02", "close": 300.0},
            {"index_code": "000906.SH", "trade_date": "2026-01-04", "close": 330.0},
        ]
    )

    metrics = calculate_backtest_metrics(portfolio, trades, benchmark)

    assert metrics["total_return"] == pytest.approx(0.10)
    assert metrics["period_return"] == pytest.approx(0.10)
    assert metrics["max_drawdown"] == pytest.approx(-0.10)
    assert metrics["cost_total"] == pytest.approx(21.0)
    assert metrics["trade_count"] == 2
    assert metrics["benchmark_returns"]["000300.SH"] == pytest.approx(0.05)
    assert metrics["benchmark_returns"]["000905.SH"] == pytest.approx(0.05)
    assert metrics["benchmark_returns"]["000906.SH"] == pytest.approx(0.10)
    assert metrics["excess_returns"]["000300.SH"] == pytest.approx(0.05)

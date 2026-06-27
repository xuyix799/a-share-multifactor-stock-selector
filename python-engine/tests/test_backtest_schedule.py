from stock_selector.backtesting.schedule import RebalanceEvent, build_rebalance_schedule


def test_monthly_rebalance_uses_month_end_signal_and_next_trade_execution():
    trade_dates = [
        "2026-01-02",
        "2026-01-30",
        "2026-02-02",
        "2026-02-27",
        "2026-03-02",
        "2026-03-31",
        "2026-04-01",
    ]

    schedule = build_rebalance_schedule(
        trade_dates=trade_dates,
        start_date="2026-01-01",
        end_date="2026-04-01",
        rebalance_mode="monthly",
    )

    assert schedule == [
        RebalanceEvent(signal_date="2026-01-30", execution_date="2026-02-02"),
        RebalanceEvent(signal_date="2026-02-27", execution_date="2026-03-02"),
        RebalanceEvent(signal_date="2026-03-31", execution_date="2026-04-01"),
    ]


def test_quarterly_rebalance_uses_quarter_end_signal_dates():
    trade_dates = [
        "2026-01-30",
        "2026-02-27",
        "2026-03-31",
        "2026-04-01",
        "2026-06-30",
        "2026-07-01",
    ]

    schedule = build_rebalance_schedule(
        trade_dates=trade_dates,
        start_date="2026-01-01",
        end_date="2026-07-01",
        rebalance_mode="quarterly",
    )

    assert schedule == [
        RebalanceEvent(signal_date="2026-03-31", execution_date="2026-04-01"),
        RebalanceEvent(signal_date="2026-06-30", execution_date="2026-07-01"),
    ]

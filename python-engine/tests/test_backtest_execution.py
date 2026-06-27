import pytest

from stock_selector.backtesting.execution import ExecutionConfig, execute_order


def _price_row(**overrides):
    row = {
        "stock_code": "000001.SZ",
        "trade_date": "2026-02-02",
        "open": 10.0,
        "adj_open": 10.0,
        "adj_close": 10.5,
        "is_paused": False,
        "limit_up": 11.0,
        "limit_down": 9.0,
    }
    row.update(overrides)
    return row


def test_buy_execution_applies_positive_slippage_and_commission():
    result = execute_order(
        stock_code="000001.SZ",
        side="buy",
        shares=100,
        price_row=_price_row(),
        config=ExecutionConfig(commission_rate=0.001, slippage_bps=50, stamp_tax_rate=0.001),
    )

    assert result.status == "filled"
    assert result.fill_price == pytest.approx(10.05)
    assert result.gross_amount == pytest.approx(1005.0)
    assert result.commission == pytest.approx(1.005)
    assert result.stamp_tax == 0
    assert result.cash_delta == pytest.approx(-1006.005)


def test_sell_execution_applies_negative_slippage_commission_and_stamp_tax():
    result = execute_order(
        stock_code="000001.SZ",
        side="sell",
        shares=100,
        price_row=_price_row(),
        config=ExecutionConfig(commission_rate=0.001, slippage_bps=50, stamp_tax_rate=0.001),
    )

    assert result.status == "filled"
    assert result.fill_price == pytest.approx(9.95)
    assert result.gross_amount == pytest.approx(995.0)
    assert result.commission == pytest.approx(0.995)
    assert result.stamp_tax == pytest.approx(0.995)
    assert result.cash_delta == pytest.approx(993.01)


def test_execution_blocks_paused_limit_up_buys_and_limit_down_sells():
    config = ExecutionConfig(commission_rate=0.001, slippage_bps=0, stamp_tax_rate=0.001)

    paused = execute_order("000001.SZ", "buy", 100, _price_row(is_paused=True), config)
    limit_up = execute_order("000001.SZ", "buy", 100, _price_row(open=11.0, adj_open=11.0), config)
    limit_down = execute_order("000001.SZ", "sell", 100, _price_row(open=9.0, adj_open=9.0), config)

    assert paused.status == "blocked"
    assert paused.reason == "PAUSED"
    assert limit_up.status == "blocked"
    assert limit_up.reason == "LIMIT_UP"
    assert limit_down.status == "blocked"
    assert limit_down.reason == "LIMIT_DOWN"


def test_limit_checks_use_raw_open_when_adjusted_price_scale_differs():
    config = ExecutionConfig(commission_rate=0.001, slippage_bps=0, stamp_tax_rate=0.001)

    buy = execute_order(
        "000001.SZ",
        "buy",
        100,
        _price_row(open=10.0, adj_open=20.0, limit_up=11.0, limit_down=9.0),
        config,
    )
    sell = execute_order(
        "000001.SZ",
        "sell",
        100,
        _price_row(open=10.0, adj_open=5.0, limit_up=11.0, limit_down=9.0),
        config,
    )

    assert buy.status == "filled"
    assert sell.status == "filled"

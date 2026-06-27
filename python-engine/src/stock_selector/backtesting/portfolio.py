from dataclasses import dataclass
from typing import Any

import pandas as pd

from stock_selector.backtesting.execution import ExecutionConfig, ExecutionResult, execute_order
from stock_selector.backtesting.schedule import RebalanceEvent, build_rebalance_schedule
from stock_selector.utils.date_validator import validate_date_range


@dataclass(frozen=True)
class PortfolioConfig:
    start_date: str
    end_date: str
    rebalance_mode: str
    initial_cash: float
    execution: ExecutionConfig
    top_n: int = 50
    execution_rule: str = "next_open"


@dataclass(frozen=True)
class PortfolioSimulationResult:
    portfolio_daily: pd.DataFrame
    trade_detail: pd.DataFrame
    rebalance_events: list[RebalanceEvent]


def simulate_equal_weight_portfolio(
    *,
    selection_history: pd.DataFrame,
    adjusted_price_history: pd.DataFrame,
    config: PortfolioConfig,
) -> PortfolioSimulationResult:
    start_date, end_date = validate_date_range(config.start_date, config.end_date)
    if config.execution_rule != "next_open":
        raise ValueError(f"unsupported execution_rule: {config.execution_rule}")
    prices = _prepare_prices(adjusted_price_history, start_date, end_date)
    selections = selection_history.copy()
    selections["trade_date"] = selections["trade_date"].astype(str)
    trade_dates = sorted(prices["trade_date"].astype(str).unique())
    events = build_rebalance_schedule(
        trade_dates=trade_dates,
        start_date=start_date,
        end_date=end_date,
        rebalance_mode=config.rebalance_mode,
    )
    event_by_execution_date = {event.execution_date: event for event in events}

    cash = float(config.initial_cash)
    holdings: dict[str, float] = {}
    portfolio_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    previous_asset: float | None = None

    for trade_date in trade_dates:
        price_by_code = _price_rows_for_date(prices, trade_date)
        event = event_by_execution_date.get(trade_date)
        if event is not None:
            cash = _rebalance(
                cash=cash,
                holdings=holdings,
                price_by_code=price_by_code,
                target_codes=_target_codes(selections, event.signal_date, config.top_n),
                event=event,
                execution=config.execution,
                trade_rows=trade_rows,
            )

        position_value = _position_value(holdings, price_by_code, price_column="adj_close")
        total_asset = cash + position_value
        daily_return = 0.0 if previous_asset in (None, 0) else total_asset / previous_asset - 1
        previous_asset = total_asset
        portfolio_rows.append(
            {
                "record_type": "portfolio",
                "trade_date": trade_date,
                "signal_date": None,
                "execution_date": None,
                "stock_code": None,
                "side": None,
                "status": "valued",
                "reason": "",
                "shares": None,
                "fill_price": None,
                "gross_amount": None,
                "commission": None,
                "stamp_tax": None,
                "cash_delta": None,
                "cash": float(cash),
                "position_value": float(position_value),
                "total_asset": float(total_asset),
                "daily_return": float(daily_return),
                "holding_count": int(sum(1 for shares in holdings.values() if shares > 0)),
            }
        )

    return PortfolioSimulationResult(
        portfolio_daily=pd.DataFrame(portfolio_rows),
        trade_detail=pd.DataFrame(trade_rows),
        rebalance_events=events,
    )


def _rebalance(
    *,
    cash: float,
    holdings: dict[str, float],
    price_by_code: dict[str, dict[str, Any]],
    target_codes: list[str],
    event: RebalanceEvent,
    execution: ExecutionConfig,
    trade_rows: list[dict[str, Any]],
) -> float:
    target_set = set(target_codes)
    for stock_code in list(holdings):
        if holdings[stock_code] <= 0 or stock_code in target_set:
            continue
        cash = _execute_or_record_blocked(cash, holdings, price_by_code, event, stock_code, "sell", holdings[stock_code], execution, trade_rows)

    if not target_codes:
        return cash

    total_value = cash + _position_value(holdings, price_by_code, price_column="adj_open")
    target_value = total_value / len(target_codes)
    for stock_code in target_codes:
        row = price_by_code.get(stock_code)
        if row is None:
            _record_missing_price(event, stock_code, "buy", 0.0, trade_rows)
            continue
        current_shares = holdings.get(stock_code, 0.0)
        current_value = current_shares * float(row["adj_open"])
        delta_value = target_value - current_value
        if delta_value > 1e-9:
            shares = _buyable_shares(delta_value, cash, float(row["adj_open"]), execution)
            cash = _execute_or_record_blocked(cash, holdings, price_by_code, event, stock_code, "buy", shares, execution, trade_rows)
        elif delta_value < -1e-9:
            shares = min(current_shares, abs(delta_value) / float(row["adj_open"]))
            cash = _execute_or_record_blocked(cash, holdings, price_by_code, event, stock_code, "sell", shares, execution, trade_rows)
    return cash


def _execute_or_record_blocked(
    cash: float,
    holdings: dict[str, float],
    price_by_code: dict[str, dict[str, Any]],
    event: RebalanceEvent,
    stock_code: str,
    side: str,
    shares: float,
    execution: ExecutionConfig,
    trade_rows: list[dict[str, Any]],
) -> float:
    row = price_by_code.get(stock_code)
    if row is None:
        _record_missing_price(event, stock_code, side, shares, trade_rows)
        return cash
    result = execute_order(stock_code, side, shares, row, execution)
    trade_rows.append(_trade_row(event, result))
    if result.status != "filled":
        return cash
    cash += result.cash_delta
    if side == "buy":
        holdings[stock_code] = holdings.get(stock_code, 0.0) + result.shares
    else:
        holdings[stock_code] = max(0.0, holdings.get(stock_code, 0.0) - result.shares)
    return cash


def _buyable_shares(delta_value: float, cash: float, adj_open: float, execution: ExecutionConfig) -> float:
    slip = execution.slippage_bps / 10000.0
    fill_price = adj_open * (1 + slip)
    per_share_cash = fill_price * (1 + execution.commission_rate)
    if per_share_cash <= 0:
        return 0.0
    return max(0.0, min(delta_value / adj_open, cash / per_share_cash))


def _prepare_prices(adjusted_price_history: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    prices = adjusted_price_history.copy()
    prices["trade_date"] = prices["trade_date"].astype(str)
    return prices.loc[(prices["trade_date"] >= start_date) & (prices["trade_date"] <= end_date)].sort_values(["trade_date", "stock_code"]).reset_index(drop=True)


def _price_rows_for_date(prices: pd.DataFrame, trade_date: str) -> dict[str, dict[str, Any]]:
    rows = prices.loc[prices["trade_date"] == trade_date]
    return {str(row["stock_code"]): row for row in rows.to_dict(orient="records")}


def _target_codes(selections: pd.DataFrame, signal_date: str, top_n: int) -> list[str]:
    rows = selections.loc[selections["trade_date"] == signal_date].sort_values(["rank", "stock_code"]).head(int(top_n))
    return [str(value) for value in rows["stock_code"].tolist()]


def _position_value(holdings: dict[str, float], price_by_code: dict[str, dict[str, Any]], *, price_column: str) -> float:
    value = 0.0
    for stock_code, shares in holdings.items():
        row = price_by_code.get(stock_code)
        if row is None:
            continue
        value += shares * float(row[price_column])
    return float(value)


def _trade_row(event: RebalanceEvent, result: ExecutionResult) -> dict[str, Any]:
    return {
        "record_type": "trade",
        "trade_date": event.execution_date,
        "signal_date": event.signal_date,
        "execution_date": event.execution_date,
        "stock_code": result.stock_code,
        "side": result.side,
        "status": result.status,
        "reason": result.reason,
        "shares": result.shares,
        "fill_price": result.fill_price,
        "gross_amount": result.gross_amount,
        "commission": result.commission,
        "stamp_tax": result.stamp_tax,
        "cash_delta": result.cash_delta,
        "cash": None,
        "position_value": None,
        "total_asset": None,
        "daily_return": None,
        "holding_count": None,
    }


def _record_missing_price(event: RebalanceEvent, stock_code: str, side: str, shares: float, trade_rows: list[dict[str, Any]]) -> None:
    trade_rows.append(
        {
            "record_type": "trade",
            "trade_date": event.execution_date,
            "signal_date": event.signal_date,
            "execution_date": event.execution_date,
            "stock_code": stock_code,
            "side": side,
            "status": "blocked",
            "reason": "MISSING_PRICE",
            "shares": float(shares),
            "fill_price": 0.0,
            "gross_amount": 0.0,
            "commission": 0.0,
            "stamp_tax": 0.0,
            "cash_delta": 0.0,
            "cash": None,
            "position_value": None,
            "total_asset": None,
            "daily_return": None,
            "holding_count": None,
        }
    )

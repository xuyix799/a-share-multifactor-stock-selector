from dataclasses import dataclass
from typing import Any

import pandas as pd

from stock_selector.data.data_validator import validate_stock_code


@dataclass(frozen=True)
class ExecutionConfig:
    commission_rate: float
    slippage_bps: float
    stamp_tax_rate: float


@dataclass(frozen=True)
class ExecutionResult:
    stock_code: str
    side: str
    shares: float
    status: str
    reason: str
    fill_price: float
    gross_amount: float
    commission: float
    stamp_tax: float
    cash_delta: float


def execute_order(
    stock_code: str,
    side: str,
    shares: float,
    price_row: dict[str, Any] | pd.Series,
    config: ExecutionConfig,
) -> ExecutionResult:
    stock_code = validate_stock_code(stock_code)
    side = side.lower()
    if side not in {"buy", "sell"}:
        raise ValueError(f"unsupported side: {side}")
    if shares <= 0:
        return _blocked(stock_code, side, shares, "ZERO_SHARES")

    row = _row_dict(price_row)
    adj_open = float(row["adj_open"])
    raw_open = float(row.get("open", adj_open))
    if bool(row.get("is_paused", False)):
        return _blocked(stock_code, side, shares, "PAUSED")
    if side == "buy" and raw_open >= float(row["limit_up"]):
        return _blocked(stock_code, side, shares, "LIMIT_UP")
    if side == "sell" and raw_open <= float(row["limit_down"]):
        return _blocked(stock_code, side, shares, "LIMIT_DOWN")

    slip = float(config.slippage_bps) / 10000.0
    fill_price = adj_open * (1 + slip if side == "buy" else 1 - slip)
    gross_amount = shares * fill_price
    commission = gross_amount * float(config.commission_rate)
    stamp_tax = gross_amount * float(config.stamp_tax_rate) if side == "sell" else 0.0
    cash_delta = -(gross_amount + commission) if side == "buy" else gross_amount - commission - stamp_tax
    return ExecutionResult(
        stock_code=stock_code,
        side=side,
        shares=float(shares),
        status="filled",
        reason="",
        fill_price=float(fill_price),
        gross_amount=float(gross_amount),
        commission=float(commission),
        stamp_tax=float(stamp_tax),
        cash_delta=float(cash_delta),
    )


def _blocked(stock_code: str, side: str, shares: float, reason: str) -> ExecutionResult:
    return ExecutionResult(
        stock_code=stock_code,
        side=side,
        shares=float(shares),
        status="blocked",
        reason=reason,
        fill_price=0.0,
        gross_amount=0.0,
        commission=0.0,
        stamp_tax=0.0,
        cash_delta=0.0,
    )


def _row_dict(price_row: dict[str, Any] | pd.Series) -> dict[str, Any]:
    if isinstance(price_row, pd.Series):
        return price_row.to_dict()
    return dict(price_row)

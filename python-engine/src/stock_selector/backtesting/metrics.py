import math

import pandas as pd

from stock_selector.data.data_validator import REQUIRED_BENCHMARK_INDEXES


def calculate_backtest_metrics(portfolio_daily: pd.DataFrame, trade_detail: pd.DataFrame, benchmark_price: pd.DataFrame) -> dict:
    if portfolio_daily.empty:
        raise ValueError("portfolio_daily is empty")

    portfolio = portfolio_daily.sort_values("trade_date").reset_index(drop=True)
    assets = pd.to_numeric(portfolio["total_asset"], errors="coerce")
    start_asset = float(assets.iloc[0])
    end_asset = float(assets.iloc[-1])
    total_return = _safe_return(end_asset, start_asset)
    benchmark_returns = _benchmark_returns(benchmark_price)
    return {
        "total_return": total_return,
        "period_return": total_return,
        "annualized_return": _annualized_return(total_return, len(portfolio)),
        "max_drawdown": _max_drawdown(assets),
        "turnover": _turnover(trade_detail, assets),
        "cost_total": _cost_total(trade_detail),
        "trade_count": _trade_count(trade_detail),
        "benchmark_returns": benchmark_returns,
        "excess_returns": {index_code: total_return - value for index_code, value in benchmark_returns.items()},
    }


def _benchmark_returns(benchmark_price: pd.DataFrame) -> dict[str, float]:
    missing = sorted(REQUIRED_BENCHMARK_INDEXES - set(benchmark_price["index_code"].astype(str)))
    if missing:
        raise ValueError(f"missing benchmark indexes: {', '.join(missing)}")

    result = {}
    for index_code in sorted(REQUIRED_BENCHMARK_INDEXES):
        rows = benchmark_price.loc[benchmark_price["index_code"].astype(str) == index_code].sort_values("trade_date")
        start_close = float(rows.iloc[0]["close"])
        end_close = float(rows.iloc[-1]["close"])
        result[index_code] = _safe_return(end_close, start_close)
    return result


def _safe_return(end_value: float, start_value: float) -> float:
    if start_value == 0:
        return 0.0
    return float(end_value / start_value - 1)


def _annualized_return(total_return: float, observations: int) -> float:
    if observations <= 1 or total_return <= -1:
        return float(total_return)
    return float(math.pow(1 + total_return, 252 / (observations - 1)) - 1)


def _max_drawdown(assets: pd.Series) -> float:
    running_max = assets.cummax()
    drawdowns = assets / running_max - 1
    return float(drawdowns.min())


def _cost_total(trade_detail: pd.DataFrame) -> float:
    if trade_detail.empty:
        return 0.0
    return float(pd.to_numeric(trade_detail.get("commission", 0), errors="coerce").fillna(0).sum() + pd.to_numeric(trade_detail.get("stamp_tax", 0), errors="coerce").fillna(0).sum())


def _trade_count(trade_detail: pd.DataFrame) -> int:
    if trade_detail.empty:
        return 0
    if "status" in trade_detail.columns:
        return int((trade_detail["status"] == "filled").sum())
    return int(len(trade_detail))


def _turnover(trade_detail: pd.DataFrame, assets: pd.Series) -> float:
    if trade_detail.empty:
        return 0.0
    avg_asset = float(assets.mean())
    if avg_asset == 0:
        return 0.0
    if "gross_amount" not in trade_detail.columns:
        return 0.0
    gross = pd.to_numeric(trade_detail["gross_amount"], errors="coerce").fillna(0).abs().sum()
    return float(gross / avg_asset)

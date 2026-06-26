import pandas as pd

from stock_selector.data.data_validator import validate_dataset_frame
from stock_selector.utils.date_validator import validate_trade_date


TREND_COLUMNS = [
    "stock_code",
    "trade_date",
    "trend_ret_20d",
    "trend_ret_60d",
    "trend_ret_120d",
    "trend_ma20",
    "trend_ma60",
    "trend_ma120",
    "trend_price_ma60_ratio",
]


def build_trend_factors(factor_input_table: pd.DataFrame, adjusted_price_history: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    trade_date = validate_trade_date(trade_date)
    validate_dataset_frame("factor_input_table", factor_input_table, trade_date)
    history = _filter_history(adjusted_price_history, trade_date)

    rows = []
    for stock_code in factor_input_table["stock_code"].astype(str):
        stock_history = history[history["stock_code"].astype(str) == stock_code].sort_values("trade_date")
        current_close = _last_close(stock_history)
        ma60 = _moving_average(stock_history, 60)
        rows.append(
            {
                "stock_code": stock_code,
                "trade_date": trade_date,
                "trend_ret_20d": _period_return(stock_history, 20),
                "trend_ret_60d": _period_return(stock_history, 60),
                "trend_ret_120d": _period_return(stock_history, 120),
                "trend_ma20": _moving_average(stock_history, 20),
                "trend_ma60": ma60,
                "trend_ma120": _moving_average(stock_history, 120),
                "trend_price_ma60_ratio": (current_close / ma60) if not pd.isna(current_close) and not pd.isna(ma60) and ma60 != 0 else pd.NA,
            }
        )
    return pd.DataFrame(rows, columns=TREND_COLUMNS)


def _filter_history(history: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    if history.empty:
        return history.copy()
    result = history.copy()
    result["trade_date"] = result["trade_date"].astype(str)
    return result[result["trade_date"] <= trade_date]


def _last_close(history: pd.DataFrame):
    if history.empty:
        return pd.NA
    return float(history.iloc[-1]["adj_close"])


def _period_return(history: pd.DataFrame, days: int):
    if len(history) <= days:
        return pd.NA
    current = float(history.iloc[-1]["adj_close"])
    previous = float(history.iloc[-(days + 1)]["adj_close"])
    return current / previous - 1 if previous else pd.NA


def _moving_average(history: pd.DataFrame, days: int):
    if len(history) < days:
        return pd.NA
    return float(pd.to_numeric(history.tail(days)["adj_close"], errors="coerce").mean())

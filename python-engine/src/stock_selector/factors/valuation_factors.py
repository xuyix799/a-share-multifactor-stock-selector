from datetime import date, timedelta

import pandas as pd

from stock_selector.data.data_validator import validate_dataset_frame
from stock_selector.utils.date_validator import validate_trade_date


VALUATION_COLUMNS = [
    "stock_code",
    "trade_date",
    "valuation_pe_ttm",
    "valuation_pb",
    "valuation_ps_ttm",
    "valuation_pe_percentile_3y",
    "valuation_pb_percentile_3y",
]


def build_valuation_factors(factor_input_table: pd.DataFrame, clean_snapshot_history: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    trade_date = validate_trade_date(trade_date)
    validate_dataset_frame("factor_input_table", factor_input_table, trade_date)
    history = _filter_history(clean_snapshot_history, trade_date)

    rows = []
    for row in factor_input_table.to_dict(orient="records"):
        stock_code = row["stock_code"]
        stock_history = history[history["stock_code"].astype(str) == stock_code]
        rows.append(
            {
                "stock_code": stock_code,
                "trade_date": trade_date,
                "valuation_pe_ttm": row["pe_ttm"],
                "valuation_pb": row["pb"],
                "valuation_ps_ttm": row["ps_ttm"],
                "valuation_pe_percentile_3y": _valuation_percentile(stock_history, "pe_ttm", row["pe_ttm"], trade_date),
                "valuation_pb_percentile_3y": _valuation_percentile(stock_history, "pb", row["pb"], trade_date),
            }
        )
    return pd.DataFrame(rows, columns=VALUATION_COLUMNS)


def _filter_history(history: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    if history.empty:
        return history.copy()
    result = history.copy()
    result["trade_date"] = result["trade_date"].astype(str)
    min_date = (date.fromisoformat(trade_date) - timedelta(days=365 * 3)).isoformat()
    return result[(result["trade_date"] < trade_date) & (result["trade_date"] >= min_date)]


def _valuation_percentile(history: pd.DataFrame, column: str, current_value: float, trade_date: str):
    current = pd.to_numeric(pd.Series([current_value]), errors="coerce").iloc[0]
    if pd.isna(current):
        return pd.NA
    values = pd.to_numeric(history[column], errors="coerce").dropna().tolist() if column in history.columns else []
    values.append(float(current))
    if not values:
        return pd.NA
    return sum(value <= current for value in values) / len(values)

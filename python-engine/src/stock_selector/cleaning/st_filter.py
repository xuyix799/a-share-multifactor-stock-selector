import pandas as pd

from stock_selector.data.data_validator import validate_stock_code
from stock_selector.utils.date_validator import validate_trade_date


def is_st_on_trade_date(st_history: pd.DataFrame, stock_code: str, trade_date: str) -> bool:
    stock_code = validate_stock_code(stock_code)
    trade_date = validate_trade_date(trade_date)
    for row in st_history.to_dict(orient="records"):
        if str(row["stock_code"]) != stock_code:
            continue
        start_date = validate_trade_date(str(row["start_date"]))
        end_value = row.get("end_date")
        end_date = None if pd.isna(end_value) else validate_trade_date(str(end_value))
        if start_date <= trade_date and (end_date is None or end_date > trade_date):
            return True
    return False


def mark_st_status(base: pd.DataFrame, st_history: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    trade_date = validate_trade_date(trade_date)
    result = base.copy()
    result["is_st_on_date"] = [
        is_st_on_trade_date(st_history, str(stock_code), trade_date)
        for stock_code in result["stock_code"]
    ]
    result["is_st_on_date"] = result["is_st_on_date"].astype(object)
    return result

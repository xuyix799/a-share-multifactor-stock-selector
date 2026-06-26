import pandas as pd

from stock_selector.data.data_validator import validate_dataset_frame
from stock_selector.utils.date_validator import validate_trade_date


def validate_clean_daily_snapshot(df: pd.DataFrame, trade_date: str) -> None:
    trade_date = validate_trade_date(trade_date)
    validate_dataset_frame("clean_daily_snapshot", df, trade_date)

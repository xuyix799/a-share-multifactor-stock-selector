import pandas as pd

from stock_selector.data.data_validator import validate_dataset_frame
from stock_selector.utils.date_validator import validate_trade_date


GROWTH_COLUMNS = ["stock_code", "trade_date", "growth_revenue_yoy", "growth_net_profit_yoy"]


def build_growth_factors(factor_input_table: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    trade_date = validate_trade_date(trade_date)
    validate_dataset_frame("factor_input_table", factor_input_table, trade_date)
    result = pd.DataFrame(
        {
            "stock_code": factor_input_table["stock_code"],
            "trade_date": trade_date,
            "growth_revenue_yoy": factor_input_table["revenue_yoy"],
            "growth_net_profit_yoy": factor_input_table["net_profit_yoy"],
        }
    )
    return result[GROWTH_COLUMNS]

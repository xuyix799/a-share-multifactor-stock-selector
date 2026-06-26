import pandas as pd

from stock_selector.data.data_validator import validate_dataset_frame
from stock_selector.utils.date_validator import validate_trade_date


QUALITY_COLUMNS = [
    "stock_code",
    "trade_date",
    "quality_roe",
    "quality_gross_margin",
    "quality_debt_ratio",
    "quality_cashflow_profit_ratio",
]


def build_quality_factors(factor_input_table: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    trade_date = validate_trade_date(trade_date)
    validate_dataset_frame("factor_input_table", factor_input_table, trade_date)
    result = pd.DataFrame(
        {
            "stock_code": factor_input_table["stock_code"],
            "trade_date": trade_date,
            "quality_roe": factor_input_table["roe"],
            "quality_gross_margin": factor_input_table["gross_margin"],
            "quality_debt_ratio": factor_input_table["debt_ratio"],
            # factor_input_table has growth rate, not absolute profit; keep this null rather than fabricating a ratio.
            "quality_cashflow_profit_ratio": pd.NA,
        }
    )
    return result[QUALITY_COLUMNS]

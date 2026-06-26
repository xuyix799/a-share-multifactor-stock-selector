import pandas as pd

from stock_selector.data.data_validator import validate_dataset_frame
from stock_selector.providers.schema_contract import get_schema_contract
from stock_selector.utils.date_validator import validate_trade_date


def build_adjusted_price(daily_price: pd.DataFrame, adj_factor: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    trade_date = validate_trade_date(trade_date)
    validate_dataset_frame("daily_price", daily_price, trade_date)
    validate_dataset_frame("adj_factor", adj_factor, trade_date)

    valid_factor = adj_factor[adj_factor["adj_factor"] > 0][["stock_code", "trade_date", "adj_factor"]]
    merged = daily_price.merge(valid_factor, on=["stock_code", "trade_date"], how="inner")
    result = pd.DataFrame(
        {
            "stock_code": merged["stock_code"],
            "trade_date": merged["trade_date"],
            "adj_open": merged["open"] * merged["adj_factor"],
            "adj_high": merged["high"] * merged["adj_factor"],
            "adj_low": merged["low"] * merged["adj_factor"],
            "adj_close": merged["close"] * merged["adj_factor"],
            "volume": merged["volume"],
            "amount": merged["amount"],
            "pct_chg": merged["pct_chg"],
            "is_paused": merged["is_paused"].map(bool),
            "limit_up": merged["limit_up"],
            "limit_down": merged["limit_down"],
        }
    )
    result = result[get_schema_contract("adjusted_price").columns]
    validate_dataset_frame("adjusted_price", result, trade_date)
    return result

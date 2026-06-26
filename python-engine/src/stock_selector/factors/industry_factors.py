import pandas as pd

from stock_selector.data.data_validator import validate_dataset_frame
from stock_selector.utils.date_validator import validate_trade_date


INDUSTRY_COLUMNS = [
    "stock_code",
    "trade_date",
    "industry",
    "industry_ret_60d",
    "industry_ret_120d",
    "industry_strength_60d",
    "industry_strength_120d",
]


def build_industry_factors(
    factor_input_table: pd.DataFrame,
    adjusted_price_history: pd.DataFrame,
    benchmark_price_history: pd.DataFrame,
    trade_date: str,
    benchmark_index: str = "000300.SH",
) -> pd.DataFrame:
    trade_date = validate_trade_date(trade_date)
    validate_dataset_frame("factor_input_table", factor_input_table, trade_date)
    adjusted_history = _filter_history(adjusted_price_history, trade_date)
    benchmark_history = _filter_history(benchmark_price_history, trade_date)

    stock_returns = []
    for row in factor_input_table[["stock_code", "industry"]].to_dict(orient="records"):
        stock_code = row["stock_code"]
        stock_history = adjusted_history[adjusted_history["stock_code"].astype(str) == stock_code].sort_values("trade_date")
        stock_returns.append(
            {
                "stock_code": stock_code,
                "industry": row["industry"],
                "ret_60d": _period_return(stock_history, "adj_close", 60),
                "ret_120d": _period_return(stock_history, "adj_close", 120),
            }
        )
    returns = pd.DataFrame(stock_returns)
    industry_ret_60 = returns.groupby("industry")["ret_60d"].mean()
    industry_ret_120 = returns.groupby("industry")["ret_120d"].mean()

    benchmark = benchmark_history[benchmark_history["index_code"].astype(str) == benchmark_index].sort_values("trade_date")
    benchmark_ret_60 = _period_return(benchmark, "close", 60)
    benchmark_ret_120 = _period_return(benchmark, "close", 120)

    rows = []
    for row in factor_input_table[["stock_code", "industry"]].to_dict(orient="records"):
        industry = row["industry"]
        ret_60 = industry_ret_60.get(industry, pd.NA)
        ret_120 = industry_ret_120.get(industry, pd.NA)
        rows.append(
            {
                "stock_code": row["stock_code"],
                "trade_date": trade_date,
                "industry": industry,
                "industry_ret_60d": _nullable(ret_60),
                "industry_ret_120d": _nullable(ret_120),
                "industry_strength_60d": _strength(ret_60, benchmark_ret_60),
                "industry_strength_120d": _strength(ret_120, benchmark_ret_120),
            }
        )
    return pd.DataFrame(rows, columns=INDUSTRY_COLUMNS)


def _filter_history(history: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    if history.empty:
        return history.copy()
    result = history.copy()
    result["trade_date"] = result["trade_date"].astype(str)
    return result[result["trade_date"] <= trade_date]


def _period_return(history: pd.DataFrame, price_column: str, days: int):
    if len(history) <= days:
        return pd.NA
    current = float(history.iloc[-1][price_column])
    previous = float(history.iloc[-(days + 1)][price_column])
    return current / previous - 1 if previous else pd.NA


def _strength(industry_return, benchmark_return):
    if pd.isna(industry_return) or pd.isna(benchmark_return):
        return pd.NA
    return float(industry_return) - float(benchmark_return)


def _nullable(value):
    return pd.NA if pd.isna(value) else float(value)

import pandas as pd

from stock_selector.data.data_validator import validate_dataset_frame
from stock_selector.factors.factor_scores import add_factor_scores
from stock_selector.factors.factor_validator import FACTOR_DAILY_COLUMNS, validate_factor_daily
from stock_selector.factors.growth_factors import build_growth_factors
from stock_selector.factors.industry_factors import build_industry_factors
from stock_selector.factors.quality_factors import build_quality_factors
from stock_selector.factors.trend_factors import build_trend_factors
from stock_selector.factors.valuation_factors import build_valuation_factors
from stock_selector.utils.date_validator import validate_trade_date


def build_factor_daily(
    *,
    factor_input_table: pd.DataFrame,
    adjusted_price_history: pd.DataFrame,
    clean_snapshot_history: pd.DataFrame,
    benchmark_price_history: pd.DataFrame,
    trade_date: str,
    factor_weights: dict[str, float] | None = None,
) -> pd.DataFrame:
    trade_date = validate_trade_date(trade_date)
    validate_dataset_frame("factor_input_table", factor_input_table, trade_date)

    base = factor_input_table[["stock_code", "trade_date", "industry", "market_type"]].copy()
    base["trade_date"] = trade_date
    result = base.merge(build_quality_factors(factor_input_table, trade_date), on=["stock_code", "trade_date"], how="left")
    result = result.merge(build_growth_factors(factor_input_table, trade_date), on=["stock_code", "trade_date"], how="left")
    result = result.merge(build_valuation_factors(factor_input_table, clean_snapshot_history, trade_date), on=["stock_code", "trade_date"], how="left")
    result = result.merge(build_trend_factors(factor_input_table, adjusted_price_history, trade_date), on=["stock_code", "trade_date"], how="left")
    result = result.merge(
        build_industry_factors(factor_input_table, adjusted_price_history, benchmark_price_history, trade_date).drop(columns=["industry"]),
        on=["stock_code", "trade_date"],
        how="left",
    )
    result["liquidity_amount"] = factor_input_table["amount"].values
    result["liquidity_turnover_rate"] = factor_input_table["turnover_rate"].values
    result = add_factor_scores(result, factor_weights)
    result = result[FACTOR_DAILY_COLUMNS]
    validate_factor_daily(result, trade_date)
    return result

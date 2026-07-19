from collections.abc import Iterable
import math

import pandas as pd

from stock_selector.data.data_validator import DataValidationError, validate_stock_code
from stock_selector.utils.date_validator import validate_trade_date


FACTOR_DAILY_COLUMNS = [
    "stock_code",
    "trade_date",
    "industry",
    "market_type",
    "quality_roe",
    "quality_gross_margin",
    "quality_debt_ratio",
    "quality_cashflow_profit_ratio",
    "growth_revenue_yoy",
    "growth_net_profit_yoy",
    "valuation_pe_ttm",
    "valuation_pb",
    "valuation_ps_ttm",
    "valuation_pe_percentile_3y",
    "valuation_pb_percentile_3y",
    "trend_ret_20d",
    "trend_ret_60d",
    "trend_ret_120d",
    "trend_ma20",
    "trend_ma60",
    "trend_ma120",
    "trend_price_ma60_ratio",
    "industry_ret_60d",
    "industry_ret_120d",
    "industry_strength_60d",
    "industry_strength_120d",
    "liquidity_amount",
    "liquidity_turnover_rate",
    "quality_score",
    "growth_score",
    "valuation_score",
    "trend_score",
    "industry_score",
]

FACTOR_SCORE_COLUMNS = ["quality_score", "growth_score", "valuation_score", "trend_score", "industry_score"]
FACTOR_BASE_COLUMNS = ["stock_code", "trade_date", "industry", "market_type"]
FACTOR_NUMERIC_COLUMNS = [
    column for column in FACTOR_DAILY_COLUMNS if column not in FACTOR_BASE_COLUMNS
]


def validate_factor_daily(df: pd.DataFrame, trade_date: str) -> None:
    trade_date = validate_trade_date(trade_date)
    _require_columns(df, FACTOR_DAILY_COLUMNS)
    _validate_trade_date_column(df, trade_date)
    _validate_stock_codes(df)
    if df.duplicated(["stock_code", "trade_date"]).any():
        raise DataValidationError("duplicate rows by key: stock_code, trade_date")
    numeric_columns = {
        column: _validate_numeric_factor_column(df, column)
        for column in FACTOR_NUMERIC_COLUMNS
    }
    for column in FACTOR_SCORE_COLUMNS:
        values = numeric_columns[column]
        if values.isna().any() or (values < 0).any() or (values > 100).any():
            raise DataValidationError(f"{column} must be between 0 and 100")
    if "total_score" in df.columns:
        raise DataValidationError("factor_daily must not include total_score in Goal 6")


def _require_columns(df: pd.DataFrame, columns: Iterable[str]) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise DataValidationError(f"missing columns: {', '.join(missing)}")


def _validate_trade_date_column(df: pd.DataFrame, trade_date: str) -> None:
    for value in df["trade_date"].astype(str):
        if validate_trade_date(value) != trade_date:
            raise DataValidationError("trade_date column must equal requested trade_date")


def _validate_stock_codes(df: pd.DataFrame) -> None:
    for value in df["stock_code"].astype(str):
        validate_stock_code(value)


def _validate_numeric_factor_column(
    df: pd.DataFrame,
    column: str,
) -> pd.Series:
    try:
        values = pd.to_numeric(df[column], errors="raise")
    except (TypeError, ValueError) as exc:
        raise DataValidationError(
            f"{column} must contain only numeric values or null"
        ) from exc
    if not values.dropna().map(lambda value: math.isfinite(float(value))).all():
        raise DataValidationError(
            f"{column} must contain only finite values or null"
        )
    return values

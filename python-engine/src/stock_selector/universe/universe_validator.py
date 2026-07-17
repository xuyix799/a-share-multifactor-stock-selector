from collections.abc import Iterable

import pandas as pd

from stock_selector.data.data_validator import DataValidationError, validate_stock_code
from stock_selector.utils.date_validator import validate_trade_date


RISK_FILTER_COLUMNS = [
    "stock_code",
    "trade_date",
    "is_eligible",
    "exclude_reasons",
    "risk_flags",
    "is_st_on_date",
    "is_paused",
    "listed_days",
    "amount",
    "roe",
    "debt_ratio",
    "report_period",
    "announce_date",
]

ELIGIBLE_UNIVERSE_COLUMNS = [
    "stock_code",
    "trade_date",
    "stock_name",
    "industry",
    "market_type",
    "listed_days",
    "amount",
    "roe",
    "debt_ratio",
]

FACTOR_INPUT_TABLE_COLUMNS = [
    "stock_code",
    "trade_date",
    "industry",
    "market_type",
    "adj_close",
    "amount",
    "turnover_rate",
    "pe_ttm",
    "pb",
    "ps_ttm",
    "total_mv",
    "circ_mv",
    "revenue_yoy",
    "net_profit_yoy",
    "roe",
    "gross_margin",
    "debt_ratio",
    "operating_cashflow",
]


def validate_risk_filter(df: pd.DataFrame, trade_date: str) -> None:
    trade_date = validate_trade_date(trade_date)
    _require_non_empty(df, "risk_filter")
    _require_columns(df, RISK_FILTER_COLUMNS)
    _validate_trade_date_column(df, trade_date)
    _validate_stock_codes(df)
    _validate_unique(df)
    _validate_no_missing_strings(df, ["exclude_reasons", "risk_flags"])
    if (df["listed_days"] < 0).any():
        raise DataValidationError("listed_days must be non-negative")
    if (df["amount"] < 0).any():
        raise DataValidationError("amount must be non-negative")


def validate_eligible_universe(df: pd.DataFrame, trade_date: str) -> None:
    trade_date = validate_trade_date(trade_date)
    _require_columns(df, ELIGIBLE_UNIVERSE_COLUMNS)
    _validate_trade_date_column(df, trade_date)
    _validate_stock_codes(df)
    _validate_unique(df)
    if (df["listed_days"] < 0).any():
        raise DataValidationError("listed_days must be non-negative")
    if df[["roe", "debt_ratio"]].isna().any().any():
        raise DataValidationError("eligible_universe financial fields must not be null")
    if (df["amount"] < 0).any():
        raise DataValidationError("amount must be non-negative")


def validate_factor_input_table(df: pd.DataFrame, trade_date: str) -> None:
    trade_date = validate_trade_date(trade_date)
    _require_columns(df, FACTOR_INPUT_TABLE_COLUMNS)
    _validate_trade_date_column(df, trade_date)
    _validate_stock_codes(df)
    _validate_unique(df)
    if df[["roe", "debt_ratio"]].isna().any().any():
        raise DataValidationError("factor_input_table financial fields must not be null")
    if (df["amount"] < 0).any():
        raise DataValidationError("amount must be non-negative")


def _require_non_empty(df: pd.DataFrame, dataset: str) -> None:
    if df.empty:
        raise DataValidationError(f"{dataset} is empty")


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


def _validate_unique(df: pd.DataFrame) -> None:
    if df.duplicated(["stock_code", "trade_date"]).any():
        raise DataValidationError("duplicate rows by key: stock_code, trade_date")


def _validate_no_missing_strings(df: pd.DataFrame, columns: list[str]) -> None:
    for column in columns:
        if df[column].isna().any():
            raise DataValidationError(f"{column} must not be null")

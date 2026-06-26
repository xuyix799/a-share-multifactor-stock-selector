import re
from typing import Iterable

import pandas as pd

from stock_selector.storage.partition import validate_dataset
from stock_selector.utils.date_validator import validate_trade_date


class DataValidationError(ValueError):
    pass


STOCK_CODE_PATTERN = re.compile(r"^\d{6}\.(SZ|SH|BJ)$")
REQUIRED_BENCHMARK_INDEXES = {"000300.SH", "000905.SH", "000906.SH"}


def validate_stock_code(stock_code: str) -> str:
    if not isinstance(stock_code, str) or not STOCK_CODE_PATTERN.fullmatch(stock_code):
        raise DataValidationError(f"invalid stock_code: {stock_code}")
    return stock_code


def validate_dataset_frame(dataset: str, df: pd.DataFrame, trade_date: str) -> None:
    dataset = validate_dataset(dataset)
    trade_date = validate_trade_date(trade_date)
    if df.empty:
        raise DataValidationError(f"{dataset} is empty")

    validators = {
        "stock_basic": _validate_stock_basic,
        "daily_price": _validate_daily_price,
        "adj_factor": _validate_adj_factor,
        "daily_basic": _validate_daily_basic,
        "financial": _validate_financial,
        "st_history": _validate_st_history,
        "benchmark_price": _validate_benchmark_price,
        "adjusted_price": _validate_adjusted_price,
        "clean_daily_snapshot": _validate_clean_daily_snapshot,
        "risk_filter": _validate_risk_filter,
        "eligible_universe": _validate_eligible_universe,
        "factor_input_table": _validate_factor_input_table,
    }
    validators[dataset](df, trade_date)


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


def _validate_unique(df: pd.DataFrame, columns: list[str]) -> None:
    if df.duplicated(columns).any():
        raise DataValidationError(f"duplicate rows by key: {columns}")


def _validate_stock_basic(df: pd.DataFrame, trade_date: str) -> None:
    _require_columns(df, ["stock_code", "stock_name", "exchange", "list_date", "delist_date", "industry", "market_type", "is_st", "trade_date"])
    _validate_trade_date_column(df, trade_date)
    _validate_stock_codes(df)
    for value in df["list_date"].astype(str):
        validate_trade_date(value)
    for value in df["delist_date"].dropna().astype(str):
        validate_trade_date(value)
    _validate_unique(df, ["stock_code", "trade_date"])


def _validate_daily_price(df: pd.DataFrame, trade_date: str) -> None:
    _require_columns(
        df,
        [
            "stock_code",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "volume",
            "amount",
            "pct_chg",
            "is_paused",
            "limit_up",
            "limit_down",
        ],
    )
    _validate_trade_date_column(df, trade_date)
    _validate_stock_codes(df)
    _validate_unique(df, ["stock_code", "trade_date"])
    if (df["close"] <= 0).any() or (df["open"] <= 0).any():
        raise DataValidationError("open and close must be positive")
    if (df["high"] < df["low"]).any():
        raise DataValidationError("high must be greater than or equal to low")
    if (df["volume"] < 0).any() or (df["amount"] < 0).any():
        raise DataValidationError("volume and amount must be non-negative")
    if (df["limit_up"] < df["close"]).any() or (df["limit_down"] > df["close"]).any():
        raise DataValidationError("limit prices must bound close")


def _validate_adj_factor(df: pd.DataFrame, trade_date: str) -> None:
    _require_columns(df, ["stock_code", "trade_date", "adj_factor"])
    _validate_trade_date_column(df, trade_date)
    _validate_stock_codes(df)
    _validate_unique(df, ["stock_code", "trade_date"])
    if (df["adj_factor"] <= 0).any():
        raise DataValidationError("adj_factor must be positive")


def _validate_daily_basic(df: pd.DataFrame, trade_date: str) -> None:
    _require_columns(df, ["stock_code", "trade_date", "pe_ttm", "pb", "ps_ttm", "total_mv", "circ_mv", "turnover_rate"])
    _validate_trade_date_column(df, trade_date)
    _validate_stock_codes(df)
    _validate_unique(df, ["stock_code", "trade_date"])


def _validate_financial(df: pd.DataFrame, trade_date: str) -> None:
    _require_columns(
        df,
        [
            "stock_code",
            "report_period",
            "announce_date",
            "revenue_yoy",
            "net_profit_yoy",
            "roe",
            "gross_margin",
            "debt_ratio",
            "operating_cashflow",
        ],
    )
    _validate_stock_codes(df)
    for value in df["report_period"].astype(str):
        validate_trade_date(value)
    for value in df["announce_date"].astype(str):
        announce_date = validate_trade_date(value)
        if announce_date > trade_date:
            raise DataValidationError("announce_date must be <= trade_date")
    if df["roe"].isna().any():
        raise DataValidationError("roe must not be null")
    if df["debt_ratio"].isna().any() or (df["debt_ratio"] < 0).any() or (df["debt_ratio"] > 1).any():
        raise DataValidationError("debt_ratio must be between 0 and 1")


def _validate_st_history(df: pd.DataFrame, trade_date: str) -> None:
    _ = trade_date
    _require_columns(df, ["stock_code", "st_type", "start_date", "end_date", "source"])
    _validate_stock_codes(df)
    for value in df["start_date"].astype(str):
        validate_trade_date(value)
    for value in df["end_date"].dropna().astype(str):
        validate_trade_date(value)


def _validate_benchmark_price(df: pd.DataFrame, trade_date: str) -> None:
    _require_columns(df, ["index_code", "trade_date", "open", "high", "low", "close", "pct_chg"])
    _validate_trade_date_column(df, trade_date)
    missing = sorted(REQUIRED_BENCHMARK_INDEXES - set(df["index_code"].astype(str)))
    if missing:
        raise DataValidationError(f"missing benchmark indexes: {', '.join(missing)}")
    if (df["close"] <= 0).any() or (df["open"] <= 0).any():
        raise DataValidationError("benchmark open and close must be positive")
    if (df["high"] < df["low"]).any():
        raise DataValidationError("benchmark high must be greater than or equal to low")


def _validate_adjusted_price(df: pd.DataFrame, trade_date: str) -> None:
    _require_columns(
        df,
        [
            "stock_code",
            "trade_date",
            "adj_open",
            "adj_high",
            "adj_low",
            "adj_close",
            "volume",
            "amount",
            "pct_chg",
            "is_paused",
            "limit_up",
            "limit_down",
        ],
    )
    _validate_trade_date_column(df, trade_date)
    _validate_stock_codes(df)
    _validate_unique(df, ["stock_code", "trade_date"])
    if (df[["adj_open", "adj_high", "adj_low", "adj_close"]] <= 0).any().any():
        raise DataValidationError("adjusted OHLC prices must be positive")
    if (df["adj_high"] < df["adj_low"]).any():
        raise DataValidationError("adjusted high must be greater than or equal to adjusted low")
    if (df["volume"] < 0).any() or (df["amount"] < 0).any():
        raise DataValidationError("volume and amount must be non-negative")


def _validate_clean_daily_snapshot(df: pd.DataFrame, trade_date: str) -> None:
    _require_columns(
        df,
        [
            "stock_code",
            "trade_date",
            "stock_name",
            "industry",
            "market_type",
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "volume",
            "amount",
            "pct_chg",
            "is_paused",
            "limit_up",
            "limit_down",
            "adj_open",
            "adj_high",
            "adj_low",
            "adj_close",
            "pe_ttm",
            "pb",
            "ps_ttm",
            "total_mv",
            "circ_mv",
            "turnover_rate",
            "report_period",
            "announce_date",
            "revenue_yoy",
            "net_profit_yoy",
            "roe",
            "gross_margin",
            "debt_ratio",
            "operating_cashflow",
            "is_st_on_date",
            "listed_days",
        ],
    )
    _validate_trade_date_column(df, trade_date)
    _validate_stock_codes(df)
    _validate_unique(df, ["stock_code", "trade_date"])
    if (df[["open", "high", "low", "close", "pre_close", "adj_open", "adj_high", "adj_low", "adj_close"]] <= 0).any().any():
        raise DataValidationError("snapshot prices must be positive")
    if (df["high"] < df["low"]).any() or (df["adj_high"] < df["adj_low"]).any():
        raise DataValidationError("snapshot high must be greater than or equal to low")
    if (df["volume"] < 0).any() or (df["amount"] < 0).any():
        raise DataValidationError("volume and amount must be non-negative")
    if (df["listed_days"] < 0).any():
        raise DataValidationError("listed_days must be non-negative")
    for value in df["announce_date"].dropna().astype(str):
        if validate_trade_date(value) > trade_date:
            raise DataValidationError("snapshot announce_date must be <= trade_date")
    for value in df["report_period"].dropna().astype(str):
        validate_trade_date(value)


def _validate_risk_filter(df: pd.DataFrame, trade_date: str) -> None:
    _require_columns(
        df,
        [
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
        ],
    )
    _validate_trade_date_column(df, trade_date)
    _validate_stock_codes(df)
    _validate_unique(df, ["stock_code", "trade_date"])
    if df[["exclude_reasons", "risk_flags"]].isna().any().any():
        raise DataValidationError("risk reason fields must not be null")
    if (df["listed_days"] < 0).any():
        raise DataValidationError("listed_days must be non-negative")
    if (df["amount"] < 0).any():
        raise DataValidationError("amount must be non-negative")
    for value in df["announce_date"].dropna().astype(str):
        if validate_trade_date(value) > trade_date:
            raise DataValidationError("risk_filter announce_date must be <= trade_date")
    for value in df["report_period"].dropna().astype(str):
        validate_trade_date(value)


def _validate_eligible_universe(df: pd.DataFrame, trade_date: str) -> None:
    _require_columns(
        df,
        [
            "stock_code",
            "trade_date",
            "stock_name",
            "industry",
            "market_type",
            "listed_days",
            "amount",
            "roe",
            "debt_ratio",
        ],
    )
    _validate_trade_date_column(df, trade_date)
    _validate_stock_codes(df)
    _validate_unique(df, ["stock_code", "trade_date"])
    if (df["listed_days"] < 0).any():
        raise DataValidationError("listed_days must be non-negative")
    if (df["amount"] < 0).any():
        raise DataValidationError("amount must be non-negative")
    if df[["roe", "debt_ratio"]].isna().any().any():
        raise DataValidationError("eligible_universe financial fields must not be null")


def _validate_factor_input_table(df: pd.DataFrame, trade_date: str) -> None:
    _require_columns(
        df,
        [
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
        ],
    )
    _validate_trade_date_column(df, trade_date)
    _validate_stock_codes(df)
    _validate_unique(df, ["stock_code", "trade_date"])
    if df[["roe", "debt_ratio"]].isna().any().any():
        raise DataValidationError("factor_input_table financial fields must not be null")
    if (df["adj_close"] <= 0).any():
        raise DataValidationError("adj_close must be positive")
    if (df["amount"] < 0).any():
        raise DataValidationError("amount must be non-negative")

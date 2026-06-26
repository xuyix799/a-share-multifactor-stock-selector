from dataclasses import dataclass
from typing import Any

import pandas as pd

from stock_selector.data.data_validator import DataValidationError, validate_stock_code
from stock_selector.storage.partition import validate_dataset
from stock_selector.utils.date_validator import validate_trade_date


REQUIRED_BENCHMARK_INDEXES = {"000300.SH", "000905.SH", "000906.SH"}


@dataclass(frozen=True)
class SchemaContract:
    dataset: str
    columns: list[str]
    numeric_columns: list[str]
    bool_columns: list[str]
    date_columns: list[str]
    nullable_columns: list[str]


SCHEMA_CONTRACTS = {
    "stock_basic": SchemaContract(
        dataset="stock_basic",
        columns=["stock_code", "stock_name", "exchange", "list_date", "delist_date", "industry", "market_type", "is_st", "trade_date"],
        numeric_columns=[],
        bool_columns=["is_st"],
        date_columns=["list_date", "delist_date", "trade_date"],
        nullable_columns=["delist_date"],
    ),
    "daily_price": SchemaContract(
        dataset="daily_price",
        columns=[
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
        numeric_columns=["open", "high", "low", "close", "pre_close", "volume", "amount", "pct_chg", "limit_up", "limit_down"],
        bool_columns=["is_paused"],
        date_columns=["trade_date"],
        nullable_columns=[],
    ),
    "adj_factor": SchemaContract(
        dataset="adj_factor",
        columns=["stock_code", "trade_date", "adj_factor"],
        numeric_columns=["adj_factor"],
        bool_columns=[],
        date_columns=["trade_date"],
        nullable_columns=[],
    ),
    "daily_basic": SchemaContract(
        dataset="daily_basic",
        columns=["stock_code", "trade_date", "pe_ttm", "pb", "ps_ttm", "total_mv", "circ_mv", "turnover_rate"],
        numeric_columns=["pe_ttm", "pb", "ps_ttm", "total_mv", "circ_mv", "turnover_rate"],
        bool_columns=[],
        date_columns=["trade_date"],
        nullable_columns=[],
    ),
    "financial": SchemaContract(
        dataset="financial",
        columns=[
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
        numeric_columns=["revenue_yoy", "net_profit_yoy", "roe", "gross_margin", "debt_ratio", "operating_cashflow"],
        bool_columns=[],
        date_columns=["report_period", "announce_date"],
        nullable_columns=[],
    ),
    "st_history": SchemaContract(
        dataset="st_history",
        columns=["stock_code", "st_type", "start_date", "end_date", "source"],
        numeric_columns=[],
        bool_columns=[],
        date_columns=["start_date", "end_date"],
        nullable_columns=["end_date"],
    ),
    "benchmark_price": SchemaContract(
        dataset="benchmark_price",
        columns=["index_code", "trade_date", "open", "high", "low", "close", "pct_chg"],
        numeric_columns=["open", "high", "low", "close", "pct_chg"],
        bool_columns=[],
        date_columns=["trade_date"],
        nullable_columns=[],
    ),
}


def get_schema_contract(dataset: str) -> SchemaContract:
    dataset = validate_dataset(dataset)
    return SCHEMA_CONTRACTS[dataset]


def inspect_schema(dataset: str) -> dict[str, Any]:
    contract = get_schema_contract(dataset)
    return {
        "dataset": contract.dataset,
        "columns": contract.columns,
        "numeric_columns": contract.numeric_columns,
        "bool_columns": contract.bool_columns,
        "date_columns": contract.date_columns,
        "nullable_columns": contract.nullable_columns,
    }


def validate_benchmark_contract(df: pd.DataFrame) -> None:
    missing = sorted(REQUIRED_BENCHMARK_INDEXES - set(df["index_code"].astype(str)))
    if missing:
        raise DataValidationError(f"missing benchmark indexes: {', '.join(missing)}")


def is_st_on_date(st_history: pd.DataFrame, stock_code: str, trade_date: str) -> bool:
    stock_code = validate_stock_code(stock_code)
    trade_date = validate_trade_date(trade_date)
    for row in st_history.to_dict(orient="records"):
        if str(row["stock_code"]) != stock_code:
            continue
        start_date = validate_trade_date(str(row["start_date"]))
        end_value = row.get("end_date")
        end_date = None if pd.isna(end_value) else validate_trade_date(str(end_value))
        if start_date <= trade_date and (end_date is None or trade_date <= end_date):
            return True
    return False


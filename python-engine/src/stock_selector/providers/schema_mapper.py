from datetime import date
import re
from typing import Any

import pandas as pd

from stock_selector.data.data_validator import DataValidationError, validate_dataset_frame
from stock_selector.providers.schema_contract import get_schema_contract
from stock_selector.storage.partition import validate_dataset


class SchemaMappingError(ValueError):
    pass


PROVIDER_FIELD_MAPPINGS = {
    "mock": {
        "stock_basic": {
            "stock_code": "ts_code",
            "stock_name": "name",
            "exchange": "exchange",
            "list_date": "list_date",
            "delist_date": "delist_date",
            "industry": "industry",
            "market_type": "market_type",
            "is_st": "is_st",
            "trade_date": "trade_date",
        },
        "daily_price": {
            "stock_code": "ts_code",
            "trade_date": "trade_date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "pre_close": "pre_close",
            "volume": "vol",
            "amount": "amount",
            "pct_chg": "pct_chg",
            "is_paused": "is_paused",
            "limit_up": "up_limit",
            "limit_down": "down_limit",
        },
        "adj_factor": {"stock_code": "ts_code", "trade_date": "trade_date", "adj_factor": "adj_factor"},
        "daily_basic": {
            "stock_code": "ts_code",
            "trade_date": "trade_date",
            "pe_ttm": "pe_ttm",
            "pb": "pb",
            "ps_ttm": "ps_ttm",
            "total_mv": "total_mv",
            "circ_mv": "circ_mv",
            "turnover_rate": "turnover_rate",
        },
        "financial": {
            "stock_code": "ts_code",
            "report_period": "end_date",
            "announce_date": "ann_date",
            "revenue_yoy": "revenue_yoy",
            "net_profit_yoy": "net_profit_yoy",
            "roe": "roe",
            "gross_margin": "gross_margin",
            "debt_ratio": "debt_ratio",
            "operating_cashflow": "operating_cashflow",
        },
        "st_history": {"stock_code": "ts_code", "st_type": "st_type", "start_date": "start_date", "end_date": "end_date", "source": "source"},
        "benchmark_price": {
            "index_code": "index_code",
            "trade_date": "trade_date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "pct_chg": "pct_chg",
        },
    }
}

for _provider in ("tushare", "akshare", "baostock"):
    PROVIDER_FIELD_MAPPINGS[_provider] = PROVIDER_FIELD_MAPPINGS["mock"]


def map_provider_frame(provider_name: str, dataset: str, raw_df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    provider_name = provider_name.lower()
    dataset = validate_dataset(dataset)
    trade_date = normalize_date(trade_date)
    contract = get_schema_contract(dataset)
    mapping = _mapping_for(provider_name, dataset)
    missing = [raw_column for raw_column in mapping.values() if raw_column not in raw_df.columns]
    if missing:
        raise SchemaMappingError(f"missing provider fields: {', '.join(missing)}")

    mapped = pd.DataFrame()
    for standard_column in contract.columns:
        mapped[standard_column] = raw_df[mapping[standard_column]]

    mapped = _normalize_columns(dataset, mapped, contract)
    validate_dataset_frame(dataset, mapped, trade_date)
    return mapped[contract.columns]


def normalize_stock_code(raw_code: str) -> str:
    if not isinstance(raw_code, str):
        raise SchemaMappingError(f"invalid stock_code: {raw_code}")
    value = raw_code.strip().upper()
    dotted = re.fullmatch(r"(\d{6})\.(SZ|SH|BJ)", value)
    if dotted:
        return f"{dotted.group(1)}.{dotted.group(2)}"
    prefixed = re.fullmatch(r"(SZ|SH|BJ)(\d{6})", value)
    if prefixed:
        return f"{prefixed.group(2)}.{prefixed.group(1)}"
    bare = re.fullmatch(r"\d{6}", value)
    if bare:
        if value.startswith("6"):
            return f"{value}.SH"
        if value.startswith(("0", "2", "3")):
            return f"{value}.SZ"
        if value.startswith(("4", "8")):
            return f"{value}.BJ"
    raise SchemaMappingError(f"invalid stock_code: {raw_code}")


def normalize_date(raw_date: Any) -> str:
    if isinstance(raw_date, date):
        return raw_date.isoformat()
    if not isinstance(raw_date, str):
        raise SchemaMappingError(f"invalid date: {raw_date}")
    value = raw_date.strip()
    try:
        if re.fullmatch(r"\d{8}", value):
            return date(int(value[0:4]), int(value[4:6]), int(value[6:8])).isoformat()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise SchemaMappingError(f"invalid date: {raw_date}") from exc
    raise SchemaMappingError(f"invalid date: {raw_date}")


def _mapping_for(provider_name: str, dataset: str) -> dict[str, str]:
    try:
        return PROVIDER_FIELD_MAPPINGS[provider_name][dataset]
    except KeyError as exc:
        raise SchemaMappingError(f"unsupported provider mapping: {provider_name}/{dataset}") from exc


def _normalize_columns(dataset: str, df: pd.DataFrame, contract) -> pd.DataFrame:
    result = df.copy()
    if "stock_code" in result.columns:
        result["stock_code"] = result["stock_code"].map(_normalize_stock_code_for_frame)
    if "index_code" in result.columns:
        result["index_code"] = result["index_code"].map(normalize_stock_code)
    for column in contract.date_columns:
        result[column] = result[column].map(lambda value: _normalize_nullable_date(value) if column in contract.nullable_columns else normalize_date(value))
    for column in contract.numeric_columns:
        try:
            result[column] = pd.to_numeric(result[column])
        except Exception as exc:
            raise SchemaMappingError(f"invalid numeric field: {dataset}.{column}") from exc
    for column in contract.bool_columns:
        result[column] = result[column].map(_to_bool)
    return result


def _normalize_stock_code_for_frame(raw_code: str) -> str:
    try:
        return normalize_stock_code(raw_code)
    except SchemaMappingError as exc:
        raise DataValidationError(str(exc)) from exc


def _normalize_nullable_date(value: Any) -> str | None:
    if value is None or pd.isna(value) or value == "":
        return None
    return normalize_date(value)


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not pd.isna(value):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "y"}:
            return True
        if normalized in {"0", "false", "no", "n"}:
            return False
    raise SchemaMappingError(f"invalid boolean value: {value}")


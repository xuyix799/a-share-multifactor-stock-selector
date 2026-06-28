from collections.abc import Callable
from dataclasses import dataclass
from time import sleep
from typing import Any

import pandas as pd

from stock_selector.providers.base import ProviderFetchError
from stock_selector.providers.tushare_provider import TushareProvider
from stock_selector.utils.date_validator import validate_trade_date


GOAL10R_INTERFACES = (
    "stock_basic",
    "daily",
    "stk_limit",
    "adj_factor",
    "daily_basic",
    "index_daily",
    "fina_indicator",
)

SAMPLE_STOCK = "000001.SZ"
SAMPLE_INDEX = "000300.SH"

WriteDatasetFn = Callable[[str, str, pd.DataFrame], str]
SleepFn = Callable[[float], None]


@dataclass(frozen=True)
class TushareInterfaceSpec:
    interface: str
    fields: str
    schema_dataset: str | None
    required_for_schema: tuple[str, ...]

    def kwargs(self, trade_date: str) -> dict[str, str]:
        compact_date = trade_date.replace("-", "")
        if self.interface == "stock_basic":
            return {"exchange": "", "list_status": "L", "fields": self.fields}
        if self.interface == "index_daily":
            return {"ts_code": SAMPLE_INDEX, "trade_date": compact_date, "fields": self.fields}
        if self.interface == "fina_indicator":
            return {"ts_code": SAMPLE_STOCK, "start_date": "20240101", "end_date": compact_date, "fields": self.fields}
        if self.interface == "adj_factor":
            return {"trade_date": compact_date, "fields": self.fields}
        return {"ts_code": SAMPLE_STOCK, "trade_date": compact_date, "fields": self.fields}


INTERFACE_SPECS = (
    TushareInterfaceSpec(
        interface="stock_basic",
        fields="ts_code,name,industry,market,list_date",
        schema_dataset="stock_basic",
        required_for_schema=("ts_code", "name", "industry", "market", "list_date"),
    ),
    TushareInterfaceSpec(
        interface="daily",
        fields="ts_code,trade_date,open,high,low,close,pre_close,pct_chg,vol,amount",
        schema_dataset="daily_price",
        required_for_schema=(
            "ts_code",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "pre_close",
            "pct_chg",
            "vol",
            "amount",
            "up_limit",
            "down_limit",
            "is_paused",
        ),
    ),
    TushareInterfaceSpec(
        interface="stk_limit",
        fields="ts_code,trade_date,up_limit,down_limit",
        schema_dataset="trading_limit",
        required_for_schema=("ts_code", "trade_date", "up_limit", "down_limit"),
    ),
    TushareInterfaceSpec(
        interface="adj_factor",
        fields="ts_code,trade_date,adj_factor",
        schema_dataset="adj_factor",
        required_for_schema=("ts_code", "trade_date", "adj_factor"),
    ),
    TushareInterfaceSpec(
        interface="daily_basic",
        fields="ts_code,trade_date,pe_ttm,pb,ps_ttm,total_mv,circ_mv,turnover_rate",
        schema_dataset="daily_basic",
        required_for_schema=("ts_code", "trade_date", "pe_ttm", "pb", "ps_ttm", "total_mv", "circ_mv", "turnover_rate"),
    ),
    TushareInterfaceSpec(
        interface="index_daily",
        fields="ts_code,trade_date,open,high,low,close,pre_close,pct_chg,vol,amount",
        schema_dataset="benchmark_price",
        required_for_schema=("ts_code", "trade_date", "open", "high", "low", "close", "pct_chg"),
    ),
    TushareInterfaceSpec(
        interface="fina_indicator",
        fields="ts_code,ann_date,end_date,or_yoy,netprofit_yoy,roe,grossprofit_margin,debt_to_assets,ocfps",
        schema_dataset="financial",
        required_for_schema=(
            "ts_code",
            "ann_date",
            "end_date",
            "or_yoy",
            "netprofit_yoy",
            "roe",
            "grossprofit_margin",
            "debt_to_assets",
            "operating_cashflow",
        ),
    ),
)


def probe_tushare_goal10r(
    provider: TushareProvider,
    trade_date: str,
    write_dataset_fn: WriteDatasetFn,
    sample_limit: int = 5,
    sleep_seconds: float = 12.0,
    sleeper: SleepFn = sleep,
) -> dict[str, Any]:
    trade_date = validate_trade_date(trade_date)
    if sample_limit <= 0:
        raise ValueError("sample_limit must be positive")
    if sleep_seconds < 0:
        raise ValueError("sleep_seconds must be non-negative")

    interfaces = []
    for index, spec in enumerate(INTERFACE_SPECS):
        if index > 0 and sleep_seconds:
            sleeper(sleep_seconds)
        interfaces.append(_probe_interface(provider, spec, trade_date, write_dataset_fn, sample_limit))

    return {
        "provider": "tushare",
        "trade_date": trade_date,
        "sample_stock": SAMPLE_STOCK,
        "sample_index": SAMPLE_INDEX,
        "sample_limit": sample_limit,
        "interfaces": interfaces,
        "daily_price_composition": _analyze_daily_price_composition(interfaces),
    }


def _probe_interface(
    provider: TushareProvider,
    spec: TushareInterfaceSpec,
    trade_date: str,
    write_dataset_fn: WriteDatasetFn,
    sample_limit: int,
) -> dict[str, Any]:
    try:
        raw = provider.fetch_raw_endpoint(spec.interface, **spec.kwargs(trade_date))
        sample = raw.head(sample_limit).copy()
        object_key = write_dataset_fn(spec.interface, trade_date, sample)
        return _success_result(spec, raw, sample, object_key)
    except Exception as exc:
        return _error_result(spec, exc)


def _success_result(spec: TushareInterfaceSpec, raw: pd.DataFrame, sample: pd.DataFrame, object_key: str) -> dict[str, Any]:
    columns = list(raw.columns)
    missing = _missing_for_schema(spec, columns)
    can_dq2 = spec.interface == "index_daily" and not missing
    return {
        "interface": spec.interface,
        "available": True,
        "row_count": int(len(raw)),
        "sample_row_count": int(len(sample)),
        "columns": columns,
        "schema_dataset": spec.schema_dataset,
        "schema_satisfied": not missing,
        "missing_for_current_schema": missing,
        "object_key": object_key,
        "can_enter_dq0_smoke": True,
        "can_enter_dq1": True,
        "can_enter_dq2": can_dq2,
        "can_enter_dq3": False,
        "note": _note_for_success(spec.interface, missing, can_dq2),
    }


def _error_result(spec: TushareInterfaceSpec, exc: Exception) -> dict[str, Any]:
    return {
        "interface": spec.interface,
        "available": False,
        "row_count": 0,
        "sample_row_count": 0,
        "columns": [],
        "schema_dataset": spec.schema_dataset,
        "schema_satisfied": False,
        "missing_for_current_schema": list(spec.required_for_schema),
        "object_key": None,
        "can_enter_dq0_smoke": False,
        "can_enter_dq1": False,
        "can_enter_dq2": False,
        "can_enter_dq3": False,
        "error_class": exc.__class__.__name__,
        "error": str(exc),
        "note": "permission, rate-limit, empty response, or provider error; not promoted",
    }


def _missing_for_schema(spec: TushareInterfaceSpec, columns: list[str]) -> list[str]:
    available = set(columns)
    if spec.interface == "daily":
        missing = [field for field in ("ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "pct_chg", "vol", "amount") if field not in available]
        if "is_paused" not in available:
            missing.append("is_paused")
        if not {"limit_down", "down_limit"} & available:
            missing.append("limit_down")
        if not {"limit_up", "up_limit"} & available:
            missing.append("limit_up")
        return missing
    return [field for field in spec.required_for_schema if field not in available]


def _note_for_success(interface: str, missing: list[str], can_dq2: bool) -> str:
    if interface == "daily":
        return "daily alone lacks limit_up, limit_down, and is_paused; combine with stk_limit for limits, but pause source is still required"
    if interface == "stk_limit":
        return "provides limit_up and limit_down source only; not a standalone daily_price schema"
    if interface == "index_daily" and can_dq2:
        return "benchmark price-only diagnostic candidate; not tradable stock data"
    if interface == "fina_indicator" and missing:
        return "financial indicator fields need explicit semantic mapping before standard financial promotion"
    if missing:
        return "raw smoke landed, but current schema fields are incomplete"
    return "raw smoke landed and fields cover the current schema candidate"


def _analyze_daily_price_composition(interfaces: list[dict[str, Any]]) -> dict[str, Any]:
    by_name = {item["interface"]: item for item in interfaces}
    daily = by_name.get("daily", {})
    limits = by_name.get("stk_limit", {})
    adj_factor = by_name.get("adj_factor", {})
    daily_basic = by_name.get("daily_basic", {})

    price_fields_complete = _has_columns(
        daily,
        {"ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "pct_chg", "vol", "amount"},
    )
    limit_fields_complete = _has_columns(limits, {"ts_code", "trade_date", "up_limit", "down_limit"})
    adj_factor_available = _has_columns(adj_factor, {"ts_code", "trade_date", "adj_factor"})
    daily_basic_available = _has_columns(daily_basic, {"ts_code", "trade_date", "pe_ttm", "pb", "ps_ttm", "total_mv", "circ_mv", "turnover_rate"})
    suspension_status_available = False

    price_and_limits = price_fields_complete and limit_fields_complete
    standard_possible = price_and_limits and suspension_status_available
    max_dq_level = "DQ3" if standard_possible else ("DQ2" if price_and_limits else "DQ1")
    missing_for_dq3 = [] if standard_possible else ["is_paused"]
    if not price_fields_complete:
        missing_for_dq3.append("daily_price_fields")
    if not limit_fields_complete:
        missing_for_dq3.extend(["limit_up", "limit_down"])

    return {
        "daily_plus_stk_limit_plus_daily_basic_plus_adj_factor_complete": (
            price_fields_complete and limit_fields_complete and adj_factor_available and daily_basic_available
        ),
        "price_fields_complete": price_fields_complete,
        "limit_fields_complete": limit_fields_complete,
        "adj_factor_available": adj_factor_available,
        "daily_basic_available": daily_basic_available,
        "suspension_status_available": suspension_status_available,
        "standard_daily_price_possible": standard_possible,
        "max_dq_level": max_dq_level,
        "missing_for_dq3": sorted(set(missing_for_dq3), key=missing_for_dq3.index),
        "note": "price and limit fields can reach at most DQ2 until a trusted is_paused source is available",
    }


def _has_columns(result: dict[str, Any], required: set[str]) -> bool:
    return bool(result.get("available")) and required.issubset(set(result.get("columns", [])))

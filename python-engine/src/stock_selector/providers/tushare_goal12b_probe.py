from collections.abc import Callable
from dataclasses import asdict, dataclass
from enum import Enum
from time import sleep
from typing import Any

import pandas as pd

from stock_selector.data.quality_contract import (
    REQUIRED_TUSHARE_SUSPEND_D_FIELDS,
    REQUIRED_TUSHARE_TRADE_CAL_FIELDS,
    classify_tushare_suspension_status_candidate,
    classify_tushare_trading_calendar_candidate,
)
from stock_selector.providers.tushare_provider import TushareProvider
from stock_selector.utils.date_validator import validate_trade_date


class ProbeStatus(str, Enum):
    PASS_WITH_ROWS = "PASS_WITH_ROWS"
    PASS_EMPTY = "PASS_EMPTY"
    BLOCKED = "BLOCKED"
    API_ERROR = "API_ERROR"
    SCHEMA_MISMATCH = "SCHEMA_MISMATCH"


GOAL12B_INTERFACES = ("trade_cal", "suspend_d")

WriteDatasetFn = Callable[[str, str, pd.DataFrame], str]
SleepFn = Callable[[float], None]


@dataclass(frozen=True)
class TushareGoal12BInterfaceSpec:
    interface: str
    fields: str
    required_fields: frozenset[str]
    contract_role: str

    def kwargs(self, trade_date: str) -> dict[str, str]:
        compact_date = trade_date.replace("-", "")
        if self.interface == "trade_cal":
            return {"exchange": "", "start_date": compact_date, "end_date": compact_date, "fields": self.fields}
        return {"trade_date": compact_date, "fields": self.fields}


INTERFACE_SPECS = (
    TushareGoal12BInterfaceSpec(
        interface="trade_cal",
        fields="exchange,cal_date,is_open,pretrade_date",
        required_fields=REQUIRED_TUSHARE_TRADE_CAL_FIELDS,
        contract_role="trading_calendar_candidate",
    ),
    TushareGoal12BInterfaceSpec(
        interface="suspend_d",
        fields="ts_code,trade_date,suspend_timing,suspend_type",
        required_fields=REQUIRED_TUSHARE_SUSPEND_D_FIELDS,
        contract_role="suspension_status_candidate",
    ),
)


def probe_tushare_goal12b(
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

    trade_cal = _by_interface(interfaces, "trade_cal")
    suspend_d = _by_interface(interfaces, "suspend_d")
    trading_calendar_contract = classify_tushare_trading_calendar_candidate(trade_cal["status"], trade_cal["columns"])
    suspension_status_contract = classify_tushare_suspension_status_candidate(suspend_d["status"], suspend_d["columns"])

    return {
        "provider": "tushare",
        "goal": "12B",
        "trade_date": trade_date,
        "sample_limit": sample_limit,
        "interfaces": interfaces,
        "trading_calendar_candidate_contract": _contract_to_dict(trading_calendar_contract),
        "suspension_status_candidate_contract": _contract_to_dict(suspension_status_contract),
        "standard_daily_price_written": False,
        "standard_raw_mainline_written": False,
        "real_backtest_run": False,
        "is_paused_fabricated": False,
    }


def _probe_interface(
    provider: TushareProvider,
    spec: TushareGoal12BInterfaceSpec,
    trade_date: str,
    write_dataset_fn: WriteDatasetFn,
    sample_limit: int,
) -> dict[str, Any]:
    try:
        raw = provider.fetch_raw_endpoint_allow_empty(spec.interface, **spec.kwargs(trade_date))
    except Exception as exc:
        return _error_result(spec, exc)

    columns = list(raw.columns)
    missing = _missing_for_contract(spec, columns)
    if missing:
        return _schema_mismatch_result(spec, raw, missing)

    sample = raw.head(sample_limit).copy()
    object_key = write_dataset_fn(spec.interface, trade_date, sample)
    status = ProbeStatus.PASS_EMPTY if raw.empty else ProbeStatus.PASS_WITH_ROWS
    return _success_result(spec, raw, sample, object_key, status)


def _success_result(
    spec: TushareGoal12BInterfaceSpec,
    raw: pd.DataFrame,
    sample: pd.DataFrame,
    object_key: str,
    status: ProbeStatus,
) -> dict[str, Any]:
    suspend_hit_candidate = spec.interface == "suspend_d" and status == ProbeStatus.PASS_WITH_ROWS
    return {
        "interface": spec.interface,
        "status": status.value,
        "available": True,
        "row_count": int(len(raw)),
        "sample_row_count": int(len(sample)),
        "columns": list(raw.columns),
        "missing_for_contract": [],
        "object_key": object_key,
        "contract_role": spec.contract_role,
        "can_enter_dq1": True,
        "can_enter_dq3": False,
        "hit_means_is_paused_true_candidate": suspend_hit_candidate,
        "miss_means_is_paused_false_candidate": False,
        "note": _note_for_success(spec.interface, status),
    }


def _schema_mismatch_result(spec: TushareGoal12BInterfaceSpec, raw: pd.DataFrame, missing: list[str]) -> dict[str, Any]:
    return {
        "interface": spec.interface,
        "status": ProbeStatus.SCHEMA_MISMATCH.value,
        "available": False,
        "row_count": int(len(raw)),
        "sample_row_count": 0,
        "columns": list(raw.columns),
        "missing_for_contract": missing,
        "object_key": None,
        "contract_role": spec.contract_role,
        "can_enter_dq1": False,
        "can_enter_dq3": False,
        "hit_means_is_paused_true_candidate": False,
        "miss_means_is_paused_false_candidate": False,
        "note": "provider response is reachable but does not expose the required Goal 12B contract fields",
    }


def _error_result(spec: TushareGoal12BInterfaceSpec, exc: Exception) -> dict[str, Any]:
    status = ProbeStatus.BLOCKED if _is_blocked_error(exc) else ProbeStatus.API_ERROR
    return {
        "interface": spec.interface,
        "status": status.value,
        "available": False,
        "row_count": 0,
        "sample_row_count": 0,
        "columns": [],
        "missing_for_contract": sorted(spec.required_fields),
        "object_key": None,
        "contract_role": spec.contract_role,
        "can_enter_dq1": False,
        "can_enter_dq3": False,
        "hit_means_is_paused_true_candidate": False,
        "miss_means_is_paused_false_candidate": False,
        "error_class": exc.__class__.__name__,
        "error": str(exc),
        "note": "provider blocked or errored; no standard-layer promotion is allowed",
    }


def _missing_for_contract(spec: TushareGoal12BInterfaceSpec, columns: list[str]) -> list[str]:
    available = set(columns)
    return [field for field in sorted(spec.required_fields) if field not in available]


def _note_for_success(interface: str, status: ProbeStatus) -> str:
    if interface == "trade_cal":
        return "trade_cal is a trading_calendar candidate only; it does not carry stock suspension or price data"
    if status == ProbeStatus.PASS_EMPTY:
        return "suspend_d returned no events for this date; this is not failure and does not imply is_paused=false"
    return "suspend_d hit can be used only as is_paused=true candidate evidence; misses cannot imply false without coverage audit"


def _is_blocked_error(exc: Exception) -> bool:
    message = str(exc)
    markers = ("没有接口", "频率超限", "权限", "积分", "permission", "rate limit", "disabled", "missing TUSHARE_TOKEN")
    return any(marker.lower() in message.lower() for marker in markers)


def _by_interface(interfaces: list[dict[str, Any]], interface: str) -> dict[str, Any]:
    return next(item for item in interfaces if item["interface"] == interface)


def _contract_to_dict(contract) -> dict[str, Any]:
    data = asdict(contract)
    if "provider_name" in data:
        data["provider"] = data.pop("provider_name")
    if "dq_level" in data:
        data["dq_level"] = data["dq_level"].value
    return data

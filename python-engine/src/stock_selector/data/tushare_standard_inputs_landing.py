from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from time import sleep
from typing import Any

import pandas as pd

from stock_selector.data.data_validator import DataValidationError, validate_dataset_frame, validate_stock_code
from stock_selector.providers.base import ProviderFetchError
from stock_selector.providers.schema_mapper import SchemaMappingError, map_provider_frame, normalize_date, normalize_stock_code
from stock_selector.utils.date_validator import validate_date_range


WriteParquetFn = Callable[[str, pd.DataFrame], str]
WriteJsonFn = Callable[[str, dict[str, Any]], str]
LoadParquetFn = Callable[[str], pd.DataFrame]
StandardReadFn = Callable[[str, str], pd.DataFrame | None]
StandardWriteFn = Callable[[str, str, pd.DataFrame], str]
GeneratedAtFn = Callable[[], str]

REPORT_SCHEMA = "goal18.tushare_standard_inputs_run_report.v1"
GOAL18_DEFAULT_MAX_CODES = 5
GOAL18_DEFAULT_MAX_TRADE_DAYS = 5
GOAL18_DEFAULT_MAX_ROWS = 50

DAILY_BASIC_COLUMNS = [
    "stock_code",
    "trade_date",
    "pe_ttm",
    "pb",
    "ps_ttm",
    "total_mv",
    "circ_mv",
    "turnover_rate",
]
FINANCIAL_COLUMNS = [
    "stock_code",
    "report_period",
    "announce_date",
    "revenue_yoy",
    "net_profit_yoy",
    "roe",
    "gross_margin",
    "debt_ratio",
    "operating_cashflow",
]
STOCK_BASIC_COLUMNS = [
    "stock_code",
    "stock_name",
    "exchange",
    "list_date",
    "delist_date",
    "industry",
    "market_type",
    "is_st",
    "trade_date",
]
ST_HISTORY_COLUMNS = ["stock_code", "st_type", "start_date", "end_date", "source"]

DOWNSTREAM_FIREWALLS = {
    "clean_daily_snapshot_entered": False,
    "factor_input_table_entered": False,
    "factor_daily_entered": False,
    "selection_result_entered": False,
    "backtest_entered": False,
}


def build_tushare_standard_inputs_output_keys(batch_id: str, trade_dates: list[str]) -> dict[str, Any]:
    batch_id = _validate_batch_id(batch_id)
    return {
        "standard_inputs_run_report": f"candidate/tushare/standard_inputs_run_report/batch_id={batch_id}/report.json",
        "stock_basic_staging": f"candidate/tushare/standard_inputs/stock_basic_staging/batch_id={batch_id}/part.parquet",
        "daily_basic_staging": {
            trade_date: (
                f"candidate/tushare/standard_inputs/daily_basic_staging/"
                f"batch_id={batch_id}/trade_date={trade_date}/part.parquet"
            )
            for trade_date in trade_dates
        },
        "financial_staging": f"candidate/tushare/standard_inputs/financial_staging/batch_id={batch_id}/part.parquet",
        "st_history_staging": f"candidate/tushare/standard_inputs/st_history_staging/batch_id={batch_id}/part.parquet",
        "stock_basic_candidate": f"candidate/tushare/standard_inputs/stock_basic_candidate/batch_id={batch_id}/part.parquet",
        "st_history_candidate": f"candidate/tushare/standard_inputs/st_history_candidate/batch_id={batch_id}/part.parquet",
    }


def build_tushare_standard_inputs_blocked_result(
    *,
    batch_id: str,
    start_date: str,
    end_date: str,
    codes: list[str],
    status: str,
    blocked_reasons: list[str],
    provider_call_enabled: bool,
    reuse_existing_staging: bool = False,
    apply_standard_write: bool = False,
    max_codes: int = GOAL18_DEFAULT_MAX_CODES,
    max_trade_days: int = GOAL18_DEFAULT_MAX_TRADE_DAYS,
    max_rows: int = GOAL18_DEFAULT_MAX_ROWS,
    generated_at_fn: GeneratedAtFn | None = None,
) -> dict[str, Any]:
    generated_at_fn = generated_at_fn or _utc_now_iso
    start_date, end_date = validate_date_range(start_date, end_date)
    codes = _normalize_codes(codes)
    trade_dates = _trade_dates(start_date, end_date)
    output_keys = build_tushare_standard_inputs_output_keys(batch_id, trade_dates)
    report = _build_run_report(
        batch_id=batch_id,
        generated_at=generated_at_fn(),
        status=status,
        requested_scope=_requested_scope(
            codes=codes,
            start_date=start_date,
            end_date=end_date,
            trade_dates=trade_dates,
            max_codes=max_codes,
            max_trade_days=max_trade_days,
            max_rows=max_rows,
        ),
        provider=_provider_report(
            provider_call_enabled=provider_call_enabled,
            reuse_existing_staging=reuse_existing_staging,
            provider_status=status,
        ),
        output_keys=output_keys,
        dataset_statuses={},
        apply_requested=apply_standard_write,
        standard_writes_performed=False,
        read_back_verification=None,
        upsert_summary={},
        blocked_reasons=_dedupe(blocked_reasons),
        cli_command=None,
    )
    return _result_from_report(report=report, output_keys=output_keys)


def run_tushare_standard_inputs_small_batch(
    *,
    batch_id: str,
    start_date: str,
    end_date: str,
    codes: list[str],
    load_parquet_fn: LoadParquetFn,
    write_parquet_fn: WriteParquetFn,
    write_json_fn: WriteJsonFn,
    standard_read_fn: StandardReadFn,
    standard_write_fn: StandardWriteFn,
    provider: Any = None,
    provider_call_enabled: bool = False,
    reuse_existing_staging: bool = False,
    apply_standard_write: bool = False,
    max_codes: int = GOAL18_DEFAULT_MAX_CODES,
    max_trade_days: int = GOAL18_DEFAULT_MAX_TRADE_DAYS,
    max_rows: int = GOAL18_DEFAULT_MAX_ROWS,
    sleep_seconds: float = 12.0,
    generated_at_fn: GeneratedAtFn | None = None,
    cli_command: str | None = None,
) -> dict[str, Any]:
    generated_at_fn = generated_at_fn or _utc_now_iso
    start_date, end_date = validate_date_range(start_date, end_date)
    codes = _normalize_codes(codes)
    batch_id = _validate_batch_id(batch_id)
    _validate_positive_limits(max_codes=max_codes, max_trade_days=max_trade_days, max_rows=max_rows)
    if sleep_seconds < 0:
        raise ValueError("sleep_seconds must be non-negative")
    if provider_call_enabled and provider is None:
        raise ValueError("provider is required when provider_call_enabled is true")

    trade_dates = _trade_dates(start_date, end_date)
    output_keys = build_tushare_standard_inputs_output_keys(batch_id, trade_dates)
    generated_at = generated_at_fn()
    fatal_blocked_reasons: list[str] = []

    if len(codes) > max_codes or len(trade_dates) > max_trade_days:
        fatal_blocked_reasons.append("BATCH_TOO_LARGE_FOR_GOAL18_SMALL_BATCH_WORKFLOW")

    provider_status = "ENABLED" if provider_call_enabled else "DISABLED"
    if provider_call_enabled and not fatal_blocked_reasons:
        try:
            _fetch_and_write_provider_staging(
                provider=provider,
                codes=codes,
                start_date=start_date,
                end_date=end_date,
                trade_dates=trade_dates,
                output_keys=output_keys,
                write_parquet_fn=write_parquet_fn,
                sleep_seconds=sleep_seconds,
            )
        except ProviderFetchError as exc:
            fatal_blocked_reasons.append("PROVIDER_FETCH_FAILED")
            fatal_blocked_reasons.append(str(exc))

    frames = _load_and_normalize_staging(
        codes=codes,
        start_date=start_date,
        trade_dates=trade_dates,
        output_keys=output_keys,
        load_parquet_fn=load_parquet_fn,
    )
    fatal_blocked_reasons.extend(frames["blocked_reasons"])
    dataset_statuses = frames["dataset_statuses"]

    total_staging_rows = sum(
        int(status.get("staging_row_count", 0))
        for status in dataset_statuses.values()
    )
    if total_staging_rows > max_rows:
        fatal_blocked_reasons.append("BATCH_TOO_LARGE_FOR_GOAL18_SMALL_BATCH_WORKFLOW")

    status = "VALIDATION_PASS" if not fatal_blocked_reasons else "BLOCKED"
    standard_writes_performed = False
    read_back_verification: dict[str, Any] | None = None
    upsert_summary = {
        "daily_basic": {"inserted_rows": 0, "updated_rows": 0, "unchanged_rows": 0},
        "financial": {"inserted_rows": 0, "updated_rows": 0, "unchanged_rows": 0},
    }

    if apply_standard_write and not fatal_blocked_reasons:
        apply_result = _apply_standard_and_candidate_writes(
            trade_dates=trade_dates,
            daily_basic_by_date=frames["daily_basic_by_date"],
            financial=frames["financial"],
            stock_basic_candidate=frames["stock_basic_candidate"],
            st_history_candidate=frames["st_history_candidate"],
            output_keys=output_keys,
            write_parquet_fn=write_parquet_fn,
            standard_read_fn=standard_read_fn,
            standard_write_fn=standard_write_fn,
        )
        standard_writes_performed = True
        read_back_verification = apply_result["read_back_verification"]
        upsert_summary = apply_result["upsert_summary"]
        _merge_dataset_statuses(dataset_statuses, apply_result["dataset_statuses"])
    elif fatal_blocked_reasons:
        for dataset in ("daily_basic", "financial"):
            dataset_statuses[dataset]["write_status"] = "BLOCKED"
    else:
        read_back_verification = {"passed": False, "skipped_reason": "DRY_RUN"}
        for dataset in ("daily_basic", "financial", "stock_basic", "st_history"):
            dataset_statuses[dataset]["write_status"] = "DRY_RUN"

    report = _build_run_report(
        batch_id=batch_id,
        generated_at=generated_at,
        status=status,
        requested_scope=_requested_scope(
            codes=codes,
            start_date=start_date,
            end_date=end_date,
            trade_dates=trade_dates,
            max_codes=max_codes,
            max_trade_days=max_trade_days,
            max_rows=max_rows,
        ),
        provider=_provider_report(
            provider_call_enabled=provider_call_enabled,
            reuse_existing_staging=reuse_existing_staging,
            provider_status=provider_status,
        ),
        output_keys=output_keys,
        dataset_statuses=dataset_statuses,
        apply_requested=apply_standard_write,
        standard_writes_performed=standard_writes_performed,
        read_back_verification=read_back_verification,
        upsert_summary=upsert_summary,
        blocked_reasons=_dedupe(fatal_blocked_reasons),
        cli_command=cli_command,
    )
    write_json_fn(output_keys["standard_inputs_run_report"], report)
    return _result_from_report(report=report, output_keys=output_keys)


def _load_and_normalize_staging(
    *,
    codes: list[str],
    start_date: str,
    trade_dates: list[str],
    output_keys: dict[str, Any],
    load_parquet_fn: LoadParquetFn,
) -> dict[str, Any]:
    blocked_reasons: list[str] = []
    dataset_statuses = _initial_dataset_statuses(output_keys)
    daily_basic_by_date: dict[str, pd.DataFrame] = {}
    financial = pd.DataFrame(columns=FINANCIAL_COLUMNS)
    stock_basic_candidate = pd.DataFrame()
    st_history_candidate = pd.DataFrame()

    daily_blocked: list[str] = []
    for trade_date in trade_dates:
        key = output_keys["daily_basic_staging"][trade_date]
        try:
            raw = load_parquet_fn(key)
            dataset_statuses["daily_basic"]["staging_row_count"] += int(len(raw))
            normalized = _normalize_daily_basic(raw, trade_date, codes)
            _validate_daily_basic_scope(normalized, trade_date, codes)
            daily_basic_by_date[trade_date] = normalized
        except FileNotFoundError:
            daily_blocked.append("DAILY_BASIC_STAGING_MISSING")
        except DataValidationError as exc:
            daily_blocked.append(_daily_basic_reason(exc))
        except (SchemaMappingError, ValueError) as exc:
            daily_blocked.append(_daily_basic_reason(exc))
    dataset_statuses["daily_basic"]["blocked_reasons"] = _dedupe(daily_blocked)
    dataset_statuses["daily_basic"]["standard_write_allowed"] = not daily_blocked
    dataset_statuses["daily_basic"]["validation_status"] = "PASS" if not daily_blocked else "BLOCKED"
    blocked_reasons.extend(dataset_statuses["daily_basic"]["blocked_reasons"])

    try:
        raw_financial = load_parquet_fn(output_keys["financial_staging"])
        dataset_statuses["financial"]["staging_row_count"] = int(len(raw_financial))
        financial = _normalize_financial(raw_financial, codes)
        _validate_financial_scope(financial, start_date)
        dataset_statuses["financial"]["as_of_check"] = {"passed": True, "as_of_date": start_date}
    except FileNotFoundError:
        dataset_statuses["financial"]["blocked_reasons"] = ["FINANCIAL_STAGING_MISSING"]
        dataset_statuses["financial"]["as_of_check"] = {"passed": False, "as_of_date": start_date}
    except DataValidationError as exc:
        reason = _financial_reason(exc)
        dataset_statuses["financial"]["blocked_reasons"] = [reason]
        dataset_statuses["financial"]["as_of_check"] = {
            "passed": reason != "FINANCIAL_ANNOUNCE_DATE_AFTER_SCOPE_START",
            "as_of_date": start_date,
        }
    except (SchemaMappingError, ValueError) as exc:
        dataset_statuses["financial"]["blocked_reasons"] = [_financial_reason(exc)]
        dataset_statuses["financial"]["as_of_check"] = {"passed": False, "as_of_date": start_date}
    dataset_statuses["financial"]["standard_write_allowed"] = not dataset_statuses["financial"]["blocked_reasons"]
    dataset_statuses["financial"]["validation_status"] = (
        "PASS" if dataset_statuses["financial"]["standard_write_allowed"] else "BLOCKED"
    )
    blocked_reasons.extend(dataset_statuses["financial"]["blocked_reasons"])

    try:
        raw_stock_basic = load_parquet_fn(output_keys["stock_basic_staging"])
        dataset_statuses["stock_basic"]["staging_row_count"] = int(len(raw_stock_basic))
        stock_basic_candidate = _normalize_stock_basic_candidate(raw_stock_basic, start_date, codes)
    except FileNotFoundError:
        stock_basic_candidate = pd.DataFrame(columns=STOCK_BASIC_COLUMNS)
        dataset_statuses["stock_basic"]["blocked_reasons"].append("STOCK_BASIC_STAGING_MISSING")
    except (DataValidationError, SchemaMappingError, ValueError) as exc:
        stock_basic_candidate = pd.DataFrame(columns=STOCK_BASIC_COLUMNS)
        dataset_statuses["stock_basic"]["blocked_reasons"].append(f"STOCK_BASIC_CANDIDATE_INVALID: {exc}")

    try:
        raw_st_history = load_parquet_fn(output_keys["st_history_staging"])
        dataset_statuses["st_history"]["staging_row_count"] = int(len(raw_st_history))
        st_history_candidate = _normalize_st_history_candidate(raw_st_history, codes)
    except FileNotFoundError:
        st_history_candidate = pd.DataFrame(columns=ST_HISTORY_COLUMNS)
        dataset_statuses["st_history"]["blocked_reasons"].append("ST_HISTORY_STAGING_MISSING")
    except (DataValidationError, SchemaMappingError, ValueError) as exc:
        st_history_candidate = pd.DataFrame(columns=ST_HISTORY_COLUMNS)
        dataset_statuses["st_history"]["blocked_reasons"].append(f"ST_HISTORY_CANDIDATE_INVALID: {exc}")

    return {
        "blocked_reasons": _dedupe(blocked_reasons),
        "dataset_statuses": dataset_statuses,
        "daily_basic_by_date": daily_basic_by_date,
        "financial": financial,
        "stock_basic_candidate": stock_basic_candidate,
        "st_history_candidate": st_history_candidate,
    }


def _apply_standard_and_candidate_writes(
    *,
    trade_dates: list[str],
    daily_basic_by_date: dict[str, pd.DataFrame],
    financial: pd.DataFrame,
    stock_basic_candidate: pd.DataFrame,
    st_history_candidate: pd.DataFrame,
    output_keys: dict[str, Any],
    write_parquet_fn: WriteParquetFn,
    standard_read_fn: StandardReadFn,
    standard_write_fn: StandardWriteFn,
) -> dict[str, Any]:
    upsert_summary = {
        "daily_basic": {"inserted_rows": 0, "updated_rows": 0, "unchanged_rows": 0},
        "financial": {"inserted_rows": 0, "updated_rows": 0, "unchanged_rows": 0},
    }
    written_keys = {"daily_basic": {}, "financial": {}, "stock_basic": None, "st_history": None}

    for trade_date in trade_dates:
        daily_incoming = daily_basic_by_date[trade_date]
        daily_existing = standard_read_fn("daily_basic", trade_date)
        daily_upsert = _upsert_frame(
            existing=daily_existing,
            incoming=daily_incoming,
            key_columns=["stock_code", "trade_date"],
            columns=DAILY_BASIC_COLUMNS,
        )
        _add_counts(upsert_summary["daily_basic"], daily_upsert["summary"])
        written_keys["daily_basic"][trade_date] = standard_write_fn("daily_basic", trade_date, daily_upsert["frame"])

        financial_incoming = financial[financial["announce_date"].astype(str) <= trade_date][FINANCIAL_COLUMNS].copy()
        validate_dataset_frame("financial", financial_incoming, trade_date)
        financial_existing = standard_read_fn("financial", trade_date)
        financial_upsert = _upsert_frame(
            existing=financial_existing,
            incoming=financial_incoming,
            key_columns=["stock_code", "report_period", "announce_date"],
            columns=FINANCIAL_COLUMNS,
        )
        _add_counts(upsert_summary["financial"], financial_upsert["summary"])
        written_keys["financial"][trade_date] = standard_write_fn("financial", trade_date, financial_upsert["frame"])

    stock_basic_to_write = _with_candidate_metadata(
        stock_basic_candidate,
        dq_level="DQ2_CURRENT_SNAPSHOT_ONLY",
        usage_limit="current_snapshot_only_not_historical",
    )
    st_history_to_write = _with_candidate_metadata(
        st_history_candidate,
        dq_level="DQ2_CURRENT_SNAPSHOT_ONLY",
        usage_limit="current_snapshot_only_not_standard_suspension_status",
    )
    written_keys["stock_basic"] = write_parquet_fn(output_keys["stock_basic_candidate"], stock_basic_to_write)
    written_keys["st_history"] = write_parquet_fn(output_keys["st_history_candidate"], st_history_to_write)

    read_back = _verify_read_back(
        trade_dates=trade_dates,
        daily_basic_by_date=daily_basic_by_date,
        financial=financial,
        standard_read_fn=standard_read_fn,
    )
    return {
        "upsert_summary": upsert_summary,
        "read_back_verification": read_back,
        "dataset_statuses": {
            "daily_basic": {
                "write_status": "WRITTEN",
                "standard_object_keys": written_keys["daily_basic"],
            },
            "financial": {
                "write_status": "WRITTEN",
                "standard_object_keys": written_keys["financial"],
            },
            "stock_basic": {
                "write_status": "CANDIDATE_WRITTEN",
                "candidate_object_key": output_keys["stock_basic_candidate"],
            },
            "st_history": {
                "write_status": "CANDIDATE_WRITTEN",
                "candidate_object_key": output_keys["st_history_candidate"],
            },
        },
    }


def _normalize_daily_basic(raw: pd.DataFrame, trade_date: str, codes: list[str]) -> pd.DataFrame:
    if set(DAILY_BASIC_COLUMNS).issubset(raw.columns):
        mapped = raw[DAILY_BASIC_COLUMNS].copy()
        mapped["stock_code"] = mapped["stock_code"].map(normalize_stock_code)
        mapped["trade_date"] = mapped["trade_date"].map(normalize_date)
        for column in DAILY_BASIC_COLUMNS[2:]:
            mapped[column] = pd.to_numeric(mapped[column], errors="raise")
        validate_dataset_frame("daily_basic", mapped, trade_date)
    else:
        mapped = map_provider_frame("tushare", "daily_basic", raw, trade_date)
    scoped = mapped[mapped["stock_code"].isin(codes)][DAILY_BASIC_COLUMNS].copy()
    _validate_daily_basic_numeric(scoped)
    return scoped.reset_index(drop=True)


def _normalize_financial(raw: pd.DataFrame, codes: list[str]) -> pd.DataFrame:
    if set(FINANCIAL_COLUMNS).issubset(raw.columns):
        mapped = raw[FINANCIAL_COLUMNS].copy()
        mapped["stock_code"] = mapped["stock_code"].map(normalize_stock_code)
        mapped["report_period"] = mapped["report_period"].map(normalize_date)
        mapped["announce_date"] = mapped["announce_date"].map(normalize_date)
        for column in FINANCIAL_COLUMNS[3:]:
            mapped[column] = pd.to_numeric(mapped[column], errors="raise")
    else:
        mapped = map_provider_frame("tushare", "financial", raw, _max_announce_date_or_today(raw))
    scoped = mapped[mapped["stock_code"].isin(codes)][FINANCIAL_COLUMNS].copy()
    if scoped.empty:
        raise DataValidationError("financial is empty for requested codes")
    if scoped.duplicated(["stock_code", "report_period", "announce_date"]).any():
        raise DataValidationError("duplicate financial rows by key")
    return scoped.reset_index(drop=True)


def _normalize_stock_basic_candidate(raw: pd.DataFrame, trade_date: str, codes: list[str]) -> pd.DataFrame:
    if "stock_code" in raw.columns:
        stock_code = raw["stock_code"].map(normalize_stock_code)
    elif "ts_code" in raw.columns:
        stock_code = raw["ts_code"].map(normalize_stock_code)
    else:
        raise DataValidationError("missing stock code")
    result = pd.DataFrame()
    result["stock_code"] = stock_code
    result["stock_name"] = raw["stock_name"] if "stock_name" in raw.columns else raw.get("name", "")
    result["exchange"] = raw["exchange"] if "exchange" in raw.columns else result["stock_code"].map(_exchange_from_code)
    result["list_date"] = raw.get("list_date", "").map(normalize_date) if "list_date" in raw.columns else None
    result["delist_date"] = raw["delist_date"].map(_normalize_nullable_date) if "delist_date" in raw.columns else None
    result["industry"] = raw["industry"] if "industry" in raw.columns else ""
    result["market_type"] = raw["market_type"] if "market_type" in raw.columns else raw.get("market", "")
    if "is_st" in raw.columns:
        result["is_st"] = raw["is_st"].map(_to_bool)
    else:
        result["is_st"] = result["stock_name"].astype(str).str.upper().str.contains("ST")
    result["trade_date"] = trade_date
    result = result[result["stock_code"].isin(codes)][STOCK_BASIC_COLUMNS].copy()
    if not result.empty:
        validate_dataset_frame("stock_basic", result, trade_date)
    return result.reset_index(drop=True)


def _normalize_st_history_candidate(raw: pd.DataFrame, codes: list[str]) -> pd.DataFrame:
    result = pd.DataFrame()
    if "stock_code" in raw.columns:
        result["stock_code"] = raw["stock_code"].map(normalize_stock_code)
    elif "ts_code" in raw.columns:
        result["stock_code"] = raw["ts_code"].map(normalize_stock_code)
    else:
        raise DataValidationError("missing stock code")
    result["st_type"] = raw["st_type"] if "st_type" in raw.columns else "CURRENT_NAME_SNAPSHOT"
    result["start_date"] = raw["start_date"].map(normalize_date) if "start_date" in raw.columns else None
    result["end_date"] = raw["end_date"].map(_normalize_nullable_date) if "end_date" in raw.columns else None
    result["source"] = raw["source"] if "source" in raw.columns else "current_stock_basic_snapshot"
    result = result[result["stock_code"].isin(codes)][ST_HISTORY_COLUMNS].copy()
    if not result.empty:
        validate_dataset_frame("st_history", result, result["start_date"].iloc[0])
    return result.reset_index(drop=True)


def _validate_daily_basic_scope(frame: pd.DataFrame, trade_date: str, codes: list[str]) -> None:
    if frame.empty:
        raise DataValidationError("daily_basic is empty for requested scope")
    if frame.duplicated(["stock_code", "trade_date"]).any():
        raise DataValidationError("duplicate daily_basic rows by key")
    if sorted(frame["stock_code"].tolist()) != sorted(codes):
        raise DataValidationError("daily_basic coverage incomplete")
    validate_dataset_frame("daily_basic", frame, trade_date)


def _validate_daily_basic_numeric(frame: pd.DataFrame) -> None:
    numeric_columns = ["pe_ttm", "pb", "ps_ttm", "total_mv", "circ_mv", "turnover_rate"]
    for column in numeric_columns:
        values = pd.to_numeric(frame[column], errors="coerce")
        if values.isna().any():
            raise DataValidationError("daily_basic numeric invalid")
    non_negative_columns = ["pb", "ps_ttm", "total_mv", "circ_mv", "turnover_rate"]
    for column in non_negative_columns:
        if (pd.to_numeric(frame[column]) < 0).any():
            raise DataValidationError("daily_basic numeric invalid")


def _validate_financial_scope(frame: pd.DataFrame, start_date: str) -> None:
    if frame.empty:
        raise DataValidationError("financial is empty for requested scope")
    if (frame["announce_date"].astype(str) > start_date).any():
        raise DataValidationError("announce_date must be <= scope start_date")
    validate_dataset_frame("financial", frame, start_date)


def _upsert_frame(
    *,
    existing: pd.DataFrame | None,
    incoming: pd.DataFrame,
    key_columns: list[str],
    columns: list[str],
) -> dict[str, Any]:
    incoming = incoming[columns].copy().reset_index(drop=True)
    existing = pd.DataFrame(columns=columns) if existing is None or existing.empty else existing[columns].copy()
    existing = existing.drop_duplicates(key_columns, keep="last").reset_index(drop=True)

    existing_by_key = {_row_key(row, key_columns): row for _, row in existing.iterrows()}
    result_rows = []
    inserted = 0
    updated = 0
    unchanged = 0
    incoming_keys = set()
    incoming_by_key = {}

    for _, row in incoming.iterrows():
        key = _row_key(row, key_columns)
        incoming_keys.add(key)
        incoming_by_key[key] = row
        existing_row = existing_by_key.get(key)
        if existing_row is None:
            inserted += 1
        elif _rows_equal(existing_row, row, columns):
            unchanged += 1
        else:
            updated += 1

    for _, row in existing.iterrows():
        if _row_key(row, key_columns) not in incoming_keys:
            result_rows.append(row.to_dict())
    for row in incoming_by_key.values():
        result_rows.append(row.to_dict())

    result = pd.DataFrame(result_rows, columns=columns)
    if not result.empty:
        result = result.sort_values(key_columns).reset_index(drop=True)
    return {
        "frame": result,
        "summary": {
            "inserted_rows": inserted,
            "updated_rows": updated,
            "unchanged_rows": unchanged,
        },
    }


def _verify_read_back(
    *,
    trade_dates: list[str],
    daily_basic_by_date: dict[str, pd.DataFrame],
    financial: pd.DataFrame,
    standard_read_fn: StandardReadFn,
) -> dict[str, Any]:
    details = []
    for trade_date in trade_dates:
        daily = standard_read_fn("daily_basic", trade_date)
        financial_partition = standard_read_fn("financial", trade_date)
        try:
            validate_dataset_frame("daily_basic", daily, trade_date)
            validate_dataset_frame("financial", financial_partition, trade_date)
            daily_passed = _contains_keys(daily, daily_basic_by_date[trade_date], ["stock_code", "trade_date"])
            financial_passed = _contains_keys(
                financial_partition,
                financial[financial["announce_date"].astype(str) <= trade_date],
                ["stock_code", "report_period", "announce_date"],
            )
            details.append(
                {
                    "trade_date": trade_date,
                    "daily_basic": daily_passed,
                    "financial": financial_passed,
                }
            )
        except Exception as exc:
            details.append({"trade_date": trade_date, "passed": False, "error": str(exc)})
    return {"passed": all(item.get("daily_basic") and item.get("financial") for item in details), "details": details}


def _fetch_and_write_provider_staging(
    *,
    provider: Any,
    codes: list[str],
    start_date: str,
    end_date: str,
    trade_dates: list[str],
    output_keys: dict[str, Any],
    write_parquet_fn: WriteParquetFn,
    sleep_seconds: float,
) -> None:
    stock_basic = provider.fetch_raw_endpoint_allow_empty(
        "stock_basic",
        exchange="",
        list_status="L",
        fields="ts_code,name,exchange,industry,market,list_date",
    )
    write_parquet_fn(output_keys["stock_basic_staging"], stock_basic)

    for trade_date in trade_dates:
        daily_basic = provider.fetch_raw_endpoint_allow_empty(
            "daily_basic",
            trade_date=trade_date.replace("-", ""),
            fields="ts_code,trade_date,pe_ttm,pb,ps_ttm,total_mv,circ_mv,turnover_rate",
        )
        write_parquet_fn(output_keys["daily_basic_staging"][trade_date], daily_basic)
        if sleep_seconds:
            sleep(sleep_seconds)

    financial_frames = []
    for code in codes:
        financial_frames.append(
            provider.fetch_raw_endpoint_allow_empty(
                "fina_indicator",
                ts_code=code,
                start_date=start_date.replace("-", ""),
                end_date=end_date.replace("-", ""),
                fields=(
                    "ts_code,end_date,ann_date,revenue_yoy,net_profit_yoy,roe,"
                    "gross_margin,debt_ratio,operating_cashflow"
                ),
            )
        )
        if sleep_seconds:
            sleep(sleep_seconds)
    financial = pd.concat(financial_frames, ignore_index=True) if financial_frames else pd.DataFrame()
    write_parquet_fn(output_keys["financial_staging"], financial)
    write_parquet_fn(output_keys["st_history_staging"], _derive_st_history_from_stock_basic(stock_basic, start_date))


def _derive_st_history_from_stock_basic(stock_basic: pd.DataFrame, start_date: str) -> pd.DataFrame:
    if stock_basic.empty:
        return pd.DataFrame(columns=["ts_code", "st_type", "start_date", "end_date", "source"])
    name_column = "name" if "name" in stock_basic.columns else "stock_name"
    code_column = "ts_code" if "ts_code" in stock_basic.columns else "stock_code"
    st_rows = stock_basic[stock_basic[name_column].astype(str).str.upper().str.contains("ST", na=False)].copy()
    if st_rows.empty:
        return pd.DataFrame(columns=["ts_code", "st_type", "start_date", "end_date", "source"])
    return pd.DataFrame(
        {
            "ts_code": st_rows[code_column],
            "st_type": "CURRENT_NAME_SNAPSHOT",
            "start_date": start_date,
            "end_date": None,
            "source": "current_stock_basic_snapshot",
        }
    )


def _initial_dataset_statuses(output_keys: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        "daily_basic": {
            "dataset": "daily_basic",
            "source_object_keys": output_keys["daily_basic_staging"],
            "staging_row_count": 0,
            "standard_write_allowed": False,
            "blocked_reasons": [],
            "validation_status": "PENDING",
            "write_status": "PENDING",
        },
        "financial": {
            "dataset": "financial",
            "source_object_key": output_keys["financial_staging"],
            "staging_row_count": 0,
            "standard_write_allowed": False,
            "blocked_reasons": [],
            "validation_status": "PENDING",
            "write_status": "PENDING",
            "as_of_check": {"passed": False},
        },
        "stock_basic": {
            "dataset": "stock_basic",
            "source_object_key": output_keys["stock_basic_staging"],
            "staging_row_count": 0,
            "standard_write_allowed": False,
            "dq_level": "DQ2_CURRENT_SNAPSHOT_ONLY",
            "blocked_reasons": ["CURRENT_SNAPSHOT_NOT_HISTORICAL"],
            "validation_status": "CANDIDATE_ONLY",
            "write_status": "PENDING",
        },
        "st_history": {
            "dataset": "st_history",
            "source_object_key": output_keys["st_history_staging"],
            "staging_row_count": 0,
            "standard_write_allowed": False,
            "dq_level": "DQ2_CURRENT_SNAPSHOT_ONLY",
            "blocked_reasons": ["ST_STATUS_NOT_HISTORICAL"],
            "validation_status": "CANDIDATE_ONLY",
            "write_status": "PENDING",
        },
    }


def _build_run_report(
    *,
    batch_id: str,
    generated_at: str,
    status: str,
    requested_scope: dict[str, Any],
    provider: dict[str, Any],
    output_keys: dict[str, Any],
    dataset_statuses: dict[str, dict[str, Any]],
    apply_requested: bool,
    standard_writes_performed: bool,
    read_back_verification: dict[str, Any] | None,
    upsert_summary: dict[str, Any],
    blocked_reasons: list[str],
    cli_command: str | None,
) -> dict[str, Any]:
    return {
        "schema_version": REPORT_SCHEMA,
        "goal": "18",
        "provider_name": "tushare",
        "batch_id": batch_id,
        "generated_at": generated_at,
        "status": status,
        "requested_scope": requested_scope,
        "provider": provider,
        "mode": "APPLY" if apply_requested else "DRY_RUN",
        "apply": {
            "requested": bool(apply_requested),
            "standard_writes_performed": bool(standard_writes_performed),
        },
        "output_object_keys": output_keys,
        "dataset_statuses": dataset_statuses,
        "blocked_reasons": _dedupe(blocked_reasons),
        "read_back_verification": read_back_verification,
        "upsert_summary": upsert_summary,
        "downstream_firewalls": dict(DOWNSTREAM_FIREWALLS),
        "standard_writes_performed": bool(standard_writes_performed),
        "standard_suspension_status_write_performed": False,
        "clean_factor_selection_backtest_entered": False,
        "real_backtest_performed": False,
        "cli_command": cli_command,
    }


def _result_from_report(*, report: dict[str, Any], output_keys: dict[str, Any]) -> dict[str, Any]:
    return {
        "goal": "18",
        "provider": "tushare",
        "status": report["status"],
        "batch_id": report["batch_id"],
        "mode": report["mode"],
        "standard_inputs_run_report_key": output_keys["standard_inputs_run_report"],
        "standard_inputs_run_report": report,
        "output_object_keys": output_keys,
        "provider_call_requested": bool(report["provider"]["enabled"]),
        "reused_existing_staging": bool(report["provider"]["reuse_existing_staging"]),
        "apply_requested": bool(report["apply"]["requested"]),
        "standard_writes_performed": bool(report["standard_writes_performed"]),
        "standard_suspension_status_write_performed": False,
        "clean_factor_selection_backtest_entered": False,
        "real_backtest_performed": False,
        "read_back_verification": report["read_back_verification"],
        "upsert_summary": report["upsert_summary"],
        "blocked_reasons": report["blocked_reasons"],
    }


def _requested_scope(
    *,
    codes: list[str],
    start_date: str,
    end_date: str,
    trade_dates: list[str],
    max_codes: int,
    max_trade_days: int,
    max_rows: int,
) -> dict[str, Any]:
    return {
        "codes": list(codes),
        "start_date": start_date,
        "end_date": end_date,
        "trade_dates": list(trade_dates),
        "max_codes": max_codes,
        "max_trade_days": max_trade_days,
        "max_rows": max_rows,
    }


def _provider_report(*, provider_call_enabled: bool, reuse_existing_staging: bool, provider_status: str) -> dict[str, Any]:
    return {
        "enabled": bool(provider_call_enabled),
        "status": provider_status,
        "reuse_existing_staging": bool(reuse_existing_staging),
    }


def _daily_basic_reason(exc: Exception) -> str:
    message = str(exc).lower()
    if "numeric invalid" in message:
        return "DAILY_BASIC_NUMERIC_INVALID"
    if "duplicate" in message:
        return "DAILY_BASIC_DUPLICATE_CODE_DATE"
    if "coverage incomplete" in message or "empty for requested scope" in message:
        return "DAILY_BASIC_COVERAGE_INCOMPLETE"
    return "DAILY_BASIC_SCHEMA_INVALID"


def _financial_reason(exc: Exception) -> str:
    message = str(exc).lower()
    if "announce_date" in message and ("scope start" in message or "<= trade_date" in message):
        return "FINANCIAL_ANNOUNCE_DATE_AFTER_SCOPE_START"
    if "duplicate" in message:
        return "FINANCIAL_DUPLICATE_DISCLOSURE_KEY"
    if "empty" in message:
        return "FINANCIAL_EMPTY_FOR_SCOPE"
    return "FINANCIAL_SCHEMA_UNCERTAIN"


def _trade_dates(start_date: str, end_date: str) -> list[str]:
    current = datetime.fromisoformat(start_date).date()
    end = datetime.fromisoformat(end_date).date()
    dates = []
    while current <= end:
        dates.append(current.isoformat())
        current += timedelta(days=1)
    return dates


def _normalize_codes(codes: list[str]) -> list[str]:
    normalized = []
    for code in codes:
        text = str(code).strip().upper()
        if text:
            normalized.append(validate_stock_code(text))
    if not normalized:
        raise ValueError("codes must not be empty")
    return normalized


def _validate_batch_id(batch_id: str) -> str:
    text = str(batch_id or "").strip()
    if not text:
        raise ValueError("batch_id is required")
    if any(ch in text for ch in ("/", "\\", "..")):
        raise ValueError("batch_id must not contain path separators")
    return text


def _validate_positive_limits(*, max_codes: int, max_trade_days: int, max_rows: int) -> None:
    if max_codes <= 0:
        raise ValueError("max_codes must be positive")
    if max_trade_days <= 0:
        raise ValueError("max_trade_days must be positive")
    if max_rows <= 0:
        raise ValueError("max_rows must be positive")


def _merge_dataset_statuses(base: dict[str, dict[str, Any]], updates: dict[str, dict[str, Any]]) -> None:
    for dataset, update in updates.items():
        base[dataset].update(update)


def _add_counts(total: dict[str, int], item: dict[str, int]) -> None:
    for key in ("inserted_rows", "updated_rows", "unchanged_rows"):
        total[key] += int(item.get(key, 0))


def _row_key(row: pd.Series, key_columns: list[str]) -> tuple[Any, ...]:
    return tuple(row[column] for column in key_columns)


def _rows_equal(left: pd.Series, right: pd.Series, columns: list[str]) -> bool:
    for column in columns:
        left_value = left[column]
        right_value = right[column]
        if pd.isna(left_value) and pd.isna(right_value):
            continue
        if left_value != right_value:
            return False
    return True


def _contains_keys(existing: pd.DataFrame, incoming: pd.DataFrame, key_columns: list[str]) -> bool:
    existing_keys = {tuple(row[column] for column in key_columns) for _, row in existing.iterrows()}
    incoming_keys = {tuple(row[column] for column in key_columns) for _, row in incoming.iterrows()}
    return incoming_keys.issubset(existing_keys)


def _with_candidate_metadata(frame: pd.DataFrame, *, dq_level: str, usage_limit: str) -> pd.DataFrame:
    result = frame.copy()
    result["dq_level"] = dq_level
    result["usage_limit"] = usage_limit
    result["standard_write_allowed"] = False
    return result


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
    raise DataValidationError(f"invalid boolean value: {value}")


def _exchange_from_code(stock_code: str) -> str:
    if stock_code.endswith(".SZ"):
        return "SZSE"
    if stock_code.endswith(".SH"):
        return "SSE"
    if stock_code.endswith(".BJ"):
        return "BSE"
    return ""


def _max_announce_date_or_today(raw: pd.DataFrame) -> str:
    if "ann_date" in raw.columns and not raw["ann_date"].dropna().empty:
        return max(normalize_date(value) for value in raw["ann_date"].dropna())
    return datetime.now(timezone.utc).date().isoformat()


def _dedupe(values: list[str]) -> list[str]:
    seen = set()
    result = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

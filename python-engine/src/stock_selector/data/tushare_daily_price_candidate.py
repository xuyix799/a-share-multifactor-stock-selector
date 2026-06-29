from collections.abc import Callable
from dataclasses import asdict, dataclass
from typing import Any

import pandas as pd

from stock_selector.data.quality_contract import (
    DAILY_PRICE_CANDIDATE_REQUIRED_INPUTS,
    DataQualityLevel,
    can_build_daily_price_candidate_dry_run,
    can_promote_daily_price_candidate_to_standard,
    classify_tushare_daily_price_candidate,
)
from stock_selector.utils.date_validator import validate_trade_date


@dataclass(frozen=True)
class SmokeInput:
    dataset: str
    object_key: str
    frame: pd.DataFrame | None


LoadSmokeInputFn = Callable[[str, str], SmokeInput]

JOIN_KEYS = ["ts_code", "trade_date"]

FIELD_ALIASES = {
    "daily": {
        "ts_code": ("ts_code",),
        "trade_date": ("trade_date",),
        "open": ("open",),
        "high": ("high",),
        "low": ("low",),
        "close": ("close",),
        "pre_close": ("pre_close",),
        "volume": ("volume", "vol"),
        "amount": ("amount",),
    },
    "stk_limit": {
        "ts_code": ("ts_code",),
        "trade_date": ("trade_date",),
        "limit_up": ("limit_up", "up_limit"),
        "limit_down": ("limit_down", "down_limit"),
    },
    "adj_factor": {
        "ts_code": ("ts_code",),
        "trade_date": ("trade_date",),
        "adj_factor": ("adj_factor",),
    },
    "trade_cal": {
        "trade_date": ("trade_date", "cal_date"),
        "is_open": ("is_open",),
        "exchange": ("exchange",),
    },
    "suspend_d": {
        "ts_code": ("ts_code",),
        "trade_date": ("suspend_date", "trade_date"),
    },
}

OPTIONAL_ALIASES = {
    "suspend_d": {
        "resume_date": ("resume_date",),
        "suspend_type": ("suspend_type",),
        "suspend_timing": ("suspend_timing",),
    }
}

FIELD_SOURCES = {
    "open": "daily",
    "high": "daily",
    "low": "daily",
    "close": "daily",
    "pre_close": "daily",
    "volume": "daily",
    "amount": "daily",
    "limit_up": "stk_limit",
    "limit_down": "stk_limit",
    "adj_factor": "adj_factor",
    "trading_day_confirmed": "trade_cal",
    "trade_cal_is_open": "trade_cal",
    "trade_cal_exchange": "trade_cal",
    "pause_status": "suspend_d_hit_or_unresolved_unknown",
    "is_paused_true_candidate": "suspend_d_hit_only",
    "suspend_type": "suspend_d",
    "suspend_timing": "suspend_d",
}

SAFETY_FLAGS = {
    "standard_daily_price_written": False,
    "real_raw_mainline_written": False,
    "cleaning_mainline_entered": False,
    "factor_mainline_entered": False,
    "selection_mainline_entered": False,
    "backtest_mainline_entered": False,
    "spring_api_changed": False,
    "is_paused_fabricated": False,
    "suspend_miss_inferred_false": False,
}

INFERENCE_GUARDS = {
    "volume_zero_used_as_pause": False,
    "amount_zero_used_as_pause": False,
    "daily_missing_row_used_as_pause": False,
    "unchanged_price_used_as_pause": False,
}


def build_dry_run_output_keys(trade_date: str) -> dict[str, str]:
    trade_date = validate_trade_date(trade_date)
    prefix = f"smoke/tushare/daily_price_candidate_dry_run/trade_date={trade_date}"
    return {
        "report": f"{prefix}/report.json",
        "candidate": f"{prefix}/part.parquet",
    }


def dry_run_tushare_daily_price_candidate(
    trade_date: str,
    *,
    sample_limit: int,
    load_smoke_input_fn: LoadSmokeInputFn,
) -> dict[str, Any]:
    trade_date = validate_trade_date(trade_date)
    if sample_limit <= 0:
        raise ValueError("sample_limit must be positive")

    smoke_inputs = [load_smoke_input_fn(dataset, trade_date) for dataset in DAILY_PRICE_CANDIDATE_REQUIRED_INPUTS]
    inputs_report = {item.dataset: _input_report(item) for item in smoke_inputs}
    missing_inputs = [{"dataset": item.dataset, "object_key": item.object_key} for item in smoke_inputs if item.frame is None]
    output_keys = build_dry_run_output_keys(trade_date)
    contract = classify_tushare_daily_price_candidate(source_layer="smoke")

    if missing_inputs:
        return _blocked_report(
            trade_date=trade_date,
            sample_limit=sample_limit,
            status="BLOCKED_BY_MISSING_SMOKE_INPUT",
            inputs_report=inputs_report,
            missing_inputs=missing_inputs,
            output_keys=output_keys,
            contract=contract,
            reasons=[f"missing smoke input: {item['object_key']}" for item in missing_inputs],
        )

    available_inputs = [item.dataset for item in smoke_inputs]
    if not can_build_daily_price_candidate_dry_run(available_inputs):
        return _blocked_report(
            trade_date=trade_date,
            sample_limit=sample_limit,
            status="BLOCKED_BY_MISSING_SMOKE_INPUT",
            inputs_report=inputs_report,
            missing_inputs=missing_inputs,
            output_keys=output_keys,
            contract=contract,
            reasons=["not all required Tushare smoke inputs are available"],
        )

    frames = {item.dataset: item.frame.copy() for item in smoke_inputs if item.frame is not None}
    missing_required = _missing_required_fields(frames)
    if missing_required:
        return _blocked_report(
            trade_date=trade_date,
            sample_limit=sample_limit,
            status="BLOCKED_BY_MISSING_FIELDS",
            inputs_report=inputs_report,
            missing_inputs=[],
            output_keys=output_keys,
            contract=contract,
            reasons=[f"{dataset} missing fields: {', '.join(fields)}" for dataset, fields in missing_required.items()],
        )

    normalized = _normalize_inputs(frames, trade_date)
    duplicate_checks = _duplicate_key_checks(normalized)
    trade_date_consistency = _trade_date_consistency(frames, trade_date)
    candidate = _build_candidate(normalized, trade_date, sample_limit)
    coverage = _coverage_report(candidate, normalized)
    missing_field_stats = _missing_field_stats(candidate)
    pause_status_counts = _pause_status_counts(candidate)
    readiness = _readiness_report(candidate, coverage, missing_field_stats)

    report = _base_report(trade_date, sample_limit, inputs_report, output_keys, contract)
    report.update(
        {
            "status": "DRY_RUN_COMPLETED",
            "missing_inputs": [],
            "join": {
                "base_table": "daily",
                "joined_tables": ["stk_limit", "adj_factor", "trade_cal", "suspend_d"],
                "join_keys": JOIN_KEYS,
                "candidate_row_count": int(len(candidate)),
            },
            "field_sources": FIELD_SOURCES,
            "missing_field_stats": missing_field_stats,
            "duplicate_key_checks": duplicate_checks,
            "trade_date_consistency": trade_date_consistency,
            "coverage": coverage,
            "pause_status_counts": pause_status_counts,
            "readiness": readiness,
            "inference_guards": INFERENCE_GUARDS,
            "safety": SAFETY_FLAGS,
            "candidate_rows": _records(candidate),
        }
    )
    return report


def candidate_frame_from_report(report: dict[str, Any]) -> pd.DataFrame:
    return pd.DataFrame(report.get("candidate_rows", []))


def _blocked_report(
    *,
    trade_date: str,
    sample_limit: int,
    status: str,
    inputs_report: dict[str, dict[str, Any]],
    missing_inputs: list[dict[str, str]],
    output_keys: dict[str, str],
    contract,
    reasons: list[str],
) -> dict[str, Any]:
    report = _base_report(trade_date, sample_limit, inputs_report, output_keys, contract)
    report.update(
        {
            "status": status,
            "missing_inputs": missing_inputs,
            "join": {
                "base_table": "daily",
                "joined_tables": ["stk_limit", "adj_factor", "trade_cal", "suspend_d"],
                "join_keys": JOIN_KEYS,
                "candidate_row_count": 0,
            },
            "field_sources": FIELD_SOURCES,
            "missing_field_stats": {},
            "duplicate_key_checks": {},
            "trade_date_consistency": {},
            "coverage": {},
            "pause_status_counts": {"true_candidate": 0, "unknown": 0, "false": 0},
            "readiness": {
                "ready_for_dq3_promotion": False,
                "status": status,
                "reasons": reasons,
                "required_future_gates": list(contract.required_future_gates),
            },
            "inference_guards": INFERENCE_GUARDS,
            "safety": SAFETY_FLAGS,
            "candidate_rows": [],
        }
    )
    return report


def _base_report(
    trade_date: str,
    sample_limit: int,
    inputs_report: dict[str, dict[str, Any]],
    output_keys: dict[str, str],
    contract,
) -> dict[str, Any]:
    return {
        "provider": "tushare",
        "goal": "12C",
        "trade_date": trade_date,
        "sample_limit": int(sample_limit),
        "candidate_dataset": "daily_price_candidate",
        "input_smoke_object_keys": {dataset: info["object_key"] for dataset, info in inputs_report.items()},
        "inputs": inputs_report,
        "output_object_keys": output_keys,
        "contract": _contract_to_report(contract),
    }


def _input_report(item: SmokeInput) -> dict[str, Any]:
    if item.frame is None:
        return {"object_key": item.object_key, "exists": False, "row_count": 0, "columns": [], "missing_required_fields": []}
    return {
        "object_key": item.object_key,
        "exists": True,
        "row_count": int(len(item.frame)),
        "columns": list(item.frame.columns),
        "missing_required_fields": _missing_fields(item.dataset, item.frame),
    }


def _missing_required_fields(frames: dict[str, pd.DataFrame]) -> dict[str, list[str]]:
    result = {}
    for dataset, frame in frames.items():
        missing = _missing_fields(dataset, frame)
        if missing:
            result[dataset] = missing
    return result


def _missing_fields(dataset: str, frame: pd.DataFrame) -> list[str]:
    aliases = FIELD_ALIASES[dataset]
    return [field for field, options in aliases.items() if _pick_column(frame, options) is None]


def _normalize_inputs(frames: dict[str, pd.DataFrame], trade_date: str) -> dict[str, pd.DataFrame]:
    daily = _normalized_frame(frames["daily"], "daily")
    stk_limit = _normalized_frame(frames["stk_limit"], "stk_limit")
    adj_factor = _normalized_frame(frames["adj_factor"], "adj_factor")
    trade_cal = _normalized_frame(frames["trade_cal"], "trade_cal")
    suspend_d = _normalized_frame(frames["suspend_d"], "suspend_d")

    return {
        "daily": daily[daily["trade_date"] == trade_date].copy(),
        "stk_limit": stk_limit[stk_limit["trade_date"] == trade_date].copy(),
        "adj_factor": adj_factor[adj_factor["trade_date"] == trade_date].copy(),
        "trade_cal": trade_cal[trade_cal["trade_date"] == trade_date].copy(),
        "suspend_d": suspend_d[suspend_d["trade_date"] == trade_date].copy(),
    }


def _normalized_frame(frame: pd.DataFrame, dataset: str) -> pd.DataFrame:
    data = pd.DataFrame()
    for field, aliases in FIELD_ALIASES[dataset].items():
        column = _pick_column(frame, aliases)
        data[field] = frame[column] if column is not None else pd.NA
    for field, aliases in OPTIONAL_ALIASES.get(dataset, {}).items():
        column = _pick_column(frame, aliases)
        if column is not None:
            data[field] = frame[column]
    if "trade_date" in data.columns:
        data["trade_date"] = data["trade_date"].map(_normalize_tushare_date)
    if "resume_date" in data.columns:
        data["resume_date"] = data["resume_date"].map(_normalize_tushare_date)
    return data


def _build_candidate(normalized: dict[str, pd.DataFrame], trade_date: str, sample_limit: int) -> pd.DataFrame:
    daily = normalized["daily"].head(sample_limit).copy()
    limits = _drop_duplicate_keys(normalized["stk_limit"])[JOIN_KEYS + ["limit_up", "limit_down"]]
    adj_factor = _drop_duplicate_keys(normalized["adj_factor"])[JOIN_KEYS + ["adj_factor"]]
    suspend_columns = [column for column in ["ts_code", "trade_date", "suspend_type", "suspend_timing", "resume_date"] if column in normalized["suspend_d"].columns]
    suspend_d = _drop_duplicate_keys(normalized["suspend_d"])[suspend_columns].copy()
    suspend_d["_suspend_hit"] = True

    candidate = daily.merge(limits, on=JOIN_KEYS, how="left")
    candidate = candidate.merge(adj_factor, on=JOIN_KEYS, how="left")
    candidate = candidate.merge(suspend_d, on=JOIN_KEYS, how="left")

    trade_cal = normalized["trade_cal"]
    open_rows = trade_cal[pd.to_numeric(trade_cal["is_open"], errors="coerce") == 1]
    candidate["trading_day_confirmed"] = bool(not open_rows.empty)
    candidate["trade_cal_is_open"] = 1 if not open_rows.empty else pd.NA
    candidate["trade_cal_exchange"] = ",".join(sorted(set(open_rows["exchange"].dropna().astype(str)))) if not open_rows.empty else pd.NA
    candidate["pause_status"] = candidate["_suspend_hit"].map(lambda value: "true_candidate" if value is True else "unknown")
    candidate["is_paused_true_candidate"] = candidate["_suspend_hit"].map(lambda value: True if value is True else None)
    candidate["dq_level"] = DataQualityLevel.DQ1.value
    candidate["source_layer"] = "smoke_candidate_dry_run"
    candidate["source_provider"] = "tushare"
    return candidate.drop(columns=["_suspend_hit"])


def _coverage_report(candidate: pd.DataFrame, normalized: dict[str, pd.DataFrame]) -> dict[str, Any]:
    row_count = int(len(candidate))
    limit_matches = int(candidate[["limit_up", "limit_down"]].notna().all(axis=1).sum()) if row_count else 0
    adj_matches = int(candidate["adj_factor"].notna().sum()) if row_count else 0
    suspend_matches = int((candidate["pause_status"] == "true_candidate").sum()) if row_count else 0
    daily_keys = set(map(tuple, normalized["daily"][JOIN_KEYS].itertuples(index=False, name=None)))
    suspend_keys = set(map(tuple, normalized["suspend_d"][JOIN_KEYS].itertuples(index=False, name=None)))
    events_not_in_daily = sorted(suspend_keys - daily_keys)
    trade_cal_open = bool(candidate["trading_day_confirmed"].all()) if row_count else False
    return {
        "limit_price_fields": {
            "all_fields": _coverage_item(limit_matches, row_count),
            "limit_up": _coverage_item(int(candidate["limit_up"].notna().sum()) if row_count else 0, row_count),
            "limit_down": _coverage_item(int(candidate["limit_down"].notna().sum()) if row_count else 0, row_count),
        },
        "adj_factor": _coverage_item(adj_matches, row_count),
        "suspend_d_match": _coverage_item(suspend_matches, row_count),
        "suspend_d_events_not_in_daily": {"row_count": len(events_not_in_daily), "keys": [list(item) for item in events_not_in_daily]},
        "trade_cal": {
            "confirmed_open_trading_day": trade_cal_open,
            "pause_source": False,
            "row_count": int(len(normalized["trade_cal"])),
        },
    }


def _coverage_item(matched_rows: int, row_count: int) -> dict[str, Any]:
    return {
        "matched_rows": int(matched_rows),
        "missing_rows": int(row_count - matched_rows),
        "coverage_rate": float(matched_rows / row_count) if row_count else 0.0,
    }


def _missing_field_stats(candidate: pd.DataFrame) -> dict[str, int]:
    fields = [
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "volume",
        "amount",
        "limit_up",
        "limit_down",
        "adj_factor",
        "trading_day_confirmed",
    ]
    return {field: int(candidate[field].isna().sum()) for field in fields if field in candidate.columns}


def _readiness_report(candidate: pd.DataFrame, coverage: dict[str, Any], missing_field_stats: dict[str, int]) -> dict[str, Any]:
    reasons = [
        "is_paused=false is unresolved because suspend_d misses cannot imply false",
        "suspension_status coverage audit is required",
        "source layer is smoke candidate dry-run, not standard daily_price",
    ]
    if coverage["limit_price_fields"]["all_fields"]["missing_rows"]:
        reasons.append("limit_up/limit_down coverage is incomplete")
    if coverage["adj_factor"]["missing_rows"]:
        reasons.append("adj_factor coverage is incomplete")
    if not coverage["trade_cal"]["confirmed_open_trading_day"]:
        reasons.append("trade_cal did not confirm an open trading day")
    promotion = can_promote_daily_price_candidate_to_standard(
        source_layer="smoke",
        fields=set(candidate.columns),
        stk_limit_fields_complete=coverage["limit_price_fields"]["all_fields"]["missing_rows"] == 0,
        trade_cal_valid=coverage["trade_cal"]["confirmed_open_trading_day"],
        suspension_status_coverage_audited=False,
        is_paused_boolean=False,
        validator_passed=False,
        dq_level=DataQualityLevel.DQ1,
    )
    return {
        "ready_for_dq3_promotion": False,
        "status": "BLOCKED_BY_UNRESOLVED_IS_PAUSED",
        "reasons": reasons,
        "required_future_gates": list(promotion.required_future_gates),
        "missing_field_stats_considered": missing_field_stats,
    }


def _pause_status_counts(candidate: pd.DataFrame) -> dict[str, int]:
    counts = candidate["pause_status"].value_counts(dropna=False).to_dict()
    return {
        "true_candidate": int(counts.get("true_candidate", 0)),
        "unknown": int(counts.get("unknown", 0)),
        "false": 0,
    }


def _duplicate_key_checks(normalized: dict[str, pd.DataFrame]) -> dict[str, dict[str, Any]]:
    checks = {}
    for dataset in ("daily", "stk_limit", "adj_factor", "suspend_d"):
        frame = normalized[dataset]
        duplicate_rows = int(frame.duplicated(JOIN_KEYS, keep=False).sum()) if set(JOIN_KEYS).issubset(frame.columns) else 0
        checks[dataset] = {"key": JOIN_KEYS, "duplicate_rows": duplicate_rows}
    trade_cal = normalized["trade_cal"]
    checks["trade_cal"] = {
        "key": ["trade_date", "exchange"],
        "duplicate_rows": int(trade_cal.duplicated(["trade_date", "exchange"], keep=False).sum()) if {"trade_date", "exchange"}.issubset(trade_cal.columns) else 0,
    }
    return checks


def _trade_date_consistency(frames: dict[str, pd.DataFrame], trade_date: str) -> dict[str, dict[str, Any]]:
    result = {}
    for dataset, frame in frames.items():
        column = _pick_column(frame, FIELD_ALIASES[dataset]["trade_date"])
        normalized_dates = frame[column].map(_normalize_tushare_date) if column is not None else pd.Series([], dtype="object")
        result[dataset] = {
            "requested_trade_date": trade_date,
            "date_column": column,
            "mismatched_rows": int((normalized_dates != trade_date).sum()),
            "unique_dates": sorted({value for value in normalized_dates.dropna().astype(str)}),
        }
    return result


def _drop_duplicate_keys(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.drop_duplicates(JOIN_KEYS, keep="first").copy()


def _pick_column(frame: pd.DataFrame, aliases: tuple[str, ...]) -> str | None:
    columns = set(frame.columns)
    for alias in aliases:
        if alias in columns:
            return alias
    return None


def _normalize_tushare_date(value) -> str | None:
    if pd.isna(value):
        return None
    text = str(value).strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[0:4]}-{text[4:6]}-{text[6:8]}"
    try:
        return validate_trade_date(text)
    except Exception:
        return text


def _records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    sanitized = frame.astype(object).where(pd.notna(frame), None)
    records = sanitized.to_dict(orient="records")
    return [{key: _json_scalar(value) for key, value in row.items()} for row in records]


def _json_scalar(value):
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


def _contract_to_report(contract) -> dict[str, Any]:
    data = asdict(contract)
    if "provider_name" in data:
        data["provider"] = data.pop("provider_name")
    data["dq_level"] = data["dq_level"].value
    return data

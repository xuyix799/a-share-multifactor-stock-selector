from __future__ import annotations

from collections.abc import Callable, Iterator
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, timedelta
import math
import re
from typing import Any

import numpy as np
import pandas as pd

from stock_selector.data.data_validator import DataValidationError, validate_dataset_frame
from stock_selector.data.historical_backfill import (
    FAILURE_CATEGORIES,
    build_history_backfill_output_keys,
    build_history_backfill_plan,
    classify_backfill_failure,
)
from stock_selector.data.real_clean_inputs_landing import KEY_COLUMNS
from stock_selector.providers.base import ProviderConfigurationError
from stock_selector.providers.schema_contract import REQUIRED_BENCHMARK_INDEXES, get_schema_contract
from stock_selector.providers.schema_mapper import SchemaMappingError, normalize_date, normalize_stock_code


RawFetchFn = Callable[[str, dict[str, Any]], pd.DataFrame]
TradingCalendarFn = Callable[[str, str], pd.DataFrame]
FetchChunkFn = Callable[[dict[str, Any]], "HistoricalChunkFetchResult"]

_RESULT_STATUSES = {"FETCHED", "VALID_EMPTY", "BLOCKED", "FAILED"}
_RETRYABLE_FAILURES = {
    "RATE_LIMITED",
    "TRANSIENT_PROVIDER_ERROR",
    "WRITE_FAILED",
    "READBACK_FAILED",
    "UNKNOWN",
}
_PARTITION_STRATEGIES = {
    "stock_basic": "BY_TRADE_DATE_COLUMN",
    "daily_price": "BY_TRADE_DATE_COLUMN",
    "adj_factor": "BY_TRADE_DATE_COLUMN",
    "daily_basic": "BY_TRADE_DATE_COLUMN",
    "financial": "FINANCIAL_ANNOUNCE_DATE_AS_OF",
    "st_history": "ST_INTERVAL_HISTORY",
    "benchmark_price": "BY_TRADE_DATE_COLUMN",
}
_SOURCE_SEMANTICS = {
    "stock_basic": "POINT_IN_TIME_HISTORICAL_SNAPSHOT",
    "daily_price": "HISTORICAL_DAILY_PRICE_SOURCE",
    "adj_factor": "HISTORICAL_RANGE_SOURCE",
    "daily_basic": "HISTORICAL_RANGE_SOURCE",
    "financial": "TRUSTED_STANDARD_FINANCIAL_SOURCE",
    "st_history": "HISTORICAL_INTERVAL_SOURCE",
    "benchmark_price": "HISTORICAL_INDEX_RANGE_SOURCE",
}
_SENSITIVE_KEY = re.compile(r"(?i)(?:token|secret|password|authorization|credential|api[_-]?key)")
_DRIVE_PREFIX = re.compile(r"^[A-Za-z]:")
_AUTH_SCHEME = re.compile(
    r"(?i)\b(?:bearer|basic|apikey|token)\s+(?:\"[^\"]*\"|'[^']*'|[^\s,;&}\]]+)"
)


class HistoricalProviderError(RuntimeError):
    """A deliberate provider-boundary failure with the Goal 21 taxonomy."""

    def __init__(self, failure_category: str, message: str) -> None:
        if failure_category not in FAILURE_CATEGORIES:
            raise ValueError(f"unsupported historical provider failure category: {failure_category}")
        super().__init__(classify_backfill_failure(RuntimeError(str(message)))["message"])
        self.failure_category = failure_category


@dataclass(frozen=True)
class HistoricalChunkFetchResult:
    dataset: str
    chunk_id: str
    frame: pd.DataFrame
    provider_status: str
    provider_name: str
    source_keys: tuple[str, ...]
    source_semantics: str | None
    provider_calls: tuple[dict[str, Any], ...]
    actual_schema: tuple[str, ...]
    target_schema: tuple[str, ...]
    dq: dict[str, Any]
    coverage: dict[str, Any]
    canonical_trade_dates: tuple[str, ...]
    partition_strategy: str
    valid_empty: bool
    validation: dict[str, Any]
    failure: dict[str, Any] | None

    def __post_init__(self) -> None:
        try:
            target = tuple(get_schema_contract(self.dataset).columns)
        except Exception as exc:
            raise ValueError(f"unsupported historical dataset: {self.dataset}") from exc
        if self.provider_status not in _RESULT_STATUSES:
            raise ValueError(f"invalid historical provider status: {self.provider_status}")
        if tuple(self.target_schema) != target:
            raise ValueError("target_schema must equal the standard schema contract")
        if not isinstance(self.frame, pd.DataFrame):
            raise ValueError("frame must be a pandas DataFrame")

        frame = self.frame.copy(deep=True)
        frame.attrs = deepcopy(self.frame.attrs)
        if not isinstance(self.chunk_id, str) or not self.chunk_id:
            raise ValueError("chunk_id must be a non-empty string")
        if not isinstance(self.provider_name, str) or not self.provider_name:
            raise ValueError("provider_name must be a non-empty string")
        if self.source_semantics is not None and not isinstance(self.source_semantics, str):
            raise ValueError("source_semantics must be a string or None")
        if any(not isinstance(value, str) for value in self.source_keys):
            raise ValueError("source_keys must be strings")
        if any(not isinstance(value, str) for value in self.canonical_trade_dates):
            raise ValueError("canonical_trade_dates must contain strings")

        source_keys = tuple(self.source_keys)
        if any(not _safe_source_key(value) for value in source_keys):
            raise ValueError("source_keys must contain safe upstream logical object keys")
        provider_calls = tuple(_sanitize_provider_call(value) for value in self.provider_calls)
        dq = _sanitize_result_metadata(self.dq)
        coverage = _sanitize_result_metadata(self.coverage)
        validation = _sanitize_result_metadata(self.validation)
        failure = _sanitize_result_metadata(self.failure)
        actual_schema = tuple(_sanitize_schema_label(value) for value in self.actual_schema)
        canonical_dates = tuple(self.canonical_trade_dates)

        object.__setattr__(self, "frame", frame)
        object.__setattr__(self, "source_keys", source_keys)
        object.__setattr__(self, "provider_calls", provider_calls)
        object.__setattr__(self, "actual_schema", actual_schema)
        object.__setattr__(self, "target_schema", target)
        object.__setattr__(self, "dq", dq)
        object.__setattr__(self, "coverage", coverage)
        object.__setattr__(self, "canonical_trade_dates", canonical_dates)
        object.__setattr__(self, "validation", validation)
        object.__setattr__(self, "failure", failure)

        if list(frame.columns) != list(target):
            raise ValueError("result frame must contain only the ordered standard columns")
        if self.partition_strategy != _PARTITION_STRATEGIES[self.dataset]:
            raise ValueError("partition_strategy does not match dataset")
        if not isinstance(dq, dict) or not isinstance(coverage, dict) or not isinstance(validation, dict):
            raise ValueError("dq, coverage and validation must be dictionaries")
        if type(dq.get("passed")) is not bool or type(validation.get("passed")) is not bool:
            raise ValueError("dq and validation must expose explicit passed booleans")
        if type(coverage.get("complete")) is not bool or type(coverage.get("valid_empty")) is not bool:
            raise ValueError("coverage must expose an explicit complete boolean")
        if type(self.valid_empty) is not bool:
            raise ValueError("valid_empty must be an explicit boolean")
        _validate_result_evidence(
            frame=frame,
            dq=dq,
            coverage=coverage,
            validation=validation,
            canonical_dates=canonical_dates,
            partition_strategy=self.partition_strategy,
        )

        if self.provider_status == "FETCHED":
            if frame.empty or self.valid_empty or failure is not None:
                raise ValueError("FETCHED requires non-empty evidence and no failure")
            if dq.get("passed") is not True or validation.get("passed") is not True:
                raise ValueError("FETCHED requires successful DQ and validation")
            if coverage.get("complete") is not True or coverage.get("valid_empty") is True:
                raise ValueError("FETCHED requires complete non-empty coverage")
        elif self.provider_status == "VALID_EMPTY":
            if self.dataset not in {"stock_basic", "st_history"} or not frame.empty or not self.valid_empty:
                raise ValueError("VALID_EMPTY is limited to proven empty historical membership or ST evidence")
            required_semantics = _SOURCE_SEMANTICS[self.dataset]
            if self.source_semantics != required_semantics or not source_keys:
                raise ValueError("VALID_EMPTY requires trusted historical lineage")
            if failure is not None:
                raise ValueError("VALID_EMPTY cannot carry a failure")
            if dq.get("passed") is not True or validation.get("passed") is not True:
                raise ValueError("VALID_EMPTY requires successful DQ and validation")
            if coverage.get("complete") is not True or coverage.get("valid_empty") is not True:
                raise ValueError("VALID_EMPTY requires complete explicit empty coverage")
        else:
            if not frame.empty or self.valid_empty or validation.get("passed") is not False:
                raise ValueError("failed provider states require a safe empty frame and failed validation")
            if dq.get("passed") is not False or dq.get("level") != "BLOCKED" or not dq.get("blocked_reasons"):
                raise ValueError("failed provider states require explicit blocked DQ evidence")
            if coverage.get("complete") is not False or coverage.get("valid_empty") is not False:
                raise ValueError("failed provider states cannot claim complete or valid-empty coverage")
            if not validation.get("errors"):
                raise ValueError("failed provider states require validation errors")
            if not _canonical_failure_record(failure):
                raise ValueError("failed provider states require a canonical failure record")
            expected_retryable = self.provider_status == "FAILED"
            if failure["retryable"] is not expected_retryable:
                raise ValueError("provider status and failure retryability disagree")


def _validated_plan_copy(plan: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(plan, dict):
        raise HistoricalProviderError("CONFIGURATION_ERROR", "invalid Goal 21 plan")
    copied = deepcopy(plan)
    try:
        scope = copied["scope"]
        limits = copied["limits"]
        if not isinstance(scope, dict) or not isinstance(limits, dict):
            raise ValueError("plan scope and limits must be dictionaries")
        universe_source = scope.get("universe_source")
        common = {
            "run_id": copied["run_id"],
            "start_date": scope["start_date"],
            "end_date": scope["end_date"],
            "code_batch_size": limits["code_batch_size"],
            "date_batch_days": limits["date_batch_days"],
            "report_period_months": limits["report_period_months"],
            "datasets": copied["datasets"],
            "generated_at_fn": lambda: copied["generated_at"],
        }
        if universe_source == "codes":
            rebuilt = build_history_backfill_plan(codes=scope["codes"], **common)
        elif universe_source == "universe_frame":
            rebuilt = build_history_backfill_plan(
                universe_frame=pd.DataFrame({"stock_code": scope["codes"]}),
                universe_key=scope.get("universe_key"),
                **common,
            )
        else:
            raise ValueError("unsupported universe_source")
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as exc:
        raise HistoricalProviderError("CONFIGURATION_ERROR", f"invalid Goal 21 plan: {exc}") from exc
    if copied != rebuilt:
        raise HistoricalProviderError(
            "CONFIGURATION_ERROR",
            "Goal 21 plan identity, fingerprint, scope, limits, datasets, or chunks are inconsistent",
        )
    return copied


class HistoricalProviderRouter:
    """Side-effect-free router from immutable plan chunks to normalized evidence."""

    def __init__(
        self,
        *,
        plan: dict[str, Any],
        provider_name: str,
        raw_fetch_fn: RawFetchFn | None,
        trading_calendar_fn: TradingCalendarFn | None,
    ) -> None:
        copied_plan = _validated_plan_copy(plan)
        build_history_backfill_output_keys(copied_plan.get("run_id", ""), copied_plan["chunks"])
        chunks = copied_plan["chunks"]
        chunk_ids = [chunk.get("chunk_id") for chunk in chunks]
        if len(chunk_ids) != len(set(chunk_ids)):
            raise HistoricalProviderError("CONFIGURATION_ERROR", "duplicate chunk_id in plan")
        self._plan = copied_plan
        self._chunks = {chunk["chunk_id"]: deepcopy(chunk) for chunk in chunks}
        self._provider_name = str(provider_name).strip().lower()
        self._raw_fetch_fn = raw_fetch_fn
        self._trading_calendar_fn = trading_calendar_fn
        self._calendar_open_dates: tuple[str, ...] | None = None

    def fetch_chunk(self, chunk: dict[str, Any]) -> HistoricalChunkFetchResult:
        candidate = deepcopy(chunk)
        planned = self._chunks.get(candidate.get("chunk_id")) if isinstance(candidate, dict) else None
        if planned is None or candidate != planned:
            raise HistoricalProviderError("CONFIGURATION_ERROR", "foreign or tampered Goal 21 chunk")

        dataset = planned["dataset"]
        capability_error = _capability_error(self._provider_name, dataset)
        if capability_error is not None:
            return self._failure_result(planned, HistoricalProviderError(*capability_error))

        try:
            open_dates = self._calendar()
            canonical_dates = _canonical_dates(planned, open_dates, dataset)
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as exc:
            return self._failure_result(planned, exc)

        try:
            if self._raw_fetch_fn is None:
                raise HistoricalProviderError("CONFIGURATION_ERROR", "raw_fetch_fn is required")
            if self._provider_name == "tushare" and dataset == "daily_price":
                normalized, raw_schema, attrs, calls = self._fetch_tushare_daily_price(planned, canonical_dates)
            else:
                raw, calls = self._fetch_simple(planned, canonical_dates)
                raw_schema = tuple(raw.columns)
                attrs = deepcopy(raw.attrs)
                if dataset == "stock_basic" and "list_status" in raw.columns:
                    raise _ObservedProviderError(
                        HistoricalProviderError(
                            "SEMANTIC_SOURCE_UNAVAILABLE",
                            "current list_status snapshot cannot be historical",
                        ),
                        actual_schema=raw_schema,
                        provider_calls=calls,
                    )
                if dataset == "stock_basic" and "snapshot_date" in raw.columns:
                    try:
                        snapshot_dates = raw["snapshot_date"].map(normalize_date)
                        trade_dates = raw["trade_date"].map(normalize_date)
                    except Exception as exc:
                        raise _ObservedProviderError(
                            HistoricalProviderError("SCHEMA_DRIFT", f"invalid stock snapshot date: {exc}"),
                            actual_schema=raw_schema,
                            provider_calls=calls,
                        ) from exc
                    if not snapshot_dates.equals(trade_dates):
                        raise _ObservedProviderError(
                            HistoricalProviderError(
                                "SEMANTIC_SOURCE_UNAVAILABLE",
                                "stock snapshot date does not equal trade_date",
                            ),
                            actual_schema=raw_schema,
                            provider_calls=calls,
                        )
                if dataset == "benchmark_price" and "pre_close" not in raw.columns:
                    raise _ObservedProviderError(
                        HistoricalProviderError(
                            "DQ_FAILED",
                            "benchmark previous-close evidence is required",
                        ),
                        actual_schema=raw_schema,
                        provider_calls=calls,
                    )
                if dataset == "benchmark_price":
                    previous_close = pd.to_numeric(raw["pre_close"], errors="coerce")
                    if (
                        previous_close.isna().any()
                        or not np.isfinite(previous_close.to_numpy(dtype=float)).all()
                        or (previous_close <= 0).any()
                    ):
                        raise _ObservedProviderError(
                            HistoricalProviderError(
                                "DQ_FAILED",
                                "benchmark previous-close evidence is invalid",
                            ),
                            actual_schema=raw_schema,
                            provider_calls=calls,
                        )
                    missing_change_fields = [column for column in ("close", "pct_chg") if column not in raw.columns]
                    if missing_change_fields:
                        raise _ObservedProviderError(
                            HistoricalProviderError(
                                "SCHEMA_DRIFT",
                                f"missing provider fields: {', '.join(missing_change_fields)}",
                            ),
                            actual_schema=raw_schema,
                            provider_calls=calls,
                        )
                    close = pd.to_numeric(raw["close"], errors="coerce")
                    pct_chg = pd.to_numeric(raw["pct_chg"], errors="coerce")
                    expected_pct_chg = ((close / previous_close) - 1.0) * 100.0
                    if (
                        close.isna().any()
                        or pct_chg.isna().any()
                        or not np.isfinite(close.to_numpy(dtype=float)).all()
                        or not np.isfinite(pct_chg.to_numpy(dtype=float)).all()
                        or not np.isclose(
                            pct_chg.to_numpy(dtype=float),
                            expected_pct_chg.to_numpy(dtype=float),
                            rtol=1e-6,
                            atol=1e-4,
                        ).all()
                    ):
                        raise _ObservedProviderError(
                            HistoricalProviderError(
                                "DQ_FAILED",
                                "benchmark pct_chg is inconsistent with close and pre_close",
                            ),
                            actual_schema=raw_schema,
                            provider_calls=calls,
                        )
                try:
                    normalized = _normalize_dataset(dataset, raw)
                except _ObservedProviderError as exc:
                    raise _ObservedProviderError(
                        exc.error,
                        actual_schema=exc.actual_schema or raw_schema,
                        provider_calls=calls,
                    ) from exc
                except (KeyboardInterrupt, SystemExit):
                    raise
                except Exception as exc:
                    raise _ObservedProviderError(
                        exc,
                        actual_schema=raw_schema,
                        provider_calls=calls,
                    ) from exc
            try:
                return self._success_result(
                    planned,
                    normalized,
                    raw_schema=raw_schema,
                    attrs=attrs,
                    provider_calls=calls,
                    canonical_dates=canonical_dates,
                )
            except (KeyboardInterrupt, SystemExit):
                raise
            except _ObservedProviderError:
                raise
            except Exception as exc:
                raise _ObservedProviderError(
                    exc,
                    actual_schema=raw_schema,
                    provider_calls=calls,
                ) from exc
        except (KeyboardInterrupt, SystemExit):
            raise
        except _ObservedProviderError as exc:
            return self._failure_result(
                planned,
                exc.error,
                actual_schema=exc.actual_schema,
                provider_calls=exc.provider_calls,
                canonical_dates=canonical_dates,
            )
        except Exception as exc:
            return self._failure_result(planned, exc, canonical_dates=canonical_dates)

    def _calendar(self) -> tuple[str, ...]:
        if self._calendar_open_dates is not None:
            return tuple(self._calendar_open_dates)
        if self._trading_calendar_fn is None:
            raise HistoricalProviderError("CONFIGURATION_ERROR", "trading_calendar_fn is required")
        scope = self._plan.get("scope", {})
        start_date = scope.get("start_date")
        end_date = scope.get("end_date")
        if not isinstance(start_date, str) or not isinstance(end_date, str):
            raise HistoricalProviderError("CONFIGURATION_ERROR", "plan scope is missing dates")
        raw = self._trading_calendar_fn(start_date, end_date)
        if not isinstance(raw, pd.DataFrame) or raw.empty:
            raise HistoricalProviderError("SEMANTIC_SOURCE_UNAVAILABLE", "trading calendar is empty")
        frame = raw.copy(deep=True)
        date_columns = [column for column in ("trade_date", "cal_date") if column in frame.columns]
        if len(date_columns) != 1 or "is_open" not in frame.columns:
            raise HistoricalProviderError("SCHEMA_DRIFT", "trading calendar requires one date column and is_open")
        date_column = date_columns[0]
        try:
            frame["_date"] = frame[date_column].map(normalize_date)
        except Exception as exc:
            raise HistoricalProviderError("DQ_FAILED", f"invalid trading calendar date: {exc}") from exc
        if frame["_date"].duplicated().any():
            raise HistoricalProviderError("DQ_FAILED", "duplicate trading calendar date")
        normalized_open: list[bool] = []
        for value in frame["is_open"].tolist():
            if isinstance(value, (bool, np.bool_)):
                normalized_open.append(bool(value))
            elif isinstance(value, (int, np.integer)) and int(value) in {0, 1}:
                normalized_open.append(bool(value))
            else:
                raise HistoricalProviderError("DQ_FAILED", "invalid trading calendar is_open value")
        frame["_is_open"] = normalized_open
        expected = list(_iter_dates(start_date, end_date))
        observed = frame["_date"].tolist()
        if any(value < start_date or value > end_date for value in observed):
            raise HistoricalProviderError("DQ_FAILED", "trading calendar contains out-of-scope dates")
        if set(observed) != set(expected):
            raise HistoricalProviderError(
                "SEMANTIC_SOURCE_UNAVAILABLE",
                "trading calendar cannot prove the full requested range",
            )
        self._calendar_open_dates = tuple(sorted(frame.loc[frame["_is_open"], "_date"].tolist()))
        return tuple(self._calendar_open_dates)

    def _fetch_simple(
        self,
        chunk: dict[str, Any],
        canonical_dates: tuple[str, ...],
    ) -> tuple[pd.DataFrame, tuple[dict[str, Any], ...]]:
        dataset = chunk["dataset"]
        calls: list[dict[str, Any]] = []
        if self._provider_name in {"fixture", "mock"}:
            parameters = _simple_parameters(chunk)
            raw = _call_raw(self._raw_fetch_fn, dataset, parameters, "fixture_historical_chunk", calls)
            return raw, tuple(calls)
        if self._provider_name == "tushare":
            frames: list[pd.DataFrame] = []
            fields = {
                "adj_factor": "ts_code,trade_date,adj_factor",
                "daily_basic": "ts_code,trade_date,pe_ttm,pb,ps_ttm,total_mv,circ_mv,turnover_rate",
            }.get(dataset)
            for code in chunk.get("codes", []):
                parameters = {
                    "ts_code": code,
                    "start_date": chunk["start_date"].replace("-", ""),
                    "end_date": chunk["end_date"].replace("-", ""),
                }
                if fields is not None:
                    parameters["fields"] = fields
                endpoint = dataset
                frames.append(_call_raw(self._raw_fetch_fn, endpoint, parameters, "by_code_date_window", calls))
            return _concat_frames(frames), tuple(calls)
        if self._provider_name == "akshare" and dataset == "benchmark_price":
            frames = []
            for index_code in chunk.get("index_codes", []):
                parameters = {
                    "index_code": index_code,
                    "start_date": chunk["start_date"],
                    "end_date": chunk["end_date"],
                }
                frames.append(_call_raw(self._raw_fetch_fn, dataset, parameters, "by_index_date_window", calls))
            return _concat_frames(frames), tuple(calls)
        raise HistoricalProviderError("CONFIGURATION_ERROR", "unsupported provider route")

    def _fetch_tushare_daily_price(
        self,
        chunk: dict[str, Any],
        canonical_dates: tuple[str, ...],
    ) -> tuple[pd.DataFrame, tuple[str, ...], dict[str, Any], tuple[dict[str, Any], ...]]:
        calls: list[dict[str, Any]] = []
        schema_holder: list[str] = []
        try:
            return self._fetch_tushare_daily_price_impl(
                chunk,
                canonical_dates,
                calls,
                lambda value: schema_holder.extend(value),
            )
        except (KeyboardInterrupt, SystemExit):
            raise
        except _ObservedProviderError as exc:
            raise _ObservedProviderError(
                exc.error,
                actual_schema=exc.actual_schema or tuple(schema_holder),
                provider_calls=exc.provider_calls or tuple(calls),
            ) from exc
        except Exception as exc:
            if isinstance(exc, (SchemaMappingError, ValueError, TypeError)):
                exc = HistoricalProviderError("SCHEMA_DRIFT", str(exc))
            raise _ObservedProviderError(
                exc,
                actual_schema=tuple(schema_holder),
                provider_calls=tuple(calls),
            ) from exc

    def _fetch_tushare_daily_price_impl(
        self,
        chunk: dict[str, Any],
        canonical_dates: tuple[str, ...],
        calls: list[dict[str, Any]],
        observe_schema: Callable[[tuple[str, ...]], None],
    ) -> tuple[pd.DataFrame, tuple[str, ...], dict[str, Any], tuple[dict[str, Any], ...]]:
        daily_frames = []
        for code in chunk.get("codes", []):
            parameters = {
                "start_date": chunk["start_date"].replace("-", ""),
                "end_date": chunk["end_date"].replace("-", ""),
                "fields": "ts_code,trade_date,open,high,low,close,pre_close,vol,amount",
                "ts_code": code,
            }
            daily_frames.append(_call_raw(self._raw_fetch_fn, "daily", parameters, "by_code_date_window", calls))
        daily = _concat_frames(daily_frames)
        raw_schema = tuple(daily.columns)
        observe_schema(raw_schema)
        limit_frames = []
        suspend_frames = []
        for trade_date in canonical_dates:
            compact = trade_date.replace("-", "")
            limit = _call_raw(
                self._raw_fetch_fn,
                "stk_limit",
                {
                    "trade_date": compact,
                    "fields": "ts_code,trade_date,up_limit,down_limit",
                },
                "by_open_trade_date",
                calls,
            )
            if not limit.empty:
                if "trade_date" not in limit.columns:
                    raise HistoricalProviderError("SCHEMA_DRIFT", "stk_limit schema is incomplete")
                try:
                    returned_dates = set(limit["trade_date"].map(normalize_date))
                except Exception as exc:
                    raise HistoricalProviderError("SCHEMA_DRIFT", f"invalid stk_limit trade_date: {exc}") from exc
                if returned_dates != {trade_date}:
                    raise HistoricalProviderError(
                        "DQ_FAILED",
                        f"stk_limit returned rows outside requested trade date {trade_date}",
                    )
            limit_frames.append(limit)
        for trade_date in canonical_dates:
            compact = trade_date.replace("-", "")
            suspend = _call_raw(
                self._raw_fetch_fn,
                "suspend_d",
                {
                    "trade_date": compact,
                    "fields": "ts_code,trade_date,suspend_timing,suspend_type",
                },
                "full_market_event_set_by_trade_date",
                calls,
                allow_none=True,
            )
            if suspend is None:
                raise HistoricalProviderError(
                    "SEMANTIC_SOURCE_UNAVAILABLE",
                    f"suspend_d coverage is missing for {trade_date}",
                )
            attrs = suspend.attrs
            proof = (
                attrs.get("full_market_event_set") is True
                and attrs.get("coverage_complete") is True
                and attrs.get("sample_truncated") is False
                and attrs.get("empty_after_retries") is False
                and compact in [str(value).replace("-", "") for value in attrs.get("covered_trade_dates", [])]
            )
            if not proof:
                raise HistoricalProviderError(
                    "SEMANTIC_SOURCE_UNAVAILABLE",
                    f"suspend_d full-market coverage is not proven for {trade_date}",
                )
            if not suspend.empty:
                if "trade_date" not in suspend.columns:
                    raise HistoricalProviderError("SCHEMA_DRIFT", "suspend_d schema is incomplete")
                try:
                    returned_dates = set(suspend["trade_date"].map(normalize_date))
                except Exception as exc:
                    raise HistoricalProviderError("SCHEMA_DRIFT", f"invalid suspend_d trade_date: {exc}") from exc
                if returned_dates != {trade_date}:
                    raise HistoricalProviderError(
                        "DQ_FAILED",
                        f"suspend_d returned rows outside requested trade date {trade_date}",
                    )
            suspend_frames.append(suspend)
        limits = _concat_frames(limit_frames)
        suspends = _concat_frames(suspend_frames)
        normalized_daily = _normalize_dataset("daily_price", daily, partial_daily=True)
        normalized_limits = _normalize_limit_frame(limits)
        normalized_suspends = _normalize_suspend_frame(suspends)
        codes = set(chunk.get("codes", []))
        normalized_limits = normalized_limits[normalized_limits["stock_code"].isin(codes)].copy()
        normalized_suspends = normalized_suspends[normalized_suspends["stock_code"].isin(codes)].copy()
        merged = normalized_daily.merge(
            normalized_limits,
            on=["stock_code", "trade_date"],
            how="left",
            validate="one_to_one",
        )
        suspend_keys = set(
            map(tuple, normalized_suspends[["stock_code", "trade_date"]].drop_duplicates().itertuples(index=False, name=None))
        )
        merged["is_paused"] = [
            (row.stock_code, row.trade_date) in suspend_keys
            for row in merged[["stock_code", "trade_date"]].itertuples(index=False)
        ]
        merged["pct_chg"] = ((merged["close"] / merged["pre_close"]) - 1.0) * 100.0
        target = get_schema_contract("daily_price").columns
        return merged[target], raw_schema, {}, tuple(calls)

    def _success_result(
        self,
        chunk: dict[str, Any],
        frame: pd.DataFrame,
        *,
        raw_schema: tuple[str, ...],
        attrs: dict[str, Any],
        provider_calls: tuple[dict[str, Any], ...],
        canonical_dates: tuple[str, ...],
    ) -> HistoricalChunkFetchResult:
        dataset = chunk["dataset"]
        target = tuple(get_schema_contract(dataset).columns)
        source_keys, source_semantics = _lineage(dataset, attrs, self._provider_name)
        if source_keys:
            _validate_fixture_scope_proof(dataset, attrs, chunk)
        coverage, dq, validation, status, valid_empty = _validate_normalized_result(
            dataset,
            frame,
            chunk,
            canonical_dates,
            attrs,
            source_keys,
            source_semantics,
        )
        ordered = frame.loc[:, list(target)].copy(deep=True)
        ordered = ordered.sort_values(KEY_COLUMNS[dataset], kind="mergesort").reset_index(drop=True)
        return HistoricalChunkFetchResult(
            dataset=dataset,
            chunk_id=chunk["chunk_id"],
            frame=ordered,
            provider_status=status,
            provider_name=self._provider_name,
            source_keys=source_keys,
            source_semantics=source_semantics,
            provider_calls=provider_calls,
            actual_schema=raw_schema,
            target_schema=target,
            dq=dq,
            coverage=coverage,
            canonical_trade_dates=canonical_dates,
            partition_strategy=_PARTITION_STRATEGIES[dataset],
            valid_empty=valid_empty,
            validation=validation,
            failure=None,
        )

    def _failure_result(
        self,
        chunk: dict[str, Any],
        error: BaseException,
        *,
        actual_schema: tuple[str, ...] = (),
        provider_calls: tuple[dict[str, Any], ...] = (),
        canonical_dates: tuple[str, ...] = (),
    ) -> HistoricalChunkFetchResult:
        dataset = chunk["dataset"]
        failure = classify_backfill_failure(error)
        if isinstance(error, ProviderConfigurationError):
            failure.update(category="CONFIGURATION_ERROR", retryable=False)
        status = "FAILED" if failure["retryable"] else "BLOCKED"
        target = tuple(get_schema_contract(dataset).columns)
        coverage = _coverage_base(chunk, canonical_dates, expected_count=0, covered_count=0)
        coverage["complete"] = False
        return HistoricalChunkFetchResult(
            dataset=dataset,
            chunk_id=chunk["chunk_id"],
            frame=pd.DataFrame(columns=target),
            provider_status=status,
            provider_name=self._provider_name,
            source_keys=(),
            source_semantics=None,
            provider_calls=provider_calls,
            actual_schema=actual_schema,
            target_schema=target,
            dq={"passed": False, "level": "BLOCKED", "blocked_reasons": [failure["category"]]},
            coverage=coverage,
            canonical_trade_dates=canonical_dates,
            partition_strategy=_PARTITION_STRATEGIES[dataset],
            valid_empty=False,
            validation={"passed": False, "errors": [failure["message"]]},
            failure=failure,
        )


def iter_historical_canonical_partitions(
    result: HistoricalChunkFetchResult,
) -> Iterator[tuple[str, pd.DataFrame]]:
    target = list(result.target_schema)
    for trade_date in result.canonical_trade_dates:
        if result.partition_strategy == "BY_TRADE_DATE_COLUMN":
            projected = result.frame[result.frame["trade_date"].astype(str) == trade_date]
        elif result.partition_strategy == "FINANCIAL_ANNOUNCE_DATE_AS_OF":
            projected = result.frame[
                (result.frame["report_period"].astype(str) <= trade_date)
                & (result.frame["announce_date"].astype(str) <= trade_date)
            ]
        elif result.partition_strategy == "ST_INTERVAL_HISTORY":
            projected = result.frame
        else:
            raise ValueError(f"unsupported partition strategy: {result.partition_strategy}")
        yield trade_date, projected.loc[:, target].copy(deep=True).reset_index(drop=True)


@dataclass(frozen=True)
class _ObservedProviderError(Exception):
    error: BaseException
    actual_schema: tuple[str, ...] = ()
    provider_calls: tuple[dict[str, Any], ...] = ()


def _capability_error(provider_name: str, dataset: str) -> tuple[str, str] | None:
    if provider_name in {"fixture", "mock"}:
        return None
    if provider_name == "tushare":
        if dataset in {"daily_price", "adj_factor", "daily_basic"}:
            return None
        return "SEMANTIC_SOURCE_UNAVAILABLE", f"tushare historical semantics are unavailable for {dataset}"
    if provider_name == "akshare":
        if dataset == "benchmark_price":
            return None
        return "SEMANTIC_SOURCE_UNAVAILABLE", f"akshare historical semantics are unavailable for {dataset}"
    if provider_name == "baostock":
        return "SEMANTIC_SOURCE_UNAVAILABLE", f"baostock cannot produce the complete {dataset} contract"
    return "CONFIGURATION_ERROR", f"unsupported or disabled historical provider: {provider_name}"


def _canonical_dates(chunk: dict[str, Any], open_dates: tuple[str, ...], dataset: str) -> tuple[str, ...]:
    if dataset in {"financial", "st_history"}:
        return tuple(open_dates)
    return tuple(value for value in open_dates if chunk["start_date"] <= value <= chunk["end_date"])


def _call_raw(
    fetch_fn: RawFetchFn | None,
    endpoint: str,
    parameters: dict[str, Any],
    strategy: str,
    calls: list[dict[str, Any]],
    *,
    allow_none: bool = False,
) -> pd.DataFrame | None:
    if fetch_fn is None:
        raise HistoricalProviderError("CONFIGURATION_ERROR", "raw_fetch_fn is required")
    clean_parameters = deepcopy(parameters)
    try:
        raw = fetch_fn(endpoint, deepcopy(parameters))
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as exc:
        calls.append(
            {
                "endpoint": endpoint,
                "strategy": strategy,
                "parameters": clean_parameters,
                "row_count": 0,
                "status": "FAILED",
            }
        )
        raise _ObservedProviderError(exc, provider_calls=tuple(calls)) from exc
    if raw is None and allow_none:
        calls.append(
            {
                "endpoint": endpoint,
                "strategy": strategy,
                "parameters": clean_parameters,
                "row_count": 0,
                "status": "MISSING",
            }
        )
        return None
    if not isinstance(raw, pd.DataFrame):
        actual_schema = (f"NON_DATAFRAME:{type(raw).__name__}",)
        calls.append(
            {
                "endpoint": endpoint,
                "strategy": strategy,
                "parameters": clean_parameters,
                "row_count": 0,
                "status": "SCHEMA_DRIFT",
            }
        )
        raise _ObservedProviderError(
            HistoricalProviderError("SCHEMA_DRIFT", f"{endpoint} did not return a DataFrame"),
            actual_schema=actual_schema,
            provider_calls=tuple(calls),
        )
    if raw.empty and len(raw.columns) == 0 and not allow_none:
        calls.append(
            {
                "endpoint": endpoint,
                "strategy": strategy,
                "parameters": clean_parameters,
                "row_count": 0,
                "status": "VALID_EMPTY",
            }
        )
        raise _ObservedProviderError(
            HistoricalProviderError("EMPTY_RESULT", f"{endpoint} returned no rows"),
            actual_schema=(),
            provider_calls=tuple(calls),
        )
    copied = raw.copy(deep=True)
    copied.attrs = deepcopy(raw.attrs)
    calls.append(
        {
            "endpoint": endpoint,
            "strategy": strategy,
            "parameters": clean_parameters,
            "row_count": int(len(copied)),
            "status": "VALID_EMPTY" if copied.empty else "FETCHED",
        }
    )
    return copied


def _concat_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame()
    result = pd.concat(frames, ignore_index=True)
    result.attrs = _merge_frame_attrs(frames)
    return result


def _merge_frame_attrs(frames: list[pd.DataFrame]) -> dict[str, Any]:
    merged = deepcopy(frames[0].attrs)
    all_keys = set().union(*(frame.attrs.keys() for frame in frames))
    unsafe_true_flags = {"sample_truncated", "empty_after_retries"}
    complete_true_flags = {
        "coverage_complete",
        "snapshot_coverage_complete",
        "suspend_d_full_event_coverage",
        "full_market_event_set",
    }
    union_fields = {
        "source_keys",
        "requested_codes",
        "requested_indexes",
        "source_coverage_codes",
        "covered_trade_dates",
        "pause_event_keys",
        "list_delist_master",
    }
    for key in all_keys:
        if key in unsafe_true_flags:
            merged[key] = any(frame.attrs.get(key) is True for frame in frames)
            continue
        if key in complete_true_flags:
            merged[key] = all(frame.attrs.get(key) is True for frame in frames)
            continue
        if key in union_fields:
            values: list[Any] = []
            for frame in frames:
                raw = frame.attrs.get(key, [])
                if isinstance(raw, (list, tuple)):
                    for item in raw:
                        if not any(_attr_values_equal(item, existing) for existing in values):
                            values.append(deepcopy(item))
            merged[key] = values
            continue
        values = [frame.attrs[key] for frame in frames if key in frame.attrs]
        if values and all(_attr_values_equal(values[0], value) for value in values[1:]):
            merged[key] = deepcopy(values[0])
        elif values:
            merged[key] = None
    return merged


def _attr_values_equal(left: Any, right: Any) -> bool:
    try:
        result = left == right
    except Exception:
        return False
    return bool(result) if isinstance(result, (bool, np.bool_)) else False


def _simple_parameters(chunk: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "strategy": chunk["strategy"],
        "start_date": chunk["start_date"],
        "end_date": chunk["end_date"],
    }
    if chunk.get("codes") is not None:
        result["codes"] = list(chunk["codes"])
    if chunk.get("index_codes") is not None:
        result["index_codes"] = list(chunk["index_codes"])
    if "report_period_start" in chunk:
        result["report_period_start"] = chunk["report_period_start"]
        result["report_period_end"] = chunk["report_period_end"]
    return result


def _normalize_dataset(dataset: str, raw: pd.DataFrame, *, partial_daily: bool = False) -> pd.DataFrame:
    if not isinstance(raw, pd.DataFrame):
        raise HistoricalProviderError("SCHEMA_DRIFT", "provider result is not a DataFrame")
    contract = get_schema_contract(dataset)
    if not partial_daily and set(contract.columns).issubset(raw.columns):
        result = raw.loc[:, contract.columns].copy(deep=True)
        try:
            if "stock_code" in result:
                result["stock_code"] = result["stock_code"].map(normalize_stock_code)
            if "index_code" in result:
                result["index_code"] = result["index_code"].map(normalize_stock_code)
            for column in contract.date_columns:
                if column in contract.nullable_columns:
                    result[column] = result[column].map(_nullable_date)
                else:
                    result[column] = result[column].map(normalize_date)
            for column in contract.bool_columns:
                result[column] = result[column].map(_bool_value)
        except (ValueError, TypeError, SchemaMappingError) as exc:
            raise HistoricalProviderError("SCHEMA_DRIFT", str(exc)) from exc
        for column in contract.numeric_columns:
            try:
                result[column] = pd.to_numeric(result[column], errors="raise")
            except (ValueError, TypeError) as exc:
                raise HistoricalProviderError(
                    "DQ_FAILED",
                    f"invalid numeric value: {dataset}.{column}",
                ) from exc
        return result

    mapping = {
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
        "st_history": {
            "stock_code": "ts_code",
            "st_type": "st_type",
            "start_date": "start_date",
            "end_date": "end_date",
            "source": "source",
        },
        "benchmark_price": {
            "index_code": "index_code",
            "trade_date": "trade_date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "pct_chg": "pct_chg",
        },
    }[dataset]
    if dataset == "daily_price" and not partial_daily:
        mapping = {
            **mapping,
            "pct_chg": "pct_chg",
            "is_paused": "is_paused",
            "limit_up": "up_limit" if "up_limit" in raw.columns else "limit_up",
            "limit_down": "down_limit" if "down_limit" in raw.columns else "limit_down",
        }
    missing = [source for source in mapping.values() if source not in raw.columns]
    if missing:
        raise _ObservedProviderError(
            HistoricalProviderError("SCHEMA_DRIFT", f"missing provider fields: {', '.join(missing)}"),
            actual_schema=tuple(raw.columns),
        )
    result = pd.DataFrame({target: raw[source] for target, source in mapping.items()})
    try:
        if "stock_code" in result:
            result["stock_code"] = result["stock_code"].map(normalize_stock_code)
        if "index_code" in result:
            result["index_code"] = result["index_code"].map(normalize_stock_code)
        for column in contract.date_columns:
            if column not in result:
                continue
            if column in contract.nullable_columns:
                result[column] = result[column].map(_nullable_date)
            else:
                result[column] = result[column].map(normalize_date)
        for column in contract.bool_columns:
            if column in result:
                result[column] = result[column].map(_bool_value)
    except (ValueError, TypeError, SchemaMappingError) as exc:
        raise HistoricalProviderError("SCHEMA_DRIFT", str(exc)) from exc
    for column in contract.numeric_columns:
        if column not in result:
            continue
        try:
            result[column] = pd.to_numeric(result[column], errors="raise")
        except (ValueError, TypeError) as exc:
            raise HistoricalProviderError(
                "DQ_FAILED",
                f"invalid numeric value: {dataset}.{column}",
            ) from exc
    if partial_daily:
        return result
    return result.loc[:, get_schema_contract(dataset).columns]


def _normalize_limit_frame(raw: pd.DataFrame) -> pd.DataFrame:
    required = ["ts_code", "trade_date", "up_limit", "down_limit"]
    if any(column not in raw.columns for column in required):
        raise HistoricalProviderError("SCHEMA_DRIFT", "stk_limit schema is incomplete")
    try:
        limit_up = pd.to_numeric(raw["up_limit"], errors="raise")
        limit_down = pd.to_numeric(raw["down_limit"], errors="raise")
    except (ValueError, TypeError) as exc:
        raise HistoricalProviderError("DQ_FAILED", "stk_limit contains invalid numeric values") from exc
    result = pd.DataFrame(
        {
            "stock_code": raw["ts_code"].map(normalize_stock_code),
            "trade_date": raw["trade_date"].map(normalize_date),
            "limit_up": limit_up,
            "limit_down": limit_down,
        }
    )
    if result.duplicated(["stock_code", "trade_date"]).any():
        raise HistoricalProviderError("DQ_FAILED", "duplicate stk_limit rows")
    return result


def _normalize_suspend_frame(raw: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=["stock_code", "trade_date"])
    required = ["ts_code", "trade_date"]
    if any(column not in raw.columns for column in required):
        raise HistoricalProviderError("SCHEMA_DRIFT", "suspend_d schema is incomplete")
    return pd.DataFrame(
        {
            "stock_code": raw["ts_code"].map(normalize_stock_code),
            "trade_date": raw["trade_date"].map(normalize_date),
        }
    )


def _validate_normalized_result(
    dataset: str,
    frame: pd.DataFrame,
    chunk: dict[str, Any],
    canonical_dates: tuple[str, ...],
    attrs: dict[str, Any],
    source_keys: tuple[str, ...],
    source_semantics: str | None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], str, bool]:
    codes = list(chunk.get("codes", []))
    indexes = list(chunk.get("index_codes", []))
    if dataset == "st_history" and frame.empty:
        _validate_st_empty_proof(attrs, chunk, source_keys, source_semantics)
        coverage = _coverage_base(chunk, canonical_dates, expected_count=0, covered_count=0)
        coverage.update(complete=True, valid_empty=True)
        return (
            coverage,
            {"passed": True, "level": "STRICT", "blocked_reasons": []},
            {"passed": True, "errors": []},
            "VALID_EMPTY",
            True,
        )
    if dataset == "stock_basic" and frame.empty:
        _validate_stock_semantics(frame, chunk, attrs, source_semantics)
        masters = _explicit_list_delist_master(attrs, chunk, required=True)
        not_listed, delisted = _require_entirely_nonmember_window(masters, chunk, canonical_dates)
        coverage = _coverage_base(chunk, canonical_dates, expected_count=0, covered_count=0)
        coverage.update(
            complete=True,
            valid_empty=True,
            not_yet_listed_count=not_listed,
            already_delisted_count=delisted,
        )
        return (
            coverage,
            {"passed": True, "level": "STRICT", "blocked_reasons": []},
            {"passed": True, "errors": []},
            "VALID_EMPTY",
            True,
        )
    if frame.empty:
        raise HistoricalProviderError("EMPTY_RESULT", f"{dataset} returned no rows")
    if attrs.get("sample_truncated") is True:
        raise HistoricalProviderError("DQ_FAILED", f"{dataset} source is sample truncated")
    if source_keys and attrs.get("coverage_complete") is not True:
        raise HistoricalProviderError("SEMANTIC_SOURCE_UNAVAILABLE", f"{dataset} coverage is not proven")
    if frame.duplicated(KEY_COLUMNS[dataset]).any():
        raise HistoricalProviderError("DQ_FAILED", f"duplicate {dataset} canonical keys")
    _validate_finite(dataset, frame)

    requested_start = chunk["start_date"]
    requested_end = chunk["end_date"]
    if dataset in {"stock_basic", "daily_price", "adj_factor", "daily_basic", "benchmark_price"}:
        if (~frame["trade_date"].isin(canonical_dates)).any():
            raise HistoricalProviderError("DQ_FAILED", f"{dataset} contains out-of-window rows")
    if dataset in {"daily_price", "adj_factor", "daily_basic"}:
        if dataset == "daily_price" and source_keys:
            if attrs.get("suspend_d_full_event_coverage") is not True:
                raise HistoricalProviderError(
                    "SEMANTIC_SOURCE_UNAVAILABLE",
                    "daily_price pause values lack full-market event coverage",
                )
            covered_dates = {
                str(value).replace("-", "") for value in attrs.get("covered_trade_dates", [])
            }
            if {value.replace("-", "") for value in canonical_dates} - covered_dates:
                raise HistoricalProviderError(
                    "SEMANTIC_SOURCE_UNAVAILABLE",
                    "daily_price pause event coverage is incomplete",
                )
            pause_event_keys = {
                tuple(value) for value in attrs.get("pause_event_keys", []) if isinstance(value, (list, tuple))
            }
            asserted_pauses = set(
                map(
                    tuple,
                    frame.loc[
                        frame["is_paused"].astype(bool), ["stock_code", "trade_date"]
                    ].itertuples(index=False, name=None),
                )
            )
            if asserted_pauses != pause_event_keys:
                raise HistoricalProviderError(
                    "SEMANTIC_SOURCE_UNAVAILABLE",
                    "daily_price pause values and historical event evidence do not match exactly",
                )
        expected = {(code, day) for code in codes for day in canonical_dates}
        observed = set(map(tuple, frame[["stock_code", "trade_date"]].itertuples(index=False, name=None)))
        _require_exact_axis(dataset, expected, observed)
        expected_count = len(expected)
        missing = _missing_pairs(expected - observed, "stock_code")
    elif dataset == "benchmark_price":
        if set(indexes) != set(REQUIRED_BENCHMARK_INDEXES):
            raise HistoricalProviderError("DQ_FAILED", "benchmark chunk does not contain the exact required indexes")
        observed_indexes = set(frame["index_code"].astype(str))
        if observed_indexes != set(REQUIRED_BENCHMARK_INDEXES):
            raise HistoricalProviderError("DQ_FAILED", "benchmark source must contain exactly three required indexes")
        expected = {(index_code, day) for index_code in sorted(REQUIRED_BENCHMARK_INDEXES) for day in canonical_dates}
        observed = set(map(tuple, frame[["index_code", "trade_date"]].itertuples(index=False, name=None)))
        _require_exact_axis(dataset, expected, observed)
        expected_count = len(expected)
        missing = _missing_pairs(expected - observed, "index_code")
    elif dataset == "stock_basic":
        _validate_stock_semantics(frame, chunk, attrs, source_semantics)
        expected, not_listed, delisted = _stock_expected_pairs(frame, codes, canonical_dates, attrs)
        observed = set(map(tuple, frame[["stock_code", "trade_date"]].itertuples(index=False, name=None)))
        _require_exact_axis(dataset, expected, observed)
        expected_count = len(expected)
        missing = _missing_pairs(expected - observed, "stock_code")
    elif dataset == "financial":
        _validate_financial_semantics(frame, chunk, attrs, source_semantics)
        expected_count = len(frame)
        missing = []
    elif dataset == "st_history":
        _validate_st_semantics(frame, chunk, attrs, source_semantics)
        expected_count = len(frame)
        missing = []
    else:
        raise HistoricalProviderError("CONFIGURATION_ERROR", f"unsupported dataset: {dataset}")

    errors: list[str] = []
    for trade_date, partition in _project_frame(dataset, frame, canonical_dates):
        if partition.empty and dataset not in {"st_history", "stock_basic", "financial"}:
            errors.append(f"empty canonical partition: {trade_date}")
            continue
        if partition.empty:
            continue
        try:
            validate_dataset_frame(dataset, partition, trade_date)
        except (DataValidationError, ValueError) as exc:
            errors.append(str(exc))
    if errors:
        raise HistoricalProviderError("DQ_FAILED", "; ".join(errors))

    coverage = _coverage_base(chunk, canonical_dates, expected_count=expected_count, covered_count=len(frame))
    coverage["missing"] = missing
    coverage["complete"] = not missing and len(frame) == expected_count
    if dataset == "stock_basic":
        coverage["not_yet_listed_count"] = not_listed
        coverage["already_delisted_count"] = delisted
    if not coverage["complete"]:
        raise HistoricalProviderError("DQ_FAILED", f"incomplete {dataset} coverage")
    return (
        coverage,
        {"passed": True, "level": "STRICT", "blocked_reasons": []},
        {"passed": True, "errors": []},
        "FETCHED",
        False,
    )


def _project_frame(
    dataset: str,
    frame: pd.DataFrame,
    canonical_dates: tuple[str, ...],
) -> Iterator[tuple[str, pd.DataFrame]]:
    target = get_schema_contract(dataset).columns
    for trade_date in canonical_dates:
        if dataset == "financial":
            part = frame[
                (frame["report_period"].astype(str) <= trade_date)
                & (frame["announce_date"].astype(str) <= trade_date)
            ]
        elif dataset == "st_history":
            part = frame
        else:
            part = frame[frame["trade_date"].astype(str) == trade_date]
        yield trade_date, part.loc[:, target].copy(deep=True)


def _lineage(
    dataset: str,
    attrs: dict[str, Any],
    provider_name: str,
) -> tuple[tuple[str, ...], str | None]:
    raw_keys = attrs.get("source_keys", [])
    semantics = attrs.get("source_semantics")
    if provider_name in {"fixture", "mock"}:
        if not isinstance(raw_keys, (list, tuple)) or not raw_keys:
            raise HistoricalProviderError("SEMANTIC_SOURCE_UNAVAILABLE", "fixture source_keys are required")
        keys = tuple(str(value) for value in raw_keys)
        for key in keys:
            if not _safe_source_key(key):
                raise HistoricalProviderError("SEMANTIC_SOURCE_UNAVAILABLE", "unsafe or self-referential source key")
        if semantics != _SOURCE_SEMANTICS[dataset]:
            raise HistoricalProviderError("SEMANTIC_SOURCE_UNAVAILABLE", f"untrusted {dataset} source semantics")
        return keys, str(semantics)
    return (), str(semantics) if semantics is not None else None


def _safe_source_key(value: str) -> bool:
    if not value or value.startswith("/") or "\\" in value or _DRIVE_PREFIX.match(value):
        return False
    parts = value.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        return False
    lowered = value.lower()
    return not (
        lowered.startswith("smoke/")
        or lowered.startswith("candidate/real_history_backfill/")
        or lowered.startswith("candidate/real_clean_inputs/")
    )


def _validate_fixture_scope_proof(
    dataset: str,
    attrs: dict[str, Any],
    chunk: dict[str, Any],
) -> None:
    if attrs.get("coverage_complete") is not True or attrs.get("sample_truncated") is not False:
        raise HistoricalProviderError(
            "SEMANTIC_SOURCE_UNAVAILABLE",
            f"{dataset} fixture lacks explicit complete untruncated coverage proof",
        )
    try:
        proof_start = normalize_date(attrs.get("coverage_start_date"))
        proof_end = normalize_date(attrs.get("coverage_end_date"))
    except Exception as exc:
        raise HistoricalProviderError(
            "SEMANTIC_SOURCE_UNAVAILABLE",
            f"{dataset} fixture date coverage proof is missing or invalid",
        ) from exc
    if proof_start > chunk["start_date"] or proof_end < chunk["end_date"]:
        raise HistoricalProviderError(
            "SEMANTIC_SOURCE_UNAVAILABLE",
            f"{dataset} fixture date coverage does not span the chunk",
        )
    if dataset == "benchmark_price":
        if set(attrs.get("requested_indexes", [])) != set(chunk.get("index_codes", [])):
            raise HistoricalProviderError(
                "SEMANTIC_SOURCE_UNAVAILABLE",
                "benchmark fixture index coverage does not match the chunk",
            )
    elif set(attrs.get("requested_codes", [])) != set(chunk.get("codes", [])):
        raise HistoricalProviderError(
            "SEMANTIC_SOURCE_UNAVAILABLE",
            f"{dataset} fixture code coverage does not match the chunk",
        )


def _validate_finite(dataset: str, frame: pd.DataFrame) -> None:
    contract = get_schema_contract(dataset)
    for column in contract.numeric_columns:
        values = pd.to_numeric(frame[column], errors="coerce")
        if values.isna().any() or not np.isfinite(values.to_numpy(dtype=float)).all():
            raise HistoricalProviderError("DQ_FAILED", f"invalid numeric value: {dataset}.{column}")
    if dataset == "adj_factor" and (frame["adj_factor"] <= 0).any():
        raise HistoricalProviderError("DQ_FAILED", "adj_factor must be finite and positive")
    if dataset in {"daily_price", "benchmark_price"}:
        for column in ["open", "high", "low", "close"]:
            if (frame[column] <= 0).any():
                raise HistoricalProviderError("DQ_FAILED", f"{dataset}.{column} must be positive")
    if dataset == "benchmark_price" and "pre_close" in frame.columns:
        pre_close = pd.to_numeric(frame["pre_close"], errors="coerce")
        if pre_close.isna().any() or not np.isfinite(pre_close).all() or (pre_close <= 0).any():
            raise HistoricalProviderError("DQ_FAILED", "benchmark previous-close evidence is invalid")


def _validate_stock_semantics(
    frame: pd.DataFrame,
    chunk: dict[str, Any],
    attrs: dict[str, Any],
    source_semantics: str | None,
) -> None:
    if source_semantics != "POINT_IN_TIME_HISTORICAL_SNAPSHOT" or attrs.get("snapshot_coverage_complete") is not True:
        raise HistoricalProviderError("SEMANTIC_SOURCE_UNAVAILABLE", "point-in-time stock history is not proven")
    if "list_status" in frame.columns:
        raise HistoricalProviderError("SEMANTIC_SOURCE_UNAVAILABLE", "current list_status snapshot cannot be historical")
    if "snapshot_date" in frame.columns and (frame["snapshot_date"].map(normalize_date) != frame["trade_date"]).any():
        raise HistoricalProviderError("SEMANTIC_SOURCE_UNAVAILABLE", "stock snapshot date does not equal trade_date")
    if set(attrs.get("requested_codes", [])) != set(chunk.get("codes", [])):
        raise HistoricalProviderError("SEMANTIC_SOURCE_UNAVAILABLE", "stock history code coverage is incomplete")


def _stock_expected_pairs(
    frame: pd.DataFrame,
    codes: list[str],
    canonical_dates: tuple[str, ...],
    attrs: dict[str, Any],
) -> tuple[set[tuple[str, str]], int, int]:
    observed_masters: dict[str, tuple[str, str | None]] = {}
    for code, group in frame.groupby("stock_code", sort=True):
        list_dates = set(group["list_date"].astype(str))
        delist_dates = set(None if pd.isna(value) else str(value) for value in group["delist_date"])
        if len(list_dates) != 1 or len(delist_dates) != 1:
            raise HistoricalProviderError("DQ_FAILED", f"inconsistent list/delist history for {code}")
        observed_masters[str(code)] = (next(iter(list_dates)), next(iter(delist_dates)))
    explicit = _explicit_list_delist_master(attrs, {"codes": codes}, required=False)
    if explicit is not None:
        for code, observed in observed_masters.items():
            if explicit.get(code) != observed:
                raise HistoricalProviderError("DQ_FAILED", f"stock list/delist rows disagree with master for {code}")
        masters = explicit
    else:
        masters = observed_masters
    if set(masters) != set(codes):
        raise HistoricalProviderError("SEMANTIC_SOURCE_UNAVAILABLE", "missing list/delist master for requested codes")
    expected: set[tuple[str, str]] = set()
    not_listed = 0
    delisted = 0
    for code in codes:
        list_date, delist_date = masters[code]
        for trade_date in canonical_dates:
            if trade_date < list_date:
                not_listed += 1
            elif delist_date is not None and trade_date >= delist_date:
                delisted += 1
            else:
                expected.add((code, trade_date))
    return expected, not_listed, delisted


def _explicit_list_delist_master(
    attrs: dict[str, Any],
    chunk: dict[str, Any],
    *,
    required: bool,
) -> dict[str, tuple[str, str | None]] | None:
    raw_master = attrs.get("list_delist_master")
    if raw_master is None:
        if required:
            raise HistoricalProviderError(
                "SEMANTIC_SOURCE_UNAVAILABLE",
                "empty stock history requires explicit list/delist master proof",
            )
        return None
    if not isinstance(raw_master, (list, tuple)):
        raise HistoricalProviderError("SEMANTIC_SOURCE_UNAVAILABLE", "invalid list/delist master proof")
    masters: dict[str, tuple[str, str | None]] = {}
    try:
        for item in raw_master:
            if not isinstance(item, dict) or set(item) != {"stock_code", "list_date", "delist_date"}:
                raise ValueError("master fields are invalid")
            code = normalize_stock_code(item["stock_code"])
            if code in masters:
                raise ValueError("duplicate master code")
            list_date = normalize_date(item["list_date"])
            delist_date = _nullable_date(item["delist_date"])
            if delist_date is not None and delist_date <= list_date:
                raise ValueError("delist_date must be later than list_date")
            masters[code] = (list_date, delist_date)
    except (TypeError, ValueError, SchemaMappingError) as exc:
        raise HistoricalProviderError("DQ_FAILED", f"invalid list/delist master proof: {exc}") from exc
    if set(masters) != set(chunk.get("codes", [])):
        raise HistoricalProviderError(
            "SEMANTIC_SOURCE_UNAVAILABLE",
            "list/delist master does not cover exactly the requested codes",
        )
    return masters


def _require_entirely_nonmember_window(
    masters: dict[str, tuple[str, str | None]],
    chunk: dict[str, Any],
    canonical_dates: tuple[str, ...],
) -> tuple[int, int]:
    not_listed = 0
    delisted = 0
    for code in chunk.get("codes", []):
        list_date, delist_date = masters[code]
        if list_date > chunk["end_date"]:
            not_listed += len(canonical_dates)
        elif delist_date is not None and delist_date <= chunk["start_date"]:
            delisted += len(canonical_dates)
        else:
            raise HistoricalProviderError(
                "DQ_FAILED",
                f"empty stock history omits an active member window for {code}",
            )
    return not_listed, delisted


def _validate_financial_semantics(
    frame: pd.DataFrame,
    chunk: dict[str, Any],
    attrs: dict[str, Any],
    source_semantics: str | None,
) -> None:
    if source_semantics != "TRUSTED_STANDARD_FINANCIAL_SOURCE":
        raise HistoricalProviderError("SEMANTIC_SOURCE_UNAVAILABLE", "trusted financial semantics are not proven")
    if set(attrs.get("source_coverage_codes", [])) != set(chunk.get("codes", [])):
        raise HistoricalProviderError("SEMANTIC_SOURCE_UNAVAILABLE", "financial code coverage is incomplete")
    if not set(frame["stock_code"].astype(str)).issubset(set(chunk.get("codes", []))):
        raise HistoricalProviderError("DQ_FAILED", "financial source contains unrequested stock codes")
    if frame.duplicated(["stock_code", "report_period", "announce_date"]).any():
        raise HistoricalProviderError("DQ_FAILED", "duplicate financial disclosure keys")
    start = chunk.get("report_period_start")
    end = chunk.get("report_period_end")
    if ((frame["report_period"] < start) | (frame["report_period"] > end)).any():
        raise HistoricalProviderError("DQ_FAILED", "financial report period is outside the chunk")


def _validate_st_semantics(
    frame: pd.DataFrame,
    chunk: dict[str, Any],
    attrs: dict[str, Any],
    source_semantics: str | None,
) -> None:
    if source_semantics != "HISTORICAL_INTERVAL_SOURCE":
        raise HistoricalProviderError("SEMANTIC_SOURCE_UNAVAILABLE", "historical ST interval source is not proven")
    _validate_st_coverage(attrs, chunk)
    if attrs.get("interval_row_count") != len(frame):
        raise HistoricalProviderError("DQ_FAILED", "ST interval count does not match returned rows")
    if not set(frame["stock_code"].astype(str)).issubset(set(chunk.get("codes", []))):
        raise HistoricalProviderError("DQ_FAILED", "ST source contains unrequested stock codes")
    current_markers = {"CURRENT_NAME_SNAPSHOT", "CURRENT_ST_SNAPSHOT"}
    current_sources = {"current_stock_basic_snapshot", "current_name_snapshot", "current_st_snapshot"}
    if set(frame["st_type"].astype(str)) & current_markers or set(frame["source"].astype(str)) & current_sources:
        raise HistoricalProviderError("SEMANTIC_SOURCE_UNAVAILABLE", "current ST/name snapshot cannot be historical")
    for row in frame.itertuples(index=False):
        if row.end_date is not None and row.end_date <= row.start_date:
            raise HistoricalProviderError("DQ_FAILED", "ST end_date must be greater than start_date")
        if row.start_date > chunk["end_date"] or (
            row.end_date is not None and row.end_date <= chunk["start_date"]
        ):
            raise HistoricalProviderError("DQ_FAILED", "ST interval is outside the requested scope")
    if frame.duplicated(KEY_COLUMNS["st_history"]).any():
        raise HistoricalProviderError("DQ_FAILED", "duplicate ST interval key")


def _validate_st_empty_proof(
    attrs: dict[str, Any],
    chunk: dict[str, Any],
    source_keys: tuple[str, ...],
    source_semantics: str | None,
) -> None:
    if not source_keys or source_semantics != "HISTORICAL_INTERVAL_SOURCE":
        raise HistoricalProviderError("SEMANTIC_SOURCE_UNAVAILABLE", "empty ST result lacks historical lineage")
    _validate_st_coverage(attrs, chunk)
    if attrs.get("interval_row_count") != 0:
        raise HistoricalProviderError("SEMANTIC_SOURCE_UNAVAILABLE", "empty ST proof has an invalid interval count")


def _validate_st_coverage(attrs: dict[str, Any], chunk: dict[str, Any]) -> None:
    if attrs.get("coverage_schema_version") != "goal20.st_history_coverage.v1":
        raise HistoricalProviderError("SEMANTIC_SOURCE_UNAVAILABLE", "ST coverage schema is not proven")
    if attrs.get("coverage_complete") is not True:
        raise HistoricalProviderError("SEMANTIC_SOURCE_UNAVAILABLE", "ST coverage is incomplete")
    if set(attrs.get("requested_codes", [])) != set(chunk.get("codes", [])):
        raise HistoricalProviderError("SEMANTIC_SOURCE_UNAVAILABLE", "ST code coverage is incomplete")
    if attrs.get("coverage_start_date") != chunk["start_date"] or attrs.get("coverage_end_date") != chunk["end_date"]:
        raise HistoricalProviderError("SEMANTIC_SOURCE_UNAVAILABLE", "ST date coverage is incomplete")
    if "interval_row_count" not in attrs:
        raise HistoricalProviderError("SEMANTIC_SOURCE_UNAVAILABLE", "ST interval count proof is missing")


def _require_exact_axis(
    dataset: str,
    expected: set[tuple[str, str]],
    observed: set[tuple[str, str]],
) -> None:
    if observed != expected:
        raise HistoricalProviderError("DQ_FAILED", f"incomplete or extra {dataset} natural-axis coverage")


def _missing_pairs(values: set[tuple[str, str]], code_field: str) -> list[dict[str, str]]:
    return [{code_field: code, "trade_date": trade_date} for code, trade_date in sorted(values)]


def _coverage_base(
    chunk: dict[str, Any],
    canonical_dates: tuple[str, ...],
    *,
    expected_count: int,
    covered_count: int,
) -> dict[str, Any]:
    return {
        "axis": chunk["strategy"],
        "complete": expected_count == covered_count,
        "expected_count": int(expected_count),
        "covered_count": int(covered_count),
        "missing": [],
        "requested_codes": list(chunk.get("codes", [])),
        "requested_indexes": list(chunk.get("index_codes", [])),
        "requested_start_date": chunk["start_date"],
        "requested_end_date": chunk["end_date"],
        "canonical_trade_dates": list(canonical_dates),
        "valid_empty": False,
    }


def _canonical_failure_record(value: Any) -> bool:
    if not isinstance(value, dict) or set(value) != {"category", "retryable", "exception_type", "message"}:
        return False
    category = value.get("category")
    return (
        category in FAILURE_CATEGORIES
        and isinstance(value.get("retryable"), bool)
        and value["retryable"] is (category in _RETRYABLE_FAILURES)
        and isinstance(value.get("exception_type"), str)
        and isinstance(value.get("message"), str)
    )


def _validate_result_evidence(
    *,
    frame: pd.DataFrame,
    dq: dict[str, Any],
    coverage: dict[str, Any],
    validation: dict[str, Any],
    canonical_dates: tuple[str, ...],
    partition_strategy: str,
) -> None:
    coverage_fields = {
        "axis",
        "complete",
        "expected_count",
        "covered_count",
        "missing",
        "requested_codes",
        "requested_indexes",
        "requested_start_date",
        "requested_end_date",
        "canonical_trade_dates",
        "valid_empty",
    }
    if not coverage_fields.issubset(coverage):
        raise ValueError("coverage evidence is incomplete")
    expected_count = coverage["expected_count"]
    covered_count = coverage["covered_count"]
    if any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in (expected_count, covered_count)):
        raise ValueError("coverage counts must be non-negative integers")
    if covered_count != len(frame):
        raise ValueError("covered_count must equal the result row count")
    if not isinstance(coverage["missing"], list):
        raise ValueError("coverage missing evidence must be a list")
    if not isinstance(coverage["requested_codes"], list) or not isinstance(coverage["requested_indexes"], list):
        raise ValueError("coverage requested axes must be lists")
    if type(coverage.get("valid_empty")) is not bool:
        raise ValueError("coverage valid_empty must be a boolean")
    if coverage["complete"] is True and (
        expected_count != covered_count or coverage["missing"]
    ):
        raise ValueError("complete coverage requires equal counts and no missing keys")

    if set(dq) != {"passed", "level", "blocked_reasons"}:
        raise ValueError("DQ evidence must expose passed, level and blocked_reasons")
    if dq["level"] not in {"STRICT", "BLOCKED"} or not isinstance(dq["blocked_reasons"], list):
        raise ValueError("DQ level or blocked reasons are invalid")
    if dq["passed"] is True and (dq["level"] != "STRICT" or dq["blocked_reasons"]):
        raise ValueError("successful DQ cannot carry blocked evidence")
    if dq["passed"] is False and dq["level"] != "BLOCKED":
        raise ValueError("failed DQ must be BLOCKED")

    if set(validation) != {"passed", "errors"} or not isinstance(validation["errors"], list):
        raise ValueError("validation evidence must expose passed and errors")
    if validation["passed"] is True and validation["errors"]:
        raise ValueError("successful validation cannot carry errors")

    try:
        requested_start = normalize_date(coverage["requested_start_date"])
        requested_end = normalize_date(coverage["requested_end_date"])
        normalized_dates = tuple(normalize_date(value) for value in canonical_dates)
    except Exception as exc:
        raise ValueError("coverage dates are invalid") from exc
    if requested_start > requested_end or normalized_dates != canonical_dates:
        raise ValueError("coverage dates are not normalized")
    if canonical_dates != tuple(sorted(set(canonical_dates))):
        raise ValueError("canonical trade dates must be sorted and unique")
    if list(canonical_dates) != coverage["canonical_trade_dates"]:
        raise ValueError("coverage canonical dates do not match the result")
    if partition_strategy == "BY_TRADE_DATE_COLUMN" and any(
        value < requested_start or value > requested_end for value in canonical_dates
    ):
        raise ValueError("canonical trade dates are outside requested coverage")
    if partition_strategy == "BY_TRADE_DATE_COLUMN" and not frame.empty:
        observed_dates = set(frame["trade_date"].astype(str))
        if not observed_dates.issubset(set(canonical_dates)):
            raise ValueError("result trade dates are inconsistent with canonical dates")


def _sanitize_provider_call(value: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("provider call evidence must be a dictionary")
    allowed = {"endpoint", "strategy", "parameters", "row_count", "status"}
    if set(value) != allowed:
        raise ValueError("provider call evidence fields are incomplete or unsupported")
    if not isinstance(value["endpoint"], str) or not value["endpoint"]:
        raise ValueError("provider call endpoint must be a non-empty string")
    if not isinstance(value["strategy"], str) or not value["strategy"]:
        raise ValueError("provider call strategy must be a non-empty string")
    if not isinstance(value["parameters"], dict):
        raise ValueError("provider call parameters must be a dictionary")
    row_count = value["row_count"]
    if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 0:
        raise ValueError("provider call row_count must be a non-negative integer")
    status = value["status"]
    if status not in {"FETCHED", "VALID_EMPTY", "MISSING", "FAILED", "SCHEMA_DRIFT"}:
        raise ValueError("provider call status is invalid")
    if (status == "FETCHED") != (row_count > 0):
        raise ValueError("provider call status and row_count disagree")
    result = {key: deepcopy(value[key]) for key in allowed}
    result["endpoint"] = _sanitize_json_value(result["endpoint"])
    result["strategy"] = _sanitize_json_value(result["strategy"])
    result["parameters"] = _sanitize_json_value(result["parameters"])
    return result


def _sanitize_result_metadata(value: Any) -> Any:
    if isinstance(value, np.generic) or isinstance(value, pd.Timestamp):
        raise ValueError("result metadata must be strictly JSON-safe")
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise ValueError("result metadata keys must be strings")
        return {
            key: _sanitize_result_metadata(item)
            for key, item in value.items()
            if not _SENSITIVE_KEY.search(key)
        }
    if isinstance(value, (list, tuple)):
        return [_sanitize_result_metadata(item) for item in value]
    if isinstance(value, str):
        redacted = classify_backfill_failure(RuntimeError(value))["message"]
        return _AUTH_SCHEME.sub("[REDACTED]", redacted)
    if value is None or type(value) in {bool, int}:
        return value
    if type(value) is float and math.isfinite(value):
        return value
    raise ValueError("result metadata must be strictly JSON-safe")


def _sanitize_schema_label(value: Any) -> str:
    try:
        text = value if isinstance(value, str) else str(value)
    except Exception:
        text = f"UNPRINTABLE_COLUMN:{type(value).__name__}"
    return _sanitize_result_metadata(text)


def _sanitize_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _sanitize_json_value(item)
            for key, item in value.items()
            if not _SENSITIVE_KEY.search(str(key))
        }
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_sanitize_json_value(item) for item in value]
    if isinstance(value, str):
        redacted = classify_backfill_failure(RuntimeError(value))["message"]
        return _AUTH_SCHEME.sub("[REDACTED]", redacted)
    if isinstance(value, (np.generic,)):
        return value.item()
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return _sanitize_json_value(str(value))


def _nullable_date(value: Any) -> str | None:
    if value is None or value == "" or pd.isna(value):
        return None
    return normalize_date(value)


def _bool_value(value: Any) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    if isinstance(value, (int, np.integer)) and int(value) in {0, 1}:
        return bool(value)
    if isinstance(value, str) and value.strip().lower() in {"true", "false", "1", "0"}:
        return value.strip().lower() in {"true", "1"}
    raise SchemaMappingError(f"invalid boolean value: {value}")


def _iter_dates(start_date: str, end_date: str) -> Iterator[str]:
    current = date.fromisoformat(start_date)
    final = date.fromisoformat(end_date)
    while current <= final:
        yield current.isoformat()
        current += timedelta(days=1)

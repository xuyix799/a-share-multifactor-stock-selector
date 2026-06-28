import os
from time import sleep

import pandas as pd

from stock_selector.config.config_loader import load_settings
from stock_selector.providers.base import ExternalProviderSkeleton, ProviderConfigurationError, ProviderFetchError
from stock_selector.providers.retry_policy import RetryPolicy
from stock_selector.utils.date_validator import validate_trade_date


GOAL10_TUSHARE_DATASETS = ("stock_basic", "daily_price", "adj_factor", "daily_basic")


class TushareProvider(ExternalProviderSkeleton):
    name = "tushare"

    def __init__(self, settings: dict | None = None, pro_client=None):
        settings = settings or load_settings()
        config = settings.get("provider", {}).get("tushare", {})
        if not _is_enabled(config):
            raise ProviderConfigurationError("tushare provider is disabled in settings")
        token_env = config.get("token_env", "TUSHARE_TOKEN")
        token = os.getenv(token_env)
        if not token:
            raise ProviderConfigurationError(f"missing {token_env} for tushare provider")
        retry_config = settings.get("provider", {}).get("retry", {})
        self._retry = RetryPolicy(
            max_attempts=int(retry_config.get("max_attempts", 3)),
            backoff_seconds=float(retry_config.get("backoff_seconds", 2)),
        )
        self._pro = pro_client or self._create_client(token)

    def fetch_stock_basic(self, trade_date: str) -> pd.DataFrame:
        trade_date = validate_trade_date(trade_date)
        raw = self._fetch(
            "stock_basic",
            exchange="",
            list_status="L",
            fields="ts_code,name,industry,market,list_date",
        )
        result = raw.copy()
        result["exchange"] = result.get("exchange", result["ts_code"].map(_exchange_from_ts_code))
        result["market_type"] = result.get("market_type", result.get("market", ""))
        result["delist_date"] = None
        result["is_st"] = result["name"].astype(str).str.upper().str.contains("ST")
        result["trade_date"] = _compact_trade_date(trade_date)
        return result

    def fetch_daily_price(self, trade_date: str) -> pd.DataFrame:
        compact_date = _compact_trade_date(validate_trade_date(trade_date))
        daily = self._fetch(
            "daily",
            trade_date=compact_date,
            fields="ts_code,trade_date,open,high,low,close,pre_close,pct_chg,vol,amount",
        )
        limits = self._fetch("stk_limit", trade_date=compact_date, fields="ts_code,trade_date,up_limit,down_limit")
        merged = daily.merge(limits[["ts_code", "trade_date", "up_limit", "down_limit"]], on=["ts_code", "trade_date"], how="left")
        if merged[["up_limit", "down_limit"]].isna().any().any():
            raise ProviderFetchError("missing Tushare limit prices for daily_price smoke")
        raise ProviderFetchError("Tushare daily_price smoke has price and limit fields, but is_paused is not available from a trusted source")

    def fetch_adj_factor(self, trade_date: str) -> pd.DataFrame:
        compact_date = _compact_trade_date(validate_trade_date(trade_date))
        return self._fetch("adj_factor", trade_date=compact_date, fields="ts_code,trade_date,adj_factor")

    def fetch_daily_basic(self, trade_date: str) -> pd.DataFrame:
        compact_date = _compact_trade_date(validate_trade_date(trade_date))
        return self._fetch(
            "daily_basic",
            trade_date=compact_date,
            fields="ts_code,trade_date,pe_ttm,pb,ps_ttm,total_mv,circ_mv,turnover_rate",
        )

    def fetch_financial(self, trade_date: str) -> pd.DataFrame:
        return self._unsupported("financial", trade_date)

    def fetch_st_history(self, trade_date: str) -> pd.DataFrame:
        return self._unsupported("st_history", trade_date)

    def fetch_benchmark_price(self, trade_date: str) -> pd.DataFrame:
        return self._unsupported("benchmark_price", trade_date)

    def fetch_raw_endpoint(self, endpoint: str, **kwargs) -> pd.DataFrame:
        return self._fetch(endpoint, **kwargs)

    def fetch_raw_endpoint_allow_empty(self, endpoint: str, **kwargs) -> pd.DataFrame:
        method = getattr(self._pro, endpoint)
        try:
            df = self._run_tushare_call(lambda: method(**kwargs))
        except Exception as exc:
            raise ProviderFetchError(f"tushare {endpoint} fetch failed: {exc}") from exc
        if df is None:
            return pd.DataFrame()
        return df.copy()

    def _fetch(self, endpoint: str, **kwargs) -> pd.DataFrame:
        method = getattr(self._pro, endpoint)
        try:
            df = self._run_tushare_call(lambda: method(**kwargs))
        except Exception as exc:
            raise ProviderFetchError(f"tushare {endpoint} fetch failed: {exc}") from exc
        if df is None or df.empty:
            raise ProviderFetchError(f"tushare {endpoint} returned no rows; check Tushare token permission and trade_date")
        return df.copy()

    def _run_tushare_call(self, operation):
        last_error: Exception | None = None
        for attempt in range(1, self._retry.max_attempts + 1):
            try:
                return operation()
            except Exception as exc:
                if _is_non_retryable_tushare_error(exc):
                    raise
                last_error = exc
                if attempt < self._retry.max_attempts:
                    sleep(self._retry.backoff_seconds)
        assert last_error is not None
        raise last_error

    def _unsupported(self, dataset: str, trade_date: str) -> pd.DataFrame:
        _ = validate_trade_date(trade_date)
        raise ProviderFetchError(f"{dataset} is not supported by Goal 10 Tushare smoke")

    def _create_client(self, token: str):
        try:
            import tushare as ts
        except ImportError as exc:
            raise ProviderConfigurationError("tushare package is not installed") from exc
        return ts.pro_api(token)


def _compact_trade_date(trade_date: str) -> str:
    return validate_trade_date(trade_date).replace("-", "")


def _exchange_from_ts_code(ts_code: str) -> str:
    value = str(ts_code).upper()
    if value.endswith(".SZ"):
        return "SZSE"
    if value.endswith(".SH"):
        return "SSE"
    if value.endswith(".BJ"):
        return "BSE"
    return ""


def _is_enabled(config: dict) -> bool:
    return bool(config.get("enabled", False)) or os.getenv("STOCK_TUSHARE_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}


def _is_non_retryable_tushare_error(exc: Exception) -> bool:
    message = str(exc)
    return any(marker in message for marker in ("没有接口", "频率超限", "权限"))


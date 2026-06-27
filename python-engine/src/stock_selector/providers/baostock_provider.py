import os

import pandas as pd

from stock_selector.config.config_loader import load_settings
from stock_selector.providers.base import ExternalProviderSkeleton, ProviderConfigurationError, ProviderFetchError
from stock_selector.utils.date_validator import validate_trade_date


DAILY_RAW_SMOKE_SYMBOLS = {
    "000001.SZ": "sz.000001",
}
DAILY_RAW_SMOKE_FIELDS = "date,code,open,high,low,close,preclose,volume,amount,pctChg"


class BaostockProvider(ExternalProviderSkeleton):
    name = "baostock"

    def __init__(self, settings: dict | None = None, bs_module=None):
        settings = settings or load_settings()
        config = settings.get("provider", {}).get("baostock", {})
        if not _is_enabled(config):
            raise ProviderConfigurationError("baostock provider is disabled in settings")
        self._bs = bs_module or self._import_baostock()

    def fetch_daily_price(self, trade_date: str) -> pd.DataFrame:
        return self._capability_gap("daily_price", trade_date, ["limit_up", "limit_down", "is_paused"])

    def fetch_daily_price_raw_smoke(self, trade_date: str) -> pd.DataFrame:
        trade_date = validate_trade_date(trade_date)
        rows = []
        login_result = self._bs.login()
        if str(getattr(login_result, "error_code", "")) != "0":
            raise ProviderFetchError(f"baostock login failed: {getattr(login_result, 'error_msg', '')}")
        try:
            for stock_code, symbol in DAILY_RAW_SMOKE_SYMBOLS.items():
                query_result = self._bs.query_history_k_data_plus(
                    symbol,
                    DAILY_RAW_SMOKE_FIELDS,
                    start_date=trade_date,
                    end_date=trade_date,
                    frequency="d",
                    adjustflag="3",
                )
                if str(getattr(query_result, "error_code", "")) != "0":
                    raise ProviderFetchError(f"baostock daily_price_raw_smoke fetch failed for {symbol}: {getattr(query_result, 'error_msg', '')}")
                raw = _query_result_to_frame(query_result)
                if raw.empty:
                    raise ProviderFetchError(f"baostock daily_price_raw_smoke returned no rows for {symbol} {trade_date}")
                rows.append(_map_daily_raw_row(stock_code, symbol, raw.iloc[-1], trade_date))
        finally:
            self._bs.logout()
        return pd.DataFrame(rows)

    def _capability_gap(self, dataset: str, trade_date: str, missing_fields: list[str]) -> pd.DataFrame:
        _ = validate_trade_date(trade_date)
        raise ProviderFetchError(f"provider capability insufficient: baostock {dataset} missing required fields: {', '.join(missing_fields)}")

    def _import_baostock(self):
        try:
            import baostock as bs
        except ImportError as exc:
            raise ProviderConfigurationError("baostock package is not installed") from exc
        return bs


def _query_result_to_frame(query_result) -> pd.DataFrame:
    rows = []
    while query_result.next():
        rows.append(query_result.get_row_data())
    return pd.DataFrame(rows, columns=list(query_result.fields))


def _map_daily_raw_row(stock_code: str, symbol: str, row: pd.Series, trade_date: str) -> dict[str, object]:
    date_value = validate_trade_date(str(row["date"]))
    if date_value != trade_date:
        raise ProviderFetchError(f"baostock daily_price_raw_smoke returned mismatched date for {symbol}: {date_value}")
    return {
        "stock_code": stock_code,
        "trade_date": trade_date,
        "open": _number(row["open"], "open", symbol),
        "high": _number(row["high"], "high", symbol),
        "low": _number(row["low"], "low", symbol),
        "close": _number(row["close"], "close", symbol),
        "volume": _number(row["volume"], "volume", symbol),
        "amount": _number(row["amount"], "amount", symbol),
        "pct_chg": _number(row["pctChg"], "pctChg", symbol),
        "source_symbol": symbol,
    }


def _number(value, column: str, symbol: str) -> float:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        raise ProviderFetchError(f"baostock daily_price_raw_smoke returned invalid numeric {column} for {symbol}")
    return float(numeric)


def _is_enabled(config: dict) -> bool:
    return bool(config.get("enabled", False)) or os.getenv("STOCK_BAOSTOCK_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}


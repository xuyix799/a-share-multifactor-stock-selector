import os

import pandas as pd

from stock_selector.config.config_loader import load_settings
from stock_selector.providers.base import ExternalProviderSkeleton, ProviderConfigurationError, ProviderFetchError
from stock_selector.utils.date_validator import validate_trade_date


BENCHMARK_SYMBOLS = {
    "000300.SH": "sh000300",
    "000905.SH": "sh000905",
    "000906.SH": "sh000906",
}
DAILY_RAW_SMOKE_SYMBOLS = {
    "000001.SZ": "000001",
}


class AKShareProvider(ExternalProviderSkeleton):
    name = "akshare"

    def __init__(self, settings: dict | None = None, ak_module=None):
        settings = settings or load_settings()
        config = settings.get("provider", {}).get("akshare", {})
        if not _is_enabled(config):
            raise ProviderConfigurationError("akshare provider is disabled in settings")
        self._ak = ak_module or self._import_akshare()

    def fetch_stock_basic(self, trade_date: str) -> pd.DataFrame:
        return self._capability_gap("stock_basic", trade_date, ["list_date", "industry", "market_type", "is_st"])

    def fetch_daily_price(self, trade_date: str) -> pd.DataFrame:
        return self._capability_gap("daily_price", trade_date, ["limit_up", "limit_down", "is_paused"])

    def fetch_daily_price_raw_smoke(self, trade_date: str) -> pd.DataFrame:
        trade_date = validate_trade_date(trade_date)
        rows = []
        for stock_code, symbol in DAILY_RAW_SMOKE_SYMBOLS.items():
            history = self._fetch_daily_raw_history(symbol, trade_date)
            row = history.iloc[-1]
            rows.append(
                {
                    "stock_code": stock_code,
                    "trade_date": trade_date,
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "amount": row["amount"],
                    "pct_chg": row["pct_chg"],
                    "source_symbol": symbol,
                }
            )
        return pd.DataFrame(rows)

    def fetch_adj_factor(self, trade_date: str) -> pd.DataFrame:
        return self._capability_gap("adj_factor", trade_date, ["adj_factor"])

    def fetch_daily_basic(self, trade_date: str) -> pd.DataFrame:
        return self._capability_gap("daily_basic", trade_date, ["pe_ttm", "pb", "ps_ttm", "total_mv", "circ_mv", "turnover_rate"])

    def fetch_financial(self, trade_date: str) -> pd.DataFrame:
        return self._capability_gap("financial", trade_date, ["announce_date", "report_period", "roe", "debt_ratio"])

    def fetch_st_history(self, trade_date: str) -> pd.DataFrame:
        return self._capability_gap("st_history", trade_date, ["st_type", "start_date", "end_date"])

    def fetch_benchmark_price(self, trade_date: str) -> pd.DataFrame:
        trade_date = validate_trade_date(trade_date)
        rows = []
        for index_code, symbol in BENCHMARK_SYMBOLS.items():
            history = self._fetch_index_history(symbol)
            row = _row_for_trade_date(history, symbol, trade_date)
            previous = _previous_row(history, symbol, trade_date)
            rows.append(
                {
                    "index_code": index_code,
                    "trade_date": trade_date,
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "pct_chg": (float(row["close"]) / float(previous["close"]) - 1.0) * 100.0,
                }
            )
        return pd.DataFrame(rows)

    def _fetch_index_history(self, symbol: str) -> pd.DataFrame:
        try:
            df = self._ak.stock_zh_index_daily(symbol=symbol)
        except Exception as exc:
            raise ProviderFetchError(f"akshare benchmark_price fetch failed for {symbol}: {exc}") from exc
        required = {"date", "open", "high", "low", "close"}
        missing = sorted(required - set(df.columns))
        if missing:
            raise ProviderFetchError(f"provider capability insufficient: akshare benchmark_price missing fields: {', '.join(missing)}")
        result = df.copy()
        result["_trade_date"] = pd.to_datetime(result["date"], errors="coerce").dt.date.astype(str)
        for column in ["open", "high", "low", "close"]:
            result[column] = pd.to_numeric(result[column], errors="coerce")
        if result[["open", "high", "low", "close"]].isna().any().any():
            raise ProviderFetchError(f"akshare benchmark_price returned invalid numeric values for {symbol}")
        return result

    def _fetch_daily_raw_history(self, symbol: str, trade_date: str) -> pd.DataFrame:
        compact_date = trade_date.replace("-", "")
        try:
            df = self._ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=compact_date, end_date=compact_date, adjust="")
        except Exception as exc:
            raise ProviderFetchError(f"akshare daily_price_raw_smoke fetch failed for {symbol}: {exc}") from exc
        required = {"日期", "开盘", "最高", "最低", "收盘", "成交量", "成交额", "涨跌幅"}
        missing = sorted(required - set(df.columns))
        if missing:
            raise ProviderFetchError(f"provider capability insufficient: akshare daily_price_raw_smoke missing fields: {', '.join(missing)}")
        if df.empty:
            raise ProviderFetchError(f"akshare daily_price_raw_smoke returned no rows for {symbol} {trade_date}")
        result = pd.DataFrame(
            {
                "_trade_date": pd.to_datetime(df["日期"], errors="coerce").dt.date.astype(str),
                "open": pd.to_numeric(df["开盘"], errors="coerce"),
                "high": pd.to_numeric(df["最高"], errors="coerce"),
                "low": pd.to_numeric(df["最低"], errors="coerce"),
                "close": pd.to_numeric(df["收盘"], errors="coerce"),
                "volume": pd.to_numeric(df["成交量"], errors="coerce"),
                "amount": pd.to_numeric(df["成交额"], errors="coerce"),
                "pct_chg": pd.to_numeric(df["涨跌幅"], errors="coerce"),
            }
        )
        result = result[result["_trade_date"] == trade_date]
        if result.empty:
            raise ProviderFetchError(f"akshare daily_price_raw_smoke returned no rows for {symbol} {trade_date}")
        if result[["open", "high", "low", "close", "volume", "amount", "pct_chg"]].isna().any().any():
            raise ProviderFetchError(f"akshare daily_price_raw_smoke returned invalid numeric values for {symbol}")
        return result

    def _capability_gap(self, dataset: str, trade_date: str, missing_fields: list[str]) -> pd.DataFrame:
        _ = validate_trade_date(trade_date)
        raise ProviderFetchError(f"provider capability insufficient: akshare {dataset} missing required fields: {', '.join(missing_fields)}")

    def _import_akshare(self):
        try:
            import akshare as ak
        except ImportError as exc:
            raise ProviderConfigurationError("akshare package is not installed") from exc
        return ak


def _row_for_trade_date(history: pd.DataFrame, symbol: str, trade_date: str) -> pd.Series:
    rows = history[history["_trade_date"] == trade_date]
    if rows.empty:
        raise ProviderFetchError(f"akshare benchmark_price returned no rows for {symbol} {trade_date}")
    return rows.iloc[-1]


def _previous_row(history: pd.DataFrame, symbol: str, trade_date: str) -> pd.Series:
    rows = history[history["_trade_date"] < trade_date].sort_values("_trade_date")
    if rows.empty:
        raise ProviderFetchError(f"akshare benchmark_price missing previous close for {symbol} before {trade_date}")
    return rows.iloc[-1]


def _is_enabled(config: dict) -> bool:
    return bool(config.get("enabled", False)) or os.getenv("STOCK_AKSHARE_ENABLED", "").strip().lower() in {"1", "true", "yes", "on"}


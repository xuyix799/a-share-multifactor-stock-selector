from datetime import date, timedelta

import pandas as pd


FACTOR_STOCKS = [
    ("000001.SZ", "银行", "主板"),
    ("600000.SH", "银行", "主板"),
    ("600519.SH", "食品饮料", "主板"),
]


def factor_input_frame(trade_date: str = "2026-06-19") -> pd.DataFrame:
    rows = []
    for idx, (stock_code, industry, market_type) in enumerate(FACTOR_STOCKS):
        rows.append(
            {
                "stock_code": stock_code,
                "trade_date": trade_date,
                "industry": industry,
                "market_type": market_type,
                "adj_close": 20.0 + idx * 10,
                "amount": 100_000_000 + idx * 10_000_000,
                "turnover_rate": 0.8 + idx * 0.1,
                "pe_ttm": 10.0 + idx * 5,
                "pb": 1.0 + idx * 0.3,
                "ps_ttm": 2.0 + idx * 0.5,
                "total_mv": 80_000_000_000 + idx * 10_000_000_000,
                "circ_mv": 60_000_000_000 + idx * 8_000_000_000,
                "revenue_yoy": 0.08 + idx * 0.02,
                "net_profit_yoy": 0.06 + idx * 0.02,
                "roe": 0.10 + idx * 0.02,
                "gross_margin": 0.25 + idx * 0.05,
                "debt_ratio": 0.40 + idx * 0.05,
                "operating_cashflow": 1_000_000_000 + idx * 100_000_000,
            }
        )
    return pd.DataFrame(rows)


def adjusted_price_history(trade_date: str = "2026-06-19", days: int = 130, include_future: bool = False) -> pd.DataFrame:
    end = date.fromisoformat(trade_date)
    start = end - timedelta(days=days - 1)
    rows = []
    for offset in range(days):
        current = start + timedelta(days=offset)
        for idx, (stock_code, *_rest) in enumerate(FACTOR_STOCKS):
            close = 20.0 + idx * 10 + offset
            rows.append(
                {
                    "stock_code": stock_code,
                    "trade_date": current.isoformat(),
                    "adj_open": close - 0.2,
                    "adj_high": close + 0.5,
                    "adj_low": close - 0.5,
                    "adj_close": close,
                    "volume": 1_000_000 + idx * 10_000,
                    "amount": close * 1_000_000,
                    "pct_chg": 1.0,
                    "is_paused": False,
                    "limit_up": close * 1.1,
                    "limit_down": close * 0.9,
                }
            )
    if include_future:
        future = end + timedelta(days=1)
        for stock_code, *_rest in FACTOR_STOCKS:
            rows.append(
                {
                    "stock_code": stock_code,
                    "trade_date": future.isoformat(),
                    "adj_open": 999.0,
                    "adj_high": 1000.0,
                    "adj_low": 998.0,
                    "adj_close": 999.0,
                    "volume": 1_000_000,
                    "amount": 999_000_000,
                    "pct_chg": 99.0,
                    "is_paused": False,
                    "limit_up": 1100.0,
                    "limit_down": 900.0,
                }
            )
    return pd.DataFrame(rows)


def clean_snapshot_history(trade_date: str = "2026-06-19", days: int = 5, include_future: bool = False) -> pd.DataFrame:
    end = date.fromisoformat(trade_date)
    rows = []
    for offset in range(days):
        current = end - timedelta(days=days - 1 - offset)
        for idx, (stock_code, industry, market_type) in enumerate(FACTOR_STOCKS):
            rows.append(
                {
                    "stock_code": stock_code,
                    "trade_date": current.isoformat(),
                    "stock_name": stock_code,
                    "industry": industry,
                    "market_type": market_type,
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.0,
                    "close": 10.0,
                    "pre_close": 9.8,
                    "volume": 1_000_000,
                    "amount": 100_000_000,
                    "pct_chg": 1.0,
                    "is_paused": False,
                    "limit_up": 11.0,
                    "limit_down": 9.0,
                    "adj_open": 10.0,
                    "adj_high": 11.0,
                    "adj_low": 9.0,
                    "adj_close": 10.0,
                    "pe_ttm": 30.0 - offset - idx,
                    "pb": 3.0 - offset * 0.1 - idx * 0.1,
                    "ps_ttm": 2.0,
                    "total_mv": 80_000_000_000,
                    "circ_mv": 60_000_000_000,
                    "turnover_rate": 0.8,
                    "report_period": "2026-03-31",
                    "announce_date": "2026-05-20",
                    "revenue_yoy": 0.08,
                    "net_profit_yoy": 0.06,
                    "roe": 0.10,
                    "gross_margin": 0.25,
                    "debt_ratio": 0.40,
                    "operating_cashflow": 1_000_000_000,
                    "is_st_on_date": False,
                    "listed_days": 3000,
                }
            )
    if include_future:
        future = end + timedelta(days=1)
        for stock_code, industry, market_type in FACTOR_STOCKS:
            row = rows[-1].copy()
            row.update({"stock_code": stock_code, "industry": industry, "market_type": market_type, "trade_date": future.isoformat(), "pe_ttm": 1.0, "pb": 0.1})
            rows.append(row)
    return pd.DataFrame(rows)


def benchmark_price_history(trade_date: str = "2026-06-19", days: int = 130) -> pd.DataFrame:
    end = date.fromisoformat(trade_date)
    start = end - timedelta(days=days - 1)
    rows = []
    for offset in range(days):
        current = start + timedelta(days=offset)
        close = 4000.0 + offset
        rows.append(
            {
                "index_code": "000300.SH",
                "trade_date": current.isoformat(),
                "open": close - 1,
                "high": close + 5,
                "low": close - 5,
                "close": close,
                "pct_chg": 0.1,
            }
        )
    return pd.DataFrame(rows)

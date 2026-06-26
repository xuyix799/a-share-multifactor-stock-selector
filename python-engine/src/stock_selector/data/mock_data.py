from datetime import date, timedelta

import pandas as pd

from stock_selector.storage.partition import SUPPORTED_DATASETS, validate_dataset
from stock_selector.utils.date_validator import validate_trade_date


MOCK_STOCKS = [
    ("000001.SZ", "平安银行", "银行", "主板"),
    ("000002.SZ", "万科A", "房地产", "主板"),
    ("600000.SH", "浦发银行", "银行", "主板"),
    ("600519.SH", "贵州茅台", "食品饮料", "主板"),
    ("300750.SZ", "宁德时代", "电力设备", "创业板"),
]


def _d(value: str) -> date:
    return date.fromisoformat(value)


def generate_mock_dataset(dataset: str, trade_date: str) -> pd.DataFrame:
    dataset = validate_dataset(dataset)
    trade_date = validate_trade_date(trade_date)

    generators = {
        "stock_basic": _stock_basic,
        "daily_price": _daily_price,
        "adj_factor": _adj_factor,
        "daily_basic": _daily_basic,
        "financial": _financial,
        "st_history": _st_history,
        "benchmark_price": _benchmark_price,
    }
    return generators[dataset](trade_date)


def generate_all_mock_datasets(trade_date: str) -> dict[str, pd.DataFrame]:
    trade_date = validate_trade_date(trade_date)
    return {dataset: generate_mock_dataset(dataset, trade_date) for dataset in SUPPORTED_DATASETS}


def _stock_basic(trade_date: str) -> pd.DataFrame:
    rows = []
    for idx, (code, name, industry, market_type) in enumerate(MOCK_STOCKS):
        rows.append(
            {
                "stock_code": code,
                "stock_name": name,
                "exchange": code.split(".")[1],
                "list_date": "2010-01-01",
                "delist_date": None,
                "industry": industry,
                "market_type": market_type,
                "is_st": idx == 1,
                "trade_date": trade_date,
            }
        )
    return pd.DataFrame(rows)


def _daily_price(trade_date: str) -> pd.DataFrame:
    rows = []
    for idx, (code, *_rest) in enumerate(MOCK_STOCKS):
        pre_close = 10.0 + idx * 8.0
        close = round(pre_close * (1.01 + idx * 0.002), 2)
        open_price = round(pre_close * (1.005 + idx * 0.001), 2)
        high = round(max(open_price, close) * 1.02, 2)
        low = round(min(open_price, close) * 0.98, 2)
        volume = 1_000_000 + idx * 120_000
        amount = round(volume * close, 2)
        rows.append(
            {
                "stock_code": code,
                "trade_date": trade_date,
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "pre_close": pre_close,
                "volume": volume,
                "amount": amount,
                "pct_chg": round((close / pre_close - 1) * 100, 4),
                "is_paused": False,
                "limit_up": round(pre_close * 1.10, 2),
                "limit_down": round(pre_close * 0.90, 2),
            }
        )
    return pd.DataFrame(rows)


def _adj_factor(trade_date: str) -> pd.DataFrame:
    return pd.DataFrame(
        [{"stock_code": code, "trade_date": trade_date, "adj_factor": 1.0 + idx * 0.01} for idx, (code, *_rest) in enumerate(MOCK_STOCKS)]
    )


def _daily_basic(trade_date: str) -> pd.DataFrame:
    rows = []
    for idx, (code, *_rest) in enumerate(MOCK_STOCKS):
        rows.append(
            {
                "stock_code": code,
                "trade_date": trade_date,
                "pe_ttm": 8.0 + idx * 5.0,
                "pb": 0.8 + idx * 0.4,
                "ps_ttm": 1.2 + idx * 0.5,
                "total_mv": 50_000_000_000 + idx * 8_000_000_000,
                "circ_mv": 40_000_000_000 + idx * 6_000_000_000,
                "turnover_rate": 0.5 + idx * 0.2,
            }
        )
    return pd.DataFrame(rows)


def _financial(trade_date: str) -> pd.DataFrame:
    announce_date = (_d(trade_date) - timedelta(days=30)).isoformat()
    rows = []
    for idx, (code, *_rest) in enumerate(MOCK_STOCKS):
        rows.append(
            {
                "stock_code": code,
                "report_period": "2026-03-31",
                "announce_date": announce_date,
                "revenue_yoy": 0.05 + idx * 0.02,
                "net_profit_yoy": 0.04 + idx * 0.015,
                "roe": 0.06 + idx * 0.015,
                "gross_margin": 0.20 + idx * 0.04,
                "debt_ratio": 0.35 + idx * 0.05,
                "operating_cashflow": 1_000_000_000 + idx * 100_000_000,
            }
        )
    return pd.DataFrame(rows)


def _st_history(trade_date: str) -> pd.DataFrame:
    _ = trade_date
    return pd.DataFrame(
        [
            {
                "stock_code": "000002.SZ",
                "st_type": "ST",
                "start_date": "2026-01-01",
                "end_date": None,
                "source": "mock",
            }
        ]
    )


def _benchmark_price(trade_date: str) -> pd.DataFrame:
    rows = []
    for idx, index_code in enumerate(["000300.SH", "000905.SH", "000906.SH"]):
        pre_close = 4000.0 + idx * 500
        close = round(pre_close * (1.002 + idx * 0.001), 2)
        rows.append(
            {
                "index_code": index_code,
                "trade_date": trade_date,
                "open": round(pre_close * 1.001, 2),
                "high": round(close * 1.01, 2),
                "low": round(pre_close * 0.99, 2),
                "close": close,
                "pct_chg": round((close / pre_close - 1) * 100, 4),
            }
        )
    return pd.DataFrame(rows)

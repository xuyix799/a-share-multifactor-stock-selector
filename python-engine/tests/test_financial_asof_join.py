import pandas as pd

from stock_selector.cleaning.asof_join import join_latest_financial


def test_financial_asof_join_selects_latest_announced_row():
    base = pd.DataFrame([{"stock_code": "000001.SZ", "trade_date": "2026-06-19"}])
    financial = pd.DataFrame(
        [
            {
                "stock_code": "000001.SZ",
                "report_period": "2025-12-31",
                "announce_date": "2026-04-30",
                "revenue_yoy": 0.01,
                "net_profit_yoy": 0.02,
                "roe": 0.03,
                "gross_margin": 0.2,
                "debt_ratio": 0.4,
                "operating_cashflow": 100.0,
            },
            {
                "stock_code": "000001.SZ",
                "report_period": "2026-03-31",
                "announce_date": "2026-06-18",
                "revenue_yoy": 0.11,
                "net_profit_yoy": 0.12,
                "roe": 0.13,
                "gross_margin": 0.3,
                "debt_ratio": 0.5,
                "operating_cashflow": 200.0,
            },
            {
                "stock_code": "000001.SZ",
                "report_period": "2026-06-30",
                "announce_date": "2026-06-20",
                "revenue_yoy": 9.0,
                "net_profit_yoy": 9.0,
                "roe": 9.0,
                "gross_margin": 9.0,
                "debt_ratio": 0.9,
                "operating_cashflow": 900.0,
            },
        ]
    )

    result = join_latest_financial(base, financial, "2026-06-19")

    assert result.iloc[0]["report_period"] == "2026-03-31"
    assert result.iloc[0]["announce_date"] == "2026-06-18"
    assert result.iloc[0]["revenue_yoy"] == 0.11


def test_financial_asof_join_keeps_stock_without_available_financial_data():
    base = pd.DataFrame([{"stock_code": "000001.SZ", "trade_date": "2026-06-19"}])
    financial = pd.DataFrame(
        [
            {
                "stock_code": "000001.SZ",
                "report_period": "2026-06-30",
                "announce_date": "2026-06-20",
                "revenue_yoy": 9.0,
                "net_profit_yoy": 9.0,
                "roe": 9.0,
                "gross_margin": 9.0,
                "debt_ratio": 0.9,
                "operating_cashflow": 900.0,
            }
        ]
    )

    result = join_latest_financial(base, financial, "2026-06-19")

    assert len(result) == 1
    assert pd.isna(result.iloc[0]["announce_date"])

import pandas as pd

from factor_test_helpers import factor_input_frame
from stock_selector.factors.factor_builder import build_factor_daily


def factor_daily_frame(trade_date: str = "2026-06-19") -> pd.DataFrame:
    factor_input = factor_input_frame(trade_date)
    rows = []
    score_sets = [
        (82.0, 76.0, 68.0, 72.0, 70.0),
        (60.0, 58.0, 55.0, 52.0, 54.0),
        (90.0, 88.0, 80.0, 78.0, 82.0),
    ]
    for row, scores in zip(factor_input.to_dict(orient="records"), score_sets, strict=True):
        quality, growth, valuation, trend, industry = scores
        rows.append(
            {
                "stock_code": row["stock_code"],
                "trade_date": trade_date,
                "industry": row["industry"],
                "market_type": row["market_type"],
                "quality_roe": row["roe"],
                "quality_gross_margin": row["gross_margin"],
                "quality_debt_ratio": row["debt_ratio"],
                "quality_cashflow_profit_ratio": None,
                "growth_revenue_yoy": row["revenue_yoy"],
                "growth_net_profit_yoy": row["net_profit_yoy"],
                "valuation_pe_ttm": row["pe_ttm"],
                "valuation_pb": row["pb"],
                "valuation_ps_ttm": row["ps_ttm"],
                "valuation_pe_percentile_3y": 0.30,
                "valuation_pb_percentile_3y": 0.30,
                "trend_ret_20d": 0.12,
                "trend_ret_60d": 0.20,
                "trend_ret_120d": 0.35,
                "trend_ma20": 20.0,
                "trend_ma60": 18.0,
                "trend_ma120": 16.0,
                "trend_price_ma60_ratio": 1.10,
                "industry_ret_60d": 0.10,
                "industry_ret_120d": 0.20,
                "industry_strength_60d": 0.03,
                "industry_strength_120d": 0.05,
                "liquidity_amount": row["amount"],
                "liquidity_turnover_rate": row["turnover_rate"],
                "quality_score": quality,
                "growth_score": growth,
                "valuation_score": valuation,
                "trend_score": trend,
                "industry_score": industry,
            }
        )
    return pd.DataFrame(rows)


def eligible_universe_frame(trade_date: str = "2026-06-19") -> pd.DataFrame:
    factor_input = factor_input_frame(trade_date)
    rows = []
    for row in factor_input.to_dict(orient="records"):
        if row["stock_code"] == "600000.SH":
            continue
        rows.append(
            {
                "stock_code": row["stock_code"],
                "trade_date": trade_date,
                "stock_name": row["stock_code"],
                "industry": row["industry"],
                "market_type": row["market_type"],
                "listed_days": 3000,
                "amount": row["amount"],
                "roe": row["roe"],
                "debt_ratio": row["debt_ratio"],
            }
        )
    return pd.DataFrame(rows)


def risk_filter_frame(trade_date: str = "2026-06-19") -> pd.DataFrame:
    factor_input = factor_input_frame(trade_date)
    rows = []
    for row in factor_input.to_dict(orient="records"):
        excluded = row["stock_code"] == "600000.SH"
        rows.append(
            {
                "stock_code": row["stock_code"],
                "trade_date": trade_date,
                "is_eligible": not excluded,
                "exclude_reasons": "AMOUNT_LT_MIN" if excluded else "",
                "risk_flags": "AMOUNT_LT_MIN" if excluded else "",
                "is_st_on_date": False,
                "is_paused": False,
                "listed_days": 3000,
                "amount": row["amount"],
                "roe": row["roe"],
                "debt_ratio": row["debt_ratio"],
                "report_period": "2026-03-31",
                "announce_date": "2026-05-20",
            }
        )
    result = pd.DataFrame(rows)
    result["is_eligible"] = result["is_eligible"].astype(object)
    result["is_st_on_date"] = result["is_st_on_date"].astype(object)
    result["is_paused"] = result["is_paused"].astype(object)
    return result


def built_factor_daily_from_goal6(trade_date: str = "2026-06-19") -> pd.DataFrame:
    from factor_test_helpers import adjusted_price_history, benchmark_price_history, clean_snapshot_history

    return build_factor_daily(
        factor_input_table=factor_input_frame(trade_date),
        adjusted_price_history=adjusted_price_history(trade_date, days=130),
        clean_snapshot_history=clean_snapshot_history(trade_date, days=5),
        benchmark_price_history=benchmark_price_history(trade_date, days=130),
        trade_date=trade_date,
        factor_weights={},
    )

import pandas as pd

from stock_selector.cleaning.snapshot_validator import validate_clean_daily_snapshot
from stock_selector.universe.universe_validator import RISK_FILTER_COLUMNS, validate_risk_filter
from stock_selector.utils.date_validator import validate_trade_date


MIN_LISTED_DAYS = 180
MIN_AMOUNT = 50_000_000
MIN_ROE = 0.03
MAX_DEBT_RATIO = 0.80

def build_risk_filter(clean_daily_snapshot: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    trade_date = validate_trade_date(trade_date)
    validate_clean_daily_snapshot(clean_daily_snapshot, trade_date)

    rows = []
    for row in clean_daily_snapshot.to_dict(orient="records"):
        reasons = _exclude_reasons(row)
        reason_text = ";".join(reasons)
        rows.append(
            {
                "stock_code": row["stock_code"],
                "trade_date": trade_date,
                "is_eligible": not reasons,
                "exclude_reasons": reason_text,
                "risk_flags": reason_text,
                "is_st_on_date": bool(row["is_st_on_date"]),
                "is_paused": bool(row["is_paused"]),
                "listed_days": row["listed_days"],
                "amount": row["amount"],
                "roe": row["roe"],
                "debt_ratio": row["debt_ratio"],
                "report_period": row["report_period"],
                "announce_date": row["announce_date"],
            }
        )

    result = pd.DataFrame(rows, columns=RISK_FILTER_COLUMNS)
    result["is_eligible"] = result["is_eligible"].astype(object)
    result["is_st_on_date"] = result["is_st_on_date"].astype(object)
    result["is_paused"] = result["is_paused"].astype(object)
    validate_risk_filter(result, trade_date)
    return result


def _exclude_reasons(row: dict) -> list[str]:
    reasons = []
    if bool(row["is_st_on_date"]):
        reasons.append("ST")
    if bool(row["is_paused"]):
        reasons.append("PAUSED")
    if row["listed_days"] < MIN_LISTED_DAYS:
        reasons.append("LISTED_DAYS_LT_MIN")
    if row["amount"] < MIN_AMOUNT:
        reasons.append("AMOUNT_LT_MIN")

    financial_missing = pd.isna(row["roe"]) or pd.isna(row["debt_ratio"])
    if financial_missing:
        reasons.append("FINANCIAL_MISSING")
        return reasons

    if row["roe"] < MIN_ROE:
        reasons.append("ROE_LT_MIN")
    if row["debt_ratio"] > MAX_DEBT_RATIO:
        reasons.append("DEBT_RATIO_GT_MAX")
    return reasons

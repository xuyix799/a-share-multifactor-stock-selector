import pandas as pd

from stock_selector.utils.date_validator import validate_trade_date


FINANCIAL_COLUMNS = [
    "report_period",
    "announce_date",
    "revenue_yoy",
    "net_profit_yoy",
    "roe",
    "gross_margin",
    "debt_ratio",
    "operating_cashflow",
]


def join_latest_financial(base: pd.DataFrame, financial: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    trade_date = validate_trade_date(trade_date)
    result = base.copy()
    required = ["stock_code", *FINANCIAL_COLUMNS]
    missing = [column for column in required if column not in financial.columns]
    if missing:
        raise ValueError(f"missing financial columns: {', '.join(missing)}")

    usable = financial.copy()
    usable["announce_date"] = usable["announce_date"].map(lambda value: None if pd.isna(value) else validate_trade_date(str(value)))
    usable["report_period"] = usable["report_period"].map(lambda value: None if pd.isna(value) else validate_trade_date(str(value)))
    usable = usable[(usable["announce_date"] <= trade_date) & (usable["report_period"] <= trade_date)]
    usable = usable.sort_values(["stock_code", "announce_date", "report_period"])
    latest = usable.groupby("stock_code", as_index=False).tail(1)[["stock_code", *FINANCIAL_COLUMNS]]

    joined = result.merge(latest, on="stock_code", how="left")
    for column in FINANCIAL_COLUMNS:
        if column not in joined.columns:
            joined[column] = pd.NA
    return joined

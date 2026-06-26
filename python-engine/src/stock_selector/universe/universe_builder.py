import pandas as pd

from stock_selector.cleaning.snapshot_validator import validate_clean_daily_snapshot
from stock_selector.universe.risk_filter import build_risk_filter
from stock_selector.universe.universe_validator import (
    ELIGIBLE_UNIVERSE_COLUMNS,
    FACTOR_INPUT_TABLE_COLUMNS,
    validate_eligible_universe,
    validate_factor_input_table,
    validate_risk_filter,
)
from stock_selector.utils.date_validator import validate_trade_date


def build_universe_tables(clean_daily_snapshot: pd.DataFrame, trade_date: str) -> dict[str, pd.DataFrame]:
    trade_date = validate_trade_date(trade_date)
    risk_filter = build_risk_filter(clean_daily_snapshot, trade_date)
    eligible_universe = build_eligible_universe(clean_daily_snapshot, risk_filter, trade_date)
    factor_input_table = build_factor_input_table(clean_daily_snapshot, eligible_universe, trade_date)
    return {
        "risk_filter": risk_filter,
        "eligible_universe": eligible_universe,
        "factor_input_table": factor_input_table,
    }


def build_eligible_universe(clean_daily_snapshot: pd.DataFrame, risk_filter: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    trade_date = validate_trade_date(trade_date)
    validate_clean_daily_snapshot(clean_daily_snapshot, trade_date)
    validate_risk_filter(risk_filter, trade_date)

    eligible_codes = set(risk_filter.loc[risk_filter["is_eligible"], "stock_code"].astype(str))
    result = clean_daily_snapshot.loc[clean_daily_snapshot["stock_code"].isin(eligible_codes), ELIGIBLE_UNIVERSE_COLUMNS].copy()
    validate_eligible_universe(result, trade_date)
    return result


def build_factor_input_table(clean_daily_snapshot: pd.DataFrame, eligible_universe: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    trade_date = validate_trade_date(trade_date)
    validate_clean_daily_snapshot(clean_daily_snapshot, trade_date)
    validate_eligible_universe(eligible_universe, trade_date)

    eligible_codes = set(eligible_universe["stock_code"].astype(str))
    result = clean_daily_snapshot.loc[clean_daily_snapshot["stock_code"].isin(eligible_codes), FACTOR_INPUT_TABLE_COLUMNS].copy()
    validate_factor_input_table(result, trade_date)
    return result

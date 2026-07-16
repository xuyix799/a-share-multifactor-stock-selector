from datetime import date

import pandas as pd

from stock_selector.cleaning.adjust_price import build_adjusted_price
from stock_selector.cleaning.asof_join import join_latest_financial
from stock_selector.cleaning.snapshot_validator import validate_clean_daily_snapshot
from stock_selector.cleaning.st_filter import mark_st_status
from stock_selector.data.data_validator import validate_dataset_frame
from stock_selector.providers.schema_contract import get_schema_contract
from stock_selector.utils.date_validator import validate_trade_date


CLEAN_DAILY_SNAPSHOT_COLUMNS = get_schema_contract("clean_daily_snapshot").columns


def build_clean_daily_snapshot(
    *,
    stock_basic: pd.DataFrame,
    daily_price: pd.DataFrame,
    adj_factor: pd.DataFrame,
    daily_basic: pd.DataFrame,
    financial: pd.DataFrame,
    st_history: pd.DataFrame,
    benchmark_price: pd.DataFrame,
    trade_date: str,
    adjusted_price: pd.DataFrame | None = None,
) -> pd.DataFrame:
    trade_date = validate_trade_date(trade_date)
    active_stock_basic, _membership_exclusions = filter_stock_basic_as_of(stock_basic, trade_date)
    validate_dataset_frame("daily_price", daily_price, trade_date)
    validate_dataset_frame("adj_factor", adj_factor, trade_date)
    validate_dataset_frame("daily_basic", daily_basic, trade_date)
    validate_dataset_frame("st_history", st_history, trade_date)
    validate_dataset_frame("benchmark_price", benchmark_price, trade_date)

    adjusted = adjusted_price.copy(deep=True) if adjusted_price is not None else build_adjusted_price(daily_price, adj_factor, trade_date)
    validate_dataset_frame("adjusted_price", adjusted, trade_date)
    snapshot = daily_price.merge(
        active_stock_basic[["stock_code", "stock_name", "industry", "market_type", "list_date"]],
        on="stock_code",
        how="inner",
    )
    snapshot = snapshot.merge(adjusted[["stock_code", "trade_date", "adj_open", "adj_high", "adj_low", "adj_close"]], on=["stock_code", "trade_date"], how="inner")
    snapshot = snapshot.merge(daily_basic, on=["stock_code", "trade_date"], how="left")
    snapshot = join_latest_financial(snapshot, financial, trade_date)
    snapshot = mark_st_status(snapshot, st_history, trade_date)
    snapshot["listed_days"] = snapshot["list_date"].map(lambda value: (date.fromisoformat(trade_date) - date.fromisoformat(str(value))).days)
    snapshot = snapshot.drop(columns=["list_date"])
    snapshot["is_paused"] = snapshot["is_paused"].map(bool).astype(object)
    snapshot["is_st_on_date"] = snapshot["is_st_on_date"].map(bool).astype(object)
    snapshot = snapshot[CLEAN_DAILY_SNAPSHOT_COLUMNS]
    validate_clean_daily_snapshot(snapshot, trade_date)
    return snapshot


def filter_stock_basic_as_of(stock_basic: pd.DataFrame, trade_date: str) -> tuple[pd.DataFrame, dict[str, int]]:
    """Return historically active stocks using [list_date, delist_date) membership."""

    trade_date = validate_trade_date(trade_date)
    validate_dataset_frame("stock_basic", stock_basic, trade_date)
    listed_dates = stock_basic["list_date"].map(lambda value: validate_trade_date(str(value)))
    delisted_dates = stock_basic["delist_date"].map(
        lambda value: None if pd.isna(value) else validate_trade_date(str(value))
    )
    not_yet_listed = listed_dates > trade_date
    already_delisted = delisted_dates.map(lambda value: value is not None and value <= trade_date)
    active = stock_basic.loc[~not_yet_listed & ~already_delisted].copy()
    exclusions = {
        reason: count
        for reason, count in (
            ("DELISTED", int(already_delisted.sum())),
            ("NOT_YET_LISTED", int(not_yet_listed.sum())),
        )
        if count
    }
    return active, exclusions

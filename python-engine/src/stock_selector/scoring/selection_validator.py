from collections.abc import Iterable

import pandas as pd

from stock_selector.data.data_validator import DataValidationError, validate_stock_code
from stock_selector.scoring.rule_explainer import FORBIDDEN_WORDS
from stock_selector.utils.date_validator import validate_trade_date


SELECTION_RESULT_COLUMNS = [
    "stock_code",
    "trade_date",
    "industry",
    "market_type",
    "quality_score",
    "growth_score",
    "valuation_score",
    "trend_score",
    "industry_score",
    "total_score",
    "risk_level",
    "rank",
    "suggestion",
    "reason",
    "exclude_reasons",
    "risk_flags",
]

SELECTION_SCORE_COLUMNS = ["quality_score", "growth_score", "valuation_score", "trend_score", "industry_score", "total_score"]


def validate_selection_result(df: pd.DataFrame, trade_date: str) -> None:
    trade_date = validate_trade_date(trade_date)
    if df.empty:
        raise DataValidationError("selection_result is empty")
    _require_columns(df, SELECTION_RESULT_COLUMNS)
    _validate_trade_date_column(df, trade_date)
    _validate_stock_codes(df)
    if df.duplicated(["stock_code", "trade_date"]).any():
        raise DataValidationError("duplicate rows by key: stock_code, trade_date")
    _validate_scores(df)
    _validate_risk_levels(df)
    _validate_rank(df)
    _validate_sorted(df)
    _validate_text_fields(df)


def _require_columns(df: pd.DataFrame, columns: Iterable[str]) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise DataValidationError(f"missing columns: {', '.join(missing)}")


def _validate_trade_date_column(df: pd.DataFrame, trade_date: str) -> None:
    for value in df["trade_date"].astype(str):
        if validate_trade_date(value) != trade_date:
            raise DataValidationError("trade_date column must equal requested trade_date")


def _validate_stock_codes(df: pd.DataFrame) -> None:
    for value in df["stock_code"].astype(str):
        validate_stock_code(value)


def _validate_scores(df: pd.DataFrame) -> None:
    for column in SELECTION_SCORE_COLUMNS:
        values = pd.to_numeric(df[column], errors="coerce")
        if values.isna().any() or (values < 0).any() or (values > 100).any():
            raise DataValidationError(f"{column} must be between 0 and 100")


def _validate_risk_levels(df: pd.DataFrame) -> None:
    allowed = {"low", "medium", "high"}
    invalid = sorted(set(df["risk_level"].astype(str)) - allowed)
    if invalid:
        raise DataValidationError(f"invalid risk_level: {', '.join(invalid)}")


def _validate_rank(df: pd.DataFrame) -> None:
    expected = list(range(1, len(df) + 1))
    actual = [int(value) for value in df["rank"]]
    if actual != expected:
        raise DataValidationError("rank must be continuous from 1")


def _validate_sorted(df: pd.DataFrame) -> None:
    scores = pd.to_numeric(df["total_score"], errors="coerce").tolist()
    if scores != sorted(scores, reverse=True):
        raise DataValidationError("selection_result must be sorted by total_score descending")


def _validate_text_fields(df: pd.DataFrame) -> None:
    for column in ["suggestion", "reason", "exclude_reasons", "risk_flags"]:
        if df[column].isna().any():
            raise DataValidationError(f"{column} must not be null")
    for column in ["suggestion", "reason"]:
        if (df[column].astype(str).str.strip() == "").any():
            raise DataValidationError(f"{column} must not be empty")
        for text in df[column].astype(str):
            found = [word for word in FORBIDDEN_WORDS if word in text]
            if found:
                raise DataValidationError(f"{column} contains forbidden words: {', '.join(found)}")

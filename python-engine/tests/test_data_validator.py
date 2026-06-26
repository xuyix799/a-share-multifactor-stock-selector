import pandas as pd
import pytest

from stock_selector.data.data_validator import DataValidationError, validate_dataset_frame
from stock_selector.data.mock_data import generate_mock_dataset


def test_validate_daily_price_accepts_mock_data():
    df = generate_mock_dataset("daily_price", "2026-06-19")

    validate_dataset_frame("daily_price", df, "2026-06-19")


def test_validate_daily_price_rejects_duplicate_stock_code_trade_date():
    df = generate_mock_dataset("daily_price", "2026-06-19")
    df = pd.concat([df, df.iloc[[0]]], ignore_index=True)

    with pytest.raises(DataValidationError):
        validate_dataset_frame("daily_price", df, "2026-06-19")


def test_validate_daily_price_rejects_non_positive_close():
    df = generate_mock_dataset("daily_price", "2026-06-19")
    df.loc[0, "close"] = 0

    with pytest.raises(DataValidationError):
        validate_dataset_frame("daily_price", df, "2026-06-19")


def test_validate_financial_rejects_future_announce_date():
    df = generate_mock_dataset("financial", "2026-06-19")
    df.loc[0, "announce_date"] = "2026-06-20"

    with pytest.raises(DataValidationError):
        validate_dataset_frame("financial", df, "2026-06-19")

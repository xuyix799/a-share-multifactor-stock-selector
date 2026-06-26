import pandas as pd
import pytest

from stock_selector.data.data_validator import DataValidationError
from stock_selector.providers.schema_mapper import (
    SchemaMappingError,
    map_provider_frame,
    normalize_date,
    normalize_stock_code,
)


def _raw_daily_price() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ts_code": "000001",
                "trade_date": "20260619",
                "open": "10.1",
                "high": "10.5",
                "low": "9.9",
                "close": "10.2",
                "pre_close": "10.0",
                "vol": "1000",
                "amount": "10200",
                "pct_chg": "2.0",
                "is_paused": 0,
                "up_limit": "11.0",
                "down_limit": "9.0",
            }
        ]
    )


def test_normalize_stock_code_accepts_common_provider_formats():
    assert normalize_stock_code("000001.SZ") == "000001.SZ"
    assert normalize_stock_code("600000.SH") == "600000.SH"
    assert normalize_stock_code("000001") == "000001.SZ"
    assert normalize_stock_code("600000") == "600000.SH"
    assert normalize_stock_code("SZ000001") == "000001.SZ"
    assert normalize_stock_code("SH600000") == "600000.SH"


def test_normalize_stock_code_rejects_invalid_format():
    with pytest.raises(SchemaMappingError):
        normalize_stock_code("BAD001")


def test_normalize_date_accepts_compact_and_iso_formats():
    assert normalize_date("20260619") == "2026-06-19"
    assert normalize_date("2026-06-19") == "2026-06-19"


def test_schema_mapper_maps_raw_daily_price_to_standard_schema_and_validates():
    mapped = map_provider_frame("mock", "daily_price", _raw_daily_price(), "2026-06-19")

    assert list(mapped.columns) == [
        "stock_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "volume",
        "amount",
        "pct_chg",
        "is_paused",
        "limit_up",
        "limit_down",
    ]
    assert mapped.iloc[0]["stock_code"] == "000001.SZ"
    assert mapped.iloc[0]["trade_date"] == "2026-06-19"
    assert mapped.iloc[0]["volume"] == 1000


def test_schema_mapper_rejects_missing_provider_field():
    raw = _raw_daily_price().drop(columns=["vol"])

    with pytest.raises(SchemaMappingError, match="missing provider fields"):
        map_provider_frame("mock", "daily_price", raw, "2026-06-19")


def test_schema_mapper_rejects_invalid_stock_code_before_storage():
    raw = _raw_daily_price()
    raw.loc[0, "ts_code"] = "99999"

    with pytest.raises(DataValidationError):
        map_provider_frame("mock", "daily_price", raw, "2026-06-19")


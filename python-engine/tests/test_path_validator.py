import pytest

from stock_selector.utils.path_validator import PathValidationError, safe_object_key


def test_safe_object_key_accepts_expected_partition_path():
    key = safe_object_key("raw/daily_price/trade_date=2026-06-19/part.parquet")

    assert key == "raw/daily_price/trade_date=2026-06-19/part.parquet"


@pytest.mark.parametrize(
    "key",
    [
        "../secret.env",
        "raw/../../secret.env",
        "/absolute/path.parquet",
        "raw\\daily_price\\part.parquet",
        "raw/daily_price/trade_date=2026-06-19/../part.parquet",
        "",
    ],
)
def test_safe_object_key_rejects_traversal_absolute_or_windows_paths(key):
    with pytest.raises(PathValidationError):
        safe_object_key(key)

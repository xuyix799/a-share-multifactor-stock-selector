import pytest

from stock_selector.utils.date_validator import DateValidationError, validate_trade_date


def test_validate_trade_date_accepts_strict_iso_date():
    assert validate_trade_date("2026-06-19") == "2026-06-19"


@pytest.mark.parametrize(
    "value",
    [
        "2026-6-19",
        "2026/06/19",
        "2026-02-30",
        "../bad-date",
        "2026-06-19/../../x",
        "",
    ],
)
def test_validate_trade_date_rejects_invalid_or_unsafe_values(value):
    with pytest.raises(DateValidationError):
        validate_trade_date(value)

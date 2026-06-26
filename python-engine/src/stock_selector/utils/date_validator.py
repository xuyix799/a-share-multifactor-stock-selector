from datetime import date
import re


class DateValidationError(ValueError):
    pass


_STRICT_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def validate_trade_date(value: str) -> str:
    if not isinstance(value, str) or not _STRICT_DATE.fullmatch(value):
        raise DateValidationError("date must match YYYY-MM-DD")

    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise DateValidationError("date is not a valid calendar date") from exc

    normalized = parsed.isoformat()
    if normalized != value:
        raise DateValidationError("date must be zero-padded YYYY-MM-DD")
    return normalized


def validate_date_range(start_date: str, end_date: str) -> tuple[str, str]:
    start = validate_trade_date(start_date)
    end = validate_trade_date(end_date)
    if start > end:
        raise DateValidationError("start_date must be before or equal to end_date")
    return start, end

from dataclasses import dataclass

from stock_selector.utils.date_validator import validate_date_range, validate_trade_date


VALID_REBALANCE_MODES = {"monthly", "quarterly"}


@dataclass(frozen=True)
class RebalanceEvent:
    signal_date: str
    execution_date: str


def build_rebalance_schedule(
    *,
    trade_dates: list[str],
    start_date: str,
    end_date: str,
    rebalance_mode: str,
) -> list[RebalanceEvent]:
    start_date, end_date = validate_date_range(start_date, end_date)
    if rebalance_mode not in VALID_REBALANCE_MODES:
        raise ValueError(f"unsupported rebalance_mode: {rebalance_mode}")

    dates = sorted({validate_trade_date(value) for value in trade_dates if start_date <= validate_trade_date(value) <= end_date})
    if len(dates) < 2:
        return []

    signal_dates = _last_trade_date_by_period(dates, rebalance_mode)
    events = []
    for signal_date in signal_dates:
        execution_date = _next_trade_date(dates, signal_date)
        if execution_date is None or execution_date > end_date:
            continue
        events.append(RebalanceEvent(signal_date=signal_date, execution_date=execution_date))
    return events


def _last_trade_date_by_period(trade_dates: list[str], rebalance_mode: str) -> list[str]:
    by_period: dict[tuple[int, int], str] = {}
    for trade_date in trade_dates:
        year, month = _year_month(trade_date)
        period = (year, month) if rebalance_mode == "monthly" else (year, (month - 1) // 3 + 1)
        by_period[period] = trade_date
    return list(by_period.values())


def _next_trade_date(trade_dates: list[str], signal_date: str) -> str | None:
    for trade_date in trade_dates:
        if trade_date > signal_date:
            return trade_date
    return None


def _year_month(trade_date: str) -> tuple[int, int]:
    return int(trade_date[:4]), int(trade_date[5:7])

from collections.abc import Callable
from typing import Any

import pandas as pd

from stock_selector.cleaning.clean_pipeline import _default_read_dataset, _default_write_dataset, read_dataset_history
from stock_selector.config.config_loader import load_factor_weights
from stock_selector.data.update_log import UpdateLogRepository, create_update_log_repository
from stock_selector.factors.factor_builder import build_factor_daily
from stock_selector.factors.factor_validator import validate_factor_daily
from stock_selector.utils.date_validator import validate_trade_date


ReadDatasetFn = Callable[[str, str], pd.DataFrame]
ReadHistoryFn = Callable[[str, str], pd.DataFrame]
WriteDatasetFn = Callable[[str, str, pd.DataFrame], str]

FACTOR_DAILY_STEP_NAME = "factors:factor_daily"


def build_factor_daily_for_date(
    trade_date: str,
    force: bool = False,
    update_log_repo: UpdateLogRepository | None = None,
    read_dataset_fn: ReadDatasetFn | None = None,
    read_history_fn: ReadHistoryFn | None = None,
    write_dataset_fn: WriteDatasetFn | None = None,
) -> dict[str, Any]:
    trade_date = validate_trade_date(trade_date)
    repo = update_log_repo or create_update_log_repository()
    if not repo.should_run_step(trade_date, FACTOR_DAILY_STEP_NAME, force=force):
        return {"dataset": "factor_daily", "step_name": FACTOR_DAILY_STEP_NAME, "status": "skipped"}

    reader = read_dataset_fn or _default_read_dataset
    history_reader = read_history_fn or read_dataset_history
    writer = write_dataset_fn or _default_write_dataset
    try:
        repo.mark_step_running(trade_date, FACTOR_DAILY_STEP_NAME)
        factor_daily = build_factor_daily(
            factor_input_table=reader("factor_input_table", trade_date),
            adjusted_price_history=history_reader("adjusted_price", trade_date),
            clean_snapshot_history=history_reader("clean_daily_snapshot", trade_date),
            benchmark_price_history=history_reader("benchmark_price", trade_date),
            trade_date=trade_date,
            factor_weights=load_factor_weights(),
        )
        validate_factor_daily(factor_daily, trade_date)
        object_key = writer("factor_daily", trade_date, factor_daily)
        repo.mark_step_done(trade_date, FACTOR_DAILY_STEP_NAME, object_key)
        return {"dataset": "factor_daily", "step_name": FACTOR_DAILY_STEP_NAME, "status": "done", "object_key": object_key, "row_count": len(factor_daily)}
    except Exception as exc:
        repo.mark_step_failed(trade_date, FACTOR_DAILY_STEP_NAME, str(exc))
        raise

from collections.abc import Callable
import json
from typing import Any

import pandas as pd

from stock_selector.cleaning.clean_pipeline import _default_read_dataset, _default_write_dataset
from stock_selector.data.update_log import UpdateLogRepository, create_update_log_repository
from stock_selector.universe.universe_builder import build_universe_tables
from stock_selector.utils.date_validator import validate_trade_date


ReadDatasetFn = Callable[[str, str], pd.DataFrame]
WriteDatasetFn = Callable[[str, str, pd.DataFrame], str]

UNIVERSE_INPUTS_STEP_NAME = "universe:inputs"
UNIVERSE_INPUT_DATASETS = ("risk_filter", "eligible_universe", "factor_input_table")


def build_universe_inputs_for_date(
    trade_date: str,
    force: bool = False,
    update_log_repo: UpdateLogRepository | None = None,
    read_dataset_fn: ReadDatasetFn | None = None,
    write_dataset_fn: WriteDatasetFn | None = None,
) -> dict[str, Any]:
    trade_date = validate_trade_date(trade_date)
    repo = update_log_repo or create_update_log_repository()
    if not repo.should_run_step(trade_date, UNIVERSE_INPUTS_STEP_NAME, force=force):
        return {"datasets": list(UNIVERSE_INPUT_DATASETS), "step_name": UNIVERSE_INPUTS_STEP_NAME, "status": "skipped"}

    reader = read_dataset_fn or _default_read_dataset
    writer = write_dataset_fn or _default_write_dataset
    try:
        repo.mark_step_running(trade_date, UNIVERSE_INPUTS_STEP_NAME)
        clean_daily_snapshot = reader("clean_daily_snapshot", trade_date)
        tables = build_universe_tables(clean_daily_snapshot, trade_date)
        object_keys = {}
        row_counts = {}
        for dataset in UNIVERSE_INPUT_DATASETS:
            df = tables[dataset]
            object_keys[dataset] = writer(dataset, trade_date, df)
            row_counts[dataset] = len(df)

        repo.mark_step_done(trade_date, UNIVERSE_INPUTS_STEP_NAME, json.dumps(object_keys, ensure_ascii=False, sort_keys=True))
        return {
            "datasets": list(UNIVERSE_INPUT_DATASETS),
            "step_name": UNIVERSE_INPUTS_STEP_NAME,
            "status": "done",
            "object_keys": object_keys,
            "row_counts": row_counts,
        }
    except Exception as exc:
        repo.mark_step_failed(trade_date, UNIVERSE_INPUTS_STEP_NAME, str(exc))
        raise

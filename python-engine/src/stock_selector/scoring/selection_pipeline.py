from collections.abc import Callable
from typing import Any

import pandas as pd

from stock_selector.cleaning.clean_pipeline import _default_read_dataset, _default_write_dataset
from stock_selector.config.config_loader import load_factor_weights_config
from stock_selector.data.update_log import UpdateLogRepository, create_update_log_repository
from stock_selector.scoring.score_engine import parse_scoring_config
from stock_selector.scoring.selection_builder import build_selection_result
from stock_selector.scoring.selection_snapshot_repo import (
    SelectionSnapshotRepository,
    create_selection_snapshot_repository,
    summarize_selection_result,
)
from stock_selector.scoring.selection_validator import validate_selection_result
from stock_selector.utils.date_validator import validate_trade_date


ReadDatasetFn = Callable[[str, str], pd.DataFrame | None]
WriteDatasetFn = Callable[[str, str, pd.DataFrame], str]

SELECTION_RESULT_STEP_NAME = "scoring:selection_result"


def build_selection_for_date(
    trade_date: str,
    force: bool = False,
    update_log_repo: UpdateLogRepository | None = None,
    snapshot_repo: SelectionSnapshotRepository | None = None,
    read_dataset_fn: ReadDatasetFn | None = None,
    write_dataset_fn: WriteDatasetFn | None = None,
    scoring_config_raw: dict[str, Any] | None = None,
) -> dict[str, Any]:
    trade_date = validate_trade_date(trade_date)
    repo = update_log_repo or create_update_log_repository()
    if not repo.should_run_step(trade_date, SELECTION_RESULT_STEP_NAME, force=force):
        return {"dataset": "selection_result", "step_name": SELECTION_RESULT_STEP_NAME, "status": "skipped"}

    reader = read_dataset_fn or _default_read_dataset
    writer = write_dataset_fn or _default_write_dataset
    try:
        repo.mark_step_running(trade_date, SELECTION_RESULT_STEP_NAME)
        scoring_config = parse_scoring_config(scoring_config_raw or load_factor_weights_config())
        selection_result = build_selection_result(
            factor_daily=reader("factor_daily", trade_date),
            risk_filter=reader("risk_filter", trade_date),
            eligible_universe=reader("eligible_universe", trade_date),
            factor_input_table=reader("factor_input_table", trade_date),
            trade_date=trade_date,
            scoring_config=scoring_config,
        )
        validate_selection_result(selection_result, trade_date)
        object_key = writer("selection_result", trade_date, selection_result)
        summary = summarize_selection_result(selection_result, trade_date=trade_date, top_n=scoring_config.top_n, object_key=object_key)
        snapshots = snapshot_repo or create_selection_snapshot_repository()
        snapshots.upsert_snapshot(summary)
        repo.mark_step_done(trade_date, SELECTION_RESULT_STEP_NAME, object_key)
        return {
            "dataset": "selection_result",
            "step_name": SELECTION_RESULT_STEP_NAME,
            "status": "done",
            "object_key": object_key,
            "row_count": len(selection_result),
            "snapshot": summary,
        }
    except Exception as exc:
        repo.mark_step_failed(trade_date, SELECTION_RESULT_STEP_NAME, str(exc))
        raise

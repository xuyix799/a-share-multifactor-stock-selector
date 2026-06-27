import json

from factor_test_helpers import factor_input_frame
from selection_test_helpers import eligible_universe_frame, factor_daily_frame, risk_filter_frame
from stock_selector.cli import main
from stock_selector.scoring.selection_pipeline import build_selection_for_date
from stock_selector.storage.duckdb_query import query_dataset_file


class FakeUpdateLogRepository:
    def __init__(self):
        self.done_steps = set()

    def should_run_step(self, trade_date, step_name, force=False):
        return force or (trade_date, step_name) not in self.done_steps

    def mark_step_running(self, trade_date, step_name):
        pass

    def mark_step_done(self, trade_date, step_name, object_key):
        self.done_steps.add((trade_date, step_name))

    def mark_step_failed(self, trade_date, step_name, error_message):
        pass


class FakeSnapshotRepository:
    def __init__(self):
        self.snapshots = []

    def upsert_snapshot(self, summary):
        self.snapshots.append(summary)


def test_duckdb_can_query_selection_result(tmp_path, monkeypatch, capsys):
    trade_date = "2026-06-19"
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))

    for dataset, df in {
        "factor_daily": factor_daily_frame(trade_date),
        "risk_filter": risk_filter_frame(trade_date),
        "eligible_universe": eligible_universe_frame(trade_date),
        "factor_input_table": factor_input_frame(trade_date),
    }.items():
        path = tmp_path / "raw" / dataset / f"trade_date={trade_date}" / "part.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False)

    result = build_selection_for_date(
        trade_date,
        update_log_repo=FakeUpdateLogRepository(),
        snapshot_repo=FakeSnapshotRepository(),
    )

    assert result["status"] == "done"
    output_path = tmp_path / "raw" / "selection_result" / f"trade_date={trade_date}" / "part.parquet"
    rows = query_dataset_file(output_path)
    assert rows
    assert "total_score" in rows[0]

    exit_code = main(["query-parquet", "--dataset", "selection_result", "--trade-date", trade_date])
    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert output["dataset"] == "selection_result"
    assert output["row_count"] > 0

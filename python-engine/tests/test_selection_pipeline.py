from selection_test_helpers import eligible_universe_frame, factor_daily_frame, risk_filter_frame
from stock_selector.scoring.selection_pipeline import SELECTION_RESULT_STEP_NAME, build_selection_for_date


class FakeUpdateLogRepository:
    def __init__(self):
        self.done_steps = set()
        self.running_steps = []
        self.done_marks = []
        self.failed_marks = []

    def should_run_step(self, trade_date, step_name, force=False):
        return force or (trade_date, step_name) not in self.done_steps

    def mark_step_running(self, trade_date, step_name):
        self.running_steps.append((trade_date, step_name))

    def mark_step_done(self, trade_date, step_name, object_key):
        self.done_steps.add((trade_date, step_name))
        self.done_marks.append((trade_date, step_name, object_key))

    def mark_step_failed(self, trade_date, step_name, error_message):
        self.failed_marks.append((trade_date, step_name, error_message))


class FakeSnapshotRepository:
    def __init__(self):
        self.snapshots = []

    def upsert_snapshot(self, summary):
        self.snapshots.append(summary)


def test_build_selection_for_date_is_idempotent_and_force_reruns():
    trade_date = "2026-06-19"
    update_repo = FakeUpdateLogRepository()
    snapshot_repo = FakeSnapshotRepository()
    reads = []
    writes = []

    def read_dataset(dataset, requested_date):
        reads.append((dataset, requested_date))
        if dataset == "factor_daily":
            return factor_daily_frame(requested_date)
        if dataset == "risk_filter":
            return risk_filter_frame(requested_date)
        if dataset == "eligible_universe":
            return eligible_universe_frame(requested_date)
        if dataset == "factor_input_table":
            return None
        raise AssertionError(dataset)

    def write_dataset(dataset, requested_date, df):
        writes.append((dataset, requested_date, len(df), tuple(df.columns)))
        return f"raw/{dataset}/trade_date={requested_date}/part.parquet"

    first = build_selection_for_date(
        trade_date,
        update_log_repo=update_repo,
        snapshot_repo=snapshot_repo,
        read_dataset_fn=read_dataset,
        write_dataset_fn=write_dataset,
    )
    skipped = build_selection_for_date(
        trade_date,
        update_log_repo=update_repo,
        snapshot_repo=snapshot_repo,
        read_dataset_fn=read_dataset,
        write_dataset_fn=write_dataset,
    )
    forced = build_selection_for_date(
        trade_date,
        force=True,
        update_log_repo=update_repo,
        snapshot_repo=snapshot_repo,
        read_dataset_fn=read_dataset,
        write_dataset_fn=write_dataset,
    )

    assert first["status"] == "done"
    assert skipped["status"] == "skipped"
    assert forced["status"] == "done"
    assert [item[0] for item in writes] == ["selection_result", "selection_result"]
    assert len(snapshot_repo.snapshots) == 2
    assert snapshot_repo.snapshots[-1]["object_key"] == f"raw/selection_result/trade_date={trade_date}/part.parquet"
    assert update_repo.done_marks[-1][1] == SELECTION_RESULT_STEP_NAME
    assert update_repo.done_marks[-1][2] == snapshot_repo.snapshots[-1]["object_key"]
    assert reads.count(("factor_daily", trade_date)) == 2


def test_build_selection_for_date_marks_failed_on_error():
    trade_date = "2026-06-19"
    update_repo = FakeUpdateLogRepository()

    def read_dataset(dataset, requested_date):
        raise RuntimeError(f"missing {dataset} {requested_date}")

    try:
        build_selection_for_date(trade_date, update_log_repo=update_repo, read_dataset_fn=read_dataset, write_dataset_fn=lambda *args: "unused")
    except RuntimeError:
        pass

    assert update_repo.failed_marks
    assert update_repo.failed_marks[-1][1] == SELECTION_RESULT_STEP_NAME

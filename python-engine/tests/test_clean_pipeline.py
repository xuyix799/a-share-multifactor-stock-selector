from stock_selector.cleaning.clean_pipeline import build_adjusted_price_for_date, build_clean_snapshot_for_date, read_dataset_history
from stock_selector.data.mock_data import generate_mock_dataset


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


def test_build_adjusted_price_for_date_is_idempotent_and_force_reruns():
    repo = FakeUpdateLogRepository()
    writes = []

    def read_dataset(dataset, trade_date):
        return generate_mock_dataset(dataset, trade_date)

    def write_dataset(dataset, trade_date, df):
        writes.append((dataset, trade_date, len(df)))
        return f"raw/{dataset}/trade_date={trade_date}/part.parquet"

    first = build_adjusted_price_for_date("2026-06-19", update_log_repo=repo, read_dataset_fn=read_dataset, write_dataset_fn=write_dataset)
    skipped = build_adjusted_price_for_date("2026-06-19", update_log_repo=repo, read_dataset_fn=read_dataset, write_dataset_fn=write_dataset)
    forced = build_adjusted_price_for_date("2026-06-19", force=True, update_log_repo=repo, read_dataset_fn=read_dataset, write_dataset_fn=write_dataset)

    assert first["status"] == "done"
    assert skipped["status"] == "skipped"
    assert forced["status"] == "done"
    assert [item[0] for item in writes] == ["adjusted_price", "adjusted_price"]


def test_build_clean_snapshot_for_date_is_idempotent_and_force_reruns():
    repo = FakeUpdateLogRepository()
    writes = []

    def read_dataset(dataset, trade_date):
        return generate_mock_dataset(dataset, trade_date)

    def write_dataset(dataset, trade_date, df):
        writes.append((dataset, trade_date, tuple(df.columns)))
        return f"raw/{dataset}/trade_date={trade_date}/part.parquet"

    first = build_clean_snapshot_for_date("2026-06-19", update_log_repo=repo, read_dataset_fn=read_dataset, write_dataset_fn=write_dataset)
    skipped = build_clean_snapshot_for_date("2026-06-19", update_log_repo=repo, read_dataset_fn=read_dataset, write_dataset_fn=write_dataset)
    forced = build_clean_snapshot_for_date("2026-06-19", force=True, update_log_repo=repo, read_dataset_fn=read_dataset, write_dataset_fn=write_dataset)

    assert first["status"] == "done"
    assert skipped["status"] == "skipped"
    assert forced["status"] == "done"
    assert [item[0] for item in writes] == ["clean_daily_snapshot", "clean_daily_snapshot"]


def test_read_dataset_history_reads_local_partitions_no_later_than_trade_date(tmp_path, monkeypatch):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    for day in ["2026-06-18", "2026-06-19", "2026-06-20"]:
        path = tmp_path / "raw" / "financial" / f"trade_date={day}" / "part.parquet"
        path.parent.mkdir(parents=True)
        generate_mock_dataset("financial", day).to_parquet(path, index=False)

    result = read_dataset_history("financial", "2026-06-19")

    assert len(result) == 10
    assert set(result["announce_date"]) == {"2026-05-19", "2026-05-20"}

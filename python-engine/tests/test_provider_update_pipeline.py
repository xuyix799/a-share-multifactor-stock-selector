from stock_selector.data.update_pipeline import update_provider_data
from stock_selector.storage.partition import PROVIDER_DATASETS


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


def test_update_provider_data_writes_all_mapped_datasets_and_marks_done():
    repo = FakeUpdateLogRepository()
    writes = []

    def write_dataset(dataset, trade_date, df):
        writes.append((dataset, trade_date, tuple(df.columns)))
        return f"raw/{dataset}/trade_date={trade_date}/part.parquet"

    result = update_provider_data(
        "2026-06-19",
        provider_name="mock",
        update_log_repo=repo,
        write_dataset_fn=write_dataset,
    )

    assert [item["status"] for item in result] == ["done"] * len(PROVIDER_DATASETS)
    assert [item[0] for item in writes] == list(PROVIDER_DATASETS)
    assert len(repo.done_marks) == len(PROVIDER_DATASETS)
    assert repo.failed_marks == []


def test_update_provider_data_skips_done_steps_by_default_and_force_reruns():
    repo = FakeUpdateLogRepository()
    writes = []

    def write_dataset(dataset, trade_date, df):
        writes.append((dataset, trade_date))
        return f"raw/{dataset}/trade_date={trade_date}/part.parquet"

    update_provider_data("2026-06-19", "mock", update_log_repo=repo, write_dataset_fn=write_dataset)
    skipped = update_provider_data("2026-06-19", "mock", update_log_repo=repo, write_dataset_fn=write_dataset)
    forced = update_provider_data("2026-06-19", "mock", force=True, update_log_repo=repo, write_dataset_fn=write_dataset)

    assert [item["status"] for item in skipped] == ["skipped"] * len(PROVIDER_DATASETS)
    assert [item["status"] for item in forced] == ["done"] * len(PROVIDER_DATASETS)
    assert len(writes) == len(PROVIDER_DATASETS) * 2


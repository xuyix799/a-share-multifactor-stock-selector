import pandas as pd
import pytest

from stock_selector.data.update_pipeline import update_provider_data
from stock_selector.storage.partition import DatasetValidationError, PROVIDER_DATASETS


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


def test_update_provider_data_can_limit_datasets_for_goal10_smoke():
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
        datasets=["stock_basic", "daily_basic"],
    )

    assert [item["dataset"] for item in result] == ["stock_basic", "daily_basic"]
    assert [item["status"] for item in result] == ["done", "done"]
    assert [item[0] for item in writes] == ["stock_basic", "daily_basic"]
    assert [item[1] for item in repo.done_marks] == ["provider_data:stock_basic", "provider_data:daily_basic"]


def test_update_provider_data_rejects_non_provider_dataset_selection():
    with pytest.raises(DatasetValidationError, match="unsupported provider dataset"):
        update_provider_data("2026-06-19", provider_name="mock", datasets=["factor_daily"])


def test_update_provider_data_rejects_raw_smoke_dataset_without_smoke_allowlist():
    with pytest.raises(DatasetValidationError, match="unsupported provider dataset"):
        update_provider_data("2026-06-19", provider_name="mock", datasets=["daily_price_raw_smoke"])


def test_update_provider_data_can_use_smoke_step_prefix_without_marking_standard_provider_step():
    repo = FakeUpdateLogRepository()

    def write_dataset(dataset, trade_date, df):
        return f"smoke/tushare/{dataset}/trade_date={trade_date}/part.parquet"

    result = update_provider_data(
        "2026-06-19",
        provider_name="mock",
        update_log_repo=repo,
        write_dataset_fn=write_dataset,
        datasets=["stock_basic"],
        step_prefix="provider_smoke:tushare",
    )

    assert result == [
        {
            "dataset": "stock_basic",
            "step_name": "provider_smoke:tushare:stock_basic",
            "status": "done",
            "object_key": "smoke/tushare/stock_basic/trade_date=2026-06-19/part.parquet",
        }
    ]
    assert repo.done_marks[0][1] == "provider_smoke:tushare:stock_basic"
    assert ("2026-06-19", "provider_data:stock_basic") not in repo.done_steps


def test_update_provider_data_writes_raw_smoke_without_standard_schema_mapping(monkeypatch):
    repo = FakeUpdateLogRepository()
    writes = []

    class FakeProvider:
        name = "akshare"

        def fetch_dataset(self, dataset, trade_date):
            assert dataset == "daily_price_raw_smoke"
            return pd.DataFrame(
                [
                    {
                        "stock_code": "000001.SZ",
                        "trade_date": trade_date,
                        "open": 10.0,
                        "close": 10.2,
                    }
                ]
            )

    monkeypatch.setattr("stock_selector.data.update_pipeline.create_provider", lambda provider_name, settings=None: FakeProvider())

    def write_dataset(dataset, trade_date, df):
        writes.append((dataset, trade_date, tuple(df.columns)))
        return f"smoke/akshare/{dataset}/trade_date={trade_date}/part.parquet"

    result = update_provider_data(
        "2026-06-19",
        provider_name="akshare",
        update_log_repo=repo,
        write_dataset_fn=write_dataset,
        datasets=["daily_price_raw_smoke"],
        step_prefix="provider_smoke:akshare",
        allow_smoke_datasets=True,
    )

    assert result == [
        {
            "dataset": "daily_price_raw_smoke",
            "step_name": "provider_smoke:akshare:daily_price_raw_smoke",
            "status": "done",
            "object_key": "smoke/akshare/daily_price_raw_smoke/trade_date=2026-06-19/part.parquet",
        }
    ]
    assert writes == [("daily_price_raw_smoke", "2026-06-19", ("stock_code", "trade_date", "open", "close"))]
    assert repo.failed_marks == []


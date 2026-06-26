import json

from factor_test_helpers import adjusted_price_history, benchmark_price_history, clean_snapshot_history, factor_input_frame
from stock_selector.cli import main
from stock_selector.factors.factor_pipeline import build_factor_daily_for_date
from stock_selector.storage.duckdb_query import query_dataset_file


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


def test_build_factor_daily_for_date_is_idempotent_and_force_reruns():
    trade_date = "2026-06-19"
    repo = FakeUpdateLogRepository()
    writes = []
    history_reads = []

    def read_dataset(dataset, requested_date):
        assert dataset == "factor_input_table"
        return factor_input_frame(requested_date)

    def read_history(dataset, requested_date):
        history_reads.append((dataset, requested_date))
        if dataset == "adjusted_price":
            return adjusted_price_history(requested_date, days=130)
        if dataset == "clean_daily_snapshot":
            return clean_snapshot_history(requested_date, days=5)
        if dataset == "benchmark_price":
            return benchmark_price_history(requested_date, days=130)
        raise AssertionError(dataset)

    def write_dataset(dataset, requested_date, df):
        writes.append((dataset, requested_date, len(df), tuple(df.columns)))
        return f"raw/{dataset}/trade_date={requested_date}/part.parquet"

    first = build_factor_daily_for_date(trade_date, update_log_repo=repo, read_dataset_fn=read_dataset, read_history_fn=read_history, write_dataset_fn=write_dataset)
    skipped = build_factor_daily_for_date(trade_date, update_log_repo=repo, read_dataset_fn=read_dataset, read_history_fn=read_history, write_dataset_fn=write_dataset)
    forced = build_factor_daily_for_date(
        trade_date,
        force=True,
        update_log_repo=repo,
        read_dataset_fn=read_dataset,
        read_history_fn=read_history,
        write_dataset_fn=write_dataset,
    )

    assert first["status"] == "done"
    assert skipped["status"] == "skipped"
    assert forced["status"] == "done"
    assert [item[0] for item in writes] == ["factor_daily", "factor_daily"]
    assert history_reads == [
        ("adjusted_price", trade_date),
        ("clean_daily_snapshot", trade_date),
        ("benchmark_price", trade_date),
        ("adjusted_price", trade_date),
        ("clean_daily_snapshot", trade_date),
        ("benchmark_price", trade_date),
    ]
    assert repo.done_marks[-1][1] == "factors:factor_daily"


def test_build_factor_daily_for_date_writes_local_parquet_and_query_cli_reads_it(tmp_path, monkeypatch, capsys):
    trade_date = "2026-06-19"
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))

    for dataset, df in {
        "factor_input_table": factor_input_frame(trade_date),
        "adjusted_price": adjusted_price_history(trade_date, days=130),
        "clean_daily_snapshot": clean_snapshot_history(trade_date, days=5),
        "benchmark_price": benchmark_price_history(trade_date, days=130),
    }.items():
        for partition_date, part in df.groupby("trade_date"):
            path = tmp_path / "raw" / dataset / f"trade_date={partition_date}" / "part.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            part.to_parquet(path, index=False)

    result = build_factor_daily_for_date(trade_date, update_log_repo=FakeUpdateLogRepository())

    assert result["status"] == "done"
    output_path = tmp_path / "raw" / "factor_daily" / f"trade_date={trade_date}" / "part.parquet"
    assert output_path.exists()
    assert query_dataset_file(output_path)

    exit_code = main(["query-parquet", "--dataset", "factor_daily", "--trade-date", trade_date])
    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert output["dataset"] == "factor_daily"
    assert output["row_count"] > 0

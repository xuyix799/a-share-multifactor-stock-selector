import json

from stock_selector.cleaning.snapshot_builder import build_clean_daily_snapshot
from stock_selector.cli import main
from stock_selector.data.mock_data import generate_mock_dataset
from stock_selector.storage.duckdb_query import query_dataset_file
from stock_selector.universe.universe_pipeline import build_universe_inputs_for_date


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


def _clean_snapshot(trade_date: str):
    return build_clean_daily_snapshot(
        stock_basic=generate_mock_dataset("stock_basic", trade_date),
        daily_price=generate_mock_dataset("daily_price", trade_date),
        adj_factor=generate_mock_dataset("adj_factor", trade_date),
        daily_basic=generate_mock_dataset("daily_basic", trade_date),
        financial=generate_mock_dataset("financial", trade_date),
        st_history=generate_mock_dataset("st_history", trade_date),
        benchmark_price=generate_mock_dataset("benchmark_price", trade_date),
        trade_date=trade_date,
    )


def test_build_universe_inputs_for_date_is_idempotent_and_force_reruns():
    trade_date = "2026-06-19"
    repo = FakeUpdateLogRepository()
    reads = []
    writes = []

    def read_dataset(dataset, requested_date):
        reads.append((dataset, requested_date))
        assert dataset == "clean_daily_snapshot"
        return _clean_snapshot(requested_date)

    def write_dataset(dataset, requested_date, df):
        writes.append((dataset, requested_date, len(df), tuple(df.columns)))
        return f"raw/{dataset}/trade_date={requested_date}/part.parquet"

    first = build_universe_inputs_for_date(trade_date, update_log_repo=repo, read_dataset_fn=read_dataset, write_dataset_fn=write_dataset)
    skipped = build_universe_inputs_for_date(trade_date, update_log_repo=repo, read_dataset_fn=read_dataset, write_dataset_fn=write_dataset)
    forced = build_universe_inputs_for_date(
        trade_date,
        force=True,
        update_log_repo=repo,
        read_dataset_fn=read_dataset,
        write_dataset_fn=write_dataset,
    )

    assert first["status"] == "done"
    assert skipped["status"] == "skipped"
    assert forced["status"] == "done"
    assert [item[0] for item in writes] == [
        "risk_filter",
        "eligible_universe",
        "factor_input_table",
        "risk_filter",
        "eligible_universe",
        "factor_input_table",
    ]
    assert reads == [("clean_daily_snapshot", trade_date), ("clean_daily_snapshot", trade_date)]
    assert repo.done_marks[-1][1] == "universe:inputs"


def test_build_universe_inputs_for_date_writes_local_parquet_outputs(tmp_path, monkeypatch):
    trade_date = "2026-06-19"
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    input_path = tmp_path / "raw" / "clean_daily_snapshot" / f"trade_date={trade_date}" / "part.parquet"
    input_path.parent.mkdir(parents=True)
    _clean_snapshot(trade_date).to_parquet(input_path, index=False)

    result = build_universe_inputs_for_date(trade_date, update_log_repo=FakeUpdateLogRepository())

    assert result["status"] == "done"
    for dataset in ["risk_filter", "eligible_universe", "factor_input_table"]:
        output_path = tmp_path / "raw" / dataset / f"trade_date={trade_date}" / "part.parquet"
        assert output_path.exists()
        rows = query_dataset_file(output_path)
        assert rows
        assert str(rows[0]["trade_date"]).startswith(trade_date)


def test_query_parquet_cli_can_read_goal5_local_outputs(tmp_path, monkeypatch, capsys):
    trade_date = "2026-06-19"
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    input_path = tmp_path / "raw" / "clean_daily_snapshot" / f"trade_date={trade_date}" / "part.parquet"
    input_path.parent.mkdir(parents=True)
    _clean_snapshot(trade_date).to_parquet(input_path, index=False)
    build_universe_inputs_for_date(trade_date, update_log_repo=FakeUpdateLogRepository())

    for dataset in ["risk_filter", "eligible_universe", "factor_input_table"]:
        exit_code = main(["query-parquet", "--dataset", dataset, "--trade-date", trade_date])
        output = json.loads(capsys.readouterr().out)
        assert exit_code == 0
        assert output["dataset"] == dataset
        assert output["row_count"] > 0

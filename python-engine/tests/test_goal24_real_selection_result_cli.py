from copy import deepcopy
from datetime import date, datetime
import json

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from goal24_test_helpers import (
    END_DATE,
    fresh_goal24_stores,
)
from stock_selector import cli
from stock_selector.data.data_validator import DataValidationError
from stock_selector.scoring.real_selection_result import (
    SELECTION_FLOAT_COLUMNS,
    SELECTION_TEXT_COLUMNS,
    read_goal24_published_selection_result,
)


def _args(stores, *extra, run_id="goal24-cli-test"):
    return [
        "run-real-selection-range",
        "--run-id",
        run_id,
        "--start-date",
        END_DATE,
        "--end-date",
        END_DATE,
        "--selection-dates",
        END_DATE,
        "--rebalance-mode",
        "monthly",
        "--goal23-manifest-key",
        stores.goal23_manifest_key,
        *extra,
    ]


def _wire_memory_storage(monkeypatch, stores):
    monkeypatch.setattr(
        cli,
        "_load_tushare_candidate_batch_json",
        stores.read_control_json,
    )
    monkeypatch.setattr(
        cli,
        "_write_tushare_candidate_batch_json",
        stores.write_control_json,
    )
    monkeypatch.setattr(
        cli,
        "_read_goal23_factor_object",
        stores.read_goal23_factor_object,
    )
    monkeypatch.setattr(
        cli,
        "_read_goal23_factor_commit",
        stores.read_goal23_commit,
    )
    monkeypatch.setattr(
        cli,
        "_read_goal22_processed_object",
        stores.read_goal22_object,
    )
    monkeypatch.setattr(
        cli,
        "_read_goal22_processed_commit",
        stores.read_goal22_commit,
    )
    monkeypatch.setattr(
        cli,
        "_read_goal24_selection_object",
        stores.read_selection_object,
    )
    monkeypatch.setattr(
        cli,
        "_write_goal24_selection_object",
        stores.write_selection_object,
    )
    monkeypatch.setattr(
        cli,
        "_read_goal24_selection_commit",
        stores.read_selection_commit,
    )
    monkeypatch.setattr(
        cli,
        "_write_goal24_selection_commit",
        stores.write_selection_commit,
    )
    monkeypatch.setattr(cli, "_ensure_db_schema", lambda: None)
    monkeypatch.setattr(
        cli,
        "create_selection_snapshot_repository",
        lambda: stores,
    )


def _must_not_run(*_args, **_kwargs):
    raise AssertionError("forbidden Goal 24 dependency ran")


def test_goal24_parser_is_explicit_dry_run_resume_and_has_no_provider_gate():
    stores = fresh_goal24_stores()
    parser = cli.build_parser()

    parsed = parser.parse_args(_args(stores))

    assert parsed.command == "run-real-selection-range"
    assert parsed.apply is False
    assert parsed.resume is True
    assert parsed.force is False
    assert parsed.rebalance_mode == "monthly"
    with pytest.raises(SystemExit):
        parser.parse_args(_args(stores, "--provider-call"))


def test_goal24_parser_requires_dates_mode_and_goal23_manifest():
    stores = fresh_goal24_stores()
    parser = cli.build_parser()
    values = _args(stores)
    marker = values.index("--goal23-manifest-key")
    del values[marker : marker + 2]

    with pytest.raises(SystemExit):
        parser.parse_args(values)


def test_goal24_cli_missing_goal23_manifest_fails_before_any_write(
    monkeypatch,
    capsys,
):
    stores = fresh_goal24_stores()
    _wire_memory_storage(monkeypatch, stores)
    del stores.control_json[stores.goal23_manifest_key]
    control_before = deepcopy(stores.control_json)
    monkeypatch.setattr(cli, "_ensure_db_schema", _must_not_run)
    monkeypatch.setattr(
        cli,
        "create_selection_snapshot_repository",
        _must_not_run,
    )

    exit_code = cli.main(_args(stores))

    captured = capsys.readouterr()
    assert exit_code == 2
    assert "missing Goal 23 manifest" in captured.err
    assert stores.control_json == control_before
    assert stores.selection_objects == {}
    assert stores.selection_commits == {}
    assert stores.snapshots == {}


def test_goal24_cli_dry_run_has_zero_control_database_or_canonical_writes(
    monkeypatch,
    capsys,
):
    stores = fresh_goal24_stores()
    _wire_memory_storage(monkeypatch, stores)
    control_before = deepcopy(stores.control_json)
    monkeypatch.setattr(cli, "_ensure_db_schema", _must_not_run)
    monkeypatch.setattr(
        cli,
        "create_selection_snapshot_repository",
        _must_not_run,
    )

    exit_code = cli.main(_args(stores, "--force", "--no-resume"))

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert output["status"] == "READY_FOR_APPLY"
    assert output["mode"] == "DRY_RUN"
    assert output["manifest_persisted"] is False
    assert stores.control_json == control_before
    assert stores.selection_objects == {}
    assert stores.selection_commits == {}
    assert stores.snapshots == {}


def test_goal24_cli_apply_never_calls_provider_legacy_selection_or_backtest(
    monkeypatch,
    capsys,
):
    stores = fresh_goal24_stores()
    _wire_memory_storage(monkeypatch, stores)
    monkeypatch.setattr(cli, "TushareProvider", _must_not_run)
    monkeypatch.setattr(cli, "AKShareProvider", _must_not_run)
    monkeypatch.setattr(cli, "HistoricalProviderRouter", _must_not_run)
    monkeypatch.setattr(cli, "build_selection_for_date", _must_not_run)
    monkeypatch.setattr(cli, "run_backtest", _must_not_run)
    monkeypatch.setattr(cli, "build_factor_daily_for_date", _must_not_run)

    exit_code = cli.main(_args(stores, "--apply"))

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert output["status"] == "COMPLETED"
    assert output["mode"] == "APPLY"
    assert output["firewalls"] == {
        "provider_call": False,
        "backtest": False,
        "llm": False,
        "api_page_scheduler": False,
        "auto_trading": False,
    }
    assert (END_DATE, "monthly") in stores.selection_commits
    assert (END_DATE, "monthly") in stores.snapshots


def test_goal24_local_parquet_commit_and_reader_round_trip(
    monkeypatch,
    tmp_path,
):
    stores = fresh_goal24_stores()
    result = stores.run_goal24(apply=True)
    assert result["status"] == "COMPLETED"
    commit = stores.selection_commits[(END_DATE, "monthly")]
    frame = stores.selection_objects[commit["output"]["object_key"]]

    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(cli, "load_settings", lambda: {"storage": {}})

    object_key = cli._write_goal24_selection_object(
        END_DATE,
        "monthly",
        commit["generation_id"],
        frame,
    )
    commit_key = cli._write_goal24_selection_commit(
        END_DATE,
        "monthly",
        commit,
    )
    published = read_goal24_published_selection_result(
        trade_date=END_DATE,
        rebalance_mode="monthly",
        selection_commit_read_fn=cli._read_goal24_selection_commit,
        selection_object_read_fn=cli._read_goal24_selection_object,
    )

    assert object_key == commit["output"]["object_key"]
    assert (
        commit_key
        == "processed/_goal24_selection_commits/"
        f"trade_date={END_DATE}/rebalance_mode=monthly/commit.json"
    )
    assert len(published) == commit["output"]["row_count"]
    assert list(published.columns) == list(frame.columns)
    competing = deepcopy(commit)
    competing["run_id"] = "goal24-local-competing-writer"
    competing["plan_fingerprint"] = "c" * 64

    with pytest.raises(FileExistsError):
        cli._write_goal24_selection_commit(
            END_DATE,
            "monthly",
            competing,
        )

    assert cli._read_goal24_selection_commit(
        END_DATE,
        "monthly",
    ) == commit


def test_goal24_minio_commit_writer_uses_conditional_create_only(
    monkeypatch,
):
    stores = fresh_goal24_stores()
    result = stores.run_goal24(apply=True)
    assert result["status"] == "COMPLETED"
    commit = stores.selection_commits[(END_DATE, "monthly")]
    competing = deepcopy(commit)
    competing["run_id"] = "goal24-minio-competing-writer"
    competing["plan_fingerprint"] = "d" * 64

    class FakeMinio:
        def __init__(self):
            self.objects = {}
            self.headers = []

        def _put_object(
            self,
            bucket,
            object_key,
            body,
            *,
            headers,
        ):
            self.headers.append(deepcopy(headers))
            identity = (bucket, object_key)
            if identity in self.objects:
                raise cli.S3Error(
                    None,
                    "PreconditionFailed",
                    "object already exists",
                    object_key,
                    "request-id",
                    "host-id",
                    bucket,
                    object_key,
                )
            self.objects[identity] = bytes(body)

        def stat_object(self, bucket, object_key):
            return self.objects[(bucket, object_key)]

    client = FakeMinio()
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "minio")
    monkeypatch.setattr(
        cli,
        "load_settings",
        lambda: {
            "storage": {
                "minio_bucket_processed": "processed-test",
            }
        },
    )
    monkeypatch.setattr(
        cli,
        "create_minio_client",
        lambda _settings: client,
    )
    monkeypatch.setattr(
        cli,
        "ensure_buckets",
        lambda _client, _buckets: None,
    )

    object_key = cli._write_goal24_selection_commit(
        END_DATE,
        "monthly",
        commit,
    )
    original = client.objects[("processed-test", object_key)]

    with pytest.raises(FileExistsError):
        cli._write_goal24_selection_commit(
            END_DATE,
            "monthly",
            competing,
        )

    assert client.objects[("processed-test", object_key)] == original
    assert client.headers == [
        {
            "Content-Type": "application/json",
            "If-None-Match": "*",
        },
        {
            "Content-Type": "application/json",
            "If-None-Match": "*",
        },
    ]


def test_goal24_goal22_reader_rejects_large_string_before_pandas(
    monkeypatch,
    tmp_path,
):
    stores = fresh_goal24_stores()
    committed = stores.goal22_commits[END_DATE]["outputs"][
        "factor_input_table"
    ]
    object_key = committed["object_key"]
    frame = stores.goal22_objects[object_key].copy(deep=True)
    table = pa.Table.from_pandas(frame, preserve_index=False)
    industry_index = table.schema.get_field_index("industry")
    table = table.set_column(
        industry_index,
        "industry",
        pa.array(
            frame["industry"].astype(str).tolist(),
            type=pa.large_string(),
        ),
    )
    path = tmp_path / object_key
    path.parent.mkdir(parents=True)
    pq.write_table(table, path)

    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(cli, "load_settings", lambda: {"storage": {}})
    monkeypatch.setattr(
        cli.pd,
        "read_parquet",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("pandas read ran before Goal 22 Arrow validation")
        ),
    )

    with pytest.raises(
        DataValidationError,
        match="industry Parquet/Arrow type must be string",
    ):
        cli._read_goal22_processed_object(object_key)


@pytest.mark.parametrize(
    ("column", "drift_kind", "expected_message"),
    [
        *[
            (
                column,
                "dictionary",
                f"{column} Parquet/Arrow type must be string",
            )
            for column in SELECTION_TEXT_COLUMNS
        ],
        *[
            (
                column,
                "float32",
                f"{column} Parquet/Arrow type must be float64",
            )
            for column in SELECTION_FLOAT_COLUMNS
        ],
        (
            "rank",
            "int32",
            "rank Parquet/Arrow type must be int64",
        ),
        (
            "trade_date",
            "date32",
            "trade_date Parquet/Arrow type must be string",
        ),
        (
            "trade_date",
            "timestamp",
            "trade_date Parquet/Arrow type must be string",
        ),
    ],
)
def test_goal24_local_reader_rejects_physical_arrow_schema_drift_before_pandas(
    monkeypatch,
    tmp_path,
    column,
    drift_kind,
    expected_message,
):
    stores = fresh_goal24_stores()
    stores.run_goal24(apply=True)
    commit = stores.selection_commits[(END_DATE, "monthly")]
    object_key = commit["output"]["object_key"]
    frame = stores.selection_objects[object_key].copy(deep=True)
    table = pa.Table.from_pandas(frame, preserve_index=False)

    if drift_kind == "date32":
        values = pa.array(
            [date.fromisoformat(END_DATE)] * len(frame),
            type=pa.date32(),
        )
    elif drift_kind == "timestamp":
        values = pa.array(
            [datetime.fromisoformat(f"{END_DATE}T00:00:00")]
            * len(frame),
            type=pa.timestamp("ns"),
        )
    elif drift_kind == "dictionary":
        values = pa.array(
            frame[column].astype(str).tolist(),
            type=pa.string(),
        ).dictionary_encode()
    elif drift_kind == "float32":
        values = pa.array(
            frame[column].astype(float).tolist(),
            type=pa.float32(),
        )
    else:
        values = pa.array(
            frame[column].astype(int).tolist(),
            type=pa.int32(),
        )
    column_index = table.schema.get_field_index(column)
    table = table.set_column(column_index, column, values)
    path = tmp_path / object_key
    path.parent.mkdir(parents=True)
    pq.write_table(table, path)

    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(cli, "load_settings", lambda: {"storage": {}})
    monkeypatch.setattr(
        cli.pd,
        "read_parquet",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("pandas read ran before Arrow schema validation")
        ),
    )

    with pytest.raises(DataValidationError, match=expected_message):
        cli._read_goal24_selection_object(object_key)

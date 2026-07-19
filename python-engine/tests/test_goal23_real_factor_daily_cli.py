from datetime import date, datetime
import json

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from goal23_test_helpers import Goal23MemoryStores
from stock_selector import cli
from stock_selector.data.data_validator import DataValidationError


TRADE_DATE = "2026-06-19"


def _args(
    stores: Goal23MemoryStores,
    *extra: str,
    run_id: str = "goal23-cli-test",
) -> list[str]:
    return [
        "run-real-factor-range",
        "--run-id",
        run_id,
        "--start-date",
        TRADE_DATE,
        "--end-date",
        TRADE_DATE,
        "--trade-dates",
        TRADE_DATE,
        "--goal22-manifest-key",
        stores.goal22_manifest_key,
        *extra,
    ]


def _wire_memory_storage(monkeypatch, stores: Goal23MemoryStores) -> None:
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
        "_load_tushare_candidate_batch_parquet",
        stores.read_canonical_object,
    )
    monkeypatch.setattr(
        cli,
        "_read_goal22_processed_object",
        stores.read_goal22_processed_object,
    )
    monkeypatch.setattr(
        cli,
        "_read_goal22_processed_commit",
        stores.read_goal22_commit,
    )
    monkeypatch.setattr(
        cli,
        "_read_goal23_factor_object",
        stores.read_factor_object,
    )
    monkeypatch.setattr(
        cli,
        "_write_goal23_factor_object",
        stores.write_factor_object,
    )
    monkeypatch.setattr(
        cli,
        "_read_goal23_factor_commit",
        stores.read_factor_commit,
    )
    monkeypatch.setattr(
        cli,
        "_write_goal23_factor_commit",
        stores.write_factor_commit,
    )


def _must_not_run(*_args, **_kwargs):
    raise AssertionError("Goal 23 downstream/provider entrypoint must not run")


def test_goal23_parser_requires_manifest_and_defaults_to_dry_run_resume():
    stores = Goal23MemoryStores([TRADE_DATE])
    parser = cli.build_parser()
    parsed = parser.parse_args(_args(stores))

    assert parsed.command == "run-real-factor-range"
    assert parsed.apply is False
    assert parsed.resume is True
    assert parsed.force is False

    without_manifest = _args(stores)
    marker_index = without_manifest.index("--goal22-manifest-key")
    del without_manifest[marker_index : marker_index + 2]
    with pytest.raises(SystemExit):
        parser.parse_args(without_manifest)


def test_goal23_cli_missing_manifest_object_fails_before_control_outputs(
    monkeypatch,
    capsys,
):
    stores = Goal23MemoryStores([TRADE_DATE])
    _wire_memory_storage(monkeypatch, stores)
    del stores.control_json[stores.goal22_manifest_key]

    exit_code = cli.main(_args(stores))

    assert exit_code == 2
    assert "missing" in capsys.readouterr().err.lower()
    assert not any(
        key.startswith("candidate/real_factor_daily/")
        for key in stores.control_json
    )


def test_goal23_cli_force_and_no_resume_do_not_enable_apply(
    monkeypatch,
    capsys,
):
    stores = Goal23MemoryStores([TRADE_DATE])
    _wire_memory_storage(monkeypatch, stores)

    exit_code = cli.main(_args(stores, "--force", "--no-resume"))

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert output["status"] == "READY_FOR_APPLY"
    assert output["mode"] == "DRY_RUN"
    assert output["apply_requested"] is False
    assert stores.factor_objects == {}
    assert stores.factor_commits == {}


def test_goal23_cli_apply_never_calls_provider_selection_or_backtest(
    monkeypatch,
    capsys,
):
    stores = Goal23MemoryStores([TRADE_DATE])
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
    assert output["downstream_firewalls"] == {
        "selection_result": False,
        "backtest": False,
        "llm": False,
        "provider_call": False,
    }
    assert TRADE_DATE in stores.factor_commits


def test_goal23_local_empty_parquet_and_commit_reader_round_trip(
    monkeypatch,
    tmp_path,
):
    stores = Goal23MemoryStores([TRADE_DATE], empty_universe=True)
    result = stores.run_goal23(apply=True)
    assert result["status"] == "COMPLETED"
    commit = stores.factor_commits[TRADE_DATE]
    frame = stores.factor_objects[commit["output"]["object_key"]]

    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(cli, "load_settings", lambda: {"storage": {}})

    object_key = cli._write_goal23_factor_object(
        TRADE_DATE,
        commit["generation_id"],
        frame,
    )
    commit_key = cli._write_goal23_factor_commit(TRADE_DATE, commit)
    published = cli._read_goal23_factor_daily(TRADE_DATE)

    assert object_key == commit["output"]["object_key"]
    assert commit_key.endswith(f"trade_date={TRADE_DATE}/commit.json")
    assert published.empty
    assert list(published.columns) == list(frame.columns)
    assert "total_score" not in published.columns


@pytest.mark.parametrize(
    ("raw_dtype", "tampered_value", "expected_arrow_type"),
    [
        pytest.param("string", "1.25", "string", id="string"),
        pytest.param("object", "1.25", "string", id="object"),
        pytest.param("bool", True, "bool", id="bool"),
        pytest.param("int64", 1, "int64", id="int64"),
        pytest.param("float32", 1.25, "float", id="float32"),
    ],
)
def test_goal23_local_published_reader_rejects_non_numeric_parquet_schema_before_pandas(
    monkeypatch,
    tmp_path,
    raw_dtype,
    tampered_value,
    expected_arrow_type,
):
    stores = Goal23MemoryStores([TRADE_DATE])
    result = stores.run_goal23(apply=True)
    assert result["status"] == "COMPLETED"
    commit = stores.factor_commits[TRADE_DATE]
    object_key = commit["output"]["object_key"]
    frame = stores.factor_objects[object_key].copy(deep=True)
    frame["trend_ret_20d"] = pd.Series(
        [tampered_value] * len(frame),
        index=frame.index,
        dtype=raw_dtype,
    )
    parquet_path = tmp_path / object_key
    parquet_path.parent.mkdir(parents=True)
    frame.to_parquet(parquet_path, index=False)
    assert (
        str(pq.read_schema(parquet_path).field("trend_ret_20d").type)
        == expected_arrow_type
    )

    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(cli, "load_settings", lambda: {"storage": {}})
    cli._write_goal23_factor_commit(TRADE_DATE, commit)
    monkeypatch.setattr(
        cli.pd,
        "read_parquet",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("pandas read ran before Arrow schema validation")
        ),
    )

    with pytest.raises(
        DataValidationError,
        match="trend_ret_20d Parquet/Arrow type must be float64",
    ):
        cli._read_goal23_factor_daily(TRADE_DATE)


@pytest.mark.parametrize(
    ("column", "drift_kind"),
    [
        pytest.param("trade_date", "date32", id="trade-date-date32"),
        pytest.param("trade_date", "timestamp", id="trade-date-timestamp"),
        pytest.param("industry", "dictionary", id="industry-dictionary"),
    ],
)
def test_goal23_local_published_reader_rejects_base_arrow_type_drift(
    monkeypatch,
    tmp_path,
    column,
    drift_kind,
):
    stores = Goal23MemoryStores([TRADE_DATE])
    result = stores.run_goal23(apply=True)
    assert result["status"] == "COMPLETED"
    commit = stores.factor_commits[TRADE_DATE]
    object_key = commit["output"]["object_key"]
    frame = stores.factor_objects[object_key].copy(deep=True)
    table = pa.Table.from_pandas(frame, preserve_index=False)

    if drift_kind == "date32":
        drift_values = pa.array(
            [date.fromisoformat(TRADE_DATE)] * len(frame),
            type=pa.date32(),
        )
    elif drift_kind == "timestamp":
        drift_values = pa.array(
            [datetime.fromisoformat(f"{TRADE_DATE}T00:00:00")] * len(frame),
            type=pa.timestamp("ns"),
        )
    else:
        drift_values = pa.array(
            frame[column].astype(str).tolist(),
            type=pa.string(),
        ).dictionary_encode()

    column_index = table.schema.get_field_index(column)
    table = table.set_column(column_index, column, drift_values)
    parquet_path = tmp_path / object_key
    parquet_path.parent.mkdir(parents=True)
    pq.write_table(table, parquet_path)
    drift_type = pq.read_schema(parquet_path).field(column).type
    if drift_kind == "date32":
        assert pa.types.is_date32(drift_type)
    elif drift_kind == "timestamp":
        assert pa.types.is_timestamp(drift_type)
    else:
        assert pa.types.is_dictionary(drift_type)

    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(cli, "load_settings", lambda: {"storage": {}})
    cli._write_goal23_factor_commit(TRADE_DATE, commit)
    monkeypatch.setattr(
        cli.pd,
        "read_parquet",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("pandas read ran before Arrow schema validation")
        ),
    )

    with pytest.raises(
        DataValidationError,
        match=rf"{column} Parquet/Arrow type must be string",
    ):
        cli._read_goal23_factor_daily(TRADE_DATE)

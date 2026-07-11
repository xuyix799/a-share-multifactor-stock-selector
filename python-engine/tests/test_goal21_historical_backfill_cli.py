from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from stock_selector import cli
from stock_selector.data import historical_backfill


BASE_ARGS = [
    "run-real-history-backfill",
    "--run-id",
    "goal21-cli-test",
    "--start-date",
    "2024-01-02",
    "--end-date",
    "2024-01-03",
    "--codes",
    "000001.SZ,600519.SH",
]


def test_goal21_parser_exposes_exact_defaults():
    args = cli.build_parser().parse_args(BASE_ARGS)

    assert args.provider_call is False
    assert args.apply is False
    assert args.resume is True
    assert args.force is False
    assert args.code_batch_size == 10
    assert args.date_batch_days == 31
    assert args.report_period_months == 3


@pytest.mark.parametrize(
    "scope_args",
    [
        [],
        ["--codes", "000001.SZ", "--universe-key", "raw/universe/u.parquet"],
    ],
)
def test_goal21_parser_requires_exactly_one_codes_or_universe_key(scope_args):
    argv = BASE_ARGS[:7] + scope_args
    with pytest.raises(SystemExit) as exc_info:
        cli.build_parser().parse_args(argv)
    assert exc_info.value.code == 2


@pytest.mark.parametrize(
    "flag",
    ["--universe", "--max-rows", "--max-chunks", "--max-attempts", "--retry-delay-seconds"],
)
def test_goal21_parser_rejects_unapproved_aliases_and_limit_retry_flags(flag):
    with pytest.raises(SystemExit) as exc_info:
        cli.build_parser().parse_args(BASE_ARGS + [flag, "1"])
    assert exc_info.value.code == 2


def test_goal21_parser_resume_flags_are_mutually_exclusive():
    with pytest.raises(SystemExit) as exc_info:
        cli.build_parser().parse_args(BASE_ARGS + ["--resume", "--no-resume"])
    assert exc_info.value.code == 2


@pytest.mark.parametrize(
    "argv",
    [
        BASE_ARGS + ["--prov"],
        BASE_ARGS + ["--provider"],
        BASE_ARGS + ["--app"],
        BASE_ARGS[:7] + ["--universe", "raw/universe/u.parquet"],
        BASE_ARGS + ["--code-batch", "1"],
    ],
)
def test_goal21_parser_rejects_all_option_abbreviations(argv):
    with pytest.raises(SystemExit) as exc_info:
        cli.build_parser().parse_args(argv)
    assert exc_info.value.code == 2


def _fake_result(plan, *, provider=False, apply=False, state="PENDING"):
    counts = {
        "PENDING": 0,
        "RUNNING": 0,
        "STAGED": 0,
        "COMPLETED": 0,
        "FAILED": 0,
        "BLOCKED": 0,
        "INTERRUPTED": 0,
    }
    counts[state] = plan["chunk_count"]
    summary = {
        "total": plan["chunk_count"],
        "planned": plan["chunk_count"],
        "state_counts": counts,
        "completion_rate": 1.0 if state == "COMPLETED" else 0.0,
        "canonical_ready": state == "COMPLETED",
        "gaps": [] if state == "COMPLETED" else [
            {"category": "CONFIGURATION_ERROR" if state == "BLOCKED" else None}
        ],
    }
    return {
        "goal": "Goal 21 resumable historical backfill",
        "run_id": plan["run_id"],
        "plan_fingerprint": plan["plan_fingerprint"],
        "gates": {
            "provider_call_enabled": provider,
            "apply_standard_write": apply,
            "resume": True,
            "force": False,
        },
        "plan_key": f"candidate/real_history_backfill/run_id={plan['run_id']}/plan.json",
        "root_manifest_key": f"candidate/real_history_backfill/run_id={plan['run_id']}/manifest.json",
        "summary": summary,
        "attempted_chunk_ids": [] if state == "PENDING" else ["one"],
        "skipped_chunk_ids": [],
        "reconciled_chunk_ids": [],
        "downstream_firewalls": {
            "clean_daily_snapshot": False,
            "factor": False,
            "selection": False,
            "backtest": False,
        },
    }


def test_goal21_handler_default_wiring_is_dry_run_and_preserves_explicit_codes(monkeypatch, capsys):
    captured = {}

    def capture(**kwargs):
        captured.update(kwargs)
        return _fake_result(kwargs["plan"])

    monkeypatch.setattr(cli, "run_real_history_backfill", capture, raising=False)
    monkeypatch.setattr(cli, "TushareProvider", lambda *a, **k: pytest.fail("provider constructed"))
    monkeypatch.setattr(cli, "AKShareProvider", lambda *a, **k: pytest.fail("provider constructed"))

    exit_code = cli.main(BASE_ARGS)

    assert exit_code == 0
    assert captured["plan"]["scope"]["codes"] == ["000001.SZ", "600519.SH"]
    assert captured["fetch_chunk_fn"] is None
    assert captured["canonical_read_fn"] is None
    assert captured["canonical_write_fn"] is None
    assert captured["provider_call_enabled"] is False
    assert captured["apply_standard_write"] is False
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "DRY_RUN"
    assert output["mode"] == "PLAN_ONLY"
    assert output["planned_chunks"] == captured["plan"]["chunk_count"]


@pytest.mark.parametrize(
    ("flags", "provider", "apply"),
    [
        ([], False, False),
        (["--provider-call"], True, False),
        (["--apply"], False, True),
        (["--provider-call", "--apply"], True, True),
        (["--force", "--no-resume"], False, False),
    ],
)
def test_goal21_provider_and_apply_gates_are_independent_and_lazy(
    monkeypatch,
    capsys,
    flags,
    provider,
    apply,
):
    captured = {}

    def capture(**kwargs):
        captured.update(kwargs)
        state = "COMPLETED" if apply else ("STAGED" if provider else "PENDING")
        return _fake_result(kwargs["plan"], provider=provider, apply=apply, state=state)

    monkeypatch.setattr(cli, "run_real_history_backfill", capture, raising=False)
    monkeypatch.setattr(cli, "load_settings", lambda: pytest.fail("provider settings loaded eagerly"))
    monkeypatch.setattr(cli, "TushareProvider", lambda *a, **k: pytest.fail("provider constructed eagerly"))
    monkeypatch.setattr(cli, "AKShareProvider", lambda *a, **k: pytest.fail("provider constructed eagerly"))

    assert cli.main(BASE_ARGS + flags) == 0

    assert captured["provider_call_enabled"] is provider
    assert captured["apply_standard_write"] is apply
    assert callable(captured["fetch_chunk_fn"]) is provider
    assert callable(captured["canonical_read_fn"]) is apply
    assert callable(captured["canonical_write_fn"]) is apply
    assert captured["force"] is ("--force" in flags)
    assert captured["resume"] is ("--no-resume" not in flags)
    capsys.readouterr()


def test_goal21_default_dry_run_writes_only_local_control_artifacts_without_network(
    monkeypatch,
    tmp_path,
    capsys,
):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(cli, "TushareProvider", lambda *a, **k: pytest.fail("provider constructed"))
    monkeypatch.setattr(cli, "AKShareProvider", lambda *a, **k: pytest.fail("provider constructed"))
    monkeypatch.setattr(cli, "create_minio_client", lambda *a, **k: pytest.fail("MinIO initialized"))
    monkeypatch.setattr(cli, "_write_dataset", lambda *a, **k: pytest.fail("canonical write"))

    assert cli.main(BASE_ARGS) == 0

    prefix = tmp_path / "candidate" / "real_history_backfill" / "run_id=goal21-cli-test"
    assert (prefix / "plan.json").exists()
    assert (prefix / "manifest.json").exists()
    assert not list(prefix.glob("dataset=*/chunk_id=*/manifest.json"))
    assert not list(prefix.rglob("*.parquet"))
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "DRY_RUN"
    assert output["canonical_ready"] is False
    assert output["downstream_firewalls"] == {
        "clean_daily_snapshot": False,
        "factor": False,
        "selection": False,
        "backtest": False,
    }
    assert "plan" not in output and "chunks" not in output and "gaps" not in output


def test_goal21_same_run_id_can_resume_across_cli_process_timestamps(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    timestamps = iter(["2026-07-12T00:00:00Z", "2030-01-01T00:00:00Z"])
    monkeypatch.setattr(historical_backfill, "_utc_now_iso", lambda: next(timestamps))

    assert cli.main(BASE_ARGS) == 0
    first = json.loads(capsys.readouterr().out)
    assert cli.main(BASE_ARGS) == 0
    second = json.loads(capsys.readouterr().out)

    assert first["plan_fingerprint"] == second["plan_fingerprint"]
    plan_path = tmp_path / first["plan_key"]
    persisted = json.loads(plan_path.read_text(encoding="utf-8"))
    assert persisted["generated_at"] == "2026-07-12T00:00:00Z"


def test_goal21_universe_key_loads_parquet_and_preserves_lineage(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    universe_key = "raw/universe/history.parquet"
    universe_path = tmp_path / universe_key
    universe_path.parent.mkdir(parents=True)
    pd.DataFrame({"stock_code": ["000001.SZ", "600519.SH"]}).to_parquet(universe_path, index=False)
    argv = BASE_ARGS[:7] + ["--universe-key", universe_key]

    assert cli.main(argv) == 0

    plan_path = (
        tmp_path
        / "candidate"
        / "real_history_backfill"
        / "run_id=goal21-cli-test"
        / "plan.json"
    )
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    assert plan["scope"]["universe_source"] == "universe_frame"
    assert plan["scope"]["universe_key"] == universe_key
    capsys.readouterr()


@pytest.mark.parametrize(
    "universe_key",
    [
        "../escape.parquet",
        "C:/secret.parquet",
        "/absolute.parquet",
        "raw\\bad.parquet",
        "raw/u.json",
        " raw/universe/u.parquet",
        "raw/universe/u.parquet ",
    ],
)
def test_goal21_unsafe_or_non_parquet_universe_fails_before_materialization(
    monkeypatch,
    capsys,
    universe_key,
):
    monkeypatch.setattr(cli, "_load_history_universe_frame", lambda key: pytest.fail("materialized"), raising=False)
    argv = BASE_ARGS[:7] + ["--universe-key", universe_key]

    assert cli.main(argv) == 2
    assert "invalid input" in capsys.readouterr().err


@pytest.mark.parametrize(
    "extra",
    [
        ["--code-batch-size", "0"],
        ["--date-batch-days", "0"],
        ["--report-period-months", "0"],
    ],
)
def test_goal21_invalid_batch_sizes_fail_before_provider_or_canonical(monkeypatch, capsys, extra):
    monkeypatch.setattr(cli, "TushareProvider", lambda *a, **k: pytest.fail("provider constructed"))
    monkeypatch.setattr(cli, "_write_dataset", lambda *a, **k: pytest.fail("canonical write"))

    assert cli.main(BASE_ARGS + extra + ["--provider-call", "--apply"]) == 2
    assert "invalid input" in capsys.readouterr().err


def test_goal21_explicit_empty_codes_never_fall_back_to_goal13_defaults(monkeypatch, capsys):
    argv = BASE_ARGS[:-1] + [""]
    assert cli.main(argv) == 2
    assert "invalid input" in capsys.readouterr().err


def test_goal21_apply_only_without_verified_staging_is_blocked_and_never_writes_canonical(
    monkeypatch,
    tmp_path,
    capsys,
):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(cli, "TushareProvider", lambda *a, **k: pytest.fail("provider constructed"))
    monkeypatch.setattr(cli, "_write_dataset", lambda *a, **k: pytest.fail("canonical write"))

    assert cli.main(BASE_ARGS + ["--apply"]) == 1

    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "BLOCKED"
    assert output["mode"] == "APPLY_ONLY"
    assert output["canonical_ready"] is False
    assert output["state_counts"]["BLOCKED"] == output["planned_chunks"]
    assert output["failure_categories"] == ["CONFIGURATION_ERROR"]


def test_goal21_unsupported_historical_semantics_block_before_provider_construction(monkeypatch):
    plan = cli.build_history_backfill_plan(
        run_id="goal21-cli-unsupported",
        start_date="2024-01-02",
        end_date="2024-01-02",
        codes=["000001.SZ"],
        code_batch_size=10,
        date_batch_days=31,
        report_period_months=3,
        datasets=["stock_basic"],
    )
    monkeypatch.setattr(cli, "TushareProvider", lambda *a, **k: pytest.fail("provider constructed"))
    fetch = cli._build_history_fetch_chunk_fn(plan)

    result = fetch(plan["chunks"][0])

    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] == "SEMANTIC_SOURCE_UNAVAILABLE"


def test_goal21_supported_provider_is_constructed_only_when_fetch_callback_is_invoked(monkeypatch):
    plan = cli.build_history_backfill_plan(
        run_id="goal21-cli-lazy-provider",
        start_date="2024-01-02",
        end_date="2024-01-02",
        codes=["000001.SZ"],
        code_batch_size=10,
        date_batch_days=31,
        report_period_months=3,
        datasets=["adj_factor"],
    )
    events = []

    class FakeProvider:
        def __init__(self, settings):
            events.append(("construct", settings))

        def fetch_raw_endpoint_allow_empty(self, endpoint, **parameters):
            events.append(("fetch", endpoint, parameters))
            if endpoint == "trade_cal":
                return pd.DataFrame({"cal_date": ["20240102"], "is_open": [1]})
            assert endpoint == "adj_factor"
            return pd.DataFrame(
                {
                    "ts_code": ["000001.SZ"],
                    "trade_date": ["20240102"],
                    "adj_factor": [1.0],
                }
            )

    monkeypatch.setattr(cli, "load_settings", lambda: {"fixture": True})
    monkeypatch.setattr(cli, "TushareProvider", FakeProvider)
    fetch = cli._build_history_fetch_chunk_fn(plan)
    assert events == []

    result = fetch(plan["chunks"][0])

    assert result.provider_status == "FETCHED"
    assert [event[0] for event in events].count("construct") == 1


def test_goal21_router_is_not_constructed_until_executor_invokes_fetch(monkeypatch, capsys):
    events = []

    class ForbiddenEagerRouter:
        def __init__(self, **kwargs):
            events.append(("router_construct", kwargs))

        def fetch_chunk(self, chunk):
            pytest.fail(f"fetch should not run: {chunk}")

    def capture(**kwargs):
        assert callable(kwargs["fetch_chunk_fn"])
        return _fake_result(kwargs["plan"], provider=True, state="STAGED")

    monkeypatch.setattr(cli, "HistoricalProviderRouter", ForbiddenEagerRouter)
    monkeypatch.setattr(cli, "run_real_history_backfill", capture)

    assert cli.main(BASE_ARGS + ["--provider-call"]) == 0
    assert events == []
    capsys.readouterr()


@pytest.mark.parametrize(
    ("state", "expected_status", "expected_exit"),
    [
        ("BLOCKED", "BLOCKED", 1),
        ("FAILED", "FAILED", 1),
        ("INTERRUPTED", "INTERRUPTED", 1),
        ("STAGED", "STAGED", 0),
        ("COMPLETED", "COMPLETED", 0),
    ],
)
def test_goal21_cli_status_and_exit_code_derive_from_root_summary(
    monkeypatch,
    capsys,
    state,
    expected_status,
    expected_exit,
):
    def capture(**kwargs):
        apply = state == "COMPLETED"
        provider = not apply
        return _fake_result(kwargs["plan"], provider=provider, apply=apply, state=state)

    monkeypatch.setattr(cli, "run_real_history_backfill", capture)
    flags = ["--apply"] if state == "COMPLETED" else ["--provider-call"]

    assert cli.main(BASE_ARGS + flags) == expected_exit

    output = json.loads(capsys.readouterr().out)
    assert output["status"] == expected_status
    assert "gaps" not in output
    assert "provider_calls" not in output

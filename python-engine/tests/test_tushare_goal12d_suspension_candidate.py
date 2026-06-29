import json

import pandas as pd

from stock_selector.cli import main
from stock_selector.storage.partition import build_partition, build_provider_smoke_partition


TRADE_DATE = "2024-06-19"


def test_goal12d_missing_required_inputs_block_without_candidate_rows():
    from stock_selector.data.tushare_suspension_status_candidate import (
        SuspensionCandidateInput,
        build_tushare_suspension_status_candidate,
    )

    for missing_dataset in ("daily_price_candidate", "trade_cal", "suspend_d"):
        report = build_tushare_suspension_status_candidate(
            TRADE_DATE,
            sample_limit=5,
            load_input_fn=lambda dataset, trade_date: SuspensionCandidateInput(
                dataset=dataset,
                object_key=_object_key_for(dataset, trade_date),
                frame=None if dataset in {missing_dataset, "daily_price_candidate_report"} else _frame_for(dataset),
                payload=None if dataset != "daily_price_candidate_report" else _daily_price_candidate_report(),
            ),
        )

        assert report["status"] == "BLOCKED_BY_MISSING_INPUT"
        assert report["missing_inputs"] == [{"dataset": missing_dataset, "object_key": _object_key_for(missing_dataset, TRADE_DATE)}]
        assert report["candidate_row_count"] == 0
        assert report["suspend_d_event_coverage"]["coverage_status"] == "MISSING_INPUT"
        assert report["readiness"]["ready_for_dq3_promotion"] is False


def test_goal12d_sample_truncated_suspend_misses_stay_unknown_and_never_false():
    from stock_selector.data.tushare_suspension_status_candidate import (
        SuspensionCandidateInput,
        SuspensionCoverageMetadata,
        build_tushare_suspension_status_candidate,
        candidate_frame_from_report,
    )

    report = build_tushare_suspension_status_candidate(
        TRADE_DATE,
        sample_limit=5,
        load_input_fn=_loader,
        coverage_metadata=SuspensionCoverageMetadata(api_total_rows_if_known=20, rows_written_if_known=2),
    )
    frame = candidate_frame_from_report(report)
    rows = {row.ts_code: row for row in frame.itertuples(index=False)}

    assert report["status"] == "CANDIDATE_AUDIT_COMPLETED_NOT_PROMOTABLE"
    assert report["coverage_universe"]["source"] == "daily_price_candidate_dry_run"
    assert report["coverage_universe"]["is_full_market_universe"] is False
    assert report["coverage_universe"]["is_explicit_candidate_universe"] is True
    assert report["suspend_d_event_coverage"]["coverage_status"] == "SAMPLE_TRUNCATED"
    assert report["suspend_d_event_coverage"]["is_sample_truncated"] is True
    assert report["suspend_d_event_coverage"]["api_total_rows_if_known"] == 20
    assert report["suspend_d_event_coverage"]["rows_written_if_known"] == 2
    assert report["suspend_d_event_coverage"]["full_coverage_proven"] is False
    assert rows["000001.SZ"].pause_status == "true_candidate"
    assert rows["000001.SZ"].is_paused_candidate is True
    assert rows["000001.SZ"].pause_evidence == "suspend_d_match"
    assert rows["000002.SZ"].pause_status == "unknown"
    assert rows["000002.SZ"].is_paused_candidate is None
    assert rows["000002.SZ"].pause_evidence == "blocked_by_sample_truncated_suspend_d"
    assert rows["000003.SZ"].pause_status == "unknown"
    assert rows["000004.SZ"].pause_status == "unknown"
    assert report["pause_status_counts"] == {"true_candidate": 1, "false_candidate": 0, "unknown": 3}
    assert report["evidence_counts"]["suspend_d_match"] == 1
    assert report["evidence_counts"]["blocked_by_sample_truncated_suspend_d"] == 3
    assert report["evidence_counts"]["full_event_coverage_no_match"] == 0
    assert report["readiness"]["status"] == "BLOCKED_BY_INCOMPLETE_SUSPEND_D_COVERAGE"
    assert report["readiness"]["ready_for_dq3_promotion"] is False
    assert report["safety"]["suspend_miss_inferred_as_false_without_coverage"] is False


def test_goal12d_coverage_unknown_suspend_miss_stays_unknown():
    from stock_selector.data.tushare_suspension_status_candidate import (
        SuspensionCoverageMetadata,
        build_tushare_suspension_status_candidate,
        candidate_frame_from_report,
    )

    report = build_tushare_suspension_status_candidate(
        TRADE_DATE,
        sample_limit=10,
        load_input_fn=_loader,
        coverage_metadata=SuspensionCoverageMetadata(),
    )
    frame = candidate_frame_from_report(report)
    rows = {row.ts_code: row for row in frame.itertuples(index=False)}

    assert report["suspend_d_event_coverage"]["coverage_status"] == "COVERAGE_UNKNOWN"
    assert report["suspend_d_event_coverage"]["full_coverage_proven"] is False
    assert rows["000002.SZ"].pause_status == "unknown"
    assert rows["000002.SZ"].is_paused_candidate is None
    assert rows["000002.SZ"].pause_evidence == "unresolved_no_event_match"
    assert report["pause_status_counts"]["false_candidate"] == 0
    assert report["readiness"]["status"] == "BLOCKED_BY_UNRESOLVED_IS_PAUSED"


def test_goal12d_full_event_coverage_allows_false_candidate_but_still_not_standard():
    from stock_selector.data.tushare_suspension_status_candidate import (
        SuspensionCoverageMetadata,
        build_tushare_suspension_status_candidate,
        candidate_frame_from_report,
    )

    report = build_tushare_suspension_status_candidate(
        TRADE_DATE,
        sample_limit=5,
        load_input_fn=_loader,
        coverage_metadata=SuspensionCoverageMetadata(full_event_coverage_proven=True, api_total_rows_if_known=2, rows_written_if_known=2),
    )
    frame = candidate_frame_from_report(report)
    rows = {row.ts_code: row for row in frame.itertuples(index=False)}

    assert report["suspend_d_event_coverage"]["coverage_status"] == "FULL_EVENT_COVERAGE"
    assert rows["000002.SZ"].pause_status == "false_candidate"
    assert rows["000002.SZ"].is_paused_candidate is False
    assert rows["000002.SZ"].pause_evidence == "full_event_coverage_no_match"
    assert rows["000002.SZ"].is_standard is False
    assert "is_paused" not in frame.columns
    assert report["pause_status_counts"] == {"true_candidate": 1, "false_candidate": 3, "unknown": 0}
    assert report["evidence_counts"]["full_event_coverage_no_match"] == 3
    assert report["safety"]["standard_suspension_status_written"] is False
    assert report["safety"]["standard_daily_price_written"] is False
    assert report["readiness"]["ready_for_dq3_promotion"] is False


def test_goal12d_zero_volume_zero_amount_missing_daily_and_unchanged_price_do_not_infer_pause():
    from stock_selector.data.tushare_suspension_status_candidate import (
        SuspensionCoverageMetadata,
        build_tushare_suspension_status_candidate,
        candidate_frame_from_report,
    )

    report = build_tushare_suspension_status_candidate(
        TRADE_DATE,
        sample_limit=10,
        load_input_fn=_loader,
        coverage_metadata=SuspensionCoverageMetadata(),
    )
    rows = {row.ts_code: row for row in candidate_frame_from_report(report).itertuples(index=False)}

    assert rows["000002.SZ"].volume == 0
    assert rows["000002.SZ"].pause_status == "unknown"
    assert rows["000003.SZ"].amount == 0
    assert rows["000003.SZ"].pause_status == "unknown"
    assert rows["000004.SZ"].open == rows["000004.SZ"].close == rows["000004.SZ"].pre_close
    assert rows["000004.SZ"].pause_status == "unknown"
    assert report["inference_guards"]["volume_used_as_pause"] is False
    assert report["inference_guards"]["amount_used_as_pause"] is False
    assert report["inference_guards"]["missing_daily_used_as_pause"] is False
    assert report["inference_guards"]["unchanged_price_used_as_pause"] is False
    assert report["inference_guards"]["suspend_d_miss_used_as_false_without_coverage"] is False


def test_goal12d_report_schema_and_candidate_only_paths_are_isolated():
    from stock_selector.data.tushare_suspension_status_candidate import (
        build_suspension_status_candidate_output_keys,
        build_tushare_suspension_status_candidate,
    )

    output_keys = build_suspension_status_candidate_output_keys(TRADE_DATE)
    report = build_tushare_suspension_status_candidate(TRADE_DATE, sample_limit=5, load_input_fn=_loader)

    assert output_keys == {
        "candidate": f"candidate/tushare/suspension_status_candidate/trade_date={TRADE_DATE}/part.parquet",
        "coverage_audit": f"candidate/tushare/suspension_status_coverage_audit/trade_date={TRADE_DATE}/report.json",
    }
    assert report["input_object_keys"]["daily_price_candidate"] == _object_key_for("daily_price_candidate", TRADE_DATE)
    assert report["input_object_keys"]["daily_price_candidate_report"] == _object_key_for("daily_price_candidate_report", TRADE_DATE)
    assert report["output_object_keys"] == output_keys
    assert report["input_row_counts"]["daily_price_candidate"] == 4
    assert "candidate_row_count" in report
    assert "schema_check" in report
    assert "pause_status_counts" in report
    assert "evidence_counts" in report
    assert "inference_guards" in report
    assert "safety" in report
    assert report["safety"]["standard_daily_price_written"] is False
    assert report["safety"]["standard_suspension_status_written"] is False
    assert report["safety"]["real_raw_mainline_written"] is False
    assert report["safety"]["cleaning_mainline_entered"] is False
    assert report["safety"]["factor_mainline_entered"] is False
    assert report["safety"]["selection_mainline_entered"] is False
    assert report["safety"]["backtest_mainline_entered"] is False
    assert report["safety"]["spring_api_changed"] is False
    assert report["safety"]["is_paused_fabricated"] is False
    assert report["safety"]["suspend_miss_inferred_as_false_without_coverage"] is False
    assert not output_keys["candidate"].startswith("raw/")
    assert not output_keys["coverage_audit"].startswith("raw/")
    assert "clean_daily_snapshot" not in output_keys["candidate"]
    assert "factor_daily" not in output_keys["candidate"]
    assert "selection_result" not in output_keys["candidate"]
    assert build_partition("clean_daily_snapshot", TRADE_DATE).object_key != output_keys["candidate"]
    assert build_partition("factor_daily", TRADE_DATE).object_key != output_keys["candidate"]
    assert build_partition("selection_result", TRADE_DATE).object_key != output_keys["candidate"]


def test_goal12d_cli_reads_local_inputs_and_writes_candidate_artifacts(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))

    candidate_key = _object_key_for("daily_price_candidate", TRADE_DATE)
    candidate_path = tmp_path / candidate_key
    candidate_path.parent.mkdir(parents=True, exist_ok=True)
    _frame_for("daily_price_candidate").to_parquet(candidate_path, index=False)

    report_key = _object_key_for("daily_price_candidate_report", TRADE_DATE)
    report_path = tmp_path / report_key
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(_daily_price_candidate_report(), ensure_ascii=False), encoding="utf-8")

    for dataset in ("trade_cal", "suspend_d"):
        partition = build_provider_smoke_partition("tushare", dataset, TRADE_DATE, local_root=tmp_path)
        partition.local_path.parent.mkdir(parents=True, exist_ok=True)
        _frame_for(dataset).to_parquet(partition.local_path, index=False)

    exit_code = main(["build-tushare-suspension-status-candidate", "--trade-date", TRADE_DATE, "--sample-limit", "2"])

    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "CANDIDATE_AUDIT_COMPLETED_NOT_PROMOTABLE"
    assert output["coverage_status"] == "SAMPLE_TRUNCATED"
    assert output["ready_for_dq3_promotion"] is False
    output_candidate_path = tmp_path / output["candidate_key"]
    output_report_path = tmp_path / output["coverage_audit_key"]
    assert output_candidate_path.exists()
    assert output_report_path.exists()
    written_candidate = pd.read_parquet(output_candidate_path)
    written_report = json.loads(output_report_path.read_text(encoding="utf-8"))
    assert len(written_candidate) == 4
    assert "is_paused" not in written_candidate.columns
    assert written_report["output_object_keys"]["candidate"] == output["candidate_key"]
    assert written_report["suspend_d_event_coverage"]["coverage_status"] == "SAMPLE_TRUNCATED"
    assert written_report["pause_status_counts"]["false_candidate"] == 0
    assert "raw/daily_price" not in output["candidate_key"]
    assert "processed/selection_result" not in output["coverage_audit_key"]


def _loader(dataset, trade_date):
    from stock_selector.data.tushare_suspension_status_candidate import SuspensionCandidateInput

    return SuspensionCandidateInput(
        dataset=dataset,
        object_key=_object_key_for(dataset, trade_date),
        frame=None if dataset == "daily_price_candidate_report" else _frame_for(dataset),
        payload=_daily_price_candidate_report() if dataset == "daily_price_candidate_report" else None,
    )


def _object_key_for(dataset, trade_date):
    if dataset == "daily_price_candidate":
        return f"smoke/tushare/daily_price_candidate_dry_run/trade_date={trade_date}/part.parquet"
    if dataset == "daily_price_candidate_report":
        return f"smoke/tushare/daily_price_candidate_dry_run/trade_date={trade_date}/report.json"
    return f"smoke/tushare/{dataset}/trade_date={trade_date}/part.parquet"


def _daily_price_candidate_report():
    return {
        "provider": "tushare",
        "goal": "12C",
        "trade_date": TRADE_DATE,
        "sample_limit": 5,
        "candidate_dataset": "daily_price_candidate",
        "join": {"candidate_row_count": 4},
        "output_object_keys": {
            "candidate": _object_key_for("daily_price_candidate", TRADE_DATE),
            "report": _object_key_for("daily_price_candidate_report", TRADE_DATE),
        },
        "readiness": {"ready_for_dq3_promotion": False, "status": "BLOCKED_BY_UNRESOLVED_IS_PAUSED"},
    }


def _frame_for(dataset):
    frames = {
        "daily_price_candidate": pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": TRADE_DATE,
                    "open": 10.1,
                    "high": 10.2,
                    "low": 10.0,
                    "close": 10.15,
                    "pre_close": 10.05,
                    "volume": 1000.0,
                    "amount": 10000.0,
                },
                {
                    "ts_code": "000002.SZ",
                    "trade_date": TRADE_DATE,
                    "open": 9.1,
                    "high": 9.2,
                    "low": 9.0,
                    "close": 9.15,
                    "pre_close": 9.05,
                    "volume": 0.0,
                    "amount": 9000.0,
                },
                {
                    "ts_code": "000003.SZ",
                    "trade_date": TRADE_DATE,
                    "open": 8.1,
                    "high": 8.2,
                    "low": 8.0,
                    "close": 8.15,
                    "pre_close": 8.05,
                    "volume": 800.0,
                    "amount": 0.0,
                },
                {
                    "ts_code": "000004.SZ",
                    "trade_date": TRADE_DATE,
                    "open": 7.0,
                    "high": 7.0,
                    "low": 7.0,
                    "close": 7.0,
                    "pre_close": 7.0,
                    "volume": 700.0,
                    "amount": 7000.0,
                },
            ]
        ),
        "trade_cal": pd.DataFrame([{"exchange": "SSE", "cal_date": "20240619", "is_open": 1, "pretrade_date": "20240618"}]),
        "suspend_d": pd.DataFrame(
            [
                {"ts_code": "000001.SZ", "trade_date": "20240619", "suspend_timing": "09:30:00", "suspend_type": "S"},
                {"ts_code": "999999.SZ", "trade_date": "20240619", "suspend_timing": "09:30:00", "suspend_type": "S"},
            ]
        ),
    }
    return frames[dataset].copy()

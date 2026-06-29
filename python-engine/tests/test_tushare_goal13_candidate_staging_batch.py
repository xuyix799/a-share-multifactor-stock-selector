import json

import pandas as pd

from stock_selector.cli import main
from stock_selector.storage.partition import build_partition


START_DATE = "2024-06-01"
END_DATE = "2024-06-05"
CODES = ["000001.SZ", "600519.SH"]
BATCH_ID = "goal13-test-batch"
TRADE_DATES = ["2024-06-03", "2024-06-04"]


def test_goal13_builds_candidate_staging_batch_reports_and_never_writes_standard_paths():
    from stock_selector.data.tushare_candidate_staging_batch import (
        build_tushare_candidate_staging_batch,
        build_tushare_candidate_staging_batch_output_keys,
    )

    writer = _MemoryWriter()
    result = build_tushare_candidate_staging_batch(
        start_date=START_DATE,
        end_date=END_DATE,
        codes=CODES,
        provider=_FakeTushareBatchProvider(),
        batch_id=BATCH_ID,
        sleep_seconds=0,
        write_parquet_fn=writer.write_parquet,
        write_json_fn=writer.write_json,
        generated_at_fn=lambda: "2026-06-29T00:00:00Z",
    )
    keys = build_tushare_candidate_staging_batch_output_keys(BATCH_ID, TRADE_DATES)

    assert result["status"] == "CANDIDATE_BATCH_COMPLETED_NOT_PROMOTABLE"
    assert result["batch_id"] == BATCH_ID
    assert result["provider"] == "tushare"
    assert result["goal"] == "13B"
    assert result["start_date"] == START_DATE
    assert result["end_date"] == END_DATE
    assert result["codes"] == CODES
    assert result["trade_dates"] == TRADE_DATES
    assert result["output_object_keys"] == keys
    assert result["daily_price_candidate_row_count"] == 4
    assert result["suspension_status_candidate_row_count"] == 4
    assert result["staging_row_counts"]["daily"] == 4
    assert result["staging_row_counts"]["stk_limit"] == 3
    assert result["staging_row_counts"]["adj_factor"] == 4
    assert result["staging_row_counts"]["daily_basic"] == 4
    assert result["staging_row_counts"]["trade_cal"] == 3
    assert result["staging_row_counts"]["suspend_d"] == 2

    assert set(writer.json_payloads) == {
        keys["manifest"],
        keys["provider_coverage_report"],
        keys["fetch_semantics_report"],
        keys["coverage_gap_report"],
        keys["dq3_readiness_audit"],
    }
    assert keys["daily_staging"]["2024-06-03"] in writer.parquet_payloads
    assert keys["stk_limit_staging"]["2024-06-04"] in writer.parquet_payloads
    assert keys["daily_price_candidate_batch"] in writer.parquet_payloads
    assert keys["suspension_status_candidate_batch"] in writer.parquet_payloads

    daily_candidate = writer.parquet_payloads[keys["daily_price_candidate_batch"]]
    suspension_candidate = writer.parquet_payloads[keys["suspension_status_candidate_batch"]]
    assert "is_paused" not in daily_candidate.columns
    assert "is_paused" not in suspension_candidate.columns
    assert set(daily_candidate["is_standard"]) == {False}
    assert set(suspension_candidate["is_standard"]) == {False}
    assert set(daily_candidate["is_promotable"]) == {False}
    assert set(suspension_candidate["is_promotable"]) == {False}
    assert daily_candidate.set_index(["ts_code", "trade_date"]).loc[("000001.SZ", "2024-06-03")]["pause_status"] == "true_candidate"
    assert daily_candidate.set_index(["ts_code", "trade_date"]).loc[("600519.SH", "2024-06-03")]["pause_status"] == "unknown"
    assert suspension_candidate["pause_status"].value_counts().to_dict() == {"unknown": 3, "true_candidate": 1}

    coverage = writer.json_payloads[keys["provider_coverage_report"]]
    assert coverage["code_count_requested"] == 2
    assert coverage["trade_day_count"] == 2
    assert coverage["expected_code_date_count"] == 4
    assert coverage["expected_code_trade_date_count"] == 4
    assert coverage["interfaces"]["daily"]["coverage"]["matched_rows"] == 4
    assert coverage["interfaces"]["daily"]["coverage"]["denominator"] == 4
    assert coverage["daily_coverage"]["numerator"] == 4
    assert coverage["daily_coverage"]["denominator"] == 4
    assert coverage["stk_limit_coverage"]["numerator"] == 3
    assert coverage["stk_limit_coverage"]["missing_rows"] == 1
    assert coverage["adj_factor_coverage"]["coverage_rate"] == 1.0
    assert coverage["daily_basic_coverage"]["coverage_rate"] == 1.0
    assert coverage["trade_cal_coverage"]["trade_day_count"] == 2
    assert coverage["suspend_d_event_coverage"]["coverage_status"] == "COVERAGE_UNKNOWN"
    assert coverage["duplicate_key_counts"]["daily"] == 0
    assert coverage["missing_key_counts"]["stk_limit"] == 1
    assert coverage["schema_check"]["daily"]["missing_required_fields"] == []
    assert coverage["date_range_check"]["start_date"] == START_DATE
    assert coverage["date_range_check"]["end_date"] == END_DATE
    assert coverage["rate_limit_or_blocked_errors"] == []
    assert coverage["interfaces"]["stk_limit"]["coverage"]["matched_rows"] == 3
    assert coverage["interfaces"]["stk_limit"]["coverage"]["missing_rows"] == 1
    assert coverage["interfaces"]["adj_factor"]["coverage"]["coverage_rate"] == 1.0
    assert coverage["interfaces"]["daily_basic"]["coverage"]["coverage_rate"] == 1.0
    assert coverage["interfaces"]["suspend_d"]["matched_candidate_events"] == 1
    assert coverage["interfaces"]["suspend_d"]["events_not_in_requested_universe"] == 1
    assert coverage["duplicate_key_checks"]["daily"]["duplicate_rows"] == 0
    assert coverage["schema_checks"]["daily"]["missing_required_fields"] == []
    assert coverage["provider_errors"] == []
    assert coverage["sample_truncated"] is False

    manifest = writer.json_payloads[keys["manifest"]]
    assert manifest["batch_id"] == BATCH_ID
    assert manifest["provider"] == "tushare"
    assert manifest["interfaces_requested"] == ["daily", "stk_limit", "adj_factor", "daily_basic", "trade_cal", "suspend_d"]
    assert manifest["interfaces_succeeded"] == ["daily", "stk_limit", "adj_factor", "daily_basic", "trade_cal", "suspend_d"]
    assert manifest["interfaces_failed"] == []
    assert manifest["row_counts"]["daily_price_candidate_batch"] == 4
    assert manifest["schema_versions"]["daily_price_candidate_batch"] == "goal13.v1"
    assert manifest["dq_level"] == "DQ1"
    assert manifest["is_standard"] is False
    assert manifest["is_promotable"] is False
    assert "token" not in json.dumps(manifest).lower()

    dq3 = writer.json_payloads[keys["dq3_readiness_audit"]]
    assert dq3["ready_for_dq3_promotion"] is False
    assert dq3["status"] == "BLOCKED_BY_UNRESOLVED_IS_PAUSED"
    assert "INCOMPLETE_OR_UNKNOWN_SUSPEND_D_COVERAGE" in dq3["blocked_reasons"]
    assert dq3["duplicate_check"]["ok"] is True
    assert dq3["schema_check"]["ok"] is True
    assert dq3["safety"]["standard_daily_price_written"] is False
    assert dq3["safety"]["standard_suspension_status_written"] is False
    assert dq3["safety"]["real_raw_mainline_written"] is False
    assert dq3["safety"]["cleaning_mainline_entered"] is False
    assert dq3["safety"]["factor_mainline_entered"] is False
    assert dq3["safety"]["selection_mainline_entered"] is False
    assert dq3["safety"]["backtest_mainline_entered"] is False
    assert dq3["safety"]["spring_api_changed"] is False
    assert dq3["inference_guards"]["volume_used_as_pause"] is False
    assert dq3["inference_guards"]["amount_used_as_pause"] is False
    assert dq3["inference_guards"]["missing_daily_used_as_pause"] is False
    assert dq3["inference_guards"]["unchanged_price_used_as_pause"] is False
    assert dq3["inference_guards"]["suspend_d_miss_used_as_false_without_coverage"] is False

    for object_key in _flatten_object_keys(keys):
        assert not object_key.startswith("raw/")
        assert "clean_daily_snapshot" not in object_key
        assert "factor_daily" not in object_key
        assert "selection_result" not in object_key
    assert build_partition("daily_price", "2024-06-03").object_key != keys["daily_price_candidate_batch"]
    assert build_partition("clean_daily_snapshot", "2024-06-03").object_key != keys["daily_price_candidate_batch"]
    assert build_partition("factor_daily", "2024-06-03").object_key != keys["daily_price_candidate_batch"]
    assert build_partition("selection_result", "2024-06-03").object_key != keys["daily_price_candidate_batch"]


def test_goal13b_coverage_expansion_records_fetch_semantics_and_uses_date_strategy_for_stk_limit():
    from stock_selector.data.tushare_candidate_staging_batch import build_tushare_candidate_staging_batch

    writer = _MemoryWriter()
    provider = _FakeTushareBatchProvider(complete_limit_coverage=True)
    result = build_tushare_candidate_staging_batch(
        start_date=START_DATE,
        end_date=END_DATE,
        codes=CODES,
        provider=provider,
        batch_id=BATCH_ID,
        sleep_seconds=0,
        coverage_expansion=True,
        fetch_semantics_audit=True,
        write_parquet_fn=writer.write_parquet,
        write_json_fn=writer.write_json,
        generated_at_fn=lambda: "2026-06-29T00:00:00Z",
    )

    fetch_report = writer.json_payloads[result["output_object_keys"]["fetch_semantics_report"]]
    matrix = {item["interface"]: item for item in fetch_report["fetch_semantics_matrix"]}

    assert result["coverage_expansion"] is True
    assert result["fetch_semantics_audit"] is True
    assert matrix["daily"]["fetch_strategy"] == "by_code_range"
    assert matrix["daily"]["loops_per_code"] is True
    assert matrix["stk_limit"]["fetch_strategy"] == "by_trade_date"
    assert matrix["stk_limit"]["loops_per_code"] is False
    assert matrix["stk_limit"]["loops_per_trade_day"] is True
    assert [call["parameters"]["trade_date"] for call in matrix["stk_limit"]["calls"]] == ["20240603", "20240604"]
    assert all("ts_code" not in call["parameters"] for call in matrix["stk_limit"]["calls"])
    assert matrix["adj_factor"]["fetch_strategy"] == "by_code_range"
    assert matrix["adj_factor"]["loops_per_code"] is True
    assert matrix["daily_basic"]["fetch_strategy"] == "by_code_range"
    assert matrix["trade_cal"]["fetch_strategy"] == "by_date_range"
    assert matrix["suspend_d"]["fetch_strategy"] == "by_date_range"
    assert matrix["stk_limit"]["row_count_alignment"]["status"] == "ALIGNED"
    assert fetch_report["sample_limit_policy"]["critical_staging_sample_limit_allowed"] is False
    assert fetch_report["sample_limit_policy"]["sample_limit_applied"] is False
    assert "TUSHARE_TOKEN" not in json.dumps(fetch_report)


def test_goal13b_coverage_gap_report_lists_missing_keys_and_reason_codes():
    from stock_selector.data.tushare_candidate_staging_batch import build_tushare_candidate_staging_batch

    writer = _MemoryWriter()
    result = build_tushare_candidate_staging_batch(
        start_date=START_DATE,
        end_date=END_DATE,
        codes=CODES,
        provider=_FakeTushareBatchProvider(half_limit_coverage=True, half_adj_factor_coverage=True),
        batch_id=BATCH_ID,
        sleep_seconds=0,
        coverage_expansion=True,
        fetch_semantics_audit=True,
        write_parquet_fn=writer.write_parquet,
        write_json_fn=writer.write_json,
        generated_at_fn=lambda: "2026-06-29T00:00:00Z",
    )

    coverage = result["provider_coverage_report"]
    gap = writer.json_payloads[result["output_object_keys"]["coverage_gap_report"]]

    assert coverage["stk_limit_coverage"]["numerator"] == 2
    assert coverage["stk_limit_coverage"]["denominator"] == 4
    assert coverage["stk_limit_coverage"]["ratio"] == 0.5
    assert coverage["adj_factor_coverage"]["numerator"] == 2
    assert coverage["adj_factor_coverage"]["denominator"] == 4
    assert coverage["daily_coverage"]["ratio"] == 1.0
    assert coverage["daily_basic_coverage"]["ratio"] == 1.0
    assert any(
        item["ts_code"] == "000001.SZ"
        and item["trade_date"] == "2024-06-04"
        and item["missing_interface"] == "stk_limit"
        and item["missing_fields"] == ["limit_up", "limit_down"]
        and item["reason_code"] == "DATE_ALIGNMENT_GAP"
        for item in gap["missing_key_examples"]
    )
    assert any(
        item["ts_code"] == "600519.SH"
        and item["trade_date"] == "2024-06-04"
        and item["missing_interface"] == "adj_factor"
        and item["missing_fields"] == ["adj_factor"]
        and item["reason_code"] == "DATE_ALIGNMENT_GAP"
        for item in gap["missing_key_examples"]
    )
    assert gap["interface_gap_summary"]["stk_limit"]["reason_codes"] == ["DATE_ALIGNMENT_GAP"]
    assert gap["interface_gap_summary"]["adj_factor"]["reason_codes"] == ["DATE_ALIGNMENT_GAP"]
    assert "stk_limit likely fetched by incomplete date set" in gap["fetch_strategy_suspicions"]
    assert "adj_factor likely fetched by incomplete date set" in gap["fetch_strategy_suspicions"]
    assert result["dq3_readiness_audit"]["blocked_reasons"][:2] == [
        "INCOMPLETE_LIMIT_PRICE_COVERAGE",
        "INCOMPLETE_ADJ_FACTOR_COVERAGE",
    ]


def test_goal13b_full_price_coverage_but_unresolved_pause_blocks_only_pause_and_suspend():
    from stock_selector.data.tushare_candidate_staging_batch import build_tushare_candidate_staging_batch

    writer = _MemoryWriter()
    result = build_tushare_candidate_staging_batch(
        start_date=START_DATE,
        end_date=END_DATE,
        codes=CODES,
        provider=_FakeTushareBatchProvider(complete_limit_coverage=True, empty_suspend_d=True),
        batch_id=BATCH_ID,
        sleep_seconds=0,
        coverage_expansion=True,
        write_parquet_fn=writer.write_parquet,
        write_json_fn=writer.write_json,
        generated_at_fn=lambda: "2026-06-29T00:00:00Z",
    )

    dq3 = result["dq3_readiness_audit"]

    assert result["provider_coverage_report"]["daily_coverage"]["ratio"] == 1.0
    assert result["provider_coverage_report"]["stk_limit_coverage"]["ratio"] == 1.0
    assert result["provider_coverage_report"]["adj_factor_coverage"]["ratio"] == 1.0
    assert result["provider_coverage_report"]["daily_basic_coverage"]["ratio"] == 1.0
    assert dq3["status"] == "BLOCKED_BY_UNRESOLVED_IS_PAUSED"
    assert dq3["blocked_reasons"] == ["UNRESOLVED_IS_PAUSED", "INCOMPLETE_OR_UNKNOWN_SUSPEND_D_COVERAGE"]
    assert dq3["ready_for_promotion_validator"] is False
    assert dq3["ready_for_dq3_promotion"] is False
    assert dq3["coverage_summary"]["critical_price_coverage_complete"] is True
    assert dq3["coverage_summary"]["price_coverage_complete_pause_unresolved"] is True


def test_goal13b_sample_truncated_critical_staging_blocks_full_coverage():
    from stock_selector.data.tushare_candidate_staging_batch import build_tushare_candidate_staging_batch

    writer = _MemoryWriter()
    result = build_tushare_candidate_staging_batch(
        start_date=START_DATE,
        end_date=END_DATE,
        codes=CODES,
        provider=_FakeTushareBatchProvider(sample_truncated_interfaces={"stk_limit"}),
        batch_id=BATCH_ID,
        sleep_seconds=0,
        coverage_expansion=True,
        write_parquet_fn=writer.write_parquet,
        write_json_fn=writer.write_json,
        generated_at_fn=lambda: "2026-06-29T00:00:00Z",
    )

    coverage = result["provider_coverage_report"]
    gap = writer.json_payloads[result["output_object_keys"]["coverage_gap_report"]]

    assert coverage["sample_truncated"] is True
    assert coverage["sample_truncation"]["stk_limit"]["sample_truncated"] is True
    assert coverage["sample_truncation"]["stk_limit"]["full_coverage_proven"] is False
    assert coverage["stk_limit_coverage"]["blocked_reason"] == "SAMPLE_TRUNCATED"
    assert "SAMPLE_TRUNCATED" in result["dq3_readiness_audit"]["blocked_reasons"]
    assert "PROVIDER_FETCH_INCOMPLETE" in result["dq3_readiness_audit"]["blocked_reasons"]
    assert gap["interface_gap_summary"]["stk_limit"]["reason_codes"] == ["SAMPLE_TRUNCATED"]
    assert "sample_limit may have truncated critical staging data" in gap["fetch_strategy_suspicions"]


def test_goal13_missing_or_failed_interface_blocks_dq3_readiness_without_fake_rows():
    from stock_selector.data.tushare_candidate_staging_batch import build_tushare_candidate_staging_batch

    writer = _MemoryWriter()
    result = build_tushare_candidate_staging_batch(
        start_date=START_DATE,
        end_date=END_DATE,
        codes=CODES,
        provider=_FakeTushareBatchProvider(fail_interfaces={"adj_factor": RuntimeError("provider adj_factor unavailable")}),
        batch_id=BATCH_ID,
        sleep_seconds=0,
        write_parquet_fn=writer.write_parquet,
        write_json_fn=writer.write_json,
        generated_at_fn=lambda: "2026-06-29T00:00:00Z",
    )

    assert result["status"] == "BLOCKED_BY_PROVIDER_ERROR"
    assert result["daily_price_candidate_row_count"] == 0
    assert result["staging_row_counts"]["adj_factor"] == 0
    assert result["coverage_summary"]["adj_factor"]["matched_rows"] == 0
    assert result["coverage_summary"]["adj_factor"]["missing_rows"] == 4
    assert "adj_factor" in result["interfaces_failed"]
    assert result["ready_for_dq3_promotion"] is False
    assert writer.parquet_payloads == {}
    assert writer.json_payloads == {}


def test_goal13_empty_required_interface_blocks_as_provider_empty_result_not_schema_mismatch():
    from stock_selector.data.tushare_candidate_staging_batch import build_tushare_candidate_staging_batch

    writer = _MemoryWriter()
    result = build_tushare_candidate_staging_batch(
        start_date=START_DATE,
        end_date=END_DATE,
        codes=CODES,
        provider=_FakeTushareBatchProvider(empty_interfaces={"trade_cal"}),
        batch_id=BATCH_ID,
        sleep_seconds=0,
        write_parquet_fn=writer.write_parquet,
        write_json_fn=writer.write_json,
        generated_at_fn=lambda: "2026-06-29T00:00:00Z",
    )

    assert result["status"] == "BLOCKED_BY_PROVIDER_EMPTY_RESULT"
    assert "trade_cal" in result["interfaces_failed"]
    assert result["ready_for_dq3_promotion"] is False
    assert result["blocked_reasons"] == ["trade_cal provider returned empty result"]
    assert result["daily_price_candidate_row_count"] == 0
    assert writer.parquet_payloads == {}
    assert writer.json_payloads == {}


def test_goal13_readiness_audit_lists_all_specific_blocking_reason_codes():
    from stock_selector.data.tushare_candidate_staging_batch import build_tushare_candidate_staging_batch

    writer = _MemoryWriter()
    result = build_tushare_candidate_staging_batch(
        start_date=START_DATE,
        end_date=END_DATE,
        codes=CODES,
        provider=_FakeTushareBatchProvider(
            half_limit_coverage=True,
            half_adj_factor_coverage=True,
            empty_suspend_d=True,
        ),
        batch_id=BATCH_ID,
        sleep_seconds=0,
        write_parquet_fn=writer.write_parquet,
        write_json_fn=writer.write_json,
        generated_at_fn=lambda: "2026-06-29T00:00:00Z",
    )
    readiness = result["dq3_readiness_audit"]
    blocked_reasons = set(readiness["blocked_reasons"])

    assert result["provider_coverage_report"]["stk_limit_coverage"]["numerator"] == 2
    assert result["provider_coverage_report"]["stk_limit_coverage"]["denominator"] == 4
    assert result["provider_coverage_report"]["adj_factor_coverage"]["numerator"] == 2
    assert result["provider_coverage_report"]["adj_factor_coverage"]["denominator"] == 4
    assert readiness["pause_status_summary"]["unknown"] == 4
    assert result["provider_coverage_report"]["suspend_d_event_coverage"]["coverage_status"] == "COVERAGE_UNKNOWN"
    assert readiness["ready_for_promotion_validator"] is False
    assert readiness["ready_for_dq3_promotion"] is False
    assert {
        "INCOMPLETE_LIMIT_PRICE_COVERAGE",
        "INCOMPLETE_ADJ_FACTOR_COVERAGE",
        "UNRESOLVED_IS_PAUSED",
        "INCOMPLETE_OR_UNKNOWN_SUSPEND_D_COVERAGE",
    }.issubset(blocked_reasons)
    assert len(blocked_reasons) >= 4


def test_goal13_suspend_miss_stays_unknown_even_with_zero_volume_zero_amount_missing_daily_and_unchanged_price():
    from stock_selector.data.tushare_candidate_staging_batch import build_tushare_candidate_staging_batch

    writer = _MemoryWriter()
    result = build_tushare_candidate_staging_batch(
        start_date=START_DATE,
        end_date=END_DATE,
        codes=CODES + ["000333.SZ"],
        provider=_FakeTushareBatchProvider(include_pathological_rows=True),
        batch_id=BATCH_ID,
        sleep_seconds=0,
        write_parquet_fn=writer.write_parquet,
        write_json_fn=writer.write_json,
        generated_at_fn=lambda: "2026-06-29T00:00:00Z",
    )
    candidate = writer.parquet_payloads[result["output_object_keys"]["daily_price_candidate_batch"]]
    rows = {(row.ts_code, row.trade_date): row for row in candidate.itertuples(index=False)}

    assert rows[("600519.SH", "2024-06-03")].volume == 0
    assert rows[("600519.SH", "2024-06-03")].pause_status == "unknown"
    assert rows[("000333.SZ", "2024-06-03")].amount == 0
    assert rows[("000333.SZ", "2024-06-03")].pause_status == "unknown"
    assert rows[("000333.SZ", "2024-06-03")].open == rows[("000333.SZ", "2024-06-03")].close == rows[("000333.SZ", "2024-06-03")].pre_close
    assert rows[("000333.SZ", "2024-06-03")].pause_status == "unknown"
    assert result["pause_status_counts"]["false_candidate"] == 0
    assert result["inference_guards"]["suspend_d_miss_used_as_false_without_coverage"] is False


def test_goal13_empty_suspend_d_response_without_columns_keeps_pause_unknown_not_schema_blocked():
    from stock_selector.data.tushare_candidate_staging_batch import build_tushare_candidate_staging_batch

    writer = _MemoryWriter()
    result = build_tushare_candidate_staging_batch(
        start_date=START_DATE,
        end_date=END_DATE,
        codes=CODES,
        provider=_FakeTushareBatchProvider(empty_suspend_d=True),
        batch_id=BATCH_ID,
        sleep_seconds=0,
        write_parquet_fn=writer.write_parquet,
        write_json_fn=writer.write_json,
        generated_at_fn=lambda: "2026-06-29T00:00:00Z",
    )
    daily_candidate = writer.parquet_payloads[result["output_object_keys"]["daily_price_candidate_batch"]]

    assert result["status"] == "CANDIDATE_BATCH_COMPLETED_NOT_PROMOTABLE"
    assert result["pause_status_counts"] == {"true_candidate": 0, "false_candidate": 0, "unknown": 4}
    assert result["provider_coverage_report"]["schema_checks"]["suspend_d"]["missing_required_fields"] == []
    assert result["provider_coverage_report"]["suspend_d_event_coverage"]["row_count"] == 0
    assert set(daily_candidate["pause_status"]) == {"unknown"}
    assert "is_paused" not in daily_candidate.columns


def test_goal13_full_event_coverage_allows_false_candidate_but_not_standard_write():
    from stock_selector.data.tushare_candidate_staging_batch import build_tushare_candidate_staging_batch

    writer = _MemoryWriter()
    result = build_tushare_candidate_staging_batch(
        start_date=START_DATE,
        end_date=END_DATE,
        codes=CODES,
        provider=_FakeTushareBatchProvider(complete_limit_coverage=True),
        batch_id=BATCH_ID,
        sleep_seconds=0,
        suspend_d_full_event_coverage_proven=True,
        write_parquet_fn=writer.write_parquet,
        write_json_fn=writer.write_json,
        generated_at_fn=lambda: "2026-06-29T00:00:00Z",
    )
    daily_candidate = writer.parquet_payloads[result["output_object_keys"]["daily_price_candidate_batch"]]
    suspension_candidate = writer.parquet_payloads[result["output_object_keys"]["suspension_status_candidate_batch"]]
    rows = {(row.ts_code, row.trade_date): row for row in daily_candidate.itertuples(index=False)}

    assert rows[("000001.SZ", "2024-06-03")].pause_status == "true_candidate"
    assert rows[("600519.SH", "2024-06-03")].pause_status == "false_candidate"
    assert rows[("600519.SH", "2024-06-03")].is_paused_candidate is False
    assert rows[("600519.SH", "2024-06-03")].pause_evidence == "full_event_coverage_no_match"
    assert "is_paused" not in daily_candidate.columns
    assert "is_paused" not in suspension_candidate.columns
    assert set(daily_candidate["is_standard"]) == {False}
    assert set(suspension_candidate["is_standard"]) == {False}
    assert result["pause_status_counts"] == {"true_candidate": 1, "false_candidate": 3, "unknown": 0}
    assert result["dq3_readiness_audit"]["status"] == "BLOCKED_BY_VALIDATOR"
    assert result["dq3_readiness_audit"]["ready_for_dq3_promotion"] is False


def test_goal13_cli_blocks_when_provider_disabled_or_token_missing(monkeypatch, capsys):
    monkeypatch.delenv("STOCK_TUSHARE_ENABLED", raising=False)
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)

    exit_code = main(["build-tushare-candidate-staging-batch", "--start-date", START_DATE, "--end-date", END_DATE, "--codes", "000001.SZ"])

    assert exit_code == 1
    disabled_output = json.loads(capsys.readouterr().out)
    assert disabled_output["status"] == "BLOCKED_BY_PROVIDER_DISABLED"
    assert disabled_output["ready_for_dq3_promotion"] is False

    monkeypatch.setenv("STOCK_TUSHARE_ENABLED", "true")
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    exit_code = main(["build-tushare-candidate-staging-batch", "--start-date", START_DATE, "--end-date", END_DATE, "--codes", "000001.SZ"])

    assert exit_code == 1
    missing_token_output = json.loads(capsys.readouterr().out)
    assert missing_token_output["status"] == "BLOCKED_BY_MISSING_TUSHARE_TOKEN"
    assert "TUSHARE_TOKEN" in missing_token_output["blocked_reasons"][0]
    assert "token" not in json.dumps(missing_token_output.get("manifest", {})).lower()


def test_goal13_cli_writes_local_candidate_artifacts(monkeypatch, tmp_path, capsys):
    import stock_selector.cli as cli_module

    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("STOCK_TUSHARE_ENABLED", "true")
    monkeypatch.setenv("TUSHARE_TOKEN", "fake-token")
    monkeypatch.setattr(cli_module, "TushareProvider", lambda settings=None: _FakeTushareBatchProvider())

    exit_code = main(
        [
            "build-tushare-candidate-staging-batch",
            "--start-date",
            START_DATE,
            "--end-date",
            END_DATE,
            "--codes",
            "000001.SZ,600519.SH",
            "--batch-id",
            BATCH_ID,
            "--sleep-seconds",
            "0",
        ]
    )

    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "CANDIDATE_BATCH_COMPLETED_NOT_PROMOTABLE"
    assert output["ready_for_dq3_promotion"] is False
    assert (tmp_path / output["manifest_key"]).exists()
    assert (tmp_path / output["provider_coverage_report_key"]).exists()
    assert (tmp_path / output["dq3_readiness_audit_key"]).exists()
    assert (tmp_path / output["daily_price_candidate_batch_key"]).exists()
    assert (tmp_path / output["suspension_status_candidate_batch_key"]).exists()
    written_candidate = pd.read_parquet(tmp_path / output["daily_price_candidate_batch_key"])
    written_audit = json.loads((tmp_path / output["dq3_readiness_audit_key"]).read_text(encoding="utf-8"))
    assert len(written_candidate) == 4
    assert "is_paused" not in written_candidate.columns
    assert written_audit["status"] == "BLOCKED_BY_UNRESOLVED_IS_PAUSED"
    assert "raw/daily_price" not in output["daily_price_candidate_batch_key"]
    assert "processed/selection_result" not in output["daily_price_candidate_batch_key"]


def test_goal13b_cli_supports_audit_flags_and_fail_on_incomplete_critical_coverage(monkeypatch, tmp_path, capsys):
    import stock_selector.cli as cli_module

    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("STOCK_TUSHARE_ENABLED", "true")
    monkeypatch.setenv("TUSHARE_TOKEN", "fake-token")
    monkeypatch.setattr(
        cli_module,
        "TushareProvider",
        lambda settings=None: _FakeTushareBatchProvider(half_limit_coverage=True, half_adj_factor_coverage=True),
    )

    base_args = [
        "build-tushare-candidate-staging-batch",
        "--start-date",
        START_DATE,
        "--end-date",
        END_DATE,
        "--codes",
        "000001.SZ,600519.SH",
        "--batch-id",
        BATCH_ID,
        "--sleep-seconds",
        "0",
        "--coverage-expansion",
        "--fetch-semantics-audit",
    ]

    assert main(base_args) == 0
    audit_output = json.loads(capsys.readouterr().out)
    assert audit_output["fetch_semantics_report_key"]
    assert audit_output["coverage_gap_report_key"]
    assert audit_output["coverage_summary"]["stk_limit"]["ratio"] == 0.5

    assert main(base_args + ["--fail-on-incomplete-critical-coverage"]) == 1
    fail_output = json.loads(capsys.readouterr().out)
    assert fail_output["status"] == "CANDIDATE_BATCH_COMPLETED_NOT_PROMOTABLE"
    assert "INCOMPLETE_LIMIT_PRICE_COVERAGE" in fail_output["blocked_reasons"]
    assert "INCOMPLETE_ADJ_FACTOR_COVERAGE" in fail_output["blocked_reasons"]


def test_goal13b_cli_no_provider_call_reuses_existing_staging(monkeypatch, tmp_path, capsys):
    import stock_selector.cli as cli_module

    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("STOCK_TUSHARE_ENABLED", "true")
    monkeypatch.setenv("TUSHARE_TOKEN", "fake-token")
    monkeypatch.setattr(cli_module, "TushareProvider", lambda settings=None: _FakeTushareBatchProvider(complete_limit_coverage=True))

    build_args = [
        "build-tushare-candidate-staging-batch",
        "--start-date",
        START_DATE,
        "--end-date",
        END_DATE,
        "--codes",
        "000001.SZ,600519.SH",
        "--batch-id",
        BATCH_ID,
        "--sleep-seconds",
        "0",
        "--coverage-expansion",
    ]
    assert main(build_args) == 0
    _ = capsys.readouterr()

    monkeypatch.setattr(cli_module, "TushareProvider", lambda settings=None: _ProviderShouldNotBeConstructed())
    assert main(build_args + ["--no-provider-call", "--reuse-existing-staging", "--fetch-semantics-audit"]) == 0
    reuse_output = json.loads(capsys.readouterr().out)

    assert reuse_output["reused_existing_staging"] is True
    assert reuse_output["staging_row_counts"]["daily"] == 4
    assert reuse_output["coverage_summary"]["daily"]["ratio"] == 1.0
    assert reuse_output["fetch_semantics_report_key"]


class _MemoryWriter:
    def __init__(self):
        self.parquet_payloads = {}
        self.json_payloads = {}

    def write_parquet(self, object_key, frame):
        self.parquet_payloads[object_key] = frame.copy()
        return object_key

    def write_json(self, object_key, payload):
        self.json_payloads[object_key] = json.loads(json.dumps(payload, ensure_ascii=False, default=str))
        return object_key


class _FakeTushareBatchProvider:
    def __init__(
        self,
        *,
        fail_interfaces=None,
        include_pathological_rows=False,
        complete_limit_coverage=False,
        empty_suspend_d=False,
        empty_interfaces=None,
        half_limit_coverage=False,
        half_adj_factor_coverage=False,
        sample_truncated_interfaces=None,
    ):
        self.fail_interfaces = fail_interfaces or {}
        self.include_pathological_rows = include_pathological_rows
        self.complete_limit_coverage = complete_limit_coverage
        self.empty_suspend_d = empty_suspend_d
        self.empty_interfaces = set(empty_interfaces or ())
        self.half_limit_coverage = half_limit_coverage
        self.half_adj_factor_coverage = half_adj_factor_coverage
        self.sample_truncated_interfaces = set(sample_truncated_interfaces or ())
        self.calls = []

    def fetch_raw_endpoint_allow_empty(self, interface, **kwargs):
        self.calls.append((interface, kwargs))
        if interface in self.fail_interfaces:
            raise self.fail_interfaces[interface]
        if interface in self.empty_interfaces:
            return pd.DataFrame()
        frames = _provider_frames(
            include_pathological_rows=self.include_pathological_rows,
            complete_limit_coverage=self.complete_limit_coverage,
            half_limit_coverage=self.half_limit_coverage,
            half_adj_factor_coverage=self.half_adj_factor_coverage,
        )
        if interface == "suspend_d" and self.empty_suspend_d:
            return pd.DataFrame()
        frame = frames[interface].copy()
        if "ts_code" in kwargs and "ts_code" in frame.columns:
            frame = frame[frame["ts_code"] == kwargs["ts_code"]].copy()
        if "trade_date" in kwargs and "trade_date" in frame.columns:
            frame = frame[frame["trade_date"] == kwargs["trade_date"]].copy()
        if interface in self.sample_truncated_interfaces:
            frame.attrs["sample_truncated"] = True
            frame.attrs["sample_limit"] = len(frame)
            frame.attrs["full_coverage_proven"] = False
        return frame


class _ProviderShouldNotBeConstructed:
    def __init__(self):
        raise AssertionError("TushareProvider should not be constructed when --no-provider-call reuses staging")


def _provider_frames(*, include_pathological_rows=False, complete_limit_coverage=False, half_limit_coverage=False, half_adj_factor_coverage=False):
    rows = []
    for trade_date in ("20240603", "20240604"):
        rows.extend(
            [
                _daily_row("000001.SZ", trade_date, 10.0, volume=1000.0, amount=10000.0),
                _daily_row("600519.SH", trade_date, 1700.0, volume=2000.0, amount=20000.0),
            ]
        )
    if include_pathological_rows:
        rows.extend(
            [
                _daily_row("000333.SZ", "20240603", 50.0, volume=3000.0, amount=0.0),
                _daily_row("000333.SZ", "20240604", 50.0, volume=3000.0, amount=5000.0),
            ]
        )
    daily = pd.DataFrame(rows)
    if include_pathological_rows:
        daily.loc[(daily["ts_code"] == "600519.SH") & (daily["trade_date"] == "20240603"), "vol"] = 0.0
        daily.loc[(daily["ts_code"] == "000333.SZ") & (daily["trade_date"] == "20240603"), ["open", "high", "low", "close", "pre_close"]] = 50.0

    stk_limit_rows = [
        {"ts_code": "000001.SZ", "trade_date": "20240603", "up_limit": 11.0, "down_limit": 9.0},
        {"ts_code": "600519.SH", "trade_date": "20240603", "up_limit": 1870.0, "down_limit": 1530.0},
        *(
            []
            if half_limit_coverage
            else [
                {"ts_code": "000001.SZ", "trade_date": "20240604", "up_limit": 11.2, "down_limit": 9.2},
                *(
                    [{"ts_code": "600519.SH", "trade_date": "20240604", "up_limit": 1871.0, "down_limit": 1531.0}]
                    if complete_limit_coverage
                    else []
                ),
            ]
        ),
        {"ts_code": "000333.SZ", "trade_date": "20240603", "up_limit": 55.0, "down_limit": 45.0},
        {"ts_code": "000333.SZ", "trade_date": "20240604", "up_limit": 55.0, "down_limit": 45.0},
    ]
    adj_factor_dates = ("20240603",) if half_adj_factor_coverage else ("20240603", "20240604")

    return {
        "trade_cal": pd.DataFrame(
            [
                {"exchange": "SSE", "cal_date": "20240601", "is_open": 0, "pretrade_date": "20240531"},
                {"exchange": "SSE", "cal_date": "20240603", "is_open": 1, "pretrade_date": "20240531"},
                {"exchange": "SSE", "cal_date": "20240604", "is_open": 1, "pretrade_date": "20240603"},
            ]
        ),
        "daily": daily,
        "stk_limit": pd.DataFrame(stk_limit_rows),
        "adj_factor": pd.DataFrame(
            [
                {"ts_code": code, "trade_date": trade_date, "adj_factor": 1.1}
                for trade_date in adj_factor_dates
                for code in ("000001.SZ", "600519.SH", "000333.SZ")
            ]
        ),
        "daily_basic": pd.DataFrame(
            [
                {
                    "ts_code": code,
                    "trade_date": trade_date,
                    "pe_ttm": 10.0,
                    "pb": 1.2,
                    "ps_ttm": 2.0,
                    "total_mv": 100000.0,
                    "circ_mv": 90000.0,
                    "turnover_rate": 0.8,
                }
                for trade_date in ("20240603", "20240604")
                for code in ("000001.SZ", "600519.SH", "000333.SZ")
            ]
        ),
        "suspend_d": pd.DataFrame(
            [
                {"ts_code": "000001.SZ", "trade_date": "20240603", "suspend_timing": "09:30:00", "suspend_type": "S"},
                {"ts_code": "999999.SZ", "trade_date": "20240603", "suspend_timing": "09:30:00", "suspend_type": "S"},
            ]
        ),
    }


def _daily_row(code, trade_date, price, *, volume, amount):
    return {
        "ts_code": code,
        "trade_date": trade_date,
        "open": price,
        "high": price + 1,
        "low": price - 1,
        "close": price + 0.5,
        "pre_close": price - 0.5,
        "vol": volume,
        "amount": amount,
    }


def _flatten_object_keys(keys):
    for value in keys.values():
        if isinstance(value, dict):
            yield from value.values()
        else:
            yield value

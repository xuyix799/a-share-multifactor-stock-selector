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
    assert result["goal"] == "13"
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
    assert "suspend_d event source full coverage is not proven" in dq3["blocked_reasons"]
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
    ):
        self.fail_interfaces = fail_interfaces or {}
        self.include_pathological_rows = include_pathological_rows
        self.complete_limit_coverage = complete_limit_coverage
        self.empty_suspend_d = empty_suspend_d
        self.empty_interfaces = set(empty_interfaces or ())
        self.half_limit_coverage = half_limit_coverage
        self.half_adj_factor_coverage = half_adj_factor_coverage
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
        return frame


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

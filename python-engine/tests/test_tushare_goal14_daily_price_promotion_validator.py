import json

import pandas as pd

from stock_selector.cli import main


BATCH_ID = "goal14-test-batch"
TRADE_DATES = ["2024-06-03", "2024-06-04"]
CODES = ["000001.SZ", "600519.SH"]


def test_goal14_ready_preflight_passes_validator_and_default_dry_run_writes_no_standard_rows():
    from stock_selector.data.tushare_daily_price_promotion_validator import (
        build_tushare_daily_price_promotion_validator,
    )

    writer = _StandardWriter()
    result = build_tushare_daily_price_promotion_validator(
        batch_id=BATCH_ID,
        promotion_preflight_report=_ready_preflight(),
        daily_price_candidate_batch=_candidate_frame(),
        suspension_status_candidate_batch=_suspension_candidate_frame(),
        standard_daily_price_write_fn=writer.write,
        generated_at_fn=lambda: "2026-07-01T00:00:00Z",
    )

    validator = result["daily_price_promotion_validator_report"]
    dry_run = result["standard_daily_price_promotion_dry_run_report"]

    assert validator["schema_version"] == "goal14.daily_price_promotion_validator_report.v1"
    assert validator["goal"] == "14"
    assert validator["status"] == "VALIDATOR_PASS"
    assert validator["blocked_reasons"] == []
    assert "STANDARD_SUSPENSION_STATUS_WRITE_NOT_ALLOWED_IN_GOAL14" in validator["blocked_reason_catalog"]
    assert "CLEAN_FACTOR_SELECTION_BACKTEST_NOT_ALLOWED_IN_GOAL14" in validator["blocked_reason_catalog"]
    assert validator["small_range_guard"] == {
        "max_codes": 5,
        "max_trade_days": 10,
        "max_rows": 50,
        "code_count": 2,
        "trade_day_count": 2,
        "row_count": 4,
        "passed": True,
    }
    assert validator["coverage_check"]["passed"] is True
    assert validator["pause_resolution_check"]["unknown_count"] == 0
    assert validator["schema_contract_check"]["passed"] is True
    assert validator["standard_daily_price_write_allowed_with_explicit_flag"] is True
    assert validator["standard_daily_price_write_performed"] is False
    assert validator["standard_suspension_status_write_performed"] is False
    assert validator["real_backtest_performed"] is False
    assert validator["clean_factor_selection_backtest_entered"] is False
    assert dry_run["schema_version"] == "goal14.standard_daily_price_promotion_dry_run_report.v1"
    assert dry_run["mode"] == "DRY_RUN"
    assert dry_run["validator_status"] == "VALIDATOR_PASS"
    assert dry_run["target_table"] == "daily_price"
    assert dry_run["would_insert_rows"] == 4
    assert dry_run["would_update_rows"] == 0
    assert dry_run["would_skip_rows"] == 0
    assert dry_run["standard_write_performed"] is False
    assert dry_run["requires_explicit_execute_flag"] is True
    assert writer.calls == []


def test_goal14_preflight_missing_or_not_ready_blocks_validator():
    from stock_selector.data.tushare_daily_price_promotion_validator import (
        build_tushare_daily_price_promotion_validator,
    )

    missing = build_tushare_daily_price_promotion_validator(
        batch_id=BATCH_ID,
        promotion_preflight_report=None,
        daily_price_candidate_batch=_candidate_frame(),
        suspension_status_candidate_batch=_suspension_candidate_frame(),
    )
    blocked_preflight = _ready_preflight()
    blocked_preflight["status"] = "BLOCKED"
    blocked_preflight["blocked_reasons"] = ["INCOMPLETE_DAILY_COVERAGE"]
    blocked = build_tushare_daily_price_promotion_validator(
        batch_id=BATCH_ID,
        promotion_preflight_report=blocked_preflight,
        daily_price_candidate_batch=_candidate_frame(),
        suspension_status_candidate_batch=_suspension_candidate_frame(),
    )

    assert missing["status"] == "BLOCKED"
    assert "GOAL13C_PREFLIGHT_REPORT_MISSING" in missing["blocked_reasons"]
    assert blocked["status"] == "BLOCKED"
    assert "GOAL13C_PREFLIGHT_NOT_READY" in blocked["blocked_reasons"]
    assert "INCOMPLETE_DAILY_COVERAGE" in blocked["blocked_reasons"]


def test_goal14_unknown_pause_blocks_without_inferring_from_price_or_volume():
    from stock_selector.data.tushare_daily_price_promotion_validator import (
        build_tushare_daily_price_promotion_validator,
    )

    candidate = _candidate_frame()
    candidate.loc[0, "pause_candidate_status"] = "unknown"
    candidate.loc[0, "is_paused_candidate"] = None
    candidate.loc[0, "resolution_source"] = "UNRESOLVED"
    candidate.loc[0, "volume"] = 0.0
    candidate.loc[0, "amount"] = 0.0
    result = build_tushare_daily_price_promotion_validator(
        batch_id=BATCH_ID,
        promotion_preflight_report=_ready_preflight(),
        daily_price_candidate_batch=candidate,
        suspension_status_candidate_batch=_suspension_candidate_frame(),
    )

    assert result["status"] == "BLOCKED"
    assert "UNRESOLVED_IS_PAUSED" in result["blocked_reasons"]
    assert "IS_PAUSED_SOURCE_NOT_AUDITABLE" in result["blocked_reasons"]
    guards = result["daily_price_promotion_validator_report"]["inference_guards"]
    assert guards["volume_used_as_pause"] is False
    assert guards["amount_used_as_pause"] is False
    assert guards["missing_daily_used_as_pause"] is False
    assert guards["unchanged_price_used_as_pause"] is False
    assert guards["suspend_d_miss_used_as_false_without_coverage"] is False


def test_goal14_missing_required_field_blocks_standard_contract():
    from stock_selector.data.tushare_daily_price_promotion_validator import (
        build_tushare_daily_price_promotion_validator,
    )

    candidate = _candidate_frame().drop(columns=["limit_down"])
    result = build_tushare_daily_price_promotion_validator(
        batch_id=BATCH_ID,
        promotion_preflight_report=_ready_preflight(),
        daily_price_candidate_batch=candidate,
        suspension_status_candidate_batch=_suspension_candidate_frame(),
    )

    assert result["status"] == "BLOCKED"
    assert "MISSING_REQUIRED_DAILY_PRICE_FIELD" in result["blocked_reasons"]
    assert "CANDIDATE_SCHEMA_INCOMPATIBLE_WITH_DAILY_PRICE_CONTRACT" in result["blocked_reasons"]


def test_goal14_invalid_ohlc_and_invalid_volume_or_amount_block_validator():
    from stock_selector.data.tushare_daily_price_promotion_validator import (
        build_tushare_daily_price_promotion_validator,
    )

    bad_ohlc = _candidate_frame()
    bad_ohlc.loc[0, "high"] = bad_ohlc.loc[0, "low"] - 1
    ohlc_result = build_tushare_daily_price_promotion_validator(
        batch_id=BATCH_ID,
        promotion_preflight_report=_ready_preflight(),
        daily_price_candidate_batch=bad_ohlc,
        suspension_status_candidate_batch=_suspension_candidate_frame(),
    )

    bad_volume = _candidate_frame()
    bad_volume.loc[0, "volume"] = -1
    volume_result = build_tushare_daily_price_promotion_validator(
        batch_id=BATCH_ID,
        promotion_preflight_report=_ready_preflight(),
        daily_price_candidate_batch=bad_volume,
        suspension_status_candidate_batch=_suspension_candidate_frame(),
    )

    bad_adj_factor = _candidate_frame()
    bad_adj_factor.loc[0, "adj_factor"] = 0
    adj_factor_result = build_tushare_daily_price_promotion_validator(
        batch_id=BATCH_ID,
        promotion_preflight_report=_ready_preflight(),
        daily_price_candidate_batch=bad_adj_factor,
        suspension_status_candidate_batch=_suspension_candidate_frame(),
    )

    assert "INVALID_OHLC" in ohlc_result["blocked_reasons"]
    assert "INVALID_VOLUME_OR_AMOUNT" in volume_result["blocked_reasons"]
    assert "CANDIDATE_SCHEMA_INCOMPATIBLE_WITH_DAILY_PRICE_CONTRACT" in adj_factor_result["blocked_reasons"]


def test_goal14_requires_auditable_limit_pre_close_and_pause_sources():
    from stock_selector.data.tushare_daily_price_promotion_validator import (
        build_tushare_daily_price_promotion_validator,
    )

    candidate = _candidate_frame()
    candidate.loc[0, "limit_source_object_key"] = ""
    candidate.loc[1, "daily_source_object_key"] = ""
    candidate.loc[2, "resolution_source"] = "UNRESOLVED"
    result = build_tushare_daily_price_promotion_validator(
        batch_id=BATCH_ID,
        promotion_preflight_report=_ready_preflight(),
        daily_price_candidate_batch=candidate,
        suspension_status_candidate_batch=_suspension_candidate_frame(),
    )

    assert "LIMIT_PRICE_SOURCE_NOT_AUDITABLE" in result["blocked_reasons"]
    assert "PRE_CLOSE_SOURCE_NOT_AUDITABLE" in result["blocked_reasons"]
    assert "IS_PAUSED_SOURCE_NOT_AUDITABLE" in result["blocked_reasons"]


def test_goal14_duplicate_code_date_and_non_open_trade_date_block_validator():
    from stock_selector.data.tushare_daily_price_promotion_validator import (
        build_tushare_daily_price_promotion_validator,
    )

    duplicated = pd.concat([_candidate_frame(), _candidate_frame().iloc[[0]]], ignore_index=True)
    duplicated_result = build_tushare_daily_price_promotion_validator(
        batch_id=BATCH_ID,
        promotion_preflight_report=_ready_preflight(),
        daily_price_candidate_batch=duplicated,
        suspension_status_candidate_batch=_suspension_candidate_frame(),
    )

    non_open = _candidate_frame()
    non_open.loc[0, "trade_date"] = "2024-06-01"
    non_open.loc[0, "trading_day_confirmed"] = False
    non_open_result = build_tushare_daily_price_promotion_validator(
        batch_id=BATCH_ID,
        promotion_preflight_report=_ready_preflight(),
        daily_price_candidate_batch=non_open,
        suspension_status_candidate_batch=_suspension_candidate_frame(),
    )

    assert "CANDIDATE_DUPLICATE_CODE_DATE" in duplicated_result["blocked_reasons"]
    assert "CANDIDATE_ROW_COUNT_MISMATCH" in duplicated_result["blocked_reasons"]
    assert "CANDIDATE_NON_OPEN_TRADE_DATE" in non_open_result["blocked_reasons"]


def test_goal14_small_range_guard_blocks_large_batches():
    from stock_selector.data.tushare_daily_price_promotion_validator import (
        build_tushare_daily_price_promotion_validator,
    )

    result = build_tushare_daily_price_promotion_validator(
        batch_id=BATCH_ID,
        promotion_preflight_report=_ready_preflight(),
        daily_price_candidate_batch=_candidate_frame(),
        suspension_status_candidate_batch=_suspension_candidate_frame(),
        max_rows=3,
    )

    assert result["status"] == "BLOCKED"
    assert "BATCH_TOO_LARGE_FOR_GOAL14_SMALL_RANGE_VALIDATOR" in result["blocked_reasons"]
    assert result["daily_price_promotion_validator_report"]["small_range_guard"]["passed"] is False


def test_goal14_requested_standard_write_requires_explicit_execute_flag():
    from stock_selector.data.tushare_daily_price_promotion_validator import (
        build_tushare_daily_price_promotion_validator,
    )

    writer = _StandardWriter()
    result = build_tushare_daily_price_promotion_validator(
        batch_id=BATCH_ID,
        promotion_preflight_report=_ready_preflight(),
        daily_price_candidate_batch=_candidate_frame(),
        suspension_status_candidate_batch=_suspension_candidate_frame(),
        request_standard_write=True,
        standard_daily_price_write_fn=writer.write,
    )

    assert result["status"] == "BLOCKED"
    assert "STANDARD_WRITE_REQUIRES_EXPLICIT_EXECUTE_FLAG" in result["blocked_reasons"]
    assert writer.calls == []


def test_goal14_explicit_execute_writes_only_standard_daily_price_and_never_suspension_status_or_downstream():
    from stock_selector.data.tushare_daily_price_promotion_validator import (
        build_tushare_daily_price_promotion_validator,
    )

    writer = _StandardWriter()
    result = build_tushare_daily_price_promotion_validator(
        batch_id=BATCH_ID,
        promotion_preflight_report=_ready_preflight(),
        daily_price_candidate_batch=_candidate_frame(),
        suspension_status_candidate_batch=_suspension_candidate_frame(),
        request_standard_write=True,
        execute_standard_write=True,
        standard_daily_price_write_fn=writer.write,
    )

    assert result["status"] == "VALIDATOR_PASS"
    assert result["standard_daily_price_write_performed"] is True
    assert sorted(call["dataset"] for call in writer.calls) == ["daily_price", "daily_price"]
    assert {call["trade_date"] for call in writer.calls} == set(TRADE_DATES)
    assert all(call["frame"]["is_paused"].notna().all() for call in writer.calls)
    execution_report = result["standard_daily_price_promotion_execution_report"]
    assert execution_report["schema_version"] == "goal14.standard_daily_price_promotion_execution_report.v1"
    assert execution_report["mode"] == "EXECUTE"
    assert execution_report["target_table"] == "daily_price"
    assert execution_report["standard_write_performed"] is True
    assert execution_report["written_rows"] == 4
    assert sorted(item["trade_date"] for item in execution_report["write_results"]) == TRADE_DATES
    assert result["standard_suspension_status_write_performed"] is False
    assert result["clean_factor_selection_backtest_entered"] is False
    assert "STANDARD_SUSPENSION_STATUS_WRITE_NOT_ALLOWED_IN_GOAL14" not in result["blocked_reasons"]
    assert "CLEAN_FACTOR_SELECTION_BACKTEST_NOT_ALLOWED_IN_GOAL14" not in result["blocked_reasons"]


def test_goal14_cli_writes_validator_and_dry_run_reports_under_candidate_paths(monkeypatch, tmp_path, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))
    _write_goal13c_artifacts(tmp_path, BATCH_ID)

    exit_code = main(["build-tushare-daily-price-promotion-validator", "--batch-id", BATCH_ID])

    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    assert output["goal"] == "14"
    assert output["status"] == "VALIDATOR_PASS"
    assert output["mode"] == "DRY_RUN"
    assert output["standard_daily_price_write_performed"] is False
    validator_key = output["daily_price_promotion_validator_report_key"]
    dry_run_key = output["standard_daily_price_promotion_dry_run_report_key"]
    assert validator_key == f"candidate/tushare/daily_price_promotion_validator_report/batch_id={BATCH_ID}/report.json"
    assert dry_run_key == f"candidate/tushare/standard_daily_price_promotion_dry_run_report/batch_id={BATCH_ID}/report.json"
    assert (tmp_path / validator_key).exists()
    assert (tmp_path / dry_run_key).exists()
    assert output["standard_daily_price_promotion_execution_report_key"] is None
    assert not validator_key.startswith("raw/")
    assert "suspension_status" not in dry_run_key
    assert "clean_daily_snapshot" not in json.dumps(output)
    assert "factor_daily" not in json.dumps(output)
    assert "selection_result" not in json.dumps(output)


def test_goal14_backward_compatibility_existing_goal13c_builder_still_emits_candidate_only_artifacts():
    from stock_selector.data.tushare_candidate_staging_batch import build_tushare_candidate_staging_batch_output_keys

    keys = build_tushare_candidate_staging_batch_output_keys(BATCH_ID, TRADE_DATES, goal13c_preflight=True)

    assert keys["daily_price_candidate_batch"].startswith("candidate/tushare/")
    assert keys["promotion_preflight_report"].startswith("candidate/tushare/")
    assert "daily_price_promotion_validator_report" not in keys
    assert "standard_daily_price_promotion_dry_run_report" not in keys
    assert not keys["daily_price_candidate_batch"].startswith("raw/daily_price")


class _StandardWriter:
    def __init__(self):
        self.calls = []

    def write(self, dataset, trade_date, frame):
        self.calls.append({"dataset": dataset, "trade_date": trade_date, "frame": frame.copy()})
        return f"raw/{dataset}/trade_date={trade_date}/part.parquet"


def _ready_preflight():
    return {
        "schema_version": "goal13c.promotion_preflight_report.v1",
        "goal": "13C",
        "batch_id": BATCH_ID,
        "provider": "tushare",
        "generated_at": "2026-06-29T00:00:00Z",
        "start_date": "2024-06-01",
        "end_date": "2024-06-05",
        "codes": CODES,
        "trade_dates": TRADE_DATES,
        "status": "READY_FOR_PROMOTION_VALIDATOR",
        "price_coverage": {
            "daily": _coverage(4),
            "stk_limit": _coverage(4),
            "adj_factor": _coverage(4),
            "daily_basic": _coverage(4),
        },
        "suspension_resolution": {
            "resolved": 4,
            "unknown": 0,
            "true_candidate": 1,
            "false_candidate": 3,
        },
        "blocked_reasons": [],
        "standard_daily_price_write_performed": False,
        "standard_suspension_status_write_performed": False,
        "real_backtest_performed": False,
        "ready_for_standard_write": False,
        "ready_for_real_backtest": False,
        "production_ready": False,
    }


def _coverage(count):
    return {
        "numerator": count,
        "denominator": count,
        "matched_rows": count,
        "missing_rows": 0,
        "ratio": 1.0,
        "coverage_rate": 1.0,
        "blocked_reason": None,
    }


def _candidate_frame():
    rows = []
    for trade_date in TRADE_DATES:
        rows.extend(
            [
                _candidate_row("000001.SZ", trade_date, 10.0, paused=(trade_date == "2024-06-03")),
                _candidate_row("600519.SH", trade_date, 1700.0, paused=False),
            ]
        )
    frame = pd.DataFrame(rows)
    frame["is_paused_candidate"] = frame["is_paused_candidate"].astype(object)
    return frame


def _candidate_row(code, trade_date, price, *, paused):
    resolution_source = "SUSPEND_D_EVENT_ROW" if paused else "SUSPEND_D_FULL_COVERAGE_MISS_AS_FALSE_CANDIDATE"
    pause_status = "true_candidate" if paused else "false_candidate"
    return {
        "ts_code": code,
        "trade_date": trade_date,
        "open": price,
        "high": price + 1,
        "low": price - 1,
        "close": price + 0.5,
        "pre_close": price - 0.5,
        "volume": 1000.0,
        "amount": 10000.0,
        "adj_factor": 1.1,
        "limit_up": price + 2,
        "limit_down": price - 2,
        "trading_day_confirmed": True,
        "pause_status": pause_status,
        "pause_candidate_status": pause_status,
        "is_paused_candidate": paused,
        "pause_evidence": "suspend_d_match" if paused else "full_event_coverage_no_match",
        "resolution_source": resolution_source,
        "resolution_reason": "audited source",
        "ready_for_daily_price_candidate_join": True,
        "daily_source_object_key": f"candidate/tushare/daily_staging/batch_id={BATCH_ID}/trade_date={trade_date}/part.parquet",
        "limit_source_object_key": f"candidate/tushare/stk_limit_staging/batch_id={BATCH_ID}/trade_date={trade_date}/part.parquet",
        "adj_factor_source_object_key": f"candidate/tushare/adj_factor_staging/batch_id={BATCH_ID}/trade_date={trade_date}/part.parquet",
        "daily_basic_source_object_key": f"candidate/tushare/daily_basic_staging/batch_id={BATCH_ID}/trade_date={trade_date}/part.parquet",
        "event_source_object_key": f"candidate/tushare/suspend_d_staging/batch_id={BATCH_ID}/part.parquet",
        "calendar_source_object_key": f"candidate/tushare/trade_cal_staging/batch_id={BATCH_ID}/part.parquet",
        "dq_level": "DQ1",
        "is_standard": False,
        "is_promotable": False,
        "generated_at": "2026-06-29T00:00:00Z",
    }


def _suspension_candidate_frame():
    return _candidate_frame()[
        [
            "ts_code",
            "trade_date",
            "pause_candidate_status",
            "is_paused_candidate",
            "pause_evidence",
            "resolution_source",
            "event_source_object_key",
            "calendar_source_object_key",
        ]
    ].copy()


def _write_goal13c_artifacts(root, batch_id):
    preflight_key = f"candidate/tushare/promotion_preflight_report/batch_id={batch_id}/report.json"
    daily_key = f"candidate/tushare/daily_price_candidate_batch/batch_id={batch_id}/part.parquet"
    suspension_key = f"candidate/tushare/suspension_status_candidate_batch/batch_id={batch_id}/part.parquet"
    preflight_path = root / preflight_key
    daily_path = root / daily_key
    suspension_path = root / suspension_key
    preflight_path.parent.mkdir(parents=True, exist_ok=True)
    daily_path.parent.mkdir(parents=True, exist_ok=True)
    suspension_path.parent.mkdir(parents=True, exist_ok=True)
    preflight_path.write_text(json.dumps(_ready_preflight(), ensure_ascii=False), encoding="utf-8")
    _candidate_frame().to_parquet(daily_path, index=False)
    _suspension_candidate_frame().to_parquet(suspension_path, index=False)

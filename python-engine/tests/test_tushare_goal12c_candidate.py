import json

import pandas as pd

from stock_selector.cli import main
from stock_selector.storage.partition import build_partition, build_provider_smoke_partition


TRADE_DATE = "2024-06-19"


def test_goal12c_dry_run_reads_smoke_fixtures_and_reports_candidate_join():
    from stock_selector.data.tushare_daily_price_candidate import (
        SmokeInput,
        dry_run_tushare_daily_price_candidate,
    )

    report = dry_run_tushare_daily_price_candidate(
        TRADE_DATE,
        sample_limit=5,
        load_smoke_input_fn=lambda dataset, trade_date: SmokeInput(
            dataset=dataset,
            object_key=f"smoke/tushare/{dataset}/trade_date={trade_date}/part.parquet",
            frame=_frame_for(dataset),
        ),
    )

    assert report["status"] == "DRY_RUN_COMPLETED"
    assert "report_key" not in report
    assert "candidate_key" not in report
    assert "output_keys" not in report
    assert report["output_object_keys"] == {
        "report": f"smoke/tushare/daily_price_candidate_dry_run/trade_date={TRADE_DATE}/report.json",
        "candidate": f"smoke/tushare/daily_price_candidate_dry_run/trade_date={TRADE_DATE}/part.parquet",
    }
    assert report["input_smoke_object_keys"]["daily"] == f"smoke/tushare/daily/trade_date={TRADE_DATE}/part.parquet"
    assert report["inputs"]["daily"]["object_key"] == f"smoke/tushare/daily/trade_date={TRADE_DATE}/part.parquet"
    assert "path" not in report["inputs"]["daily"]
    assert report["contract"]["provider"] == "tushare"
    assert "provider_name" not in report["contract"]
    assert report["join"]["join_keys"] == ["ts_code", "trade_date"]
    assert report["join"]["candidate_row_count"] == 4
    assert report["inputs"]["daily"]["row_count"] == 4
    assert report["inputs"]["stk_limit"]["row_count"] == 4
    assert report["inputs"]["adj_factor"]["row_count"] == 3
    assert report["inputs"]["trade_cal"]["row_count"] == 1
    assert report["inputs"]["suspend_d"]["row_count"] == 2
    assert report["field_sources"]["open"] == "daily"
    assert report["field_sources"]["high"] == "daily"
    assert report["field_sources"]["low"] == "daily"
    assert report["field_sources"]["close"] == "daily"
    assert report["field_sources"]["pre_close"] == "daily"
    assert report["field_sources"]["volume"] == "daily"
    assert report["field_sources"]["amount"] == "daily"
    assert report["field_sources"]["limit_up"] == "stk_limit"
    assert report["field_sources"]["limit_down"] == "stk_limit"
    assert report["field_sources"]["adj_factor"] == "adj_factor"
    assert report["field_sources"]["trading_day_confirmed"] == "trade_cal"
    assert report["field_sources"]["trade_cal_is_open"] == "trade_cal"
    assert report["field_sources"]["trade_cal_exchange"] == "trade_cal"
    assert report["field_sources"]["pause_status"] == "suspend_d_hit_or_unresolved_unknown"
    assert report["field_sources"]["is_paused_true_candidate"] == "suspend_d_hit_only"
    assert report["field_sources"]["suspend_type"] == "suspend_d"
    assert report["field_sources"]["suspend_timing"] == "suspend_d"
    assert set(report["field_sources"]).issubset(set(report["candidate_rows"][0]))
    assert "trading_day" not in report["field_sources"]
    assert "pause_status_true_candidate" not in report["field_sources"]
    assert report["coverage"]["limit_price_fields"]["all_fields"]["coverage_rate"] == 1.0
    assert report["coverage"]["limit_price_fields"]["limit_up"]["coverage_rate"] == 1.0
    assert report["coverage"]["limit_price_fields"]["limit_down"]["coverage_rate"] == 1.0
    assert report["coverage"]["adj_factor"]["matched_rows"] == 3
    assert report["coverage"]["suspend_d_match"]["matched_rows"] == 1
    assert report["pause_status_counts"] == {"true_candidate": 1, "unknown": 3, "false": 0}
    assert "pause_status" not in report["missing_field_stats"]
    assert "is_paused_true_candidate" not in report["missing_field_stats"]
    assert report["readiness"]["ready_for_dq3_promotion"] is False
    assert report["readiness"]["status"] == "BLOCKED_BY_UNRESOLVED_IS_PAUSED"
    assert report["safety"]["standard_daily_price_written"] is False
    assert report["safety"]["real_raw_mainline_written"] is False
    assert report["safety"]["cleaning_mainline_entered"] is False
    assert report["safety"]["factor_mainline_entered"] is False
    assert report["safety"]["selection_mainline_entered"] is False
    assert report["safety"]["backtest_mainline_entered"] is False
    assert report["safety"]["spring_api_changed"] is False


def test_goal12c_missing_daily_returns_missing_input_without_candidate():
    from stock_selector.data.tushare_daily_price_candidate import (
        SmokeInput,
        dry_run_tushare_daily_price_candidate,
    )

    def loader(dataset, trade_date):
        frame = None if dataset == "daily" else _frame_for(dataset)
        return SmokeInput(dataset=dataset, object_key=f"smoke/tushare/{dataset}/trade_date={trade_date}/part.parquet", frame=frame)

    report = dry_run_tushare_daily_price_candidate(TRADE_DATE, sample_limit=5, load_smoke_input_fn=loader)

    assert report["status"] == "BLOCKED_BY_MISSING_SMOKE_INPUT"
    assert report["missing_inputs"] == [{"dataset": "daily", "object_key": f"smoke/tushare/daily/trade_date={TRADE_DATE}/part.parquet"}]
    assert report["join"]["candidate_row_count"] == 0
    assert report["readiness"]["ready_for_dq3_promotion"] is False
    assert report["readiness"]["status"] == "BLOCKED_BY_MISSING_SMOKE_INPUT"


def test_goal12c_missing_stk_limit_cannot_promote_candidate():
    from stock_selector.data.tushare_daily_price_candidate import (
        SmokeInput,
        dry_run_tushare_daily_price_candidate,
    )

    def loader(dataset, trade_date):
        frame = None if dataset == "stk_limit" else _frame_for(dataset)
        return SmokeInput(dataset=dataset, object_key=f"smoke/tushare/{dataset}/trade_date={trade_date}/part.parquet", frame=frame)

    report = dry_run_tushare_daily_price_candidate(TRADE_DATE, sample_limit=5, load_smoke_input_fn=loader)

    assert report["status"] == "BLOCKED_BY_MISSING_SMOKE_INPUT"
    assert report["missing_inputs"] == [{"dataset": "stk_limit", "object_key": f"smoke/tushare/stk_limit/trade_date={TRADE_DATE}/part.parquet"}]
    assert report["readiness"]["ready_for_dq3_promotion"] is False


def test_goal12c_missing_adj_factor_reports_adjustment_coverage_gap():
    from stock_selector.data.tushare_daily_price_candidate import (
        SmokeInput,
        dry_run_tushare_daily_price_candidate,
    )

    def loader(dataset, trade_date):
        frame = _frame_for(dataset)
        if dataset == "adj_factor":
            frame = frame.iloc[0:0].copy()
        return SmokeInput(dataset=dataset, object_key=f"smoke/tushare/{dataset}/trade_date={trade_date}/part.parquet", frame=frame)

    report = dry_run_tushare_daily_price_candidate(TRADE_DATE, sample_limit=5, load_smoke_input_fn=loader)

    assert report["coverage"]["adj_factor"]["matched_rows"] == 0
    assert report["coverage"]["adj_factor"]["missing_rows"] == 4
    assert report["missing_field_stats"]["adj_factor"] == 4
    assert "adj_factor coverage is incomplete" in report["readiness"]["reasons"]
    assert report["readiness"]["ready_for_dq3_promotion"] is False


def test_goal12c_missing_trade_cal_cannot_confirm_trading_day():
    from stock_selector.data.tushare_daily_price_candidate import (
        SmokeInput,
        dry_run_tushare_daily_price_candidate,
    )

    def loader(dataset, trade_date):
        frame = None if dataset == "trade_cal" else _frame_for(dataset)
        return SmokeInput(dataset=dataset, object_key=f"smoke/tushare/{dataset}/trade_date={trade_date}/part.parquet", frame=frame)

    report = dry_run_tushare_daily_price_candidate(TRADE_DATE, sample_limit=5, load_smoke_input_fn=loader)

    assert report["status"] == "BLOCKED_BY_MISSING_SMOKE_INPUT"
    assert report["readiness"]["status"] == "BLOCKED_BY_MISSING_SMOKE_INPUT"
    assert report["readiness"]["ready_for_dq3_promotion"] is False


def test_goal12c_trade_cal_is_not_used_as_pause_source():
    from stock_selector.data.tushare_daily_price_candidate import (
        SmokeInput,
        dry_run_tushare_daily_price_candidate,
    )

    report = dry_run_tushare_daily_price_candidate(
        TRADE_DATE,
        sample_limit=5,
        load_smoke_input_fn=lambda dataset, trade_date: SmokeInput(
            dataset=dataset,
            object_key=f"smoke/tushare/{dataset}/trade_date={trade_date}/part.parquet",
            frame=_frame_for(dataset),
        ),
    )

    assert report["coverage"]["trade_cal"]["pause_source"] is False
    assert report["field_sources"]["trading_day_confirmed"] == "trade_cal"
    assert report["field_sources"]["trade_cal_is_open"] == "trade_cal"
    assert report["field_sources"]["pause_status"] == "suspend_d_hit_or_unresolved_unknown"
    assert report["field_sources"]["is_paused_true_candidate"] == "suspend_d_hit_only"


def test_goal12c_suspend_hit_is_true_candidate_and_miss_is_unknown_not_false():
    from stock_selector.data.tushare_daily_price_candidate import (
        SmokeInput,
        dry_run_tushare_daily_price_candidate,
    )

    report = dry_run_tushare_daily_price_candidate(
        TRADE_DATE,
        sample_limit=5,
        load_smoke_input_fn=lambda dataset, trade_date: SmokeInput(
            dataset=dataset,
            object_key=f"smoke/tushare/{dataset}/trade_date={trade_date}/part.parquet",
            frame=_frame_for(dataset),
        ),
    )
    rows = {row["ts_code"]: row for row in report["candidate_rows"]}

    assert rows["000001.SZ"]["pause_status"] == "true_candidate"
    assert rows["000001.SZ"]["is_paused_true_candidate"] is True
    assert rows["000002.SZ"]["pause_status"] == "unknown"
    assert rows["000002.SZ"]["is_paused_true_candidate"] is None
    assert rows["000003.SZ"]["pause_status"] == "unknown"
    assert rows["000004.SZ"]["pause_status"] == "unknown"
    assert "is_paused" not in rows["000002.SZ"]
    assert report["pause_status_counts"]["false"] == 0


def test_goal12c_zero_volume_zero_amount_missing_daily_and_unchanged_price_do_not_infer_pause():
    from stock_selector.data.tushare_daily_price_candidate import (
        SmokeInput,
        dry_run_tushare_daily_price_candidate,
    )

    report = dry_run_tushare_daily_price_candidate(
        TRADE_DATE,
        sample_limit=5,
        load_smoke_input_fn=lambda dataset, trade_date: SmokeInput(
            dataset=dataset,
            object_key=f"smoke/tushare/{dataset}/trade_date={trade_date}/part.parquet",
            frame=_frame_for(dataset),
        ),
    )
    rows = {row["ts_code"]: row for row in report["candidate_rows"]}

    assert rows["000002.SZ"]["volume"] == 0
    assert rows["000002.SZ"]["pause_status"] == "unknown"
    assert rows["000003.SZ"]["amount"] == 0
    assert rows["000003.SZ"]["pause_status"] == "unknown"
    assert rows["000004.SZ"]["open"] == rows["000004.SZ"]["close"] == rows["000004.SZ"]["pre_close"]
    assert rows["000004.SZ"]["pause_status"] == "unknown"
    assert report["coverage"]["suspend_d_events_not_in_daily"]["row_count"] == 1
    assert report["inference_guards"]["volume_zero_used_as_pause"] is False
    assert report["inference_guards"]["amount_zero_used_as_pause"] is False
    assert report["inference_guards"]["daily_missing_row_used_as_pause"] is False
    assert report["inference_guards"]["unchanged_price_used_as_pause"] is False


def test_goal12c_output_path_is_diagnostic_and_not_standard_or_downstream():
    from stock_selector.data.tushare_daily_price_candidate import build_dry_run_output_keys

    keys = build_dry_run_output_keys(TRADE_DATE)

    assert keys["report"] == f"smoke/tushare/daily_price_candidate_dry_run/trade_date={TRADE_DATE}/report.json"
    assert keys["candidate"] == f"smoke/tushare/daily_price_candidate_dry_run/trade_date={TRADE_DATE}/part.parquet"
    assert not keys["report"].startswith("raw/daily_price/")
    assert "clean_daily_snapshot" not in keys["report"]
    assert "factor_daily" not in keys["report"]
    assert "selection_result" not in keys["report"]
    assert build_partition("daily_price", TRADE_DATE).object_key == f"raw/daily_price/trade_date={TRADE_DATE}/part.parquet"
    assert build_partition("clean_daily_snapshot", TRADE_DATE).object_key != keys["candidate"]
    assert build_partition("factor_daily", TRADE_DATE).object_key != keys["candidate"]
    assert build_partition("selection_result", TRADE_DATE).object_key != keys["candidate"]


def test_goal12c_cli_reads_local_smoke_and_writes_diagnostic_report(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("STOCK_PARQUET_BACKEND", "local")
    monkeypatch.setenv("STOCK_LOCAL_DATA_DIR", str(tmp_path))

    for dataset in ("daily", "stk_limit", "adj_factor", "trade_cal", "suspend_d"):
        partition = build_provider_smoke_partition("tushare", dataset, TRADE_DATE, local_root=tmp_path)
        partition.local_path.parent.mkdir(parents=True, exist_ok=True)
        _frame_for(dataset).to_parquet(partition.local_path, index=False)

    exit_code = main(["dry-run-tushare-daily-price-candidate", "--trade-date", TRADE_DATE, "--sample-limit", "5"])

    assert exit_code == 0
    output = json.loads(capsys.readouterr().out)
    report_path = tmp_path / output["report_key"]
    candidate_path = tmp_path / output["candidate_key"]
    assert output["status"] == "DRY_RUN_COMPLETED"
    assert output["ready_for_dq3_promotion"] is False
    assert report_path.exists()
    assert candidate_path.exists()
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert "report_key" not in report
    assert "candidate_key" not in report
    assert "output_keys" not in report
    assert report["output_object_keys"] == {
        "report": output["report_key"],
        "candidate": output["candidate_key"],
    }
    assert report["join"]["candidate_row_count"] == 4
    assert report["readiness"]["status"] == "BLOCKED_BY_UNRESOLVED_IS_PAUSED"
    assert "is_paused_true_candidate" not in report["missing_field_stats"]
    assert "raw/daily_price" not in output["report_key"]
    assert "processed/selection_result" not in output["candidate_key"]


def _frame_for(dataset):
    frames = {
        "daily": pd.DataFrame(
            [
                {
                    "ts_code": "000001.SZ",
                    "trade_date": "20240619",
                    "open": 10.1,
                    "high": 10.2,
                    "low": 10.0,
                    "close": 10.15,
                    "pre_close": 10.05,
                    "vol": 1000.0,
                    "amount": 10000.0,
                },
                {
                    "ts_code": "000002.SZ",
                    "trade_date": "20240619",
                    "open": 9.1,
                    "high": 9.2,
                    "low": 9.0,
                    "close": 9.15,
                    "pre_close": 9.05,
                    "vol": 0.0,
                    "amount": 9000.0,
                },
                {
                    "ts_code": "000003.SZ",
                    "trade_date": "20240619",
                    "open": 8.1,
                    "high": 8.2,
                    "low": 8.0,
                    "close": 8.15,
                    "pre_close": 8.05,
                    "vol": 800.0,
                    "amount": 0.0,
                },
                {
                    "ts_code": "000004.SZ",
                    "trade_date": "20240619",
                    "open": 7.0,
                    "high": 7.0,
                    "low": 7.0,
                    "close": 7.0,
                    "pre_close": 7.0,
                    "vol": 700.0,
                    "amount": 7000.0,
                },
            ]
        ),
        "stk_limit": pd.DataFrame(
            [
                {"ts_code": "000001.SZ", "trade_date": "20240619", "up_limit": 11.1, "down_limit": 9.1},
                {"ts_code": "000002.SZ", "trade_date": "20240619", "up_limit": 10.1, "down_limit": 8.1},
                {"ts_code": "000003.SZ", "trade_date": "20240619", "up_limit": 9.1, "down_limit": 7.1},
                {"ts_code": "000004.SZ", "trade_date": "20240619", "up_limit": 8.1, "down_limit": 6.1},
            ]
        ),
        "adj_factor": pd.DataFrame(
            [
                {"ts_code": "000001.SZ", "trade_date": "20240619", "adj_factor": 120.0},
                {"ts_code": "000002.SZ", "trade_date": "20240619", "adj_factor": 121.0},
                {"ts_code": "000003.SZ", "trade_date": "20240619", "adj_factor": 122.0},
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

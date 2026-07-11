from __future__ import annotations

from collections import Counter
from copy import deepcopy
from datetime import date, timedelta
import importlib
import json

import numpy as np
import pandas as pd
import pytest

from stock_selector.data import historical_backfill as backfill
from stock_selector.data.historical_backfill import (
    PLANNER_VERSION,
    build_history_backfill_output_keys,
    build_history_backfill_plan,
)
from stock_selector.data.real_clean_inputs_landing import KEY_COLUMNS, REQUIRED_INPUTS


CODES = ["000001.SZ", "600519.SH"]
BENCHMARK_INDEXES = ["000300.SH", "000905.SH", "000906.SH"]
ALL_DATASETS = [
    "stock_basic",
    "daily_price",
    "adj_factor",
    "daily_basic",
    "financial",
    "st_history",
    "benchmark_price",
]


@pytest.mark.parametrize(
    ("start_date", "end_date"),
    [
        ("2020-01-01", "2024-12-31"),
        ("2015-01-01", "2024-12-31"),
    ],
)
def test_planner_accepts_exact_five_and_ten_calendar_year_scopes(start_date, end_date):
    plan = build_history_backfill_plan(
        run_id="history-scale-proof",
        start_date=start_date,
        end_date=end_date,
        codes=CODES,
        code_batch_size=10,
        date_batch_days=4000,
        report_period_months=120,
        generated_at_fn=lambda: "2026-07-11T00:00:00Z",
    )

    assert plan["planner_version"] == PLANNER_VERSION
    assert plan["scope"]["start_date"] == start_date
    assert plan["scope"]["end_date"] == end_date
    assert plan["scope"]["date_count"] == (date.fromisoformat(end_date) - date.fromisoformat(start_date)).days + 1
    assert plan["datasets"] == list(REQUIRED_INPUTS)
    assert {chunk["dataset"] for chunk in plan["chunks"]} == set(REQUIRED_INPUTS)


def test_one_day_incremental_plan_keeps_every_chunk_inside_that_day():
    plan = build_history_backfill_plan(
        run_id="one-day",
        start_date="2024-06-03",
        end_date="2024-06-03",
        codes=CODES,
        code_batch_size=1,
        date_batch_days=1,
        report_period_months=1,
    )

    assert plan["scope"]["date_count"] == 1
    assert plan["scope"]["codes"] == CODES
    for chunk in plan["chunks"]:
        assert chunk["start_date"] == "2024-06-03"
        assert chunk["end_date"] == "2024-06-03"
        if chunk["dataset"] == "financial":
            assert chunk["report_period_start"] == "2024-06-03"
            assert chunk["report_period_end"] == "2024-06-03"


def test_plan_fingerprint_and_chunk_ids_ignore_run_timestamp_and_input_code_order():
    common = {
        "start_date": "2024-01-01",
        "end_date": "2024-03-31",
        "code_batch_size": 1,
        "date_batch_days": 31,
        "report_period_months": 2,
    }
    first = build_history_backfill_plan(
        run_id="first-run",
        codes=["600519.sh", "000001.sz", "600519.SH"],
        generated_at_fn=lambda: "2026-07-11T00:00:00Z",
        **common,
    )
    second = build_history_backfill_plan(
        run_id="second-run",
        codes=["000001.SZ", "600519.SH"],
        generated_at_fn=lambda: "2030-01-01T00:00:00Z",
        **common,
    )

    assert first["scope"]["codes"] == CODES
    assert first["plan_fingerprint"] == second["plan_fingerprint"]
    assert [item["chunk_id"] for item in first["chunks"]] == [item["chunk_id"] for item in second["chunks"]]
    assert first["run_id"] != second["run_id"]
    assert first["generated_at"] != second["generated_at"]


def test_code_list_and_universe_frame_are_mutually_exclusive_and_one_is_required():
    universe = pd.DataFrame({"stock_code": ["600519.sh", "000001.SZ", "600519.SH"]})

    with pytest.raises(ValueError, match="exactly one"):
        _plan(codes=None, universe_frame=None)
    with pytest.raises(ValueError, match="exactly one"):
        _plan(codes=CODES, universe_frame=universe)

    from_universe = _plan(codes=None, universe_frame=universe, universe_key="raw/universe/snapshot.parquet")
    assert from_universe["scope"]["codes"] == CODES
    assert from_universe["scope"]["universe_source"] == "universe_frame"
    assert from_universe["scope"]["universe_key"] == "raw/universe/snapshot.parquet"


def test_universe_frame_requires_stock_code_and_nonempty_normalized_codes():
    with pytest.raises(ValueError, match="stock_code"):
        _plan(codes=None, universe_frame=pd.DataFrame({"symbol": ["000001.SZ"]}))
    with pytest.raises(ValueError, match="must not be empty"):
        _plan(codes=None, universe_frame=pd.DataFrame({"stock_code": []}))


@pytest.mark.parametrize(
    "codes",
    [
        pd.Series(["600519.sh", "000001.SZ", "600519.SH"]),
        pd.Index(["600519.sh", "000001.SZ", "600519.SH"]),
        (code for code in ["600519.sh", "000001.SZ", "600519.SH"]),
    ],
    ids=["series", "index", "ordinary-iterable"],
)
def test_codes_accept_series_index_and_ordinary_iterables_without_truth_evaluation(codes):
    assert _plan(codes=codes)["scope"]["codes"] == CODES


def test_universe_frame_rejects_duplicate_stock_code_columns_with_stable_error():
    universe = pd.DataFrame(
        [["000001.SZ", "600519.SH"]],
        columns=["stock_code", "stock_code"],
    )

    _assert_error_code(
        "INVALID_UNIVERSE_FRAME",
        lambda: _plan(codes=None, universe_frame=universe),
    )


def test_universe_key_accepts_relative_logical_object_keys():
    plan = _plan(
        codes=None,
        universe_frame=pd.DataFrame({"stock_code": CODES}),
        universe_key="raw/universe/snapshot.parquet",
    )

    assert plan["scope"]["universe_key"] == "raw/universe/snapshot.parquet"


@pytest.mark.parametrize(
    "universe_key",
    [
        "",
        " ",
        "/raw/universe/snapshot.parquet",
        "C:/raw/universe/snapshot.parquet",
        "raw\\universe\\snapshot.parquet",
        "raw/./snapshot.parquet",
        "raw/../snapshot.parquet",
        "../snapshot.parquet",
        "raw//snapshot.parquet",
        "raw/universe/",
        "raw/universe/part\x00.parquet",
    ],
)
def test_universe_key_rejects_non_logical_or_traversing_paths(universe_key):
    _assert_error_code(
        "UNSAFE_UNIVERSE_KEY",
        lambda: _plan(
            codes=None,
            universe_frame=pd.DataFrame({"stock_code": CODES}),
            universe_key=universe_key,
        ),
    )


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("code_batch_size", 0),
        ("code_batch_size", -1),
        ("date_batch_days", 0),
        ("date_batch_days", -1),
        ("report_period_months", 0),
        ("report_period_months", -1),
    ],
)
def test_planner_rejects_non_positive_chunk_limits(name, value):
    kwargs = {
        "code_batch_size": 2,
        "date_batch_days": 31,
        "report_period_months": 3,
        name: value,
    }
    with pytest.raises(ValueError, match=f"{name} must be positive"):
        _plan(**kwargs)


def test_dataset_strategies_and_key_contracts_are_explicit():
    expected = {
        "stock_basic": "point_in_time_snapshot_by_code_date_window",
        "daily_price": "mixed_price_by_code_date_window",
        "adj_factor": "by_code_date_window",
        "daily_basic": "by_code_date_window",
        "financial": "by_code_report_period_window",
        "st_history": "by_code_interval_window",
        "benchmark_price": "by_index_date_window",
    }
    plan = _plan(date_batch_days=400, report_period_months=24)

    assert plan["strategies"] == expected
    for chunk in plan["chunks"]:
        assert chunk["strategy"] == expected[chunk["dataset"]]
        assert chunk["key_columns"] == KEY_COLUMNS[chunk["dataset"]]


def test_date_and_report_windows_are_contiguous_inclusive_and_cover_the_full_range():
    plan = _plan(
        start_date="2024-01-15",
        end_date="2024-07-04",
        datasets=["daily_price", "financial", "benchmark_price"],
        code_batch_size=2,
        date_batch_days=40,
        report_period_months=2,
    )

    daily = [chunk for chunk in plan["chunks"] if chunk["dataset"] == "daily_price"]
    benchmark = [chunk for chunk in plan["chunks"] if chunk["dataset"] == "benchmark_price"]
    financial = [chunk for chunk in plan["chunks"] if chunk["dataset"] == "financial"]
    _assert_contiguous(daily, "start_date", "end_date", "2024-01-15", "2024-07-04")
    _assert_contiguous(benchmark, "start_date", "end_date", "2024-01-15", "2024-07-04")
    _assert_contiguous(financial, "report_period_start", "report_period_end", "2024-01-15", "2024-07-04")


def test_financial_month_windows_stay_anchored_to_january_31_across_leap_february():
    plan = _plan(
        start_date="2024-01-31",
        end_date="2024-06-30",
        datasets=["financial"],
        report_period_months=1,
    )

    assert [
        (chunk["report_period_start"], chunk["report_period_end"])
        for chunk in plan["chunks"]
    ] == [
        ("2024-01-31", "2024-02-28"),
        ("2024-02-29", "2024-03-30"),
        ("2024-03-31", "2024-04-29"),
        ("2024-04-30", "2024-05-30"),
        ("2024-05-31", "2024-06-29"),
        ("2024-06-30", "2024-06-30"),
    ]


@pytest.mark.parametrize(
    ("start_date", "end_date"),
    [
        ("9999-12-31", "9999-12-31"),
        ("2024-01-01", "9999-12-31"),
    ],
)
def test_window_planning_saturates_at_date_upper_bound_with_huge_positive_limits(start_date, end_date):
    plan = _plan(
        start_date=start_date,
        end_date=end_date,
        datasets=["daily_price", "financial"],
        date_batch_days=10**30,
        report_period_months=10**30,
    )

    assert [(chunk["start_date"], chunk["end_date"]) for chunk in plan["chunks"]] == [
        (start_date, end_date),
        (start_date, end_date),
    ]


@pytest.mark.parametrize("dataset", ALL_DATASETS)
def test_each_dataset_respects_window_limits_and_exact_window_code_cartesian_product(dataset):
    codes = ["601318.SH", "000003.SZ", "000001.SZ", "600519.SH", "000002.SZ"]
    plan = _plan(
        start_date="2024-01-31",
        end_date="2024-08-05",
        codes=codes,
        datasets=[dataset],
        code_batch_size=2,
        date_batch_days=31,
        report_period_months=2,
    )
    chunks = plan["chunks"]
    assert len({chunk["chunk_id"] for chunk in chunks}) == len(chunks)

    if dataset == "financial":
        expected_windows = [
            ("2024-01-31", "2024-03-30"),
            ("2024-03-31", "2024-05-30"),
            ("2024-05-31", "2024-07-30"),
            ("2024-07-31", "2024-08-05"),
        ]
        actual = Counter(
            (
                chunk["report_period_start"],
                chunk["report_period_end"],
                tuple(chunk["codes"]),
            )
            for chunk in chunks
        )
    else:
        expected_windows = _expected_day_windows("2024-01-31", "2024-08-05", 31)
        assert all(
            (date.fromisoformat(end) - date.fromisoformat(start)).days + 1 <= 31
            for start, end in expected_windows
        )
        if dataset == "benchmark_price":
            assert all(chunk["codes"] == [] for chunk in chunks)
            assert all(chunk["index_codes"] == BENCHMARK_INDEXES for chunk in chunks)
            assert Counter((chunk["start_date"], chunk["end_date"]) for chunk in chunks) == Counter(expected_windows)
            return
        actual = Counter(
            (chunk["start_date"], chunk["end_date"], tuple(chunk["codes"]))
            for chunk in chunks
        )

    expected_batches = [
        ("000001.SZ", "000002.SZ"),
        ("000003.SZ", "600519.SH"),
        ("601318.SH",),
    ]
    expected = Counter(
        (window_start, window_end, code_batch)
        for window_start, window_end in expected_windows
        for code_batch in expected_batches
    )
    assert actual == expected


def test_chunk_ids_are_unique_across_the_full_seven_dataset_plan():
    plan = _plan(
        start_date="2024-01-01",
        end_date="2024-05-31",
        codes=["000001.SZ", "000002.SZ", "600519.SH"],
        code_batch_size=2,
        date_batch_days=31,
        report_period_months=2,
    )
    chunk_ids = [chunk["chunk_id"] for chunk in plan["chunks"]]

    assert len(chunk_ids) == len(set(chunk_ids))


def test_code_shards_cover_each_code_once_per_window_and_benchmark_is_not_code_sharded():
    plan = _plan(
        codes=["000001.SZ", "000002.SZ", "600519.SH"],
        datasets=["adj_factor", "benchmark_price"],
        code_batch_size=2,
        date_batch_days=400,
    )
    adj_chunks = [chunk for chunk in plan["chunks"] if chunk["dataset"] == "adj_factor"]
    benchmark_chunks = [chunk for chunk in plan["chunks"] if chunk["dataset"] == "benchmark_price"]

    assert [chunk["codes"] for chunk in adj_chunks] == [
        ["000001.SZ", "000002.SZ"],
        ["600519.SH"],
    ]
    assert len(benchmark_chunks) == 1
    assert benchmark_chunks[0]["codes"] == []
    assert benchmark_chunks[0]["index_codes"] == list(BENCHMARK_INDEXES)


def test_output_keys_are_safe_and_include_all_control_and_attempt_templates():
    plan = _plan(datasets=["adj_factor", "benchmark_price"], date_batch_days=400)
    keys = build_history_backfill_output_keys("goal21.safe_run-1", plan["chunks"])
    prefix = "candidate/real_history_backfill/run_id=goal21.safe_run-1/"

    assert keys["root_prefix"] == prefix
    assert keys["plan"] == f"{prefix}plan.json"
    assert keys["root_manifest"] == f"{prefix}manifest.json"
    assert set(keys["chunks"]) == {chunk["chunk_id"] for chunk in plan["chunks"]}
    for chunk in plan["chunks"]:
        chunk_keys = keys["chunks"][chunk["chunk_id"]]
        chunk_prefix = f"{prefix}dataset={chunk['dataset']}/chunk_id={chunk['chunk_id']}/"
        assert chunk_keys == {
            "manifest": f"{chunk_prefix}manifest.json",
            "attempt_report_template": f"{chunk_prefix}attempt={{attempt}}/report.json",
            "staging_template": f"{chunk_prefix}attempt={{attempt}}/part.parquet",
        }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("strategy", "tampered_strategy"),
        ("end_date", "2024-12-30"),
        ("codes", ["000002.SZ"]),
        ("key_columns", ["stock_code"]),
        ("dataset", "adj_factor"),
        ("extra_semantic_field", "forged"),
    ],
)
def test_output_keys_recompute_chunk_id_from_full_chunk_semantics(field, value):
    chunk = dict(_plan(datasets=["daily_price"], date_batch_days=400)["chunks"][0])
    chunk[field] = value

    _assert_error_code(
        "TAMPERED_CHUNK_ID",
        lambda: build_history_backfill_output_keys("safe-run", [chunk]),
    )


def test_output_keys_distinguish_invalid_duplicate_and_tampered_chunk_ids():
    chunk = _plan(datasets=["daily_price"], date_batch_days=400)["chunks"][0]
    invalid = dict(chunk, chunk_id="../unsafe")
    forged = dict(chunk, chunk_id="daily_price-00000000000000000000")

    _assert_error_code(
        "INVALID_CHUNK_ID",
        lambda: build_history_backfill_output_keys("safe-run", [invalid]),
    )
    _assert_error_code(
        "DUPLICATE_CHUNK_ID",
        lambda: build_history_backfill_output_keys("safe-run", [chunk, dict(chunk)]),
    )
    _assert_error_code(
        "TAMPERED_CHUNK_ID",
        lambda: build_history_backfill_output_keys("safe-run", [forged]),
    )


def test_output_keys_wrap_invalid_chunk_field_types_in_stable_planning_error():
    chunk = dict(_plan(datasets=["daily_price"], date_batch_days=400)["chunks"][0])
    chunk["key_columns"] = None

    _assert_error_code(
        "TAMPERED_CHUNK_ID",
        lambda: build_history_backfill_output_keys("safe-run", [chunk]),
    )


def test_small_plan_has_golden_identity_values():
    plan = _golden_plan()

    assert plan["identity_schema_version"] == "goal21.history_backfill_identity.v1"
    assert plan["chunks"][0]["chunk_id"] == "daily_price-2e42fb2a7902e7e9d016"
    assert plan["plan_fingerprint"] == "0759cae404beeb021d03a40a49ef0a8596ac25f7dbc3008f6ffca6388dcac9ee"


def test_plan_identity_changes_with_scope_limits_dataset_and_universe_lineage():
    baseline = _golden_plan()["plan_fingerprint"]
    variants = [
        _golden_plan(start_date="2024-01-30")["plan_fingerprint"],
        _golden_plan(date_batch_days=30)["plan_fingerprint"],
        _golden_plan(datasets=["adj_factor"])["plan_fingerprint"],
        _golden_plan(universe_key="raw/universe/other.parquet")["plan_fingerprint"],
    ]

    assert all(fingerprint != baseline for fingerprint in variants)
    assert len(set(variants)) == len(variants)


def test_planning_error_type_exposes_a_stable_machine_readable_code():
    error_type = getattr(backfill, "BackfillPlanningError", None)
    assert error_type is not None

    error = error_type("EXAMPLE_CONFIGURATION_ERROR", "human-readable detail")
    assert isinstance(error, ValueError)
    assert error.code == "EXAMPLE_CONFIGURATION_ERROR"
    assert str(error) == "human-readable detail"


@pytest.mark.parametrize(
    ("expected_code", "action"),
    [
        ("INVALID_DATE_RANGE", lambda: _plan(start_date="2024-13-01")),
        ("INVALID_STOCK_CODE", lambda: _plan(codes=["not-a-stock-code"])),
        ("INVALID_SCOPE", lambda: _plan(codes=None, universe_frame=None)),
        ("EMPTY_UNIVERSE", lambda: _plan(codes=[])),
        ("INVALID_LIMIT", lambda: _plan(code_batch_size=0)),
        ("UNSUPPORTED_DATASET", lambda: _plan(datasets=["factor_daily"])),
        ("UNSAFE_RUN_ID", lambda: _plan(run_id="../escape")),
        (
            "UNSAFE_UNIVERSE_KEY",
            lambda: _plan(
                codes=None,
                universe_frame=pd.DataFrame({"stock_code": CODES}),
                universe_key="../escape.parquet",
            ),
        ),
        (
            "INVALID_CHUNK",
            lambda: build_history_backfill_output_keys("safe-run", ["not-a-chunk"]),
        ),
    ],
)
def test_planner_configuration_failures_have_stable_error_codes(expected_code, action):
    _assert_error_code(expected_code, action)


@pytest.mark.parametrize("run_id", ["", "../escape", "a/b", "with space", ".hidden"])
def test_planner_and_key_builder_reject_unsafe_run_ids(run_id):
    with pytest.raises(ValueError, match="run_id"):
        _plan(run_id=run_id)
    with pytest.raises(ValueError, match="run_id"):
        build_history_backfill_output_keys(run_id, [])


def test_dataset_selection_is_normalized_to_contract_order_and_rejects_unknowns():
    plan = _plan(datasets=["benchmark_price", "daily_price", "benchmark_price"])
    assert plan["datasets"] == ["daily_price", "benchmark_price"]

    with pytest.raises(ValueError, match="unsupported dataset"):
        _plan(datasets=["daily_price", "factor_daily"])
    with pytest.raises(ValueError, match="datasets must not be empty"):
        _plan(datasets=[])


def test_dataframe_checksum_normalizes_column_and_row_order_without_mutating_input():
    first = pd.DataFrame(
        {
            "stock_code": ["600519.SH", "000001.SZ"],
            "trade_date": ["2024-01-02", "2024-01-01"],
            "close": [10.5, 8.0],
        }
    )
    original = first.copy(deep=True)
    second = first.iloc[::-1][["close", "trade_date", "stock_code"]].reset_index(drop=True)

    first_checksum = backfill.dataframe_checksum(
        first,
        key_columns=["stock_code", "trade_date"],
    )
    second_checksum = backfill.dataframe_checksum(
        second,
        key_columns=["stock_code", "trade_date"],
    )

    assert first_checksum == second_checksum
    pd.testing.assert_frame_equal(first, original)


def test_dataframe_checksum_distinguishes_schema_and_value_changes_and_handles_empty_frames():
    baseline = pd.DataFrame({"stock_code": ["000001.SZ"], "value": [1]})
    value_changed = pd.DataFrame({"stock_code": ["000001.SZ"], "value": [2]})
    schema_changed = pd.DataFrame({"stock_code": ["000001.SZ"], "value": [1.0]})
    empty_int = baseline.iloc[0:0]
    empty_float = schema_changed.iloc[0:0]

    baseline_checksum = backfill.dataframe_checksum(baseline)
    assert baseline_checksum == backfill.dataframe_checksum(baseline.copy())
    assert baseline_checksum != backfill.dataframe_checksum(value_changed)
    assert baseline_checksum != backfill.dataframe_checksum(schema_changed)
    assert backfill.dataframe_checksum(empty_int) == backfill.dataframe_checksum(empty_int.copy())
    assert backfill.dataframe_checksum(empty_int) != backfill.dataframe_checksum(empty_float)


def test_dataframe_checksum_rejects_missing_key_columns_with_stable_error():
    _assert_error_code(
        "MISSING_KEY_COLUMNS",
        lambda: backfill.dataframe_checksum(
            pd.DataFrame({"stock_code": ["000001.SZ"]}),
            key_columns=["trade_date"],
        ),
    )


def test_dataframe_checksum_canonicalizes_nested_container_cells_without_truth_ambiguity():
    payloads = [
        [1, "two"],
        (1, "two"),
        {"beta", "alpha"},
        frozenset({3, 2}),
        np.array([[1, 2], [3, 4]], dtype=np.int64),
        pd.Series([2, 1], index=["b", "a"], dtype="int64"),
        pd.Index(["x", "y"], name="letters"),
    ]
    first = pd.DataFrame({"id": range(len(payloads)), "payload": pd.Series(payloads, dtype=object)})
    second = first.iloc[::-1].reset_index(drop=True)

    assert backfill.dataframe_checksum(first, key_columns=["id"]) == backfill.dataframe_checksum(
        second,
        key_columns=["id"],
    )

    set_one = pd.DataFrame({"payload": pd.Series([set(["gamma", "alpha", "beta"])], dtype=object)})
    set_two = pd.DataFrame({"payload": pd.Series([set(["beta", "gamma", "alpha"])], dtype=object)})
    assert backfill.dataframe_checksum(set_one) == backfill.dataframe_checksum(set_two)


def test_dataframe_checksum_preserves_typed_dict_keys_and_typed_column_identities():
    int_key = pd.DataFrame({"payload": pd.Series([{1: "same"}], dtype=object)})
    string_key = pd.DataFrame({"payload": pd.Series([{"1": "same"}], dtype=object)})
    int_column = pd.DataFrame([[7]], columns=[1])
    string_column = pd.DataFrame([[7]], columns=["1"])

    assert backfill.dataframe_checksum(int_key) != backfill.dataframe_checksum(string_key)
    assert backfill.dataframe_checksum(int_column) != backfill.dataframe_checksum(string_column)


def test_dataframe_checksum_preserves_categorical_categories_and_order():
    baseline = pd.DataFrame(
        {"grade": pd.Categorical(["low"], categories=["low", "high"], ordered=False)}
    )
    category_changed = pd.DataFrame(
        {"grade": pd.Categorical(["low"], categories=["low", "other"], ordered=False)}
    )
    order_changed = pd.DataFrame(
        {"grade": pd.Categorical(["low"], categories=["low", "high"], ordered=True)}
    )

    assert backfill.dataframe_checksum(baseline) != backfill.dataframe_checksum(category_changed)
    assert backfill.dataframe_checksum(baseline) != backfill.dataframe_checksum(order_changed)


def test_dataframe_checksum_rejects_unsupported_object_cells_instead_of_hashing_repr_addresses():
    class UnsupportedCell:
        pass

    _assert_error_code(
        "UNSUPPORTED_CHECKSUM_VALUE",
        lambda: backfill.dataframe_checksum(
            pd.DataFrame({"id": [1], "payload": pd.Series([UnsupportedCell()], dtype=object)})
        ),
    )


class EmptyResultError(Exception):
    pass


class RateLimitedError(Exception):
    pass


class SchemaDriftError(Exception):
    pass


class TransientProviderError(Exception):
    pass


class SemanticSourceUnavailableError(Exception):
    pass


class DQFailedError(Exception):
    pass


class WriteFailedError(Exception):
    pass


class ReadbackFailedError(Exception):
    pass


@pytest.mark.parametrize(
    ("error", "category", "retryable"),
    [
        (EmptyResultError("empty"), "EMPTY_RESULT", False),
        (RateLimitedError("429"), "RATE_LIMITED", True),
        (PermissionError("forbidden"), "PERMISSION_DENIED", False),
        (SchemaDriftError("schema"), "SCHEMA_DRIFT", False),
        (TransientProviderError("timeout"), "TRANSIENT_PROVIDER_ERROR", True),
        (backfill.BackfillPlanningError("BAD_CONFIG", "bad config"), "CONFIGURATION_ERROR", False),
        (SemanticSourceUnavailableError("missing history"), "SEMANTIC_SOURCE_UNAVAILABLE", False),
        (DQFailedError("bad rows"), "DQ_FAILED", False),
        (WriteFailedError("write"), "WRITE_FAILED", True),
        (ReadbackFailedError("readback"), "READBACK_FAILED", True),
        (KeyboardInterrupt(), "INTERRUPTED", False),
        (RuntimeError("other"), "UNKNOWN", True),
    ],
)
def test_failure_classification_has_stable_categories_and_retryability(error, category, retryable):
    record = backfill.classify_backfill_failure(error)

    assert record["category"] == category
    assert record["retryable"] is retryable
    assert record["exception_type"]
    assert isinstance(record["message"], str)


def test_failure_classification_redacts_all_supported_secret_forms_and_sanitizes_type():
    secret_values = ["tok-1", "api-2", "access-3", "sec-4", "pwd-5", "auth-6", "bear-7"]
    error_type = type("Odd<Credential>Error", (RuntimeError,), {})
    error = error_type(
        "token=tok-1 api_token:api-2 access_token access-3 secret=sec-4 "
        "password: pwd-5 Authorization=auth-6 Bearer bear-7"
    )

    record = backfill.classify_backfill_failure(error)

    assert all(value not in record["message"] for value in secret_values)
    assert record["message"].count("[REDACTED]") >= 7
    assert record["exception_type"] == "Odd_Credential_Error"


@pytest.mark.parametrize(
    ("message", "category", "retryable"),
    [
        ("provider returned empty result", "EMPTY_RESULT", False),
        ("request returned no rows for the interval", "EMPTY_RESULT", False),
        ("schema mismatch: expected close", "SCHEMA_DRIFT", False),
        ("HTTP 401 Unauthorized", "PERMISSION_DENIED", False),
        ("HTTP 403 forbidden", "PERMISSION_DENIED", False),
        ("抱歉，您没有访问该接口的权限", "PERMISSION_DENIED", False),
        ("Tushare 每分钟最多访问该接口 200 次", "RATE_LIMITED", True),
        ("Tushare access frequency limit exceeded", "RATE_LIMITED", True),
        ("HTTP 500 internal server error", "TRANSIENT_PROVIDER_ERROR", True),
        ("upstream HTTP 503 service unavailable", "TRANSIENT_PROVIDER_ERROR", True),
    ],
)
def test_failure_classification_understands_realistic_provider_messages(message, category, retryable):
    record = backfill.classify_backfill_failure(RuntimeError(message))
    assert record["category"] == category
    assert record["retryable"] is retryable


def test_failure_redaction_covers_quoted_json_python_query_basic_and_bearer_forms():
    secrets = [
        "json secret with spaces",
        "python secret with spaces",
        "query-secret",
        "header-bearer-secret",
        "header-basic-secret",
        "header-apikey-secret",
        "header-token-secret",
        "standalone secret with spaces",
    ]
    message = (
        '{"token": "json secret with spaces"} '
        "{'password': 'python secret with spaces'} "
        "https://example.test/path?access_token=query-secret&x=1 "
        "Authorization: Bearer header-bearer-secret "
        "Authorization: Basic header-basic-secret "
        "Authorization: ApiKey header-apikey-secret "
        "Authorization: Token header-token-secret "
        'Bearer "standalone secret with spaces"'
    )

    record = backfill.classify_backfill_failure(RuntimeError(message))

    assert all(secret not in record["message"] for secret in secrets)
    assert record["message"].count("[REDACTED]") >= len(secrets)


def test_chunk_manifest_preserves_explicit_false_zero_and_empty_values_and_copies_scope():
    chunk = _plan(datasets=["daily_price"], date_batch_days=400)["chunks"][0]
    manifest = backfill.build_chunk_manifest(
        chunk=chunk,
        state="PENDING",
        attempt_count=0,
        provider_status=False,
        row_count=0,
        actual_schema=[],
        target_schema={},
        dq={},
        coverage=[],
        source_key="",
        staging_key="",
        staging_checksum="",
        canonical_key="",
        canonical_checksum="",
        validation=False,
        write_result={},
        read_back_result=[],
        failure={},
    )
    original_chunk_id = manifest["chunk"]["chunk_id"]
    chunk["chunk_id"] = "mutated-after-build"

    assert manifest["schema_version"] == "goal21.chunk_manifest.v1"
    assert manifest["attempt_count"] == 0
    assert manifest["provider_status"] is False
    assert manifest["row_count"] == 0
    assert manifest["actual_schema"] == []
    assert manifest["target_schema"] == {}
    assert manifest["validation"] is False
    assert manifest["failure"] == {}
    assert manifest["chunk"]["chunk_id"] == original_chunk_id


@pytest.mark.parametrize(
    ("state", "attempt_count", "failure"),
    [
        ("PENDING", 0, None),
        ("RUNNING", 1, None),
        ("FAILED", 1, WriteFailedError("write failed")),
        ("BLOCKED", 1, DQFailedError("bad rows")),
        ("INTERRUPTED", 1, KeyboardInterrupt()),
    ],
)
def test_chunk_manifest_accepts_truthful_non_evidence_states(state, attempt_count, failure):
    manifest = backfill.build_chunk_manifest(
        chunk=_single_chunk(),
        state=state,
        attempt_count=attempt_count,
        failure=failure,
    )
    assert manifest["state"] == state


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("codes", ["000002.SZ"]),
        ("start_date", "2024-01-02"),
        ("strategy", "forged_strategy"),
        ("key_columns", ["stock_code"]),
        ("dataset", "adj_factor"),
    ],
)
def test_chunk_manifest_revalidates_full_chunk_identity_before_persisting(field, value):
    chunk = dict(_single_chunk())
    chunk[field] = value

    _assert_error_code(
        "TAMPERED_CHUNK_ID",
        lambda: backfill.build_chunk_manifest(chunk=chunk, state="PENDING", attempt_count=0),
    )


def test_chunk_manifest_rejects_unknown_dataset_with_stable_error():
    chunk = dict(_single_chunk(), dataset="unknown_dataset")
    _assert_error_code(
        "UNSUPPORTED_DATASET",
        lambda: backfill.build_chunk_manifest(chunk=chunk, state="PENDING", attempt_count=0),
    )


@pytest.mark.parametrize("row_count", [-1, True, 1.5, "1"])
def test_chunk_manifest_rejects_invalid_row_counts(row_count):
    _assert_error_code(
        "INVALID_ROW_COUNT",
        lambda: backfill.build_chunk_manifest(
            chunk=_single_chunk(),
            state="PENDING",
            attempt_count=0,
            row_count=row_count,
        ),
    )


@pytest.mark.parametrize("state", ["RUNNING", "STAGED", "COMPLETED", "FAILED", "BLOCKED", "INTERRUPTED"])
def test_every_non_pending_state_requires_a_positive_attempt_count(state):
    kwargs = {"failure": WriteFailedError("write failed")} if state == "FAILED" else {}
    if state == "BLOCKED":
        kwargs["failure"] = DQFailedError("bad rows")
    if state == "INTERRUPTED":
        kwargs["failure"] = KeyboardInterrupt()
    _assert_error_code(
        "INVALID_ATTEMPT_COUNT",
        lambda: backfill.build_chunk_manifest(
            chunk=_single_chunk(),
            state=state,
            attempt_count=0,
            **kwargs,
        ),
    )


@pytest.mark.parametrize("attempt_count", [-1, True, 1.5])
def test_chunk_manifest_rejects_invalid_attempt_counts(attempt_count):
    _assert_error_code(
        "INVALID_ATTEMPT_COUNT",
        lambda: backfill.build_chunk_manifest(
            chunk=_single_chunk(),
            state="PENDING",
            attempt_count=attempt_count,
        ),
    )


def test_chunk_manifest_validates_state_and_staged_evidence_without_relabelling():
    _assert_error_code(
        "INVALID_CHUNK_STATE",
        lambda: backfill.build_chunk_manifest(chunk=_single_chunk(), state="DONE", attempt_count=0),
    )
    _assert_error_code(
        "INVALID_MANIFEST_EVIDENCE",
        lambda: backfill.build_chunk_manifest(chunk=_single_chunk(), state="STAGED", attempt_count=1),
    )

    staged = backfill.build_chunk_manifest(
        chunk=_single_chunk(),
        state="STAGED",
        attempt_count=1,
        **_staged_evidence(_single_chunk()),
    )
    assert staged["state"] == "STAGED"


def test_completed_manifest_requires_validation_write_and_readback_evidence():
    chunk = _single_chunk()
    base = {
        "chunk": chunk,
        "state": "COMPLETED",
        "attempt_count": 1,
        **_completed_evidence(chunk),
    }
    completed = backfill.build_chunk_manifest(**base)
    assert completed["state"] == "COMPLETED"

    for missing in ["canonical_key", "canonical_checksum", "validation", "write_result", "read_back_result"]:
        invalid = dict(base)
        invalid[missing] = None
        _assert_error_code(
            "INVALID_MANIFEST_EVIDENCE",
            lambda invalid=invalid: backfill.build_chunk_manifest(**invalid),
        )


@pytest.mark.parametrize(
    "missing",
    [
        "provider_status",
        "row_count",
        "actual_schema",
        "target_schema",
        "dq",
        "coverage",
        "source_key",
        "staging_key",
        "staging_checksum",
    ],
)
def test_staged_manifest_requires_complete_truthful_provider_schema_dq_coverage_and_staging_evidence(missing):
    chunk = _single_chunk()
    evidence = _staged_evidence(chunk)
    evidence[missing] = None

    _assert_error_code(
        "INVALID_MANIFEST_EVIDENCE",
        lambda: backfill.build_chunk_manifest(
            chunk=chunk,
            state="STAGED",
            attempt_count=1,
            **evidence,
        ),
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("provider_status", {"success": True, "status": "FAILED"}),
        ("dq", {"success": True, "passed": False}),
        ("coverage", {"complete": False, "success": True}),
        ("actual_schema", False),
        ("target_schema", 0),
    ],
)
def test_staged_manifest_rejects_contradictory_or_non_schema_evidence(field, value):
    chunk = _single_chunk()
    evidence = _staged_evidence(chunk)
    evidence[field] = value

    _assert_error_code(
        "INVALID_MANIFEST_EVIDENCE",
        lambda: backfill.build_chunk_manifest(
            chunk=chunk,
            state="STAGED",
            attempt_count=1,
            **evidence,
        ),
    )


def test_staged_manifest_cross_checks_zero_rows_with_explicit_valid_empty_status_and_coverage():
    chunk = _single_chunk()
    fetched_empty = _staged_evidence(chunk, row_count=0)
    _assert_error_code(
        "INVALID_MANIFEST_EVIDENCE",
        lambda: backfill.build_chunk_manifest(
            chunk=chunk,
            state="STAGED",
            attempt_count=1,
            **fetched_empty,
        ),
    )

    invalid_empty = _staged_evidence(chunk, row_count=0)
    invalid_empty["provider_status"] = "VALID_EMPTY"
    invalid_empty["coverage"] = dict(invalid_empty["coverage"], valid_empty=True)
    _assert_error_code(
        "INVALID_MANIFEST_EVIDENCE",
        lambda: backfill.build_chunk_manifest(
            chunk=chunk,
            state="STAGED",
            attempt_count=1,
            **invalid_empty,
        ),
    )

    valid_chunk = _plan(datasets=["st_history"], date_batch_days=400)["chunks"][0]
    valid_empty = _staged_evidence(valid_chunk, row_count=0)
    valid_empty["provider_status"] = "VALID_EMPTY"
    valid_empty["coverage"] = dict(valid_empty["coverage"], valid_empty=True)
    manifest = backfill.build_chunk_manifest(
        chunk=valid_chunk,
        state="STAGED",
        attempt_count=1,
        **valid_empty,
    )
    assert manifest["row_count"] == 0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("provider_status", False),
        ("provider_status", {"success": False}),
        ("dq", False),
        ("dq", {"success": False}),
    ],
)
def test_staged_manifest_requires_successful_provider_and_dq_evidence(field, value):
    chunk = _single_chunk()
    evidence = _staged_evidence(chunk)
    evidence[field] = value

    _assert_error_code(
        "INVALID_MANIFEST_EVIDENCE",
        lambda: backfill.build_chunk_manifest(
            chunk=chunk,
            state="STAGED",
            attempt_count=1,
            **evidence,
        ),
    )


@pytest.mark.parametrize(
    ("container", "field", "value"),
    [
        ("write_result", "object_key", "raw/wrong.parquet"),
        ("write_result", "checksum", "wrong-write-checksum"),
        ("write_result", "row_count", 999),
        ("read_back_result", "object_key", "raw/wrong.parquet"),
        ("read_back_result", "checksum", "wrong-readback-checksum"),
        ("read_back_result", "row_count", 999),
    ],
)
def test_completed_manifest_rejects_contradictory_write_and_readback_evidence(container, field, value):
    chunk = _single_chunk()
    evidence = _completed_evidence(chunk)
    evidence[container] = dict(evidence[container], **{field: value})

    _assert_error_code(
        "INVALID_MANIFEST_EVIDENCE",
        lambda: backfill.build_chunk_manifest(
            chunk=chunk,
            state="COMPLETED",
            attempt_count=1,
            **evidence,
        ),
    )


@pytest.mark.parametrize(
    ("container", "value"),
    [
        ("write_result", True),
        ("read_back_result", True),
        ("write_result", {"success": True}),
        ("read_back_result", {"success": True}),
    ],
)
def test_completed_manifest_requires_auditable_canonical_result_links(container, value):
    chunk = _single_chunk()
    evidence = _completed_evidence(chunk)
    evidence[container] = value

    _assert_error_code(
        "INVALID_MANIFEST_EVIDENCE",
        lambda: backfill.build_chunk_manifest(
            chunk=chunk,
            state="COMPLETED",
            attempt_count=1,
            **evidence,
        ),
    )


def test_failure_states_normalize_exceptions_and_canonical_dicts_before_persisting():
    failed = backfill.build_chunk_manifest(
        chunk=_single_chunk(),
        state="FAILED",
        attempt_count=1,
        failure=WriteFailedError('write failed token="secret with spaces"'),
    )
    blocked = backfill.build_chunk_manifest(
        chunk=_single_chunk(),
        state="BLOCKED",
        attempt_count=1,
        failure={
            "category": "DQ_FAILED",
            "retryable": False,
            "exception_type": "Odd<DQ>Error",
            "message": "bad rows password=query-secret",
        },
    )

    assert failed["failure"]["category"] == "WRITE_FAILED"
    assert failed["failure"]["retryable"] is True
    assert "secret with spaces" not in failed["failure"]["message"]
    assert blocked["failure"] == {
        "category": "DQ_FAILED",
        "retryable": False,
        "exception_type": "Odd_DQ_Error",
        "message": "bad rows password=[REDACTED]",
    }


@pytest.mark.parametrize(
    ("state", "failure"),
    [
        ("FAILED", DQFailedError("non-retryable")),
        ("BLOCKED", WriteFailedError("retryable")),
        ("INTERRUPTED", RuntimeError("not interrupted")),
        ("FAILED", None),
        ("BLOCKED", {}),
        ("INTERRUPTED", None),
    ],
)
def test_failure_state_must_agree_with_normalized_failure_retryability(state, failure):
    _assert_error_code(
        "INVALID_FAILURE_STATE",
        lambda: backfill.build_chunk_manifest(
            chunk=_single_chunk(),
            state=state,
            attempt_count=1,
            failure=failure,
        ),
    )


@pytest.mark.parametrize("state", ["PENDING", "RUNNING", "STAGED", "COMPLETED"])
def test_non_failure_states_reject_nonempty_failure_records(state):
    kwargs = {}
    if state == "STAGED":
        kwargs.update(_staged_evidence(_single_chunk()))
    if state == "COMPLETED":
        kwargs.update(_completed_evidence(_single_chunk()))
    _assert_error_code(
        "INVALID_FAILURE_STATE",
        lambda: backfill.build_chunk_manifest(
            chunk=_single_chunk(),
            state=state,
            attempt_count=0 if state == "PENDING" else 1,
            failure=WriteFailedError("must not persist"),
            **kwargs,
        ),
    )


def test_persisted_failure_dict_rejects_extra_fields_and_retryability_contradictions():
    canonical = {
        "category": "WRITE_FAILED",
        "retryable": True,
        "exception_type": "WriteFailedError",
        "message": "write failed",
    }
    with_extra = dict(canonical, raw_secret="must-not-persist")
    contradictory = dict(canonical, retryable=False)

    for failure in [with_extra, contradictory]:
        _assert_error_code(
            "INVALID_FAILURE_RECORD",
            lambda failure=failure: backfill.build_chunk_manifest(
                chunk=_single_chunk(),
                state="FAILED",
                attempt_count=1,
                failure=failure,
            ),
        )


def test_manifest_rejects_malformed_failure_types_with_stable_error():
    _assert_error_code(
        "INVALID_FAILURE_RECORD",
        lambda: backfill.build_chunk_manifest(
            chunk=_single_chunk(),
            state="FAILED",
            attempt_count=1,
            failure=np.array(["one", "two"], dtype=object),
        ),
    )


def test_summary_accounts_for_planned_chunks_missing_manifests_and_all_states():
    plan = _plan(
        datasets=["daily_price"],
        codes=["000001.SZ", "000002.SZ", "600519.SH"],
        code_batch_size=1,
        date_batch_days=400,
    )
    completed = _completed_manifest(plan["chunks"][0])
    failure = {
        "category": "WRITE_FAILED",
        "retryable": True,
        "exception_type": "WriteFailedError",
        "message": "write failed",
    }
    failed = backfill.build_chunk_manifest(
        chunk=plan["chunks"][1],
        state="FAILED",
        attempt_count=1,
        failure=failure,
    )

    summary = backfill.summarize_chunk_manifests(plan, [completed, failed])

    assert summary["total"] == 3
    assert summary["planned"] == 3
    assert summary["accounted_count"] == 3
    assert summary["state_counts"] == {
        "PENDING": 1,
        "RUNNING": 0,
        "STAGED": 0,
        "COMPLETED": 1,
        "FAILED": 1,
        "BLOCKED": 0,
        "INTERRUPTED": 0,
    }
    assert summary["completion_rate"] == pytest.approx(1 / 3)
    assert summary["canonical_ready"] is False
    assert summary["per_dataset"]["daily_price"]["total"] == 3
    assert summary["per_dataset"]["daily_price"]["state_counts"] == summary["state_counts"]
    assert [(gap["state"], gap["category"]) for gap in summary["gaps"]] == [
        ("FAILED", "WRITE_FAILED"),
        ("PENDING", None),
    ]


def test_summary_rejects_duplicate_and_foreign_manifests_and_requires_nonempty_all_completed():
    plan = _plan(datasets=["daily_price"], codes=["000001.SZ"], date_batch_days=400)
    completed = _completed_manifest(plan["chunks"][0])
    _assert_error_code(
        "DUPLICATE_MANIFEST",
        lambda: backfill.summarize_chunk_manifests(plan, [completed, completed]),
    )
    foreign = dict(completed, chunk_id="foreign-chunk")
    _assert_error_code(
        "FOREIGN_MANIFEST",
        lambda: backfill.summarize_chunk_manifests(plan, [foreign]),
    )

    assert backfill.summarize_chunk_manifests(plan, [completed])["canonical_ready"] is True
    empty_plan = dict(plan, chunks=[], chunk_count=0)
    empty_summary = backfill.summarize_chunk_manifests(empty_plan, [])
    assert empty_summary["canonical_ready"] is False
    assert empty_summary["completion_rate"] == 0.0


def test_summary_rejects_non_builder_shaped_and_corrupted_persisted_manifests():
    plan = _plan(datasets=["daily_price"], codes=["000001.SZ"], date_batch_days=400)
    completed = _completed_manifest(plan["chunks"][0])
    corruptions = []

    extra = dict(completed, unexpected_field="unsafe")
    corruptions.append(extra)
    missing = dict(completed)
    missing.pop("provider_status")
    corruptions.append(missing)
    wrong_version = dict(completed, schema_version="goal21.chunk_manifest.v999")
    corruptions.append(wrong_version)
    invalid_attempt = dict(completed, attempt_count=0)
    corruptions.append(invalid_attempt)
    invalid_rows = dict(completed, row_count=-1)
    corruptions.append(invalid_rows)
    contradictory = deepcopy(completed)
    contradictory["read_back_result"]["row_count"] += 1
    corruptions.append(contradictory)

    for manifest in corruptions:
        with pytest.raises(backfill.BackfillPlanningError):
            backfill.summarize_chunk_manifests(plan, [manifest])


def test_summary_requires_embedded_chunk_to_exactly_match_the_planned_chunk():
    plan = _plan(datasets=["daily_price"], codes=["000001.SZ"], date_batch_days=400)
    completed = _completed_manifest(plan["chunks"][0])
    completed["chunk"]["codes"] = ["000002.SZ"]

    with pytest.raises(backfill.BackfillPlanningError):
        backfill.summarize_chunk_manifests(plan, [completed])


def test_persisted_failure_cannot_bypass_redaction_and_summary_gap_reason_is_safe():
    plan = _plan(datasets=["daily_price"], codes=["000001.SZ"], date_batch_days=400)
    failed = backfill.build_chunk_manifest(
        chunk=plan["chunks"][0],
        state="FAILED",
        attempt_count=1,
        failure=WriteFailedError('{"password": "persisted secret with spaces"}'),
    )

    assert "persisted secret with spaces" not in failed["failure"]["message"]
    summary = backfill.summarize_chunk_manifests(plan, [failed])
    assert "persisted secret with spaces" not in summary["gaps"][0]["reason"]

    bypass = deepcopy(failed)
    bypass["failure"]["message"] = "token=bypass-secret"
    with pytest.raises(backfill.BackfillPlanningError):
        backfill.summarize_chunk_manifests(plan, [bypass])


def test_historical_fetch_result_contract_rejects_inconsistent_status_evidence():
    provider = _historical_provider_module()
    frame = pd.DataFrame(
        {
            "stock_code": ["000001.SZ"],
            "trade_date": ["2024-01-02"],
            "adj_factor": [1.25],
        }
    )
    dq = {"passed": True, "level": "STRICT", "blocked_reasons": []}
    coverage = _provider_coverage(
        axis="stock_code_x_open_trade_date",
        expected_count=1,
        covered_count=1,
        codes=["000001.SZ"],
        trade_dates=["2024-01-02"],
    )
    provider_call = {
        "endpoint": "adj_factor",
        "strategy": "BY_CODE_RANGE",
        "parameters": {"ts_code": "000001.SZ", "token": "must-not-survive"},
        "row_count": 1,
        "status": "FETCHED",
    }
    result = provider.HistoricalChunkFetchResult(
        dataset="adj_factor",
        chunk_id="adj_factor-contract-test",
        frame=frame,
        provider_status="FETCHED",
        provider_name="fixture",
        source_keys=("archive/adj_factor/2024.parquet",),
        source_semantics="HISTORICAL_RANGE_SOURCE",
        provider_calls=(provider_call,),
        actual_schema=("ts_code", "trade_date", "adj_factor"),
        target_schema=("stock_code", "trade_date", "adj_factor"),
        dq=dq,
        coverage=coverage,
        canonical_trade_dates=("2024-01-02",),
        partition_strategy="BY_TRADE_DATE_COLUMN",
        valid_empty=False,
        validation={"passed": True, "errors": []},
        failure=None,
    )

    frame.loc[0, "adj_factor"] = 99.0
    dq["passed"] = False
    coverage["complete"] = False
    provider_call["parameters"]["ts_code"] = "600519.SH"
    assert result.frame.loc[0, "adj_factor"] == 1.25
    assert result.dq["passed"] is True
    assert result.coverage["complete"] is True
    assert result.provider_calls[0]["parameters"]["ts_code"] == "000001.SZ"
    assert "must-not-survive" not in repr(result.provider_calls)

    for status in ["FETCHED", "VALID_EMPTY", "BLOCKED", "FAILED"]:
        consistent = provider.HistoricalChunkFetchResult(**_result_contract_kwargs(provider, status=status))
        assert consistent.provider_status == status

    non_st_valid_empty = _result_contract_kwargs(provider, status="VALID_EMPTY")
    non_st_valid_empty.update(
        dataset="adj_factor",
        chunk_id="adj_factor-contract-test",
        frame=_empty_standard_frame("adj_factor"),
        actual_schema=("stock_code", "trade_date", "adj_factor"),
        target_schema=("stock_code", "trade_date", "adj_factor"),
        source_semantics="HISTORICAL_RANGE_SOURCE",
        partition_strategy="BY_TRADE_DATE_COLUMN",
    )
    invalid_cases = [
        dict(_result_contract_kwargs(provider, status="FETCHED"), frame=_empty_standard_frame("adj_factor")),
        non_st_valid_empty,
        dict(_result_contract_kwargs(provider, status="BLOCKED"), frame=frame),
        dict(
            _result_contract_kwargs(provider, status="FAILED"),
            failure={
                "category": "DQ_FAILED",
                "retryable": False,
                "exception_type": "HistoricalProviderError",
                "message": "bad rows",
            },
        ),
    ]
    for kwargs in invalid_cases:
        with pytest.raises(ValueError):
            provider.HistoricalChunkFetchResult(**kwargs)


def test_router_constructor_is_side_effect_free_and_fixture_fetch_chunk_is_offline():
    provider = _historical_provider_module()
    plan = _provider_plan("adj_factor", start_date="2024-01-02", end_date="2024-01-02")
    calls = []

    def calendar_fn(start_date, end_date):
        calls.append(("calendar", start_date, end_date))
        return pd.DataFrame({"cal_date": ["20240102"], "is_open": [1]})

    def raw_fetch_fn(endpoint, parameters):
        calls.append(("raw", endpoint, deepcopy(parameters)))
        frame = pd.DataFrame(
            {"ts_code": ["000001.SZ"], "trade_date": ["20240102"], "adj_factor": [1.25]}
        )
        frame.attrs.update(
            source_keys=["archive/adj_factor/2024.parquet"],
            source_semantics="HISTORICAL_RANGE_SOURCE",
            coverage_complete=True,
            sample_truncated=False,
            requested_codes=["000001.SZ"],
            coverage_start_date="2024-01-02",
            coverage_end_date="2024-01-02",
        )
        return frame

    router = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=raw_fetch_fn,
        trading_calendar_fn=calendar_fn,
    )
    assert calls == []

    result = router.fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "FETCHED"
    assert result.dataset == "adj_factor"
    assert result.chunk_id == plan["chunks"][0]["chunk_id"]
    assert [call[0] for call in calls] == ["calendar", "raw"]
    assert result.frame.to_dict(orient="records") == [
        {"stock_code": "000001.SZ", "trade_date": "2024-01-02", "adj_factor": 1.25}
    ]


def test_router_rejects_foreign_or_tampered_chunk_before_fetch():
    provider = _historical_provider_module()
    plan = _provider_plan("adj_factor", start_date="2024-01-02", end_date="2024-01-02")
    calls = []
    router = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: calls.append((endpoint, parameters)),
        trading_calendar_fn=lambda start_date, end_date: calls.append((start_date, end_date)),
    )
    chunk = plan["chunks"][0]
    foreign = _provider_plan(
        "adj_factor",
        start_date="2024-01-03",
        end_date="2024-01-03",
    )["chunks"][0]
    tampered = [
        dict(chunk, chunk_id="adj_factor-forged"),
        dict(chunk, dataset="daily_basic"),
        dict(chunk, strategy="forged_strategy"),
        dict(chunk, key_columns=["stock_code"]),
        dict(chunk, codes=["600519.SH"]),
        dict(chunk, start_date="2024-01-01"),
        dict(chunk, end_date="2024-01-03"),
        foreign,
    ]

    for candidate in tampered:
        with pytest.raises(provider.HistoricalProviderError) as exc_info:
            router.fetch_chunk(candidate)
        assert exc_info.value.failure_category == "CONFIGURATION_ERROR"
    assert calls == []

    financial_plan = _provider_plan("financial", start_date="2024-01-02", end_date="2024-01-02")
    financial_router = provider.HistoricalProviderRouter(
        plan=financial_plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: calls.append((endpoint, parameters)),
        trading_calendar_fn=lambda start_date, end_date: calls.append((start_date, end_date)),
    )
    forged_financial = dict(financial_plan["chunks"][0], report_period_end="2024-01-03")
    with pytest.raises(provider.HistoricalProviderError):
        financial_router.fetch_chunk(forged_financial)

    benchmark_plan = _provider_plan("benchmark_price", start_date="2024-01-02", end_date="2024-01-02")
    benchmark_router = provider.HistoricalProviderRouter(
        plan=benchmark_plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: calls.append((endpoint, parameters)),
        trading_calendar_fn=lambda start_date, end_date: calls.append((start_date, end_date)),
    )
    forged_benchmark = dict(benchmark_plan["chunks"][0], index_codes=["000300.SH"])
    with pytest.raises(provider.HistoricalProviderError):
        benchmark_router.fetch_chunk(forged_benchmark)
    assert calls == []


def test_trading_calendar_defines_sorted_open_dates_without_weekday_inference():
    provider = _historical_provider_module()
    plan = _provider_plan("adj_factor", start_date="2024-01-05", end_date="2024-01-08")
    raw = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ"],
            "trade_date": ["20240108", "20240105"],
            "adj_factor": [1.02, 1.01],
        }
    )
    raw.attrs.update(
        source_keys=["archive/adj_factor/2024.parquet"],
        source_semantics="HISTORICAL_RANGE_SOURCE",
        coverage_complete=True,
        sample_truncated=False,
        requested_codes=["000001.SZ"],
        coverage_start_date="2024-01-05",
        coverage_end_date="2024-01-08",
    )
    rows = [
        {"cal_date": "20240108", "is_open": 1},
        {"cal_date": "20240107", "is_open": 0},
        {"cal_date": "20240105", "is_open": 1},
        {"cal_date": "20240106", "is_open": 0},
    ]
    router = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: raw.copy(deep=True),
        trading_calendar_fn=lambda start_date, end_date: pd.DataFrame(rows),
    )

    result = router.fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "FETCHED"
    assert result.canonical_trade_dates == ("2024-01-05", "2024-01-08")
    assert result.coverage["expected_count"] == 2
    assert result.coverage["covered_count"] == 2
    assert result.frame["trade_date"].tolist() == ["2024-01-05", "2024-01-08"]

    duplicate_router = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: raw.copy(deep=True),
        trading_calendar_fn=lambda start_date, end_date: pd.DataFrame(rows + [rows[0]]),
    )
    duplicate = duplicate_router.fetch_chunk(plan["chunks"][0])
    assert duplicate.provider_status == "BLOCKED"
    assert duplicate.failure["category"] == "DQ_FAILED"

    incomplete_router = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: raw.copy(deep=True),
        trading_calendar_fn=lambda start_date, end_date: pd.DataFrame([rows[0], rows[2]]),
    )
    incomplete = incomplete_router.fetch_chunk(plan["chunks"][0])
    assert incomplete.provider_status == "BLOCKED"
    assert incomplete.failure["category"] == "SEMANTIC_SOURCE_UNAVAILABLE"


@pytest.mark.parametrize("dataset", ALL_DATASETS)
def test_historical_dataset_natural_axis_is_complete_and_deterministic(dataset):
    provider = _historical_provider_module()
    dates = ["2024-01-02", "2024-01-03"]
    plan = _provider_plan(dataset, start_date=dates[0], end_date=dates[-1])
    raw = _historical_fixture_frame(dataset, dates=dates)
    calendar = _complete_calendar_frame(dates[0], dates[-1], dates)
    router = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(raw),
        trading_calendar_fn=lambda start_date, end_date: calendar.copy(deep=True),
    )

    first = router.fetch_chunk(plan["chunks"][0])
    second = router.fetch_chunk(deepcopy(plan["chunks"][0]))

    target = tuple(_empty_standard_frame(dataset).columns)
    assert first.provider_status == "FETCHED"
    assert first.target_schema == target
    assert tuple(first.frame.columns) == target
    assert first.actual_schema == tuple(raw.columns)
    assert not first.frame.duplicated(KEY_COLUMNS[dataset]).any()
    expected_order = first.frame.sort_values(KEY_COLUMNS[dataset], kind="mergesort").reset_index(drop=True)
    pd.testing.assert_frame_equal(first.frame.reset_index(drop=True), expected_order)
    assert first.coverage["expected_count"] == len(first.frame)
    assert first.coverage["covered_count"] == len(first.frame)
    assert first.coverage["complete"] is True
    assert first.coverage["missing"] == []
    assert first.dq["passed"] is True
    assert first.validation["passed"] is True
    pd.testing.assert_frame_equal(first.frame, second.frame)
    assert first.coverage == second.coverage
    assert first.canonical_trade_dates == second.canonical_trade_dates


def test_daily_price_uses_code_range_daily_date_limit_and_full_market_suspend_calls():
    provider = _historical_provider_module()
    dates = ["2024-01-02", "2024-01-03"]
    plan = _provider_plan("daily_price", start_date=dates[0], end_date=dates[-1])
    observed = []

    def raw_fetch(endpoint, parameters):
        clean_parameters = deepcopy(parameters)
        observed.append((endpoint, clean_parameters))
        parameters["Authorization"] = "Bearer header-secret"
        parameters["nested"] = {
            "password": "nested-password",
            "credential": "nested-credential",
            "secret": "nested-secret",
            "headers": ["Basic basic-secret", "ApiKey apikey-secret", "Token token-secret"],
        }
        if endpoint == "daily":
            return pd.DataFrame(
                {
                    "ts_code": ["000001.SZ", "000001.SZ"],
                    "trade_date": ["20240103", "20240102"],
                    "open": [10.1, 10.0],
                    "high": [10.5, 10.4],
                    "low": [9.9, 9.8],
                    "close": [10.2, 10.1],
                    "pre_close": [10.1, 10.0],
                    "vol": [101.0, 100.0],
                    "amount": [1030.0, 1010.0],
                    "pct_chg": [0.99, 1.0],
                }
            )
        if endpoint == "stk_limit":
            trade_date = clean_parameters["trade_date"]
            return pd.DataFrame(
                {
                    "ts_code": ["000001.SZ"],
                    "trade_date": [trade_date],
                    "up_limit": [11.0],
                    "down_limit": [9.0],
                }
            )
        if endpoint == "suspend_d":
            frame = pd.DataFrame(columns=["ts_code", "trade_date", "suspend_type", "suspend_timing"])
            frame.attrs.update(
                full_market_event_set=True,
                coverage_complete=True,
                covered_trade_dates=[clean_parameters["trade_date"]],
                sample_truncated=False,
                empty_after_retries=False,
            )
            return frame
        raise AssertionError(f"unexpected endpoint: {endpoint}")

    router = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="tushare",
        raw_fetch_fn=raw_fetch,
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, dates
        ),
    )
    result = router.fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "FETCHED"
    assert result.frame["is_paused"].tolist() == [False, False]
    assert observed == [
        (
            "daily",
            {
                "start_date": "20240102",
                "end_date": "20240103",
                "fields": "ts_code,trade_date,open,high,low,close,pre_close,vol,amount",
                "ts_code": "000001.SZ",
            },
        ),
        (
            "stk_limit",
            {
                "trade_date": "20240102",
                "fields": "ts_code,trade_date,up_limit,down_limit",
            },
        ),
        (
            "stk_limit",
            {
                "trade_date": "20240103",
                "fields": "ts_code,trade_date,up_limit,down_limit",
            },
        ),
        (
            "suspend_d",
            {
                "trade_date": "20240102",
                "fields": "ts_code,trade_date,suspend_timing,suspend_type",
            },
        ),
        (
            "suspend_d",
            {
                "trade_date": "20240103",
                "fields": "ts_code,trade_date,suspend_timing,suspend_type",
            },
        ),
    ]
    assert [call["endpoint"] for call in result.provider_calls] == [item[0] for item in observed]
    persisted_calls = json.dumps(result.provider_calls, ensure_ascii=False).lower()
    for secret in [
        "header-secret",
        "nested-password",
        "nested-credential",
        "nested-secret",
        "basic-secret",
        "apikey-secret",
        "token-secret",
    ]:
        assert secret not in persisted_calls
    assert all("ts_code" not in call["parameters"] for call in result.provider_calls if call["endpoint"] == "suspend_d")


@pytest.mark.parametrize(
    ("mode", "expected_status", "expected_category"),
    [
        ("proven", "FETCHED", None),
        ("unproven", "BLOCKED", "SEMANTIC_SOURCE_UNAVAILABLE"),
        ("missing", "BLOCKED", "SEMANTIC_SOURCE_UNAVAILABLE"),
        ("truncated", "BLOCKED", "SEMANTIC_SOURCE_UNAVAILABLE"),
        ("exhausted_empty", "BLOCKED", "SEMANTIC_SOURCE_UNAVAILABLE"),
        ("provider_error", "FAILED", "TRANSIENT_PROVIDER_ERROR"),
    ],
)
def test_daily_price_suspend_miss_requires_complete_full_market_event_proof(
    mode, expected_status, expected_category
):
    provider = _historical_provider_module()
    trade_date = "2024-01-02"
    plan = _provider_plan("daily_price", start_date=trade_date, end_date=trade_date)

    def raw_fetch(endpoint, parameters):
        if endpoint == "daily":
            return pd.DataFrame(
                {
                    "ts_code": ["000001.SZ"],
                    "trade_date": ["20240102"],
                    "open": [10.0],
                    "high": [10.5],
                    "low": [9.5],
                    "close": [10.1],
                    "pre_close": [10.0],
                    "vol": [0.0],
                    "amount": [0.0],
                    "pct_chg": [1.0],
                }
            )
        if endpoint == "stk_limit":
            return pd.DataFrame(
                {
                    "ts_code": ["000001.SZ"],
                    "trade_date": ["20240102"],
                    "up_limit": [11.0],
                    "down_limit": [9.0],
                }
            )
        if endpoint != "suspend_d":
            raise AssertionError(endpoint)
        if mode == "missing":
            return None
        if mode == "provider_error":
            raise RuntimeError("suspend_d HTTP 503 token=provider-secret")
        frame = pd.DataFrame(columns=["ts_code", "trade_date", "suspend_type", "suspend_timing"])
        if mode != "unproven":
            frame.attrs.update(
                full_market_event_set=True,
                coverage_complete=True,
                covered_trade_dates=["20240102"],
                sample_truncated=mode == "truncated",
                empty_after_retries=mode == "exhausted_empty",
            )
        return frame

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="tushare",
        raw_fetch_fn=raw_fetch,
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, [trade_date]
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == expected_status
    if mode == "proven":
        assert result.frame.loc[0, "is_paused"] is False or result.frame.loc[0, "is_paused"] == False
        assert result.failure is None
    else:
        assert result.frame.empty
        assert result.failure["category"] == expected_category
        assert "provider-secret" not in result.failure["message"]


@pytest.mark.parametrize("dataset", ["adj_factor", "daily_basic"])
@pytest.mark.parametrize("defect", ["missing_pair", "duplicate", "bad_numeric", "out_of_window"])
def test_adj_factor_and_daily_basic_require_complete_finite_open_date_cartesian_coverage(dataset, defect):
    provider = _historical_provider_module()
    dates = ["2024-01-02", "2024-01-03"]
    plan = _provider_plan(dataset, start_date=dates[0], end_date=dates[-1])
    raw = _historical_fixture_frame(dataset, dates=dates)
    if defect == "missing_pair":
        raw = raw.iloc[:1].copy()
    elif defect == "duplicate":
        raw = pd.concat([raw, raw.iloc[[0]]], ignore_index=True)
    elif defect == "bad_numeric":
        column = "adj_factor" if dataset == "adj_factor" else "pe_ttm"
        raw.loc[0, column] = 0.0 if dataset == "adj_factor" else np.inf
    elif defect == "out_of_window":
        raw.loc[0, "trade_date"] = "20240104"
    _set_fixture_attrs(raw, dataset, dates=dates)

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(raw),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, dates
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] == "DQ_FAILED"
    assert result.coverage["complete"] is False
    assert result.frame.empty


def test_financial_projection_uses_announce_date_not_report_period():
    provider = _historical_provider_module()
    dates = ["2024-01-02", "2024-01-03", "2024-01-04"]
    plan = _provider_plan("financial", start_date=dates[0], end_date=dates[-1])
    raw = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ"],
            "end_date": ["20240102", "20240102"],
            "ann_date": ["20240103", "20240105"],
            "revenue_yoy": [10.0, 11.0],
            "net_profit_yoy": [8.0, 9.0],
            "roe": [0.12, 0.13],
            "gross_margin": [0.30, 0.31],
            "debt_ratio": [0.40, 0.41],
            "operating_cashflow": [100.0, 110.0],
        }
    )
    _set_fixture_attrs(
        raw,
        "financial",
        dates=dates,
        source_semantics="TRUSTED_STANDARD_FINANCIAL_SOURCE",
    )
    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(raw),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, dates
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "FETCHED"
    assert result.partition_strategy == "FINANCIAL_ANNOUNCE_DATE_AS_OF"
    assert result.canonical_trade_dates == tuple(dates)
    assert result.frame["announce_date"].tolist() == ["2024-01-03", "2024-01-05"]
    partitions = list(provider.iter_historical_canonical_partitions(result))
    assert [trade_date for trade_date, _ in partitions] == dates
    assert partitions[0][1].empty
    assert partitions[1][1]["announce_date"].tolist() == ["2024-01-03"]
    assert partitions[2][1]["announce_date"].tolist() == ["2024-01-03"]


def test_stock_basic_historical_membership_uses_list_and_exclusive_delist_boundaries():
    provider = _historical_provider_module()
    dates = ["2024-01-02", "2024-01-03", "2024-01-04"]
    codes = ["000001.SZ", "600519.SH"]
    plan = _provider_plan(
        "stock_basic",
        start_date=dates[0],
        end_date=dates[-1],
        codes=codes,
    )
    raw = pd.DataFrame(
        {
            "ts_code": ["000001.SZ", "000001.SZ", "600519.SH", "600519.SH"],
            "name": ["Alpha", "Alpha", "Beta", "Beta"],
            "exchange": ["SZ", "SZ", "SH", "SH"],
            "list_date": ["20240101", "20240101", "20240103", "20240103"],
            "delist_date": ["20240104", "20240104", None, None],
            "industry": ["Bank", "Bank", "Consumer", "Consumer"],
            "market_type": ["MAIN", "MAIN", "MAIN", "MAIN"],
            "is_st": [False, False, False, False],
            "trade_date": ["20240102", "20240103", "20240103", "20240104"],
            "snapshot_date": ["20240102", "20240103", "20240103", "20240104"],
        }
    )
    _set_fixture_attrs(
        raw,
        "stock_basic",
        dates=dates,
        codes=codes,
        source_semantics="POINT_IN_TIME_HISTORICAL_SNAPSHOT",
    )
    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(raw),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, dates
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "FETCHED"
    assert list(map(tuple, result.frame[["stock_code", "trade_date"]].to_numpy())) == [
        ("000001.SZ", "2024-01-02"),
        ("000001.SZ", "2024-01-03"),
        ("600519.SH", "2024-01-03"),
        ("600519.SH", "2024-01-04"),
    ]
    assert result.coverage["expected_count"] == 4
    assert result.coverage["covered_count"] == 4
    assert result.coverage["not_yet_listed_count"] == 1
    assert result.coverage["already_delisted_count"] == 1
    assert result.source_keys == ("archive/stock_basic/history.parquet",)

    for marker in ["source_semantics", "list_status"]:
        current = _copy_frame(raw)
        if marker == "source_semantics":
            current.attrs["source_semantics"] = "CURRENT_SNAPSHOT"
        else:
            current["list_status"] = "L"
        blocked = provider.HistoricalProviderRouter(
            plan=plan,
            provider_name="fixture",
            raw_fetch_fn=lambda endpoint, parameters, current=current: _copy_frame(current),
            trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
                start_date, end_date, dates
            ),
        ).fetch_chunk(plan["chunks"][0])
        assert blocked.provider_status == "BLOCKED"
        assert blocked.failure["category"] == "SEMANTIC_SOURCE_UNAVAILABLE"


def test_st_history_requires_historical_interval_proof_and_strict_valid_empty_evidence():
    provider = _historical_provider_module()
    dates = ["2024-01-02", "2024-01-03", "2024-01-04"]
    plan = _provider_plan("st_history", start_date=dates[0], end_date=dates[-1])
    valid = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "st_type": ["ST"],
            "start_date": ["20240102"],
            "end_date": ["20240104"],
            "source": ["exchange_history"],
        }
    )
    _set_fixture_attrs(
        valid,
        "st_history",
        dates=dates,
        source_semantics="HISTORICAL_INTERVAL_SOURCE",
        interval_row_count=1,
    )

    fetched = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(valid),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, dates
        ),
    ).fetch_chunk(plan["chunks"][0])
    assert fetched.provider_status == "FETCHED"
    assert fetched.frame.loc[0, "start_date"] == "2024-01-02"
    assert fetched.frame.loc[0, "end_date"] == "2024-01-04"
    assert "2024-01-03" < fetched.frame.loc[0, "end_date"]
    assert not ("2024-01-04" < fetched.frame.loc[0, "end_date"])

    invalid_interval = _copy_frame(valid)
    invalid_interval.loc[0, "end_date"] = "20240102"
    invalid_interval_result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(invalid_interval),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, dates
        ),
    ).fetch_chunk(plan["chunks"][0])
    assert invalid_interval_result.provider_status == "BLOCKED"
    assert invalid_interval_result.failure["category"] == "DQ_FAILED"

    current = _copy_frame(valid)
    current.loc[0, "st_type"] = "CURRENT_ST_SNAPSHOT"
    current.loc[0, "source"] = "current_st_snapshot"
    current_result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(current),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, dates
        ),
    ).fetch_chunk(plan["chunks"][0])
    assert current_result.provider_status == "BLOCKED"
    assert current_result.failure["category"] == "SEMANTIC_SOURCE_UNAVAILABLE"

    empty = _empty_standard_frame("st_history")
    _set_fixture_attrs(
        empty,
        "st_history",
        dates=dates,
        source_semantics="HISTORICAL_INTERVAL_SOURCE",
        interval_row_count=0,
    )
    valid_empty = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(empty),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, dates
        ),
    ).fetch_chunk(plan["chunks"][0])
    assert valid_empty.provider_status == "VALID_EMPTY"
    assert valid_empty.valid_empty is True
    assert valid_empty.coverage["valid_empty"] is True
    assert valid_empty.coverage["complete"] is True
    assert valid_empty.source_semantics == "HISTORICAL_INTERVAL_SOURCE"

    for field in ["coverage_complete", "requested_codes", "coverage_start_date", "interval_row_count"]:
        incomplete = _copy_frame(empty)
        incomplete.attrs.pop(field, None)
        blocked = provider.HistoricalProviderRouter(
            plan=plan,
            provider_name="fixture",
            raw_fetch_fn=lambda endpoint, parameters, incomplete=incomplete: _copy_frame(incomplete),
            trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
                start_date, end_date, dates
            ),
        ).fetch_chunk(plan["chunks"][0])
        assert blocked.provider_status == "BLOCKED"
        assert blocked.failure["category"] == "SEMANTIC_SOURCE_UNAVAILABLE"


@pytest.mark.parametrize(
    "defect",
    ["missing_index", "extra_index", "duplicate", "out_of_window", "bad_numeric", "missing_previous_close"],
)
def test_benchmark_requires_exact_three_index_open_date_product_and_previous_close(defect):
    provider = _historical_provider_module()
    dates = ["2024-01-02", "2024-01-03"]
    plan = _provider_plan("benchmark_price", start_date=dates[0], end_date=dates[-1])
    raw = _historical_fixture_frame("benchmark_price", dates=dates)
    if defect == "missing_index":
        raw = raw.iloc[1:].copy()
    elif defect == "extra_index":
        extra = raw.iloc[[0]].copy()
        extra.loc[:, "index_code"] = "000001.SH"
        raw = pd.concat([raw, extra], ignore_index=True)
    elif defect == "duplicate":
        raw = pd.concat([raw, raw.iloc[[0]]], ignore_index=True)
    elif defect == "out_of_window":
        raw.loc[0, "trade_date"] = "20240104"
    elif defect == "bad_numeric":
        raw.loc[0, "close"] = np.inf
    elif defect == "missing_previous_close":
        raw = raw.drop(columns=["pre_close"])
    _set_fixture_attrs(raw, "benchmark_price", dates=dates)

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(raw),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, dates
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] == "DQ_FAILED"
    assert result.frame.empty


@pytest.mark.parametrize(
    ("case", "category", "retryable", "status"),
    [
        ("empty", "EMPTY_RESULT", False, "BLOCKED"),
        ("rate", "RATE_LIMITED", True, "FAILED"),
        ("permission", "PERMISSION_DENIED", False, "BLOCKED"),
        ("schema", "SCHEMA_DRIFT", False, "BLOCKED"),
        ("transient", "TRANSIENT_PROVIDER_ERROR", True, "FAILED"),
        ("configuration", "CONFIGURATION_ERROR", False, "BLOCKED"),
        ("semantic", "SEMANTIC_SOURCE_UNAVAILABLE", False, "BLOCKED"),
        ("dq", "DQ_FAILED", False, "BLOCKED"),
    ],
)
def test_historical_provider_failure_conversion_is_precise_and_redacted(case, category, retryable, status):
    provider = _historical_provider_module()
    dataset = "stock_basic" if case == "semantic" else "adj_factor"
    plan = _provider_plan(dataset, start_date="2024-01-02", end_date="2024-01-02")
    raw_fetch = None if case == "configuration" else _failure_fetcher(case)
    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="tushare" if case == "semantic" else "fixture",
        raw_fetch_fn=raw_fetch,
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, ["2024-01-02"]
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == status
    assert result.failure["category"] == category
    assert result.failure["retryable"] is retryable
    assert result.validation["passed"] is False
    assert tuple(result.frame.columns) == tuple(_empty_standard_frame(dataset).columns)
    assert result.frame.empty
    assert "super-secret" not in result.failure["message"]
    if case == "schema":
        assert result.actual_schema == ("ts_code", "trade_date")
        assert len(result.provider_calls) == 1
        assert result.provider_calls[0]["endpoint"] == "adj_factor"


@pytest.mark.parametrize("interrupt", [KeyboardInterrupt(), SystemExit(7)], ids=["keyboard", "system-exit"])
def test_router_does_not_swallow_keyboard_interrupt(interrupt):
    provider = _historical_provider_module()
    plan = _provider_plan("adj_factor", start_date="2024-01-02", end_date="2024-01-02")

    def raw_fetch(endpoint, parameters):
        raise interrupt

    with pytest.raises(type(interrupt)):
        provider.HistoricalProviderRouter(
            plan=plan,
            provider_name="fixture",
            raw_fetch_fn=raw_fetch,
            trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
                start_date, end_date, ["2024-01-02"]
            ),
        ).fetch_chunk(plan["chunks"][0])


@pytest.mark.parametrize(
    ("provider_name", "dataset", "category"),
    (
        [("tushare", dataset, "SEMANTIC_SOURCE_UNAVAILABLE") for dataset in ["stock_basic", "financial", "st_history", "benchmark_price"]]
        + [("akshare", dataset, "SEMANTIC_SOURCE_UNAVAILABLE") for dataset in ALL_DATASETS if dataset != "benchmark_price"]
        + [("baostock", dataset, "SEMANTIC_SOURCE_UNAVAILABLE") for dataset in ALL_DATASETS]
        + [(name, dataset, "CONFIGURATION_ERROR") for name in ["unknown", "disabled"] for dataset in ALL_DATASETS]
    ),
)
def test_conservative_live_matrix_blocks_unsupported_datasets_before_raw_fetch(provider_name, dataset, category):
    provider = _historical_provider_module()
    plan = _provider_plan(dataset, start_date="2024-01-02", end_date="2024-01-02")
    raw_calls = []

    def raw_fetch(endpoint, parameters):
        raw_calls.append((endpoint, parameters))
        raise AssertionError("unsupported matrix path must not call raw provider")

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name=provider_name,
        raw_fetch_fn=raw_fetch,
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, ["2024-01-02"]
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert raw_calls == []
    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] == category


def test_partition_projection_contract_is_unambiguous_for_financial_and_valid_empty_st():
    provider = _historical_provider_module()
    dates = ["2024-01-02", "2024-01-03", "2024-01-04"]
    financial_plan = _provider_plan("financial", start_date=dates[0], end_date=dates[-1])
    financial_raw = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "end_date": ["20240102"],
            "ann_date": ["20240103"],
            "revenue_yoy": [10.0],
            "net_profit_yoy": [8.0],
            "roe": [0.12],
            "gross_margin": [0.30],
            "debt_ratio": [0.40],
            "operating_cashflow": [100.0],
        }
    )
    _set_fixture_attrs(
        financial_raw,
        "financial",
        dates=dates,
        source_semantics="TRUSTED_STANDARD_FINANCIAL_SOURCE",
    )
    financial = provider.HistoricalProviderRouter(
        plan=financial_plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(financial_raw),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, dates
        ),
    ).fetch_chunk(financial_plan["chunks"][0])
    iterator = provider.iter_historical_canonical_partitions(financial)
    assert iter(iterator) is iterator
    first_date, first_frame = next(iterator)
    assert first_date == "2024-01-02"
    assert first_frame.empty
    assert next(iterator)[1]["announce_date"].tolist() == ["2024-01-03"]

    st_plan = _provider_plan("st_history", start_date=dates[0], end_date=dates[-1])
    empty_st = _empty_standard_frame("st_history")
    _set_fixture_attrs(
        empty_st,
        "st_history",
        dates=dates,
        source_semantics="HISTORICAL_INTERVAL_SOURCE",
        interval_row_count=0,
    )
    valid_empty_st = provider.HistoricalProviderRouter(
        plan=st_plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(empty_st),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, dates
        ),
    ).fetch_chunk(st_plan["chunks"][0])
    partitions = list(provider.iter_historical_canonical_partitions(valid_empty_st))
    assert valid_empty_st.provider_status == "VALID_EMPTY"
    assert valid_empty_st.partition_strategy == "ST_INTERVAL_HISTORY"
    assert [trade_date for trade_date, _ in partitions] == dates
    assert all(frame.empty and tuple(frame.columns) == valid_empty_st.target_schema for _, frame in partitions)
    partitions[0][1]["stock_code"] = ["000001.SZ"]
    assert list(provider.iter_historical_canonical_partitions(valid_empty_st))[0][1].empty


def test_calendar_uses_full_plan_scope_once_and_cached_copy_cannot_be_mutated():
    provider = _historical_provider_module()
    plan = _plan(
        run_id="goal21-calendar-cache",
        datasets=["adj_factor"],
        codes=["000001.SZ"],
        start_date="2024-01-01",
        end_date="2024-01-04",
        date_batch_days=2,
        report_period_months=12,
    )
    calendar_calls = []
    returned_calendars = []

    def calendar_fn(start_date, end_date):
        calendar_calls.append((start_date, end_date))
        frame = _complete_calendar_frame(start_date, end_date, ["2024-01-02", "2024-01-04"])
        returned_calendars.append(frame)
        return frame

    def raw_fetch(endpoint, parameters):
        start = parameters["start_date"]
        end = parameters["end_date"]
        dates = [item for item in ["2024-01-02", "2024-01-04"] if start <= item <= end]
        frame = pd.DataFrame(
            {
                "ts_code": ["000001.SZ" for _ in dates],
                "trade_date": [item.replace("-", "") for item in dates],
                "adj_factor": [1.0 + offset / 100 for offset, _ in enumerate(dates)],
            }
        )
        _set_fixture_attrs(frame, "adj_factor", dates=dates or [start], coverage_complete=bool(dates))
        frame.attrs["coverage_start_date"] = start
        frame.attrs["coverage_end_date"] = end
        return frame

    router = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=raw_fetch,
        trading_calendar_fn=calendar_fn,
    )
    first = router.fetch_chunk(plan["chunks"][0])
    returned_calendars[0].loc[:, "is_open"] = 0
    second = router.fetch_chunk(plan["chunks"][1])

    assert calendar_calls == [("2024-01-01", "2024-01-04")]
    assert first.canonical_trade_dates == ("2024-01-02",)
    assert second.canonical_trade_dates == ("2024-01-04",)
    assert first.provider_status == second.provider_status == "FETCHED"


@pytest.mark.parametrize(
    ("case", "category"),
    [
        ("missing_fn", "CONFIGURATION_ERROR"),
        ("empty", "SEMANTIC_SOURCE_UNAVAILABLE"),
        ("duplicate_conflict", "DQ_FAILED"),
        ("out_of_range", "DQ_FAILED"),
        ("invalid_is_open", "DQ_FAILED"),
        ("incomplete", "SEMANTIC_SOURCE_UNAVAILABLE"),
    ],
)
def test_invalid_calendar_proof_blocks_before_raw_fetch(case, category):
    provider = _historical_provider_module()
    plan = _provider_plan("adj_factor", start_date="2024-01-02", end_date="2024-01-03")
    rows = [
        {"trade_date": "2024-01-02", "is_open": 1},
        {"trade_date": "2024-01-03", "is_open": 1},
    ]
    if case == "empty":
        calendar_fn = lambda start_date, end_date: pd.DataFrame(columns=["trade_date", "is_open"])
    elif case == "duplicate_conflict":
        calendar_fn = lambda start_date, end_date: pd.DataFrame(rows + [{"trade_date": "2024-01-02", "is_open": 0}])
    elif case == "out_of_range":
        calendar_fn = lambda start_date, end_date: pd.DataFrame(rows + [{"trade_date": "2024-01-04", "is_open": 1}])
    elif case == "invalid_is_open":
        calendar_fn = lambda start_date, end_date: pd.DataFrame([rows[0], {"trade_date": "2024-01-03", "is_open": "maybe"}])
    elif case == "incomplete":
        calendar_fn = lambda start_date, end_date: pd.DataFrame(rows[:1])
    else:
        calendar_fn = None
    raw_calls = []
    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: raw_calls.append((endpoint, parameters)),
        trading_calendar_fn=calendar_fn,
    ).fetch_chunk(plan["chunks"][0])

    assert raw_calls == []
    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] == category


@pytest.mark.parametrize(
    "source_key",
    [
        "",
        "/archive/adj_factor.parquet",
        "C:/archive/adj_factor.parquet",
        "archive\\adj_factor.parquet",
        "archive//adj_factor.parquet",
        "archive/./adj_factor.parquet",
        "archive/../adj_factor.parquet",
        "smoke/adj_factor.parquet",
        "candidate/real_history_backfill/run_id=x/part.parquet",
        "candidate/real_clean_inputs/adj_factor_staging/batch_id=x/part.parquet",
    ],
)
def test_fixture_source_keys_reject_unsafe_smoke_and_self_lineage(source_key):
    provider = _historical_provider_module()
    plan = _provider_plan("adj_factor", start_date="2024-01-02", end_date="2024-01-02")
    raw = _historical_fixture_frame("adj_factor", dates=["2024-01-02"])
    raw.attrs["source_keys"] = [source_key] if source_key else []
    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(raw),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, ["2024-01-02"]
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] == "SEMANTIC_SOURCE_UNAVAILABLE"
    assert result.source_keys == ()


def test_router_defensive_copies_plan_chunk_raw_attrs_calendar_calls_and_result_metadata():
    provider = _historical_provider_module()
    caller_plan = _provider_plan("adj_factor", start_date="2024-01-02", end_date="2024-01-02")
    original_chunk = deepcopy(caller_plan["chunks"][0])
    returned_frames = []
    calendar_frames = []
    raw_parameters = []

    def raw_fetch(endpoint, parameters):
        raw_parameters.append(parameters)
        parameters["nested"] = {
            "Authorization": "Bearer nested-auth-secret",
            "password": "nested-password-secret",
        }
        frame = _historical_fixture_frame("adj_factor", dates=["2024-01-02"])
        frame.attrs["ignored_callable"] = lambda: "must not persist"
        frame.attrs["ignored_timestamp"] = pd.Timestamp("2024-01-02")
        frame.attrs["ignored_numpy"] = np.int64(7)
        returned_frames.append(frame)
        return frame

    def calendar_fn(start_date, end_date):
        frame = _complete_calendar_frame(start_date, end_date, ["2024-01-02"])
        calendar_frames.append(frame)
        return frame

    router = provider.HistoricalProviderRouter(
        plan=caller_plan,
        provider_name="fixture",
        raw_fetch_fn=raw_fetch,
        trading_calendar_fn=calendar_fn,
    )
    caller_plan["scope"]["start_date"] = "1999-01-01"
    caller_plan["chunks"][0]["codes"][0] = "600519.SH"
    first = router.fetch_chunk(original_chunk)

    returned_frames[0].loc[0, "adj_factor"] = 99.0
    returned_frames[0].attrs["source_keys"][0] = "mutated/source.parquet"
    calendar_frames[0].loc[0, "is_open"] = 0
    assert first.frame.loc[0, "adj_factor"] == 1.0
    assert first.source_keys == ("archive/adj_factor/history.parquet",)

    first.frame.loc[0, "adj_factor"] = 77.0
    first.coverage["complete"] = False
    first.dq["blocked_reasons"].append("caller mutation")
    first.provider_calls[0]["parameters"]["start_date"] = "19000101"
    second = router.fetch_chunk(deepcopy(original_chunk))

    assert second.frame.loc[0, "adj_factor"] == 1.0
    assert second.coverage["complete"] is True
    assert second.dq["blocked_reasons"] == []
    assert second.provider_calls[0]["parameters"]["start_date"] == "2024-01-02"
    assert len(calendar_frames) == 1
    serialized = json.dumps(
        {
            "provider_status": second.provider_status,
            "source_keys": second.source_keys,
            "source_semantics": second.source_semantics,
            "provider_calls": second.provider_calls,
            "actual_schema": second.actual_schema,
            "target_schema": second.target_schema,
            "dq": second.dq,
            "coverage": second.coverage,
            "canonical_trade_dates": second.canonical_trade_dates,
            "partition_strategy": second.partition_strategy,
            "valid_empty": second.valid_empty,
            "validation": second.validation,
            "failure": second.failure,
        }
    ).lower()
    assert "callable" not in serialized
    assert "timestamp" not in serialized
    assert "numpy" not in serialized
    assert "nested-auth-secret" not in serialized
    assert "nested-password-secret" not in serialized


def test_stock_snapshot_date_must_match_trade_date_before_standard_projection():
    provider = _historical_provider_module()
    plan = _provider_plan("stock_basic", start_date="2024-01-02", end_date="2024-01-02")
    raw = _historical_fixture_frame("stock_basic", dates=["2024-01-02"])
    raw.loc[0, "snapshot_date"] = "20240103"

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(raw),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, ["2024-01-02"]
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] == "SEMANTIC_SOURCE_UNAVAILABLE"
    assert result.actual_schema == tuple(raw.columns)


@pytest.mark.parametrize("defect", ["count_mismatch", "extra_code", "outside_scope"])
def test_st_history_proof_and_rows_must_match_the_requested_interval_scope(defect):
    provider = _historical_provider_module()
    plan = _provider_plan("st_history", start_date="2024-01-02", end_date="2024-01-04")
    raw = _historical_fixture_frame("st_history", dates=["2024-01-02", "2024-01-03", "2024-01-04"])
    if defect == "count_mismatch":
        raw.attrs["interval_row_count"] = 2
    elif defect == "extra_code":
        raw.loc[0, "ts_code"] = "600519.SH"
    else:
        raw.loc[0, "start_date"] = "20240105"
        raw.loc[0, "end_date"] = "20240106"

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(raw),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, ["2024-01-02", "2024-01-03", "2024-01-04"]
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] in {"DQ_FAILED", "SEMANTIC_SOURCE_UNAVAILABLE"}
    assert result.actual_schema == tuple(raw.columns)


@pytest.mark.parametrize("pre_close", [0.0, np.nan, np.inf])
def test_benchmark_previous_close_evidence_must_be_finite_and_positive(pre_close):
    provider = _historical_provider_module()
    plan = _provider_plan("benchmark_price", start_date="2024-01-02", end_date="2024-01-02")
    raw = _historical_fixture_frame("benchmark_price", dates=["2024-01-02"])
    raw.loc[0, "pre_close"] = pre_close

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(raw),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, ["2024-01-02"]
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] == "DQ_FAILED"


def test_fixture_daily_price_cannot_infer_pause_values_without_event_proof():
    provider = _historical_provider_module()
    plan = _provider_plan("daily_price", start_date="2024-01-02", end_date="2024-01-02")
    raw = _historical_fixture_frame("daily_price", dates=["2024-01-02"])
    raw.attrs.pop("suspend_d_full_event_coverage")

    unproven = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(raw),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, ["2024-01-02"]
        ),
    ).fetch_chunk(plan["chunks"][0])
    assert unproven.provider_status == "BLOCKED"
    assert unproven.failure["category"] == "SEMANTIC_SOURCE_UNAVAILABLE"

    asserted_pause = _historical_fixture_frame("daily_price", dates=["2024-01-02"])
    asserted_pause.loc[0, "is_paused"] = True
    asserted_pause.attrs["pause_event_keys"] = []
    missing_hit = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(asserted_pause),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, ["2024-01-02"]
        ),
    ).fetch_chunk(plan["chunks"][0])
    assert missing_hit.provider_status == "BLOCKED"
    assert missing_hit.failure["category"] == "SEMANTIC_SOURCE_UNAVAILABLE"


def test_failure_results_preserve_raw_schema_and_sanitized_provider_calls():
    provider = _historical_provider_module()
    plan = _provider_plan("adj_factor", start_date="2024-01-02", end_date="2024-01-02")
    raw = _historical_fixture_frame("adj_factor", dates=["2024-01-02"])
    raw = pd.concat([raw, raw], ignore_index=True)
    raw.attrs = deepcopy(_historical_fixture_frame("adj_factor", dates=["2024-01-02"]).attrs)

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(raw),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, ["2024-01-02"]
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "BLOCKED"
    assert result.actual_schema == tuple(raw.columns)
    assert len(result.provider_calls) == 1
    assert result.provider_calls[0]["endpoint"] == "adj_factor"


def test_failure_schema_labels_are_normalized_to_json_safe_strings():
    provider = _historical_provider_module()
    plan = _provider_plan("adj_factor", start_date="2024-01-02", end_date="2024-01-02")
    raw = pd.DataFrame([["000001.SZ", "20240102", 1.0]], columns=[0, 1, 2])

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: raw.copy(deep=True),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, ["2024-01-02"]
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] == "SCHEMA_DRIFT"
    assert result.actual_schema == ("0", "1", "2")
    json.dumps(result.actual_schema)


def test_result_contract_rejects_unsafe_lineage_and_redacts_standalone_auth_schemes():
    provider = _historical_provider_module()
    kwargs = _result_contract_kwargs(provider, status="FETCHED")
    with pytest.raises(ValueError):
        provider.HistoricalChunkFetchResult(
            **dict(kwargs, source_keys=("candidate/real_history_backfill/run_id=x/part.parquet",))
        )

    calls = (
        {
            "endpoint": "adj_factor",
            "strategy": "fixture_historical_chunk",
            "parameters": {
                "headers": [
                    "Basic basic-secret",
                    "ApiKey apikey-secret",
                    "Token token-secret",
                    "Bearer bearer-secret",
                ]
            },
            "row_count": 1,
            "status": "FETCHED",
        },
    )
    result = provider.HistoricalChunkFetchResult(**dict(kwargs, provider_calls=calls))
    serialized = json.dumps(result.provider_calls).lower()
    for secret in ["basic-secret", "apikey-secret", "token-secret", "bearer-secret"]:
        assert secret not in serialized


@pytest.mark.parametrize(
    ("dataset", "mutation"),
    [
        ("adj_factor", "missing_sample_proof"),
        ("adj_factor", "wrong_codes"),
        ("adj_factor", "late_start"),
        ("adj_factor", "early_end"),
        ("benchmark_price", "wrong_indexes"),
    ],
)
def test_fixture_lineage_requires_explicit_complete_scope_proof(dataset, mutation):
    provider = _historical_provider_module()
    dates = ["2024-01-02", "2024-01-03"]
    plan = _provider_plan(dataset, start_date=dates[0], end_date=dates[-1])
    raw = _historical_fixture_frame(dataset, dates=dates)
    if mutation == "missing_sample_proof":
        raw.attrs.pop("sample_truncated")
    elif mutation == "wrong_codes":
        raw.attrs["requested_codes"] = ["600519.SH"]
    elif mutation == "late_start":
        raw.attrs["coverage_start_date"] = "2024-01-03"
    elif mutation == "early_end":
        raw.attrs["coverage_end_date"] = "2024-01-02"
    else:
        raw.attrs["requested_indexes"] = ["000300.SH"]

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(raw),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, dates
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] == "SEMANTIC_SOURCE_UNAVAILABLE"


@pytest.mark.parametrize(
    "mutation",
    [
        "fingerprint",
        "scope_date_count",
        "scope_codes",
        "scope_lineage",
        "limits",
        "datasets",
        "strategies",
        "chunk_count",
        "extra_field",
    ],
)
def test_router_rejects_any_tampered_plan_identity_or_scope_before_callbacks(mutation):
    provider = _historical_provider_module()
    plan = _provider_plan("adj_factor", start_date="2024-01-02", end_date="2024-01-03")
    if mutation == "fingerprint":
        plan["plan_fingerprint"] = "0" * 64
    elif mutation == "scope_date_count":
        plan["scope"]["date_count"] += 1
    elif mutation == "scope_codes":
        plan["scope"]["codes"] = ["600519.SH"]
    elif mutation == "scope_lineage":
        plan["scope"]["universe_source"] = "universe_frame"
        plan["scope"]["universe_key"] = "archive/universe/history.parquet"
    elif mutation == "limits":
        plan["limits"]["date_batch_days"] += 1
    elif mutation == "datasets":
        plan["datasets"] = ["daily_basic"]
    elif mutation == "strategies":
        plan["strategies"]["adj_factor"] = "forged"
    elif mutation == "chunk_count":
        plan["chunk_count"] += 1
    else:
        plan["unexpected_identity_field"] = "forged"
    calls = []

    with pytest.raises(provider.HistoricalProviderError) as exc_info:
        provider.HistoricalProviderRouter(
            plan=plan,
            provider_name="fixture",
            raw_fetch_fn=lambda endpoint, parameters: calls.append((endpoint, parameters)),
            trading_calendar_fn=lambda start_date, end_date: calls.append((start_date, end_date)),
        )

    assert exc_info.value.failure_category == "CONFIGURATION_ERROR"
    assert calls == []


def test_fixture_pause_event_proof_must_match_is_paused_in_both_directions():
    provider = _historical_provider_module()
    date_value = "2024-01-02"
    plan = _provider_plan("daily_price", start_date=date_value, end_date=date_value)
    raw = _historical_fixture_frame("daily_price", dates=[date_value])
    assert raw["is_paused"].tolist() == [False]
    raw.attrs["pause_event_keys"] = [("000001.SZ", date_value)]

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(raw),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, [date_value]
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] == "SEMANTIC_SOURCE_UNAVAILABLE"
    assert result.actual_schema == tuple(raw.columns)
    assert len(result.provider_calls) == 1


def test_financial_rows_cannot_escape_the_requested_code_scope():
    provider = _historical_provider_module()
    date_value = "2024-01-02"
    plan = _provider_plan("financial", start_date=date_value, end_date=date_value)
    raw = _historical_fixture_frame("financial", dates=[date_value])
    raw.loc[0, "ts_code"] = "600519.SH"

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(raw),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, [date_value]
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] == "DQ_FAILED"
    assert result.actual_schema == tuple(raw.columns)


def test_stock_basic_valid_empty_requires_complete_nonmember_master_proof():
    provider = _historical_provider_module()
    dates = ["2024-01-02", "2024-01-03"]
    codes = ["000001.SZ", "600519.SH"]
    plan = _provider_plan(
        "stock_basic",
        start_date=dates[0],
        end_date=dates[-1],
        codes=codes,
    )
    raw = _historical_fixture_frame("stock_basic", dates=dates).iloc[0:0].copy()
    _set_fixture_attrs(raw, "stock_basic", dates=dates, codes=codes)
    raw.attrs["list_delist_master"] = [
        {"stock_code": "000001.SZ", "list_date": "2024-02-01", "delist_date": None},
        {"stock_code": "600519.SH", "list_date": "2020-01-01", "delist_date": "2024-01-02"},
    ]

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(raw),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, dates
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "VALID_EMPTY"
    assert result.valid_empty is True
    assert result.frame.empty
    assert result.coverage["complete"] is True
    assert result.coverage["expected_count"] == 0
    assert result.coverage["covered_count"] == 0
    assert result.coverage["not_yet_listed_count"] == 2
    assert result.coverage["already_delisted_count"] == 2


@pytest.mark.parametrize("defect", ["missing_master", "active_member"])
def test_stock_basic_empty_is_blocked_without_proof_that_every_code_is_a_nonmember(defect):
    provider = _historical_provider_module()
    dates = ["2024-01-02", "2024-01-03"]
    plan = _provider_plan("stock_basic", start_date=dates[0], end_date=dates[-1])
    raw = _historical_fixture_frame("stock_basic", dates=dates).iloc[0:0].copy()
    _set_fixture_attrs(raw, "stock_basic", dates=dates)
    if defect == "active_member":
        raw.attrs["list_delist_master"] = [
            {"stock_code": "000001.SZ", "list_date": "2020-01-01", "delist_date": None}
        ]

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(raw),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, dates
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] in {"SEMANTIC_SOURCE_UNAVAILABLE", "DQ_FAILED"}


def test_stock_basic_master_allows_a_requested_nonmember_to_have_no_snapshot_rows():
    provider = _historical_provider_module()
    dates = ["2024-01-02", "2024-01-03"]
    codes = ["000001.SZ", "600519.SH"]
    plan = _provider_plan(
        "stock_basic",
        start_date=dates[0],
        end_date=dates[-1],
        codes=codes,
    )
    raw = _historical_fixture_frame("stock_basic", dates=dates)
    _set_fixture_attrs(raw, "stock_basic", dates=dates, codes=codes)
    raw.attrs["list_delist_master"] = [
        {"stock_code": "000001.SZ", "list_date": "2020-01-01", "delist_date": None},
        {"stock_code": "600519.SH", "list_date": "2024-02-01", "delist_date": None},
    ]

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(raw),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, dates
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "FETCHED"
    assert set(result.frame["stock_code"]) == {"000001.SZ"}
    assert result.coverage["not_yet_listed_count"] == 2


def test_benchmark_pct_chg_must_be_consistent_with_close_and_pre_close():
    provider = _historical_provider_module()
    date_value = "2024-01-02"
    plan = _provider_plan("benchmark_price", start_date=date_value, end_date=date_value)
    raw = _historical_fixture_frame("benchmark_price", dates=[date_value])
    raw.loc[0, "pct_chg"] = 99.0

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(raw),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, [date_value]
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] == "DQ_FAILED"
    assert result.actual_schema == tuple(raw.columns)


def test_benchmark_missing_change_field_is_schema_drift_with_observed_evidence():
    provider = _historical_provider_module()
    date_value = "2024-01-02"
    plan = _provider_plan("benchmark_price", start_date=date_value, end_date=date_value)
    raw = _historical_fixture_frame("benchmark_price", dates=[date_value]).drop(columns=["pct_chg"])

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(raw),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, [date_value]
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] == "SCHEMA_DRIFT"
    assert result.actual_schema == tuple(raw.columns)
    assert len(result.provider_calls) == 1


def test_provider_configuration_error_is_nonretryable_and_keeps_failed_call_evidence():
    from stock_selector.providers.base import ProviderConfigurationError

    provider = _historical_provider_module()
    date_value = "2024-01-02"
    plan = _provider_plan("adj_factor", start_date=date_value, end_date=date_value)

    def raise_configuration_error(endpoint, parameters):
        _ = endpoint, parameters
        raise ProviderConfigurationError("missing token=configuration-secret")

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=raise_configuration_error,
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, [date_value]
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] == "CONFIGURATION_ERROR"
    assert result.failure["retryable"] is False
    assert result.failure["exception_type"] == "ProviderConfigurationError"
    assert len(result.provider_calls) == 1
    assert result.provider_calls[0]["status"] == "FAILED"
    assert "configuration-secret" not in json.dumps(result.provider_calls).lower()
    assert "configuration-secret" not in result.failure["message"].lower()


def test_non_dataframe_response_records_stable_schema_and_failed_provider_call():
    provider = _historical_provider_module()
    date_value = "2024-01-02"
    plan = _provider_plan("adj_factor", start_date=date_value, end_date=date_value)

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: {"unexpected": "mapping"},
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, [date_value]
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] == "SCHEMA_DRIFT"
    assert result.actual_schema == ("NON_DATAFRAME:dict",)
    assert len(result.provider_calls) == 1
    assert result.provider_calls[0]["status"] == "SCHEMA_DRIFT"


def test_composite_daily_price_schema_error_keeps_all_calls_and_primary_raw_schema():
    provider = _historical_provider_module()
    date_value = "2024-01-02"
    plan = _provider_plan("daily_price", start_date=date_value, end_date=date_value)
    daily = pd.DataFrame(
        {
            "ts_code": ["000001.SZ"],
            "trade_date": ["20240102"],
            "open": [10.0],
            "high": [10.5],
            "low": [9.5],
            "close": [10.1],
            "pre_close": [10.0],
            "vol": [100.0],
            "amount": [1000.0],
        }
    )

    def fetch(endpoint, parameters):
        _ = parameters
        if endpoint == "daily":
            return daily.copy(deep=True)
        if endpoint == "stk_limit":
            return pd.DataFrame(
                {"ts_code": ["000001.SZ"], "trade_date": ["20240102"], "up_limit": [11.0]}
            )
        if endpoint == "suspend_d":
            frame = pd.DataFrame(columns=["ts_code", "trade_date", "suspend_timing", "suspend_type"])
            frame.attrs.update(
                full_market_event_set=True,
                coverage_complete=True,
                sample_truncated=False,
                empty_after_retries=False,
                covered_trade_dates=["20240102"],
            )
            return frame
        raise AssertionError(endpoint)

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="tushare",
        raw_fetch_fn=fetch,
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, [date_value]
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] == "SCHEMA_DRIFT"
    assert result.actual_schema == tuple(daily.columns)
    assert [call["endpoint"] for call in result.provider_calls] == ["daily", "stk_limit", "suspend_d"]


def test_unknown_provider_parameter_objects_are_stringified_then_redacted():
    provider = _historical_provider_module()

    class OpaqueCredential:
        def __str__(self):
            return "Bearer opaque-bearer-secret token=opaque-token-secret"

    kwargs = _result_contract_kwargs(provider, status="FETCHED")
    calls = (
        {
            "endpoint": "adj_factor",
            "strategy": "fixture_historical_chunk",
            "parameters": {"opaque": OpaqueCredential()},
            "row_count": 1,
            "status": "FETCHED",
        },
    )

    result = provider.HistoricalChunkFetchResult(**dict(kwargs, provider_calls=calls))
    serialized = json.dumps(result.provider_calls).lower()
    assert "opaque-bearer-secret" not in serialized
    assert "opaque-token-secret" not in serialized


@pytest.mark.parametrize(
    "defect",
    [
        "missing_expected_count",
        "missing_covered_count",
        "missing_missing",
        "wrong_covered_count",
        "missing_not_list",
        "missing_dq_level",
        "invalid_blocked_reasons",
        "missing_validation_errors",
        "nonboolean_dq_passed",
        "nonboolean_valid_empty",
        "nonboolean_result_valid_empty",
        "duplicate_canonical_dates",
        "unsorted_canonical_dates",
        "coverage_date_mismatch",
    ],
)
def test_result_contract_rejects_incomplete_or_inconsistent_dq_coverage_and_dates(defect):
    provider = _historical_provider_module()
    kwargs = _result_contract_kwargs(provider, status="FETCHED")
    if defect == "missing_expected_count":
        kwargs["coverage"].pop("expected_count")
    elif defect == "missing_covered_count":
        kwargs["coverage"].pop("covered_count")
    elif defect == "missing_missing":
        kwargs["coverage"].pop("missing")
    elif defect == "wrong_covered_count":
        kwargs["coverage"]["covered_count"] = 2
    elif defect == "missing_not_list":
        kwargs["coverage"]["missing"] = "none"
    elif defect == "missing_dq_level":
        kwargs["dq"].pop("level")
    elif defect == "invalid_blocked_reasons":
        kwargs["dq"]["blocked_reasons"] = "none"
    elif defect == "missing_validation_errors":
        kwargs["validation"].pop("errors")
    elif defect == "nonboolean_dq_passed":
        kwargs["dq"]["passed"] = 1
    elif defect == "nonboolean_valid_empty":
        kwargs["coverage"]["valid_empty"] = 0
    elif defect == "nonboolean_result_valid_empty":
        kwargs["valid_empty"] = 0
    elif defect == "duplicate_canonical_dates":
        kwargs["canonical_trade_dates"] = ("2024-01-02", "2024-01-02")
        kwargs["coverage"]["canonical_trade_dates"] = ["2024-01-02", "2024-01-02"]
    elif defect == "unsorted_canonical_dates":
        kwargs["canonical_trade_dates"] = ("2024-01-03", "2024-01-02")
        kwargs["coverage"]["canonical_trade_dates"] = ["2024-01-03", "2024-01-02"]
        kwargs["coverage"]["requested_end_date"] = "2024-01-03"
    else:
        kwargs["coverage"]["canonical_trade_dates"] = ["2024-01-03"]

    with pytest.raises(ValueError):
        provider.HistoricalChunkFetchResult(**kwargs)


@pytest.mark.parametrize("status", ["BLOCKED", "FAILED"])
@pytest.mark.parametrize("contradiction", ["successful_dq", "complete_coverage"])
def test_failed_result_statuses_reject_success_evidence(status, contradiction):
    provider = _historical_provider_module()
    kwargs = _result_contract_kwargs(provider, status=status)
    if contradiction == "successful_dq":
        kwargs["dq"] = {"passed": True, "level": "STRICT", "blocked_reasons": []}
    else:
        kwargs["coverage"].update(
            complete=True,
            expected_count=0,
            covered_count=0,
            missing=[],
        )

    with pytest.raises(ValueError):
        provider.HistoricalChunkFetchResult(**kwargs)


@pytest.mark.parametrize("unsafe_kind", ["callable", "provider", "timestamp", "numpy"])
def test_result_metadata_rejects_non_json_safe_values(unsafe_kind):
    provider = _historical_provider_module()
    kwargs = _result_contract_kwargs(provider, status="FETCHED")

    class OpaqueProvider:
        pass

    unsafe = {
        "callable": lambda: None,
        "provider": OpaqueProvider(),
        "timestamp": pd.Timestamp("2024-01-02"),
        "numpy": np.int64(7),
    }[unsafe_kind]
    kwargs["coverage"]["opaque"] = unsafe

    with pytest.raises(ValueError, match="JSON-safe"):
        provider.HistoricalChunkFetchResult(**kwargs)


def test_result_metadata_strings_are_redacted_before_json_serialization():
    provider = _historical_provider_module()
    kwargs = _result_contract_kwargs(provider, status="FETCHED")
    kwargs["coverage"]["diagnostic"] = {
        "message": "Bearer result-bearer-secret token=result-token-secret"
    }

    result = provider.HistoricalChunkFetchResult(**kwargs)
    serialized = json.dumps(
        {
            "dq": result.dq,
            "coverage": result.coverage,
            "validation": result.validation,
            "failure": result.failure,
        }
    ).lower()

    assert "result-bearer-secret" not in serialized
    assert "result-token-secret" not in serialized


@pytest.mark.parametrize("dataset", ["financial", "st_history"])
def test_financial_and_st_chunk_results_keep_full_plan_canonical_dates(dataset):
    provider = _historical_provider_module()
    start_date = "2024-01-02"
    end_date = "2024-03-05" if dataset == "financial" else "2024-01-05"
    plan = _plan(
        run_id=f"goal21-{dataset}-multichunk-provider-test",
        datasets=[dataset],
        codes=["000001.SZ"],
        start_date=start_date,
        end_date=end_date,
        date_batch_days=2,
        report_period_months=1,
    )
    chunk = plan["chunks"][0]
    open_dates = [start_date, end_date]
    if dataset == "financial":
        raw = _historical_fixture_frame("financial", dates=[start_date])
        _set_fixture_attrs(
            raw,
            "financial",
            dates=[chunk["start_date"], chunk["end_date"]],
        )
    else:
        raw = _historical_fixture_frame("st_history", dates=[start_date])
        _set_fixture_attrs(
            raw,
            "st_history",
            dates=[chunk["start_date"], chunk["end_date"]],
        )

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(raw),
        trading_calendar_fn=lambda requested_start, requested_end: _complete_calendar_frame(
            requested_start, requested_end, open_dates
        ),
    ).fetch_chunk(chunk)

    assert result.provider_status == "FETCHED"
    assert result.canonical_trade_dates == tuple(open_dates)
    assert result.coverage["requested_start_date"] == chunk["start_date"]
    assert result.coverage["requested_end_date"] == chunk["end_date"]


@pytest.mark.parametrize(
    "defect",
    [
        "missing_endpoint",
        "extra_field",
        "empty_strategy",
        "invalid_parameters",
        "invalid_status",
        "string_row_count",
        "negative_row_count",
        "boolean_row_count",
        "fetched_zero_rows",
    ],
)
def test_provider_call_contract_rejects_incomplete_or_invalid_evidence(defect):
    provider = _historical_provider_module()
    kwargs = _result_contract_kwargs(provider, status="FETCHED")
    call = {
        "endpoint": "adj_factor",
        "strategy": "by_code_date_window",
        "parameters": {"ts_code": "000001.SZ"},
        "row_count": 1,
        "status": "FETCHED",
    }
    if defect == "missing_endpoint":
        call.pop("endpoint")
    elif defect == "extra_field":
        call["raw_response"] = "unsafe"
    elif defect == "empty_strategy":
        call["strategy"] = ""
    elif defect == "invalid_parameters":
        call["parameters"] = ["not", "a", "mapping"]
    elif defect == "invalid_status":
        call["status"] = "SUCCEEDED"
    elif defect == "string_row_count":
        call["row_count"] = "1"
    elif defect == "negative_row_count":
        call["row_count"] = -1
    elif defect == "boolean_row_count":
        call["row_count"] = True
    else:
        call["row_count"] = 0

    with pytest.raises(ValueError):
        provider.HistoricalChunkFetchResult(**dict(kwargs, provider_calls=(call,)))


def test_provider_call_endpoint_and_strategy_are_sanitized_like_parameters():
    provider = _historical_provider_module()
    kwargs = _result_contract_kwargs(provider, status="FETCHED")
    call = {
        "endpoint": "Bearer endpoint-secret",
        "strategy": "token=strategy-secret",
        "parameters": {},
        "row_count": 1,
        "status": "FETCHED",
    }

    result = provider.HistoricalChunkFetchResult(**dict(kwargs, provider_calls=(call,)))
    serialized = json.dumps(result.provider_calls).lower()
    assert "endpoint-secret" not in serialized
    assert "strategy-secret" not in serialized


def test_live_suspend_rows_must_equal_the_requested_trade_date():
    provider = _historical_provider_module()
    date_value = "2024-01-02"
    plan = _provider_plan("daily_price", start_date=date_value, end_date=date_value)

    def fetch(endpoint, parameters):
        _ = parameters
        if endpoint == "daily":
            return pd.DataFrame(
                {
                    "ts_code": ["000001.SZ"],
                    "trade_date": ["20240102"],
                    "open": [10.0],
                    "high": [10.5],
                    "low": [9.5],
                    "close": [10.1],
                    "pre_close": [10.0],
                    "vol": [100.0],
                    "amount": [1000.0],
                }
            )
        if endpoint == "stk_limit":
            return pd.DataFrame(
                {
                    "ts_code": ["000001.SZ"],
                    "trade_date": ["20240102"],
                    "up_limit": [11.0],
                    "down_limit": [9.0],
                }
            )
        if endpoint == "suspend_d":
            frame = pd.DataFrame(
                {"ts_code": ["000001.SZ"], "trade_date": ["20240103"], "suspend_type": ["S"]}
            )
            frame.attrs.update(
                full_market_event_set=True,
                coverage_complete=True,
                sample_truncated=False,
                empty_after_retries=False,
                covered_trade_dates=["20240102"],
            )
            return frame
        raise AssertionError(endpoint)

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="tushare",
        raw_fetch_fn=fetch,
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, [date_value]
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] == "DQ_FAILED"
    assert [call["endpoint"] for call in result.provider_calls] == ["daily", "stk_limit", "suspend_d"]


def test_live_limit_rows_must_equal_the_requested_trade_date():
    provider = _historical_provider_module()
    date_value = "2024-01-02"
    plan = _provider_plan("daily_price", start_date=date_value, end_date=date_value)

    def fetch(endpoint, parameters):
        _ = parameters
        if endpoint == "daily":
            return pd.DataFrame(
                {
                    "ts_code": ["000001.SZ"],
                    "trade_date": ["20240102"],
                    "open": [10.0],
                    "high": [10.5],
                    "low": [9.5],
                    "close": [10.1],
                    "pre_close": [10.0],
                    "vol": [100.0],
                    "amount": [1000.0],
                }
            )
        if endpoint == "stk_limit":
            return pd.DataFrame(
                {
                    "ts_code": ["000001.SZ", "000001.SZ"],
                    "trade_date": ["20240102", "20240103"],
                    "up_limit": [11.0, 11.1],
                    "down_limit": [9.0, 9.1],
                }
            )
        if endpoint == "suspend_d":
            frame = pd.DataFrame(columns=["ts_code", "trade_date", "suspend_timing", "suspend_type"])
            frame.attrs.update(
                full_market_event_set=True,
                coverage_complete=True,
                sample_truncated=False,
                empty_after_retries=False,
                covered_trade_dates=["20240102"],
            )
            return frame
        raise AssertionError(endpoint)

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="tushare",
        raw_fetch_fn=fetch,
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, [date_value]
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] == "DQ_FAILED"
    assert [call["endpoint"] for call in result.provider_calls] == ["daily", "stk_limit"]


def test_proven_complete_empty_suspend_without_columns_is_accepted():
    provider = _historical_provider_module()
    date_value = "2024-01-02"
    plan = _provider_plan("daily_price", start_date=date_value, end_date=date_value)

    def fetch(endpoint, parameters):
        _ = parameters
        if endpoint == "daily":
            return pd.DataFrame(
                {
                    "ts_code": ["000001.SZ"],
                    "trade_date": ["20240102"],
                    "open": [10.0],
                    "high": [10.5],
                    "low": [9.5],
                    "close": [10.1],
                    "pre_close": [10.0],
                    "vol": [100.0],
                    "amount": [1000.0],
                }
            )
        if endpoint == "stk_limit":
            return pd.DataFrame(
                {
                    "ts_code": ["000001.SZ"],
                    "trade_date": ["20240102"],
                    "up_limit": [11.0],
                    "down_limit": [9.0],
                }
            )
        if endpoint == "suspend_d":
            frame = pd.DataFrame()
            frame.attrs.update(
                full_market_event_set=True,
                coverage_complete=True,
                sample_truncated=False,
                empty_after_retries=False,
                covered_trade_dates=["20240102"],
            )
            return frame
        raise AssertionError(endpoint)

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="tushare",
        raw_fetch_fn=fetch,
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, [date_value]
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "FETCHED"
    assert result.frame["is_paused"].tolist() == [False]
    assert result.provider_calls[-1]["endpoint"] == "suspend_d"
    assert result.provider_calls[-1]["status"] == "VALID_EMPTY"


def test_empty_dataframe_without_columns_is_empty_result_not_schema_drift():
    provider = _historical_provider_module()
    date_value = "2024-01-02"
    plan = _provider_plan("adj_factor", start_date=date_value, end_date=date_value)
    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: pd.DataFrame(),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, [date_value]
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] == "EMPTY_RESULT"
    assert result.actual_schema == ()
    assert len(result.provider_calls) == 1


def test_present_but_invalid_numeric_value_is_dq_failed_not_schema_drift():
    provider = _historical_provider_module()
    date_value = "2024-01-02"
    plan = _provider_plan("adj_factor", start_date=date_value, end_date=date_value)
    raw = _historical_fixture_frame("adj_factor", dates=[date_value])
    raw["adj_factor"] = raw["adj_factor"].astype(object)
    raw.loc[0, "adj_factor"] = "not-a-number"
    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="fixture",
        raw_fetch_fn=lambda endpoint, parameters: _copy_frame(raw),
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, [date_value]
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] == "DQ_FAILED"
    assert result.actual_schema == tuple(raw.columns)


@pytest.mark.parametrize("bad_endpoint", ["stk_limit", "suspend_d"])
def test_live_limit_and_suspend_conversion_errors_are_schema_drift_not_unknown(bad_endpoint):
    provider = _historical_provider_module()
    date_value = "2024-01-02"
    plan = _provider_plan("daily_price", start_date=date_value, end_date=date_value)

    def fetch(endpoint, parameters):
        _ = parameters
        if endpoint == "daily":
            return pd.DataFrame(
                {
                    "ts_code": ["000001.SZ"],
                    "trade_date": ["20240102"],
                    "open": [10.0],
                    "high": [10.5],
                    "low": [9.5],
                    "close": [10.1],
                    "pre_close": [10.0],
                    "vol": [100.0],
                    "amount": [1000.0],
                }
            )
        if endpoint == "stk_limit":
            code = "BAD" if bad_endpoint == endpoint else "000001.SZ"
            return pd.DataFrame(
                {
                    "ts_code": [code],
                    "trade_date": ["20240102"],
                    "up_limit": [11.0],
                    "down_limit": [9.0],
                }
            )
        if endpoint == "suspend_d":
            code = "BAD" if bad_endpoint == endpoint else "000001.SZ"
            frame = pd.DataFrame({"ts_code": [code], "trade_date": ["20240102"]})
            frame.attrs.update(
                full_market_event_set=True,
                coverage_complete=True,
                sample_truncated=False,
                empty_after_retries=False,
                covered_trade_dates=["20240102"],
            )
            return frame
        raise AssertionError(endpoint)

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="tushare",
        raw_fetch_fn=fetch,
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, [date_value]
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] == "SCHEMA_DRIFT"
    assert result.actual_schema[0] == "ts_code"
    assert result.provider_calls


@pytest.mark.parametrize(
    ("dataset", "expected_fields"),
    [
        ("adj_factor", "ts_code,trade_date,adj_factor"),
        (
            "daily_basic",
            "ts_code,trade_date,pe_ttm,pb,ps_ttm,total_mv,circ_mv,turnover_rate",
        ),
    ],
)
def test_tushare_range_endpoints_request_explicit_fields(dataset, expected_fields):
    provider = _historical_provider_module()
    date_value = "2024-01-02"
    plan = _provider_plan(dataset, start_date=date_value, end_date=date_value)
    observed = []

    def fetch(endpoint, parameters):
        observed.append((endpoint, deepcopy(parameters)))
        if dataset == "adj_factor":
            return pd.DataFrame(
                {"ts_code": ["000001.SZ"], "trade_date": ["20240102"], "adj_factor": [1.0]}
            )
        return pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "trade_date": ["20240102"],
                "pe_ttm": [10.0],
                "pb": [1.0],
                "ps_ttm": [2.0],
                "total_mv": [1000.0],
                "circ_mv": [800.0],
                "turnover_rate": [1.0],
            }
        )

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="tushare",
        raw_fetch_fn=fetch,
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, [date_value]
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "FETCHED"
    assert observed[0][1]["fields"] == expected_fields


def test_multicode_concat_propagates_later_sample_truncation_evidence():
    provider = _historical_provider_module()
    date_value = "2024-01-02"
    codes = ["000001.SZ", "600519.SH"]
    plan = _provider_plan(
        "adj_factor",
        start_date=date_value,
        end_date=date_value,
        codes=codes,
    )

    def fetch(endpoint, parameters):
        _ = endpoint
        frame = pd.DataFrame(
            {
                "ts_code": [parameters["ts_code"]],
                "trade_date": ["20240102"],
                "adj_factor": [1.0],
            }
        )
        frame.attrs["sample_truncated"] = parameters["ts_code"] == "600519.SH"
        return frame

    result = provider.HistoricalProviderRouter(
        plan=plan,
        provider_name="tushare",
        raw_fetch_fn=fetch,
        trading_calendar_fn=lambda start_date, end_date: _complete_calendar_frame(
            start_date, end_date, [date_value]
        ),
    ).fetch_chunk(plan["chunks"][0])

    assert result.provider_status == "BLOCKED"
    assert result.failure["category"] == "DQ_FAILED"
    assert len(result.provider_calls) == 2


def _historical_provider_module():
    try:
        return importlib.import_module("stock_selector.providers.historical_provider")
    except ModuleNotFoundError as exc:
        pytest.fail(f"Task 3 historical provider module is missing: {exc}")


def _provider_plan(dataset, *, start_date, end_date, codes=None):
    return _plan(
        run_id=f"goal21-{dataset}-provider-test",
        datasets=[dataset],
        codes=codes if codes is not None else ["000001.SZ"],
        start_date=start_date,
        end_date=end_date,
        date_batch_days=400,
        report_period_months=400,
    )


def _provider_coverage(*, axis, expected_count, covered_count, codes, trade_dates):
    return {
        "axis": axis,
        "complete": expected_count == covered_count,
        "expected_count": expected_count,
        "covered_count": covered_count,
        "missing": [],
        "requested_codes": list(codes),
        "requested_indexes": [],
        "requested_start_date": trade_dates[0],
        "requested_end_date": trade_dates[-1],
        "canonical_trade_dates": list(trade_dates),
        "valid_empty": False,
    }


def _empty_standard_frame(dataset):
    from stock_selector.providers.schema_contract import get_schema_contract

    return pd.DataFrame(columns=get_schema_contract(dataset).columns)


def _result_contract_kwargs(provider, *, status):
    failure = None
    validation = {"passed": True, "errors": []}
    dq = {"passed": True, "level": "STRICT", "blocked_reasons": []}
    coverage = _provider_coverage(
        axis="stock_code_x_open_trade_date",
        expected_count=1,
        covered_count=1,
        codes=["000001.SZ"],
        trade_dates=["2024-01-02"],
    )
    frame = pd.DataFrame(
        {"stock_code": ["000001.SZ"], "trade_date": ["2024-01-02"], "adj_factor": [1.25]}
    )
    valid_empty = False
    dataset = "adj_factor"
    if status in {"BLOCKED", "FAILED"}:
        frame = _empty_standard_frame(dataset)
        validation = {"passed": False, "errors": ["provider blocked"]}
        dq = {"passed": False, "level": "BLOCKED", "blocked_reasons": ["provider blocked"]}
        coverage["complete"] = False
        coverage["covered_count"] = 0
        coverage["missing"] = [{"stock_code": "000001.SZ", "trade_date": "2024-01-02"}]
        category = "RATE_LIMITED" if status == "FAILED" else "DQ_FAILED"
        failure = {
            "category": category,
            "retryable": status == "FAILED",
            "exception_type": "HistoricalProviderError",
            "message": "provider blocked",
        }
    elif status == "VALID_EMPTY":
        dataset = "st_history"
        frame = _empty_standard_frame(dataset)
        valid_empty = True
        coverage["expected_count"] = 0
        coverage["covered_count"] = 0
        coverage["valid_empty"] = True
    return {
        "dataset": dataset,
        "chunk_id": f"{dataset}-contract-test",
        "frame": frame,
        "provider_status": status,
        "provider_name": "fixture",
        "source_keys": (f"archive/{dataset}/2024.parquet",),
        "source_semantics": "HISTORICAL_INTERVAL_SOURCE" if dataset == "st_history" else "HISTORICAL_RANGE_SOURCE",
        "provider_calls": (),
        "actual_schema": tuple(frame.columns),
        "target_schema": tuple(frame.columns),
        "dq": dq,
        "coverage": coverage,
        "canonical_trade_dates": ("2024-01-02",),
        "partition_strategy": "ST_INTERVAL_HISTORY" if dataset == "st_history" else "BY_TRADE_DATE_COLUMN",
        "valid_empty": valid_empty,
        "validation": validation,
        "failure": failure,
    }


def _complete_calendar_frame(start_date, end_date, open_dates):
    current = date.fromisoformat(start_date)
    final = date.fromisoformat(end_date)
    open_set = set(open_dates)
    rows = []
    while current <= final:
        value = current.isoformat()
        rows.append({"cal_date": value.replace("-", ""), "is_open": 1 if value in open_set else 0})
        current += timedelta(days=1)
    return pd.DataFrame(rows)


def _copy_frame(frame):
    copied = frame.copy(deep=True)
    copied.attrs = deepcopy(frame.attrs)
    return copied


def _set_fixture_attrs(
    frame,
    dataset,
    *,
    dates,
    codes=None,
    source_semantics=None,
    interval_row_count=None,
    coverage_complete=True,
):
    codes = list(codes or ["000001.SZ"])
    semantics = source_semantics or {
        "stock_basic": "POINT_IN_TIME_HISTORICAL_SNAPSHOT",
        "daily_price": "HISTORICAL_DAILY_PRICE_SOURCE",
        "adj_factor": "HISTORICAL_RANGE_SOURCE",
        "daily_basic": "HISTORICAL_RANGE_SOURCE",
        "financial": "TRUSTED_STANDARD_FINANCIAL_SOURCE",
        "st_history": "HISTORICAL_INTERVAL_SOURCE",
        "benchmark_price": "HISTORICAL_INDEX_RANGE_SOURCE",
    }[dataset]
    frame.attrs = {
        "source_keys": [f"archive/{dataset}/history.parquet"],
        "source_semantics": semantics,
        "coverage_complete": coverage_complete,
        "sample_truncated": False,
        "requested_codes": codes,
        "coverage_start_date": dates[0],
        "coverage_end_date": dates[-1],
    }
    if dataset == "daily_price":
        frame.attrs.update(
            suspend_d_full_event_coverage=True,
            covered_trade_dates=list(dates),
        )
    if dataset == "stock_basic":
        frame.attrs["snapshot_coverage_complete"] = True
    if dataset == "financial":
        frame.attrs["source_coverage_codes"] = codes
    if dataset == "st_history":
        frame.attrs.update(
            coverage_schema_version="goal20.st_history_coverage.v1",
            interval_row_count=len(frame) if interval_row_count is None else interval_row_count,
        )
    if dataset == "benchmark_price":
        frame.attrs["requested_indexes"] = list(BENCHMARK_INDEXES)
    return frame


def _historical_fixture_frame(dataset, *, dates):
    compact_dates = [value.replace("-", "") for value in dates]
    if dataset == "stock_basic":
        frame = pd.DataFrame(
            {
                "ts_code": ["000001.SZ" for _ in dates],
                "name": ["Alpha" for _ in dates],
                "exchange": ["SZ" for _ in dates],
                "list_date": ["20200101" for _ in dates],
                "delist_date": [None for _ in dates],
                "industry": ["Bank" for _ in dates],
                "market_type": ["MAIN" for _ in dates],
                "is_st": [False for _ in dates],
                "trade_date": list(reversed(compact_dates)),
                "snapshot_date": list(reversed(compact_dates)),
            }
        )
    elif dataset == "daily_price":
        size = len(dates)
        frame = pd.DataFrame(
            {
                "ts_code": ["000001.SZ" for _ in dates],
                "trade_date": list(reversed(compact_dates)),
                "open": [10.0 + index for index in range(size)],
                "high": [10.5 + index for index in range(size)],
                "low": [9.5 + index for index in range(size)],
                "close": [10.1 + index for index in range(size)],
                "pre_close": [10.0 + index for index in range(size)],
                "vol": [100.0 + index for index in range(size)],
                "amount": [1000.0 + index for index in range(size)],
                "pct_chg": [1.0 for _ in dates],
                "is_paused": [False for _ in dates],
                "up_limit": [20.0 + index for index in range(size)],
                "down_limit": [1.0 for _ in dates],
            }
        )
    elif dataset == "adj_factor":
        frame = pd.DataFrame(
            {
                "ts_code": ["000001.SZ" for _ in dates],
                "trade_date": list(reversed(compact_dates)),
                "adj_factor": [1.0 + index / 100 for index in range(len(dates))],
            }
        )
    elif dataset == "daily_basic":
        frame = pd.DataFrame(
            {
                "ts_code": ["000001.SZ" for _ in dates],
                "trade_date": list(reversed(compact_dates)),
                "pe_ttm": [10.0 + index for index in range(len(dates))],
                "pb": [1.0 + index / 10 for index in range(len(dates))],
                "ps_ttm": [2.0 + index / 10 for index in range(len(dates))],
                "total_mv": [1000.0 + index for index in range(len(dates))],
                "circ_mv": [800.0 + index for index in range(len(dates))],
                "turnover_rate": [1.0 + index / 10 for index in range(len(dates))],
            }
        )
    elif dataset == "financial":
        frame = pd.DataFrame(
            {
                "ts_code": ["000001.SZ" for _ in dates],
                "end_date": list(reversed(compact_dates)),
                "ann_date": list(reversed(compact_dates)),
                "revenue_yoy": [10.0 + index for index in range(len(dates))],
                "net_profit_yoy": [8.0 + index for index in range(len(dates))],
                "roe": [0.12 + index / 100 for index in range(len(dates))],
                "gross_margin": [0.30 + index / 100 for index in range(len(dates))],
                "debt_ratio": [0.40 + index / 100 for index in range(len(dates))],
                "operating_cashflow": [100.0 + index for index in range(len(dates))],
            }
        )
    elif dataset == "st_history":
        frame = pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "st_type": ["ST"],
                "start_date": [compact_dates[0]],
                "end_date": [None],
                "source": ["exchange_history"],
            }
        )
    elif dataset == "benchmark_price":
        rows = []
        for trade_date in reversed(compact_dates):
            for offset, index_code in enumerate(reversed(BENCHMARK_INDEXES)):
                rows.append(
                    {
                        "index_code": index_code,
                        "trade_date": trade_date,
                        "open": 100.0 + offset,
                        "high": 102.0 + offset,
                        "low": 99.0 + offset,
                        "close": 101.0 + offset,
                        "pct_chg": ((101.0 + offset) / (100.0 + offset) - 1.0) * 100.0,
                        "pre_close": 100.0 + offset,
                    }
                )
        frame = pd.DataFrame(rows)
    else:
        raise AssertionError(dataset)
    return _set_fixture_attrs(frame, dataset, dates=dates)


def _failure_fetcher(case):
    def fetch(endpoint, parameters):
        _ = endpoint, parameters
        if case == "rate":
            raise RuntimeError('HTTP 429 too many requests token="super-secret"')
        if case == "permission":
            raise PermissionError("HTTP 403 Authorization: Bearer super-secret")
        if case == "transient":
            raise TimeoutError("HTTP 503 service unavailable password=super-secret")
        if case == "schema":
            frame = pd.DataFrame({"ts_code": ["000001.SZ"], "trade_date": ["20240102"]})
            _set_fixture_attrs(frame, "adj_factor", dates=["2024-01-02"])
            return frame
        if case == "empty":
            frame = pd.DataFrame(columns=["ts_code", "trade_date", "adj_factor"])
            _set_fixture_attrs(frame, "adj_factor", dates=["2024-01-02"])
            frame.attrs["empty_after_retries"] = True
            return frame
        if case == "dq":
            frame = _historical_fixture_frame("adj_factor", dates=["2024-01-02"])
            duplicate = pd.concat([frame, frame], ignore_index=True)
            duplicate.attrs = deepcopy(frame.attrs)
            return duplicate
        raise AssertionError(case)

    return fetch


def _single_chunk():
    return _plan(datasets=["daily_price"], codes=["000001.SZ"], date_batch_days=400)["chunks"][0]


def _completed_manifest(chunk):
    return backfill.build_chunk_manifest(
        chunk=chunk,
        state="COMPLETED",
        attempt_count=1,
        **_completed_evidence(chunk),
    )


def _staged_evidence(chunk, row_count=2):
    return {
        "provider_status": {"success": True, "provider": "fixture"},
        "row_count": row_count,
        "actual_schema": ["stock_code", "trade_date"],
        "target_schema": ["stock_code", "trade_date"],
        "dq": {"success": True, "duplicate_count": 0},
        "coverage": {
            "start_date": chunk["start_date"],
            "end_date": chunk["end_date"],
            "complete": True,
        },
        "source_key": f"candidate/source/{chunk['chunk_id']}.parquet",
        "staging_key": f"candidate/staging/{chunk['chunk_id']}.parquet",
        "staging_checksum": "staging-sha",
    }


def _completed_evidence(chunk, row_count=2):
    canonical_key = f"raw/{chunk['dataset']}/{chunk['chunk_id']}.parquet"
    canonical_checksum = "canonical-sha"
    return {
        **_staged_evidence(chunk, row_count=row_count),
        "canonical_key": canonical_key,
        "canonical_checksum": canonical_checksum,
        "validation": {"success": True},
        "write_result": {
            "success": True,
            "object_key": canonical_key,
            "checksum": canonical_checksum,
            "row_count": row_count,
        },
        "read_back_result": {
            "success": True,
            "object_key": canonical_key,
            "checksum": canonical_checksum,
            "row_count": row_count,
        },
    }


def _golden_plan(**overrides):
    values = {
        "run_id": "golden-run",
        "start_date": "2024-01-31",
        "end_date": "2024-01-31",
        "codes": None,
        "universe_frame": pd.DataFrame({"stock_code": ["000001.SZ"]}),
        "universe_key": "raw/universe/golden.parquet",
        "code_batch_size": 10,
        "date_batch_days": 31,
        "report_period_months": 3,
        "datasets": ["daily_price"],
        "generated_at_fn": lambda: "2026-07-12T00:00:00Z",
    }
    values.update(overrides)
    return build_history_backfill_plan(**values)


def _assert_error_code(expected_code, action):
    error_type = getattr(backfill, "BackfillPlanningError", None)
    assert error_type is not None, "BackfillPlanningError must be defined"
    with pytest.raises(error_type) as exc_info:
        action()
    assert exc_info.value.code == expected_code
    assert str(exc_info.value)


def _expected_day_windows(start_date, end_date, batch_days):
    current = date.fromisoformat(start_date)
    final = date.fromisoformat(end_date)
    windows = []
    while current <= final:
        window_end = min(current + timedelta(days=batch_days - 1), final)
        windows.append((current.isoformat(), window_end.isoformat()))
        current = window_end + timedelta(days=1)
    return windows


def _plan(**overrides):
    values = {
        "run_id": "goal21-plan-test",
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "codes": CODES,
        "universe_frame": None,
        "universe_key": None,
        "code_batch_size": 2,
        "date_batch_days": 90,
        "report_period_months": 3,
        "datasets": None,
        "generated_at_fn": lambda: "2026-07-11T00:00:00Z",
    }
    values.update(overrides)
    return build_history_backfill_plan(**values)


def _assert_contiguous(chunks, start_field, end_field, expected_start, expected_end):
    windows = []
    for chunk in chunks:
        window = (chunk[start_field], chunk[end_field])
        if window not in windows:
            windows.append(window)
    assert windows[0][0] == expected_start
    assert windows[-1][1] == expected_end
    for previous, current in zip(windows, windows[1:], strict=False):
        assert date.fromisoformat(current[0]) == date.fromisoformat(previous[1]) + timedelta(days=1)

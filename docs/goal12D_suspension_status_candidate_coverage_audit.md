# Goal 12D Suspension Status Candidate Coverage Audit

Goal 12D adds a candidate/staging-only suspension status audit for Tushare. It reads existing smoke and candidate artifacts, builds `suspension_status_candidate`, and writes a coverage audit report.

It does not call Tushare, does not write standard `suspension_status`, does not write standard `daily_price`, and does not feed clean, factor, selection, or backtest paths.

## CLI

```powershell
python -m stock_selector.cli build-tushare-suspension-status-candidate `
  --trade-date 2024-06-19 `
  --sample-limit 5
```

The CLI reads only existing inputs:

```text
smoke/tushare/daily_price_candidate_dry_run/trade_date=YYYY-MM-DD/part.parquet
smoke/tushare/daily_price_candidate_dry_run/trade_date=YYYY-MM-DD/report.json
smoke/tushare/trade_cal/trade_date=YYYY-MM-DD/part.parquet
smoke/tushare/suspend_d/trade_date=YYYY-MM-DD/part.parquet
```

If an input is missing, the report returns `BLOCKED_BY_MISSING_INPUT`, lists the missing `object_key`, and leaves `ready_for_dq3_promotion=false`.

## Output

Goal 12D writes only candidate/diagnostic objects:

```text
candidate/tushare/suspension_status_candidate/trade_date=YYYY-MM-DD/part.parquet
candidate/tushare/suspension_status_coverage_audit/trade_date=YYYY-MM-DD/report.json
```

These paths are not standard `suspension_status` or `daily_price` paths. They are not read by `clean_daily_snapshot`, `factor_input_table`, `factor_daily`, `selection_result`, or `run-backtest`.

## Candidate Schema

`suspension_status_candidate` contains:

```text
ts_code
trade_date
provider
pause_status
is_paused_candidate
pause_evidence
event_match
event_source_object_key
calendar_source_object_key
candidate_source_object_key
coverage_status
coverage_block_reason
dq_level
is_standard
is_promotable
generated_at
```

It may also carry source diagnostic columns such as `open`, `close`, `volume`, `amount`, `suspend_type`, and `suspend_timing`. Those columns remain diagnostic evidence and are not used to infer suspension.

`is_standard` is always `false`. `is_paused_candidate` is not the standard `is_paused` field.

## Pause Semantics

`suspend_d` is an event source.

A matching `(ts_code, trade_date)` can generate:

```text
pause_status = true_candidate
is_paused_candidate = true
pause_evidence = suspend_d_match
```

A miss without full event coverage can only generate:

```text
pause_status = unknown
is_paused_candidate = null
pause_evidence = unresolved_no_event_match
```

If the event source is sample-truncated, a miss remains unknown:

```text
pause_status = unknown
is_paused_candidate = null
pause_evidence = blocked_by_sample_truncated_suspend_d
```

Only after full event coverage is proven can a miss become:

```text
pause_status = false_candidate
is_paused_candidate = false
pause_evidence = full_event_coverage_no_match
```

Even then it is still candidate false, not standard `is_paused=false`.

## False Candidate Conditions

`false_candidate` requires all of the following:

- `trade_cal` confirms the target date is an open trading day
- coverage universe is explicit
- `suspend_d` event source coverage is proven complete
- `suspend_d` input is not sample-truncated
- `suspend_d` query range is the target trading date
- `suspend_d` schema is valid
- no price, volume, amount, missing-row, or unchanged-price shortcut is used
- output remains candidate-only

If any condition is missing, suspend misses stay `unknown`.

## Coverage Audit Report

`report.json` includes:

- `input_object_keys`
- `output_object_keys`
- `input_row_counts`
- `schema_check`
- `coverage_universe`
- `suspend_d_event_coverage`
- `pause_status_counts`
- `evidence_counts`
- `blocked_reasons`
- `readiness`
- `safety`
- `inference_guards`

`coverage_universe` is based on `daily_price_candidate_dry_run`. It is an explicit candidate universe, not a full-market universe.

`suspend_d_event_coverage.coverage_status` can be:

```text
FULL_EVENT_COVERAGE
SAMPLE_TRUNCATED
COVERAGE_UNKNOWN
MISSING_INPUT
SCHEMA_MISMATCH
```

If API total rows are known and `rows_written < api_total_rows`, the report marks:

```text
is_sample_truncated = true
full_coverage_proven = false
coverage_status = SAMPLE_TRUNCATED
```

If full coverage cannot be proven, the report marks:

```text
full_coverage_proven = false
coverage_status = COVERAGE_UNKNOWN
```

## Current 2024-06-19 Smoke State

The current Goal 12B `suspend_d` smoke was written with `sample-limit=5`. The real API returned 20 rows, but only 5 sample rows were written. Therefore the current `smoke/tushare/suspend_d/.../part.parquet` cannot prove full event coverage.

Under that state, Goal 12D should normally report:

```text
ready_for_dq3_promotion = false
coverage_status = SAMPLE_TRUNCATED
false_candidate = 0
```

This is a correct blocked state, not a failure. It prevents turning absent sample events into `is_paused=false`.

## Candidate vs Standard Suspension Status

`suspension_status_candidate` is an audit artifact. It records event matches, unresolved misses, coverage status, and safety gates.

Standard `suspension_status` would require validated full event coverage, promotion validator evidence, standard schema ownership, and DQ3 or DQ4 readiness. Goal 12D does not create that table.

## Safety Flags

The report keeps these boundaries explicit:

```text
standard_daily_price_written = false
standard_suspension_status_written = false
real_raw_mainline_written = false
cleaning_mainline_entered = false
factor_mainline_entered = false
selection_mainline_entered = false
backtest_mainline_entered = false
spring_api_changed = false
is_paused_fabricated = false
suspend_miss_inferred_as_false_without_coverage = false
```

The inference guards are also explicit:

```text
volume_used_as_pause = false
amount_used_as_pause = false
missing_daily_used_as_pause = false
unchanged_price_used_as_pause = false
suspend_d_miss_used_as_false_without_coverage = false
```

## Before DQ3

Later promotion requires:

- full coverage smoke or a complete event-source landing
- staging audit
- promotion validator
- small-range standard write
- mainline isolation tests

Until those gates are complete, real data remains outside standard `daily_price`, selection, and backtest mainline.

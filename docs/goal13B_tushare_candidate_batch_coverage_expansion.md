# Goal 13B Tushare Candidate Batch Coverage Expansion

Goal 13B expands the Goal 13 real-provider candidate batch from a small diagnostic sample into a coverage-expansion and fetch-semantics audit.

It still writes only `candidate/tushare/...` artifacts. It does not write standard `daily_price`, does not write standard `suspension_status`, does not write the real raw mainline, and does not feed `clean_daily_snapshot`, factor, selection, or backtest paths.

## Purpose

Goal 13 exposed incomplete critical coverage:

```text
stk_limit_coverage = 2/4
adj_factor_coverage = 2/4
pause_status unknown = 4
```

That was not enough evidence to tell whether the gap came from provider absence, date alignment, code alignment, fetch strategy, schema mismatch, or sample truncation. Goal 13B makes those causes explicit.

## CLI

```powershell
python -m stock_selector.cli build-tushare-candidate-staging-batch `
  --start-date 2024-06-01 `
  --end-date 2024-07-31 `
  --codes 000001.SZ,600519.SH,300750.SZ,000333.SZ,601318.SH,600036.SH,000858.SZ,601899.SH,600900.SH,002415.SZ `
  --max-trade-days 20 `
  --sleep-seconds 12 `
  --coverage-expansion `
  --fetch-semantics-audit
```

Additional controls:

```text
--max-codes
--max-trade-days
--batch-id
--no-provider-call
--reuse-existing-staging
--fail-on-incomplete-critical-coverage
```

`--no-provider-call --reuse-existing-staging` rebuilds reports from existing candidate staging Parquet and does not construct the Tushare provider. Without `--reuse-existing-staging`, `--no-provider-call` returns a blocked report.

## Fetch Semantics Matrix

`fetch_semantics_report/report.json` records one matrix entry per interface:

| Interface | Goal 13B strategy | Expected granularity | Loop |
| --- | --- | --- | --- |
| `daily` | `by_code_range` | per code-date | per code |
| `stk_limit` | `by_trade_date` in coverage expansion | per code-date | per selected trade day |
| `adj_factor` | `by_code_range` | per code-date | per code |
| `daily_basic` | `by_code_range` | per code-date | per code |
| `trade_cal` | `by_date_range` | calendar rows | date range |
| `suspend_d` | `by_date_range` | event rows only | date range |

Each entry includes sanitized actual call parameters, call row counts, schema status, row-count alignment, whether a sample limit may have truncated data, and whether the fetch strategy may explain coverage gaps. Tokens and `.env` values must not appear in this report.

## Coverage Gap Report

`coverage_gap_report/report.json` contains:

- `expected_code_trade_date_count`
- `interface_gap_summary`
- `missing_key_examples`
- `duplicate_key_examples`
- `fetch_strategy_suspicions`
- `recommended_next_actions`

Missing keys include `ts_code`, `trade_date`, `missing_interface`, `missing_fields`, and `reason_code`.

Reason codes include:

```text
MISSING_PROVIDER_ROW
DATE_ALIGNMENT_GAP
CODE_ALIGNMENT_GAP
SAMPLE_TRUNCATED
SCHEMA_MISMATCH
PROVIDER_BLOCKED
RATE_LIMITED
UNKNOWN
```

If there is no strategy suspicion, `fetch_strategy_suspicions` is an empty list.

## Sample-Limit Policy

Goal 13B does not allow sample-limit truncation of critical staging data.

Scale must be controlled by:

```text
max_codes
max_trade_days
start_date
end_date
```

If a critical interface is marked sample-truncated, reports set:

```text
sample_truncated = true
full_coverage_proven = false
blocked_reason = SAMPLE_TRUNCATED
```

The CLI may print compact summaries, but staging Parquet for the selected universe must keep the complete selected rows.

## Coverage Versus DQ3

Complete price coverage is not the same as DQ3 readiness.

`daily`, `stk_limit`, `adj_factor`, and `daily_basic` can all reach 100% for the selected universe, while DQ3 remains blocked if `is_paused` is unresolved or `suspend_d` full event coverage is not proven.

In that improved state, the DQ3 audit reports:

```text
ready_for_promotion_validator = false
ready_for_dq3_promotion = false
status = BLOCKED_BY_UNRESOLVED_IS_PAUSED
blocked_reasons = [
  UNRESOLVED_IS_PAUSED,
  INCOMPLETE_OR_UNKNOWN_SUSPEND_D_COVERAGE
]
```

Goal 13B still does not run the standard `daily_price` validator and does not write standard data.

## Output

All output remains candidate-only:

```text
candidate/tushare/batch_manifest/batch_id=<batch_id>/manifest.json
candidate/tushare/daily_staging/batch_id=<batch_id>/trade_date=<YYYY-MM-DD>/part.parquet
candidate/tushare/stk_limit_staging/batch_id=<batch_id>/trade_date=<YYYY-MM-DD>/part.parquet
candidate/tushare/adj_factor_staging/batch_id=<batch_id>/trade_date=<YYYY-MM-DD>/part.parquet
candidate/tushare/daily_basic_staging/batch_id=<batch_id>/trade_date=<YYYY-MM-DD>/part.parquet
candidate/tushare/trade_cal_staging/batch_id=<batch_id>/part.parquet
candidate/tushare/suspend_d_staging/batch_id=<batch_id>/part.parquet
candidate/tushare/daily_price_candidate_batch/batch_id=<batch_id>/part.parquet
candidate/tushare/suspension_status_candidate_batch/batch_id=<batch_id>/part.parquet
candidate/tushare/provider_coverage_report/batch_id=<batch_id>/report.json
candidate/tushare/fetch_semantics_report/batch_id=<batch_id>/report.json
candidate/tushare/coverage_gap_report/batch_id=<batch_id>/report.json
candidate/tushare/dq3_readiness_audit/batch_id=<batch_id>/report.json
```

These objects are not read by clean, factor, selection, or backtest commands.

## Before Goal 14

Before any later standard-layer promotion goal, the project still needs:

- complete critical price coverage on a selected real-provider universe
- proven `suspend_d` full event coverage or another trusted pause source
- no duplicate `(ts_code, trade_date)` keys
- valid schemas for all required interfaces
- explicit standard validator coverage
- an explicit later goal that authorizes any standard `daily_price` or `suspension_status` write

Until then, real Tushare data remains outside formal selection and backtest mainline.

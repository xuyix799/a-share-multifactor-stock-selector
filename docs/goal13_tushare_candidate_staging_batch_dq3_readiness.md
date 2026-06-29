# Goal 13 Tushare Candidate Staging Batch DQ3 Readiness

Goal 13 builds a small-range Tushare real-provider candidate/staging batch and DQ3 readiness audit.

It may call Tushare when explicitly enabled, but it still writes only candidate artifacts. It does not write standard `daily_price`, does not write standard `suspension_status`, does not write the real raw mainline, and does not feed clean, factor, selection, or backtest paths.

## CLI

```powershell
python -m stock_selector.cli build-tushare-candidate-staging-batch `
  --start-date 2024-06-01 `
  --end-date 2024-07-31 `
  --codes 000001.SZ,600519.SH,300750.SZ,000333.SZ,601318.SH,600036.SH,000858.SZ,601899.SH,600900.SH,002415.SZ `
  --sleep-seconds 12
```

Optional controls:

```text
--batch-id
--max-codes
--max-trade-days
--sleep-seconds
--no-provider-call
```

Tushare remains opt-in. If `STOCK_TUSHARE_ENABLED` is not true, the CLI returns `BLOCKED_BY_PROVIDER_DISABLED`. If `TUSHARE_TOKEN` is missing, it returns `BLOCKED_BY_MISSING_TUSHARE_TOKEN`. These blocked states do not mock success.

## Provider Interfaces

Goal 13 requests:

```text
daily
stk_limit
adj_factor
daily_basic
trade_cal
suspend_d
```

`trade_cal` defines the open trading dates in the requested range. It does not define per-stock suspension. `suspend_d` is the only pause event-source candidate in this goal.

## Output

All output stays under `candidate/tushare/...`:

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
candidate/tushare/dq3_readiness_audit/batch_id=<batch_id>/report.json
```

These paths are not read by `clean_daily_snapshot`, `factor_input_table`, `factor_daily`, `selection_result`, or `run-backtest`.

## Manifest

`manifest.json` records:

- `batch_id`
- provider and goal
- start/end dates
- requested codes
- generated time
- CLI command when available
- interfaces requested, succeeded, and failed
- output object keys
- row counts
- schema versions
- `dq_level=DQ1`
- `is_standard=false`
- `is_promotable=false`

The manifest must not contain `.env` values or Tushare token text.

## Candidate Schemas

`daily_price_candidate_batch` includes:

```text
ts_code
trade_date
open
high
low
close
pre_close
volume
amount
limit_up
limit_down
adj_factor
pe_ttm
pb
ps_ttm
total_mv
circ_mv
turnover_rate
trading_day_confirmed
pause_status
is_paused_candidate
pause_evidence
provider
batch_id
dq_level
is_standard
is_promotable
generated_at
```

It deliberately does not contain standard `is_paused`.

`suspension_status_candidate_batch` includes:

```text
ts_code
trade_date
provider
batch_id
pause_status
is_paused_candidate
pause_evidence
event_match
event_source_object_key
calendar_source_object_key
coverage_status
coverage_block_reason
dq_level
is_standard
is_promotable
generated_at
```

## Pause Semantics

`suspend_d` has event-source semantics:

- A matched `(ts_code, trade_date)` can produce `pause_status=true_candidate` and `is_paused_candidate=true`.
- A miss without proven full event coverage remains `pause_status=unknown` and `is_paused_candidate=null`.
- A miss is not `is_paused=false`.

Goal 13 does not infer pause from:

- `volume=0`
- `amount=0`
- missing `daily` row
- unchanged prices
- `trade_cal`

`is_paused_candidate` is candidate evidence only. It is not standard `is_paused`.

## Coverage Report

`provider_coverage_report/report.json` includes:

- requested code count
- open trade day count
- `expected_code_trade_date_count`
- per-interface row counts
- `daily_coverage`, `stk_limit_coverage`, `adj_factor_coverage`, and `daily_basic_coverage` with numerator, denominator, missing rows, and coverage rate
- `trade_cal_coverage`
- `suspend_d_event_coverage`
- duplicate key checks
- duplicate key counts
- missing key counts
- schema checks
- date range check
- `suspend_d` matched events
- `suspend_d` events outside the requested universe
- provider errors
- rate-limit or blocked errors
- safety flags and inference guards

Provider failure blocks the batch with `BLOCKED_BY_PROVIDER_ERROR` and does not write candidate artifacts.

## DQ3 Readiness

Goal 13 normally reports:

```text
ready_for_dq3_promotion = false
status = BLOCKED_BY_UNRESOLVED_IS_PAUSED
```

Even if later evidence proves full event coverage and complete fields, Goal 13 can at most reach:

```text
READY_FOR_PROMOTION_VALIDATOR
```

It still cannot write standard `daily_price` in this goal.

## Before DQ3

Later promotion requires:

- complete staging
- coverage audit
- duplicate check
- missing check
- promotion validator
- explicit boolean standard suspension status
- small-range standard write in a later goal
- mock mainline protection

Until those gates are complete, real Tushare data remains outside standard `daily_price`, selection, and backtest mainline.

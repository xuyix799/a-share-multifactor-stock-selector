# Goal 13C Suspend_d Full Coverage Preflight

Goal 13C adds a promotion preflight for Tushare candidate/staging batches. It audits whether `suspend_d` misses can safely become `false_candidate` rows, then reports whether the candidate batch is ready for a later Goal 14 promotion validator.

Goal 13C is still candidate-only. It does not write standard `daily_price`, does not write standard `suspension_status`, does not write the real raw mainline, and does not feed `clean_daily_snapshot`, `factor_input_table`, `factor_daily`, `selection_result`, or real backtest paths.

## CLI

```powershell
python -m stock_selector.cli build-tushare-candidate-staging-batch `
  --start-date 2024-06-01 `
  --end-date 2024-07-31 `
  --codes 000001.SZ,600519.SH,300750.SZ,000333.SZ,601318.SH,600036.SH,000858.SZ,601899.SH,600900.SH,002415.SZ `
  --max-trade-days 20 `
  --sleep-seconds 12 `
  --coverage-expansion `
  --fetch-semantics-audit `
  --goal13c-preflight
```

Default execution does not call live Tushare unless the provider is explicitly enabled and a valid token is configured. `--no-provider-call --reuse-existing-staging --goal13c-preflight` can rebuild reports from existing candidate staging, but reused staging without auditable `suspend_d` call scope keeps misses unresolved.

## Output

Goal 13C reuses the Goal 13/13B candidate paths and adds two JSON reports:

```text
candidate/tushare/suspend_d_full_coverage_report/batch_id=<batch_id>/report.json
candidate/tushare/promotion_preflight_report/batch_id=<batch_id>/report.json
```

It also writes the existing candidate-only artifacts:

```text
candidate/tushare/daily_price_candidate_batch/batch_id=<batch_id>/part.parquet
candidate/tushare/suspension_status_candidate_batch/batch_id=<batch_id>/part.parquet
candidate/tushare/provider_coverage_report/batch_id=<batch_id>/report.json
candidate/tushare/fetch_semantics_report/batch_id=<batch_id>/report.json
candidate/tushare/coverage_gap_report/batch_id=<batch_id>/report.json
candidate/tushare/dq3_readiness_audit/batch_id=<batch_id>/report.json
```

None of these paths are standard `daily_price`, standard `suspension_status`, clean, factor, selection, or backtest paths.

## Suspend_d Full Coverage Semantics

A `suspend_d` event row can produce `true_candidate`. A `suspend_d` miss can produce `false_candidate` only when the date-level audit confirms a full event set for the target universe.

Goal 13C recognizes these query scope modes:

| Mode | Meaning |
| --- | --- |
| `DATE_FULL_MARKET_EVENT_SET` | The batch queried the full-market `suspend_d` event set for a trade date. |
| `CODE_DATE_EXPLICIT_FULL_UNIVERSE` | The batch explicitly queried every target code/date pair. |
| `PARTIAL` | The query covered only part of the target universe. |
| `UNKNOWN` | The query scope cannot prove full coverage. |

`false_candidate` is allowed only when all of these are true for the date:

- The date is an open trade date in the batch.
- The query scope is `DATE_FULL_MARKET_EVENT_SET` or `CODE_DATE_EXPLICIT_FULL_UNIVERSE`.
- Provider fetch status is successful.
- There is no `PROVIDER_EMPTY_AFTER_RETRIES` or `PROVIDER_FETCH_INCOMPLETE`.
- Required `suspend_d` schema fields are present, unless the provider returned a valid empty event set.
- There is no pagination, sample truncation, or unknown query-scope risk.
- The target `ts_code + trade_date` does not appear in `suspend_d`.

The generated source is recorded as `SUSPEND_D_FULL_COVERAGE_MISS_AS_FALSE_CANDIDATE`. It remains candidate evidence, not standard truth.

If the scope is unknown, partial, failed, truncated, schema-incomplete, or provider-incomplete, misses stay `unknown`.

## Forbidden Inference

Goal 13C never infers pause state from:

- `volume = 0`
- `amount = 0`
- missing `daily` rows
- unchanged prices
- `trade_cal` alone

Those guards are recorded in report `safety` and `inference_guards` fields.

## Promotion Preflight

`promotion_preflight_report` answers whether the current candidate batch can enter a later promotion validator. It does not grant standard write permission.

The only ready status in Goal 13C is:

```text
READY_FOR_PROMOTION_VALIDATOR
```

The report remains blocked when any critical requirement is incomplete, including:

```text
INCOMPLETE_DAILY_COVERAGE
INCOMPLETE_LIMIT_PRICE_COVERAGE
INCOMPLETE_ADJ_FACTOR_COVERAGE
INCOMPLETE_DAILY_BASIC_COVERAGE
PROVIDER_FETCH_INCOMPLETE
PROVIDER_EMPTY_AFTER_RETRIES
SUSPEND_D_FULL_COVERAGE_NOT_CONFIRMED
UNRESOLVED_IS_PAUSED
CANDIDATE_SCHEMA_INCOMPATIBLE_WITH_DAILY_PRICE_CONTRACT
```

Even when status is `READY_FOR_PROMOTION_VALIDATOR`, these fields remain false:

```text
standard_daily_price_write_performed = false
standard_suspension_status_write_performed = false
real_backtest_performed = false
ready_for_standard_write = false
ready_for_real_backtest = false
production_ready = false
```

## Boundary With Goal 14

Goal 13C stops before standard-layer promotion. The only allowed next step is Goal 14: a small-range standard `daily_price` promotion validator.

Goal 14 would need its own explicit authorization before any standard `daily_price` or standard `suspension_status` write. Real selection and real backtest remain out of scope until later goals prove the standard layer.

## Current System Meaning

The system has a verified Docker mock/offline closed loop and Tushare candidate/staging audits through Goal 13C. It still cannot be described as a completed real-market stock-selection or real-backtest system.

Spring API query endpoints can return mock/offline selection and backtest summaries. That proves the local mock/offline path, not the real Tushare mainline.

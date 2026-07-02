# Goal 14 Daily Price Promotion Validator

Goal 14 adds the first controlled standard `daily_price` promotion validator for Tushare candidate/staging output. It is intentionally small-range and dry-run by default.

Goal 14 does not call live Tushare by itself. It reads existing Goal 13C artifacts and decides whether the candidate batch can satisfy the existing standard `daily_price` validator. It does not lower the `daily_price` contract.

## Required Inputs

For one `batch_id`, Goal 14 reads:

```text
candidate/tushare/promotion_preflight_report/batch_id=<batch_id>/report.json
candidate/tushare/daily_price_candidate_batch/batch_id=<batch_id>/part.parquet
candidate/tushare/suspension_status_candidate_batch/batch_id=<batch_id>/part.parquet
```

The preflight report must have:

```text
status = READY_FOR_PROMOTION_VALIDATOR
standard_daily_price_write_performed = false
standard_suspension_status_write_performed = false
real_backtest_performed = false
```

## CLI

Default dry-run:

```powershell
python -m stock_selector.cli build-tushare-daily-price-promotion-validator `
  --batch-id <batch_id>
```

Optional guard overrides:

```powershell
python -m stock_selector.cli build-tushare-daily-price-promotion-validator `
  --batch-id <batch_id> `
  --goal14-max-codes 5 `
  --goal14-max-trade-days 10 `
  --goal14-max-rows 50
```

Actual standard write is off by default. Goal 15 requires:

```powershell
python -m stock_selector.cli build-tushare-daily-price-promotion-validator `
  --batch-id <batch_id> `
  --apply
```

The apply flag is intentionally explicit. Without it, the command writes only
validator and dry-run reports.

## Outputs

Default Goal 14 writes:

```text
candidate/tushare/daily_price_promotion_validator_report/batch_id=<batch_id>/report.json
candidate/tushare/standard_daily_price_promotion_dry_run_report/batch_id=<batch_id>/report.json
```

Report schemas:

```text
goal14.daily_price_promotion_validator_report.v1
goal14.standard_daily_price_promotion_dry_run_report.v1
```

The dry-run report records `would_insert_rows`, `would_update_rows`, `would_skip_rows`, an idempotency key, `target_table=daily_price`, and `standard_write_performed=false`.

Goal 15 replaces the old write path with explicit `--apply` semantics. The
apply contract, upsert behavior, and read-back verification are documented in
`docs/goal15_tushare_daily_price_apply.md`.

When `--apply` is explicitly provided and the validator passes, the command also writes:

```text
candidate/tushare/standard_daily_price_promotion_apply_report/batch_id=<batch_id>/report.json
```

with schema:

```text
goal15.standard_daily_price_promotion_apply_report.v1
```

The apply report records target table, upsert counts, per-date write object
keys, idempotency key, read-back verification, and the same downstream
firewalls. The default validation path does not generate this apply report
because no standard write is attempted.

## Validator Gates

The validator blocks unless all of these are true:

- Goal 13C preflight exists and is `READY_FOR_PROMOTION_VALIDATOR`.
- Price coverage is complete for `daily`, `stk_limit`, `adj_factor`, and `daily_basic`.
- No provider empty-retry or incomplete-fetch reason is present.
- Candidate row count matches expected code-date pairs.
- `(ts_code, trade_date)` is unique.
- Every candidate date is an open trade date confirmed by the candidate batch.
- Required standard fields are present: `ts_code`, `trade_date`, OHLC, `pre_close`, `volume` or `vol`, `amount`, `adj_factor`, `limit_up`, `limit_down`, and an explicit pause candidate.
- OHLC, volume, amount, positive `adj_factor`, and limit fields satisfy the existing `daily_price` validator and Goal 14 source checks.
- `limit_up` / `limit_down`, `pre_close`, and `is_paused` have auditable source object keys or resolution sources.
- The batch stays within the small-range guard: default `max_codes <= 5`, `max_trade_days <= 10`, `max_rows <= 50`.

Goal 14 never infers `is_paused` from `volume=0`, `amount=0`, missing daily rows, unchanged prices, or a `suspend_d` miss without Goal 13C full coverage evidence.

## Blocked Reasons

Goal 14 can emit these blocked reason codes:

```text
GOAL13C_PREFLIGHT_REPORT_MISSING
GOAL13C_PREFLIGHT_NOT_READY
INCOMPLETE_DAILY_COVERAGE
INCOMPLETE_LIMIT_PRICE_COVERAGE
INCOMPLETE_ADJ_FACTOR_COVERAGE
INCOMPLETE_DAILY_BASIC_COVERAGE
UNRESOLVED_IS_PAUSED
PROVIDER_EMPTY_AFTER_RETRIES
PROVIDER_FETCH_INCOMPLETE
CANDIDATE_BATCH_MISSING
CANDIDATE_ROW_COUNT_MISMATCH
CANDIDATE_DUPLICATE_CODE_DATE
CANDIDATE_NON_OPEN_TRADE_DATE
CANDIDATE_SCHEMA_INCOMPATIBLE_WITH_DAILY_PRICE_CONTRACT
MISSING_REQUIRED_DAILY_PRICE_FIELD
INVALID_OHLC
INVALID_VOLUME_OR_AMOUNT
LIMIT_PRICE_SOURCE_NOT_AUDITABLE
PRE_CLOSE_SOURCE_NOT_AUDITABLE
IS_PAUSED_SOURCE_NOT_AUDITABLE
BATCH_TOO_LARGE_FOR_GOAL14_SMALL_RANGE_VALIDATOR
STANDARD_WRITE_REQUIRES_EXPLICIT_EXECUTE_FLAG
```

The following boundaries remain hardcoded as not allowed in Goal 14:

```text
STANDARD_SUSPENSION_STATUS_WRITE_NOT_ALLOWED_IN_GOAL14
CLEAN_FACTOR_SELECTION_BACKTEST_NOT_ALLOWED_IN_GOAL14
```

## Current System Meaning

After Goal 14, the accurate system status is:

- Local Docker + mock/offline mainline is complete.
- Real Tushare candidate/staging coverage expansion is complete.
- `suspend_d` full coverage audit and promotion preflight are complete.
- Small-range standard `daily_price` promotion validator and default dry-run report are available.
- Real standard `daily_price` write is possible only with the Goal 15 `--apply` flag and only after validator pass.
- Explicit standard `daily_price` apply emits a separate apply report and read-back verification.
- Real standard `suspension_status` write has not started.
- Real clean/factor/selection/backtest has not started.

This is still not a formal real-market stock-selection or real-backtest system.

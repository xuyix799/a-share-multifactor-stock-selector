# Goal 15 Tushare Daily Price Apply Mode

Goal 15 adds the first explicit apply mode for promoting validated Tushare
`daily_price_candidate_batch` rows into the canonical `daily_price` layer.
It reuses the Goal 14 validator and keeps dry-run as the default behavior.

## Inputs

Goal 15 reads the same Goal 13C artifacts as Goal 14:

```text
candidate/tushare/promotion_preflight_report/batch_id=<batch_id>/report.json
candidate/tushare/daily_price_candidate_batch/batch_id=<batch_id>/part.parquet
candidate/tushare/suspension_status_candidate_batch/batch_id=<batch_id>/part.parquet
```

Optional scope filters can restrict the apply set:

```powershell
docker compose run --rm stock-python python -m stock_selector.cli build-tushare-daily-price-promotion-validator `
  --batch-id <batch_id> `
  --apply `
  --codes 000001.SZ,600519.SH `
  --start-date 2024-06-03 `
  --end-date 2024-06-04
```

Without `--apply`, the command remains dry-run only and writes no canonical
`daily_price` rows.

## Apply Contract

`--apply` is required for any canonical write. When provided, the command:

1. Filters the candidate batch by the optional code list and trade-date range.
2. Runs the existing Goal 14 validator on the scoped candidate rows.
3. Builds standard `daily_price` rows only after validation passes.
4. Reads the existing canonical `daily_price` partition for each trade date.
5. Upserts by `(stock_code, trade_date)` with candidate rows winning conflicts.
6. Validates the merged canonical partition before writing.
7. Writes each partition atomically through the existing storage writer.
8. Reads the written partitions back and verifies the result.

The conflict policy is deterministic:

```text
upsert_candidate_wins_by_stock_code_trade_date
```

Running the same apply twice must not create duplicate canonical keys. A
second identical run reports the rows as unchanged.

## Read-Back Verification

After write, Goal 15 verifies:

- expected promoted row count equals actual promoted row count;
- canonical `(stock_code, trade_date)` keys are not duplicated;
- promoted trade-date range matches the scoped input range;
- required `daily_price` fields are present and non-null;
- OHLC and `pre_close` remain valid;
- `is_paused`, `limit_up`, and `limit_down` keep canonical semantics.

Read-back failures are reported with explicit blocked reason codes such as:

```text
READ_BACK_ROW_COUNT_MISMATCH
READ_BACK_PROMOTED_ROW_COUNT_MISMATCH
READ_BACK_DUPLICATE_CANONICAL_KEYS
READ_BACK_TRADE_DATE_RANGE_MISMATCH
READ_BACK_SCHEMA_INVALID
READ_BACK_CANONICAL_SEMANTICS_INVALID
```

## Outputs

Dry-run mode writes:

```text
candidate/tushare/daily_price_promotion_validator_report/batch_id=<batch_id>/report.json
candidate/tushare/standard_daily_price_promotion_dry_run_report/batch_id=<batch_id>/report.json
```

Apply mode also writes:

```text
candidate/tushare/standard_daily_price_promotion_apply_report/batch_id=<batch_id>/report.json
raw/daily_price/trade_date=YYYY-MM-DD/part.parquet
```

The apply report uses:

```text
goal15.standard_daily_price_promotion_apply_report.v1
```

## Boundaries

Goal 15 remains small-scope. It does not perform a 5-10 year backfill, does
not write standard `suspension_status`, and does not start
`clean_daily_snapshot`, `factor_input_table`, `factor_daily`,
`selection_result`, or backtest workflows.

Live Tushare calls remain a manual prerequisite from earlier candidate/staging
goals. Goal 15 reads existing candidate artifacts and does not call Tushare by
itself.

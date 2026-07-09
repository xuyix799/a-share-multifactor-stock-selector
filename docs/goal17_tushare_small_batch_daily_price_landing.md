# Goal 17 Tushare Small-Batch Daily Price Landing

Goal 17 adds a repeatable small-batch landing workflow for Tushare
`daily_price`. It orchestrates existing Goal 13C candidate/preflight artifacts
and the Goal 14/15 promotion validator/apply path. It does not add a new
canonical write rule.

## Command

Default mode is dry-run and provider-disabled:

```powershell
docker compose run --rm stock-python python -m stock_selector.cli run-tushare-daily-price-small-batch `
  --batch-id <batch_id> `
  --codes 000001.SZ,600519.SH `
  --start-date 2024-06-03 `
  --end-date 2024-06-04
```

The command reads existing Goal 13C artifacts:

```text
candidate/tushare/promotion_preflight_report/batch_id=<batch_id>/report.json
candidate/tushare/daily_price_candidate_batch/batch_id=<batch_id>/part.parquet
candidate/tushare/suspension_status_candidate_batch/batch_id=<batch_id>/part.parquet
```

To explicitly reuse existing staging without constructing a Tushare provider:

```powershell
docker compose run --rm stock-python python -m stock_selector.cli run-tushare-daily-price-small-batch `
  --batch-id <batch_id> `
  --codes 000001.SZ,600519.SH `
  --start-date 2024-06-03 `
  --end-date 2024-06-04 `
  --no-provider-call `
  --reuse-existing-staging
```

Provider calls are opt-in only:

```powershell
docker compose run --rm stock-python python -m stock_selector.cli run-tushare-daily-price-small-batch `
  --batch-id <batch_id> `
  --codes 000001.SZ,600519.SH `
  --start-date 2024-06-03 `
  --end-date 2024-06-04 `
  --provider-call
```

If the provider is disabled or the token is missing, the workflow writes a
blocked run report and does not write canonical data.

## Apply Contract

Canonical `raw/daily_price/...` writes still require explicit `--apply`:

```powershell
docker compose run --rm stock-python python -m stock_selector.cli run-tushare-daily-price-small-batch `
  --batch-id <batch_id> `
  --codes 000001.SZ,600519.SH `
  --start-date 2024-06-03 `
  --end-date 2024-06-04 `
  --apply
```

Apply mode routes through the Goal 15 validator/apply implementation. The
workflow keeps:

- idempotent upsert by `(stock_code, trade_date)`;
- read-back verification;
- invalid row rejection before canonical writes;
- no partial canonical pollution when validation fails.

## Small-Scope Guards

The command supports:

```text
--max-codes
--max-trade-days
--max-rows
```

Defaults are `5`, `10`, and `50`. They are passed into the existing Goal 14/15
small-range validator.

## Run Report

Goal 17 writes:

```text
candidate/tushare/daily_price_small_batch_run_report/batch_id=<batch_id>/report.json
```

The report schema is:

```text
goal17.tushare_daily_price_small_batch_run_report.v1
```

It records:

- `batch_id`;
- requested code/date scope and guard limits;
- provider enabled/disabled state;
- source artifact keys;
- staging and candidate/preflight status;
- promotion validator status;
- apply requested/performed state;
- read-back verification result;
- blocked reasons;
- downstream firewalls.

## Boundaries

Goal 17 remains small-scope. It does not perform a 5-10 year backfill, does not
write standard `suspension_status`, and does not trigger
`clean_daily_snapshot`, `factor_input_table`, `factor_daily`,
`selection_result`, or backtest workflows.

The canonical `daily_price` schema remains strict. Goal 17 does not infer
`is_paused` from volume, missing daily rows, unchanged prices, or `suspend_d`
misses without full coverage evidence.

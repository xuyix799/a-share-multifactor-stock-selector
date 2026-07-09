# Goal 18 Tushare Standard Inputs Landing

Goal 18 adds a small-batch landing workflow for Tushare standard input datasets
used by later clean/factor workflows. It is dry-run by default and does not
start downstream workflows.

## Command

Default mode is provider-disabled and dry-run:

```powershell
docker compose run --rm stock-python python -m stock_selector.cli run-tushare-standard-inputs-small-batch `
  --batch-id <batch_id> `
  --codes 000001.SZ,600519.SH `
  --start-date 2024-06-03 `
  --end-date 2024-06-04
```

To rebuild from existing staging without constructing the provider:

```powershell
docker compose run --rm stock-python python -m stock_selector.cli run-tushare-standard-inputs-small-batch `
  --batch-id <batch_id> `
  --codes 000001.SZ,600519.SH `
  --start-date 2024-06-03 `
  --end-date 2024-06-04 `
  --no-provider-call `
  --reuse-existing-staging
```

Provider calls are opt-in only:

```powershell
docker compose run --rm stock-python python -m stock_selector.cli run-tushare-standard-inputs-small-batch `
  --batch-id <batch_id> `
  --codes 000001.SZ,600519.SH `
  --start-date 2024-06-03 `
  --end-date 2024-06-04 `
  --provider-call
```

If the provider is disabled or `TUSHARE_TOKEN` is missing, the command writes a
blocked report and does not write standard data.

## Staging Inputs

Goal 18 reads or writes these staging objects:

```text
candidate/tushare/standard_inputs/stock_basic_staging/batch_id=<batch_id>/part.parquet
candidate/tushare/standard_inputs/daily_basic_staging/batch_id=<batch_id>/trade_date=YYYY-MM-DD/part.parquet
candidate/tushare/standard_inputs/financial_staging/batch_id=<batch_id>/part.parquet
candidate/tushare/standard_inputs/st_history_staging/batch_id=<batch_id>/part.parquet
```

The batch remains small-scope through:

```text
--max-codes
--max-trade-days
--max-rows
```

Defaults are `5`, `5`, and `50`.

## Apply Contract

Standard writes require explicit `--apply`:

```powershell
docker compose run --rm stock-python python -m stock_selector.cli run-tushare-standard-inputs-small-batch `
  --batch-id <batch_id> `
  --codes 000001.SZ,600519.SH `
  --start-date 2024-06-03 `
  --end-date 2024-06-04 `
  --apply
```

Apply mode can write only:

```text
raw/daily_basic/trade_date=YYYY-MM-DD/part.parquet
raw/financial/trade_date=YYYY-MM-DD/part.parquet
candidate/tushare/standard_inputs/stock_basic_candidate/batch_id=<batch_id>/part.parquet
candidate/tushare/standard_inputs/st_history_candidate/batch_id=<batch_id>/part.parquet
```

It does not write `raw/stock_basic`, `raw/st_history`, or standard
`suspension_status`.

## Dataset Rules

`daily_basic` is standard-writable only when the scoped rows:

- cover every requested code/date pair;
- have no duplicate `(stock_code, trade_date)` key;
- pass numeric validity checks;
- pass the existing standard `daily_basic` validator.

`financial` is standard-writable only when the scoped rows:

- contain the strict standard financial fields;
- have no duplicate `(stock_code, report_period, announce_date)` key;
- pass `announce_date <= start_date` for the requested batch scope;
- pass the existing standard `financial` validator.

The as-of rule is intentionally conservative. A disclosure announced after the
batch start date is blocked instead of being written into earlier trade-date
partitions.

`stock_basic` remains candidate-only because Tushare `stock_basic` is a current
snapshot. Goal 18 marks it as `DQ2_CURRENT_SNAPSHOT_ONLY` and records
`CURRENT_SNAPSHOT_NOT_HISTORICAL`.

`st_history` remains candidate-only unless a true historical ST interval source
is available. Goal 18 marks current snapshot-derived rows as
`DQ2_CURRENT_SNAPSHOT_ONLY` and records `ST_STATUS_NOT_HISTORICAL`.

## Run Report

Goal 18 writes:

```text
candidate/tushare/standard_inputs_run_report/batch_id=<batch_id>/report.json
```

The report schema is:

```text
goal18.tushare_standard_inputs_run_report.v1
```

It records:

- `batch_id`;
- requested code/date scope and guard limits;
- provider enabled/disabled state;
- apply requested/performed state;
- per-dataset source object keys;
- per-dataset staging row count, validation status, write status, blocked
  reasons, and DQ status;
- read-back verification;
- upsert summaries;
- downstream firewalls.

## Boundaries

Goal 18 does not perform a 5-10 year backfill, does not run live provider calls
unless `--provider-call` is explicit, and does not trigger
`clean_daily_snapshot`, `factor_input_table`, `factor_daily`,
`selection_result`, scheduler, frontend, Streamlit, LLM, strategy, or backtest
workflows.

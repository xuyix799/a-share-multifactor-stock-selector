# Goal 22 Real Clean Snapshot, Universe and Factor Input

Goal 22 consumes the seven trusted canonical inputs produced or audited by Goals 20/21 and closes the derived-data path through:

```text
adjusted_price
  -> clean_daily_snapshot
  -> risk_filter
  -> eligible_universe
  -> factor_input_table
```

It reuses the existing cleaning and universe builders. It does not implement a parallel set of prices, filters or factor-input formulas, and it does not enter `factor_daily`, `selection_result` or backtest.

Goal 22 is not a provider landing command. It does not make a provider call, promote candidate/smoke data or claim that Goal 21's remaining live-provider capability gaps are closed. Every requested date still requires readable, standard `raw/...` inputs.

## Command and gates

Default dry-run:

```powershell
docker compose run --rm stock-python python -m stock_selector.cli run-real-clean-universe-range `
  --run-id goal22-2024-h1 `
  --start-date 2024-01-02 `
  --end-date 2024-06-28
```

Dry-run reads and validates the standard inputs, executes all five calculations in memory, and writes only the range manifest and per-day DQ JSON. It does not write any `processed/...` Parquet object.

Processed writes require the independent explicit gate:

```powershell
docker compose run --rm stock-python python -m stock_selector.cli run-real-clean-universe-range `
  --run-id goal22-2024-h1 `
  --start-date 2024-01-02 `
  --end-date 2024-06-28 `
  --apply
```

The command defaults to `--resume`. `--no-resume` ignores a prior daily completion report, while `--force` recomputes every date. Neither option implies `--apply`; unchanged processed objects are still not rewritten.

### Trading-date source

Without `--trade-dates`, the CLI uses the union of standard `daily_price`, `adj_factor`, `daily_basic` and `benchmark_price` partition dates in the requested range. This lets one missing market input block a date still evidenced by another market input.

For an authoritative calendar audit, especially when a date could be missing from all four market datasets, pass the exact dates explicitly:

```powershell
--trade-dates 2024-06-03,2024-06-04,2024-06-05
```

The manifest records the date source as `EXPLICIT_CLI` or `STANDARD_MARKET_PARTITION_UNION`. An explicit date outside the requested start/end range is rejected.

## Input contract and historical semantics

Every date requires all seven standard input families:

```text
stock_basic daily_price adj_factor daily_basic financial st_history benchmark_price
```

- `stock_basic`, `daily_price`, `adj_factor`, `daily_basic` and `benchmark_price` use the exact requested-date partition.
- `financial` and `st_history` read canonical partitions no later than the requested date, retain every source object version in the DQ report and deduplicate repeated historical logical keys.
- A column-complete empty `st_history` object is a valid all-clear input. No `st_history` object is a missing input and blocks the date.
- Every input version records its object key, row count and deterministic checksum. The combined as-of frame also records its row count, checksum and column missing rates.

Historical decisions are made as follows:

- Membership is `[list_date, delist_date)`: a row is active only when `list_date <= trade_date` and either `delist_date` is absent or `trade_date < delist_date`.
- ST intervals are `[start_date, end_date)`. Invalid or reversed intervals are rejected; the current `stock_basic.is_st` snapshot is not used to infer history.
- Suspension comes only from the explicit Boolean `daily_price.is_paused` for that historical date. Strings, numeric guesses and price/volume inference are rejected.
- Financial rows are usable only when both `announce_date <= trade_date` and `report_period <= trade_date`; the existing as-of join then chooses the latest known `(announce_date, report_period)` per stock. Later disclosures are counted as excluded and cannot enter the snapshot.
- `daily_price`, `adj_factor` and `daily_basic` must cover every historically active stock. `adj_factor` must be present, finite and strictly positive.
- `financial` must provide at least one usable as-of row for every historically active stock; a future-only or missing code is blocked rather than filled from a later disclosure.
- Benchmark coverage must contain exactly one row for each of `000300.SH`, `000905.SH` and `000906.SH`.

Missing datasets, missing active-stock coverage, duplicate logical keys, invalid schema/nullability, invalid historical intervals, incomplete benchmark coverage or any failed standard validator blocks only the affected date. Other dates continue.

## Derived outputs

With `--apply`, the five validated outputs are written to the processed layer:

```text
processed/adjusted_price/trade_date=YYYY-MM-DD/part.parquet
processed/clean_daily_snapshot/trade_date=YYYY-MM-DD/part.parquet
processed/risk_filter/trade_date=YYYY-MM-DD/part.parquet
processed/eligible_universe/trade_date=YYYY-MM-DD/part.parquet
processed/factor_input_table/trade_date=YYYY-MM-DD/part.parquet
```

The local backend writes below `STOCK_LOCAL_DATA_DIR/processed`. The MinIO backend uses the configured processed bucket. Each Parquet replacement is atomic and is followed by schema, row-count and checksum read-back.

The control artifacts remain in the candidate/control layer:

```text
candidate/real_clean_universe/run_id=<run_id>/manifest.json
candidate/real_clean_universe/run_id=<run_id>/trade_date=YYYY-MM-DD/dq_report.json
```

`run_id` has immutable scope. Reusing it with different dates or a different date source is rejected.

## DQ and recovery

Every daily DQ report records:

- all input keys, versions, row counts, checksums and missing rates;
- financial source/usable/latest-known counts and future-row exclusions;
- not-yet-listed and delisted membership exclusions;
- risk-filter exclusion counts such as `ST`, `PAUSED`, `LISTED_DAYS_LT_MIN`, `AMOUNT_LT_MIN` and financial-quality reasons;
- output row counts, processed keys and checksums;
- requested/performed/unchanged write state and processed read-back evidence;
- blocking/failure evidence and downstream firewalls.

The range manifest records the immutable plan fingerprint, per-date status counts and links to every DQ report and processed output key.

Apply is idempotent. A completed date is reused only when its current input-version record, recomputed output checksums and all five processed read-backs still match. A hard interruption or mid-date write failure leaves the date uncommitted in its DQ state; the next resume recomputes that date, preserves already matching atomic objects and writes only missing or mismatched objects. Completed dates are independently verified and are not rewritten because another date failed.

Do not run two Goal 22 commands concurrently with the same `run_id` or overlapping processed dates. Atomic object replacement prevents a partial Parquet object but is not a cross-process transaction across five object keys.

## Status and downstream boundary

- `READY_FOR_APPLY`: every dry-run date passed and no processed write was requested.
- `COMPLETED`: every requested date passed apply and processed read-back.
- `PARTIAL`: some dates completed/are ready while another date failed or was blocked.
- `BLOCKED` or `FAILED`: no requested date reached the applicable success state.

All reports keep these firewalls closed:

```text
factor_daily     = false
selection_result = false
backtest         = false
```

Goal 22 produces `factor_input_table`; it never calls the factor, selection or backtest pipelines.

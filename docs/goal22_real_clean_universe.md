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
  --run-id goal22-2024-01-small `
  --start-date 2024-01-02 `
  --end-date 2024-01-03 `
  --trade-dates 2024-01-02,2024-01-03 `
  --readiness-report-key candidate/real_clean_inputs/readiness_report/batch_id=<batch-id>/report.json
```

Dry-run reads and validates the standard inputs, executes all five calculations in memory, and writes only the range manifest and per-day DQ JSON. It does not write any `processed/...` Parquet object.

Processed writes require the independent explicit gate:

```powershell
docker compose run --rm stock-python python -m stock_selector.cli run-real-clean-universe-range `
  --run-id goal22-2024-01-small `
  --start-date 2024-01-02 `
  --end-date 2024-01-03 `
  --trade-dates 2024-01-02,2024-01-03 `
  --readiness-report-key candidate/real_clean_inputs/readiness_report/batch_id=<batch-id>/report.json `
  --apply
```

The command defaults to `--resume`. `--no-resume` ignores a prior daily completion report, while `--force` recomputes every date. Neither option implies `--apply`; unchanged processed objects are still not rewritten.

### Trusted readiness and trading-date gates

Both gates are mandatory:

```powershell
--trade-dates 2024-06-03,2024-06-04,2024-06-05
--readiness-report-key candidate/real_clean_inputs/readiness_report/batch_id=<batch-id>/report.json
```

Repeat `--readiness-report-key` when several Goal 20 batches audited the requested dates. Every contributing report must cover the requested dates with the same exact code scope.

Goal 22 accepts a receipt only when:

- the key uses the Goal 20 readiness-report prefix and its companion manifest is readable;
- the report is `READY`, `ready_for_apply=true`, `ready_for_clean=true`, has no blockers, and all seven validation, coverage and canonical read-back checks passed;
- the companion manifest is `COMPLETED`, points to that report, has the same audit scope and source lineage, and binds the report checksum;
- every requested date has all seven canonical object keys, whole-object row counts/checksums and audited-scope row counts/checksums;
- every `st_history` audited-scope row count equals the receipt-level `row_count`; mixed per-date ST states cannot be emitted by Goal 20 and are rejected;
- an empty `st_history` additionally has the exact Goal 20 batch staging key, `coverage.json`, full historical code/date coverage and readable upstream empty-interval evidence; synchronized hand-written report/manifest checksums do not substitute for these artifacts;
- the current canonical objects still match those versions exactly.

There is deliberately no raw-partition date discovery fallback. Omitting either gate is a CLI error. A requested date remains in the plan and DQ even when all four market partitions are later missing.

## Input contract and historical semantics

Every date requires all seven standard input families:

```text
stock_basic daily_price adj_factor daily_basic financial st_history benchmark_price
```

- All seven inputs use the exact canonical partition version authorized for the requested date by Goal 20.
- The audited financial partition may contain several known reports. Goal 22 still applies `announce_date <= trade_date` and `report_period <= trade_date`, then the existing as-of join selects the latest known report per stock.
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

Missing datasets, receipt/object checksum drift, missing active-stock coverage, duplicate logical keys, invalid schema/nullability, invalid historical intervals, incomplete benchmark coverage or any failed standard validator blocks only the affected date. Other dates continue.

## Derived outputs

With `--apply`, the five validated outputs keep these logical dataset identities:

```text
processed/adjusted_price/trade_date=YYYY-MM-DD/part.parquet
processed/clean_daily_snapshot/trade_date=YYYY-MM-DD/part.parquet
processed/risk_filter/trade_date=YYYY-MM-DD/part.parquet
processed/eligible_universe/trade_date=YYYY-MM-DD/part.parquet
processed/factor_input_table/trade_date=YYYY-MM-DD/part.parquet
```

They are physically staged as immutable generations:

```text
processed/<dataset>/trade_date=YYYY-MM-DD/generation=<sha256>/part.parquet
```

After all five generation objects pass schema, row-count and checksum read-back, one atomic date-level commit marker publishes the complete mapping:

```text
processed/_goal22_commits/trade_date=YYYY-MM-DD/commit.json
```

The Goal 22 processed reader resolves only this marker and verifies all five mappings and checksums. A direct `part.parquet` file is not the publication mechanism. Generation objects left by a failed or interrupted attempt remain invisible until a valid commit marker references the complete five-object set.

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

The range manifest records the immutable plan fingerprint, trusted receipt and canonical-version lineage, per-date status/attempt counts, links to every DQ report, logical output key and date commit key.

Apply is idempotent. A completed date is reused only when its commit marker binds the same run, plan, input fingerprint and output checksums and all five committed read-backs still match. A hard interruption or mid-date generation failure leaves the current DQ `RUNNING`/uncommitted and does not publish a partial set. The next resume recomputes that date, preserves already matching generation objects and writes only missing or mismatched objects before atomically committing. An older committed generation remains intact until a replacement commit succeeds.

Resume/commit-read and Parquet I/O errors are caught inside the affected date boundary, persisted as that date's failure and do not stop later dates. A schema-complete empty `eligible_universe` and matching empty `factor_input_table` are valid outputs when every stock was legitimately filtered; `risk_filter` and upstream clean outputs remain non-empty.

Do not run two Goal 22 commands concurrently with the same `run_id` or overlapping processed dates. The commit marker makes one publication atomic, but it is not a cross-process compare-and-swap lock; serialize overlapping publishers.

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

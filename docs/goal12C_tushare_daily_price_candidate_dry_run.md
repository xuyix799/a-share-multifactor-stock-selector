# Goal 12C Tushare Daily Price Candidate Dry Run

Goal 12C is a standard-layer precheck only. It reads existing Tushare smoke Parquet files and runs a small join dry-run to see whether a diagnostic `daily_price_candidate` report can be assembled from:

- `daily`
- `stk_limit`
- `adj_factor`
- `trade_cal`
- `suspend_d`

It does not call Tushare, does not write standard `daily_price`, and does not feed any real selection or backtest path.

## CLI

```powershell
python -m stock_selector.cli dry-run-tushare-daily-price-candidate `
  --trade-date 2024-06-19 `
  --sample-limit 5
```

The CLI reads only smoke inputs:

```text
smoke/tushare/daily/trade_date=YYYY-MM-DD/part.parquet
smoke/tushare/stk_limit/trade_date=YYYY-MM-DD/part.parquet
smoke/tushare/adj_factor/trade_date=YYYY-MM-DD/part.parquet
smoke/tushare/trade_cal/trade_date=YYYY-MM-DD/part.parquet
smoke/tushare/suspend_d/trade_date=YYYY-MM-DD/part.parquet
```

If any input is missing, it returns `BLOCKED_BY_MISSING_SMOKE_INPUT`, lists the missing smoke `object_key`, and does not call a real provider or fabricate data.

## Output

The dry-run writes only diagnostic smoke output:

```text
smoke/tushare/daily_price_candidate_dry_run/trade_date=YYYY-MM-DD/report.json
smoke/tushare/daily_price_candidate_dry_run/trade_date=YYYY-MM-DD/part.parquet
```

These paths are not standard `daily_price` paths and are not read by `clean_daily_snapshot`, `factor_input_table`, `factor_daily`, `selection_result`, or `run-backtest`.

Inside `report.json`, MinIO locations use object-key naming only:

```text
input_smoke_object_keys.<dataset>
inputs.<dataset>.object_key
output_object_keys.report
output_object_keys.candidate
```

The report JSON does not also carry top-level `report_key` or `candidate_key`. The CLI summary may still print those two names for operator readability, but they are not duplicated inside the persisted report.

## Candidate vs Standard Daily Price

`daily_price_candidate` is a report-level composition used to inspect field coverage and join quality. It is not a promoted dataset.

Standard `daily_price` is the strict tradable quote layer. It must have validated OHLCV, `pre_close`, `limit_up`, `limit_down`, and an explicit boolean `is_paused`, plus DQ3 or DQ4 provenance and validator evidence.

Goal 12C deliberately leaves:

- `READY_FOR_DQ3_PROMOTION=false`
- `standard_daily_price_written=false`
- `real_raw_mainline_written=false`
- `cleaning_mainline_entered=false`
- `factor_mainline_entered=false`
- `selection_mainline_entered=false`
- `backtest_mainline_entered=false`
- `spring_api_changed=false`

## Field Sources

The dry-run maps fields as follows:

| Candidate field | Source |
| --- | --- |
| `open` | `daily` |
| `high` | `daily` |
| `low` | `daily` |
| `close` | `daily` |
| `pre_close` | `daily` |
| `volume` | `daily` |
| `amount` | `daily` |
| `limit_up` | `stk_limit` |
| `limit_down` | `stk_limit` |
| `adj_factor` | `adj_factor` |
| `trading_day_confirmed` | `trade_cal` |
| `trade_cal_is_open` | `trade_cal` |
| `trade_cal_exchange` | `trade_cal` |
| `pause_status` | `suspend_d_hit_or_unresolved_unknown` |
| `is_paused_true_candidate` | `suspend_d_hit_only` |
| `suspend_type` | `suspend_d` |
| `suspend_timing` | `suspend_d` |

`trade_cal` only says whether an exchange is open on a date. It is not a per-stock suspension source.

## Suspension Semantics

`suspend_d` has event-source semantics:

- A matching `ts_code` and date can produce `pause_status=true_candidate`.
- A miss can only produce `pause_status=unknown`.
- A miss must not become `is_paused=false`.

Pause unknowns are counted in `pause_status_counts.unknown`. They are not counted as `missing_field_stats.is_paused_true_candidate`, because that would make expected unknown status look like a standard field-quality failure.

The dry-run also forbids these pause shortcuts:

- `volume=0`
- `amount=0`
- a missing `daily` row
- unchanged prices

Those patterns may be useful for later diagnostics, but they are not authoritative suspension evidence.

## Report Contents

`report.json` includes:

- input smoke object keys
- output object keys
- row counts and columns for each input
- join row count
- join keys: `ts_code`, `trade_date`
- field-source mapping
- missing field counts for real price, limit, adjustment, and calendar fields only
- duplicate key checks
- trade-date consistency checks
- `coverage.limit_price_fields.all_fields`
- `coverage.limit_price_fields.limit_up`
- `coverage.limit_price_fields.limit_down`
- `adj_factor` coverage
- `suspend_d` match coverage
- pause status counts: `true_candidate`, `unknown`, and always `false=0`
- DQ3 readiness status
- safety flags showing no standard, clean, factor, selection, Spring, LLM, or backtest path was used

## DQ3 Readiness

Goal 12C normally reports:

```text
READY_FOR_DQ3_PROMOTION=false
BLOCKED_BY_UNRESOLVED_IS_PAUSED
```

Promotion to standard `daily_price` requires all of the following later gates:

- staging
- coverage audit
- duplicate check
- missing check
- validator
- small-range promotion
- mock mainline protection

Until a standard `suspension_status` staging layer has audited coverage and can produce explicit boolean `is_paused` values, the candidate cannot become DQ3.

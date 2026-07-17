# Goal 20 Real Clean Input Readiness

Goal 20 adds a bounded landing and readiness command for the seven canonical inputs consumed by `build_clean_daily_snapshot`. It stops at input readiness: it never invokes clean, universe, factor, selection, or backtest workflows.

## Command

Local audit, with no provider construction and no canonical writes:

```powershell
$env:PYTHONPATH='python-engine/src'
python -m stock_selector.cli run-real-clean-inputs-small-batch `
  --batch-id <batch_id> `
  --codes 000001.SZ,600519.SH `
  --start-date 2024-06-03 `
  --end-date 2024-06-04
```

Explicit provider staging:

```powershell
python -m stock_selector.cli run-real-clean-inputs-small-batch `
  --batch-id <batch_id> `
  --codes 000001.SZ,600519.SH `
  --start-date 2024-06-03 `
  --end-date 2024-06-04 `
  --provider-call
```

Explicit canonical promotion additionally requires `--apply`. Existing Goal 13 adj-factor staging is considered only with `--reuse-existing-staging`, and its manifest must prove the requested batch, code/date scope, and successful `adj_factor` interface.

## Safety Gates and Small-Scope Guards

- Provider construction and network calls are disabled unless `--provider-call` is present.
- Canonical writes are disabled unless `--apply` is present.
- `--provider-call` and `--apply` are independent; neither bypasses validation.
- `--max-codes` and `--max-trade-days` bound the requested scope; `--max-rows` additionally bounds each provider result before it can be staged.
- All seven inputs must pass preflight before any Goal 20 canonical write begins.
- Provider results are written only to Goal 20 staging before preflight.
- Canonical writes reuse the existing partition builder, atomic writer, MinIO/local backend, and Goal 18 idempotent upsert helper.
- A failed write or read-back keeps `ready_for_clean=false`.

## Seven-Input Audit Contract

| Input | Accepted source | Required validation | Blocking examples |
| --- | --- | --- | --- |
| `stock_basic` | Goal 20 point-in-time historical staging plus readable upstream evidence | standard schema; complete code/date coverage; unique `(stock_code, trade_date)`; snapshot date equality; `list_date <= trade_date`; `delist_date` preserved and later than the snapshot if present | current snapshot, future listing, already-delisted row, missing or mismatched evidence |
| `daily_price` | Goal 17 canonical `raw/daily_price` | existing standard validator; complete requested code/date coverage; unique key | missing partition/code, invalid prices or duplicate key |
| `adj_factor` | Goal 20 provider staging, valid Goal 13 staging/manifest, or existing canonical | complete code/date coverage; unique key; finite numeric values; `adj_factor > 0` | zero/negative/NaN/infinite factor, incomplete coverage, invalid Goal 13 manifest |
| `daily_basic` | Goal 18 canonical `raw/daily_basic` | existing standard validator; complete code/date coverage; unique key | missing partition/code, duplicate key or schema failure |
| `financial` | Goal 18 canonical `raw/financial` | existing standard validator; unique disclosure key; every `announce_date` no later than the audited partition date | future disclosure, missing code, duplicate disclosure or schema failure |
| `st_history` | Goal 20 interval staging plus readable upstream historical evidence | historical semantics and source; complete code/date proof; valid `[start_date, end_date)`; unique interval key | current name/ST snapshot, self/smoke lineage, incomplete coverage, invalid interval or unreadable evidence |
| `benchmark_price` | Goal 20 AKShare provider staging or existing canonical | every requested date has exactly `000300.SH`, `000905.SH`, `000906.SH`; unique keys; finite numeric values; standard price validation | one-index smoke, missing/extra index, duplicate key, invalid numeric/price |

Smoke objects are never searched as promotion inputs. In particular, `smoke/akshare/benchmark_price/...` cannot satisfy benchmark readiness.

## Historical Source Contract

### `stock_basic`

Every history-staging row contains the standard `stock_basic` columns plus:

```text
source_snapshot_date
source_object_key
source_semantics
```

`source_snapshot_date` equals the partition date. `source_semantics` is exactly `POINT_IN_TIME_HISTORICAL_SNAPSHOT`. `source_object_key` points to a readable, non-smoke upstream Parquet object whose standard values match the staged rows. Goal 18 current `list_status=L` / current-name snapshots do not meet this contract and remain `DQ2_CURRENT_SNAPSHOT_ONLY`.

### `st_history`

History staging contains the standard interval columns plus:

```text
source_object_key
source_semantics
coverage_codes
coverage_start_date
coverage_end_date
coverage_complete
```

`source_semantics` is exactly `HISTORICAL_INTERVAL_SOURCE`. The source key points to readable upstream historical interval evidence and cannot point to smoke or Goal 20 staging. Coverage metadata proves the requested codes and full date range, including codes with no matching ST interval. Interval rows use `[start_date, end_date)` semantics. Current names/current ST flags are never converted into history.

An empty interval file is valid only with `coverage.json` using schema `goal20.st_history_coverage.v1`. The sidecar records `source_semantics`, readable `source_object_keys`, `coverage_codes`, `coverage_start_date`, `coverage_end_date`, `coverage_complete=true`, and `interval_row_count=0`. The standard validator accepts a column-complete empty `st_history` frame, so an all-clear scope can be written and read back without fabricating an ST interval. An empty file without that proof remains blocked.

## Object Keys and Lineage

Goal 20 control and staging keys are:

```text
candidate/real_clean_inputs/manifest/batch_id=<batch_id>/manifest.json
candidate/real_clean_inputs/readiness_report/batch_id=<batch_id>/report.json
candidate/real_clean_inputs/adj_factor_staging/batch_id=<batch_id>/trade_date=YYYY-MM-DD/part.parquet
candidate/real_clean_inputs/benchmark_price_staging/batch_id=<batch_id>/trade_date=YYYY-MM-DD/part.parquet
candidate/real_clean_inputs/stock_basic_history_staging/batch_id=<batch_id>/trade_date=YYYY-MM-DD/part.parquet
candidate/real_clean_inputs/st_history_interval_staging/batch_id=<batch_id>/part.parquet
candidate/real_clean_inputs/st_history_interval_staging/batch_id=<batch_id>/coverage.json
```

Canonical destinations remain:

```text
raw/<dataset>/trade_date=YYYY-MM-DD/part.parquet
```

The adj-factor audit reuses Goal 13 keys returned by `build_tushare_candidate_staging_batch_output_keys`; the report records the Goal 13 manifest and staging keys as lineage. Daily price, daily basic, and financial are audited from Goal 17/18 canonical partitions rather than reimplementing their landing workflows.

## Apply and Read-Back Contract

Goal 20 promotes only `stock_basic`, `adj_factor`, `st_history`, and `benchmark_price`; it audits the Goal 17/18 canonical inputs in place. No canonical writes start until all seven source audits pass.

Each promoted partition is merged with existing canonical data using these keys:

```text
stock_basic       (stock_code, trade_date)
adj_factor        (stock_code, trade_date)
st_history        (stock_code, st_type, start_date, source)
benchmark_price   (index_code, trade_date)
```

The merged frame is validated before the atomic write. The command then reads every canonical input back, reruns the standard validator, scopes it to the requested codes/indexes, and compares complete keys, row counts, columns, and values with the audited source. A repeated identical apply reports zero inserts/updates and positive unchanged counts.

## Readiness Report

The report schema is `goal20.real_clean_input_readiness.v1`. Every input records:

```text
source_keys
row_count
dq_level
coverage
validation
write
read_back
ready_for_apply
ready_for_clean
blocked_reasons
```

Each successful canonical read-back detail additionally binds:

```text
object_key
object_row_count
object_checksum
scope_row_count
scope_checksum
```

The companion manifest records `readiness_report_checksum`. Goal 22 requires this checksum binding and rejects older or hand-edited report/manifest pairs that do not match. Regenerate a Goal 20 readiness receipt after upgrading before using it as a Goal 22 gate.

For an empty `st_history`, Goal 22 also reopens the exact batch-scoped interval staging object and `coverage.json`, checks the audited code/date range, and reads every upstream evidence object named by the sidecar. The report and manifest lineage must be exactly the staging key, coverage key and those evidence keys in producer order. Recomputing the report checksum cannot replace this proof.

The top level records the requested scope, provider/apply mode, staging writes, per-input status, aggregate upsert summaries, read-back details, blocked reasons, output keys, and downstream firewalls.

- `ready_for_apply=true` means all seven inputs are already trusted canonical data or have validated sources safe to promote.
- `ready_for_clean=true` means all seven canonical inputs in the requested scope passed full read-back verification.
- A completed manifest means the Goal 20 audit run finished; its separate `readiness_status` and readiness booleans determine whether the data is usable.

## Boundaries

Goal 20 does not call `build_clean_daily_snapshot`, build adjusted prices, construct the universe, compute factors, select stocks, or run backtests. It does not treat current snapshots, inferred ST history, default/fabricated values, future data, or smoke-only artifacts as canonical history.

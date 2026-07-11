# Goal 21 Resumable Historical Backfill

Goal 21 provides a deterministic, resumable and auditable backfill framework for the seven canonical inputs required by Goal 20:

```text
stock_basic
daily_price
adj_factor
daily_basic
financial
st_history
benchmark_price
```

The framework can plan five-to-ten-year ranges and one-day increments, but this Goal does not claim that a real ten-year provider fetch has been completed. Historical completeness still depends on source capability, semantics, coverage, DQ and canonical read-back evidence for every planned chunk.

## Command and exact flags

After changing the Python image source, rebuild it before running the container command:

```powershell
docker compose build stock-python
```

Default plan-only mode:

```powershell
docker compose run --rm stock-python python -m stock_selector.cli run-real-history-backfill `
  --run-id <run_id> `
  --start-date 2019-01-01 `
  --end-date 2024-12-31 `
  --codes 000001.SZ,600519.SH
```

The required scope arguments are `--run-id`, `--start-date`, `--end-date` and exactly one of:

- `--codes <comma-separated-codes>`;
- `--universe-key <safe-parquet-object-key>`.

`--universe-key` must identify a safe relative `.parquet` object containing `stock_code`. The resolved codes and the source key are frozen into the plan lineage. An empty `--codes` value is invalid and never falls back to a default universe.

The complete optional flag contract is:

| Flag | Default | Meaning |
| --- | --- | --- |
| `--provider-call` | off | Permit construction and invocation of the historical provider adapter. |
| `--apply` | off | Permit canonical `raw/...` writes after verified staging and validation. |
| `--resume` / `--no-resume` | resume on | Reconcile and reuse valid checkpoints, or request a new attempt for enabled stages. |
| `--force` | off | Rerun only stages whose provider/apply gate is already enabled. It does not open either gate. |
| `--code-batch-size` | `10` | Maximum stock codes in a stock-scoped planner chunk. |
| `--date-batch-days` | `31` | Maximum calendar days in a date-window planner chunk. |
| `--report-period-months` | `3` | Maximum report-period months in a financial planner chunk. |

These are the only planner batch-size controls. There are no Goal 21 CLI flags for dataset selection, row limits, chunk limits, attempt limits, retry delay or provider selection.

## Four execution modes

The provider and canonical-write gates are independent:

| Invocation | Mode | Permitted effects |
| --- | --- | --- |
| no opt-in flags | plan only / dry-run | Write the immutable plan and root control manifest only. It does not construct a provider, access a provider network, write staging Parquet or call a canonical writer. With the local backend it also does not initialize MinIO. |
| `--provider-call` | provider-only | Fetch, validate and write immutable per-attempt staging. It cannot write canonical `raw/...` objects. Successful chunks may stop at `STAGED`. |
| `--apply` | apply-only | Do not construct or call a provider. Consume checksum-verified existing staging and write/read back canonical partitions. Missing, foreign or mismatched staging blocks the affected chunk. |
| `--provider-call --apply` | combined | Fetch into immutable staging first, then validate, idempotently upsert and read back canonical partitions. |

Examples:

```powershell
# Provider-only staging
docker compose run --rm stock-python python -m stock_selector.cli run-real-history-backfill `
  --run-id <run_id> `
  --start-date 2024-01-01 `
  --end-date 2024-03-31 `
  --codes 000001.SZ,600519.SH `
  --provider-call

# Apply-only continuation from verified staging
docker compose run --rm stock-python python -m stock_selector.cli run-real-history-backfill `
  --run-id <same_run_id> `
  --start-date 2024-01-01 `
  --end-date 2024-03-31 `
  --codes 000001.SZ,600519.SH `
  --apply

# One-day incremental combined run
docker compose run --rm stock-python python -m stock_selector.cli run-real-history-backfill `
  --run-id <new_increment_run_id> `
  --start-date 2025-01-02 `
  --end-date 2025-01-02 `
  --universe-key raw/universe/history.parquet `
  --provider-call `
  --apply
```

The same `run_id` must keep the same dates, resolved codes, datasets and planner limits. Changing immutable scope under an existing `run_id` is rejected by plan-fingerprint validation; use a new run ID for a different increment.

## Dataset-specific history contract

Goal 21 plans all seven inputs by default and does not replace their standard schemas or validators.

| Dataset | Chunk axis and required historical evidence |
| --- | --- |
| `stock_basic` | Code batch plus snapshot-date window. Rows must be point-in-time history or agree with a proven list/delist master; `list_date` and `delist_date` are preserved. A current listed-only snapshot cannot prove a historical universe. |
| `daily_price` | Code batch plus date window. The Tushare route must combine trading calendar, daily bars, price limits and complete suspension-event evidence before `is_paused` is accepted. |
| `adj_factor` | Code batch plus date window. Standard code/date uniqueness, complete scope coverage and finite `adj_factor > 0` are required. |
| `daily_basic` | Code batch plus date window, with standard schema, unique code/date keys and complete requested coverage. |
| `financial` | Code batch plus report-period window. Rows are materialized into a canonical trade-date partition only when `announce_date <= trade_date`; future disclosure data is not backfilled into the past. |
| `st_history` | Code batch plus interval window. Only historical interval evidence with complete code/date coverage is accepted. Current name or current ST state is never inverted into history. A proven empty interval scope is allowed without fabricating rows. |
| `benchmark_price` | Date window covering exactly `000300.SH`, `000905.SH` and `000906.SH`, with historical range lineage and consistent price/change evidence. Smoke-only output cannot satisfy this contract. |

Any unproven historical meaning is `BLOCKED`; the executor does not insert fake values, defaults or future data to complete a chunk.

## Object keys and immutable evidence

Every run uses this control prefix:

```text
candidate/real_history_backfill/run_id=<run_id>/
```

Its control and attempt objects are:

```text
candidate/real_history_backfill/run_id=<run_id>/plan.json
candidate/real_history_backfill/run_id=<run_id>/manifest.json
candidate/real_history_backfill/run_id=<run_id>/dataset=<dataset>/chunk_id=<chunk_id>/manifest.json
candidate/real_history_backfill/run_id=<run_id>/dataset=<dataset>/chunk_id=<chunk_id>/attempt=<attempt>/report.json
candidate/real_history_backfill/run_id=<run_id>/dataset=<dataset>/chunk_id=<chunk_id>/attempt=<attempt>/part.parquet
```

`plan.json` is immutable. A chunk manifest is its current checkpoint; attempt reports and staging Parquet are immutable attempt evidence. The root manifest is a summary, not the only recovery record.

Canonical destinations remain:

```text
raw/<dataset>/trade_date=YYYY-MM-DD/part.parquet
```

Canonical partitions are handled by one serialized writer. Each incoming code shard is validated and idempotently upserted by the dataset key, atomically written, then read back and checksum/subset verified. Multiple logical canonical keys in a manifest are audited partition scopes; a proven empty `stock_basic`/`st_history` scope may record a non-materialized partition and does not create a fake canonical row or object.

Operationally, do not run two Goal 21 processes with the same `run_id`, and do not run concurrent `--apply` jobs whose scopes can replace the same canonical partition. The atomic writer prevents partial objects, but it is not a cross-process compare-and-swap lock: concurrent read-merge-write cycles can otherwise lose one writer's update. Serialize overlapping applies or schedule provably disjoint canonical partitions.

## Resume, force and recovery

Chunk states are:

```text
PENDING RUNNING STAGED COMPLETED FAILED BLOCKED INTERRUPTED
```

With the default `--resume`, a chunk is skipped only after its immutable identity, staging checksum and required canonical read-back evidence reconcile. A stale `RUNNING` checkpoint is inspected against staging and canonical data before retry. Ordinary chunk failures are checkpointed and independent chunks continue; an interruption is persisted as `INTERRUPTED` before it is re-raised.

`--no-resume` requests a new attempt for stages enabled in the invocation. `--force` also reruns enabled stages, but neither option implies `--provider-call` or `--apply`. A provider-only run can therefore be resumed later with apply-only mode, while a completed apply can be skipped only when its evidence still matches.

## Failure taxonomy and secret redaction

Manifests and attempt reports preserve the failure category instead of converting a failed or empty provider call into success:

```text
EMPTY_RESULT
RATE_LIMITED
PERMISSION_DENIED
SCHEMA_DRIFT
TRANSIENT_PROVIDER_ERROR
CONFIGURATION_ERROR
SEMANTIC_SOURCE_UNAVAILABLE
DQ_FAILED
WRITE_FAILED
READBACK_FAILED
INTERRUPTED
UNKNOWN
```

Retryability is stored with the category. Provider parameters, exception messages and saved failure records are sanitized so tokens, passwords, authorization values, API keys and other credentials do not enter the artifacts or compact CLI output.

Each attempted chunk records its immutable scope, attempt number, provider calls/status, source keys, row count, actual/target schema, DQ and coverage, staging key/checksum, canonical keys/checksums, validation, write/read-back evidence, failure and final state. Partial canonical work remains explicitly `FAILED` or `BLOCKED` with per-partition evidence; it is never reported as a completed all-or-nothing write.

## Current live-provider capability boundary

The built-in routing is intentionally conservative:

- Tushare is eligible only for `daily_price`, `adj_factor` and `daily_basic`, and still blocks a chunk when calendar, endpoint, field, suspension, coverage or DQ evidence is incomplete.
- The provider contract permits AKShare only for `benchmark_price`, but the current CLI has no trusted public historical-range adapter that proves all three required indexes and the standard price/change contract. Live benchmark chunks therefore remain `SEMANTIC_SOURCE_UNAVAILABLE`; existing smoke-only objects are not reused or promoted.
- Current `stock_basic` snapshots, unresolved live `financial` field/unit semantics and current-name/current-status-derived `st_history` remain `SEMANTIC_SOURCE_UNAVAILABLE` until a trusted historical adapter supplies the required evidence.
- Baostock is not treated as satisfying any complete seven-input history contract in this framework.
- Disabled providers, missing credentials and unsupported routes remain structured `CONFIGURATION_ERROR`/`SEMANTIC_SOURCE_UNAVAILABLE` results rather than silent fallback data.

Goal 21 supplies the backfill plan, checkpoints, staging, canonical reconciliation and audit trail. It does not assert full live-provider completeness. Goal 20 remains the seven-input readiness decision, and Goal 25 is responsible for closing the remaining real-provider capability and full-history completeness gaps.

## Downstream firewall

Every result keeps these downstream firewalls closed:

```text
clean_daily_snapshot = false
factor               = false
selection            = false
backtest             = false
```

Goal 21 does not import or invoke clean, universe construction, factor generation, selection or backtest workflows. A `COMPLETED` backfill chunk or `canonical_ready=true` summary is evidence about this backfill plan only; it does not trigger Goal 20 readiness or any downstream computation.

# Goal 21 Resumable Historical Backfill Design

## Approved scope

Goal 21 adds a resumable, auditable and idempotent framework for planning and incrementally executing five to ten years of the seven Goal 20 standard inputs. The command remains dry-run and offline by default, requires independent `--provider-call` and `--apply` opt-ins, and never enters clean, factor, selection or backtest code.

The seven datasets are `stock_basic`, `daily_price`, `adj_factor`, `daily_basic`, `financial`, `st_history` and `benchmark_price`. Goal 20 remains the final readiness contract; it is not widened into the historical executor.

## Chosen architecture

The implementation uses an immutable plan plus dataset-specific chunks and a shared single-writer executor.

1. The planner validates the requested date range and exactly one universe source (`codes` or a Parquet `universe_key`). It emits stable chunk IDs and a plan fingerprint independent of timestamps and execution gates.
2. Each dataset has an explicit strategy instead of a generic date loop:
   - `daily_price`: stock-code/date-window chunks whose provider adapter must combine trading calendar, daily bars, price limits and complete suspension-event coverage.
   - `adj_factor` and `daily_basic`: stock-code/date-window chunks.
   - `financial`: stock-code/report-period-window chunks, materialized only where `announce_date <= canonical trade_date`.
   - `stock_basic`: stock-code/snapshot-window chunks sourced only from point-in-time archives or a proven list/delist master; current listed-only snapshots are blocked.
   - `st_history`: stock-code/interval-window chunks sourced only from historical interval evidence; current name or current ST state is blocked.
   - `benchmark_price`: three-index/date-window chunks with range retrieval and the required `000300.SH`, `000905.SH` and `000906.SH` coverage.
3. Provider output is written to immutable attempt staging before canonical writes. The canonical writer reuses the schema contracts, validators, canonical partition builder, atomic writer and idempotent upsert semantics already proven by Goals 17–20.
4. Every attempted chunk has a mutable checkpoint manifest plus immutable attempt reports. A root manifest summarizes state without being the sole recovery record.
5. Resume skips a completed chunk only after staging checksum and, when applicable, canonical read-back reconciliation both pass. `force` reruns requested stages but never enables provider or canonical writes by itself.

## Object-key contract

Control and staging artifacts use this prefix:

```text
candidate/real_history_backfill/run_id=<run_id>/
```

The immutable plan is `plan.json`, the root summary is `manifest.json`, and each chunk uses:

```text
dataset=<dataset>/chunk_id=<chunk_id>/manifest.json
dataset=<dataset>/chunk_id=<chunk_id>/attempt=<attempt>/report.json
dataset=<dataset>/chunk_id=<chunk_id>/attempt=<attempt>/part.parquet
```

Canonical objects remain unchanged:

```text
raw/<dataset>/trade_date=YYYY-MM-DD/part.parquet
```

Code shards are serialized into canonical partitions through idempotent upsert; no two workers concurrently replace the same object.

## Manifest state and recovery

Chunk manifests record immutable scope plus `attempt_count`, provider status, row count, actual/target schema, DQ, coverage, source and canonical keys, staging and canonical checksums, validation, write/read-back results, failure classification and current state.

States are `PENDING`, `RUNNING`, `STAGED`, `COMPLETED`, `FAILED`, `BLOCKED` and `INTERRUPTED`. A chunk is `COMPLETED` only when every stage requested by the current invocation is proven. A provider-only run may be `STAGED`; a later apply-only invocation can consume that staging. A stale `RUNNING` checkpoint is reconciled from staging and canonical read-back before retry.

The root summary preserves count conservation and reports planned, staged, completed, failed, blocked, interrupted and pending chunks, completion rate, per-dataset totals and structured gap records. Canonical readiness is true only when all required chunks have passed apply/read-back.

## Provider status taxonomy

Failures are never flattened into success. The stable categories are:

- `EMPTY_RESULT`: no rows where rows are required;
- `RATE_LIMITED`: retryable frequency/HTTP throttling;
- `PERMISSION_DENIED`: non-retryable missing endpoint permission;
- `SCHEMA_DRIFT`: missing or incompatible source fields;
- `TRANSIENT_PROVIDER_ERROR`: retryable network/service failure;
- `CONFIGURATION_ERROR`: disabled provider or missing credential;
- `SEMANTIC_SOURCE_UNAVAILABLE`: data exists but cannot prove the required historical meaning;
- `DQ_FAILED`, `WRITE_FAILED` and `READBACK_FAILED` for post-fetch failures.

Messages and saved parameters are sanitized so tokens and credential values never enter reports.

Existing real-provider capability is deliberately conservative. Goal 17's Tushare range/date strategies can be reused for price-like data. Current listed-only `stock_basic`, current-name-derived ST rows and unresolved financial field/unit mappings must be blocked until a trusted adapter is supplied. This is required behavior, not a silent fallback. Goal 25 remains responsible for full live completeness.

## Safety gates

- With neither opt-in, the command writes only control artifacts and performs no provider or canonical calls.
- `--provider-call` may write immutable staging but does not write canonical data without `--apply`.
- `--apply` may consume previously verified staging without provider access; missing or mismatched staging blocks the chunk.
- `--resume` is the default. `--force` takes precedence only for stages explicitly enabled.
- One chunk failure is checkpointed and execution continues with independent chunks.
- No function from cleaning, universe construction, factor generation, selection or backtesting is imported or invoked by the framework.

## Verification contract

Tests must prove deterministic five- and ten-year plans plus a one-day incremental plan; dataset-specific strategies; dry-run/no-network behavior; provider/apply gate independence; valid apply/read-back; duplicate rerun idempotency; resume, force and stale-running reconciliation; interruption recovery; isolated failure; accurate summaries and gaps; all failure categories; financial as-of filtering; list/delist/ST history semantics; three-index benchmark coverage; secret redaction; and downstream firewalls. The final gate is the complete local Python suite, `git diff --check`, Docker image build, Docker Python suite and Docker Maven suite, followed by the same Docker tests after the fast-forward merge.

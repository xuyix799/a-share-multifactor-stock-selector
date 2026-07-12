# Goal 21 Review Fixes Design

## Approved scope

This follow-up repairs the eight correctness, auditability, recovery and scale defects found after the verified Goal 21 baseline. It stays inside historical standard-input backfill. It does not invoke or modify clean snapshot, factor, selection or backtest execution.

The existing `goal-21-historical-backfill-verified` tag remains an immutable v1 baseline. The repair is developed on `codex/goal21-review-fixes`; no existing v1 chunk identity or persisted manifest is silently reinterpreted.

## Safety invariants

- Provider access still requires explicit `--provider-call`.
- Canonical writes still require explicit `--apply`.
- Dry-run never constructs or calls a live provider.
- Failed or semantically unproven input remains blocked; the repair never fills fake values, fake lineage, current-state history or future information.
- `.qoder/` and unrelated workspace changes remain untouched.
- Canonical object keys and downstream firewalls remain unchanged.

## 1. Verifiable live provenance

A successful live response must first be persisted as an immutable raw-provider landing object using the existing atomic Parquet writer, checksum calculation and read-back verification. The landing key is upstream of Goal 21 attempt staging and is therefore valid lineage; the candidate staging key itself must never be used as its own source.

Each provider call records a sanitized request identity, endpoint, requested scope, returned row count, raw landing key, checksum and read-back result. `HistoricalChunkFetchResult.source_keys` is populated only from successfully verified raw landing objects. `daily_price` carries every contributing source, including daily bars and the market-level `stk_limit` and `suspend_d` sidecars used to establish price-limit and suspension semantics.

The scalar v1 manifest `source_key` remains readable. New manifests persist ordered `source_keys` and derive the legacy scalar field only for compatibility. A successful v2 result without verified source keys is rejected.

## 2. Truthful provider failure audit

The executor snapshots the complete validated provider result before branching on `FETCHED`, `FAILED` or `BLOCKED`. The checkpoint and immutable attempt report retain:

- provider status and sanitized calls;
- actual and target schema;
- DQ, coverage and validation evidence;
- actual source keys, including an empty list;
- the provider's original failure category, retryability, exception type and message.

Only an exception raised before a structured provider result exists is classified by the executor. Structured `FAILED` and `BLOCKED` results never write staging or canonical data and are not reclassified into a generic executor error.

## 3. Suspension completeness contract

The CLI adapter does not create or overwrite completeness attributes. Negative evidence is sticky: `sample_truncated=true`, `coverage_complete=false`, an incomplete page sequence or a mismatched request scope always blocks the result.

Completeness must come from the provider boundary and identify the exact endpoint, trade date, market scope, page sequence and terminal-page evidence. A complete empty response is acceptable only when the same proof shows the exact full-market request completed. Otherwise absence from `suspend_d` cannot be interpreted as `is_paused=false`.

Market-level `stk_limit` and `suspend_d` responses are fetched once per trade date and shared by every requested stock subset through the same immutable source key. They are never repeated per code batch.

## 4. READY-report crash recovery

At startup, before creating a new attempt, the executor checks for the previous immutable attempt report whenever the mutable manifest is stale or not final. A report is recoverable only if all of these match the current plan and manifest:

- schema and identity version;
- run ID, plan fingerprint, dataset, chunk ID and attempt number;
- requested provider/apply stages;
- `READY_TO_CHECKPOINT` state and checkpoint target state;
- source, staging and canonical keys;
- staging checksum and read-back evidence;
- canonical checksum/read-back evidence when apply was requested.

After strict validation, the final manifest is reconstructed idempotently. For `COMPLETED`, canonical scope reconciliation is run again. Invalid or ambiguous READY reports fail closed and are never trusted as recovery state.

## 5. Historical scope correctness

Empty `stock_basic` completed-resume checks use the same negative-scope predicate as the apply path: if canonical data contains any code belonging to the chunk, the chunk is not exact regardless of DataFrame dtype.

ST intervals use one shared half-open overlap predicate:

```text
interval overlaps chunk iff start_date <= chunk_end
                          and (end_date is null or end_date > chunk_start)
```

Therefore an interval whose `end_date == chunk_start` is outside the chunk in staging validation, canonical reconciliation and conflict checks.

## 6. Financial announcement semantics

Financial source chunks are scoped by `announce_date`, not by `report_period`. A row is valid when its announcement falls inside the source chunk and `report_period <= announce_date`. A one-day `2024-06-03` increment may therefore contain a `2024-03-31` report announced on that day.

Materialization remains point-in-time: a canonical trade date may see only announcements on or before that date. The reducer carries the previous canonical state forward and merges newly announced rows, then reads, writes and verifies each canonical trade-date partition at most once per run.

The v2 CLI uses `--financial-announce-months`. The v1 `--report-period-months` option is not silently redefined.

## 7. Plan/identity v2 and bounded execution

V2 separates immutable source-fetch chunks from canonical materialization chunks.

### Source axes

- `daily_price`, `adj_factor` and `daily_basic`: full-market trade-date windows, filtered to the requested universe after fetch.
- `daily_price` sidecars: one shared `stk_limit` and one shared `suspend_d` fetch per trade date.
- `financial`: announcement-date windows and bounded code cohorts only where the provider cannot supply trustworthy market-wide pagination.
- `stock_basic`: historical master/snapshot source partitions, not repeated date-by-code Cartesian chunks.
- `st_history`: historical interval source partitions, not repeated current-state snapshots.
- `benchmark_price`: the required three indices by date window.

### Materialization axis

Reducers are keyed by dataset and canonical trade-date window. They wait for all declared source dependencies, aggregate them, then perform a single canonical read/write/read-back per partition. Source chunks stop at `STAGED`; only reducers can mark canonical completion.

The plan stores the universe once and references it by identity instead of copying thousands of codes into every chunk. Dependency keys and materialization IDs are explicit parts of the fingerprint.

### Preflight budgets

The estimator runs before constructing the complete chunk list or invoking provider/storage callbacks. For the default 5,000-stock, ten-year workload, the accepted design budgets are:

- at most 25,000 source plus materialization chunks;
- at most 16 MiB serialized plan JSON;
- at most 215,000 provider calls, including a conservative 200,000-call financial fallback;
- at most two canonical reads per dataset/trade-date partition during fresh apply;
- at most one reconciliation read per dataset/trade-date partition during completed resume.

Exceeding a configured hard budget blocks preflight with an explicit estimate and performs no provider, MinIO or canonical operation.

## 8. Compatibility and migration

V1 plans and manifests remain parseable for audit and same-version resume, but v2 execution requires a new run ID. V1 staging is not promoted automatically. Any future migration must revalidate schema, coverage, DQ, checksum and lineage and emit new v2 evidence.

Plan, chunk identity and manifest schema versions are bumped together. The executor rejects mixed-version dependencies with a configuration failure instead of guessing intent.

## Verification strategy

Every defect first receives a focused failing regression test. Tests cover live router-to-executor provenance for all three enabled Tushare datasets, structured failure preservation, truncated and complete-empty suspension evidence, READY-report recovery, dtype-independent empty `stock_basic` reconciliation, both ST half-open boundaries, announcement-date financial increments, shared market sidecars, v2 scale estimates and downstream firewalls.

After focused tests, the required gates are the complete local Python suite, `git diff --check`, Docker Python image build and suite, and Docker Maven suite. Main, push and tag actions are outside this repair implementation until the repaired branch has passed review and the user requests promotion.

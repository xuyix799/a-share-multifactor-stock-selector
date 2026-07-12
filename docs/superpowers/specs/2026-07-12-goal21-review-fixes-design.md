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

A successful live response must first be persisted as an immutable raw-provider landing object using the existing atomic Parquet writer, checksum calculation and read-back verification. The exact key shape is `raw/provider_landing/provider=<provider>/run_id=<run_id>/endpoint=<endpoint>/request=<request-hash>/response=<row-checksum>/evidence=<semantic-evidence-hash>/part.parquet`. The landing key is upstream of Goal 21 attempt staging and is therefore valid lineage; the candidate staging key itself must never be used as its own source. For `suspend_d`, the evidence hash binds the completeness, truncation, covered-date and pagination/terminal-page attributes as well as the rows.

Each provider call records a sanitized request identity, endpoint, requested scope, returned row count, raw landing key, checksum and read-back result. `HistoricalChunkFetchResult.source_keys` is populated only from successfully verified raw landing objects. `daily_price` carries every contributing source, including daily bars and the market-level `stk_limit` and `suspend_d` sidecars used to establish price-limit and suspension semantics.

The chunk-manifest v1 contract intentionally remains scalar: `source_key` is the first dataset payload source (before the calendar source). Complete ordered `source_keys` and sanitized `provider_calls` live in the immutable attempt report. `staging_attempt` links the mutable manifest back to that report, and apply-only resume copies the full provenance into its own immutable report. A successful live result without verified source keys is rejected.

## 2. Truthful provider failure audit

The executor snapshots the complete validated provider result before branching on `FETCHED`, `FAILED` or `BLOCKED`. The checkpoint and immutable attempt report retain:

- provider status and sanitized calls;
- actual and target schema;
- DQ, coverage and validation evidence;
- actual source keys, including an empty list;
- the provider's original failure category, retryability, exception type and message.

Only an exception raised before a structured provider result exists is classified by the executor. A new provider attempt starts with an empty audit envelope, so such an exception cannot inherit status/schema/DQ/coverage from an earlier attempt. Structured `FAILED` and `BLOCKED` results never write staging or canonical data and are not reclassified into a generic executor error. If staging was atomically written before its immediate read-back failed, the failed report still records that staging key/checksum/attempt so the orphan is auditable; it is not treated as verified until a later resume re-reads and checks it.

## 3. Suspension completeness contract

The CLI adapter does not create or overwrite completeness attributes. Negative evidence is sticky: `sample_truncated=true`, `coverage_complete=false`, an incomplete page sequence or a mismatched request scope always blocks the result.

Completeness must come from the provider boundary and identify the exact endpoint, trade date, market scope, page sequence and terminal-page evidence. A complete empty response is acceptable only when the same proof shows the exact full-market request completed. Otherwise absence from `suspend_d` cannot be interpreted as `is_paused=false`.

Market-level `stk_limit` and `suspend_d` responses are fetched once per trade date and shared by every requested stock subset through the same immutable source key. They are never repeated per code batch. Their in-memory cache is evicted outside the active date window, retaining v1 same-window reuse without accumulating a ten-year history in RAM.

## 4. READY-report crash recovery

At startup, before creating a new attempt, the executor checks for the previous immutable attempt report whenever the mutable manifest is stale or not final. A report is recoverable only if all of these match the current plan and manifest:

- schema and identity version;
- run ID, plan fingerprint, dataset, chunk ID and attempt number;
- requested provider/apply stages;
- `READY_TO_CHECKPOINT` state and checkpoint target state;
- scalar and ordered source provenance, staging and canonical keys;
- staging checksum and read-back evidence;
- canonical checksum/read-back evidence when apply was requested.

Provider-call item types, ordered target schema, staging row count, and canonical partition counter/write-flag invariants are also checked. After strict validation, the final manifest is reconstructed idempotently. For `COMPLETED`, canonical scope reconciliation is run again. Invalid or ambiguous READY reports fail closed and are never trusted as recovery state. READY recovery is attempted before a stale mutable manifest can force an unnecessary new apply-only attempt.

## 5. Historical scope correctness

Canonical apply replaces the requested logical scope rather than merely upserting incoming rows, so stale rows inside the same stock/date or interval scope cannot survive a successful rerun. Empty `stock_basic` completed-resume checks use the same negative-scope predicate as the apply path: if canonical data contains any code belonging to the chunk, the chunk is not exact regardless of DataFrame dtype. Stock membership is valid only when `list_date <= canonical trade_date` and `delist_date` is null or later than that date; a current snapshot cannot prove prior membership. ST rows reject current-snapshot markers case-insensitively.

ST intervals use one shared half-open overlap predicate:

```text
interval overlaps chunk iff start_date <= chunk_end
                          and (end_date is null or end_date > chunk_start)
```

Therefore an interval whose `end_date == chunk_start` is outside the chunk in staging validation, canonical reconciliation and conflict checks.

## 6. Financial announcement semantics

Financial source chunks are scoped by `announce_date`, not by `report_period`. A row is valid when its announcement falls inside the source chunk and `report_period <= announce_date`. A one-day `2024-06-03` increment may therefore contain a `2024-03-31` report announced on that day.

Materialization remains point-in-time: a canonical trade date may see only announcements on or before that date. Each ordered financial chunk carries the previous state, merges newly announced rows and reads/writes/verifies each canonical trade-date partition at most once per run. Cross-run carry requires an exact completed v2 predecessor manifest for the same immutable universe, a final terminal anchor with no pending tail, successful read-back, and a safe upstream source key that is neither `smoke/` nor `candidate/`.

The v2 CLI uses `--financial-announce-days` (default `31`). The legacy `--report-period-months 3` option remains parseable and positive-validated for v1 runbook compatibility, but v2 does not use or reinterpret it.

## 7. Plan/identity v2 and bounded execution

V2 uses one natural-axis logical chunk per historical scope. The same immutable chunk can stop at `STAGED` in provider-only mode or proceed through canonical materialization in combined/apply mode; there is no second class of reducer chunks.

### Source axes

- `daily_price`, `adj_factor` and `daily_basic`: full-market trade-date windows, filtered to the requested universe after fetch.
- `daily_price` sidecars: one shared `stk_limit` and one shared `suspend_d` fetch per trade date.
- `financial`: ordered announcement-date windows over the immutable universe, with one predecessor dependency per window after the first.
- `stock_basic`: historical master/snapshot source partitions, not repeated date-by-code Cartesian chunks.
- `st_history`: historical interval source partitions, not repeated current-state snapshots.
- `benchmark_price`: the required three indices by date window.

Each chunk carries a stable `materialization_id`. Financial dependency keys form a strict chain; non-financial chunks have no dependencies. The executor projects a validated chunk into the affected canonical partitions and performs a single read/write/read-back per partition. The plan stores the universe once and chunks reference `universe_id` instead of copying thousands of codes into every chunk. Dependency keys and materialization IDs are explicit parts of the fingerprint.

### Preflight budgets

The estimator runs before constructing the complete chunk list or invoking provider/storage callbacks, and the actual serialized plan size is checked again after construction. For 5,000 stocks from `2015-01-01` through `2024-12-31` with the CLI defaults (`250` codes, `31` date days and `31` announcement days), the conservative estimate is:

- `710` chunks, `15,056,320` plan bytes, `13,520` provider calls and `36,557` canonical reads;
- the actual all-seven plan has `592` chunks: `118` each for the five date/announcement-window datasets plus one `stock_basic` and one `st_history` chunk;
- the measured compact/pretty JSON is about `0.34/0.46 MiB`, below the hard `16 MiB` plan budget;
- configured hard ceilings remain `25,000` chunks, `215,000` provider calls and `40,000` canonical reads.

Exceeding a configured hard budget blocks preflight with an explicit estimate and performs no provider, MinIO or canonical operation.

## 8. Compatibility and migration

V1 plans and manifests remain parseable for audit and same-version resume, but v2 execution requires a new run ID. V1 staging is not promoted automatically. Any future migration must revalidate schema, coverage, DQ, checksum and lineage and emit new v2 evidence.

Plan, planner and chunk identity use explicit v2 versions. The existing `goal21.chunk_manifest.v1` envelope remains readable because its scalar fields did not change; complete plural provenance is retained by immutable attempt reports. The executor validates the supported version tuple and rejects mixed or non-canonical v2 dependency chains instead of guessing intent.

## Verification strategy

Every defect first receives a focused failing regression test. Tests cover live router-to-executor provenance for all three enabled Tushare datasets, structured failure preservation and attempt isolation, truncated and complete-empty suspension evidence bound to raw identity, READY-report recovery/tamper rejection, auditable orphan staging, exact canonical scope replacement, stock membership and ST historical semantics, both ST half-open boundaries, announcement-date financial increments and predecessor proof, bounded shared market sidecars, v2 scale estimates and downstream firewalls.

After focused tests, the required gates are the complete local Python suite, `git diff --check`, Docker Python image build and suite, and Docker Maven suite. Main, push and tag actions are outside this repair implementation until the repaired branch has passed review and the user requests promotion.

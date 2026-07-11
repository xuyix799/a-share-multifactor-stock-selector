# Goal 21 Resumable Historical Backfill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a deterministic, resumable and auditable seven-input historical backfill framework that safely handles five to ten years and one-day incremental updates.

**Architecture:** An immutable dataset-specific chunk plan drives a single-writer executor. Provider attempts land immutable staging, manifests checkpoint every chunk, canonical writes reuse existing schema/validation/upsert/atomic storage primitives, and resume trusts only checksum plus read-back evidence.

**Tech Stack:** Python 3.12, pandas/Parquet, argparse, existing local/MinIO atomic storage adapters, pytest, Docker Compose and Maven.

## Global Constraints

- Default mode is dry-run and must not construct or call a real provider.
- Provider calls require `--provider-call`; canonical writes require `--apply`.
- Cover exactly `stock_basic`, `daily_price`, `adj_factor`, `daily_basic`, `financial`, `st_history` and `benchmark_price` by default.
- Do not synthesize historical stock/ST/financial semantics when the provider cannot prove them.
- Do not invoke clean, universe, factor, selection or backtest code.
- Preserve `.qoder/` and stage only Goal 21 files.
- Final commit subject is `Add Goal 21 resumable historical backfill`.

---

### Task 1: Deterministic plan and control keys

**Files:**
- Create: `python-engine/src/stock_selector/data/historical_backfill.py`
- Test: `python-engine/tests/test_goal21_historical_backfill.py`

**Interfaces:**
- Produces `build_history_backfill_plan(...) -> dict`, `build_history_backfill_output_keys(run_id, chunks) -> dict`, stable `chunk_id` and `plan_fingerprint`.
- Consumes existing date and stock-code validators plus Goal 20 dataset/key constants.

- [ ] Write failing tests for exact five-year, ten-year and one-day plans, stable fingerprints, code versus universe input, invalid limits, full range coverage and dataset-specific strategy names.
- [ ] Run `pytest python-engine/tests/test_goal21_historical_backfill.py -q` and confirm failures are caused by the missing module/API.
- [ ] Implement normalized scope, stable JSON hashing, code/date/report-window splitters and safe output keys. Keep execution gates and timestamps out of the fingerprint.
- [ ] Re-run the focused tests and keep all plan assertions green.

### Task 2: Manifest, checksum and summary state machine

**Files:**
- Modify: `python-engine/src/stock_selector/data/historical_backfill.py`
- Create: `python-engine/src/stock_selector/data/historical_backfill_executor.py`
- Test: `python-engine/tests/test_goal21_historical_backfill_executor.py`

**Interfaces:**
- Produces `dataframe_checksum`, `build_chunk_manifest`, `summarize_chunk_manifests` and `classify_backfill_failure`.
- Manifests expose provider, DQ, coverage, validation, staging, canonical write/read-back, failure and state fields.

- [ ] Write failing tests for all required manifest fields, count conservation, completion rate, structured gaps, stable checksums, error taxonomy/retryability and token redaction.
- [ ] Verify the new tests fail for missing state-machine behavior.
- [ ] Implement deterministic checksums, sanitized failure records and a pure summary reducer.
- [ ] Re-run the focused tests and confirm the reducer reports every non-completed chunk as a gap.

### Task 3: Provider adapters and normalization

**Files:**
- Create: `python-engine/src/stock_selector/providers/historical_provider.py`
- Modify: `python-engine/src/stock_selector/data/historical_backfill.py`
- Test: `python-engine/tests/test_goal21_historical_backfill.py`

**Interfaces:**
- Produces `HistoricalChunkFetchResult` and `HistoricalProviderRouter.fetch_chunk(chunk)`.
- The executor also accepts an injected `fetch_chunk_fn` so fixtures never use network.

- [ ] Write failing parameterized tests proving the natural chunk axis for every dataset, trading-date coverage, financial `announce_date` filtering, stock list/delist boundaries, interval ST proof, exact benchmark index coverage, valid empty ST proof and rejection of current snapshot semantics.
- [ ] Write failing tests for empty, rate limit, permission, schema drift, transient and semantic-source-unavailable classifications.
- [ ] Implement fixture-compatible normalization using existing schema contracts/mappers and conservative real-provider routing. Reuse Goal 17 raw endpoint strategies; return structured blocked results for unproven stock/ST/financial sources.
- [ ] Re-run the focused tests; no test may enable a real provider or reveal credentials.

### Task 4: Resumable executor and canonical reconciliation

**Files:**
- Modify: `python-engine/src/stock_selector/data/historical_backfill.py`
- Test: `python-engine/tests/test_goal21_historical_backfill.py`

**Interfaces:**
- Produces `run_real_history_backfill(...) -> dict` with injected JSON/Parquet artifact readers/writers and canonical read/write functions.
- Reuses Goal 18 `_upsert_frame`, Goal 20 key columns and existing dataset validators.

- [ ] Write failing tests for dry-run, provider-only staging, apply-only staging reuse, combined apply/read-back, repeat idempotency and force without gate escalation.
- [ ] Write failing tests for completed-chunk skip, checksum mismatch retry, stale-running reconciliation, interruption at fetch/write/checkpoint, canonical-written-before-checkpoint recovery and isolated chunk failure.
- [ ] Verify every behavior first fails for the expected missing implementation.
- [ ] Implement per-attempt staging/report, checkpoint-last completion, serialized partition upsert, canonical subset read-back and continuation after ordinary chunk failure. Re-raise `KeyboardInterrupt` only after persisting `INTERRUPTED`.
- [ ] Re-run focused tests and inspect saved manifests for truthful partial-write state.

### Task 5: CLI, artifact-store wiring and documentation

**Files:**
- Modify: `python-engine/src/stock_selector/cli.py`
- Create: `python-engine/tests/test_goal21_historical_backfill_cli.py`
- Create: `docs/goal21_historical_backfill.md`
- Modify: `README.md`

**Interfaces:**
- Adds `run-real-history-backfill` with `--run-id`, `--start-date`, `--end-date`, mutually exclusive `--codes`/`--universe-key`, `--provider-call`, `--apply`, default-on `--resume`, `--no-resume`, `--force`, and only the three planner batch-size limits `--code-batch-size`, `--date-batch-days` and `--report-period-months`.

- [ ] Write failing CLI tests that assert all parser defaults, mutually exclusive universe inputs, no provider construction by default, independent gates, exit statuses, local atomic control artifacts and compact safe output.
- [ ] Verify the CLI tests fail because the command is absent.
- [ ] Wire the command to the existing local/MinIO JSON and Parquet helpers, create providers only after opt-in, and map configuration failures into chunk/provider status without exposing tokens.
- [ ] Document operations, object keys, resume/force behavior, real-provider capability blocks, one-day increment usage and the Goal 25 completeness boundary.
- [ ] Run all three Goal 21 test files and the Goal 20 regression file.

### Task 6: Requirement audit and repository verification

**Files:**
- Review every Goal 21 changed file; make no unrelated edits.

- [ ] Run focused Goal 21 and affected Goal 20/17/18 tests.
- [ ] Run `$env:PYTHONPATH='python-engine/src'; pytest python-engine/tests -q`.
- [ ] Run `git diff --check`.
- [ ] Run `docker compose build stock-python`.
- [ ] Run `docker compose run --rm stock-python pytest python-engine/tests -q`.
- [ ] Run `docker compose run --rm --no-deps stock-api mvn test -q`.
- [ ] Request an independent requirement and code-quality review; resolve every Critical/Important finding with a RED/GREEN regression test.

### Task 7: Narrow closeout and fast-forward promotion

**Files:**
- Stage only the reviewed Goal 21 file set.

- [ ] Inspect `git status --short`, `git diff --name-status`, `git diff --check` and `git diff --cached --name-status`; prove `.qoder/` is absent from the index.
- [ ] Commit exactly `Add Goal 21 resumable historical backfill` and push `codex/goal21-historical-backfill`.
- [ ] Switch to `main`, pull with `--ff-only`, and merge the feature branch with `git merge --ff-only`. Stop if fast-forward is impossible.
- [ ] Re-run Docker Python and Maven tests on merged `main`.
- [ ] Create `goal-21-historical-backfill-verified`, push `main` and the tag, fetch, and prove local/remote commit parity.
- [ ] Mark Goal 21 complete only after the requirement-by-requirement evidence audit passes.

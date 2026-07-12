# Goal 21 Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:test-driven-development` for every behavior change and `superpowers:verification-before-completion` before any success claim.

**Goal:** Repair the eight verified Goal 21 defects without weakening provenance, historical semantics, execution gates or downstream firewalls.

**Architecture:** Keep the verified v1 baseline readable, add a v2 natural-axis plan whose logical chunks carry their own materialization identity (with an ordered dependency chain only for financial), persist live provider responses as immutable verified raw lineage, and make recovery/audit decisions exclusively from validated evidence.

**Tech Stack:** Python 3.12, pandas/Parquet, pytest, existing local/MinIO atomic storage helpers, Docker Compose and Maven.

## Global constraints

- Preserve `?? .qoder/`; do not stage it.
- Do not call a real provider in tests.
- Keep `--provider-call` and `--apply` independent and opt-in.
- Never fabricate `source_keys`, suspension completeness, historical membership/ST rows or financial values.
- Do not invoke clean, factor, selection or backtest code.
- V1 identities remain parseable; changed semantics use v2 identities and a new run ID.
- Apply all source edits with `apply_patch`.

---

### Task 1: Establish the RED regression suite

**Files:**
- Create: `python-engine/tests/test_goal21_review_provider_regressions.py`
- Create: `python-engine/tests/test_goal21_review_executor_regressions.py`
- Create: `python-engine/tests/test_goal21_review_planner_v2.py`

- [x] Add a router-to-executor test for Tushare `daily_price`, `adj_factor` and `daily_basic` proving a successful live-style response can stage only after a real raw landing key is written and read back.
- [x] Add structured `FAILED` and `BLOCKED` result tests asserting provider calls, schemas, DQ, coverage, validation, source keys and the original failure survive in both manifest and immutable attempt report.
- [x] Add CLI suspension tests proving upstream `sample_truncated=true` and `coverage_complete=false` are sticky, incomplete subsets block, and a proven complete empty result is accepted.
- [x] Add a crash-window test that fails the final checkpoint after a READY report, then resumes apply-only without refetching or rewriting canonical data.
- [x] Add dtype-parameterized empty `stock_basic` completed-resume tests with polluted canonical data.
- [x] Add ST interval tests for `end_date == chunk_start` in staging and canonical reconciliation.
- [x] Add the `announce_date=2024-06-03`, `report_period=2024-03-31` financial increment test plus future-report rejection.
- [x] Add a v1 risk estimator test for the 315,118-chunk baseline without constructing it, and v2 budget/call/read-bound tests for 5,000 stocks over ten years.
- [x] Run each new file separately and confirm failures correspond to the eight defects, not fixture mistakes.

### Task 2: Preserve structured failure evidence

**Files:**
- Modify: `python-engine/src/stock_selector/data/historical_backfill_executor.py`
- Modify: `python-engine/src/stock_selector/data/historical_backfill.py`
- Test: `python-engine/tests/test_goal21_review_executor_regressions.py`

- [x] Move provider-result evidence extraction into one helper that runs immediately after result validation and before status branching.
- [x] Keep scalar `source_key` in the manifest and retain ordered `source_keys` plus `provider_calls` in immutable attempt reports linked by `staging_attempt`.
- [x] Persist the provider's canonical failure record unchanged for structured `FAILED`/`BLOCKED`; classify only raised exceptions without a result, with a fresh evidence envelope per provider attempt.
- [x] Prove failed chunks never write canonical data, and record any successfully written staging object before a later read-back failure.
- [x] Run the focused structured-failure tests to GREEN.

### Task 3: Recover READY reports and fix historical boundaries

**Files:**
- Modify: `python-engine/src/stock_selector/data/historical_backfill_executor.py`
- Test: `python-engine/tests/test_goal21_review_executor_regressions.py`

- [x] Add strict immutable attempt-report parsing and identity validation before starting a new attempt.
- [x] Reconstruct `STAGED`/`COMPLETED` manifests from a valid `READY_TO_CHECKPOINT` report, rechecking staging checksum and canonical exactness as applicable.
- [x] Fail closed on report identity, requested-stage, provenance type, schema, row-count, key, checksum, partition-counter or target-state mismatch.
- [x] Make empty `stock_basic` fast resume call the same negative-scope check as apply reconciliation.
- [x] Centralize ST half-open overlap and use `end_date > chunk_start` consistently in staging, canonical and conflict predicates.
- [x] Run READY, stock and ST regression tests, then the existing Goal 21 executor suite.

### Task 4: Add immutable live raw landing and suspension proof

**Files:**
- Modify: `python-engine/src/stock_selector/providers/historical_provider.py`
- Modify: `python-engine/src/stock_selector/providers/tushare_provider.py`
- Modify: `python-engine/src/stock_selector/cli.py`
- Modify: `python-engine/src/stock_selector/data/historical_backfill.py`
- Test: `python-engine/tests/test_goal21_review_provider_regressions.py`
- Test: `python-engine/tests/test_goal21_historical_backfill.py`
- Test: `python-engine/tests/test_goal21_historical_backfill_cli.py`

- [x] Inject a raw landing callback into the historical provider adapter; make it atomically write/read/checksum each sanitized live endpoint response under a deterministic immutable upstream key.
- [x] Populate `source_keys` only from verified raw landing records, order dataset payload before calendar lineage, and include every daily-price sidecar source.
- [x] Bind `suspend_d` completeness/pagination metadata into raw identity; reject row or semantic-evidence collisions and reuse exact duplicates idempotently.
- [x] Delete the CLI's unconditional suspension attrs update. Preserve negative attrs when concatenating or routing frames.
- [x] Make the Tushare boundary emit explicit page/request/terminal evidence for exact-date full-market suspension calls; if the provider cannot prove termination, return `BLOCKED`.
- [x] Cache `stk_limit` and `suspend_d` by endpoint/trade date, reuse them across code subsets, and evict entries outside the active date window.
- [x] Prove default dry-run constructs neither Tushare nor the raw landing writer.
- [x] Run all provider and CLI regression tests to GREEN.

### Task 5: Correct financial announcement semantics

**Files:**
- Modify: `python-engine/src/stock_selector/data/historical_backfill.py`
- Modify: `python-engine/src/stock_selector/providers/historical_provider.py`
- Modify: `python-engine/src/stock_selector/data/historical_backfill_executor.py`
- Test: `python-engine/tests/test_goal21_review_planner_v2.py`
- Test: `python-engine/tests/test_goal21_review_executor_regressions.py`

- [x] Introduce v2 `announce_date_start/end` source scope and remove report-period equality with the run date.
- [x] Validate announcement scope plus `report_period <= announce_date`; reject future reports and out-of-window announcements.
- [x] Build financial canonical partitions from previous state plus newly announced rows, preserving point-in-time visibility.
- [x] Group materialization so each financial canonical date is read/written/read back at most once per run.
- [x] Add `--financial-announce-days` for v2; retain `--report-period-months` as parsed but ignored v1 compatibility input.
- [x] Require exact same-universe completed predecessor/terminal evidence with safe non-smoke/non-candidate lineage for cross-run seed state.
- [x] Run the financial regressions and existing financial as-of tests to GREEN.

### Task 6: Implement bounded plan/identity v2

**Files:**
- Modify: `python-engine/src/stock_selector/data/historical_backfill.py`
- Modify: `python-engine/src/stock_selector/providers/historical_provider.py`
- Modify: `python-engine/src/stock_selector/data/historical_backfill_executor.py`
- Modify: `python-engine/src/stock_selector/cli.py`
- Test: `python-engine/tests/test_goal21_review_planner_v2.py`
- Test: `python-engine/tests/test_goal21_historical_backfill.py`
- Test: `python-engine/tests/test_goal21_historical_backfill_cli.py`

- [x] Add explicit v1 and v2 plan/planner/identity constants and dispatch validation by persisted version while retaining the v1 manifest envelope.
- [x] Store the normalized universe once; v2 chunks reference its identity instead of embedding every code batch repeatedly.
- [x] Generate market-date chunks for daily/adj-factor/daily-basic/benchmark, historical-scope chunks for stock/ST and announcement-date chunks for financial.
- [x] Give every logical chunk a stable `materialization_id`; use a strict predecessor dependency chain only for financial rather than creating separate reducer chunks.
- [x] Add a constant-space estimator for chunks, serialized bytes, provider calls and canonical reads before full plan construction, followed by an actual serialized-size gate.
- [x] Enforce configurable hard budgets before any provider, MinIO or canonical callback.
- [x] Keep v1 audit/same-version resume readable and reject mixed v1/v2 dependencies or v1 staging promotion.
- [x] Demonstrate the ten-year default estimates 710 chunks/13,520 calls and the actual all-seven plan contains 592 chunks, below all hard budgets.
- [x] Run planner v2, CLI and existing deterministic-plan tests to GREEN.

### Task 7: Documentation, compatibility and firewall audit

**Files:**
- Modify: `docs/goal21_historical_backfill.md`
- Modify: `README.md`
- Test: `python-engine/tests/test_goal21_historical_backfill_cli.py`

- [x] Document raw-provider lineage keys, suspension fail-closed rules, v2 axes/budgets, financial announcement semantics and v1 new-run migration.
- [x] Correct any claim that daily-price live history is usable without proven suspension completeness.
- [x] Re-run dry-run/no-network/provider/apply gate tests.
- [x] Re-run AST/import firewalls proving clean, factor, selection and backtest remain closed.

### Task 8: Full verification and independent review

**Files:**
- Review only Goal 21 repair files; make no unrelated changes.

- [x] Run the three new regression files.
- [x] Run all focused Goal 21 files and affected Goal 17/18/20 tests.
- [x] Run `$env:PYTHONPATH='python-engine/src'; pytest python-engine/tests -q`.
- [x] Run `git diff --check`.
- [x] Run `docker compose build stock-python`.
- [x] Run `docker compose run --rm stock-python pytest python-engine/tests -q`.
- [x] Run `docker compose run --rm --no-deps stock-api mvn test -q`.
- [x] Request independent requirement and code-quality reviews; resolve all Critical/Important findings with RED/GREEN evidence.
- [ ] Inspect `git status --short`, `git diff --name-status` and `git diff --cached --name-status`; prove `.qoder/` is not staged.
- [ ] Stop after a verified local repair commit. Do not push, merge main or retag without explicit user direction.

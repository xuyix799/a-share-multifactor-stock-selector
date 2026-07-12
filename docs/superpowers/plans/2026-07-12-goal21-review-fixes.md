# Goal 21 Review Fixes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:test-driven-development` for every behavior change and `superpowers:verification-before-completion` before any success claim.

**Goal:** Repair the eight verified Goal 21 defects without weakening provenance, historical semantics, execution gates or downstream firewalls.

**Architecture:** Keep the verified v1 baseline readable, add a v2 natural-axis source plan and dependency-driven materialization plan, persist live provider responses as immutable verified raw lineage, and make recovery/audit decisions exclusively from validated evidence.

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

- [ ] Add a router-to-executor test for Tushare `daily_price`, `adj_factor` and `daily_basic` proving a successful live-style response can stage only after a real raw landing key is written and read back.
- [ ] Add structured `FAILED` and `BLOCKED` result tests asserting provider calls, schemas, DQ, coverage, validation, source keys and the original failure survive in both manifest and immutable attempt report.
- [ ] Add CLI suspension tests proving upstream `sample_truncated=true` and `coverage_complete=false` are sticky, incomplete subsets block, and a proven complete empty result is accepted.
- [ ] Add a crash-window test that fails the final checkpoint after a READY report, then resumes apply-only without refetching or rewriting canonical data.
- [ ] Add dtype-parameterized empty `stock_basic` completed-resume tests with polluted canonical data.
- [ ] Add ST interval tests for `end_date == chunk_start` in staging and canonical reconciliation.
- [ ] Add the `announce_date=2024-06-03`, `report_period=2024-03-31` financial increment test plus future-report rejection.
- [ ] Add a v1 risk estimator test for the 315,118-chunk baseline without constructing it, and v2 budget/call/read-bound tests for 5,000 stocks over ten years.
- [ ] Run each new file separately and confirm failures correspond to the eight defects, not fixture mistakes.

### Task 2: Preserve structured failure evidence

**Files:**
- Modify: `python-engine/src/stock_selector/data/historical_backfill_executor.py`
- Modify: `python-engine/src/stock_selector/data/historical_backfill.py`
- Test: `python-engine/tests/test_goal21_review_executor_regressions.py`

- [ ] Move provider-result evidence extraction into one helper that runs immediately after result validation and before status branching.
- [ ] Extend manifest/attempt evidence with ordered `source_keys` while retaining readable scalar `source_key` compatibility.
- [ ] Persist the provider's canonical failure record unchanged for structured `FAILED`/`BLOCKED`; classify only raised exceptions without a result.
- [ ] Prove failed chunks never write staging/canonical data and independent chunks continue.
- [ ] Run the focused structured-failure tests to GREEN.

### Task 3: Recover READY reports and fix historical boundaries

**Files:**
- Modify: `python-engine/src/stock_selector/data/historical_backfill_executor.py`
- Test: `python-engine/tests/test_goal21_review_executor_regressions.py`

- [ ] Add strict immutable attempt-report parsing and identity validation before starting a new attempt.
- [ ] Reconstruct `STAGED`/`COMPLETED` manifests from a valid `READY_TO_CHECKPOINT` report, rechecking staging checksum and canonical exactness as applicable.
- [ ] Fail closed on report identity, requested-stage, key, checksum or target-state mismatch.
- [ ] Make empty `stock_basic` fast resume call the same negative-scope check as apply reconciliation.
- [ ] Centralize ST half-open overlap and use `end_date > chunk_start` consistently in staging, canonical and conflict predicates.
- [ ] Run READY, stock and ST regression tests, then the existing Goal 21 executor suite.

### Task 4: Add immutable live raw landing and suspension proof

**Files:**
- Modify: `python-engine/src/stock_selector/providers/historical_provider.py`
- Modify: `python-engine/src/stock_selector/providers/tushare_provider.py`
- Modify: `python-engine/src/stock_selector/cli.py`
- Modify: `python-engine/src/stock_selector/data/historical_backfill.py`
- Test: `python-engine/tests/test_goal21_review_provider_regressions.py`
- Test: `python-engine/tests/test_goal21_historical_backfill.py`
- Test: `python-engine/tests/test_goal21_historical_backfill_cli.py`

- [ ] Inject a raw landing callback into the historical provider adapter; make it atomically write/read/checksum each sanitized live endpoint response under a deterministic immutable upstream key.
- [ ] Populate `source_keys` only from verified raw landing records and include every daily-price sidecar source.
- [ ] Reject collisions whose existing raw object checksum differs; reuse exact duplicates idempotently.
- [ ] Delete the CLI's unconditional suspension attrs update. Preserve negative attrs when concatenating or routing frames.
- [ ] Make the Tushare boundary emit explicit page/request/terminal evidence for exact-date full-market suspension calls; if the provider cannot prove termination, return `BLOCKED`.
- [ ] Cache `stk_limit` and `suspend_d` by endpoint/trade date so code subsets reuse one verified response.
- [ ] Prove default dry-run constructs neither Tushare nor the raw landing writer.
- [ ] Run all provider and CLI regression tests to GREEN.

### Task 5: Correct financial announcement semantics

**Files:**
- Modify: `python-engine/src/stock_selector/data/historical_backfill.py`
- Modify: `python-engine/src/stock_selector/providers/historical_provider.py`
- Modify: `python-engine/src/stock_selector/data/historical_backfill_executor.py`
- Test: `python-engine/tests/test_goal21_review_planner_v2.py`
- Test: `python-engine/tests/test_goal21_review_executor_regressions.py`

- [ ] Introduce v2 `announce_date_start/end` source scope and remove report-period equality with the run date.
- [ ] Validate announcement scope plus `report_period <= announce_date`; reject future reports and out-of-window announcements.
- [ ] Build financial canonical partitions from previous state plus newly announced rows, preserving point-in-time visibility.
- [ ] Group materialization so each financial canonical date is read/written/read back at most once per run.
- [ ] Add `--financial-announce-months` for v2 without silently changing v1 `--report-period-months`.
- [ ] Run the financial regressions and existing financial as-of tests to GREEN.

### Task 6: Implement bounded plan/identity v2

**Files:**
- Modify: `python-engine/src/stock_selector/data/historical_backfill.py`
- Modify: `python-engine/src/stock_selector/providers/historical_provider.py`
- Modify: `python-engine/src/stock_selector/data/historical_backfill_executor.py`
- Modify: `python-engine/src/stock_selector/cli.py`
- Test: `python-engine/tests/test_goal21_review_planner_v2.py`
- Test: `python-engine/tests/test_goal21_historical_backfill.py`
- Test: `python-engine/tests/test_goal21_historical_backfill_cli.py`

- [ ] Add explicit v1 and v2 schema/identity constants and dispatch validation by persisted version.
- [ ] Store the normalized universe once; v2 chunks reference its identity instead of embedding every code batch repeatedly.
- [ ] Generate market-date source chunks for daily/adj-factor/daily-basic, historical-master/interval chunks for stock/ST and three-index date chunks for benchmark.
- [ ] Add materialization chunks with declared source dependencies and stable `materialization_id` values.
- [ ] Add a constant-space estimator that calculates source chunks, reducer chunks, serialized-byte estimate, provider-call upper bound and canonical-read upper bound before full plan construction.
- [ ] Enforce configurable hard budgets before any provider, MinIO or canonical callback.
- [ ] Keep v1 audit/same-version resume readable and reject mixed v1/v2 dependencies or v1 staging promotion.
- [ ] Demonstrate the ten-year default stays within 25,000 chunks, 16 MiB plan and 215,000 provider calls, and market-level calls do not multiply by code-batch count.
- [ ] Run planner v2, CLI and existing deterministic-plan tests to GREEN.

### Task 7: Documentation, compatibility and firewall audit

**Files:**
- Modify: `docs/goal21_historical_backfill.md`
- Modify: `README.md`
- Test: `python-engine/tests/test_goal21_historical_backfill_cli.py`

- [ ] Document raw-provider lineage keys, suspension fail-closed rules, v2 axes/budgets, financial announcement semantics and v1 new-run migration.
- [ ] Correct any claim that daily-price live history is usable without proven suspension completeness.
- [ ] Re-run dry-run/no-network/provider/apply gate tests.
- [ ] Re-run AST/import firewalls proving clean, factor, selection and backtest remain closed.

### Task 8: Full verification and independent review

**Files:**
- Review only Goal 21 repair files; make no unrelated changes.

- [ ] Run the three new regression files.
- [ ] Run all focused Goal 21 files and affected Goal 17/18/20 tests.
- [ ] Run `$env:PYTHONPATH='python-engine/src'; pytest python-engine/tests -q`.
- [ ] Run `git diff --check`.
- [ ] Run `docker compose build stock-python`.
- [ ] Run `docker compose run --rm stock-python pytest python-engine/tests -q`.
- [ ] Run `docker compose run --rm --no-deps stock-api mvn test -q`.
- [ ] Request independent requirement and code-quality reviews; resolve all Critical/Important findings with RED/GREEN evidence.
- [ ] Inspect `git status --short`, `git diff --name-status` and `git diff --cached --name-status`; prove `.qoder/` is not staged.
- [ ] Stop after a verified local repair commit. Do not push, merge main or retag without explicit user direction.

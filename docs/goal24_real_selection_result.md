# Goal 24 Real Selection Result

Goal 24 adds a separately gated real `selection_result` range pipeline on top
of Goal 23. It consumes only explicitly supplied, completed Goal 23
publications and the exact Goal 22 publications recorded in their lineage. It
does not call a provider, run a backtest, call an LLM, add an API/page/scheduler
or perform automatic trading.

## Command and write gates

Default plan-only dry-run:

```powershell
docker compose run --rm stock-python python -m stock_selector.cli run-real-selection-range `
  --run-id <goal24-run-id> `
  --start-date 2024-06-28 `
  --end-date 2024-06-28 `
  --selection-dates 2024-06-28 `
  --rebalance-mode monthly `
  --goal23-manifest-key candidate/real_factor_daily/run_id=<goal23-run-id>/manifest.json
```

Repeat `--goal23-manifest-key` when the explicit selection dates are covered by
several Goal 23 ranges. The caller supplies every selection date. Goal 24 does
not scan raw data, discover a trading calendar or infer future rebalance dates.

The flags are:

| Flag | Default | Contract |
| --- | --- | --- |
| `--run-id` | required | Immutable Goal 24 control-plane identity. |
| `--start-date` / `--end-date` | required | Requested range. |
| `--selection-dates` | required | Explicit comma-separated selection dates. |
| `--rebalance-mode` | required | `monthly` or `quarterly`; part of publication identity. |
| `--goal23-manifest-key` | required, repeatable | Explicit trusted Goal 23 manifests. |
| `--apply` | off | Permits generation, commit and PostgreSQL summary writes. |
| `--resume` / `--no-resume` | resume on | Reuse exact publication state or start a new attempt. |
| `--force` | off | Recompute; never enables `--apply`. |

Dry-run validates the complete lineage and calculates the result in memory. It
does not write MinIO/local Parquet, PostgreSQL, generation, commit, DQ report
or Goal 24 manifest. Its JSON response contains the deterministic plan, input
manifest identities, selection dates, rebalance mode, target logical/commit
keys and the calculated immutable generation key. There is no
`--provider-call` option.

Apply is the only mode that persists Goal 24 artifacts:

```powershell
docker compose run --rm stock-python python -m stock_selector.cli run-real-selection-range `
  --run-id <goal24-run-id> `
  --start-date 2024-06-28 `
  --end-date 2024-06-28 `
  --selection-dates 2024-06-28 `
  --rebalance-mode quarterly `
  --goal23-manifest-key candidate/real_factor_daily/run_id=<goal23-run-id>/manifest.json `
  --apply
```

`--force`, `--resume` and `--no-resume` do not open the write gate.

## Trusted-input and fail-closed contract

For every selection date, Goal 24 verifies:

1. The Goal 23 manifest path, run identity, plan fingerprint, status counts,
   explicit date mapping, factor configuration and downstream firewalls.
2. A completed Goal 23 daily DQ report with no blockers or failure.
3. The daily report's Goal 23 output key, generation, checksum, row count,
   read-back result and factor-contract audit.
4. The Goal 23 commit marker and its exact agreement with the manifest, DQ,
   input/config fingerprints and immutable generation mapping.
5. The generation's exact physical Arrow schema before pandas normalization,
   followed by semantic validation, row count and checksum validation.
6. A truthful per-date Goal 23 effective-factor count of at least 15.
7. The target date's embedded Goal 22 manifest, daily DQ, date commit,
   generation id and checksums.
8. The Goal 22 `risk_filter`, `eligible_universe` and
   `factor_input_table` generations, including their exact committed keys,
   row counts and checksums. Their Parquet/Arrow schema is inspected before
   pandas: columns are exact, text/date fields must be Arrow `string`, Boolean
   fields must be Arrow `bool`, and numeric fields must use a 64-bit Arrow
   numeric type. The observed physical schema and its fingerprint are retained
   in Goal 24 lineage evidence.
9. Exact stock/date key-set equality among `factor_daily`,
   `eligible_universe` and `factor_input_table`.
10. A real Boolean, explicitly eligible `risk_filter` record for every
    eligible stock. Missing risk rows are blocked and never filled with an
    empty or safe default.

Goal 24 never reads a legacy mutable
`processed/<dataset>/trade_date=.../part.parquet` as trusted input. A direct
Goal 23 or Goal 22 generation is not sufficient without its matching commit,
manifest and DQ evidence. Smoke, candidate, staging, uncommitted, mismatched
date/run/lineage and checksum-synchronized but incomplete evidence all fail
closed before a canonical Goal 24 output is created.

## Filter, score, rank and explanation contract

Filtering precedes scoring:

```text
Goal 22 committed risk_filter / eligible_universe / factor_input_table
  + Goal 23 committed factor_daily
  -> exact key and eligibility gates
  -> existing rule-based selection builder
  -> deterministic Top50
```

The frozen first-version weights come from `config/factor_weights.yaml`:

```text
quality_score   0.30
growth_score    0.25
valuation_score 0.20
industry_score  0.15
trend_score     0.10
```

Every weight must be finite, non-negative and the sum must be one. Goal 24 also
requires the exact first-version values and an explicitly configured built-in
integer `top_n=50`; values such as `50.0`, `50.9`, `"50"` and `true` are
rejected rather than coerced. The Goal 23 weights/null policy must match the
frozen Goal 24 configuration.

For each output row:

- `total_score` is recomputed from the five score columns and frozen weights;
- ordering is `total_score DESC, stock_code ASC`;
- `rank` is continuous from one;
- no more than 50 rows are emitted and a smaller eligible set is not padded;
- `risk_level`, `reason` and `suggestion` are recomputed with the existing
  deterministic rule engine;
- forbidden return promises, target-price language, automatic trading and
  automatic order language are rejected;
- no LLM is imported or called.

Goal 24 V1 does not publish `PASS_EMPTY`. Under the trusted-input contract an
empty Goal 23 `factor_daily` has zero effective factors and is blocked by the
minimum-15 gate. After that gate, exact factor/eligible/factor-input key sets
and explicit `risk_filter.is_eligible=true` coverage leave no additional
filter that could legally reduce a non-empty input to zero. A zero-row result
is therefore rejected instead of advertising an unreachable publication
capability. This does not change unrelated provider interfaces that have
their own `PASS_EMPTY` semantics.

## Exact selection_result schema

The published columns remain:

```text
stock_code        string
trade_date        string
industry          string
market_type       string
quality_score     double
growth_score      double
valuation_score   double
trend_score       double
industry_score    double
total_score       double
risk_level        string
rank              int64
suggestion        string
reason            string
exclude_reasons   string
risk_flags        string
```

Readers inspect Parquet/Arrow types before calling `pandas.read_parquet`.
`date32`, timestamp, dictionary-encoded text, binary text, float32, integer
score columns and non-int64 rank columns are rejected even when pandas could
coerce their values.

`rebalance_mode` is intentionally not added to the frozen row schema. It is
part of the object path, commit identity, DQ identity and PostgreSQL summary
identity.

## Immutable generation and atomic commit

Apply control artifacts:

```text
candidate/real_selection_result/run_id=<run-id>/manifest.json
candidate/real_selection_result/run_id=<run-id>/trade_date=<date>/rebalance_mode=<mode>/dq_report.json
```

Immutable data:

```text
processed/selection_result/trade_date=<date>/rebalance_mode=<mode>/generation=<sha256>/part.parquet
```

Atomic publication marker:

```text
processed/_goal24_selection_commits/trade_date=<date>/rebalance_mode=<mode>/commit.json
```

Publication order is:

1. validate Goal 23 and Goal 22 lineage;
2. build the result in memory;
3. write or reuse the deterministic immutable generation;
4. read it back and validate exact Arrow schema, row count, checksum, Top50,
   sort, ranks, score calculation and rule explanations;
5. publish the commit last;
6. read the committed result back through the commit;
7. update the matching PostgreSQL summary.

The generation identity is derived from the exact input lineage fingerprint,
frozen config, date, rebalance mode, row count, output checksum and schema
fingerprint. An existing generation with the same identity but different
content is never overwritten, including with `--force`.

The canonical commit key is collision-protected. If it already contains a
valid publication whose lineage or content differs from the current attempt,
the date is `BLOCKED` before any generation, commit or PostgreSQL write. An
invalid existing canonical commit is also blocked and is never replaced in
place. The final writer is itself atomic create-only: the local backend links a
fully written and fsynced temporary file into the canonical path without
replacement, while MinIO uses a conditional PUT with `If-None-Match: *`. If
another writer wins after the final read, the losing attempt re-reads and
validates that commit; an exact compatible publication is reused, while an
invalid or incompatible publication makes the date `BLOCKED` and remains
untouched. `--force` cannot bypass any of these conditions, so correctness does
not depend on a read-then-write timing window.

One failed date does not prevent other explicit dates from completing. The
run-id plan fingerprint rejects date, mode, config and input-manifest scope
drift.

## PostgreSQL summary and recovery

`selection_snapshot` identity is `(trade_date, rebalance_mode)`. Replacement
deletes/upserts only the matching pair, so monthly and quarterly summaries for
the same date coexist.

The idempotent schema migration:

- adds/fills `rebalance_mode` for older local volumes;
- keeps legacy rows as `daily`;
- removes duplicate rows for the same date/mode while retaining the newest;
- creates a unique `(trade_date, rebalance_mode)` index;
- is safe to run repeatedly through `init-db` or commands that initialize the
  schema.

If the PostgreSQL write or read-back fails after a commit, the object-store
commit remains the publication truth and the date becomes
`DATABASE_PENDING`. A later exact `--resume` reuses the generation and commit
and repairs only the database summary. A fully matching snapshot is left
unchanged.

Spring Boot remains a PostgreSQL-only query service. Goal 24 does not add an
endpoint and never makes Spring read MinIO/Parquet or perform scoring.

## DQ evidence and boundaries

Each apply DQ report records the run/date/mode, Goal 23 and Goal 22 lineage,
manifest/DQ/commit/generation keys and checksums, input row counts, schema
fingerprints, effective-factor count, key-set/risk coverage, frozen config and
fingerprint, pre/post filter counts, TopN, score statistics, deterministic
checks, output generation/commit/checksum/read-back, snapshot state and one of
`PASS` or `BLOCKED`.

Every plan, report and commit keeps these firewalls closed:

```text
provider_call      = false
backtest           = false
llm                = false
api_page_scheduler = false
auto_trading       = false
```

The legacy mock/offline `build-selection`, its mutable single-date path,
legacy validator and backtest entry remain available and retain their default
behavior. Goal 24 does not run real data, call a provider, run real selection
with `--apply` during tests, run a backtest or start Goal 25.

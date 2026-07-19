# Goal 23 Real Factor Daily Range

Goal 23 adds a range pipeline that computes real `factor_daily` only from
explicitly supplied, atomically published Goal 22 results. It does not call a
provider, change Goal 20/21/22 canonical semantics, compute `total_score`, enter
`selection_result`, run a backtest, call an LLM or start Goal 24.

## Command and gates

Default dry-run:

```powershell
docker compose run --rm stock-python python -m stock_selector.cli run-real-factor-range `
  --run-id <run_id> `
  --start-date 2024-01-02 `
  --end-date 2024-06-28 `
  --trade-dates 2024-06-28 `
  --goal22-manifest-key candidate/real_clean_universe/run_id=<goal22_run_id>/manifest.json
```

Repeat `--goal22-manifest-key` to provide older trusted Goal 22 dates needed by
20/60/120-day and three-year windows. The target dates are always explicit;
Goal 23 never discovers dates from raw partitions.

The complete execution flags are:

| Flag | Default | Contract |
| --- | --- | --- |
| `--run-id` | required | Safe immutable Goal 23 run identity. |
| `--start-date` / `--end-date` | required | Requested target range. |
| `--trade-dates` | required | Explicit comma-separated target trading dates. |
| `--goal22-manifest-key` | required, repeatable | Explicit Goal 22 range manifests; no search or fallback. |
| `--apply` | off | Permit Goal 23 `processed/factor_daily` generation and commit writes. |
| `--resume` / `--no-resume` | resume on | Reuse an exact committed result, or request a fresh attempt. |
| `--force` | off | Recompute; it never enables `--apply`. |

`--force`, `--resume` and `--no-resume` do not open the processed-write gate.
Dry-run performs trusted-input validation and factor calculation, then writes
only the range manifest and per-date DQ report.

## Trusted Goal 22 input contract

For every target date, Goal 23 requires at least one supplied Goal 22 manifest
whose per-date state is `COMPLETED`. It validates:

- the Goal 22 manifest schema, identity, immutable plan fingerprint, exact
  output-key map, per-date state and closed downstream firewalls;
- the per-date Goal 22 DQ report, its Goal 20 receipt lineage, all seven input
  version records, output records and committed state;
- the date commit marker, its run/plan/input fingerprints and complete five
  output mappings;
- the exact committed generation checksum for the three consumed datasets:
  `adjusted_price`, `clean_daily_snapshot` and `factor_input_table`;
- the exact `benchmark_price` canonical object version and checksum bound by
  the Goal 20 lineage embedded in the Goal 22 plan.

Goal 23 never reads a direct legacy
`processed/<dataset>/trade_date=.../part.parquet` object. A generation object is
usable only through a valid Goal 22 commit marker. It does not search raw data,
guess dates, substitute a smoke/candidate object or accept an unbound
`part.parquet`.

All supplied completed Goal 22 dates at or before a target date form that
target's trusted history. A missing, changed, uncommitted or contradictory
artifact blocks the affected target. Dates after the target are never read.
Having fewer than the observations required by a factor is different from
lineage drift: a short but fully trusted history is valid, and long-window
factors remain null.

## No-future-data and history policy

- Price, industry and benchmark history is restricted to
  `history.trade_date <= target_trade_date`.
- Financial values arrive only through Goal 22's already as-of-clean
  `factor_input_table` / `clean_daily_snapshot`; Goal 23 does not reopen raw
  financial partitions.
- 20/60/120-day returns and moving averages stay null until their required
  observation counts exist.
- Three-year PE/PB percentiles in this real path require at least 720 prior
  non-null observations and history reaching to within 31 calendar days of the
  three-year boundary. Otherwise they stay null.
- Missing history is never filled with a later price, valuation or financial
  row.

The per-date DQ report records trusted dates, stock-level price and valuation
observation counts, benchmark counts, earliest valuation dates and which
windows remain insufficient.

## Factor contract audit

Goal 23 reuses the existing factor builder, factor score builder and factor
validator. It validates `config/factor_weights.yaml`, including that the five
score weights are finite and sum to 1, freezes the weights/null policy/neutral
score into the run plan, and keeps `total_score` out of `factor_daily`.

With sufficient trusted history, the current schema can calculate 23 effective
base factors:

- Quality (3): `quality_roe`, `quality_gross_margin`,
  `quality_debt_ratio`.
- Growth (2): `growth_revenue_yoy`, `growth_net_profit_yoy`.
- Valuation (5): `valuation_pe_ttm`, `valuation_pb`,
  `valuation_ps_ttm`, `valuation_pe_percentile_3y`,
  `valuation_pb_percentile_3y`.
- Trend (7): `trend_ret_20d`, `trend_ret_60d`, `trend_ret_120d`,
  `trend_ma20`, `trend_ma60`, `trend_ma120`,
  `trend_price_ma60_ratio`.
- Industry (4): `industry_ret_60d`, `industry_ret_120d`,
  `industry_strength_60d`, `industry_strength_120d`.
- Liquidity (2): `liquidity_amount`, `liquidity_turnover_rate`.

`quality_cashflow_profit_ratio` is an explicit all-null placeholder because the
trusted input has operating cash flow but no reliable absolute net-profit
denominator. It is not filled with zero, does not enter `quality_score`, and is
not counted as an effective factor.

Each date records every factor's non-null count, missing rate, all-null flag,
effective-factor list/count and whether that date reaches the first-version
minimum of 15. The implementation has 23-factor capability and therefore
meets the first-version capability threshold when the required history is
available. A short or empty real scope can have fewer than 15 effective factors
and is reported truthfully; Goal 23 does not manufacture values to make the
count pass.

Every non-null base factor must be finite. `NaN` remains the explicit missing
representation, while `+Inf` and `-Inf` block the date before publication and
cannot enter the effective-factor count.

Generation read-back validates the physical Parquet/Arrow schema before pandas
normalization. `stock_code`, `trade_date`, `industry`, and `market_type` must
each have the exact Arrow `string` type; date32, timestamp, dictionary, binary,
and other physical types are rejected. Every numeric factor/score column must
have the exact Arrow `double` type. The storage-callback fallback likewise
requires string-valued pandas text columns and 64-bit floating numeric columns
before `.astype("string")` or `pd.to_numeric` can run.

The current neutral null-score policy remains:

- a score dimension uses available component factors;
- when every component of a score dimension is missing, its score is the
  configured neutral score (currently 50);
- missing rates remain visible in DQ rather than being hidden by the score.

### Deferred gaps

These plan factors require upstream fields or historical semantics not present
in the trusted Goal 22 contract and remain deferred to Goal 25:

- `roic`: invested-capital components are absent;
- `net_margin`: absolute revenue and net-profit values are absent;
- `cashflow_profit_ratio`: reliable absolute net profit is absent;
- `fcf_yield`: free cash flow and the required denominator are absent;
- `revenue_cagr_3y`, `profit_cagr_3y`, `growth_stability`: comparable
  multi-period absolute revenue/profit series are absent;
- `earnings_revision`: forecast/revision history is absent;
- industry profit-growth change: trustworthy historical industry financial
  aggregates are absent.

The plan also mentions derivable fields such as `high_60d_ratio`, volume moving
averages and stock-within-industry rank. They do not require a Goal 20/21/22
schema expansion, but they are not silently added to the frozen current
`factor_daily` schema in this Goal.

`operating_cashflow` is already available in the trusted Goal 22
`factor_input_table` and is therefore not a Goal 25 upstream-data gap. The
current frozen `factor_daily` schema does not expose it as a standalone factor,
so it is not included in the 23 effective-factor capability count. Adding it
requires an explicit future `factor_daily` schema/weight contract revision, not
an expansion of the Goal 20/21/22 canonical input schema.

## Outputs and atomic publication

Control artifacts:

```text
candidate/real_factor_daily/run_id=<run_id>/manifest.json
candidate/real_factor_daily/run_id=<run_id>/trade_date=YYYY-MM-DD/dq_report.json
```

With explicit `--apply`, the immutable data object is:

```text
processed/factor_daily/trade_date=YYYY-MM-DD/generation=<sha256>/part.parquet
```

Only after schema, row count, checksum and generation read-back pass does Goal
23 atomically publish:

```text
processed/_goal23_factor_commits/trade_date=YYYY-MM-DD/commit.json
```

The Goal 23 reader resolves `factor_daily` only through that commit marker and
revalidates its generation and exact physical schema. A direct generation
object is not a published result. A schema-complete empty Goal 22
`factor_input_table` produces a schema-complete, zero-row `factor_daily`; no
stock or factor row is fabricated. Physical numeric values are parsed strictly;
an invalid value is rejected rather than coerced into a null that could hide
generation drift.

## Resume, status and concurrency

An existing date is reused only when run plan, Goal 22 lineage, factor config,
input fingerprint, output row count/checksum, commit mapping and committed
generation read-back all still match. Input drift blocks reuse. A generation
write/read-back failure does not publish a new commit, and another date in the
range continues independently. If the deterministic generation key already
exists with any schema or checksum mismatch, the date fails closed and the
object is never overwritten.

Range states are:

```text
READY_FOR_APPLY
COMPLETED
PARTIAL
BLOCKED
FAILED
```

Every date records its own state, attempt, Goal 22 lineage, history coverage,
factor missing rates, output checksum and failure evidence.

Do not run two Goal 23 processes with the same `run_id`, and do not run
concurrent `--apply` processes with overlapping dates. The immutable generation
plus atomic commit prevents partial publication, but the commit marker is not
a cross-process compare-and-swap lock.

All manifests and DQ reports explicitly keep:

```text
selection_result = false
backtest         = false
llm              = false
provider_call    = false
```

Goal 23 completion does not start Goal 24. Goal 24 may begin only after an
independent Goal 23 acceptance pass.

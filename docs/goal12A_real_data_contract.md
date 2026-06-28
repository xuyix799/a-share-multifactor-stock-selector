# Goal 12A Real Data Contract

Goal 12A freezes the contract that real data must satisfy before it can enter the standard layer. It does not add a real provider mainline, real backtest, factor computation, scoring, frontend work, LLM integration, or Spring Boot responsibility.

## Layer Responsibilities

| Layer | Responsibility | Explicitly not allowed |
| --- | --- | --- |
| `daily_price_raw` | Real provider stock quote landing layer. It may contain incomplete provider fields such as OHLCV, close, amount, or turnover. It is only for smoke checks, field capability checks, and source evaluation. | Must not feed `clean_daily_snapshot`, `factor_input_table`, `factor_daily`, `selection_result`, or standard backtests. It is not tradable quote data. |
| `trading_calendar` | Exchange calendar with fields such as `trade_date`, `exchange`, and `is_open`. It answers whether an exchange is open on a date. | Must not encode stock suspension, limit prices, adjusted prices, or quote prices. |
| `trading_limit` | Per-stock daily limit-up and limit-down source, at minimum expressing `stock_code`, `trade_date`, `limit_up`, `limit_down`, `provider`, and `dq_level`. It is required before a stock can enter strict tradable mode. | Must not be inferred from OHLC or close prices. Missing values block strict tradable use. |
| `suspension_status` | Per-stock daily suspension source, at minimum expressing `stock_code`, `trade_date`, `is_paused`, `provider`, and `dq_level`. It is required before a stock can enter strict tradable mode. | Must not be crudely inferred from `volume = 0`. Missing values block strict tradable use. |
| `daily_price` | Standard tradable quote layer. It must contain `stock_code`, `trade_date`, `open`, `high`, `low`, `close`, `volume`, `amount`, `pre_close`, `is_paused`, `limit_up`, and `limit_down`, and must pass the existing schema contract and validator. | Must not accept DQ1 or DQ2 data, raw provider rows, smoke rows, or fabricated `limit_up`, `limit_down`, or `is_paused`. |

`daily_price` remains the only price input for `clean_daily_snapshot`, `factor_input_table`, `factor_daily`, `selection_result`, and standard backtests. AKShare and Baostock stock daily data may only land in `daily_price_raw` or `daily_price_raw_smoke` until they have trustworthy `limit_up`, `limit_down`, and `is_paused` sources.

## Data Quality Levels

| Level | Meaning | Allowed use | Forbidden use |
| --- | --- | --- | --- |
| `DQ0` Mock / Synthetic | Artificial or mock data for the local system loop. It does not represent the real market. | Unit tests, integration tests, mock backtests, mock `selection_result`. | Real data analysis or real investment judgment. |
| `DQ1` Raw Provider Smoke | Real provider data with incomplete fields. It proves access, parsing, and landing only. | `daily_price_raw`, `daily_price_raw_smoke`, provider capability matrix, field coverage checks. | `daily_price`, `clean_daily_snapshot`, `factor_input_table`, `factor_daily`, `selection_result`, strict tradable backtest. |
| `DQ2` Price-only Diagnostic | Stable real price series for non-tradable diagnostics. | `benchmark_price`, `price_only_diagnostic`, source connectivity checks, price sequence checks. | `daily_price`, stock standard backtests, `selection_result`, tradable claims. |
| `DQ3` Strict Tradable Candidate | Complete tradable quote candidate with OHLCV, `pre_close`, `is_paused`, `limit_up`, `limit_down`, schema validation, missing-value checks, and calendar consistency. | `daily_price`, clean layer, factor inputs, factors, strict tradable backtests. | Use without provider, ingestion batch, and `dq_level` audit fields. Downgrade if provenance is not traceable. |
| `DQ4` Production-grade Reconciled | DQ3 plus multi-source reconciliation, adjustment-basis audit, missing-data audit, and idempotent import checks. | All DQ3 uses, long-term real research, production-grade backtests, stable metric evaluation. | Use after reconciliation evidence becomes stale or untraceable. |

## Backtest Modes

### `strict_tradable_required`

This is the default standard mode for tradable backtests. It aligns with the current backtest core semantics: T+1 execution, raw open feasibility checks, suspension blocks, limit-up buy blocks, and limit-down sell blocks.

Allowed:

- Read only `daily_price`.
- Accept only `DQ3` or `DQ4`.
- Require `pre_close`, `is_paused`, `limit_up`, and `limit_down`.
- Enforce schema validation and trading calendar filtering.

Forbidden:

- `daily_price_raw`, `daily_price_raw_smoke`, `benchmark_price`, AKShare stock raw rows, Baostock stock raw rows.
- Auto-filled fields.
- Limit prices inferred from close, high, low, or OHLC.
- Suspension inferred from `volume = 0`.

### `price_only_diagnostic`

This is a non-tradable diagnostic mode. It is only for price sequence checks, benchmark comparisons, and provider connectivity validation.

Allowed:

- `DQ2` `benchmark_price`.
- AKShare benchmark price as diagnostic benchmark data.
- Output labeled `diagnostic_non_tradable`.

Forbidden:

- AKShare or Baostock stock daily data as stock backtest input.
- `selection_result`, `factor_daily`, or standard performance reports.
- Any claim that the result is tradable or suitable for live advice.

## Current Provider Conclusions

- AKShare `benchmark_price` can be promoted to `DQ2` and used only for `benchmark_price` or `price_only_diagnostic`.
- AKShare stock daily lacks `limit_up`, `limit_down`, and `is_paused`; it remains `DQ1` and may only enter `daily_price_raw` or `daily_price_raw_smoke`.
- Baostock stock daily remains `DQ1` even if a future local login succeeds, unless it also has trustworthy `limit_up`, `limit_down`, and `is_paused`.
- Tushare status must be determined by the current Goal 10R capability matrix. If `daily`, `stk_limit`, `adj_factor`, and `daily_basic` are available but no trusted `is_paused` source is available, Tushare stock daily can reach at most DQ2 and must not be promoted to DQ3 `daily_price`.
- No provider may bypass `schema_contract` or `data_validator` to write `daily_price`.
- The `daily_price` standard must not be lowered to make incomplete real data run.

## Goal 12B Prerequisites

Goal 12B must not start a real standard-layer backtest until all of these are true:

- A real provider or trusted composition can supply `trading_limit`.
- A real provider or trusted composition can supply `suspension_status`.
- Trading calendar consistency checks are defined.
- Standard-layer records preserve `provider`, ingestion batch, and `dq_level` audit data.
- The existing `daily_price` validator remains strict.

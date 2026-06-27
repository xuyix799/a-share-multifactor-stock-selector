from collections.abc import Callable
from dataclasses import dataclass
import hashlib
import json
from typing import Any

import pandas as pd

from stock_selector.backtesting.execution import ExecutionConfig
from stock_selector.backtesting.metrics import calculate_backtest_metrics
from stock_selector.backtesting.portfolio import PortfolioConfig, simulate_equal_weight_portfolio
from stock_selector.backtesting.storage import read_dataset_history_between, write_backtest_detail
from stock_selector.backtesting.summary_repo import BacktestSummaryRepository, create_backtest_summary_repository
from stock_selector.utils.date_validator import validate_date_range


ReadHistoryFn = Callable[[str, str, str], pd.DataFrame]
WriteDetailFn = Callable[[str, pd.DataFrame], str]


@dataclass(frozen=True)
class BacktestConfig:
    strategy_name: str
    start_date: str
    end_date: str
    rebalance_mode: str
    initial_cash: float
    commission_rate: float
    slippage_bps: float
    stamp_tax_rate: float
    top_n: int = 50
    execution_rule: str = "next_open"

    def normalized(self) -> dict[str, Any]:
        start_date, end_date = validate_date_range(self.start_date, self.end_date)
        if self.rebalance_mode not in {"monthly", "quarterly"}:
            raise ValueError(f"unsupported rebalance_mode: {self.rebalance_mode}")
        if not self.strategy_name:
            raise ValueError("strategy_name is required")
        if self.initial_cash <= 0:
            raise ValueError("initial_cash must be positive")
        for name, value in [
            ("commission_rate", self.commission_rate),
            ("slippage_bps", self.slippage_bps),
            ("stamp_tax_rate", self.stamp_tax_rate),
        ]:
            if value < 0:
                raise ValueError(f"{name} must be non-negative")
        if self.top_n <= 0:
            raise ValueError("top_n must be positive")
        if not self.execution_rule:
            raise ValueError("execution_rule is required")
        return {
            "strategy_name": self.strategy_name,
            "start_date": start_date,
            "end_date": end_date,
            "rebalance_mode": self.rebalance_mode,
            "initial_cash": float(self.initial_cash),
            "commission_rate": float(self.commission_rate),
            "slippage_bps": float(self.slippage_bps),
            "stamp_tax_rate": float(self.stamp_tax_rate),
            "top_n": int(self.top_n),
            "execution_rule": self.execution_rule,
        }


def build_run_key(config: BacktestConfig) -> str:
    payload = json.dumps(config.normalized(), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def run_backtest(
    config: BacktestConfig,
    *,
    force: bool = False,
    read_history_fn: ReadHistoryFn | None = None,
    write_detail_fn: WriteDetailFn | None = None,
    summary_repo: BacktestSummaryRepository | None = None,
) -> dict[str, Any]:
    params = config.normalized()
    run_key = build_run_key(config)
    repo = summary_repo or create_backtest_summary_repository()
    existing = repo.find_done(run_key)
    if existing and not force:
        return {"status": "skipped", "run_key": run_key, "summary": existing}

    reader = read_history_fn or read_dataset_history_between
    writer = write_detail_fn or write_backtest_detail
    selection_history = reader("selection_result", params["start_date"], params["end_date"])
    adjusted_price_history = reader("adjusted_price", params["start_date"], params["end_date"])
    daily_price_history = reader("daily_price", params["start_date"], params["end_date"])
    benchmark_price = reader("benchmark_price", params["start_date"], params["end_date"])
    price_history = prepare_backtest_price_history(adjusted_price_history, daily_price_history)

    simulation = simulate_equal_weight_portfolio(
        selection_history=selection_history,
        adjusted_price_history=price_history,
        config=PortfolioConfig(
            start_date=params["start_date"],
            end_date=params["end_date"],
            rebalance_mode=params["rebalance_mode"],
            initial_cash=params["initial_cash"],
            top_n=params["top_n"],
            execution_rule=params["execution_rule"],
            execution=ExecutionConfig(
                commission_rate=params["commission_rate"],
                slippage_bps=params["slippage_bps"],
                stamp_tax_rate=params["stamp_tax_rate"],
            ),
        ),
    )
    metrics = calculate_backtest_metrics(simulation.portfolio_daily, simulation.trade_detail, benchmark_price)
    detail = _combine_detail(run_key, params["strategy_name"], simulation.portfolio_daily, simulation.trade_detail)
    detail_object_key = writer(run_key, detail)
    summary = {
        **params,
        "run_key": run_key,
        "status": "done",
        "metrics": metrics,
        "report_object_key": None,
        "detail_object_key": detail_object_key,
    }
    repo.upsert_done(summary)
    return {
        "status": "done",
        "run_key": run_key,
        "detail_object_key": detail_object_key,
        "metrics": metrics,
        "summary": summary,
        "rebalance_count": len(simulation.rebalance_events),
    }


def prepare_backtest_price_history(adjusted_price_history: pd.DataFrame, daily_price_history: pd.DataFrame) -> pd.DataFrame:
    raw_columns = ["stock_code", "trade_date", "open", "limit_up", "limit_down"]
    raw = daily_price_history[raw_columns].copy()
    raw["trade_date"] = raw["trade_date"].astype(str)
    adjusted = adjusted_price_history.drop(columns=[column for column in ["open"] if column in adjusted_price_history.columns]).copy()
    adjusted["trade_date"] = adjusted["trade_date"].astype(str)
    merged = adjusted.merge(raw, on=["stock_code", "trade_date"], how="left", suffixes=("", "_raw"))
    if merged["open"].isna().any():
        missing = merged.loc[merged["open"].isna(), ["stock_code", "trade_date"]].head(5).to_dict(orient="records")
        raise ValueError(f"missing raw open for adjusted_price rows: {missing}")
    for column in ["limit_up", "limit_down"]:
        raw_column = f"{column}_raw"
        if raw_column in merged.columns:
            merged[column] = merged[raw_column]
            merged = merged.drop(columns=[raw_column])
    return merged


def _combine_detail(run_key: str, strategy_name: str, portfolio_daily: pd.DataFrame, trade_detail: pd.DataFrame) -> pd.DataFrame:
    records = portfolio_daily.to_dict(orient="records")
    if not trade_detail.empty:
        records.extend(trade_detail.to_dict(orient="records"))
    detail = pd.DataFrame.from_records(records)
    detail.insert(0, "run_key", run_key)
    detail.insert(1, "strategy_name", strategy_name)
    return detail

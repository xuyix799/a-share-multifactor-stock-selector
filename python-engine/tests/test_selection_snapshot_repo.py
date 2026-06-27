import sqlite3

from selection_test_helpers import eligible_universe_frame, factor_daily_frame, risk_filter_frame
from stock_selector.scoring.score_engine import parse_scoring_config
from stock_selector.scoring.selection_builder import build_selection_result
from stock_selector.scoring.selection_snapshot_repo import SelectionSnapshotRepository, summarize_selection_result


def _connection():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE selection_snapshot (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            trade_date TEXT NOT NULL,
            rebalance_mode TEXT NOT NULL,
            selected_count INTEGER NOT NULL DEFAULT 0,
            top_stocks TEXT NOT NULL DEFAULT '[]',
            object_key TEXT NOT NULL,
            top_n INTEGER,
            stock_count INTEGER,
            avg_total_score REAL,
            max_total_score REAL,
            min_total_score REAL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    return conn


def _selection(trade_date="2026-06-19"):
    return build_selection_result(
        factor_daily=factor_daily_frame(trade_date),
        risk_filter=risk_filter_frame(trade_date),
        eligible_universe=eligible_universe_frame(trade_date),
        factor_input_table=None,
        trade_date=trade_date,
        scoring_config=parse_scoring_config(
            {
                "quality_score": 0.30,
                "growth_score": 0.25,
                "valuation_score": 0.20,
                "industry_score": 0.15,
                "trend_score": 0.10,
            }
        ),
    )


def test_summarize_selection_result_contains_only_summary_and_object_key():
    df = _selection()

    summary = summarize_selection_result(df, trade_date="2026-06-19", top_n=50, object_key="raw/selection_result/trade_date=2026-06-19/part.parquet")

    assert summary["trade_date"] == "2026-06-19"
    assert summary["top_n"] == 50
    assert summary["object_key"].endswith("part.parquet")
    assert summary["stock_count"] == len(df)
    assert "stock_code" not in summary


def test_selection_snapshot_repo_replaces_existing_summary_for_trade_date():
    conn = _connection()
    repo = SelectionSnapshotRepository(lambda: conn, placeholder="?")
    first = summarize_selection_result(_selection(), trade_date="2026-06-19", top_n=50, object_key="old")
    second = summarize_selection_result(_selection(), trade_date="2026-06-19", top_n=10, object_key="new")

    repo.upsert_snapshot(first)
    repo.upsert_snapshot(second)

    rows = conn.execute(
        "SELECT trade_date, top_n, object_key, stock_count, avg_total_score, max_total_score, min_total_score FROM selection_snapshot"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "2026-06-19"
    assert rows[0][1] == 10
    assert rows[0][2] == "new"
    assert rows[0][3] == 2

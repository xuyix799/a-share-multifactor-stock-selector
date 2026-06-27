import sqlite3

from stock_selector.backtesting.summary_repo import BacktestSummaryRepository, ensure_backtest_summary_schema


def _connect(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS backtest_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_key TEXT,
            strategy_name TEXT NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            rebalance_mode TEXT,
            initial_cash REAL,
            commission_rate REAL,
            slippage_bps REAL,
            stamp_tax_rate REAL,
            top_n INTEGER,
            execution_rule TEXT,
            status TEXT,
            metrics TEXT NOT NULL DEFAULT '{}',
            report_object_key TEXT,
            detail_object_key TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    return conn


def test_backtest_summary_repository_upserts_and_finds_done_runs(tmp_path):
    db_path = tmp_path / "summary.sqlite"
    repo = BacktestSummaryRepository(lambda: _connect(db_path), placeholder="?")
    summary = {
        "run_key": "abc123",
        "strategy_name": "goal8-core",
        "start_date": "2026-01-01",
        "end_date": "2026-03-02",
        "rebalance_mode": "monthly",
        "initial_cash": 100000.0,
        "commission_rate": 0.001,
        "slippage_bps": 5.0,
        "stamp_tax_rate": 0.001,
        "top_n": 50,
        "execution_rule": "next_open",
        "status": "done",
        "metrics": {"total_return": 0.12},
        "report_object_key": None,
        "detail_object_key": "backtest/detail/run_key=abc123/part.parquet",
    }

    repo.upsert_done(summary)
    row = repo.find_done("abc123")

    assert row["run_key"] == "abc123"
    assert row["status"] == "done"
    assert row["metrics"] == {"total_return": 0.12}
    assert row["detail_object_key"] == "backtest/detail/run_key=abc123/part.parquet"


def test_ensure_backtest_summary_schema_adds_goal8_columns_to_existing_table(tmp_path):
    db_path = tmp_path / "legacy.sqlite"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE backtest_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                strategy_name TEXT NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                metrics TEXT NOT NULL DEFAULT '{}',
                report_object_key TEXT,
                detail_object_key TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    ensure_backtest_summary_schema(lambda: sqlite3.connect(db_path), placeholder="?")

    with sqlite3.connect(db_path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(backtest_summary)").fetchall()}

    assert {
        "run_key",
        "rebalance_mode",
        "initial_cash",
        "commission_rate",
        "slippage_bps",
        "stamp_tax_rate",
        "top_n",
        "execution_rule",
        "status",
    }.issubset(columns)

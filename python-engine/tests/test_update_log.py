import sqlite3

from stock_selector.data.update_log import UpdateLogRepository


def _repo():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE update_log (
            trade_date TEXT NOT NULL,
            step_name TEXT NOT NULL,
            status TEXT NOT NULL,
            object_key TEXT,
            message TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (trade_date, step_name)
        )
        """
    )
    return UpdateLogRepository(lambda: conn, placeholder="?"), conn


def test_update_log_done_step_is_detected_and_skipped_by_default():
    repo, _ = _repo()

    repo.mark_step_running("2026-06-19", "mock_daily_price")
    repo.mark_step_done("2026-06-19", "mock_daily_price", "raw/daily_price/trade_date=2026-06-19/part.parquet")

    assert repo.is_step_done("2026-06-19", "mock_daily_price") is True
    assert repo.should_run_step("2026-06-19", "mock_daily_price", force=False) is False


def test_update_log_force_allows_done_step_to_rerun():
    repo, _ = _repo()
    repo.mark_step_done("2026-06-19", "mock_daily_price", "old")

    assert repo.should_run_step("2026-06-19", "mock_daily_price", force=True) is True


def test_update_log_failed_records_error_message():
    repo, conn = _repo()

    repo.mark_step_failed("2026-06-19", "mock_daily_price", "bad data")

    row = conn.execute(
        "SELECT status, message FROM update_log WHERE trade_date = ? AND step_name = ?",
        ("2026-06-19", "mock_daily_price"),
    ).fetchone()
    assert row == ("failed", "bad data")

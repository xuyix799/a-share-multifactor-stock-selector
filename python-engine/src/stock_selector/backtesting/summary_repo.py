from collections.abc import Callable
from dataclasses import dataclass
import json
from typing import Any

from stock_selector.utils.date_validator import validate_date_range


def _placeholders(style: str, count: int) -> str:
    token = "?" if style == "?" else "%s"
    return ", ".join([token] * count)


@dataclass
class BacktestSummaryRepository:
    connection_factory: Callable[[], Any]
    placeholder: str = "%s"

    def find_done(self, run_key: str) -> dict[str, Any] | None:
        with self.connection_factory() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                SELECT
                    run_key,
                    strategy_name,
                    start_date,
                    end_date,
                    rebalance_mode,
                    initial_cash,
                    commission_rate,
                    slippage_bps,
                    stamp_tax_rate,
                    top_n,
                    execution_rule,
                    status,
                    metrics,
                    report_object_key,
                    detail_object_key
                FROM backtest_summary
                WHERE run_key = {self._p()} AND status = 'done'
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (run_key,),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            columns = [item[0] for item in cursor.description]
            result = dict(zip(columns, row, strict=True))
            result["metrics"] = self._decode_json(result["metrics"])
            result["start_date"] = str(result["start_date"])
            result["end_date"] = str(result["end_date"])
            return result

    def upsert_done(self, summary: dict[str, Any]) -> None:
        start_date, end_date = validate_date_range(str(summary["start_date"]), str(summary["end_date"]))
        values = (
            str(summary["run_key"]),
            str(summary["strategy_name"]),
            start_date,
            end_date,
            str(summary["rebalance_mode"]),
            float(summary["initial_cash"]),
            float(summary["commission_rate"]),
            float(summary["slippage_bps"]),
            float(summary["stamp_tax_rate"]),
            int(summary["top_n"]),
            str(summary["execution_rule"]),
            "done",
            self._json_value(summary["metrics"]),
            summary.get("report_object_key"),
            str(summary["detail_object_key"]),
        )
        with self.connection_factory() as conn:
            cursor = conn.cursor()
            cursor.execute(f"DELETE FROM backtest_summary WHERE run_key = {self._p()}", (str(summary["run_key"]),))
            cursor.execute(
                f"""
                INSERT INTO backtest_summary (
                    run_key,
                    strategy_name,
                    start_date,
                    end_date,
                    rebalance_mode,
                    initial_cash,
                    commission_rate,
                    slippage_bps,
                    stamp_tax_rate,
                    top_n,
                    execution_rule,
                    status,
                    metrics,
                    report_object_key,
                    detail_object_key
                )
                VALUES ({_placeholders(self.placeholder, len(values))})
                """,
                values,
            )
            if hasattr(conn, "commit"):
                conn.commit()

    def _p(self) -> str:
        return "?" if self.placeholder == "?" else "%s"

    def _json_value(self, value):
        if self.placeholder == "?":
            return json.dumps(value, ensure_ascii=False)
        from psycopg2.extras import Json

        return Json(value)

    def _decode_json(self, value):
        if isinstance(value, str):
            return json.loads(value)
        return value


def ensure_backtest_summary_schema(connection_factory: Callable[[], Any], placeholder: str = "%s") -> None:
    if placeholder == "?":
        _ensure_sqlite_backtest_summary_schema(connection_factory)
    else:
        _ensure_postgres_backtest_summary_schema(connection_factory)


def _ensure_sqlite_backtest_summary_schema(connection_factory: Callable[[], Any]) -> None:
    with connection_factory() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS backtest_summary (
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
        existing = {row[1] for row in cursor.execute("PRAGMA table_info(backtest_summary)").fetchall()}
        columns = {
            "run_key": "TEXT",
            "rebalance_mode": "TEXT",
            "initial_cash": "REAL",
            "commission_rate": "REAL",
            "slippage_bps": "REAL",
            "stamp_tax_rate": "REAL",
            "top_n": "INTEGER",
            "execution_rule": "TEXT",
            "status": "TEXT DEFAULT 'done'",
        }
        for column, column_type in columns.items():
            if column not in existing:
                cursor.execute(f"ALTER TABLE backtest_summary ADD COLUMN {column} {column_type}")
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_backtest_summary_run_key ON backtest_summary(run_key)")
        if hasattr(conn, "commit"):
            conn.commit()


def _ensure_postgres_backtest_summary_schema(connection_factory: Callable[[], Any]) -> None:
    with connection_factory() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS backtest_summary (
                id BIGSERIAL PRIMARY KEY,
                strategy_name TEXT NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
                report_object_key TEXT,
                detail_object_key TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            ALTER TABLE backtest_summary ADD COLUMN IF NOT EXISTS run_key TEXT;
            ALTER TABLE backtest_summary ADD COLUMN IF NOT EXISTS rebalance_mode TEXT;
            ALTER TABLE backtest_summary ADD COLUMN IF NOT EXISTS initial_cash DOUBLE PRECISION;
            ALTER TABLE backtest_summary ADD COLUMN IF NOT EXISTS commission_rate DOUBLE PRECISION;
            ALTER TABLE backtest_summary ADD COLUMN IF NOT EXISTS slippage_bps DOUBLE PRECISION;
            ALTER TABLE backtest_summary ADD COLUMN IF NOT EXISTS stamp_tax_rate DOUBLE PRECISION;
            ALTER TABLE backtest_summary ADD COLUMN IF NOT EXISTS top_n INTEGER;
            ALTER TABLE backtest_summary ADD COLUMN IF NOT EXISTS execution_rule TEXT;
            ALTER TABLE backtest_summary ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'done';
            ALTER TABLE backtest_summary DROP CONSTRAINT IF EXISTS backtest_summary_status_check;
            ALTER TABLE backtest_summary
                ADD CONSTRAINT backtest_summary_status_check
                CHECK (status IN ('done', 'failed'));
            CREATE UNIQUE INDEX IF NOT EXISTS idx_backtest_summary_run_key ON backtest_summary(run_key) WHERE run_key IS NOT NULL;
            """
        )
        if hasattr(conn, "commit"):
            conn.commit()


def create_backtest_summary_repository() -> BacktestSummaryRepository:
    from stock_selector.storage.postgres_client import create_postgres_client

    client = create_postgres_client()
    ensure_backtest_summary_schema(client.connect)
    return BacktestSummaryRepository(client.connect)

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from stock_selector.utils.date_validator import validate_trade_date


def _placeholders(style: str, count: int) -> str:
    token = "?" if style == "?" else "%s"
    return ", ".join([token] * count)


@dataclass
class UpdateLogRepository:
    connection_factory: Callable[[], Any]
    placeholder: str = "%s"

    def mark_step_running(self, trade_date: str, step_name: str) -> None:
        self._upsert(trade_date, step_name, "running", None, None)

    def mark_step_done(self, trade_date: str, step_name: str, object_key: str | None) -> None:
        self._upsert(trade_date, step_name, "done", object_key, None)

    def mark_step_failed(self, trade_date: str, step_name: str, error_message: str) -> None:
        self._upsert(trade_date, step_name, "failed", None, error_message)

    def is_step_done(self, trade_date: str, step_name: str) -> bool:
        trade_date = validate_trade_date(trade_date)
        with self.connection_factory() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"SELECT status FROM update_log WHERE trade_date = {self._p()} AND step_name = {self._p()}",
                (trade_date, step_name),
            )
            row = cursor.fetchone()
        return bool(row and row[0] == "done")

    def should_run_step(self, trade_date: str, step_name: str, force: bool = False) -> bool:
        return force or not self.is_step_done(trade_date, step_name)

    def list_by_trade_date(self, trade_date: str) -> list[dict[str, Any]]:
        trade_date = validate_trade_date(trade_date)
        with self.connection_factory() as conn:
            cursor = conn.cursor()
            cursor.execute(
                f"""
                SELECT trade_date, step_name, status, object_key, message, updated_at
                FROM update_log
                WHERE trade_date = {self._p()}
                ORDER BY step_name
                """,
                (trade_date,),
            )
            rows = cursor.fetchall()
        return [
            {
                "trade_date": str(row[0]),
                "step_name": row[1],
                "status": row[2],
                "object_key": row[3],
                "message": row[4],
                "updated_at": str(row[5]),
            }
            for row in rows
        ]

    def _upsert(self, trade_date: str, step_name: str, status: str, object_key: str | None, message: str | None) -> None:
        trade_date = validate_trade_date(trade_date)
        values = _placeholders(self.placeholder, 5)
        sql = f"""
            INSERT INTO update_log (trade_date, step_name, status, object_key, message, updated_at)
            VALUES ({values}, CURRENT_TIMESTAMP)
            ON CONFLICT(trade_date, step_name) DO UPDATE SET
                status = excluded.status,
                object_key = excluded.object_key,
                message = excluded.message,
                updated_at = CURRENT_TIMESTAMP
        """
        with self.connection_factory() as conn:
            cursor = conn.cursor()
            cursor.execute(sql, (trade_date, step_name, status, object_key, message))
            if hasattr(conn, "commit"):
                conn.commit()

    def _p(self) -> str:
        return "?" if self.placeholder == "?" else "%s"


def create_update_log_repository() -> UpdateLogRepository:
    from stock_selector.storage.postgres_client import create_postgres_client

    client = create_postgres_client()
    return UpdateLogRepository(client.connect)

from collections.abc import Callable
from dataclasses import dataclass
import json
from typing import Any

import pandas as pd

from stock_selector.utils.date_validator import validate_trade_date


def _placeholders(style: str, count: int) -> str:
    token = "?" if style == "?" else "%s"
    return ", ".join([token] * count)


def summarize_selection_result(
    selection_result: pd.DataFrame,
    *,
    trade_date: str,
    top_n: int,
    object_key: str,
    rebalance_mode: str = "daily",
) -> dict[str, Any]:
    trade_date = validate_trade_date(trade_date)
    if rebalance_mode not in {"daily", "monthly", "quarterly"}:
        raise ValueError(f"unsupported rebalance_mode: {rebalance_mode}")
    scores = pd.to_numeric(selection_result["total_score"], errors="coerce")
    top_stocks = _top_stocks(selection_result, top_n)
    return {
        "trade_date": trade_date,
        "rebalance_mode": rebalance_mode,
        "top_n": int(top_n),
        "object_key": str(object_key),
        "stock_count": int(len(selection_result)),
        "avg_total_score": float(scores.mean()) if not scores.empty else None,
        "max_total_score": float(scores.max()) if not scores.empty else None,
        "min_total_score": float(scores.min()) if not scores.empty else None,
        "top_stocks": top_stocks,
    }


@dataclass
class SelectionSnapshotRepository:
    connection_factory: Callable[[], Any]
    placeholder: str = "%s"

    def upsert_snapshot(self, summary: dict[str, Any]) -> None:
        trade_date = validate_trade_date(str(summary["trade_date"]))
        rebalance_mode = str(summary.get("rebalance_mode", "daily"))
        if rebalance_mode not in {"daily", "monthly", "quarterly"}:
            raise ValueError(f"unsupported rebalance_mode: {rebalance_mode}")
        values = (
            trade_date,
            rebalance_mode,
            int(summary["stock_count"]),
            self._json_value(summary.get("top_stocks", [])),
            str(summary["object_key"]),
            int(summary["top_n"]),
            int(summary["stock_count"]),
            summary["avg_total_score"],
            summary["max_total_score"],
            summary["min_total_score"],
        )
        insert_placeholders = _placeholders(self.placeholder, len(values))
        with self.connection_factory() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM selection_snapshot "
                f"WHERE trade_date = {self._p()} "
                f"AND rebalance_mode = {self._p()}",
                (trade_date, rebalance_mode),
            )
            cursor.execute(
                f"""
                INSERT INTO selection_snapshot (
                    trade_date,
                    rebalance_mode,
                    selected_count,
                    top_stocks,
                    object_key,
                    top_n,
                    stock_count,
                    avg_total_score,
                    max_total_score,
                    min_total_score
                )
                VALUES ({insert_placeholders})
                """,
                values,
            )
            if hasattr(conn, "commit"):
                conn.commit()

    def find_snapshot(
        self,
        trade_date: str,
        rebalance_mode: str,
    ) -> dict[str, Any] | None:
        trade_date = validate_trade_date(trade_date)
        if rebalance_mode not in {"daily", "monthly", "quarterly"}:
            raise ValueError(f"unsupported rebalance_mode: {rebalance_mode}")
        with self.connection_factory() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT
                    trade_date,
                    rebalance_mode,
                    top_n,
                    stock_count,
                    avg_total_score,
                    max_total_score,
                    min_total_score,
                    top_stocks,
                    object_key
                FROM selection_snapshot
                WHERE trade_date = {trade_date_placeholder}
                  AND rebalance_mode = {mode_placeholder}
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """.format(
                    trade_date_placeholder=self._p(),
                    mode_placeholder=self._p(),
                ),
                (trade_date, rebalance_mode),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            top_stocks = row[7]
            if isinstance(top_stocks, str):
                top_stocks = json.loads(top_stocks)
            return {
                "trade_date": str(row[0]),
                "rebalance_mode": str(row[1]),
                "top_n": int(row[2]),
                "stock_count": int(row[3]),
                "avg_total_score": (
                    None if row[4] is None else float(row[4])
                ),
                "max_total_score": (
                    None if row[5] is None else float(row[5])
                ),
                "min_total_score": (
                    None if row[6] is None else float(row[6])
                ),
                "top_stocks": top_stocks,
                "object_key": str(row[8]),
            }

    def _p(self) -> str:
        return "?" if self.placeholder == "?" else "%s"

    def _json_value(self, value):
        if self.placeholder == "?":
            return json.dumps(value, ensure_ascii=False)
        from psycopg2.extras import Json

        return Json(value)


def _top_stocks(selection_result: pd.DataFrame, top_n: int) -> list[dict[str, Any]]:
    top_rows = selection_result.head(int(top_n))
    stocks = []
    for row in top_rows.to_dict(orient="records"):
        stocks.append(
            {
                "stock_code": str(row["stock_code"]),
                "rank": int(row["rank"]),
                "total_score": float(row["total_score"]),
            }
        )
    return stocks


def create_selection_snapshot_repository() -> SelectionSnapshotRepository:
    from stock_selector.storage.postgres_client import create_postgres_client

    client = create_postgres_client()
    return SelectionSnapshotRepository(client.connect)

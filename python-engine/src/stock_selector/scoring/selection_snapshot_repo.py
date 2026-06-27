from collections.abc import Callable
from dataclasses import dataclass
import json
from typing import Any

import pandas as pd

from stock_selector.utils.date_validator import validate_trade_date


def _placeholders(style: str, count: int) -> str:
    token = "?" if style == "?" else "%s"
    return ", ".join([token] * count)


def summarize_selection_result(selection_result: pd.DataFrame, *, trade_date: str, top_n: int, object_key: str) -> dict[str, Any]:
    trade_date = validate_trade_date(trade_date)
    scores = pd.to_numeric(selection_result["total_score"], errors="coerce")
    top_stocks = _top_stocks(selection_result, top_n)
    return {
        "trade_date": trade_date,
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
        values = (
            trade_date,
            "daily",
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
            cursor.execute(f"DELETE FROM selection_snapshot WHERE trade_date = {self._p()}", (trade_date,))
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

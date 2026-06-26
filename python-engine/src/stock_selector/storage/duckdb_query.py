from pathlib import Path
from typing import Any

import duckdb

from stock_selector.data.data_validator import validate_stock_code
from stock_selector.utils.date_validator import validate_date_range


def query_parquet(parquet_path: str | Path, sql: str | None = None) -> list[tuple[Any, ...]]:
    path = str(parquet_path).replace("'", "''")
    statement = sql or f"SELECT * FROM read_parquet('{path}')"
    with duckdb.connect(database=":memory:") as conn:
        return conn.execute(statement).fetchall()


def query_dataset_file(parquet_path: str | Path, limit: int | None = None) -> list[dict[str, Any]]:
    path_sql = _duckdb_string(parquet_path)
    limit_sql = f" LIMIT {int(limit)}" if limit else ""
    with duckdb.connect(database=":memory:") as conn:
        df = conn.execute(f"SELECT * FROM read_parquet({path_sql}){limit_sql}").fetchdf()
    return df.to_dict(orient="records")


def query_stock_price_files(
    parquet_paths: list[str | Path],
    stock_code: str,
    start_date: str,
    end_date: str,
) -> list[dict[str, Any]]:
    stock_code = validate_stock_code(stock_code)
    start_date, end_date = validate_date_range(start_date, end_date)
    if not parquet_paths:
        return []

    paths_sql = "[" + ", ".join(_duckdb_string(path) for path in parquet_paths) + "]"
    with duckdb.connect(database=":memory:") as conn:
        df = conn.execute(
            f"""
            SELECT *
            FROM read_parquet({paths_sql})
            WHERE stock_code = ? AND trade_date BETWEEN ? AND ?
            ORDER BY trade_date, stock_code
            """,
            [stock_code, start_date, end_date],
        ).fetchdf()
    return df.to_dict(orient="records")


def _duckdb_string(path: str | Path) -> str:
    escaped = str(path).replace("'", "''")
    return f"'{escaped}'"

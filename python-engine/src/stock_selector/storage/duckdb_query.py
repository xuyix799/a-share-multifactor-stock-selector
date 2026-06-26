from pathlib import Path
from typing import Any

import duckdb


def query_parquet(parquet_path: str | Path, sql: str | None = None) -> list[tuple[Any, ...]]:
    path = str(parquet_path).replace("'", "''")
    statement = sql or f"SELECT * FROM read_parquet('{path}')"
    with duckdb.connect(database=":memory:") as conn:
        return conn.execute(statement).fetchall()

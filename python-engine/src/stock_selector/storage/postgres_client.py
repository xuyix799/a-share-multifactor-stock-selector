import os
from dataclasses import dataclass
from pathlib import Path

import psycopg2


class PostgresConfigError(RuntimeError):
    pass


def _env_required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise PostgresConfigError(f"missing environment variable: {name}")
    return value


@dataclass(frozen=True)
class PostgresClient:
    host: str
    port: int
    database: str
    user: str
    password: str
    schema_sql_path: Path

    def connect(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
            connect_timeout=10,
        )

    def check_connection(self) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()

    def initialize_schema(self, sql_path: Path) -> None:
        sql = Path(sql_path).read_text(encoding="utf-8")
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()


def _default_schema_path() -> Path:
    return Path(__file__).resolve().parents[4] / "docker" / "postgres" / "init" / "001_schema.sql"


def create_postgres_client() -> PostgresClient:
    return PostgresClient(
        host=os.getenv("STOCK_POSTGRES_HOST", "stock-postgres"),
        port=int(os.getenv("STOCK_POSTGRES_PORT", "5432")),
        database=os.getenv("STOCK_POSTGRES_DB", "stock_selector"),
        user=os.getenv("STOCK_POSTGRES_USER", "stock_app"),
        password=_env_required("STOCK_POSTGRES_PASSWORD"),
        schema_sql_path=Path(os.getenv("STOCK_SCHEMA_SQL_PATH", str(_default_schema_path()))),
    )

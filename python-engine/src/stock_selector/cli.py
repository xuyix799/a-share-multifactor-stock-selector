import argparse
import sys
import tempfile
from pathlib import Path

import pandas as pd

from stock_selector.config.config_loader import load_settings
from stock_selector.storage.atomic_writer import AtomicObjectWriter
from stock_selector.storage.minio_client import create_minio_client, ensure_buckets, get_required_buckets
from stock_selector.storage.postgres_client import create_postgres_client
from stock_selector.utils.date_validator import DateValidationError, validate_trade_date
from stock_selector.utils.logger import get_logger

logger = get_logger(__name__)


def _cmd_validate_date(args: argparse.Namespace) -> int:
    try:
        trade_date = validate_trade_date(args.trade_date)
    except DateValidationError as exc:
        print(f"invalid trade_date: {exc}", file=sys.stderr)
        return 2

    print(f"valid trade_date: {trade_date}")
    return 0


def _cmd_init_db(args: argparse.Namespace) -> int:
    _ = args
    client = create_postgres_client()
    sql_path = client.schema_sql_path
    client.initialize_schema(sql_path)
    print(f"initialized PostgreSQL schema from {sql_path}")
    return 0


def _cmd_init_storage(args: argparse.Namespace) -> int:
    _ = args
    settings = load_settings()
    client = create_minio_client(settings)
    buckets = get_required_buckets(settings)
    ensure_buckets(client, buckets)
    print("initialized MinIO buckets: " + ", ".join(buckets))
    return 0


def _cmd_health_check(args: argparse.Namespace) -> int:
    _ = args
    settings = load_settings()
    print("config: OK")

    pg_client = create_postgres_client()
    pg_client.check_connection()
    print("postgres: OK")

    minio_client = create_minio_client(settings)
    minio_client.list_buckets()
    print("minio: OK")

    missing = [bucket for bucket in get_required_buckets(settings) if not minio_client.bucket_exists(bucket)]
    if missing:
        print(f"missing MinIO buckets: {', '.join(missing)}", file=sys.stderr)
        return 1

    print("buckets: OK")
    return 0


def _cmd_storage_smoke(args: argparse.Namespace) -> int:
    try:
        trade_date = validate_trade_date(args.trade_date)
    except DateValidationError as exc:
        print(f"invalid trade_date: {exc}", file=sys.stderr)
        return 2

    settings = load_settings()
    minio_client = create_minio_client(settings)
    bucket = settings["storage"]["minio_bucket_raw"]

    df = pd.DataFrame(
        [
            {
                "trade_date": trade_date,
                "stock_code": "000001.SZ",
                "smoke_value": 1,
            }
        ]
    )

    with tempfile.TemporaryDirectory(prefix="stock-smoke-") as tmp:
        parquet_path = Path(tmp) / "smoke.parquet"
        df.to_parquet(parquet_path, index=False)

        writer = AtomicObjectWriter(client=minio_client, tmp_dir=Path(tmp))
        result = writer.write_file_atomic(
            bucket=bucket,
            final_key=f"smoke/trade_date={trade_date}/smoke.parquet",
            source_path=parquet_path,
        )

    logger.info("storage smoke wrote %s/%s", result.bucket, result.final_key)
    print(f"storage smoke: OK {result.bucket}/{result.final_key}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="stock-selector")
    subparsers = parser.add_subparsers(dest="command", required=True)

    health = subparsers.add_parser("health-check")
    health.set_defaults(func=_cmd_health_check)

    validate = subparsers.add_parser("validate-date")
    validate.add_argument("--trade-date", required=True)
    validate.set_defaults(func=_cmd_validate_date)

    init_db = subparsers.add_parser("init-db")
    init_db.set_defaults(func=_cmd_init_db)

    init_storage = subparsers.add_parser("init-storage")
    init_storage.set_defaults(func=_cmd_init_storage)

    smoke = subparsers.add_parser("storage-smoke")
    smoke.add_argument("--trade-date", required=True)
    smoke.set_defaults(func=_cmd_storage_smoke)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except Exception as exc:
        logger.exception("command failed")
        print(f"command failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

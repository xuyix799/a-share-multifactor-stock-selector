import argparse
from datetime import date, timedelta
import json
import sys
import tempfile
from pathlib import Path

import pandas as pd
from minio.error import S3Error

from stock_selector.cleaning.clean_pipeline import build_adjusted_price_for_date, build_clean_snapshot_for_date
from stock_selector.cleaning.snapshot_validator import validate_clean_daily_snapshot
from stock_selector.config.config_loader import load_settings
from stock_selector.data.data_validator import DataValidationError, validate_dataset_frame, validate_stock_code
from stock_selector.data.mock_data import generate_mock_dataset
from stock_selector.data.update_pipeline import update_provider_data
from stock_selector.data.update_log import create_update_log_repository
from stock_selector.factors.factor_pipeline import build_factor_daily_for_date
from stock_selector.factors.factor_validator import validate_factor_daily
from stock_selector.providers.provider_factory import list_providers
from stock_selector.providers.schema_contract import inspect_schema
from stock_selector.providers.schema_mapper import SchemaMappingError, normalize_date, normalize_stock_code
from stock_selector.storage.atomic_writer import AtomicObjectWriter
from stock_selector.storage.atomic_writer import write_parquet_local_atomic
from stock_selector.storage.duckdb_query import query_dataset_file, query_stock_price_files
from stock_selector.storage.minio_client import create_minio_client, ensure_buckets, get_required_buckets
from stock_selector.storage.partition import PROVIDER_DATASETS, DatasetValidationError, build_partition, validate_dataset
from stock_selector.storage.postgres_client import create_postgres_client
from stock_selector.universe.universe_pipeline import build_universe_inputs_for_date
from stock_selector.utils.date_validator import DateValidationError, validate_date_range, validate_trade_date
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


def _cmd_validate_range(args: argparse.Namespace) -> int:
    try:
        start_date, end_date = validate_date_range(args.start_date, args.end_date)
    except DateValidationError as exc:
        print(f"invalid date range: {exc}", file=sys.stderr)
        return 2

    print(f"valid date range: {start_date}..{end_date}")
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


def _cmd_generate_mock_data(args: argparse.Namespace) -> int:
    try:
        trade_date = validate_trade_date(args.trade_date)
        datasets = _resolve_datasets(args.dataset, all_datasets=PROVIDER_DATASETS)
    except (DateValidationError, DatasetValidationError) as exc:
        print(f"invalid input: {exc}", file=sys.stderr)
        return 2

    written = []
    for dataset in datasets:
        df = generate_mock_dataset(dataset, trade_date)
        validate_dataset_frame(dataset, df, trade_date)
        written.append(_write_dataset(dataset, trade_date, df))

    print(json.dumps({"trade_date": trade_date, "written": written}, ensure_ascii=False))
    return 0


def _cmd_validate_data(args: argparse.Namespace) -> int:
    try:
        trade_date = validate_trade_date(args.trade_date)
        datasets = _resolve_datasets(args.dataset, all_datasets=PROVIDER_DATASETS)
    except (DateValidationError, DatasetValidationError) as exc:
        print(f"invalid input: {exc}", file=sys.stderr)
        return 2

    with tempfile.TemporaryDirectory(prefix="stock-validate-") as tmp:
        for dataset in datasets:
            path = _materialize_dataset(dataset, trade_date, Path(tmp))
            df = pd.read_parquet(path)
            validate_dataset_frame(dataset, df, trade_date)

    print(json.dumps({"trade_date": trade_date, "validated": datasets}, ensure_ascii=False))
    return 0


def _cmd_update_mock_data(args: argparse.Namespace) -> int:
    try:
        trade_date = validate_trade_date(args.trade_date)
    except DateValidationError as exc:
        print(f"invalid trade_date: {exc}", file=sys.stderr)
        return 2

    _ensure_db_schema()
    repo = create_update_log_repository()
    results = []
    for dataset in PROVIDER_DATASETS:
        step_name = f"mock_data:{dataset}"
        if not repo.should_run_step(trade_date, step_name, force=args.force):
            results.append({"dataset": dataset, "status": "skipped"})
            continue

        try:
            repo.mark_step_running(trade_date, step_name)
            df = generate_mock_dataset(dataset, trade_date)
            validate_dataset_frame(dataset, df, trade_date)
            object_key = _write_dataset(dataset, trade_date, df)
            repo.mark_step_done(trade_date, step_name, object_key)
            results.append({"dataset": dataset, "status": "done", "object_key": object_key})
        except Exception as exc:
            repo.mark_step_failed(trade_date, step_name, str(exc))
            raise

    print(json.dumps({"trade_date": trade_date, "force": args.force, "results": results}, ensure_ascii=False))
    return 0


def _cmd_list_providers(args: argparse.Namespace) -> int:
    _ = args
    print(json.dumps(list_providers(), ensure_ascii=False))
    return 0


def _cmd_update_provider_data(args: argparse.Namespace) -> int:
    try:
        trade_date = validate_trade_date(args.trade_date)
    except DateValidationError as exc:
        print(f"invalid trade_date: {exc}", file=sys.stderr)
        return 2

    _ensure_db_schema()
    results = update_provider_data(
        trade_date,
        provider_name=args.provider,
        force=args.force,
        write_dataset_fn=_write_dataset,
    )
    print(json.dumps({"trade_date": trade_date, "provider": args.provider, "force": args.force, "results": results}, ensure_ascii=False))
    return 0


def _cmd_validate_provider_data(args: argparse.Namespace) -> int:
    try:
        trade_date = validate_trade_date(args.trade_date)
        datasets = _resolve_datasets(args.dataset, all_datasets=PROVIDER_DATASETS)
    except (DateValidationError, DatasetValidationError) as exc:
        print(f"invalid input: {exc}", file=sys.stderr)
        return 2

    with tempfile.TemporaryDirectory(prefix="stock-provider-validate-") as tmp:
        for dataset in datasets:
            path = _materialize_dataset(dataset, trade_date, Path(tmp))
            df = pd.read_parquet(path)
            validate_dataset_frame(dataset, df, trade_date)

    print(json.dumps({"trade_date": trade_date, "validated": datasets}, ensure_ascii=False))
    return 0


def _cmd_inspect_schema(args: argparse.Namespace) -> int:
    try:
        info = inspect_schema(args.dataset)
    except DatasetValidationError as exc:
        print(f"invalid dataset: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(info, ensure_ascii=False))
    return 0


def _cmd_normalize_stock_code(args: argparse.Namespace) -> int:
    try:
        normalized = normalize_stock_code(args.stock_code)
    except SchemaMappingError as exc:
        print(f"invalid stock_code: {exc}", file=sys.stderr)
        return 2
    print(normalized)
    return 0


def _cmd_normalize_date(args: argparse.Namespace) -> int:
    try:
        normalized = normalize_date(args.date)
    except SchemaMappingError as exc:
        print(f"invalid date: {exc}", file=sys.stderr)
        return 2
    print(normalized)
    return 0


def _cmd_query_parquet(args: argparse.Namespace) -> int:
    try:
        dataset = validate_dataset(args.dataset)
        trade_date = validate_trade_date(args.trade_date)
    except (DatasetValidationError, DateValidationError) as exc:
        print(f"invalid input: {exc}", file=sys.stderr)
        return 2

    with tempfile.TemporaryDirectory(prefix="stock-query-") as tmp:
        path = _materialize_dataset(dataset, trade_date, Path(tmp))
        rows = query_dataset_file(path, limit=10)

    print(json.dumps({"dataset": dataset, "trade_date": trade_date, "row_count": len(rows), "rows": rows}, ensure_ascii=False, default=str))
    return 0


def _cmd_query_stock_price(args: argparse.Namespace) -> int:
    try:
        stock_code = validate_stock_code(args.stock_code)
        start_date, end_date = validate_date_range(args.start_date, args.end_date)
    except (DataValidationError, DateValidationError) as exc:
        print(f"invalid input: {exc}", file=sys.stderr)
        return 2

    with tempfile.TemporaryDirectory(prefix="stock-price-query-") as tmp:
        paths = []
        for day in _iter_dates(start_date, end_date):
            path = _try_materialize_dataset("daily_price", day, Path(tmp))
            if path:
                paths.append(path)
        rows = query_stock_price_files(paths, stock_code, start_date, end_date)

    print(json.dumps({"stock_code": stock_code, "start_date": start_date, "end_date": end_date, "row_count": len(rows), "rows": rows}, ensure_ascii=False, default=str))
    return 0


def _cmd_build_adjusted_price(args: argparse.Namespace) -> int:
    try:
        trade_date = validate_trade_date(args.trade_date)
    except DateValidationError as exc:
        print(f"invalid trade_date: {exc}", file=sys.stderr)
        return 2

    _ensure_db_schema()
    result = build_adjusted_price_for_date(trade_date, force=args.force, read_dataset_fn=_read_dataset, write_dataset_fn=_write_dataset)
    print(json.dumps({"trade_date": trade_date, "force": args.force, "result": result}, ensure_ascii=False, default=str))
    return 0


def _cmd_build_clean_snapshot(args: argparse.Namespace) -> int:
    try:
        trade_date = validate_trade_date(args.trade_date)
    except DateValidationError as exc:
        print(f"invalid trade_date: {exc}", file=sys.stderr)
        return 2

    _ensure_db_schema()
    result = build_clean_snapshot_for_date(trade_date, force=args.force, read_dataset_fn=_read_dataset, write_dataset_fn=_write_dataset)
    print(json.dumps({"trade_date": trade_date, "force": args.force, "result": result}, ensure_ascii=False, default=str))
    return 0


def _cmd_validate_clean_snapshot(args: argparse.Namespace) -> int:
    try:
        trade_date = validate_trade_date(args.trade_date)
    except DateValidationError as exc:
        print(f"invalid trade_date: {exc}", file=sys.stderr)
        return 2

    with tempfile.TemporaryDirectory(prefix="stock-clean-validate-") as tmp:
        path = _materialize_dataset("clean_daily_snapshot", trade_date, Path(tmp))
        df = pd.read_parquet(path)
        validate_clean_daily_snapshot(df, trade_date)

    print(json.dumps({"trade_date": trade_date, "dataset": "clean_daily_snapshot", "status": "valid"}, ensure_ascii=False))
    return 0


def _cmd_build_universe_inputs(args: argparse.Namespace) -> int:
    try:
        trade_date = validate_trade_date(args.trade_date)
    except DateValidationError as exc:
        print(f"invalid trade_date: {exc}", file=sys.stderr)
        return 2

    _ensure_db_schema()
    result = build_universe_inputs_for_date(trade_date, force=args.force, read_dataset_fn=_read_dataset, write_dataset_fn=_write_dataset)
    print(json.dumps({"trade_date": trade_date, "force": args.force, "result": result}, ensure_ascii=False, default=str))
    return 0


def _cmd_build_factors(args: argparse.Namespace) -> int:
    try:
        trade_date = validate_trade_date(args.trade_date)
    except DateValidationError as exc:
        print(f"invalid trade_date: {exc}", file=sys.stderr)
        return 2

    _ensure_db_schema()
    result = build_factor_daily_for_date(trade_date, force=args.force, read_dataset_fn=_read_dataset, write_dataset_fn=_write_dataset)
    print(json.dumps({"trade_date": trade_date, "force": args.force, "result": result}, ensure_ascii=False, default=str))
    return 0


def _cmd_validate_factors(args: argparse.Namespace) -> int:
    try:
        trade_date = validate_trade_date(args.trade_date)
    except DateValidationError as exc:
        print(f"invalid trade_date: {exc}", file=sys.stderr)
        return 2

    with tempfile.TemporaryDirectory(prefix="stock-factor-validate-") as tmp:
        path = _materialize_dataset("factor_daily", trade_date, Path(tmp))
        df = pd.read_parquet(path)
        validate_factor_daily(df, trade_date)

    print(json.dumps({"trade_date": trade_date, "dataset": "factor_daily", "status": "valid"}, ensure_ascii=False))
    return 0


def _cmd_show_update_log(args: argparse.Namespace) -> int:
    try:
        trade_date = validate_trade_date(args.trade_date)
    except DateValidationError as exc:
        print(f"invalid trade_date: {exc}", file=sys.stderr)
        return 2

    _ensure_db_schema()
    repo = create_update_log_repository()
    rows = repo.list_by_trade_date(trade_date)
    print(json.dumps({"trade_date": trade_date, "rows": rows}, ensure_ascii=False, default=str))
    return 0


def _resolve_datasets(dataset: str, all_datasets=PROVIDER_DATASETS) -> list[str]:
    if dataset == "all":
        return list(all_datasets)
    return [validate_dataset(dataset)]


def _ensure_db_schema() -> None:
    client = create_postgres_client()
    client.initialize_schema(client.schema_sql_path)


def _storage_backend(settings: dict) -> str:
    import os

    backend = os.getenv("STOCK_PARQUET_BACKEND") or settings["storage"].get("parquet_backend", "minio")
    if backend not in {"local", "minio"}:
        raise ValueError(f"unsupported storage backend: {backend}")
    return backend


def _local_root(settings: dict) -> Path:
    import os

    return Path(os.getenv("STOCK_LOCAL_DATA_DIR") or settings["storage"].get("local_data_dir", "data"))


def _write_dataset(dataset: str, trade_date: str, df: pd.DataFrame) -> str:
    settings = load_settings()
    partition = build_partition(dataset, trade_date, local_root=_local_root(settings))
    backend = _storage_backend(settings)
    if backend == "local":
        write_parquet_local_atomic(df, partition.local_path)
        return partition.local_path.as_posix()

    minio_client = create_minio_client(settings)
    bucket = settings["storage"]["minio_bucket_raw"]
    ensure_buckets(minio_client, [bucket])
    with tempfile.TemporaryDirectory(prefix="stock-write-") as tmp:
        local_file = Path(tmp) / "part.parquet"
        df.to_parquet(local_file, index=False)
        result = AtomicObjectWriter(minio_client, tmp_dir=Path(tmp)).write_file_atomic(bucket, partition.object_key, local_file)
    return result.final_key


def _read_dataset(dataset: str, trade_date: str) -> pd.DataFrame:
    with tempfile.TemporaryDirectory(prefix="stock-read-") as tmp:
        path = _materialize_dataset(dataset, trade_date, Path(tmp))
        return pd.read_parquet(path)


def _materialize_dataset(dataset: str, trade_date: str, tmp_root: Path) -> Path:
    path = _try_materialize_dataset(dataset, trade_date, tmp_root)
    if not path:
        raise FileNotFoundError(f"missing parquet for {dataset} {trade_date}")
    return path


def _try_materialize_dataset(dataset: str, trade_date: str, tmp_root: Path) -> Path | None:
    settings = load_settings()
    partition = build_partition(dataset, trade_date, local_root=_local_root(settings))
    if _storage_backend(settings) == "local":
        return partition.local_path if partition.local_path.exists() else None

    minio_client = create_minio_client(settings)
    bucket = settings["storage"]["minio_bucket_raw"]
    target = tmp_root / partition.object_key
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        minio_client.fget_object(bucket, partition.object_key, str(target))
    except S3Error as exc:
        if exc.code in {"NoSuchKey", "NoSuchBucket", "NoSuchObject"}:
            return None
        raise
    return target


def _iter_dates(start_date: str, end_date: str):
    current = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    while current <= end:
        yield current.isoformat()
        current += timedelta(days=1)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="stock-selector")
    subparsers = parser.add_subparsers(dest="command", required=True)

    health = subparsers.add_parser("health-check")
    health.set_defaults(func=_cmd_health_check)

    validate = subparsers.add_parser("validate-date")
    validate.add_argument("--trade-date", required=True)
    validate.set_defaults(func=_cmd_validate_date)

    validate_range = subparsers.add_parser("validate-range")
    validate_range.add_argument("--start-date", required=True)
    validate_range.add_argument("--end-date", required=True)
    validate_range.set_defaults(func=_cmd_validate_range)

    init_db = subparsers.add_parser("init-db")
    init_db.set_defaults(func=_cmd_init_db)

    init_storage = subparsers.add_parser("init-storage")
    init_storage.set_defaults(func=_cmd_init_storage)

    smoke = subparsers.add_parser("storage-smoke")
    smoke.add_argument("--trade-date", required=True)
    smoke.set_defaults(func=_cmd_storage_smoke)

    generate = subparsers.add_parser("generate-mock-data")
    generate.add_argument("--trade-date", required=True)
    generate.add_argument("--dataset", required=True)
    generate.set_defaults(func=_cmd_generate_mock_data)

    validate_data = subparsers.add_parser("validate-data")
    validate_data.add_argument("--trade-date", required=True)
    validate_data.add_argument("--dataset", required=True)
    validate_data.set_defaults(func=_cmd_validate_data)

    update_mock = subparsers.add_parser("update-mock-data")
    update_mock.add_argument("--trade-date", required=True)
    update_mock.add_argument("--force", action="store_true")
    update_mock.set_defaults(func=_cmd_update_mock_data)

    list_provider_parser = subparsers.add_parser("list-providers")
    list_provider_parser.set_defaults(func=_cmd_list_providers)

    update_provider = subparsers.add_parser("update-provider-data")
    update_provider.add_argument("--trade-date", required=True)
    update_provider.add_argument("--provider", default="mock")
    update_provider.add_argument("--force", action="store_true")
    update_provider.set_defaults(func=_cmd_update_provider_data)

    validate_provider_data = subparsers.add_parser("validate-provider-data")
    validate_provider_data.add_argument("--trade-date", required=True)
    validate_provider_data.add_argument("--dataset", required=True)
    validate_provider_data.set_defaults(func=_cmd_validate_provider_data)

    inspect_schema_parser = subparsers.add_parser("inspect-schema")
    inspect_schema_parser.add_argument("--dataset", required=True)
    inspect_schema_parser.set_defaults(func=_cmd_inspect_schema)

    normalize_code = subparsers.add_parser("normalize-stock-code")
    normalize_code.add_argument("--stock-code", required=True)
    normalize_code.set_defaults(func=_cmd_normalize_stock_code)

    normalize_date_parser = subparsers.add_parser("normalize-date")
    normalize_date_parser.add_argument("--date", required=True)
    normalize_date_parser.set_defaults(func=_cmd_normalize_date)

    query = subparsers.add_parser("query-parquet")
    query.add_argument("--dataset", required=True)
    query.add_argument("--trade-date", required=True)
    query.set_defaults(func=_cmd_query_parquet)

    query_stock = subparsers.add_parser("query-stock-price")
    query_stock.add_argument("--stock-code", required=True)
    query_stock.add_argument("--start-date", required=True)
    query_stock.add_argument("--end-date", required=True)
    query_stock.set_defaults(func=_cmd_query_stock_price)

    build_adjusted = subparsers.add_parser("build-adjusted-price")
    build_adjusted.add_argument("--trade-date", required=True)
    build_adjusted.add_argument("--force", action="store_true")
    build_adjusted.set_defaults(func=_cmd_build_adjusted_price)

    build_snapshot = subparsers.add_parser("build-clean-snapshot")
    build_snapshot.add_argument("--trade-date", required=True)
    build_snapshot.add_argument("--force", action="store_true")
    build_snapshot.set_defaults(func=_cmd_build_clean_snapshot)

    validate_snapshot = subparsers.add_parser("validate-clean-snapshot")
    validate_snapshot.add_argument("--trade-date", required=True)
    validate_snapshot.set_defaults(func=_cmd_validate_clean_snapshot)

    build_universe = subparsers.add_parser("build-universe-inputs")
    build_universe.add_argument("--trade-date", required=True)
    build_universe.add_argument("--force", action="store_true")
    build_universe.set_defaults(func=_cmd_build_universe_inputs)

    build_factors = subparsers.add_parser("build-factors")
    build_factors.add_argument("--trade-date", required=True)
    build_factors.add_argument("--force", action="store_true")
    build_factors.set_defaults(func=_cmd_build_factors)

    validate_factors = subparsers.add_parser("validate-factors")
    validate_factors.add_argument("--trade-date", required=True)
    validate_factors.set_defaults(func=_cmd_validate_factors)

    show_log = subparsers.add_parser("show-update-log")
    show_log.add_argument("--trade-date", required=True)
    show_log.set_defaults(func=_cmd_show_update_log)

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

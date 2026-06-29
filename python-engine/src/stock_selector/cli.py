import argparse
from datetime import date, timedelta
import json
import sys
import tempfile
from pathlib import Path

import pandas as pd
from minio.error import S3Error

from stock_selector.backtesting.backtest_pipeline import BacktestConfig, run_backtest
from stock_selector.cleaning.clean_pipeline import build_adjusted_price_for_date, build_clean_snapshot_for_date
from stock_selector.cleaning.snapshot_validator import validate_clean_daily_snapshot
from stock_selector.config.config_loader import load_factor_weights_config, load_settings
from stock_selector.data.data_validator import DataValidationError, validate_dataset_frame, validate_stock_code
from stock_selector.data.mock_data import generate_mock_dataset
from stock_selector.data.tushare_candidate_staging_batch import (
    build_tushare_candidate_staging_batch,
    build_tushare_candidate_staging_batch_blocked_report,
)
from stock_selector.data.tushare_daily_price_candidate import (
    SmokeInput,
    build_dry_run_output_keys,
    candidate_frame_from_report,
    dry_run_tushare_daily_price_candidate,
)
from stock_selector.data.tushare_suspension_status_candidate import (
    SuspensionCandidateInput,
    build_suspension_status_candidate_output_keys,
    build_tushare_suspension_status_candidate,
    candidate_frame_from_report as suspension_candidate_frame_from_report,
)
from stock_selector.data.update_pipeline import update_provider_data
from stock_selector.data.update_log import create_update_log_repository
from stock_selector.factors.factor_pipeline import build_factor_daily_for_date
from stock_selector.factors.factor_validator import validate_factor_daily
from stock_selector.providers.base import ProviderConfigurationError
from stock_selector.providers.provider_factory import list_providers
from stock_selector.providers.schema_contract import inspect_schema
from stock_selector.providers.schema_mapper import SchemaMappingError, normalize_date, normalize_stock_code
from stock_selector.providers.tushare_goal10r_probe import probe_tushare_goal10r
from stock_selector.providers.tushare_goal12b_probe import probe_tushare_goal12b
from stock_selector.providers.tushare_provider import TushareProvider
from stock_selector.scoring.selection_pipeline import build_selection_for_date
from stock_selector.scoring.selection_validator import validate_selection_result
from stock_selector.storage.atomic_writer import AtomicObjectWriter
from stock_selector.storage.atomic_writer import write_parquet_local_atomic
from stock_selector.storage.duckdb_query import query_dataset_file, query_stock_price_files
from stock_selector.storage.minio_client import create_minio_client, ensure_buckets, get_required_buckets
from stock_selector.storage.partition import (
    PROVIDER_DATASETS,
    DatasetValidationError,
    build_partition,
    build_provider_smoke_partition,
    validate_dataset,
    validate_provider_smoke_dataset,
)
from stock_selector.storage.postgres_client import create_postgres_client
from stock_selector.universe.universe_pipeline import build_universe_inputs_for_date
from stock_selector.utils.date_validator import DateValidationError, validate_date_range, validate_trade_date
from stock_selector.utils.logger import get_logger

logger = get_logger(__name__)

DEFAULT_GOAL13_CODES = [
    "000001.SZ",
    "600519.SH",
    "300750.SZ",
    "000333.SZ",
    "601318.SH",
    "600036.SH",
    "000858.SZ",
    "601899.SH",
    "600900.SH",
    "002415.SZ",
]


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
    provider_name = args.provider.lower()
    if provider_name != "mock" and not args.smoke:
        print("external provider updates must use --smoke to avoid standard raw data pollution", file=sys.stderr)
        return 2

    _ensure_db_schema()
    writer = _write_dataset
    step_prefix = "provider_data"
    if args.smoke:
        writer = lambda dataset, requested_date, df: _write_provider_smoke_dataset(provider_name, dataset, requested_date, df)
        step_prefix = f"provider_smoke:{provider_name}"
    results = update_provider_data(
        trade_date,
        provider_name=provider_name,
        force=args.force,
        datasets=args.dataset,
        step_prefix=step_prefix,
        write_dataset_fn=writer,
        allow_smoke_datasets=args.smoke,
    )
    print(json.dumps({"trade_date": trade_date, "provider": provider_name, "force": args.force, "smoke": args.smoke, "results": results}, ensure_ascii=False))
    return 0


def _cmd_probe_tushare_goal10r(args: argparse.Namespace) -> int:
    try:
        trade_date = validate_trade_date(args.trade_date)
    except DateValidationError as exc:
        print(f"invalid trade_date: {exc}", file=sys.stderr)
        return 2
    if args.sample_limit <= 0:
        print("invalid input: sample_limit must be positive", file=sys.stderr)
        return 2
    if args.sleep_seconds < 0:
        print("invalid input: sleep_seconds must be non-negative", file=sys.stderr)
        return 2

    settings = load_settings()
    provider = TushareProvider(settings=settings)
    result = probe_tushare_goal10r(
        provider,
        trade_date,
        write_dataset_fn=lambda dataset, requested_date, df: _write_provider_smoke_dataset("tushare", dataset, requested_date, df),
        sample_limit=args.sample_limit,
        sleep_seconds=args.sleep_seconds,
    )
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


def _cmd_probe_tushare_goal12b(args: argparse.Namespace) -> int:
    try:
        trade_date = validate_trade_date(args.trade_date)
    except DateValidationError as exc:
        print(f"invalid trade_date: {exc}", file=sys.stderr)
        return 2
    if args.sample_limit <= 0:
        print("invalid input: sample_limit must be positive", file=sys.stderr)
        return 2
    if args.sleep_seconds < 0:
        print("invalid input: sleep_seconds must be non-negative", file=sys.stderr)
        return 2

    settings = load_settings()
    provider = TushareProvider(settings=settings)
    result = probe_tushare_goal12b(
        provider,
        trade_date,
        write_dataset_fn=lambda dataset, requested_date, df: _write_provider_smoke_dataset("tushare", dataset, requested_date, df),
        sample_limit=args.sample_limit,
        sleep_seconds=args.sleep_seconds,
    )
    print(json.dumps(result, ensure_ascii=False, default=str))
    return 0


def _cmd_dry_run_tushare_daily_price_candidate(args: argparse.Namespace) -> int:
    try:
        trade_date = validate_trade_date(args.trade_date)
    except DateValidationError as exc:
        print(f"invalid trade_date: {exc}", file=sys.stderr)
        return 2
    if args.sample_limit <= 0:
        print("invalid input: sample_limit must be positive", file=sys.stderr)
        return 2

    report = dry_run_tushare_daily_price_candidate(
        trade_date,
        sample_limit=args.sample_limit,
        load_smoke_input_fn=_load_tushare_smoke_input,
    )
    output = _write_tushare_daily_price_candidate_dry_run(report)
    print(json.dumps(output, ensure_ascii=False, default=str))
    return 0 if report["status"] == "DRY_RUN_COMPLETED" else 1


def _cmd_build_tushare_suspension_status_candidate(args: argparse.Namespace) -> int:
    try:
        trade_date = validate_trade_date(args.trade_date)
    except DateValidationError as exc:
        print(f"invalid trade_date: {exc}", file=sys.stderr)
        return 2
    if args.sample_limit <= 0:
        print("invalid input: sample_limit must be positive", file=sys.stderr)
        return 2

    report = build_tushare_suspension_status_candidate(
        trade_date,
        sample_limit=args.sample_limit,
        load_input_fn=_load_tushare_suspension_candidate_input,
    )
    output = _write_tushare_suspension_status_candidate(report)
    print(json.dumps(output, ensure_ascii=False, default=str))
    return 0 if not report["status"].startswith("BLOCKED_BY_") else 1


def _cmd_build_tushare_candidate_staging_batch(args: argparse.Namespace) -> int:
    try:
        start_date, end_date = validate_date_range(args.start_date, args.end_date)
    except DateValidationError as exc:
        print(f"invalid date range: {exc}", file=sys.stderr)
        return 2
    if args.sleep_seconds < 0:
        print("invalid input: sleep_seconds must be non-negative", file=sys.stderr)
        return 2
    if args.max_codes is not None and args.max_codes <= 0:
        print("invalid input: max_codes must be positive", file=sys.stderr)
        return 2
    if args.max_trade_days is not None and args.max_trade_days <= 0:
        print("invalid input: max_trade_days must be positive", file=sys.stderr)
        return 2

    codes = _parse_codes_arg(args.codes)
    settings = load_settings()
    provider = None
    if not args.no_provider_call:
        try:
            provider = TushareProvider(settings=settings)
        except ProviderConfigurationError as exc:
            status = _tushare_provider_config_status(exc)
            report = build_tushare_candidate_staging_batch_blocked_report(
                start_date=start_date,
                end_date=end_date,
                codes=codes,
                batch_id=args.batch_id,
                status=status,
                blocked_reasons=[str(exc)],
            )
            print(json.dumps(_tushare_candidate_staging_batch_cli_output(report), ensure_ascii=False, default=str))
            return 1

    result = build_tushare_candidate_staging_batch(
        start_date=start_date,
        end_date=end_date,
        codes=codes,
        provider=provider,
        batch_id=args.batch_id,
        sleep_seconds=args.sleep_seconds,
        max_codes=args.max_codes,
        max_trade_days=args.max_trade_days,
        no_provider_call=args.no_provider_call,
        reuse_existing_staging=args.reuse_existing_staging,
        coverage_expansion=args.coverage_expansion,
        fetch_semantics_audit=args.fetch_semantics_audit,
        load_parquet_fn=_load_tushare_candidate_batch_parquet,
        write_parquet_fn=_write_tushare_candidate_batch_parquet,
        write_json_fn=_write_tushare_candidate_batch_json,
        cli_command=" ".join(sys.argv),
    )
    output = _tushare_candidate_staging_batch_cli_output(result)
    print(json.dumps(output, ensure_ascii=False, default=str))
    if result["status"] != "CANDIDATE_BATCH_COMPLETED_NOT_PROMOTABLE":
        return 1
    if args.fail_on_incomplete_critical_coverage and _has_incomplete_critical_coverage(result):
        return 1
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
        provider_name = args.smoke_provider.lower() if args.smoke_provider else None
        dataset = validate_provider_smoke_dataset(args.dataset) if provider_name else validate_dataset(args.dataset)
        trade_date = validate_trade_date(args.trade_date)
    except (DatasetValidationError, DateValidationError) as exc:
        print(f"invalid input: {exc}", file=sys.stderr)
        return 2

    with tempfile.TemporaryDirectory(prefix="stock-query-") as tmp:
        if provider_name:
            path = _materialize_provider_smoke_dataset(provider_name, dataset, trade_date, Path(tmp))
        else:
            path = _materialize_dataset(dataset, trade_date, Path(tmp))
        rows = query_dataset_file(path, limit=10)

    print(
        json.dumps(
            {
                "dataset": dataset,
                "trade_date": trade_date,
                "smoke": bool(args.smoke_provider),
                "provider": provider_name,
                "row_count": len(rows),
                "rows": rows,
            },
            ensure_ascii=False,
            default=str,
        )
    )
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


def _cmd_build_selection(args: argparse.Namespace) -> int:
    try:
        trade_date = validate_trade_date(args.trade_date)
    except DateValidationError as exc:
        print(f"invalid trade_date: {exc}", file=sys.stderr)
        return 2

    _ensure_db_schema()
    result = build_selection_for_date(trade_date, force=args.force, read_dataset_fn=_read_dataset, write_dataset_fn=_write_dataset)
    print(json.dumps({"trade_date": trade_date, "force": args.force, "result": result}, ensure_ascii=False, default=str))
    return 0


def _cmd_validate_selection(args: argparse.Namespace) -> int:
    try:
        trade_date = validate_trade_date(args.trade_date)
    except DateValidationError as exc:
        print(f"invalid trade_date: {exc}", file=sys.stderr)
        return 2

    with tempfile.TemporaryDirectory(prefix="stock-selection-validate-") as tmp:
        path = _materialize_dataset("selection_result", trade_date, Path(tmp))
        df = pd.read_parquet(path)
        validate_selection_result(df, trade_date)

    print(json.dumps({"trade_date": trade_date, "dataset": "selection_result", "status": "valid"}, ensure_ascii=False))
    return 0


def _cmd_run_backtest(args: argparse.Namespace) -> int:
    try:
        start_date, end_date = validate_date_range(args.start_date, args.end_date)
        settings = load_settings()
        backtest_settings = settings.get("backtest", {})
        factor_weights = load_factor_weights_config()
        top_n_default = int(factor_weights.get("scoring", {}).get("top_n", 50))
        slippage_default = float(backtest_settings.get("slippage", 0.0)) * 10000
        config = BacktestConfig(
            strategy_name=args.strategy_name or "selection_equal_weight",
            start_date=start_date,
            end_date=end_date,
            rebalance_mode=args.rebalance or backtest_settings.get("rebalance", "monthly"),
            initial_cash=float(args.initial_cash if args.initial_cash is not None else backtest_settings.get("init_cash", 100000)),
            commission_rate=float(args.commission_rate if args.commission_rate is not None else backtest_settings.get("commission", 0.0)),
            slippage_bps=float(args.slippage_bps if args.slippage_bps is not None else slippage_default),
            stamp_tax_rate=float(args.stamp_tax_rate if args.stamp_tax_rate is not None else backtest_settings.get("stamp_tax", 0.0)),
            top_n=int(args.top_n if args.top_n is not None else top_n_default),
            execution_rule=args.execution_rule or backtest_settings.get("execution", "next_open"),
        )
        config.normalized()
    except (DateValidationError, ValueError) as exc:
        print(f"invalid input: {exc}", file=sys.stderr)
        return 2

    _ensure_db_schema()
    result = run_backtest(config, force=args.force)
    print(json.dumps(result, ensure_ascii=False, default=str))
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


def _write_provider_smoke_dataset(provider_name: str, dataset: str, trade_date: str, df: pd.DataFrame) -> str:
    settings = load_settings()
    partition = build_provider_smoke_partition(provider_name, dataset, trade_date, local_root=_local_root(settings))
    backend = _storage_backend(settings)
    if backend == "local":
        write_parquet_local_atomic(df, partition.local_path)
        return partition.local_path.as_posix()

    minio_client = create_minio_client(settings)
    bucket = settings["storage"]["minio_bucket_raw"]
    ensure_buckets(minio_client, [bucket])
    with tempfile.TemporaryDirectory(prefix="stock-smoke-write-") as tmp:
        local_file = Path(tmp) / "part.parquet"
        df.to_parquet(local_file, index=False)
        result = AtomicObjectWriter(minio_client, tmp_dir=Path(tmp)).write_file_atomic(bucket, partition.object_key, local_file)
    return result.final_key


def _load_tushare_smoke_input(dataset: str, trade_date: str) -> SmokeInput:
    settings = load_settings()
    partition = build_provider_smoke_partition("tushare", dataset, trade_date, local_root=_local_root(settings))
    with tempfile.TemporaryDirectory(prefix="stock-tushare-smoke-read-") as tmp:
        path = _try_materialize_provider_smoke_dataset("tushare", dataset, trade_date, Path(tmp))
        frame = pd.read_parquet(path) if path else None
    return SmokeInput(dataset=dataset, object_key=partition.object_key, frame=frame)


def _write_tushare_daily_price_candidate_dry_run(report: dict) -> dict:
    settings = load_settings()
    keys = build_dry_run_output_keys(report["trade_date"])
    report["output_object_keys"] = keys
    candidate = candidate_frame_from_report(report)

    backend = _storage_backend(settings)
    if backend == "local":
        root = _local_root(settings)
        report_path = root / keys["report"]
        report_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_report = report_path.with_name(report_path.name + ".tmp")
        tmp_report.write_text(json.dumps(report, ensure_ascii=False, default=str, indent=2), encoding="utf-8")
        tmp_report.replace(report_path)
        candidate_path = root / keys["candidate"]
        write_parquet_local_atomic(candidate, candidate_path)
    else:
        minio_client = create_minio_client(settings)
        bucket = settings["storage"]["minio_bucket_raw"]
        ensure_buckets(minio_client, [bucket])
        with tempfile.TemporaryDirectory(prefix="stock-tushare-dry-run-write-") as tmp:
            tmp_root = Path(tmp)
            report_file = tmp_root / "report.json"
            candidate_file = tmp_root / "part.parquet"
            report_file.write_text(json.dumps(report, ensure_ascii=False, default=str, indent=2), encoding="utf-8")
            candidate.to_parquet(candidate_file, index=False)
            writer = AtomicObjectWriter(client=minio_client, tmp_dir=tmp_root)
            writer.write_file_atomic(bucket, keys["report"], report_file)
            writer.write_file_atomic(bucket, keys["candidate"], candidate_file)

    return {
        "provider": "tushare",
        "goal": "12C",
        "trade_date": report["trade_date"],
        "status": report["status"],
        "report_key": keys["report"],
        "candidate_key": keys["candidate"],
        "join_row_count": report["join"]["candidate_row_count"],
        "input_row_counts": {dataset: info["row_count"] for dataset, info in report["inputs"].items()},
        "missing_inputs": report["missing_inputs"],
        "coverage": report["coverage"],
        "missing_field_stats": report["missing_field_stats"],
        "pause_status_counts": report["pause_status_counts"],
        "readiness_status": report["readiness"]["status"],
        "ready_for_dq3_promotion": report["readiness"]["ready_for_dq3_promotion"],
        "safety": report["safety"],
    }


def _load_tushare_suspension_candidate_input(dataset: str, trade_date: str) -> SuspensionCandidateInput:
    keys = build_dry_run_output_keys(trade_date)
    if dataset == "daily_price_candidate":
        object_key = keys["candidate"]
        with tempfile.TemporaryDirectory(prefix="stock-tushare-suspension-candidate-read-") as tmp:
            path = _try_materialize_object_key(object_key, Path(tmp))
            frame = pd.read_parquet(path) if path else None
        return SuspensionCandidateInput(dataset=dataset, object_key=object_key, frame=frame)

    if dataset == "daily_price_candidate_report":
        object_key = keys["report"]
        with tempfile.TemporaryDirectory(prefix="stock-tushare-suspension-report-read-") as tmp:
            path = _try_materialize_object_key(object_key, Path(tmp))
            payload = json.loads(path.read_text(encoding="utf-8")) if path else None
        return SuspensionCandidateInput(dataset=dataset, object_key=object_key, payload=payload)

    settings = load_settings()
    partition = build_provider_smoke_partition("tushare", dataset, trade_date, local_root=_local_root(settings))
    with tempfile.TemporaryDirectory(prefix="stock-tushare-suspension-smoke-read-") as tmp:
        path = _try_materialize_provider_smoke_dataset("tushare", dataset, trade_date, Path(tmp))
        frame = pd.read_parquet(path) if path else None
    return SuspensionCandidateInput(dataset=dataset, object_key=partition.object_key, frame=frame)


def _write_tushare_suspension_status_candidate(report: dict) -> dict:
    settings = load_settings()
    keys = build_suspension_status_candidate_output_keys(report["trade_date"])
    report["output_object_keys"] = keys
    candidate = suspension_candidate_frame_from_report(report)

    backend = _storage_backend(settings)
    if backend == "local":
        root = _local_root(settings)
        report_path = root / keys["coverage_audit"]
        report_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_report = report_path.with_name(report_path.name + ".tmp")
        tmp_report.write_text(json.dumps(report, ensure_ascii=False, default=str, indent=2), encoding="utf-8")
        tmp_report.replace(report_path)
        candidate_path = root / keys["candidate"]
        write_parquet_local_atomic(candidate, candidate_path)
    else:
        minio_client = create_minio_client(settings)
        bucket = settings["storage"]["minio_bucket_raw"]
        ensure_buckets(minio_client, [bucket])
        with tempfile.TemporaryDirectory(prefix="stock-tushare-suspension-write-") as tmp:
            tmp_root = Path(tmp)
            report_file = tmp_root / "report.json"
            candidate_file = tmp_root / "part.parquet"
            report_file.write_text(json.dumps(report, ensure_ascii=False, default=str, indent=2), encoding="utf-8")
            candidate.to_parquet(candidate_file, index=False)
            writer = AtomicObjectWriter(client=minio_client, tmp_dir=tmp_root)
            writer.write_file_atomic(bucket, keys["coverage_audit"], report_file)
            writer.write_file_atomic(bucket, keys["candidate"], candidate_file)

    return {
        "provider": "tushare",
        "goal": "12D",
        "trade_date": report["trade_date"],
        "status": report["status"],
        "candidate_key": keys["candidate"],
        "coverage_audit_key": keys["coverage_audit"],
        "candidate_row_count": report["candidate_row_count"],
        "input_row_counts": report["input_row_counts"],
        "pause_status_counts": report["pause_status_counts"],
        "evidence_counts": report["evidence_counts"],
        "coverage_status": report["suspend_d_event_coverage"]["coverage_status"],
        "blocked_reasons": report["blocked_reasons"],
        "readiness_status": report["readiness"]["status"],
        "ready_for_dq3_promotion": report["readiness"]["ready_for_dq3_promotion"],
        "safety": report["safety"],
    }


def _write_tushare_candidate_batch_parquet(object_key: str, frame: pd.DataFrame) -> str:
    settings = load_settings()
    backend = _storage_backend(settings)
    if backend == "local":
        path = _local_root(settings) / object_key
        write_parquet_local_atomic(frame, path)
        return object_key

    minio_client = create_minio_client(settings)
    bucket = settings["storage"]["minio_bucket_raw"]
    ensure_buckets(minio_client, [bucket])
    with tempfile.TemporaryDirectory(prefix="stock-tushare-candidate-batch-write-") as tmp:
        tmp_root = Path(tmp)
        local_file = tmp_root / "part.parquet"
        frame.to_parquet(local_file, index=False)
        AtomicObjectWriter(client=minio_client, tmp_dir=tmp_root).write_file_atomic(bucket, object_key, local_file)
    return object_key


def _write_tushare_candidate_batch_json(object_key: str, payload: dict) -> str:
    settings = load_settings()
    backend = _storage_backend(settings)
    if backend == "local":
        path = _local_root(settings) / object_key
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(path.name + ".tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, default=str, indent=2), encoding="utf-8")
        tmp_path.replace(path)
        return object_key

    minio_client = create_minio_client(settings)
    bucket = settings["storage"]["minio_bucket_raw"]
    ensure_buckets(minio_client, [bucket])
    with tempfile.TemporaryDirectory(prefix="stock-tushare-candidate-batch-json-") as tmp:
        tmp_root = Path(tmp)
        local_file = tmp_root / "payload.json"
        local_file.write_text(json.dumps(payload, ensure_ascii=False, default=str, indent=2), encoding="utf-8")
        AtomicObjectWriter(client=minio_client, tmp_dir=tmp_root).write_file_atomic(bucket, object_key, local_file)
    return object_key


def _load_tushare_candidate_batch_parquet(object_key: str) -> pd.DataFrame:
    with tempfile.TemporaryDirectory(prefix="stock-tushare-candidate-batch-read-") as tmp:
        path = _try_materialize_object_key(object_key, Path(tmp))
        if not path:
            raise FileNotFoundError(f"missing candidate staging parquet: {object_key}")
        return pd.read_parquet(path)


def _tushare_candidate_staging_batch_cli_output(result: dict) -> dict:
    keys = result.get("output_object_keys", {})
    dq3_audit = result.get("dq3_readiness_audit", {})
    return {
        "provider": "tushare",
        "goal": "13B",
        "status": result["status"],
        "batch_id": result["batch_id"],
        "start_date": result["start_date"],
        "end_date": result["end_date"],
        "codes": result.get("codes", []),
        "trade_dates": result.get("trade_dates", []),
        "manifest_key": keys.get("manifest"),
        "provider_coverage_report_key": keys.get("provider_coverage_report"),
        "fetch_semantics_report_key": keys.get("fetch_semantics_report"),
        "coverage_gap_report_key": keys.get("coverage_gap_report"),
        "dq3_readiness_audit_key": keys.get("dq3_readiness_audit"),
        "daily_price_candidate_batch_key": keys.get("daily_price_candidate_batch"),
        "suspension_status_candidate_batch_key": keys.get("suspension_status_candidate_batch"),
        "output_object_keys": keys,
        "staging_row_counts": result.get("staging_row_counts", {}),
        "daily_price_candidate_row_count": result.get("daily_price_candidate_row_count", 0),
        "suspension_status_candidate_row_count": result.get("suspension_status_candidate_row_count", 0),
        "coverage_summary": result.get("coverage_summary", {}),
        "pause_status_counts": result.get("pause_status_counts", {}),
        "readiness_status": dq3_audit.get("status") or result["status"],
        "ready_for_promotion_validator": result.get("ready_for_promotion_validator", False),
        "ready_for_dq3_promotion": result.get("ready_for_dq3_promotion", False),
        "blocked_reasons": result.get("blocked_reasons", []),
        "coverage_expansion": result.get("coverage_expansion", False),
        "fetch_semantics_audit": result.get("fetch_semantics_audit", False),
        "reused_existing_staging": result.get("reused_existing_staging", False),
        "safety": result.get("safety", {}),
        "inference_guards": result.get("inference_guards", {}),
    }


def _has_incomplete_critical_coverage(result: dict) -> bool:
    blocking = {
        "INCOMPLETE_DAILY_COVERAGE",
        "INCOMPLETE_LIMIT_PRICE_COVERAGE",
        "INCOMPLETE_ADJ_FACTOR_COVERAGE",
        "INCOMPLETE_DAILY_BASIC_COVERAGE",
        "SAMPLE_TRUNCATED",
        "PROVIDER_FETCH_INCOMPLETE",
        "SCHEMA_MISMATCH",
        "DUPLICATE_KEYS_FOUND",
    }
    return bool(blocking & set(result.get("blocked_reasons", [])))


def _parse_codes_arg(value: str | None) -> list[str]:
    if not value:
        return list(DEFAULT_GOAL13_CODES)
    codes = []
    for item in value.split(","):
        code = item.strip().upper()
        if code:
            codes.append(code)
    return codes or list(DEFAULT_GOAL13_CODES)


def _tushare_provider_config_status(exc: ProviderConfigurationError) -> str:
    message = str(exc).lower()
    if "disabled" in message:
        return "BLOCKED_BY_PROVIDER_DISABLED"
    if "missing" in message and "tushare_token" in message:
        return "BLOCKED_BY_MISSING_TUSHARE_TOKEN"
    return "BLOCKED_BY_PROVIDER_CONFIGURATION"


def _read_dataset(dataset: str, trade_date: str) -> pd.DataFrame:
    with tempfile.TemporaryDirectory(prefix="stock-read-") as tmp:
        path = _materialize_dataset(dataset, trade_date, Path(tmp))
        return pd.read_parquet(path)


def _materialize_dataset(dataset: str, trade_date: str, tmp_root: Path) -> Path:
    path = _try_materialize_dataset(dataset, trade_date, tmp_root)
    if not path:
        raise FileNotFoundError(f"missing parquet for {dataset} {trade_date}")
    return path


def _materialize_provider_smoke_dataset(provider_name: str, dataset: str, trade_date: str, tmp_root: Path) -> Path:
    path = _try_materialize_provider_smoke_dataset(provider_name, dataset, trade_date, tmp_root)
    if not path:
        raise FileNotFoundError(f"missing provider smoke parquet for {provider_name}/{dataset} {trade_date}")
    return path


def _try_materialize_object_key(object_key: str, tmp_root: Path) -> Path | None:
    settings = load_settings()
    if _storage_backend(settings) == "local":
        path = _local_root(settings) / object_key
        return path if path.exists() else None

    minio_client = create_minio_client(settings)
    bucket = settings["storage"]["minio_bucket_raw"]
    target = tmp_root / object_key
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        minio_client.fget_object(bucket, object_key, str(target))
    except S3Error as exc:
        if exc.code in {"NoSuchKey", "NoSuchBucket", "NoSuchObject"}:
            return None
        raise
    return target


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


def _try_materialize_provider_smoke_dataset(provider_name: str, dataset: str, trade_date: str, tmp_root: Path) -> Path | None:
    settings = load_settings()
    partition = build_provider_smoke_partition(provider_name, dataset, trade_date, local_root=_local_root(settings))
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
    update_provider.add_argument("--dataset", action="append")
    update_provider.add_argument("--smoke", action="store_true")
    update_provider.add_argument("--force", action="store_true")
    update_provider.set_defaults(func=_cmd_update_provider_data)

    probe_tushare_goal10r_parser = subparsers.add_parser("probe-tushare-goal10r")
    probe_tushare_goal10r_parser.add_argument("--trade-date", required=True)
    probe_tushare_goal10r_parser.add_argument("--sample-limit", type=int, default=5)
    probe_tushare_goal10r_parser.add_argument("--sleep-seconds", type=float, default=12.0)
    probe_tushare_goal10r_parser.set_defaults(func=_cmd_probe_tushare_goal10r)

    probe_tushare_goal12b_parser = subparsers.add_parser("probe-tushare-goal12b")
    probe_tushare_goal12b_parser.add_argument("--trade-date", required=True)
    probe_tushare_goal12b_parser.add_argument("--sample-limit", type=int, default=5)
    probe_tushare_goal12b_parser.add_argument("--sleep-seconds", type=float, default=12.0)
    probe_tushare_goal12b_parser.set_defaults(func=_cmd_probe_tushare_goal12b)

    dry_run_tushare_daily_price_candidate = subparsers.add_parser("dry-run-tushare-daily-price-candidate")
    dry_run_tushare_daily_price_candidate.add_argument("--trade-date", required=True)
    dry_run_tushare_daily_price_candidate.add_argument("--sample-limit", type=int, default=5)
    dry_run_tushare_daily_price_candidate.set_defaults(func=_cmd_dry_run_tushare_daily_price_candidate)

    build_tushare_suspension_status_candidate = subparsers.add_parser("build-tushare-suspension-status-candidate")
    build_tushare_suspension_status_candidate.add_argument("--trade-date", required=True)
    build_tushare_suspension_status_candidate.add_argument("--sample-limit", type=int, default=5)
    build_tushare_suspension_status_candidate.set_defaults(func=_cmd_build_tushare_suspension_status_candidate)

    build_tushare_candidate_staging_batch = subparsers.add_parser("build-tushare-candidate-staging-batch")
    build_tushare_candidate_staging_batch.add_argument("--start-date", required=True)
    build_tushare_candidate_staging_batch.add_argument("--end-date", required=True)
    build_tushare_candidate_staging_batch.add_argument("--codes", default=",".join(DEFAULT_GOAL13_CODES))
    build_tushare_candidate_staging_batch.add_argument("--batch-id")
    build_tushare_candidate_staging_batch.add_argument("--max-codes", type=int)
    build_tushare_candidate_staging_batch.add_argument("--max-trade-days", type=int)
    build_tushare_candidate_staging_batch.add_argument("--sleep-seconds", type=float, default=12.0)
    build_tushare_candidate_staging_batch.add_argument("--dry-run", action="store_true")
    build_tushare_candidate_staging_batch.add_argument("--no-provider-call", action="store_true")
    build_tushare_candidate_staging_batch.add_argument("--reuse-existing-staging", action="store_true")
    build_tushare_candidate_staging_batch.add_argument("--coverage-expansion", action="store_true")
    build_tushare_candidate_staging_batch.add_argument("--fetch-semantics-audit", action="store_true")
    build_tushare_candidate_staging_batch.add_argument("--fail-on-incomplete-critical-coverage", action="store_true")
    build_tushare_candidate_staging_batch.add_argument("--write-candidate", action="store_true", default=True)
    build_tushare_candidate_staging_batch.set_defaults(func=_cmd_build_tushare_candidate_staging_batch)

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
    query.add_argument("--smoke-provider")
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

    build_selection = subparsers.add_parser("build-selection")
    build_selection.add_argument("--trade-date", required=True)
    build_selection.add_argument("--force", action="store_true")
    build_selection.set_defaults(func=_cmd_build_selection)

    validate_selection = subparsers.add_parser("validate-selection")
    validate_selection.add_argument("--trade-date", required=True)
    validate_selection.set_defaults(func=_cmd_validate_selection)

    run_backtest_parser = subparsers.add_parser("run-backtest")
    run_backtest_parser.add_argument("--strategy-name")
    run_backtest_parser.add_argument("--start-date", required=True)
    run_backtest_parser.add_argument("--end-date", required=True)
    run_backtest_parser.add_argument("--rebalance", choices=["monthly", "quarterly"])
    run_backtest_parser.add_argument("--initial-cash", type=float)
    run_backtest_parser.add_argument("--commission-rate", type=float)
    run_backtest_parser.add_argument("--slippage-bps", type=float)
    run_backtest_parser.add_argument("--stamp-tax-rate", type=float)
    run_backtest_parser.add_argument("--top-n", type=int)
    run_backtest_parser.add_argument("--execution-rule")
    run_backtest_parser.add_argument("--force", action="store_true")
    run_backtest_parser.set_defaults(func=_cmd_run_backtest)

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

import argparse
from datetime import date, timedelta
import json
import re
import sys
import tempfile
from pathlib import Path

import pandas as pd
from minio.error import S3Error

from stock_selector.config.config_loader import load_factor_weights_config, load_settings
from stock_selector.data.data_validator import DataValidationError, validate_dataset_frame, validate_stock_code
from stock_selector.data.historical_backfill import (
    BackfillPlanningError,
    build_history_backfill_plan,  # noqa: F401 - retained as a v1 compatibility export
    build_history_backfill_plan_v2,
    dataframe_checksum,
    persist_historical_raw_landing,
    run_real_history_backfill,
)
from stock_selector.data.mock_data import generate_mock_dataset
from stock_selector.data.real_clean_inputs_landing import run_real_clean_inputs_small_batch
from stock_selector.data.tushare_candidate_staging_batch import (
    build_tushare_candidate_staging_batch,
    build_tushare_candidate_staging_batch_blocked_report,
)
from stock_selector.data.tushare_daily_price_promotion_validator import (
    build_tushare_daily_price_promotion_validator,
)
from stock_selector.data.tushare_daily_price_small_batch import (
    build_tushare_daily_price_small_batch_blocked_result,
    run_tushare_daily_price_small_batch,
)
from stock_selector.data.tushare_standard_inputs_landing import (
    build_tushare_standard_inputs_blocked_result,
    run_tushare_standard_inputs_small_batch,
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
from stock_selector.providers.base import ProviderConfigurationError
from stock_selector.providers.akshare_provider import AKShareProvider
from stock_selector.providers.historical_provider import HistoricalProviderRouter
from stock_selector.providers.provider_factory import list_providers
from stock_selector.providers.schema_contract import inspect_schema
from stock_selector.providers.schema_mapper import SchemaMappingError, normalize_date, normalize_stock_code
from stock_selector.providers.tushare_goal10r_probe import probe_tushare_goal10r
from stock_selector.providers.tushare_goal12b_probe import probe_tushare_goal12b
from stock_selector.providers.tushare_provider import TushareProvider
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
from stock_selector.utils.date_validator import DateValidationError, validate_date_range, validate_trade_date
from stock_selector.utils.logger import get_logger
from stock_selector.utils.path_validator import safe_object_key

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


def build_adjusted_price_for_date(*args, **kwargs):
    from stock_selector.cleaning.clean_pipeline import (
        build_adjusted_price_for_date as implementation,
    )

    return implementation(*args, **kwargs)


def build_clean_snapshot_for_date(*args, **kwargs):
    from stock_selector.cleaning.clean_pipeline import (
        build_clean_snapshot_for_date as implementation,
    )

    return implementation(*args, **kwargs)


def validate_clean_daily_snapshot(*args, **kwargs):
    from stock_selector.cleaning.snapshot_validator import (
        validate_clean_daily_snapshot as implementation,
    )

    return implementation(*args, **kwargs)


def build_universe_inputs_for_date(*args, **kwargs):
    from stock_selector.universe.universe_pipeline import (
        build_universe_inputs_for_date as implementation,
    )

    return implementation(*args, **kwargs)


def build_factor_daily_for_date(*args, **kwargs):
    from stock_selector.factors.factor_pipeline import (
        build_factor_daily_for_date as implementation,
    )

    return implementation(*args, **kwargs)


def validate_factor_daily(*args, **kwargs):
    from stock_selector.factors.factor_validator import validate_factor_daily as implementation

    return implementation(*args, **kwargs)


def build_selection_for_date(*args, **kwargs):
    from stock_selector.scoring.selection_pipeline import (
        build_selection_for_date as implementation,
    )

    return implementation(*args, **kwargs)


def validate_selection_result(*args, **kwargs):
    from stock_selector.scoring.selection_validator import (
        validate_selection_result as implementation,
    )

    return implementation(*args, **kwargs)


def BacktestConfig(*args, **kwargs):
    from stock_selector.backtesting.backtest_pipeline import BacktestConfig as implementation

    return implementation(*args, **kwargs)


def run_backtest(*args, **kwargs):
    from stock_selector.backtesting.backtest_pipeline import run_backtest as implementation

    return implementation(*args, **kwargs)


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
        goal13c_preflight=args.goal13c_preflight,
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


def _cmd_build_tushare_daily_price_promotion_validator(args: argparse.Namespace) -> int:
    if args.goal14_max_codes <= 0:
        print("invalid input: goal14_max_codes must be positive", file=sys.stderr)
        return 2
    if args.goal14_max_trade_days <= 0:
        print("invalid input: goal14_max_trade_days must be positive", file=sys.stderr)
        return 2
    if args.goal14_max_rows <= 0:
        print("invalid input: goal14_max_rows must be positive", file=sys.stderr)
        return 2
    if (args.start_date is None) ^ (args.end_date is None):
        print("invalid input: start_date and end_date must be provided together", file=sys.stderr)
        return 2
    try:
        apply_start_date = None
        apply_end_date = None
        if args.start_date is not None and args.end_date is not None:
            apply_start_date, apply_end_date = validate_date_range(args.start_date, args.end_date)
        apply_codes = _parse_codes_arg(args.codes) if args.codes else None
    except (DateValidationError, DataValidationError) as exc:
        print(f"invalid input: {exc}", file=sys.stderr)
        return 2

    batch_id = args.batch_id
    source_object_keys = {
        "promotion_preflight_report": f"candidate/tushare/promotion_preflight_report/batch_id={batch_id}/report.json",
        "daily_price_candidate_batch": f"candidate/tushare/daily_price_candidate_batch/batch_id={batch_id}/part.parquet",
        "suspension_status_candidate_batch": f"candidate/tushare/suspension_status_candidate_batch/batch_id={batch_id}/part.parquet",
    }
    try:
        promotion_preflight_report = _load_tushare_candidate_batch_json(source_object_keys["promotion_preflight_report"])
        daily_price_candidate_batch = _load_tushare_candidate_batch_parquet(source_object_keys["daily_price_candidate_batch"])
        suspension_status_candidate_batch = _load_tushare_candidate_batch_parquet(source_object_keys["suspension_status_candidate_batch"])
    except FileNotFoundError as exc:
        print(json.dumps({"goal": "14", "provider": "tushare", "status": "BLOCKED", "batch_id": batch_id, "blocked_reasons": [str(exc)]}, ensure_ascii=False))
        return 1

    apply_standard_write = bool(args.apply or args.goal14_execute_standard_write)
    result = build_tushare_daily_price_promotion_validator(
        batch_id=batch_id,
        promotion_preflight_report=promotion_preflight_report,
        daily_price_candidate_batch=daily_price_candidate_batch,
        suspension_status_candidate_batch=suspension_status_candidate_batch,
        max_codes=args.goal14_max_codes,
        max_trade_days=args.goal14_max_trade_days,
        max_rows=args.goal14_max_rows,
        request_standard_write=apply_standard_write,
        execute_standard_write=args.goal14_execute_standard_write,
        apply_standard_write=apply_standard_write,
        apply_codes=apply_codes,
        apply_start_date=apply_start_date,
        apply_end_date=apply_end_date,
        standard_daily_price_read_fn=_read_dataset_or_empty,
        standard_daily_price_write_fn=_write_dataset,
        source_object_keys=source_object_keys,
    )
    output_keys = result["output_object_keys"]
    _write_tushare_candidate_batch_json(
        output_keys["daily_price_promotion_validator_report"],
        result["daily_price_promotion_validator_report"],
    )
    _write_tushare_candidate_batch_json(
        output_keys["standard_daily_price_promotion_dry_run_report"],
        result["standard_daily_price_promotion_dry_run_report"],
    )
    if result.get("standard_daily_price_promotion_apply_report") is not None:
        _write_tushare_candidate_batch_json(
            output_keys["standard_daily_price_promotion_apply_report"],
            result["standard_daily_price_promotion_apply_report"],
        )
    output = _tushare_daily_price_promotion_validator_cli_output(result)
    print(json.dumps(output, ensure_ascii=False, default=str))
    return 0 if result["status"] == "VALIDATOR_PASS" else 1


def _cmd_run_tushare_daily_price_small_batch(args: argparse.Namespace) -> int:
    if args.provider_call and args.no_provider_call:
        print("invalid input: provider_call and no_provider_call cannot both be set", file=sys.stderr)
        return 2
    if args.max_codes <= 0:
        print("invalid input: max_codes must be positive", file=sys.stderr)
        return 2
    if args.max_trade_days <= 0:
        print("invalid input: max_trade_days must be positive", file=sys.stderr)
        return 2
    if args.max_rows <= 0:
        print("invalid input: max_rows must be positive", file=sys.stderr)
        return 2
    if args.sleep_seconds < 0:
        print("invalid input: sleep_seconds must be non-negative", file=sys.stderr)
        return 2
    try:
        start_date, end_date = validate_date_range(args.start_date, args.end_date)
        codes = _parse_codes_arg(args.codes)
    except (DateValidationError, DataValidationError) as exc:
        print(f"invalid input: {exc}", file=sys.stderr)
        return 2

    provider_call_enabled = bool(args.provider_call)
    provider = None
    if provider_call_enabled:
        settings = load_settings()
        try:
            provider = TushareProvider(settings=settings)
        except ProviderConfigurationError as exc:
            status = _tushare_provider_config_status(exc)
            result = build_tushare_daily_price_small_batch_blocked_result(
                batch_id=args.batch_id,
                start_date=start_date,
                end_date=end_date,
                codes=codes,
                status=status,
                blocked_reasons=[str(exc)],
                provider_call_enabled=True,
                reuse_existing_staging=args.reuse_existing_staging,
                apply_standard_write=args.apply,
                max_codes=args.max_codes,
                max_trade_days=args.max_trade_days,
                max_rows=args.max_rows,
            )
            _write_tushare_candidate_batch_json(
                result["small_batch_run_report_key"],
                result["small_batch_run_report"],
            )
            print(json.dumps(_tushare_daily_price_small_batch_cli_output(result), ensure_ascii=False, default=str))
            return 1

    try:
        result = run_tushare_daily_price_small_batch(
            batch_id=args.batch_id,
            start_date=start_date,
            end_date=end_date,
            codes=codes,
            provider=provider,
            provider_call_enabled=provider_call_enabled,
            reuse_existing_staging=args.reuse_existing_staging,
            apply_standard_write=args.apply,
            max_codes=args.max_codes,
            max_trade_days=args.max_trade_days,
            max_rows=args.max_rows,
            sleep_seconds=args.sleep_seconds,
            load_parquet_fn=_load_tushare_candidate_batch_parquet,
            load_json_fn=_load_tushare_candidate_batch_json,
            write_parquet_fn=_write_tushare_candidate_batch_parquet,
            write_json_fn=_write_tushare_candidate_batch_json,
            standard_daily_price_read_fn=_read_dataset_or_empty,
            standard_daily_price_write_fn=_write_dataset,
            cli_command=" ".join(sys.argv),
        )
    except (DateValidationError, DataValidationError, ValueError) as exc:
        print(f"invalid input: {exc}", file=sys.stderr)
        return 2

    output = _tushare_daily_price_small_batch_cli_output(result)
    print(json.dumps(output, ensure_ascii=False, default=str))
    return 0 if result["status"] == "VALIDATOR_PASS" else 1


def _cmd_run_tushare_standard_inputs_small_batch(args: argparse.Namespace) -> int:
    if args.provider_call and args.no_provider_call:
        print("invalid input: provider_call and no_provider_call cannot both be set", file=sys.stderr)
        return 2
    if args.max_codes <= 0:
        print("invalid input: max_codes must be positive", file=sys.stderr)
        return 2
    if args.max_trade_days <= 0:
        print("invalid input: max_trade_days must be positive", file=sys.stderr)
        return 2
    if args.max_rows <= 0:
        print("invalid input: max_rows must be positive", file=sys.stderr)
        return 2
    if args.sleep_seconds < 0:
        print("invalid input: sleep_seconds must be non-negative", file=sys.stderr)
        return 2
    try:
        start_date, end_date = validate_date_range(args.start_date, args.end_date)
        codes = _parse_codes_arg(args.codes)
    except (DateValidationError, DataValidationError) as exc:
        print(f"invalid input: {exc}", file=sys.stderr)
        return 2

    provider_call_enabled = bool(args.provider_call)
    provider = None
    if provider_call_enabled:
        settings = load_settings()
        try:
            provider = TushareProvider(settings=settings)
        except ProviderConfigurationError as exc:
            status = _tushare_provider_config_status(exc)
            result = build_tushare_standard_inputs_blocked_result(
                batch_id=args.batch_id,
                start_date=start_date,
                end_date=end_date,
                codes=codes,
                status=status,
                blocked_reasons=[str(exc)],
                provider_call_enabled=True,
                reuse_existing_staging=args.reuse_existing_staging,
                apply_standard_write=args.apply,
                max_codes=args.max_codes,
                max_trade_days=args.max_trade_days,
                max_rows=args.max_rows,
            )
            _write_tushare_candidate_batch_json(
                result["standard_inputs_run_report_key"],
                result["standard_inputs_run_report"],
            )
            print(json.dumps(_tushare_standard_inputs_small_batch_cli_output(result), ensure_ascii=False, default=str))
            return 1

    try:
        result = run_tushare_standard_inputs_small_batch(
            batch_id=args.batch_id,
            start_date=start_date,
            end_date=end_date,
            codes=codes,
            provider=provider,
            provider_call_enabled=provider_call_enabled,
            reuse_existing_staging=args.reuse_existing_staging,
            apply_standard_write=args.apply,
            max_codes=args.max_codes,
            max_trade_days=args.max_trade_days,
            max_rows=args.max_rows,
            sleep_seconds=args.sleep_seconds,
            load_parquet_fn=_load_tushare_candidate_batch_parquet,
            write_parquet_fn=_write_tushare_candidate_batch_parquet,
            write_json_fn=_write_tushare_candidate_batch_json,
            standard_read_fn=_read_dataset_or_empty,
            standard_write_fn=_write_dataset,
            cli_command=" ".join(sys.argv),
        )
    except (DateValidationError, DataValidationError, ValueError) as exc:
        print(f"invalid input: {exc}", file=sys.stderr)
        return 2

    output = _tushare_standard_inputs_small_batch_cli_output(result)
    print(json.dumps(output, ensure_ascii=False, default=str))
    return 0 if result["status"] == "VALIDATION_PASS" else 1


def _cmd_run_real_clean_inputs_small_batch(args: argparse.Namespace) -> int:
    if args.provider_call and args.no_provider_call:
        print("invalid input: provider_call and no_provider_call cannot both be set", file=sys.stderr)
        return 2
    if args.max_codes <= 0:
        print("invalid input: max_codes must be positive", file=sys.stderr)
        return 2
    if args.max_trade_days <= 0:
        print("invalid input: max_trade_days must be positive", file=sys.stderr)
        return 2
    if args.max_rows <= 0:
        print("invalid input: max_rows must be positive", file=sys.stderr)
        return 2
    if args.sleep_seconds < 0:
        print("invalid input: sleep_seconds must be non-negative", file=sys.stderr)
        return 2
    try:
        start_date, end_date = validate_date_range(args.start_date, args.end_date)
        codes = _parse_codes_arg(args.codes)
    except (DateValidationError, DataValidationError) as exc:
        print(f"invalid input: {exc}", file=sys.stderr)
        return 2

    provider_call_enabled = bool(args.provider_call)
    adj_factor_provider = None
    benchmark_provider = None
    provider_errors = {}
    if provider_call_enabled:
        settings = load_settings()
        try:
            adj_factor_provider = TushareProvider(settings=settings)
        except ProviderConfigurationError as exc:
            provider_errors["adj_factor"] = str(exc)
        try:
            benchmark_provider = AKShareProvider(settings=settings)
        except ProviderConfigurationError as exc:
            provider_errors["benchmark_price"] = str(exc)

    try:
        result = run_real_clean_inputs_small_batch(
            batch_id=args.batch_id,
            start_date=start_date,
            end_date=end_date,
            codes=codes,
            adj_factor_provider=adj_factor_provider,
            benchmark_provider=benchmark_provider,
            provider_errors=provider_errors,
            provider_call_enabled=provider_call_enabled,
            reuse_existing_staging=args.reuse_existing_staging,
            apply_standard_write=args.apply,
            max_codes=args.max_codes,
            max_trade_days=args.max_trade_days,
            max_rows=args.max_rows,
            sleep_seconds=args.sleep_seconds,
            load_parquet_fn=_load_tushare_candidate_batch_parquet,
            load_json_fn=_load_tushare_candidate_batch_json,
            write_parquet_fn=_write_tushare_candidate_batch_parquet,
            write_json_fn=_write_tushare_candidate_batch_json,
            standard_read_fn=_read_dataset_or_empty,
            standard_write_fn=_write_dataset,
            cli_command=" ".join(sys.argv),
        )
    except (DateValidationError, DataValidationError, ValueError) as exc:
        print(f"invalid input: {exc}", file=sys.stderr)
        return 2

    output = _real_clean_inputs_small_batch_cli_output(result)
    print(json.dumps(output, ensure_ascii=False, default=str))
    return 0 if result["status"] in {"READY", "READY_FOR_APPLY"} else 1


def _cmd_run_real_history_backfill(args: argparse.Namespace) -> int:
    limits = {
        "code_batch_size": args.code_batch_size,
        "date_batch_days": args.date_batch_days,
        "financial_announce_days": getattr(args, "financial_announce_days", 31),
        "report_period_months": args.report_period_months,
    }
    invalid_limit = next((name for name, value in limits.items() if value <= 0), None)
    if invalid_limit is not None:
        print(f"invalid input: {invalid_limit} must be positive", file=sys.stderr)
        return 2

    try:
        universe_frame = None
        universe_key = None
        codes = None
        if args.codes is not None:
            codes = _parse_history_codes(args.codes)
        else:
            universe_key = _validate_history_universe_key(args.universe_key)
            universe_frame = _load_history_universe_frame(universe_key)

        plan = build_history_backfill_plan_v2(
            run_id=args.run_id,
            start_date=args.start_date,
            end_date=args.end_date,
            codes=codes,
            universe_frame=universe_frame,
            universe_key=universe_key,
            code_batch_size=args.code_batch_size,
            date_batch_days=args.date_batch_days,
            announce_date_batch_days=getattr(args, "financial_announce_days", 31),
        )
        raw_landing_fn = None
        if args.provider_call:
            raw_landing_fn = lambda endpoint, parameters, frame: persist_historical_raw_landing(
                provider_name="tushare",
                run_id=plan["run_id"],
                endpoint=endpoint,
                parameters=parameters,
                frame=frame,
                read_parquet_fn=_load_tushare_candidate_batch_parquet,
                write_parquet_fn=_write_tushare_candidate_batch_parquet,
            )
        fetch_chunk_fn = (
            _build_history_fetch_chunk_fn(plan, raw_landing_fn=raw_landing_fn)
            if args.provider_call
            else None
        )
        canonical_read_fn = _read_history_canonical if args.apply else None
        canonical_write_fn = _write_dataset if args.apply else None
        result = run_real_history_backfill(
            plan=plan,
            artifact_read_json_fn=_load_tushare_candidate_batch_json,
            artifact_write_json_fn=_write_tushare_candidate_batch_json,
            artifact_read_parquet_fn=_load_tushare_candidate_batch_parquet,
            artifact_write_parquet_fn=_write_tushare_candidate_batch_parquet,
            fetch_chunk_fn=fetch_chunk_fn,
            canonical_read_fn=canonical_read_fn,
            canonical_write_fn=canonical_write_fn,
            provider_call_enabled=args.provider_call,
            apply_standard_write=args.apply,
            resume=args.resume,
            force=args.force,
        )
    except (BackfillPlanningError, DateValidationError, DataValidationError, ValueError, FileNotFoundError) as exc:
        print(f"invalid input: {exc}", file=sys.stderr)
        return 2

    output = _real_history_backfill_cli_output(result)
    print(json.dumps(output, ensure_ascii=False, default=str))
    return 0 if output["status"] in {"DRY_RUN", "STAGED", "COMPLETED"} else 1


def _cmd_run_real_clean_universe_range(args: argparse.Namespace) -> int:
    from stock_selector.data.real_clean_input_gate import (
        load_goal22_trusted_input_lineage,
    )
    from stock_selector.data.real_clean_universe import run_real_clean_universe_range

    try:
        start_date, end_date = validate_date_range(args.start_date, args.end_date)
        trade_dates = _parse_goal22_trade_dates(args.trade_dates)
        trusted_input_lineage = load_goal22_trusted_input_lineage(
            readiness_report_keys=args.readiness_report_key,
            start_date=start_date,
            end_date=end_date,
            trade_dates=trade_dates,
            read_json_fn=_load_tushare_candidate_batch_json,
        )
        input_reader = _Goal22CanonicalInputReader(trusted_input_lineage)
        result = run_real_clean_universe_range(
            run_id=args.run_id,
            start_date=start_date,
            end_date=end_date,
            trade_dates=trade_dates,
            trusted_input_lineage=trusted_input_lineage,
            input_read_fn=input_reader.read,
            artifact_read_json_fn=_load_tushare_candidate_batch_json,
            artifact_write_json_fn=_write_tushare_candidate_batch_json,
            processed_read_fn=_read_goal22_processed if args.apply else None,
            processed_object_read_fn=(
                _read_goal22_processed_object if args.apply else None
            ),
            processed_object_write_fn=(
                _write_goal22_processed_object if args.apply else None
            ),
            processed_commit_read_fn=(
                _read_goal22_processed_commit if args.apply else None
            ),
            processed_commit_write_fn=(
                _write_goal22_processed_commit if args.apply else None
            ),
            apply_processed_write=args.apply,
            resume=args.resume,
            force=args.force,
            trade_date_source="EXPLICIT_CLI_GOAL20_RECEIPT",
        )
    except (DateValidationError, DataValidationError, DatasetValidationError, ValueError, FileNotFoundError) as exc:
        print(f"invalid input: {exc}", file=sys.stderr)
        return 2

    output = _real_clean_universe_range_cli_output(result)
    print(json.dumps(output, ensure_ascii=False, default=str))
    return 0 if output["status"] in {"READY_FOR_APPLY", "COMPLETED"} else 1


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


def _load_tushare_candidate_batch_json(object_key: str) -> dict:
    with tempfile.TemporaryDirectory(prefix="stock-tushare-candidate-batch-json-read-") as tmp:
        path = _try_materialize_object_key(object_key, Path(tmp))
        if not path:
            raise FileNotFoundError(f"missing candidate staging json: {object_key}")
        return json.loads(path.read_text(encoding="utf-8"))


def _tushare_candidate_staging_batch_cli_output(result: dict) -> dict:
    keys = result.get("output_object_keys", {})
    dq3_audit = result.get("dq3_readiness_audit", {})
    return {
        "provider": "tushare",
        "goal": result.get("goal", "13B"),
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
        "suspend_d_full_coverage_report_key": keys.get("suspend_d_full_coverage_report"),
        "promotion_preflight_report_key": keys.get("promotion_preflight_report"),
        "daily_price_candidate_batch_key": keys.get("daily_price_candidate_batch"),
        "suspension_status_candidate_batch_key": keys.get("suspension_status_candidate_batch"),
        "output_object_keys": keys,
        "staging_row_counts": result.get("staging_row_counts", {}),
        "daily_price_candidate_row_count": result.get("daily_price_candidate_row_count", 0),
        "suspension_status_candidate_row_count": result.get("suspension_status_candidate_row_count", 0),
        "coverage_summary": result.get("coverage_summary", {}),
        "pause_status_counts": result.get("pause_status_counts", {}),
        "readiness_status": dq3_audit.get("status") or result["status"],
        "promotion_preflight_status": result.get("promotion_preflight_status"),
        "ready_for_promotion_validator": result.get("ready_for_promotion_validator", False),
        "ready_for_dq3_promotion": result.get("ready_for_dq3_promotion", False),
        "blocked_reasons": result.get("blocked_reasons", []),
        "coverage_expansion": result.get("coverage_expansion", False),
        "fetch_semantics_audit": result.get("fetch_semantics_audit", False),
        "reused_existing_staging": result.get("reused_existing_staging", False),
        "safety": result.get("safety", {}),
        "inference_guards": result.get("inference_guards", {}),
    }


def _tushare_daily_price_promotion_validator_cli_output(result: dict) -> dict:
    keys = result.get("output_object_keys", {})
    dry_run = result.get("standard_daily_price_promotion_dry_run_report", {})
    apply_report = result.get("standard_daily_price_promotion_apply_report") or {}
    read_back_verification = result.get("read_back_verification") or apply_report.get("read_back_verification")
    return {
        "provider": "tushare",
        "goal": result.get("goal", "14"),
        "status": result["status"],
        "batch_id": result["batch_id"],
        "mode": apply_report.get("mode", dry_run.get("mode", "DRY_RUN")),
        "daily_price_promotion_validator_report_key": keys.get("daily_price_promotion_validator_report"),
        "standard_daily_price_promotion_dry_run_report_key": keys.get("standard_daily_price_promotion_dry_run_report"),
        "standard_daily_price_promotion_execution_report_key": keys.get("standard_daily_price_promotion_execution_report"),
        "standard_daily_price_promotion_apply_report_key": keys.get("standard_daily_price_promotion_apply_report"),
        "output_object_keys": keys,
        "candidate_row_count": result.get("candidate_row_count", 0),
        "would_insert_rows": dry_run.get("would_insert_rows", 0),
        "would_update_rows": dry_run.get("would_update_rows", 0),
        "would_skip_rows": dry_run.get("would_skip_rows", 0),
        "upsert_summary": result.get("upsert_summary", {}),
        "read_back_verification": read_back_verification,
        "standard_daily_price_write_performed": result.get("standard_daily_price_write_performed", False),
        "standard_suspension_status_write_performed": result.get("standard_suspension_status_write_performed", False),
        "real_backtest_performed": result.get("real_backtest_performed", False),
        "clean_factor_selection_backtest_entered": result.get("clean_factor_selection_backtest_entered", False),
        "blocked_reasons": result.get("blocked_reasons", []),
    }


def _tushare_daily_price_small_batch_cli_output(result: dict) -> dict:
    keys = result.get("output_object_keys", {})
    return {
        "provider": "tushare",
        "goal": "17",
        "status": result["status"],
        "batch_id": result["batch_id"],
        "mode": result.get("mode", "DRY_RUN"),
        "small_batch_run_report_key": result.get("small_batch_run_report_key") or keys.get("small_batch_run_report"),
        "daily_price_promotion_validator_report_key": result.get("daily_price_promotion_validator_report_key"),
        "standard_daily_price_promotion_dry_run_report_key": result.get("standard_daily_price_promotion_dry_run_report_key"),
        "standard_daily_price_promotion_apply_report_key": result.get("standard_daily_price_promotion_apply_report_key"),
        "output_object_keys": keys,
        "provider_call_requested": result.get("provider_call_requested", False),
        "reused_existing_staging": result.get("reused_existing_staging", False),
        "apply_requested": result.get("apply_requested", False),
        "standard_daily_price_write_performed": result.get("standard_daily_price_write_performed", False),
        "standard_suspension_status_write_performed": result.get("standard_suspension_status_write_performed", False),
        "clean_factor_selection_backtest_entered": result.get("clean_factor_selection_backtest_entered", False),
        "real_backtest_performed": result.get("real_backtest_performed", False),
        "read_back_verification": result.get("read_back_verification"),
        "upsert_summary": result.get("upsert_summary", {}),
        "blocked_reasons": result.get("blocked_reasons", []),
    }


def _tushare_standard_inputs_small_batch_cli_output(result: dict) -> dict:
    keys = result.get("output_object_keys", {})
    return {
        "provider": "tushare",
        "goal": "18",
        "status": result["status"],
        "batch_id": result["batch_id"],
        "mode": result.get("mode", "DRY_RUN"),
        "standard_inputs_run_report_key": result.get("standard_inputs_run_report_key") or keys.get("standard_inputs_run_report"),
        "output_object_keys": keys,
        "provider_call_requested": result.get("provider_call_requested", False),
        "reused_existing_staging": result.get("reused_existing_staging", False),
        "apply_requested": result.get("apply_requested", False),
        "standard_writes_performed": result.get("standard_writes_performed", False),
        "standard_suspension_status_write_performed": result.get("standard_suspension_status_write_performed", False),
        "clean_factor_selection_backtest_entered": result.get("clean_factor_selection_backtest_entered", False),
        "real_backtest_performed": result.get("real_backtest_performed", False),
        "read_back_verification": result.get("read_back_verification"),
        "upsert_summary": result.get("upsert_summary", {}),
        "blocked_reasons": result.get("blocked_reasons", []),
    }


def _real_clean_inputs_small_batch_cli_output(result: dict) -> dict:
    keys = result.get("output_object_keys", {})
    return {
        "goal": "20",
        "status": result["status"],
        "batch_id": result["batch_id"],
        "mode": result.get("mode", "DRY_RUN"),
        "provider_call_requested": result.get("provider_call_requested", False),
        "reused_existing_staging": result.get("reused_existing_staging", False),
        "apply_requested": result.get("apply_requested", False),
        "standard_writes_performed": result.get("standard_writes_performed", False),
        "ready_for_apply": result.get("ready_for_apply", False),
        "ready_for_clean": result.get("ready_for_clean", False),
        "readiness_report_key": result.get("readiness_report_key") or keys.get("readiness_report"),
        "manifest_key": result.get("manifest_key") or keys.get("manifest"),
        "output_object_keys": keys,
        "upsert_summary": result.get("upsert_summary", {}),
        "read_back_verification": result.get("read_back_verification"),
        "clean_factor_selection_backtest_entered": result.get("clean_factor_selection_backtest_entered", False),
        "real_backtest_performed": result.get("real_backtest_performed", False),
        "blocked_reasons": result.get("blocked_reasons", []),
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


def _parse_history_codes(value: str | None) -> list[str]:
    if not isinstance(value, str):
        raise ValueError("codes must be provided explicitly")
    codes = []
    for item in value.split(","):
        code = item.strip().upper()
        if code:
            codes.append(validate_stock_code(code))
    if not codes:
        raise ValueError("codes must contain at least one stock code")
    return codes


def _parse_goal22_trade_dates(value: str) -> list[str]:
    trade_dates = sorted(
        {
            validate_trade_date(item.strip())
            for item in value.split(",")
            if item.strip()
        }
    )
    if not trade_dates:
        raise ValueError("trade_dates must contain at least one date")
    return trade_dates


class _Goal22CanonicalInputReader:
    def __init__(self, trusted_input_lineage: dict) -> None:
        self._lineage = trusted_input_lineage
        self._codes = set(trusted_input_lineage["codes"])
        self._frames: dict[str, pd.DataFrame] = {}

    def read(self, dataset: str, trade_date: str):
        from stock_selector.data.real_clean_universe import INPUT_KEY_COLUMNS, InputArtifact, InputVersion

        dataset = validate_dataset(dataset)
        trade_date = validate_trade_date(trade_date)
        try:
            trusted = self._lineage["canonical_versions"][trade_date][dataset]
        except KeyError as exc:
            raise FileNotFoundError(
                f"trusted canonical version is missing for {dataset} {trade_date}"
            ) from exc
        object_key = trusted["object_key"]
        object_frame = self._read_object(object_key)
        object_checksum = dataframe_checksum(object_frame)
        if (
            len(object_frame) != trusted["object_row_count"]
            or object_checksum != trusted["object_checksum"]
        ):
            raise DataValidationError(
                f"canonical object version changed after Goal 20 readiness: {object_key}"
            )
        if dataset == "benchmark_price":
            scoped = object_frame.copy(deep=True)
        else:
            scoped = object_frame.loc[
                object_frame["stock_code"].astype(str).isin(self._codes)
            ].reset_index(drop=True)
        if (
            len(scoped) != trusted["scope_row_count"]
            or dataframe_checksum(scoped) != trusted["scope_checksum"]
        ):
            raise DataValidationError(
                f"canonical audited scope changed after Goal 20 readiness: {object_key}"
            )
        keys = INPUT_KEY_COLUMNS[dataset]
        if not scoped.empty and scoped.duplicated(keys).any():
            raise DataValidationError(
                f"duplicate logical keys in canonical input {object_key}"
            )
        version = InputVersion(
            object_key=object_key,
            row_count=len(object_frame),
            checksum=object_checksum,
        )
        return InputArtifact(frame=scoped, versions=(version,))

    def _read_object(self, object_key: str) -> pd.DataFrame:
        if object_key not in self._frames:
            self._frames[object_key] = _load_tushare_candidate_batch_parquet(object_key)
        return self._frames[object_key].copy(deep=True)


def _validate_history_universe_key(value: str | None) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError("universe_key must be a non-empty Parquet object key")
    if value != value.strip():
        raise ValueError("universe_key must not contain leading or trailing whitespace")
    if len(value) >= 2 and value[0].isalpha() and value[1] == ":":
        raise ValueError("universe_key must not use a drive-qualified path")
    normalized = safe_object_key(value)
    if not normalized.lower().endswith(".parquet"):
        raise ValueError("universe_key must reference a .parquet object")
    return normalized


def _load_history_universe_frame(object_key: str) -> pd.DataFrame:
    return _load_tushare_candidate_batch_parquet(object_key)


def _build_history_fetch_chunk_fn(plan: dict, *, raw_landing_fn=None):
    provider_holder: dict[str, TushareProvider] = {}
    router_holder: dict[str, HistoricalProviderRouter] = {}

    def provider() -> TushareProvider:
        if "tushare" not in provider_holder:
            provider_holder["tushare"] = TushareProvider(settings=load_settings())
        return provider_holder["tushare"]

    def raw_fetch(endpoint: str, parameters: dict) -> pd.DataFrame:
        return provider().fetch_raw_endpoint_allow_empty(endpoint, **parameters)

    def trading_calendar(start_date: str, end_date: str) -> pd.DataFrame:
        return provider().fetch_raw_endpoint_allow_empty(
            "trade_cal",
            exchange="SSE",
            start_date=start_date.replace("-", ""),
            end_date=end_date.replace("-", ""),
            fields="cal_date,is_open",
        )

    # The shipped live route is deliberately narrow: Tushare has audited
    # historical semantics for daily_price, adj_factor and daily_basic only.
    # The router returns explicit SEMANTIC_SOURCE_UNAVAILABLE evidence for the
    # remaining datasets instead of constructing current/smoke substitutes.
    def fetch_chunk(chunk: dict):
        if "tushare" not in router_holder:
            router_holder["tushare"] = HistoricalProviderRouter(
                plan=plan,
                provider_name="tushare",
                raw_fetch_fn=raw_fetch,
                trading_calendar_fn=trading_calendar,
                raw_landing_fn=raw_landing_fn,
            )
        return router_holder["tushare"].fetch_chunk(chunk)

    return fetch_chunk


def _real_history_backfill_cli_output(result: dict) -> dict:
    gates = result.get("gates", {})
    provider_requested = gates.get("provider_call_enabled") is True
    apply_requested = gates.get("apply_standard_write") is True
    if provider_requested and apply_requested:
        mode = "PROVIDER_AND_APPLY"
    elif provider_requested:
        mode = "PROVIDER_ONLY"
    elif apply_requested:
        mode = "APPLY_ONLY"
    else:
        mode = "PLAN_ONLY"

    summary = result.get("summary", {})
    counts = summary.get("state_counts", {})
    planned = int(summary.get("planned", 0))
    if not provider_requested and not apply_requested:
        status = "DRY_RUN"
    elif int(counts.get("INTERRUPTED", 0)) > 0:
        status = "INTERRUPTED"
    elif int(counts.get("FAILED", 0)) > 0:
        status = "FAILED"
    elif int(counts.get("BLOCKED", 0)) > 0:
        status = "BLOCKED"
    elif apply_requested and summary.get("canonical_ready") is True:
        status = "COMPLETED"
    elif provider_requested and not apply_requested and (
        int(counts.get("STAGED", 0)) + int(counts.get("COMPLETED", 0)) == planned
    ):
        status = "STAGED"
    else:
        status = "INCOMPLETE"

    gaps = summary.get("gaps", [])
    blocked_reasons = sorted(
        {
            str(gap.get("reason"))
            for gap in gaps
            if isinstance(gap, dict) and gap.get("reason")
        }
    )
    failure_categories = sorted(
        {
            str(gap.get("category"))
            for gap in gaps
            if isinstance(gap, dict) and gap.get("category")
        }
    )
    return {
        "goal": result.get("goal", "Goal 21 resumable historical backfill"),
        "status": status,
        "mode": mode,
        "run_id": result.get("run_id"),
        "plan_fingerprint": result.get("plan_fingerprint"),
        "provider_call_requested": provider_requested,
        "apply_requested": apply_requested,
        "resume": gates.get("resume") is True,
        "force": gates.get("force") is True,
        "plan_key": result.get("plan_key"),
        "root_manifest_key": result.get("root_manifest_key"),
        "planned_chunks": planned,
        "state_counts": counts,
        "completion_rate": summary.get("completion_rate", 0.0),
        "canonical_ready": summary.get("canonical_ready") is True,
        "attempted_chunk_count": len(result.get("attempted_chunk_ids", [])),
        "skipped_chunk_count": len(result.get("skipped_chunk_ids", [])),
        "reconciled_chunk_count": len(result.get("reconciled_chunk_ids", [])),
        "blocked_reasons": blocked_reasons,
        "failure_categories": failure_categories,
        "gap_count": len(gaps),
        "downstream_firewalls": result.get("downstream_firewalls", {}),
    }


def _real_clean_universe_range_cli_output(result: dict) -> dict:
    return {
        "goal": "22",
        "run_id": result.get("run_id"),
        "status": result.get("status"),
        "mode": result.get("mode", "DRY_RUN"),
        "apply_requested": result.get("apply_requested", False),
        "date_statuses": result.get("date_statuses", {}),
        "status_counts": result.get("status_counts", {}),
        "range_manifest_key": result.get("range_manifest_key"),
        "daily_report_keys": result.get("daily_report_keys", {}),
        "processed_output_keys": result.get("processed_output_keys", {}),
        "processed_commit_keys": result.get("processed_commit_keys", {}),
        "downstream_firewalls": result.get("downstream_firewalls", {}),
    }


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


def _read_dataset_or_empty(dataset: str, trade_date: str) -> pd.DataFrame:
    try:
        return _read_dataset(dataset, trade_date)
    except FileNotFoundError:
        return pd.DataFrame()


def _read_history_canonical(dataset: str, trade_date: str) -> pd.DataFrame | None:
    try:
        return _read_dataset(dataset, trade_date)
    except FileNotFoundError:
        return None


def _read_goal22_processed(dataset: str, trade_date: str) -> pd.DataFrame:
    from stock_selector.data.real_clean_universe import (
        OUTPUT_KEY_COLUMNS,
    )

    dataset = validate_dataset(dataset)
    trade_date = validate_trade_date(trade_date)
    commit = _read_goal22_processed_commit(trade_date)
    _validate_goal22_processed_commit_payload(commit, trade_date)
    record = commit["outputs"][dataset]
    expected_object_key = record["object_key"]
    frame = _read_goal22_processed_object(expected_object_key)
    validate_dataset_frame(dataset, frame, trade_date)
    checksum = dataframe_checksum(
        frame,
        key_columns=OUTPUT_KEY_COLUMNS[dataset],
    )
    if len(frame) != record.get("row_count") or checksum != record.get("checksum"):
        raise DataValidationError(
            f"Goal 22 committed output checksum mismatch for {dataset} {trade_date}"
        )
    return frame


def _read_goal22_processed_object(object_key: str) -> pd.DataFrame:
    object_key = _validate_goal22_generation_object_key(object_key)
    settings = load_settings()
    if _storage_backend(settings) == "local":
        path = _local_root(settings) / object_key
        if not path.exists():
            raise FileNotFoundError(object_key)
        return pd.read_parquet(path)

    client = create_minio_client(settings)
    bucket = settings["storage"]["minio_bucket_processed"]
    with tempfile.TemporaryDirectory(prefix="stock-goal22-processed-read-") as tmp:
        target = Path(tmp) / object_key
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            client.fget_object(bucket, object_key, str(target))
        except S3Error as exc:
            if exc.code in {"NoSuchKey", "NoSuchBucket", "NoSuchObject"}:
                raise FileNotFoundError(object_key) from exc
            raise
        return pd.read_parquet(target)


def _write_goal22_processed_object(
    dataset: str,
    trade_date: str,
    generation_id: str,
    frame: pd.DataFrame,
) -> str:
    from stock_selector.data.real_clean_universe import (
        build_goal22_processed_generation_key,
    )

    dataset = validate_dataset(dataset)
    trade_date = validate_trade_date(trade_date)
    validate_dataset_frame(dataset, frame, trade_date)
    object_key = build_goal22_processed_generation_key(
        dataset,
        trade_date,
        generation_id,
    )
    settings = load_settings()
    if _storage_backend(settings) == "local":
        write_parquet_local_atomic(frame, _local_root(settings) / object_key)
        return object_key

    client = create_minio_client(settings)
    bucket = settings["storage"]["minio_bucket_processed"]
    ensure_buckets(client, [bucket])
    with tempfile.TemporaryDirectory(prefix="stock-goal22-processed-write-") as tmp:
        local_file = Path(tmp) / "part.parquet"
        frame.to_parquet(local_file, index=False)
        AtomicObjectWriter(client, tmp_dir=Path(tmp)).write_file_atomic(bucket, object_key, local_file)
    return object_key


def _read_goal22_processed_commit(trade_date: str) -> dict:
    from stock_selector.data.real_clean_universe import (
        build_goal22_processed_commit_key,
    )

    trade_date = validate_trade_date(trade_date)
    object_key = build_goal22_processed_commit_key(trade_date)
    settings = load_settings()
    if _storage_backend(settings) == "local":
        path = _local_root(settings) / object_key
        if not path.exists():
            raise FileNotFoundError(object_key)
        return json.loads(path.read_text(encoding="utf-8"))

    client = create_minio_client(settings)
    bucket = settings["storage"]["minio_bucket_processed"]
    with tempfile.TemporaryDirectory(prefix="stock-goal22-commit-read-") as tmp:
        target = Path(tmp) / "commit.json"
        try:
            client.fget_object(bucket, object_key, str(target))
        except S3Error as exc:
            if exc.code in {"NoSuchKey", "NoSuchBucket", "NoSuchObject"}:
                raise FileNotFoundError(object_key) from exc
            raise
        return json.loads(target.read_text(encoding="utf-8"))


def _write_goal22_processed_commit(trade_date: str, payload: dict) -> str:
    from stock_selector.data.real_clean_universe import (
        build_goal22_processed_commit_key,
    )

    trade_date = validate_trade_date(trade_date)
    object_key = build_goal22_processed_commit_key(trade_date)
    _validate_goal22_processed_commit_payload(payload, trade_date)

    settings = load_settings()
    if _storage_backend(settings) == "local":
        path = _local_root(settings) / object_key
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_name(path.name + ".tmp")
        tmp_path.write_text(
            json.dumps(payload, ensure_ascii=False, default=str, indent=2),
            encoding="utf-8",
        )
        tmp_path.replace(path)
        return object_key

    client = create_minio_client(settings)
    bucket = settings["storage"]["minio_bucket_processed"]
    ensure_buckets(client, [bucket])
    with tempfile.TemporaryDirectory(prefix="stock-goal22-commit-write-") as tmp:
        local_file = Path(tmp) / "commit.json"
        local_file.write_text(
            json.dumps(payload, ensure_ascii=False, default=str, indent=2),
            encoding="utf-8",
        )
        AtomicObjectWriter(client, tmp_dir=Path(tmp)).write_file_atomic(
            bucket,
            object_key,
            local_file,
        )
    return object_key


def _validate_goal22_processed_commit_payload(
    payload: dict,
    trade_date: str,
) -> None:
    from stock_selector.data.real_clean_universe import (
        OUTPUT_DATASETS,
        build_goal22_processed_generation_key,
    )

    expected_firewalls = {
        "factor_daily": False,
        "selection_result": False,
        "backtest": False,
    }
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != "goal22.processed_date_commit.v1"
        or payload.get("goal") != "22"
        or payload.get("status") != "COMMITTED"
        or payload.get("trade_date") != trade_date
        or payload.get("downstream_firewalls") != expected_firewalls
        or set(payload.get("outputs", {})) != set(OUTPUT_DATASETS)
        or re.fullmatch(r"[0-9a-f]{64}", str(payload.get("generation_id", "")))
        is None
        or re.fullmatch(
            r"[0-9a-f]{64}",
            str(payload.get("plan_fingerprint", "")),
        )
        is None
        or re.fullmatch(
            r"[0-9a-f]{64}",
            str(payload.get("input_fingerprint", "")),
        )
        is None
    ):
        raise DataValidationError("invalid Goal 22 processed commit payload")
    generation_id = payload["generation_id"]
    for dataset in OUTPUT_DATASETS:
        record = payload["outputs"][dataset]
        expected_object_key = build_goal22_processed_generation_key(
            dataset,
            trade_date,
            generation_id,
        )
        expected_logical_key = (
            f"processed/{dataset}/trade_date={trade_date}/part.parquet"
        )
        if (
            not isinstance(record, dict)
            or record.get("object_key") != expected_object_key
            or record.get("logical_key") != expected_logical_key
            or isinstance(record.get("row_count"), bool)
            or not isinstance(record.get("row_count"), int)
            or record["row_count"] < 0
            or re.fullmatch(r"[0-9a-f]{64}", str(record.get("checksum", "")))
            is None
        ):
            raise DataValidationError(
                f"invalid Goal 22 generation mapping for {dataset}"
            )


def _validate_goal22_generation_object_key(object_key: str) -> str:
    object_key = safe_object_key(object_key)
    match = re.fullmatch(
        r"processed/([^/]+)/trade_date=(\d{4}-\d{2}-\d{2})/"
        r"generation=([0-9a-f]{64})/part\.parquet",
        object_key,
    )
    if match is None:
        raise ValueError("invalid Goal 22 processed generation key")
    dataset = validate_dataset(match.group(1))
    if dataset not in {
        "adjusted_price",
        "clean_daily_snapshot",
        "risk_filter",
        "eligible_universe",
        "factor_input_table",
    }:
        raise ValueError("unsupported Goal 22 processed generation dataset")
    validate_trade_date(match.group(2))
    return object_key


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
    build_tushare_candidate_staging_batch.add_argument("--goal13c-preflight", action="store_true")
    build_tushare_candidate_staging_batch.add_argument("--fail-on-incomplete-critical-coverage", action="store_true")
    build_tushare_candidate_staging_batch.add_argument("--write-candidate", action="store_true", default=True)
    build_tushare_candidate_staging_batch.set_defaults(func=_cmd_build_tushare_candidate_staging_batch)

    build_tushare_daily_price_promotion_validator = subparsers.add_parser("build-tushare-daily-price-promotion-validator")
    build_tushare_daily_price_promotion_validator.add_argument("--batch-id", required=True)
    build_tushare_daily_price_promotion_validator.add_argument("--goal14-max-codes", type=int, default=5)
    build_tushare_daily_price_promotion_validator.add_argument("--goal14-max-trade-days", type=int, default=10)
    build_tushare_daily_price_promotion_validator.add_argument("--goal14-max-rows", type=int, default=50)
    build_tushare_daily_price_promotion_validator.add_argument("--apply", action="store_true")
    build_tushare_daily_price_promotion_validator.add_argument("--codes")
    build_tushare_daily_price_promotion_validator.add_argument("--start-date")
    build_tushare_daily_price_promotion_validator.add_argument("--end-date")
    build_tushare_daily_price_promotion_validator.add_argument("--goal14-execute-standard-write", action="store_true")
    build_tushare_daily_price_promotion_validator.set_defaults(func=_cmd_build_tushare_daily_price_promotion_validator)

    run_tushare_daily_price_small_batch = subparsers.add_parser("run-tushare-daily-price-small-batch")
    run_tushare_daily_price_small_batch.add_argument("--batch-id", required=True)
    run_tushare_daily_price_small_batch.add_argument("--codes", default=",".join(DEFAULT_GOAL13_CODES))
    run_tushare_daily_price_small_batch.add_argument("--start-date", required=True)
    run_tushare_daily_price_small_batch.add_argument("--end-date", required=True)
    run_tushare_daily_price_small_batch.add_argument("--apply", action="store_true")
    run_tushare_daily_price_small_batch.add_argument("--provider-call", action="store_true")
    run_tushare_daily_price_small_batch.add_argument("--no-provider-call", action="store_true")
    run_tushare_daily_price_small_batch.add_argument("--reuse-existing-staging", action="store_true")
    run_tushare_daily_price_small_batch.add_argument("--max-codes", type=int, default=5)
    run_tushare_daily_price_small_batch.add_argument("--max-trade-days", type=int, default=10)
    run_tushare_daily_price_small_batch.add_argument("--max-rows", type=int, default=50)
    run_tushare_daily_price_small_batch.add_argument("--sleep-seconds", type=float, default=12.0)
    run_tushare_daily_price_small_batch.set_defaults(func=_cmd_run_tushare_daily_price_small_batch)

    run_tushare_standard_inputs_small_batch = subparsers.add_parser("run-tushare-standard-inputs-small-batch")
    run_tushare_standard_inputs_small_batch.add_argument("--batch-id", required=True)
    run_tushare_standard_inputs_small_batch.add_argument("--codes", default=",".join(DEFAULT_GOAL13_CODES))
    run_tushare_standard_inputs_small_batch.add_argument("--start-date", required=True)
    run_tushare_standard_inputs_small_batch.add_argument("--end-date", required=True)
    run_tushare_standard_inputs_small_batch.add_argument("--apply", action="store_true")
    run_tushare_standard_inputs_small_batch.add_argument("--provider-call", action="store_true")
    run_tushare_standard_inputs_small_batch.add_argument("--no-provider-call", action="store_true")
    run_tushare_standard_inputs_small_batch.add_argument("--reuse-existing-staging", action="store_true")
    run_tushare_standard_inputs_small_batch.add_argument("--max-codes", type=int, default=5)
    run_tushare_standard_inputs_small_batch.add_argument("--max-trade-days", type=int, default=5)
    run_tushare_standard_inputs_small_batch.add_argument("--max-rows", type=int, default=50)
    run_tushare_standard_inputs_small_batch.add_argument("--sleep-seconds", type=float, default=12.0)
    run_tushare_standard_inputs_small_batch.set_defaults(func=_cmd_run_tushare_standard_inputs_small_batch)

    run_real_clean_inputs_small_batch = subparsers.add_parser("run-real-clean-inputs-small-batch")
    run_real_clean_inputs_small_batch.add_argument("--batch-id", required=True)
    run_real_clean_inputs_small_batch.add_argument("--codes", default=",".join(DEFAULT_GOAL13_CODES[:5]))
    run_real_clean_inputs_small_batch.add_argument("--start-date", required=True)
    run_real_clean_inputs_small_batch.add_argument("--end-date", required=True)
    run_real_clean_inputs_small_batch.add_argument("--apply", action="store_true")
    run_real_clean_inputs_small_batch.add_argument("--provider-call", action="store_true")
    run_real_clean_inputs_small_batch.add_argument("--no-provider-call", action="store_true")
    run_real_clean_inputs_small_batch.add_argument("--reuse-existing-staging", action="store_true")
    run_real_clean_inputs_small_batch.add_argument("--max-codes", type=int, default=5)
    run_real_clean_inputs_small_batch.add_argument("--max-trade-days", type=int, default=5)
    run_real_clean_inputs_small_batch.add_argument("--max-rows", type=int, default=100)
    run_real_clean_inputs_small_batch.add_argument("--sleep-seconds", type=float, default=12.0)
    run_real_clean_inputs_small_batch.set_defaults(func=_cmd_run_real_clean_inputs_small_batch)

    run_real_history_backfill = subparsers.add_parser(
        "run-real-history-backfill",
        allow_abbrev=False,
    )
    run_real_history_backfill.add_argument("--run-id", required=True)
    run_real_history_backfill.add_argument("--start-date", required=True)
    run_real_history_backfill.add_argument("--end-date", required=True)
    history_scope = run_real_history_backfill.add_mutually_exclusive_group(required=True)
    history_scope.add_argument("--codes")
    history_scope.add_argument("--universe-key")
    run_real_history_backfill.add_argument("--provider-call", action="store_true")
    run_real_history_backfill.add_argument("--apply", action="store_true")
    history_resume = run_real_history_backfill.add_mutually_exclusive_group()
    history_resume.add_argument("--resume", dest="resume", action="store_true")
    history_resume.add_argument("--no-resume", dest="resume", action="store_false")
    run_real_history_backfill.add_argument("--force", action="store_true")
    run_real_history_backfill.add_argument("--code-batch-size", type=int, default=250)
    run_real_history_backfill.add_argument("--date-batch-days", type=int, default=31)
    run_real_history_backfill.add_argument("--financial-announce-days", type=int, default=31)
    # Retained only so existing v1 runbooks fail neither parsing nor audit;
    # v2 never reinterprets this report-period option as announcement scope.
    run_real_history_backfill.add_argument("--report-period-months", type=int, default=3)
    run_real_history_backfill.set_defaults(resume=True, func=_cmd_run_real_history_backfill)

    run_real_clean_universe = subparsers.add_parser(
        "run-real-clean-universe-range",
        allow_abbrev=False,
    )
    run_real_clean_universe.add_argument("--run-id", required=True)
    run_real_clean_universe.add_argument("--start-date", required=True)
    run_real_clean_universe.add_argument("--end-date", required=True)
    run_real_clean_universe.add_argument(
        "--trade-dates",
        required=True,
        help="comma-separated authoritative trading dates covered by Goal 20 readiness receipts",
    )
    run_real_clean_universe.add_argument(
        "--readiness-report-key",
        action="append",
        required=True,
        help="trusted Goal 20 readiness report key; repeat for multiple audited date batches",
    )
    run_real_clean_universe.add_argument("--apply", action="store_true")
    goal22_resume = run_real_clean_universe.add_mutually_exclusive_group()
    goal22_resume.add_argument("--resume", dest="resume", action="store_true")
    goal22_resume.add_argument("--no-resume", dest="resume", action="store_false")
    run_real_clean_universe.add_argument("--force", action="store_true")
    run_real_clean_universe.set_defaults(resume=True, func=_cmd_run_real_clean_universe_range)

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

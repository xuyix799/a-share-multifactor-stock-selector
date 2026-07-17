from __future__ import annotations

from collections.abc import Callable, Iterable
from copy import deepcopy
import hashlib
import json
import re
from typing import Any

from stock_selector.data.data_validator import validate_stock_code
from stock_selector.utils.date_validator import validate_date_range, validate_trade_date
from stock_selector.utils.path_validator import safe_object_key


REQUIRED_INPUTS = (
    "stock_basic",
    "daily_price",
    "adj_factor",
    "daily_basic",
    "financial",
    "st_history",
    "benchmark_price",
)

_READINESS_KEY_PATTERN = re.compile(
    r"^candidate/real_clean_inputs/readiness_report/"
    r"batch_id=([A-Za-z0-9][A-Za-z0-9._-]{0,127})/report\.json$"
)
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


ReadJsonFn = Callable[[str], dict[str, Any]]


def readiness_payload_checksum(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def load_goal22_trusted_input_lineage(
    *,
    readiness_report_keys: Iterable[str],
    start_date: str,
    end_date: str,
    trade_dates: Iterable[str],
    read_json_fn: ReadJsonFn,
) -> dict[str, Any]:
    start_date, end_date = validate_date_range(start_date, end_date)
    selected_dates = _normalize_trade_dates(trade_dates)
    if not selected_dates:
        raise ValueError("trade_dates must contain at least one authoritative date")
    if selected_dates[0] < start_date or selected_dates[-1] > end_date:
        raise ValueError("trade_dates must be within start_date and end_date")

    report_keys = _normalize_report_keys(readiness_report_keys)
    receipts = [
        _load_and_validate_receipt(report_key, read_json_fn)
        for report_key in report_keys
    ]
    for receipt in receipts:
        if not set(receipt["trade_dates"]) & set(selected_dates):
            raise ValueError(
                "readiness report does not cover any requested trade date: "
                f"{receipt['readiness_report_key']}"
            )

    contributing = [
        receipt
        for receipt in receipts
        if set(receipt["trade_dates"]) & set(selected_dates)
    ]
    expected_codes = contributing[0]["codes"]
    for receipt in contributing[1:]:
        if receipt["codes"] != expected_codes:
            raise ValueError(
                "all Goal 20 readiness reports must audit the same exact code scope"
            )

    canonical_versions: dict[str, dict[str, dict[str, Any]]] = {}
    for trade_date in selected_dates:
        covering = [
            receipt for receipt in contributing if trade_date in receipt["trade_dates"]
        ]
        if not covering:
            raise ValueError(
                f"Goal 20 readiness reports do not cover trade_date {trade_date}"
            )
        canonical_versions[trade_date] = {}
        for dataset in REQUIRED_INPUTS:
            candidates = [
                receipt["canonical_versions"][trade_date][dataset]
                for receipt in covering
            ]
            expected = candidates[0]
            if any(candidate != expected for candidate in candidates[1:]):
                raise ValueError(
                    "conflicting Goal 20 canonical version evidence for "
                    f"{dataset} {trade_date}"
                )
            canonical_versions[trade_date][dataset] = deepcopy(expected)

    lineage = {
        "schema_version": "goal22.trusted_input_lineage.v1",
        "trade_dates": selected_dates,
        "codes": list(expected_codes),
        "readiness_receipts": [
            {
                "batch_id": receipt["batch_id"],
                "readiness_report_key": receipt["readiness_report_key"],
                "readiness_report_checksum": receipt["readiness_report_checksum"],
                "manifest_key": receipt["manifest_key"],
                "manifest_checksum": receipt["manifest_checksum"],
            }
            for receipt in contributing
        ],
        "canonical_versions": canonical_versions,
    }
    return validate_goal22_trusted_input_lineage(
        lineage,
        expected_trade_dates=selected_dates,
    )


def validate_goal22_trusted_input_lineage(
    lineage: dict[str, Any],
    *,
    expected_trade_dates: Iterable[str],
) -> dict[str, Any]:
    if not isinstance(lineage, dict):
        raise ValueError("trusted_input_lineage must be an object")
    if lineage.get("schema_version") != "goal22.trusted_input_lineage.v1":
        raise ValueError("trusted input lineage schema is invalid")

    normalized_dates = _normalize_trade_dates(expected_trade_dates)
    if lineage.get("trade_dates") != normalized_dates:
        raise ValueError(
            "trusted input lineage trade_dates do not match the requested plan"
        )

    raw_codes = lineage.get("codes")
    if not isinstance(raw_codes, list) or not raw_codes:
        raise ValueError("trusted input lineage codes must be a non-empty list")
    codes = sorted({validate_stock_code(str(code)) for code in raw_codes})
    if raw_codes != codes:
        raise ValueError("trusted input lineage codes must be sorted and unique")

    receipts = lineage.get("readiness_receipts")
    if not isinstance(receipts, list) or not receipts:
        raise ValueError(
            "trusted input lineage must contain Goal 20 readiness receipts"
        )
    normalized_receipts = []
    seen_report_keys: set[str] = set()
    for receipt in receipts:
        if not isinstance(receipt, dict):
            raise ValueError("readiness receipt must be an object")
        report_key, batch_id = _validate_readiness_report_key(
            receipt.get("readiness_report_key")
        )
        if report_key in seen_report_keys:
            raise ValueError("readiness receipt keys must be unique")
        manifest_key = _manifest_key(batch_id)
        if receipt.get("manifest_key") != manifest_key:
            raise ValueError("readiness receipt manifest key is invalid")
        for field in ("readiness_report_checksum", "manifest_checksum"):
            if _SHA256_PATTERN.fullmatch(str(receipt.get(field, ""))) is None:
                raise ValueError(f"readiness receipt {field} must be sha256 hex")
        if receipt.get("batch_id") != batch_id:
            raise ValueError("readiness receipt batch_id does not match its key")
        normalized_receipts.append(deepcopy(receipt))
        seen_report_keys.add(report_key)

    versions = lineage.get("canonical_versions")
    if not isinstance(versions, dict) or set(versions) != set(normalized_dates):
        raise ValueError("trusted canonical versions must cover every requested date")
    normalized_versions: dict[str, dict[str, dict[str, Any]]] = {}
    for trade_date in normalized_dates:
        date_versions = versions.get(trade_date)
        if not isinstance(date_versions, dict) or set(date_versions) != set(
            REQUIRED_INPUTS
        ):
            raise ValueError(
                f"trusted canonical versions are incomplete for {trade_date}"
            )
        normalized_versions[trade_date] = {}
        for dataset in REQUIRED_INPUTS:
            normalized_versions[trade_date][dataset] = _validate_version_record(
                dataset,
                trade_date,
                date_versions[dataset],
            )

    return {
        "schema_version": "goal22.trusted_input_lineage.v1",
        "trade_dates": normalized_dates,
        "codes": codes,
        "readiness_receipts": normalized_receipts,
        "canonical_versions": normalized_versions,
    }


def _load_and_validate_receipt(
    readiness_report_key: str,
    read_json_fn: ReadJsonFn,
) -> dict[str, Any]:
    report_key, batch_id = _validate_readiness_report_key(readiness_report_key)
    manifest_key = _manifest_key(batch_id)
    report = read_json_fn(report_key)
    manifest = read_json_fn(manifest_key)
    if not isinstance(report, dict) or not isinstance(manifest, dict):
        raise ValueError("Goal 20 readiness receipt artifacts must be JSON objects")

    report_checksum = readiness_payload_checksum(report)
    if manifest.get("readiness_report_checksum") != report_checksum:
        raise ValueError(
            "Goal 20 readiness report checksum does not match its manifest"
        )
    if report.get("schema_version") != "goal20.real_clean_input_readiness.v1":
        raise ValueError("Goal 20 readiness report schema is invalid")
    if manifest.get("schema_version") != "goal20.real_clean_input_manifest.v1":
        raise ValueError("Goal 20 readiness manifest schema is invalid")
    for payload, label in ((report, "report"), (manifest, "manifest")):
        if payload.get("goal") != "20" or payload.get("batch_id") != batch_id:
            raise ValueError(f"Goal 20 readiness {label} identity is invalid")
    if manifest.get("readiness_report_key") != report_key:
        raise ValueError("Goal 20 manifest points to a different readiness report")
    if (
        report.get("status") != "READY"
        or report.get("ready_for_clean") is not True
        or report.get("ready_for_apply") is not True
    ):
        raise ValueError("Goal 20 readiness report must have ready_for_clean=true")
    if (
        manifest.get("status") != "COMPLETED"
        or manifest.get("readiness_status") != "READY"
        or manifest.get("ready_for_apply") is not True
        or manifest.get("ready_for_clean") is not True
    ):
        raise ValueError("Goal 20 readiness manifest is not completed and ready")
    if report.get("blocked_reasons") or manifest.get("blocked_reasons"):
        raise ValueError("Goal 20 readiness receipt contains blocking reasons")
    if report.get("read_back_verification", {}).get("passed") is not True:
        raise ValueError("Goal 20 canonical read-back verification did not pass")
    expected_firewalls = {
        "adjusted_price_entered": False,
        "clean_daily_snapshot_entered": False,
        "universe_entered": False,
        "factor_entered": False,
        "selection_entered": False,
        "backtest_entered": False,
    }
    if (
        report.get("downstream_firewalls") != expected_firewalls
        or manifest.get("downstream_firewalls") != expected_firewalls
    ):
        raise ValueError("Goal 20 downstream firewalls are not closed")

    scope = report.get("requested_scope")
    if not isinstance(scope, dict) or manifest.get("requested_scope") != scope:
        raise ValueError("Goal 20 readiness scope is missing or inconsistent")
    codes = _normalize_codes(scope.get("codes"))
    trade_dates = _normalize_trade_dates(scope.get("trade_dates", []))
    if not trade_dates:
        raise ValueError("Goal 20 readiness scope contains no trade dates")
    scope_start, scope_end = validate_date_range(
        str(scope.get("start_date")),
        str(scope.get("end_date")),
    )
    if trade_dates[0] < scope_start or trade_dates[-1] > scope_end:
        raise ValueError("Goal 20 readiness dates fall outside its audited range")

    inputs = report.get("inputs")
    manifest_sources = manifest.get("source_keys")
    if not isinstance(inputs, dict) or set(inputs) != set(REQUIRED_INPUTS):
        raise ValueError("Goal 20 readiness report does not contain all seven inputs")
    if not isinstance(manifest_sources, dict) or set(manifest_sources) != set(
        REQUIRED_INPUTS
    ):
        raise ValueError("Goal 20 readiness manifest lineage is incomplete")
    top_readbacks = report.get("read_back_verification", {}).get("details")
    if not isinstance(top_readbacks, list):
        raise ValueError("Goal 20 aggregate read-back details are missing")
    top_readback_by_dataset = {
        item.get("dataset"): item
        for item in top_readbacks
        if isinstance(item, dict)
    }
    if (
        len(top_readbacks) != len(REQUIRED_INPUTS)
        or set(top_readback_by_dataset) != set(REQUIRED_INPUTS)
    ):
        raise ValueError("Goal 20 aggregate read-back details are incomplete")

    canonical_versions: dict[str, dict[str, dict[str, Any]]] = {
        trade_date: {} for trade_date in trade_dates
    }
    for dataset in REQUIRED_INPUTS:
        status = inputs[dataset]
        if not isinstance(status, dict):
            raise ValueError(f"Goal 20 input status is invalid for {dataset}")
        if (
            status.get("ready_for_clean") is not True
            or status.get("ready_for_apply") is not True
            or status.get("validation", {}).get("passed") is not True
            or status.get("coverage", {}).get("passed") is not True
            or status.get("read_back", {}).get("passed") is not True
            or status.get("blocked_reasons")
        ):
            raise ValueError(f"Goal 20 input {dataset} is not fully ready")
        coverage = status["coverage"]
        if _normalize_trade_dates(
            coverage.get("requested_trade_dates", [])
        ) != trade_dates:
            raise ValueError(
                f"Goal 20 input {dataset} audit date scope is inconsistent"
            )
        if dataset == "benchmark_price":
            if coverage.get("required_indexes") != [
                "000300.SH",
                "000905.SH",
                "000906.SH",
            ]:
                raise ValueError("Goal 20 benchmark audit scope is inconsistent")
        elif _normalize_codes(coverage.get("requested_codes")) != codes:
            raise ValueError(
                f"Goal 20 input {dataset} audit code scope is inconsistent"
            )
        source_keys = status.get("source_keys")
        if source_keys != manifest_sources.get(dataset):
            raise ValueError(f"Goal 20 source lineage differs for {dataset}")
        _validate_lineage_keys(source_keys, dataset)

        details = status.get("read_back", {}).get("details")
        if not isinstance(details, list):
            raise ValueError(f"Goal 20 read-back details are missing for {dataset}")
        aggregate = top_readback_by_dataset[dataset]
        if aggregate.get("passed") is not True or aggregate.get(
            "details"
        ) != details:
            raise ValueError(
                f"Goal 20 aggregate read-back differs for {dataset}"
            )
        by_date: dict[str, dict[str, Any]] = {}
        for detail in details:
            if not isinstance(detail, dict):
                raise ValueError(f"Goal 20 read-back detail is invalid for {dataset}")
            trade_date = validate_trade_date(str(detail.get("trade_date")))
            if trade_date in by_date:
                raise ValueError(
                    f"duplicate Goal 20 read-back detail for {dataset} {trade_date}"
                )
            if detail.get("passed") is not True:
                raise ValueError(
                    f"Goal 20 read-back did not pass for {dataset} {trade_date}"
                )
            by_date[trade_date] = _validate_version_record(
                dataset,
                trade_date,
                detail,
            )
        if set(by_date) != set(trade_dates):
            raise ValueError(
                "Goal 20 read-back details do not cover the audited dates for "
                f"{dataset}"
            )
        for trade_date in trade_dates:
            canonical_versions[trade_date][dataset] = by_date[trade_date]

    return {
        "batch_id": batch_id,
        "readiness_report_key": report_key,
        "readiness_report_checksum": report_checksum,
        "manifest_key": manifest_key,
        "manifest_checksum": readiness_payload_checksum(manifest),
        "codes": codes,
        "trade_dates": trade_dates,
        "canonical_versions": canonical_versions,
    }


def _validate_version_record(
    dataset: str,
    trade_date: str,
    raw_record: Any,
) -> dict[str, Any]:
    if not isinstance(raw_record, dict):
        raise ValueError(f"canonical version record is invalid for {dataset}")
    expected_key = f"raw/{dataset}/trade_date={trade_date}/part.parquet"
    if raw_record.get("object_key") != expected_key:
        raise ValueError(
            f"canonical version object key is invalid for {dataset} {trade_date}"
        )
    object_rows = _non_negative_int(
        raw_record.get("object_row_count"),
        "object_row_count",
    )
    scope_rows = _non_negative_int(
        raw_record.get("scope_row_count"),
        "scope_row_count",
    )
    if scope_rows > object_rows:
        raise ValueError("canonical scoped row count exceeds object row count")
    object_checksum = str(raw_record.get("object_checksum", ""))
    scope_checksum = str(raw_record.get("scope_checksum", ""))
    if _SHA256_PATTERN.fullmatch(object_checksum) is None:
        raise ValueError("canonical object checksum must be sha256 hex")
    if _SHA256_PATTERN.fullmatch(scope_checksum) is None:
        raise ValueError("canonical scope checksum must be sha256 hex")
    return {
        "object_key": expected_key,
        "object_row_count": object_rows,
        "object_checksum": object_checksum,
        "scope_row_count": scope_rows,
        "scope_checksum": scope_checksum,
    }


def _validate_lineage_keys(values: Any, dataset: str) -> None:
    if not isinstance(values, list) or not values:
        raise ValueError(f"Goal 20 source lineage is missing for {dataset}")
    for value in values:
        key = safe_object_key(str(value))
        if key.lower().startswith("smoke/"):
            raise ValueError(
                f"Goal 20 source lineage cannot use smoke data for {dataset}"
            )


def _normalize_report_keys(values: Iterable[str]) -> list[str]:
    result = []
    for value in values:
        key, _batch_id = _validate_readiness_report_key(value)
        if key not in result:
            result.append(key)
    if not result:
        raise ValueError("at least one Goal 20 readiness report key is required")
    return sorted(result)


def _validate_readiness_report_key(value: Any) -> tuple[str, str]:
    key = safe_object_key(str(value))
    match = _READINESS_KEY_PATTERN.fullmatch(key)
    if match is None:
        raise ValueError(
            "readiness report key must use the Goal 20 readiness_report prefix"
        )
    return key, match.group(1)


def _manifest_key(batch_id: str) -> str:
    return (
        "candidate/real_clean_inputs/manifest/"
        f"batch_id={batch_id}/manifest.json"
    )


def _normalize_trade_dates(values: Iterable[str]) -> list[str]:
    if isinstance(values, (str, bytes)):
        raise ValueError("trade_dates must be an iterable of date values")
    return sorted({validate_trade_date(str(value)) for value in values})


def _normalize_codes(values: Any) -> list[str]:
    if not isinstance(values, list) or not values:
        raise ValueError("Goal 20 readiness scope codes must be a non-empty list")
    codes = sorted({validate_stock_code(str(value)) for value in values})
    if len(codes) != len(values):
        raise ValueError("Goal 20 readiness scope codes must be unique")
    return codes


def _non_negative_int(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return value

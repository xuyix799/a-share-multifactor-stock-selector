from dataclasses import dataclass
from enum import Enum
from typing import Iterable


class DataQualityLevel(str, Enum):
    DQ0 = "DQ0"
    DQ1 = "DQ1"
    DQ2 = "DQ2"
    DQ3 = "DQ3"
    DQ4 = "DQ4"


class BacktestMode(str, Enum):
    STRICT_TRADABLE_REQUIRED = "strict_tradable_required"
    PRICE_ONLY_DIAGNOSTIC = "price_only_diagnostic"


class SuspensionCoverageStatus(str, Enum):
    FULL_EVENT_COVERAGE = "FULL_EVENT_COVERAGE"
    SAMPLE_TRUNCATED = "SAMPLE_TRUNCATED"
    COVERAGE_UNKNOWN = "COVERAGE_UNKNOWN"
    MISSING_INPUT = "MISSING_INPUT"
    SCHEMA_MISMATCH = "SCHEMA_MISMATCH"


class DQ3ReadinessStatus(str, Enum):
    READY_FOR_PROMOTION_VALIDATOR = "READY_FOR_PROMOTION_VALIDATOR"
    BLOCKED_BY_UNRESOLVED_IS_PAUSED = "BLOCKED_BY_UNRESOLVED_IS_PAUSED"
    BLOCKED_BY_INCOMPLETE_PROVIDER_COVERAGE = "BLOCKED_BY_INCOMPLETE_PROVIDER_COVERAGE"
    BLOCKED_BY_FIELD_COMPLETENESS = "BLOCKED_BY_FIELD_COMPLETENESS"
    BLOCKED_BY_SCHEMA_OR_DUPLICATE = "BLOCKED_BY_SCHEMA_OR_DUPLICATE"
    BLOCKED_BY_VALIDATOR = "BLOCKED_BY_VALIDATOR"


class CoverageGapReason(str, Enum):
    MISSING_PROVIDER_ROW = "MISSING_PROVIDER_ROW"
    FETCH_STRATEGY_GAP = "FETCH_STRATEGY_GAP"
    DATE_ALIGNMENT_GAP = "DATE_ALIGNMENT_GAP"
    CODE_ALIGNMENT_GAP = "CODE_ALIGNMENT_GAP"
    SAMPLE_TRUNCATED = "SAMPLE_TRUNCATED"
    SCHEMA_MISMATCH = "SCHEMA_MISMATCH"
    PROVIDER_BLOCKED = "PROVIDER_BLOCKED"
    RATE_LIMITED = "RATE_LIMITED"
    UNKNOWN = "UNKNOWN"


class PauseStatus(str, Enum):
    TRUE_CANDIDATE = "true_candidate"
    FALSE_CANDIDATE = "false_candidate"
    UNKNOWN = "unknown"


class PauseEvidence(str, Enum):
    SUSPEND_D_MATCH = "suspend_d_match"
    FULL_EVENT_COVERAGE_NO_MATCH = "full_event_coverage_no_match"
    UNRESOLVED_NO_EVENT_MATCH = "unresolved_no_event_match"
    BLOCKED_BY_SAMPLE_TRUNCATED_SUSPEND_D = "blocked_by_sample_truncated_suspend_d"
    BLOCKED_BY_MISSING_COVERAGE_METADATA = "blocked_by_missing_coverage_metadata"
    BLOCKED_BY_MISSING_INPUT = "blocked_by_missing_input"


@dataclass(frozen=True)
class ProviderDatasetContract:
    provider_name: str
    dataset: str
    dq_level: DataQualityLevel
    allowed_datasets: tuple[str, ...]
    allowed_backtest_modes: tuple[BacktestMode, ...]
    non_tradable: bool
    reason: str
    strict_tradable_ready: bool = False


@dataclass(frozen=True)
class BacktestModeContract:
    mode: BacktestMode
    allowed_dq_levels: tuple[DataQualityLevel, ...]
    allowed_datasets: tuple[str, ...]
    tradable: bool
    result_label: str


@dataclass(frozen=True)
class TradingCalendarCandidateContract:
    provider_name: str
    source_interface: str
    candidate_dataset: str
    dq_level: DataQualityLevel
    event_source_candidate_available: bool
    standard_trading_calendar_ready: bool
    standard_daily_price_ready: bool
    standard_daily_price_written: bool
    real_backtest_allowed: bool
    required_future_gates: tuple[str, ...]
    reason: str


@dataclass(frozen=True)
class SuspensionStatusCandidateContract:
    provider_name: str
    source_interface: str
    candidate_dataset: str
    dq_level: DataQualityLevel
    event_source_candidate_available: bool
    hit_means_is_paused_true_candidate: bool
    miss_means_is_paused_false_candidate: bool
    standard_suspension_status_ready: bool
    standard_daily_price_ready: bool
    standard_daily_price_written: bool
    real_backtest_allowed: bool
    required_future_gates: tuple[str, ...]
    reason: str


@dataclass(frozen=True)
class DailyPriceCandidateContract:
    provider_name: str
    candidate_dataset: str
    source_layer: str
    dq_level: DataQualityLevel
    required_inputs: tuple[str, ...]
    standard_daily_price_ready: bool
    standard_daily_price_written: bool
    real_backtest_allowed: bool
    required_future_gates: tuple[str, ...]
    reason: str


@dataclass(frozen=True)
class TushareCandidateBatchContract:
    provider_name: str
    candidate_dataset: str
    source_layer: str
    dq_level: DataQualityLevel
    required_interfaces: tuple[str, ...]
    standard_daily_price_written: bool
    standard_suspension_status_written: bool
    real_backtest_allowed: bool
    required_future_gates: tuple[str, ...]
    reason: str


@dataclass(frozen=True)
class DailyPriceCandidateReadiness:
    ready_for_dq3_promotion: bool
    status: str
    reasons: tuple[str, ...]
    required_future_gates: tuple[str, ...]


@dataclass(frozen=True)
class SuspensionStatusCandidateReadiness:
    ready_for_dq3_promotion: bool
    status: str
    reasons: tuple[str, ...]
    required_future_gates: tuple[str, ...]


@dataclass(frozen=True)
class TushareCandidateBatchReadiness:
    ready_for_promotion_validator: bool
    ready_for_dq3_promotion: bool
    status: DQ3ReadinessStatus
    reasons: tuple[str, ...]
    required_future_gates: tuple[str, ...]


@dataclass(frozen=True)
class FetchSemanticsContract:
    interface: str
    fetch_strategy: str
    expected_granularity: str
    loops_per_code: bool
    loops_per_trade_day: bool


@dataclass(frozen=True)
class ProviderCoverageContract:
    interface: str
    numerator: int
    denominator: int
    ratio: float
    blocked_reason: CoverageGapReason | None


@dataclass(frozen=True)
class CriticalCoverageStatus:
    complete: bool
    blocked_reasons: tuple[CoverageGapReason, ...]


@dataclass(frozen=True)
class CandidateBatchCoverageReadiness:
    critical_price_coverage_complete: bool
    ready_for_promotion_validator: bool
    ready_for_dq3_promotion: bool
    blocked_reasons: tuple[str, ...]


REQUIRED_DAILY_PRICE_FIELDS = frozenset(
    {
        "stock_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "volume",
        "amount",
        "is_paused",
        "limit_up",
        "limit_down",
    }
)

REQUIRED_TUSHARE_TRADE_CAL_FIELDS = frozenset({"exchange", "cal_date", "is_open", "pretrade_date"})
REQUIRED_TUSHARE_SUSPEND_D_FIELDS = frozenset({"ts_code", "trade_date", "suspend_timing", "suspend_type"})
PASS_SMOKE_STATUSES = frozenset({"PASS_WITH_ROWS", "PASS_EMPTY"})
GOAL12B_FUTURE_GATES = ("staging", "join_dry_run", "coverage_audit", "validator_verification")
DAILY_PRICE_CANDIDATE_REQUIRED_INPUTS = ("daily", "stk_limit", "adj_factor", "trade_cal", "suspend_d")
TUSHARE_CANDIDATE_BATCH_REQUIRED_INTERFACES = ("daily", "stk_limit", "adj_factor", "daily_basic", "trade_cal", "suspend_d")
SUSPENSION_STATUS_CANDIDATE_REQUIRED_INPUTS = (
    "daily_price_candidate",
    "daily_price_candidate_report",
    "trade_cal",
    "suspend_d",
)
DAILY_PRICE_CANDIDATE_FUTURE_GATES = (
    "staging",
    "coverage_audit",
    "duplicate_check",
    "missing_check",
    "validator",
    "small_range_promotion",
    "mock_mainline_protection",
)
SUSPENSION_STATUS_CANDIDATE_FUTURE_GATES = (
    "full_event_coverage_smoke",
    "staging_audit",
    "promotion_validator",
    "small_range_standard_write",
    "mainline_isolation_tests",
)
TUSHARE_CANDIDATE_BATCH_FUTURE_GATES = (
    "staging",
    "coverage_audit",
    "duplicate_check",
    "missing_check",
    "validator",
    "promotion_validator",
    "small_range_promotion",
    "mock_mainline_protection",
)


def classify_provider_dataset(provider_name: str, dataset: str) -> ProviderDatasetContract:
    provider_name = provider_name.lower()
    dataset = dataset.lower()

    if provider_name == "akshare" and dataset == "benchmark_price":
        return ProviderDatasetContract(
            provider_name=provider_name,
            dataset=dataset,
            dq_level=DataQualityLevel.DQ2,
            allowed_datasets=("benchmark_price",),
            allowed_backtest_modes=(BacktestMode.PRICE_ONLY_DIAGNOSTIC,),
            non_tradable=True,
            reason="AKShare benchmark_price is DQ2 price-only diagnostic data for benchmark_price, not tradable stock data.",
        )

    if provider_name == "akshare" and dataset in {"daily_price", "daily_price_raw", "daily_price_raw_smoke"}:
        return ProviderDatasetContract(
            provider_name=provider_name,
            dataset=dataset,
            dq_level=DataQualityLevel.DQ1,
            allowed_datasets=("daily_price_raw", "daily_price_raw_smoke"),
            allowed_backtest_modes=(),
            non_tradable=True,
            reason="AKShare stock daily lacks limit_up, limit_down, and is_paused; it must remain DQ1 raw/smoke data.",
        )

    if provider_name == "baostock" and dataset in {"daily_price", "daily_price_raw", "daily_price_raw_smoke"}:
        return ProviderDatasetContract(
            provider_name=provider_name,
            dataset=dataset,
            dq_level=DataQualityLevel.DQ1,
            allowed_datasets=("daily_price_raw", "daily_price_raw_smoke"),
            allowed_backtest_modes=(),
            non_tradable=True,
            reason=(
                "Baostock stock daily remains DQ1 even if login succeeds unless it provides limit_up, "
                "limit_down, and is_paused."
            ),
        )

    if provider_name == "tushare":
        return ProviderDatasetContract(
            provider_name=provider_name,
            dataset=dataset,
            dq_level=DataQualityLevel.DQ1,
            allowed_datasets=(),
            allowed_backtest_modes=(),
            non_tradable=True,
            reason=(
                "Tushare status must come from the current Goal 10R capability matrix. Even if daily, "
                "stk_limit, adj_factor, and daily_basic are available, strict tradable closure is not ready "
                "until is_paused has a trusted source."
            ),
        )

    return ProviderDatasetContract(
        provider_name=provider_name,
        dataset=dataset,
        dq_level=DataQualityLevel.DQ1,
        allowed_datasets=(),
        allowed_backtest_modes=(),
        non_tradable=True,
        reason="Provider dataset is not approved for standard daily_price or tradable backtest use.",
    )


def get_backtest_mode_contract(mode: BacktestMode | str) -> BacktestModeContract:
    mode = _normalize_backtest_mode(mode)
    if mode == BacktestMode.STRICT_TRADABLE_REQUIRED:
        return BacktestModeContract(
            mode=mode,
            allowed_dq_levels=(DataQualityLevel.DQ3, DataQualityLevel.DQ4),
            allowed_datasets=("daily_price",),
            tradable=True,
            result_label="strict_tradable",
        )
    return BacktestModeContract(
        mode=mode,
        allowed_dq_levels=(DataQualityLevel.DQ2,),
        allowed_datasets=("benchmark_price",),
        tradable=False,
        result_label="diagnostic_non_tradable",
    )


def classify_tushare_trading_calendar_candidate(
    smoke_status: str,
    fields: Iterable[str],
) -> TradingCalendarCandidateContract:
    field_set = set(fields)
    available = smoke_status in PASS_SMOKE_STATUSES and REQUIRED_TUSHARE_TRADE_CAL_FIELDS.issubset(field_set)
    return TradingCalendarCandidateContract(
        provider_name="tushare",
        source_interface="trade_cal",
        candidate_dataset="trading_calendar_candidate",
        dq_level=DataQualityLevel.DQ1,
        event_source_candidate_available=available,
        standard_trading_calendar_ready=False,
        standard_daily_price_ready=False,
        standard_daily_price_written=False,
        real_backtest_allowed=False,
        required_future_gates=GOAL12B_FUTURE_GATES,
        reason=(
            "Tushare trade_cal is only a trading_calendar candidate in Goal 12B; standard staging, "
            "calendar consistency checks, audit fields, and validator verification are still required."
        ),
    )


def classify_tushare_suspension_status_candidate(
    smoke_status: str,
    fields: Iterable[str],
) -> SuspensionStatusCandidateContract:
    field_set = set(fields)
    available = smoke_status in PASS_SMOKE_STATUSES and REQUIRED_TUSHARE_SUSPEND_D_FIELDS.issubset(field_set)
    return SuspensionStatusCandidateContract(
        provider_name="tushare",
        source_interface="suspend_d",
        candidate_dataset="suspension_status_candidate",
        dq_level=DataQualityLevel.DQ1,
        event_source_candidate_available=available,
        hit_means_is_paused_true_candidate=smoke_status == "PASS_WITH_ROWS" and available,
        miss_means_is_paused_false_candidate=False,
        standard_suspension_status_ready=False,
        standard_daily_price_ready=False,
        standard_daily_price_written=False,
        real_backtest_allowed=False,
        required_future_gates=GOAL12B_FUTURE_GATES,
        reason=(
            "Tushare suspend_d can only be used as a suspension_status_candidate event source in Goal 12B. "
            "A hit can mean is_paused=true candidate; a miss cannot mean is_paused=false until coverage "
            "and source completeness are audited."
        ),
    )


def classify_tushare_daily_price_candidate(source_layer: str) -> DailyPriceCandidateContract:
    return DailyPriceCandidateContract(
        provider_name="tushare",
        candidate_dataset="daily_price_candidate",
        source_layer=source_layer,
        dq_level=DataQualityLevel.DQ1,
        required_inputs=DAILY_PRICE_CANDIDATE_REQUIRED_INPUTS,
        standard_daily_price_ready=False,
        standard_daily_price_written=False,
        real_backtest_allowed=False,
        required_future_gates=DAILY_PRICE_CANDIDATE_FUTURE_GATES,
        reason=(
            "Tushare daily_price_candidate is a smoke/candidate dry-run composition only. It must not become "
            "standard daily_price until staging, coverage audit, duplicate and missing checks, validator "
            "verification, and an explicit boolean suspension_status source are complete."
        ),
    )


def classify_tushare_candidate_batch(source_layer: str) -> TushareCandidateBatchContract:
    return TushareCandidateBatchContract(
        provider_name="tushare",
        candidate_dataset="tushare_candidate_staging_batch",
        source_layer=source_layer,
        dq_level=DataQualityLevel.DQ1,
        required_interfaces=TUSHARE_CANDIDATE_BATCH_REQUIRED_INTERFACES,
        standard_daily_price_written=False,
        standard_suspension_status_written=False,
        real_backtest_allowed=False,
        required_future_gates=TUSHARE_CANDIDATE_BATCH_FUTURE_GATES,
        reason=(
            "Tushare candidate staging batch is a real-provider candidate-only audit artifact. It may "
            "prepare coverage and readiness reports, but it must not write standard daily_price or "
            "standard suspension_status in Goal 13."
        ),
    )


def can_build_daily_price_candidate_dry_run(available_inputs: Iterable[str]) -> bool:
    return set(DAILY_PRICE_CANDIDATE_REQUIRED_INPUTS).issubset(set(available_inputs))


def can_build_tushare_candidate_staging_batch(
    *,
    available_interfaces: Iterable[str],
    provider_enabled: bool,
    token_available: bool,
) -> bool:
    return (
        provider_enabled
        and token_available
        and set(TUSHARE_CANDIDATE_BATCH_REQUIRED_INTERFACES).issubset(set(available_interfaces))
    )


def can_build_suspension_status_candidate(available_inputs: Iterable[str]) -> bool:
    return set(SUSPENSION_STATUS_CANDIDATE_REQUIRED_INPUTS).issubset(set(available_inputs))


def can_use_suspend_miss_as_false_candidate(
    *,
    coverage_status: SuspensionCoverageStatus | str,
    trade_cal_valid: bool,
    schema_valid: bool,
    volume_used_as_pause: bool,
    amount_used_as_pause: bool,
    missing_daily_used_as_pause: bool,
    unchanged_price_used_as_pause: bool,
) -> bool:
    status = _normalize_suspension_coverage_status(coverage_status)
    return (
        status == SuspensionCoverageStatus.FULL_EVENT_COVERAGE
        and trade_cal_valid
        and schema_valid
        and not volume_used_as_pause
        and not amount_used_as_pause
        and not missing_daily_used_as_pause
        and not unchanged_price_used_as_pause
    )


def can_promote_suspension_status_candidate(
    *,
    coverage_status: SuspensionCoverageStatus | str,
    pause_statuses: Iterable[PauseStatus | str],
    validator_passed: bool,
    dq_level: DataQualityLevel | str,
) -> SuspensionStatusCandidateReadiness:
    status = _normalize_suspension_coverage_status(coverage_status)
    normalized_pause_statuses = {_normalize_pause_status(item) for item in pause_statuses}
    normalized_level = _normalize_dq_level(dq_level)
    reasons = []

    if status != SuspensionCoverageStatus.FULL_EVENT_COVERAGE:
        reasons.append("suspend_d event source coverage is not complete")
    if PauseStatus.UNKNOWN in normalized_pause_statuses:
        reasons.append("pause_status contains unresolved unknown rows")
    if not validator_passed:
        reasons.append("suspension_status promotion validator must pass")
    if normalized_level not in {DataQualityLevel.DQ3, DataQualityLevel.DQ4}:
        reasons.append("dq_level must be DQ3 or DQ4")

    if not reasons:
        return SuspensionStatusCandidateReadiness(
            ready_for_dq3_promotion=True,
            status="READY_FOR_DQ3_PROMOTION",
            reasons=(),
            required_future_gates=(),
        )

    if status == SuspensionCoverageStatus.SAMPLE_TRUNCATED:
        readiness_status = "BLOCKED_BY_INCOMPLETE_SUSPEND_D_COVERAGE"
    elif status in {SuspensionCoverageStatus.COVERAGE_UNKNOWN, SuspensionCoverageStatus.MISSING_INPUT, SuspensionCoverageStatus.SCHEMA_MISMATCH}:
        readiness_status = "BLOCKED_BY_UNRESOLVED_IS_PAUSED"
    elif PauseStatus.UNKNOWN in normalized_pause_statuses:
        readiness_status = "BLOCKED_BY_UNRESOLVED_IS_PAUSED"
    else:
        readiness_status = "CANDIDATE_AUDIT_COMPLETED_NOT_PROMOTABLE"

    return SuspensionStatusCandidateReadiness(
        ready_for_dq3_promotion=False,
        status=readiness_status,
        reasons=tuple(reasons),
        required_future_gates=SUSPENSION_STATUS_CANDIDATE_FUTURE_GATES,
    )


def can_mark_tushare_candidate_batch_ready_for_promotion_validator(
    *,
    field_completeness_ok: bool,
    coverage_complete: bool,
    pause_statuses: Iterable[PauseStatus | str],
    duplicate_check_ok: bool,
    schema_check_ok: bool,
    validator_precheck_passed: bool,
    dq_level: DataQualityLevel | str,
) -> TushareCandidateBatchReadiness:
    normalized_pause_statuses = {_normalize_pause_status(item) for item in pause_statuses}
    normalized_level = _normalize_dq_level(dq_level)
    reasons = []

    if not field_completeness_ok:
        reasons.append("candidate batch field completeness is incomplete")
    if not coverage_complete:
        reasons.append("provider coverage is incomplete or unresolved")
    if PauseStatus.UNKNOWN in normalized_pause_statuses:
        reasons.append("pause_status contains unresolved unknown rows")
    if not duplicate_check_ok:
        reasons.append("duplicate key check must pass")
    if not schema_check_ok:
        reasons.append("schema check must pass")
    if not validator_precheck_passed:
        reasons.append("promotion validator precheck must pass")
    if normalized_level not in {DataQualityLevel.DQ3, DataQualityLevel.DQ4}:
        reasons.append("dq_level must be DQ3 or DQ4")

    if not reasons:
        return TushareCandidateBatchReadiness(
            ready_for_promotion_validator=True,
            ready_for_dq3_promotion=False,
            status=DQ3ReadinessStatus.READY_FOR_PROMOTION_VALIDATOR,
            reasons=(),
            required_future_gates=("explicit_standard_promotion_goal", "standard_write_approval"),
        )

    if PauseStatus.UNKNOWN in normalized_pause_statuses:
        status = DQ3ReadinessStatus.BLOCKED_BY_UNRESOLVED_IS_PAUSED
    elif not coverage_complete:
        status = DQ3ReadinessStatus.BLOCKED_BY_INCOMPLETE_PROVIDER_COVERAGE
    elif not field_completeness_ok:
        status = DQ3ReadinessStatus.BLOCKED_BY_FIELD_COMPLETENESS
    elif not duplicate_check_ok or not schema_check_ok:
        status = DQ3ReadinessStatus.BLOCKED_BY_SCHEMA_OR_DUPLICATE
    else:
        status = DQ3ReadinessStatus.BLOCKED_BY_VALIDATOR

    return TushareCandidateBatchReadiness(
        ready_for_promotion_validator=False,
        ready_for_dq3_promotion=False,
        status=status,
        reasons=tuple(reasons),
        required_future_gates=TUSHARE_CANDIDATE_BATCH_FUTURE_GATES,
    )


def can_mark_critical_price_coverage_complete(coverages: Iterable[ProviderCoverageContract]) -> CriticalCoverageStatus:
    blocked = []
    for coverage in coverages:
        if coverage.denominator == 0 or coverage.numerator != coverage.denominator or coverage.ratio < 1.0:
            blocked.append(coverage.blocked_reason or CoverageGapReason.MISSING_PROVIDER_ROW)
    return CriticalCoverageStatus(complete=not blocked, blocked_reasons=tuple(blocked))


def can_mark_ready_for_promotion_validator(
    *,
    critical_price_coverage_complete: bool,
    pause_statuses: Iterable[PauseStatus | str],
    suspend_d_full_event_coverage: bool,
    duplicate_check_ok: bool,
    schema_check_ok: bool,
    validator_precheck_passed: bool,
    dq_level: DataQualityLevel | str,
) -> CandidateBatchCoverageReadiness:
    readiness = can_mark_tushare_candidate_batch_ready_for_promotion_validator(
        field_completeness_ok=critical_price_coverage_complete,
        coverage_complete=suspend_d_full_event_coverage,
        pause_statuses=pause_statuses,
        duplicate_check_ok=duplicate_check_ok,
        schema_check_ok=schema_check_ok,
        validator_precheck_passed=validator_precheck_passed,
        dq_level=dq_level,
    )
    return CandidateBatchCoverageReadiness(
        critical_price_coverage_complete=critical_price_coverage_complete,
        ready_for_promotion_validator=readiness.ready_for_promotion_validator,
        ready_for_dq3_promotion=readiness.ready_for_dq3_promotion,
        blocked_reasons=readiness.reasons,
    )


def detect_sample_truncation(sample_truncated: bool, *, interface: str) -> CoverageGapReason | None:
    _ = interface
    return CoverageGapReason.SAMPLE_TRUNCATED if sample_truncated else None


def detect_fetch_strategy_gap(reason_codes: Iterable[CoverageGapReason | str]) -> bool:
    normalized = set()
    for reason in reason_codes:
        try:
            normalized.add(CoverageGapReason(str(reason)))
        except ValueError:
            continue
    return bool(
        normalized
        & {
            CoverageGapReason.FETCH_STRATEGY_GAP,
            CoverageGapReason.DATE_ALIGNMENT_GAP,
            CoverageGapReason.CODE_ALIGNMENT_GAP,
        }
    )


def cannot_write_standard_daily_price(*, source_layer: str, explicit_standard_write_enabled: bool = False) -> bool:
    _ = source_layer, explicit_standard_write_enabled
    return True


def can_write_standard_daily_price_from_tushare_candidate_batch(
    readiness: TushareCandidateBatchReadiness,
    *,
    explicit_standard_write_enabled: bool,
) -> bool:
    _ = explicit_standard_write_enabled
    return False


def can_promote_daily_price_candidate_to_standard(
    *,
    source_layer: str,
    fields: Iterable[str],
    stk_limit_fields_complete: bool,
    trade_cal_valid: bool,
    suspension_status_coverage_audited: bool,
    is_paused_boolean: bool,
    validator_passed: bool,
    dq_level: DataQualityLevel | str,
) -> DailyPriceCandidateReadiness:
    normalized_level = _normalize_dq_level(dq_level)
    field_set = set(fields)
    reasons = []

    if source_layer != "candidate":
        reasons.append(f"source layer must be candidate, not {source_layer}")
    missing_daily_fields = sorted(REQUIRED_DAILY_PRICE_FIELDS - field_set)
    if missing_daily_fields:
        reasons.append("daily_price fields are incomplete: " + ", ".join(missing_daily_fields))
    if not stk_limit_fields_complete:
        reasons.append("stk_limit fields are incomplete")
    if not trade_cal_valid:
        reasons.append("trade_cal must confirm an open trading day")
    if not suspension_status_coverage_audited:
        reasons.append("suspension_status coverage audit is required")
    if not is_paused_boolean:
        reasons.append("is_paused must be an explicit boolean field")
    if not validator_passed:
        reasons.append("daily_price validator must pass")
    if normalized_level not in {DataQualityLevel.DQ3, DataQualityLevel.DQ4}:
        reasons.append("dq_level must be DQ3 or DQ4")

    if not reasons:
        return DailyPriceCandidateReadiness(
            ready_for_dq3_promotion=True,
            status="READY_FOR_DQ3_PROMOTION",
            reasons=(),
            required_future_gates=(),
        )

    status = "BLOCKED_BY_UNRESOLVED_IS_PAUSED"
    if reasons and reasons[0].startswith("source layer must be candidate"):
        status = "BLOCKED_BY_SMOKE_SOURCE"
    elif any("suspension_status" in reason or "is_paused" in reason for reason in reasons):
        status = "BLOCKED_BY_UNRESOLVED_IS_PAUSED"
    elif any("stk_limit" in reason for reason in reasons):
        status = "BLOCKED_BY_INCOMPLETE_LIMIT_FIELDS"
    elif any("trade_cal" in reason for reason in reasons):
        status = "BLOCKED_BY_UNCONFIRMED_TRADING_DAY"
    elif any("validator" in reason for reason in reasons):
        status = "BLOCKED_BY_VALIDATOR"

    return DailyPriceCandidateReadiness(
        ready_for_dq3_promotion=False,
        status=status,
        reasons=tuple(reasons),
        required_future_gates=DAILY_PRICE_CANDIDATE_FUTURE_GATES,
    )


def can_promote_to_daily_price(dq_level: DataQualityLevel | str, fields: Iterable[str]) -> bool:
    dq_level = _normalize_dq_level(dq_level)
    return dq_level in {DataQualityLevel.DQ3, DataQualityLevel.DQ4} and REQUIRED_DAILY_PRICE_FIELDS.issubset(set(fields))


def can_use_in_strict_tradable_required(dq_level: DataQualityLevel | str, dataset: str, fields: Iterable[str]) -> bool:
    contract = get_backtest_mode_contract(BacktestMode.STRICT_TRADABLE_REQUIRED)
    normalized_level = _normalize_dq_level(dq_level)
    return dataset in contract.allowed_datasets and normalized_level in contract.allowed_dq_levels and can_promote_to_daily_price(normalized_level, fields)


def can_use_in_price_only_diagnostic(dq_level: DataQualityLevel | str, dataset: str) -> bool:
    contract = get_backtest_mode_contract(BacktestMode.PRICE_ONLY_DIAGNOSTIC)
    normalized_level = _normalize_dq_level(dq_level)
    return dataset in contract.allowed_datasets and normalized_level in contract.allowed_dq_levels


def _normalize_dq_level(dq_level: DataQualityLevel | str) -> DataQualityLevel:
    if isinstance(dq_level, DataQualityLevel):
        return dq_level
    return DataQualityLevel(str(dq_level))


def _normalize_suspension_coverage_status(status: SuspensionCoverageStatus | str) -> SuspensionCoverageStatus:
    if isinstance(status, SuspensionCoverageStatus):
        return status
    return SuspensionCoverageStatus(str(status))


def _normalize_pause_status(status: PauseStatus | str) -> PauseStatus:
    if isinstance(status, PauseStatus):
        return status
    return PauseStatus(str(status))


def _normalize_backtest_mode(mode: BacktestMode | str) -> BacktestMode:
    if isinstance(mode, BacktestMode):
        return mode
    return BacktestMode(str(mode))

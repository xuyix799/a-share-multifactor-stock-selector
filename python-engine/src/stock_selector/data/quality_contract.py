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


def _normalize_backtest_mode(mode: BacktestMode | str) -> BacktestMode:
    if isinstance(mode, BacktestMode):
        return mode
    return BacktestMode(str(mode))

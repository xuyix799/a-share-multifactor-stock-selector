import pandas as pd

from stock_selector.data.data_validator import validate_dataset_frame
from stock_selector.factors.factor_validator import validate_factor_daily
from stock_selector.scoring.risk_level import determine_risk_level
from stock_selector.scoring.rule_explainer import build_reason, build_suggestion
from stock_selector.scoring.score_engine import ScoringConfig, compute_total_scores
from stock_selector.scoring.selection_validator import SELECTION_RESULT_COLUMNS, validate_selection_result
from stock_selector.utils.date_validator import validate_trade_date


def build_selection_result(
    *,
    factor_daily: pd.DataFrame,
    risk_filter: pd.DataFrame,
    eligible_universe: pd.DataFrame,
    factor_input_table: pd.DataFrame | None,
    trade_date: str,
    scoring_config: ScoringConfig,
) -> pd.DataFrame:
    trade_date = validate_trade_date(trade_date)
    validate_factor_daily(factor_daily, trade_date)
    validate_dataset_frame("risk_filter", risk_filter, trade_date)
    validate_dataset_frame("eligible_universe", eligible_universe, trade_date)
    if factor_input_table is not None:
        validate_dataset_frame("factor_input_table", factor_input_table, trade_date)

    eligible_keys = eligible_universe[["stock_code", "trade_date"]].drop_duplicates()
    result = factor_daily.merge(eligible_keys, on=["stock_code", "trade_date"], how="inner")
    if factor_input_table is not None:
        factor_input_keys = factor_input_table[["stock_code", "trade_date"]].drop_duplicates()
        result = result.merge(factor_input_keys, on=["stock_code", "trade_date"], how="inner")

    risk_columns = risk_filter[["stock_code", "trade_date", "exclude_reasons", "risk_flags"]].drop_duplicates(["stock_code", "trade_date"])
    result = result.merge(risk_columns, on=["stock_code", "trade_date"], how="left")
    result[["exclude_reasons", "risk_flags"]] = result[["exclude_reasons", "risk_flags"]].fillna("")
    result = compute_total_scores(result, scoring_config)
    result["risk_level"] = result.apply(
        lambda row: determine_risk_level(
            total_score=row["total_score"],
            quality_score=row["quality_score"],
            growth_score=row["growth_score"],
            risk_flags=row["risk_flags"],
        ),
        axis=1,
    )
    result["reason"] = result.apply(lambda row: build_reason(row.to_dict()), axis=1)
    result["suggestion"] = result.apply(lambda row: build_suggestion(row.to_dict()), axis=1)
    result = result.sort_values(["total_score", "stock_code"], ascending=[False, True]).head(scoring_config.top_n).reset_index(drop=True)
    result["rank"] = range(1, len(result) + 1)
    result = result[SELECTION_RESULT_COLUMNS]
    validate_selection_result(result, trade_date)
    return result

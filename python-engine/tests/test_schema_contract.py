from stock_selector.providers.schema_contract import get_schema_contract, inspect_schema


def test_schema_contract_exposes_goal2_daily_price_fields_in_order():
    contract = get_schema_contract("daily_price")

    assert contract.columns == [
        "stock_code",
        "trade_date",
        "open",
        "high",
        "low",
        "close",
        "pre_close",
        "volume",
        "amount",
        "pct_chg",
        "is_paused",
        "limit_up",
        "limit_down",
    ]


def test_schema_contract_keeps_stock_basic_delist_date_and_storage_trade_date():
    contract = get_schema_contract("stock_basic")

    assert "delist_date" in contract.columns
    assert "trade_date" in contract.columns


def test_inspect_schema_returns_cli_safe_metadata():
    info = inspect_schema("financial")

    assert info["dataset"] == "financial"
    assert "announce_date" in info["columns"]
    assert "debt_ratio" in info["numeric_columns"]


def test_schema_contract_includes_goal4_derived_datasets():
    adjusted = get_schema_contract("adjusted_price")
    snapshot = get_schema_contract("clean_daily_snapshot")

    assert adjusted.columns == [
        "stock_code",
        "trade_date",
        "adj_open",
        "adj_high",
        "adj_low",
        "adj_close",
        "volume",
        "amount",
        "pct_chg",
        "is_paused",
        "limit_up",
        "limit_down",
    ]
    assert "announce_date" in snapshot.columns
    assert "is_st_on_date" in snapshot.bool_columns
    assert "listed_days" in snapshot.numeric_columns


def test_schema_contract_includes_goal5_universe_datasets():
    risk_filter = get_schema_contract("risk_filter")
    eligible = get_schema_contract("eligible_universe")
    factor_input = get_schema_contract("factor_input_table")

    assert risk_filter.columns == [
        "stock_code",
        "trade_date",
        "is_eligible",
        "exclude_reasons",
        "risk_flags",
        "is_st_on_date",
        "is_paused",
        "listed_days",
        "amount",
        "roe",
        "debt_ratio",
        "report_period",
        "announce_date",
    ]
    assert "is_eligible" in risk_filter.bool_columns
    assert eligible.columns == [
        "stock_code",
        "trade_date",
        "stock_name",
        "industry",
        "market_type",
        "listed_days",
        "amount",
        "roe",
        "debt_ratio",
    ]
    assert factor_input.columns == [
        "stock_code",
        "trade_date",
        "industry",
        "market_type",
        "adj_close",
        "amount",
        "turnover_rate",
        "pe_ttm",
        "pb",
        "ps_ttm",
        "total_mv",
        "circ_mv",
        "revenue_yoy",
        "net_profit_yoy",
        "roe",
        "gross_margin",
        "debt_ratio",
        "operating_cashflow",
    ]


def test_schema_contract_includes_goal6_factor_daily_dataset():
    factor_daily = get_schema_contract("factor_daily")

    assert factor_daily.columns == [
        "stock_code",
        "trade_date",
        "industry",
        "market_type",
        "quality_roe",
        "quality_gross_margin",
        "quality_debt_ratio",
        "quality_cashflow_profit_ratio",
        "growth_revenue_yoy",
        "growth_net_profit_yoy",
        "valuation_pe_ttm",
        "valuation_pb",
        "valuation_ps_ttm",
        "valuation_pe_percentile_3y",
        "valuation_pb_percentile_3y",
        "trend_ret_20d",
        "trend_ret_60d",
        "trend_ret_120d",
        "trend_ma20",
        "trend_ma60",
        "trend_ma120",
        "trend_price_ma60_ratio",
        "industry_ret_60d",
        "industry_ret_120d",
        "industry_strength_60d",
        "industry_strength_120d",
        "liquidity_amount",
        "liquidity_turnover_rate",
        "quality_score",
        "growth_score",
        "valuation_score",
        "trend_score",
        "industry_score",
    ]
    assert "total_score" not in factor_daily.columns
    assert "quality_score" in factor_daily.numeric_columns


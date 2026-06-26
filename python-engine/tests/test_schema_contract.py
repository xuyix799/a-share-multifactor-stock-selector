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


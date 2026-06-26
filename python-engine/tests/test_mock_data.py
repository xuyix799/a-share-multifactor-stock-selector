from stock_selector.data.mock_data import generate_mock_dataset
from stock_selector.storage.partition import SUPPORTED_DATASETS


def test_generate_mock_dataset_supports_all_goal2_datasets():
    for dataset in SUPPORTED_DATASETS:
        df = generate_mock_dataset(dataset, "2026-06-19")
        assert not df.empty, dataset


def test_daily_price_mock_has_required_trade_fields():
    df = generate_mock_dataset("daily_price", "2026-06-19")

    assert len(df) >= 5
    assert set(
        [
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
    ).issubset(df.columns)
    assert (df["trade_date"] == "2026-06-19").all()
    assert (df["close"] > 0).all()


def test_financial_mock_is_announced_no_later_than_trade_date():
    df = generate_mock_dataset("financial", "2026-06-19")

    assert (df["announce_date"] <= "2026-06-19").all()

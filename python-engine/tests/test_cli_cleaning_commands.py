from stock_selector.cli import build_parser


def test_cli_accepts_goal4_cleaning_commands():
    parser = build_parser()

    adjusted = parser.parse_args(["build-adjusted-price", "--trade-date", "2026-06-19", "--force"])
    snapshot = parser.parse_args(["build-clean-snapshot", "--trade-date", "2026-06-19"])
    validate = parser.parse_args(["validate-clean-snapshot", "--trade-date", "2026-06-19"])

    assert adjusted.command == "build-adjusted-price"
    assert adjusted.trade_date == "2026-06-19"
    assert adjusted.force is True
    assert snapshot.command == "build-clean-snapshot"
    assert snapshot.force is False
    assert validate.command == "validate-clean-snapshot"

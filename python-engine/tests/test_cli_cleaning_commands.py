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


def test_cli_accepts_goal5_universe_command():
    parser = build_parser()

    universe = parser.parse_args(["build-universe-inputs", "--trade-date", "2026-06-19", "--force"])

    assert universe.command == "build-universe-inputs"
    assert universe.trade_date == "2026-06-19"
    assert universe.force is True


def test_cli_accepts_goal6_factor_commands():
    parser = build_parser()

    build = parser.parse_args(["build-factors", "--trade-date", "2026-06-19", "--force"])
    validate = parser.parse_args(["validate-factors", "--trade-date", "2026-06-19"])

    assert build.command == "build-factors"
    assert build.trade_date == "2026-06-19"
    assert build.force is True
    assert validate.command == "validate-factors"
    assert validate.trade_date == "2026-06-19"

from stock_selector.cli import build_parser, main


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


def test_cli_accepts_goal7_selection_commands():
    parser = build_parser()

    build = parser.parse_args(["build-selection", "--trade-date", "2026-06-19", "--force"])
    validate = parser.parse_args(["validate-selection", "--trade-date", "2026-06-19"])

    assert build.command == "build-selection"
    assert build.trade_date == "2026-06-19"
    assert build.force is True
    assert validate.command == "validate-selection"
    assert validate.trade_date == "2026-06-19"


def test_cli_accepts_goal8_backtest_command():
    parser = build_parser()

    backtest = parser.parse_args(
        [
            "run-backtest",
            "--strategy-name",
            "goal8-core",
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-03-31",
            "--rebalance",
            "monthly",
            "--initial-cash",
            "100000",
            "--commission-rate",
            "0.001",
            "--slippage-bps",
            "5",
            "--stamp-tax-rate",
            "0.001",
            "--top-n",
            "30",
            "--execution-rule",
            "next_open",
            "--force",
        ]
    )

    assert backtest.command == "run-backtest"
    assert backtest.strategy_name == "goal8-core"
    assert backtest.start_date == "2026-01-01"
    assert backtest.end_date == "2026-03-31"
    assert backtest.rebalance == "monthly"
    assert backtest.initial_cash == 100000
    assert backtest.commission_rate == 0.001
    assert backtest.slippage_bps == 5
    assert backtest.stamp_tax_rate == 0.001
    assert backtest.top_n == 30
    assert backtest.execution_rule == "next_open"
    assert backtest.force is True


def test_cli_accepts_goal8_backtest_command_with_config_defaults():
    parser = build_parser()

    backtest = parser.parse_args(
        [
            "run-backtest",
            "--start-date",
            "2026-01-01",
            "--end-date",
            "2026-06-19",
            "--rebalance",
            "monthly",
            "--force",
        ]
    )

    assert backtest.command == "run-backtest"
    assert backtest.strategy_name is None
    assert backtest.initial_cash is None
    assert backtest.commission_rate is None
    assert backtest.slippage_bps is None
    assert backtest.stamp_tax_rate is None
    assert backtest.top_n is None
    assert backtest.execution_rule is None
    assert backtest.force is True


def test_cli_accepts_goal10_provider_dataset_selection():
    parser = build_parser()

    update = parser.parse_args(
        [
            "update-provider-data",
            "--trade-date",
            "2026-06-19",
            "--provider",
            "tushare",
            "--dataset",
            "stock_basic",
            "--dataset",
            "daily_price",
            "--force",
        ]
    )

    assert update.command == "update-provider-data"
    assert update.provider == "tushare"
    assert update.dataset == ["stock_basic", "daily_price"]
    assert update.force is True


def test_cli_accepts_goal10_tushare_smoke_mode():
    parser = build_parser()

    update = parser.parse_args(
        [
            "update-provider-data",
            "--trade-date",
            "2026-06-19",
            "--provider",
            "tushare",
            "--dataset",
            "stock_basic",
            "--smoke",
        ]
    )

    assert update.command == "update-provider-data"
    assert update.provider == "tushare"
    assert update.dataset == ["stock_basic"]
    assert update.smoke is True


def test_cli_accepts_goal10r_tushare_probe_command():
    parser = build_parser()

    probe = parser.parse_args(
        [
            "probe-tushare-goal10r",
            "--trade-date",
            "2024-06-19",
            "--sample-limit",
            "3",
            "--sleep-seconds",
            "12",
        ]
    )

    assert probe.command == "probe-tushare-goal10r"
    assert probe.trade_date == "2024-06-19"
    assert probe.sample_limit == 3
    assert probe.sleep_seconds == 12


def test_cli_rejects_external_provider_update_without_smoke_mode(capsys):
    exit_code = main(
        [
            "update-provider-data",
            "--trade-date",
            "2026-06-19",
            "--provider",
            "akshare",
            "--dataset",
            "stock_basic",
        ]
    )

    assert exit_code == 2
    assert "external provider updates must use --smoke" in capsys.readouterr().err


def test_cli_accepts_query_parquet_smoke_provider():
    parser = build_parser()

    query = parser.parse_args(
        [
            "query-parquet",
            "--dataset",
            "daily_price",
            "--trade-date",
            "2026-06-19",
            "--smoke-provider",
            "tushare",
        ]
    )

    assert query.command == "query-parquet"
    assert query.dataset == "daily_price"
    assert query.smoke_provider == "tushare"

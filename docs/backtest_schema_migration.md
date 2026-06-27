# Backtest Schema Migration Notes

`docker/postgres/init/001_schema.sql` is only executed by the official
PostgreSQL container when the database volume is empty. If `stock_pgdata`
already exists, editing that init file does not change the running database.

Goal 8 therefore keeps the init SQL as the fresh-install baseline and also
runs an idempotent runtime migration through
`stock_selector.backtesting.summary_repo.ensure_backtest_summary_schema`.

The runtime migration is called when the backtest summary repository is
created. It:

- creates `backtest_summary` if it is missing;
- adds Goal 8 columns such as `run_key`, `rebalance_mode`, cost parameters,
  `top_n`, `execution_rule`, and `status` if they are missing;
- recreates the `status` check constraint on PostgreSQL;
- creates the `run_key` unique index when supported.

Do not delete `stock_pgdata` to pick up Goal 8 schema changes. Run a command
that initializes the application schema, such as `init-db` or `run-backtest`,
and the runtime migration will bring an existing database forward.

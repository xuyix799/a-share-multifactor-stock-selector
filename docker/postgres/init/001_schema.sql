CREATE TABLE IF NOT EXISTS update_log (
    trade_date DATE NOT NULL,
    step_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'done', 'failed')),
    object_key TEXT,
    message TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (trade_date, step_name)
);

ALTER TABLE update_log DROP CONSTRAINT IF EXISTS update_log_status_check;
ALTER TABLE update_log
    ADD CONSTRAINT update_log_status_check
    CHECK (status IN ('pending', 'running', 'done', 'failed'));

CREATE TABLE IF NOT EXISTS selection_snapshot (
    id BIGSERIAL PRIMARY KEY,
    trade_date DATE NOT NULL,
    rebalance_mode TEXT NOT NULL,
    selected_count INTEGER NOT NULL DEFAULT 0,
    top_stocks JSONB NOT NULL DEFAULT '[]'::jsonb,
    object_key TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE selection_snapshot ADD COLUMN IF NOT EXISTS top_n INTEGER;
ALTER TABLE selection_snapshot ADD COLUMN IF NOT EXISTS stock_count INTEGER;
ALTER TABLE selection_snapshot ADD COLUMN IF NOT EXISTS avg_total_score DOUBLE PRECISION;
ALTER TABLE selection_snapshot ADD COLUMN IF NOT EXISTS max_total_score DOUBLE PRECISION;
ALTER TABLE selection_snapshot ADD COLUMN IF NOT EXISTS min_total_score DOUBLE PRECISION;
ALTER TABLE selection_snapshot ADD COLUMN IF NOT EXISTS rebalance_mode TEXT;
UPDATE selection_snapshot
SET rebalance_mode = 'daily'
WHERE rebalance_mode IS NULL OR btrim(rebalance_mode) = '';
ALTER TABLE selection_snapshot ALTER COLUMN rebalance_mode SET DEFAULT 'daily';
ALTER TABLE selection_snapshot ALTER COLUMN rebalance_mode SET NOT NULL;
DELETE FROM selection_snapshot older
USING selection_snapshot newer
WHERE older.trade_date = newer.trade_date
  AND older.rebalance_mode = newer.rebalance_mode
  AND (
      older.created_at < newer.created_at
      OR (older.created_at = newer.created_at AND older.id < newer.id)
  );
CREATE INDEX IF NOT EXISTS idx_selection_snapshot_trade_date ON selection_snapshot(trade_date);
CREATE UNIQUE INDEX IF NOT EXISTS ux_selection_snapshot_trade_date_mode
    ON selection_snapshot(trade_date, rebalance_mode);

CREATE TABLE IF NOT EXISTS backtest_summary (
    id BIGSERIAL PRIMARY KEY,
    strategy_name TEXT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    report_object_key TEXT,
    detail_object_key TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE backtest_summary ADD COLUMN IF NOT EXISTS run_key TEXT;
ALTER TABLE backtest_summary ADD COLUMN IF NOT EXISTS rebalance_mode TEXT;
ALTER TABLE backtest_summary ADD COLUMN IF NOT EXISTS initial_cash DOUBLE PRECISION;
ALTER TABLE backtest_summary ADD COLUMN IF NOT EXISTS commission_rate DOUBLE PRECISION;
ALTER TABLE backtest_summary ADD COLUMN IF NOT EXISTS slippage_bps DOUBLE PRECISION;
ALTER TABLE backtest_summary ADD COLUMN IF NOT EXISTS stamp_tax_rate DOUBLE PRECISION;
ALTER TABLE backtest_summary ADD COLUMN IF NOT EXISTS top_n INTEGER;
ALTER TABLE backtest_summary ADD COLUMN IF NOT EXISTS execution_rule TEXT;
ALTER TABLE backtest_summary ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'done';
ALTER TABLE backtest_summary DROP CONSTRAINT IF EXISTS backtest_summary_status_check;
ALTER TABLE backtest_summary
    ADD CONSTRAINT backtest_summary_status_check
    CHECK (status IN ('done', 'failed'));
CREATE UNIQUE INDEX IF NOT EXISTS idx_backtest_summary_run_key ON backtest_summary(run_key) WHERE run_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS factor_config (
    id BIGSERIAL PRIMARY KEY,
    config_name TEXT NOT NULL,
    config_version TEXT NOT NULL,
    weights JSONB NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (config_name, config_version)
);

CREATE TABLE IF NOT EXISTS user_watchlist (
    id BIGSERIAL PRIMARY KEY,
    stock_code TEXT NOT NULL,
    stock_name TEXT,
    note TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (stock_code)
);

CREATE TABLE IF NOT EXISTS task_log (
    id BIGSERIAL PRIMARY KEY,
    task_type TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'done', 'failed')),
    params JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_summary JSONB NOT NULL DEFAULT '{}'::jsonb,
    object_key TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

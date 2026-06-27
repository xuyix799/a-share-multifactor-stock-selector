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
CREATE INDEX IF NOT EXISTS idx_selection_snapshot_trade_date ON selection_snapshot(trade_date);

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

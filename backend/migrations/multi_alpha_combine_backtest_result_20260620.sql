-- Tier-1 multi-alpha combine-backtest result tables.
-- Forward migration is idempotent and additive. Apply manually; application
-- code must not execute this migration automatically.

CREATE SCHEMA IF NOT EXISTS strategy_pkg;

CREATE TABLE IF NOT EXISTS strategy_pkg.multi_alpha_combine_backtest_run (
    id TEXT PRIMARY KEY,
    roster_hash TEXT NOT NULL,
    roster_json JSONB NOT NULL,
    oos_start DATE NOT NULL,
    oos_end DATE NOT NULL,
    normalize_method TEXT NOT NULL,
    walk_forward_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    backtest_config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    baseline_leg_id TEXT,
    status TEXT NOT NULL DEFAULT 'running',
    reason JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_macb_run_window CHECK (oos_end >= oos_start),
    CONSTRAINT ck_macb_run_status CHECK (status IN ('running', 'succeeded', 'failed')),
    CONSTRAINT ck_macb_run_roster_json CHECK (jsonb_typeof(roster_json) = 'array'),
    CONSTRAINT ck_macb_run_normalize_method CHECK (normalize_method IN ('zscore', 'rank'))
);

CREATE INDEX IF NOT EXISTS idx_macb_run_created_at
    ON strategy_pkg.multi_alpha_combine_backtest_run(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_macb_run_status_created_at
    ON strategy_pkg.multi_alpha_combine_backtest_run(status, created_at DESC);

CREATE TABLE IF NOT EXISTS strategy_pkg.multi_alpha_combine_backtest_scheme_result (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES strategy_pkg.multi_alpha_combine_backtest_run(id) ON DELETE CASCADE,
    weighting_scheme TEXT NOT NULL,
    weights_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    per_window_weights_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    cagr DOUBLE PRECISION,
    max_drawdown DOUBLE PRECISION,
    sharpe DOUBLE PRECISION,
    calmar DOUBLE PRECISION,
    topk_return_20 DOUBLE PRECISION,
    topk_hit_rate_20 DOUBLE PRECISION,
    turnover DOUBLE PRECISION,
    vs_baseline_sharpe_delta DOUBLE PRECISION,
    vs_baseline_calmar_delta DOUBLE PRECISION,
    pred_persisted BOOLEAN NOT NULL DEFAULT FALSE,
    skipped BOOLEAN NOT NULL DEFAULT FALSE,
    skipped_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_macb_scheme_result UNIQUE (run_id, weighting_scheme),
    CONSTRAINT ck_macb_scheme_supported CHECK (weighting_scheme IN ('equal', 'orthogonality_aware', 'ic_weighted', 'risk_parity')),
    CONSTRAINT ck_macb_scheme_weights_json CHECK (jsonb_typeof(weights_json) = 'object'),
    CONSTRAINT ck_macb_scheme_window_weights_json CHECK (jsonb_typeof(per_window_weights_json) = 'array'),
    CONSTRAINT ck_macb_scheme_skip_reason CHECK ((skipped = FALSE) OR skipped_reason IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_macb_scheme_result_run
    ON strategy_pkg.multi_alpha_combine_backtest_scheme_result(run_id);

CREATE TABLE IF NOT EXISTS strategy_pkg.multi_alpha_combine_backtest_loo (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES strategy_pkg.multi_alpha_combine_backtest_run(id) ON DELETE CASCADE,
    weighting_scheme TEXT NOT NULL,
    dropped_leg_id TEXT NOT NULL,
    marginal_sharpe DOUBLE PRECISION,
    marginal_calmar DOUBLE PRECISION,
    marginal_cagr DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_macb_loo UNIQUE (run_id, weighting_scheme, dropped_leg_id),
    CONSTRAINT ck_macb_loo_scheme_supported CHECK (weighting_scheme IN ('equal', 'orthogonality_aware', 'ic_weighted', 'risk_parity'))
);

CREATE INDEX IF NOT EXISTS idx_macb_loo_run
    ON strategy_pkg.multi_alpha_combine_backtest_loo(run_id);

COMMENT ON TABLE strategy_pkg.multi_alpha_combine_backtest_run IS
    'Tier-1 multi-alpha combine-backtest job header; independent from QE task/scheduler tables.';
COMMENT ON TABLE strategy_pkg.multi_alpha_combine_backtest_scheme_result IS
    'Per-weighting-scheme metrics for multi-alpha combined pred-backtest runs. Metric columns are nullable for explicit skipped schemes.';
COMMENT ON TABLE strategy_pkg.multi_alpha_combine_backtest_loo IS
    'Leave-one-out marginal contribution metrics for multi-alpha combine-backtest runs.';

-- Rollback (manual only):
-- DROP TABLE IF EXISTS strategy_pkg.multi_alpha_combine_backtest_loo;
-- DROP TABLE IF EXISTS strategy_pkg.multi_alpha_combine_backtest_scheme_result;
-- DROP TABLE IF EXISTS strategy_pkg.multi_alpha_combine_backtest_run;

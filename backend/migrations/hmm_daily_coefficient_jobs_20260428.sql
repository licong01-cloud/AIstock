-- HMM daily coefficient async job audit table.
-- This table tracks manual daily as-of coefficient generation attempts. It
-- stores runtime job state and generated artifact metadata, but does not modify
-- HMM model snapshots, model weights, or historical coefficient assets.

CREATE TABLE IF NOT EXISTS model_train_daily_coefficient_jobs (
    job_id                TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
    snapshot_id           TEXT NOT NULL REFERENCES model_train_snapshots(snapshot_id) ON DELETE RESTRICT,
    config_id             TEXT NOT NULL REFERENCES model_train_configs(config_id) ON DELETE RESTRICT,
    signal_preset         TEXT NOT NULL,
    as_of_trade_date      DATE NOT NULL,
    effective_trade_date  DATE NOT NULL,
    generation_mode       TEXT NOT NULL,
    status                TEXT NOT NULL DEFAULT 'PENDING',
    result_status         TEXT,
    requested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at            TIMESTAMPTZ,
    completed_at          TIMESTAMPTZ,
    input_data_max_dates  JSONB NOT NULL DEFAULT '{}'::jsonb,
    output_path           TEXT NOT NULL,
    artifact_sha256       TEXT,
    plan_json             JSONB NOT NULL,
    result_json           JSONB,
    error_message         TEXT,
    error_context         JSONB
);

CREATE INDEX IF NOT EXISTS idx_model_train_daily_coeff_jobs_snapshot
    ON model_train_daily_coefficient_jobs (snapshot_id, requested_at DESC);

CREATE INDEX IF NOT EXISTS idx_model_train_daily_coeff_jobs_config
    ON model_train_daily_coefficient_jobs (config_id, requested_at DESC);

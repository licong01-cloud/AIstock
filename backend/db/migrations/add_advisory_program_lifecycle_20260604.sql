CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE IF NOT EXISTS app.advisory_program (
    program_id TEXT PRIMARY KEY,
    program_name TEXT NOT NULL,
    status TEXT NOT NULL,
    target_count INTEGER NOT NULL,
    package_mode TEXT NOT NULL,
    package_ids JSONB NOT NULL,
    package_weights JSONB NOT NULL,
    fusion_method TEXT,
    package_set_hash TEXT NOT NULL,
    fusion_policy_sha256 TEXT,
    review_policy JSONB NOT NULL,
    review_policy_sha256 TEXT NOT NULL,
    entry_price_basis TEXT NOT NULL,
    exit_price_basis TEXT NOT NULL,
    review_schedule JSONB NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    created_by TEXT,
    enabled_since TIMESTAMPTZ,
    last_review_status TEXT,
    latest_review_trade_date DATE,
    program_payload_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT advisory_program_status_check CHECK (status IN ('DRAFT', 'ENABLED', 'PAUSED', 'REVIEWING', 'WAITING_DATA', 'REVIEW_FAILED', 'ARCHIVED')),
    CONSTRAINT advisory_program_package_mode_check CHECK (package_mode IN ('single_package', 'fusion_pool', 'sleeve_mode_future')),
    CONSTRAINT advisory_program_entry_basis_check CHECK (entry_price_basis IN ('next_open_executable', 'signal_close', 'next_close')),
    CONSTRAINT advisory_program_exit_basis_check CHECK (exit_price_basis IN ('next_open_executable', 'signal_close', 'next_close'))
);

CREATE TABLE IF NOT EXISTS app.advisory_program_package (
    program_id TEXT NOT NULL REFERENCES app.advisory_program(program_id) ON DELETE CASCADE,
    program_version INTEGER NOT NULL,
    package_id TEXT NOT NULL,
    weight NUMERIC NOT NULL,
    package_role TEXT NOT NULL,
    package_order INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (program_id, program_version, package_id),
    CONSTRAINT advisory_program_package_weight_check CHECK (weight > 0)
);

ALTER TABLE app.advisory_daily_review
    ALTER COLUMN watchlist_item_id DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS program_id TEXT,
    ADD COLUMN IF NOT EXISTS program_version INTEGER,
    ADD COLUMN IF NOT EXISTS episode_id TEXT,
    ADD COLUMN IF NOT EXISTS review_status TEXT,
    ADD COLUMN IF NOT EXISTS fusion_evidence_json JSONB,
    ADD COLUMN IF NOT EXISTS decision_input_json JSONB;

CREATE UNIQUE INDEX IF NOT EXISTS ux_advisory_daily_review_program_symbol_date
    ON app.advisory_daily_review(program_id, code, trade_date)
    WHERE program_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS app.advisory_episode_return (
    snapshot_id BIGSERIAL PRIMARY KEY,
    episode_id TEXT NOT NULL,
    program_id TEXT NOT NULL REFERENCES app.advisory_program(program_id) ON DELETE CASCADE,
    program_version INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    episode_status TEXT NOT NULL,
    signal_date DATE NOT NULL,
    effective_entry_date DATE NOT NULL,
    entry_price NUMERIC NOT NULL,
    entry_price_basis TEXT NOT NULL,
    entry_rank INTEGER NOT NULL,
    entry_score DOUBLE PRECISION,
    current_rank INTEGER,
    current_score DOUBLE PRECISION,
    exit_signal_date DATE,
    effective_exit_date DATE,
    exit_price NUMERIC,
    exit_price_basis TEXT,
    exit_reason TEXT,
    holding_trading_days INTEGER NOT NULL DEFAULT 0,
    return_bps DOUBLE PRECISION,
    is_win BOOLEAN,
    win_rate_inclusion_status TEXT NOT NULL,
    max_runup_bps DOUBLE PRECISION,
    max_drawdown_bps DOUBLE PRECISION,
    still_active_mark_price NUMERIC,
    price_quality_status TEXT NOT NULL,
    weak_rank_confirm_days INTEGER NOT NULL DEFAULT 0,
    source_run_id TEXT,
    evidence_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    episode_payload_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT advisory_episode_status_check CHECK (episode_status IN ('ACTIVE', 'EXITED')),
    CONSTRAINT advisory_episode_entry_basis_check CHECK (entry_price_basis IN ('next_open_executable', 'signal_close', 'next_close')),
    CONSTRAINT advisory_episode_exit_basis_check CHECK (exit_price_basis IS NULL OR exit_price_basis IN ('next_open_executable', 'signal_close', 'next_close'))
);

CREATE TABLE IF NOT EXISTS app.advisory_replay_run (
    replay_run_id TEXT PRIMARY KEY,
    program_id TEXT NOT NULL REFERENCES app.advisory_program(program_id) ON DELETE CASCADE,
    program_version INTEGER NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    entry_price_basis TEXT NOT NULL,
    exit_price_basis TEXT NOT NULL,
    status TEXT NOT NULL,
    replay_config_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT advisory_replay_date_check CHECK (start_date <= end_date)
);

CREATE TABLE IF NOT EXISTS app.advisory_program_metric_snapshot (
    snapshot_id BIGSERIAL PRIMARY KEY,
    program_id TEXT NOT NULL REFERENCES app.advisory_program(program_id) ON DELETE CASCADE,
    snapshot_date DATE NOT NULL,
    enabled_since TIMESTAMPTZ,
    entered_episode_count INTEGER NOT NULL DEFAULT 0,
    active_count INTEGER NOT NULL DEFAULT 0,
    take_profit_count INTEGER NOT NULL DEFAULT 0,
    stop_loss_count INTEGER NOT NULL DEFAULT 0,
    win_rate DOUBLE PRECISION,
    avg_return_bps DOUBLE PRECISION,
    median_return_bps DOUBLE PRECISION,
    max_drawdown_bps DOUBLE PRECISION,
    avg_holding_days DOUBLE PRECISION,
    last_review_status TEXT,
    metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION app.prevent_advisory_episode_return_update()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'app.advisory_episode_return is append-only; UPDATE is forbidden';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_advisory_episode_return_no_update ON app.advisory_episode_return;
CREATE TRIGGER trg_advisory_episode_return_no_update
    BEFORE UPDATE ON app.advisory_episode_return
    FOR EACH ROW EXECUTE FUNCTION app.prevent_advisory_episode_return_update();

CREATE INDEX IF NOT EXISTS idx_advisory_program_status ON app.advisory_program(status, enabled_since);
CREATE INDEX IF NOT EXISTS idx_advisory_program_package_package ON app.advisory_program_package(package_id);
CREATE INDEX IF NOT EXISTS idx_advisory_episode_program_symbol ON app.advisory_episode_return(program_id, symbol, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_advisory_episode_program_status ON app.advisory_episode_return(program_id, episode_status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_advisory_replay_program_date ON app.advisory_replay_run(program_id, start_date, end_date);
CREATE INDEX IF NOT EXISTS idx_advisory_metric_program_created ON app.advisory_program_metric_snapshot(program_id, created_at DESC);

COMMENT ON TABLE app.advisory_program IS 'Advisory Program configuration dimension; one row per long-running recommendation program, advisory-only and not an execution account.';
COMMENT ON COLUMN app.advisory_program.program_id IS 'Stable Advisory Program identifier used to isolate concurrent recommendation lifecycles.';
COMMENT ON COLUMN app.advisory_program.program_name IS 'Operator-facing Advisory Program name.';
COMMENT ON COLUMN app.advisory_program.status IS 'Lifecycle status: DRAFT, ENABLED, PAUSED, REVIEWING, WAITING_DATA, REVIEW_FAILED, or ARCHIVED.';
COMMENT ON COLUMN app.advisory_program.target_count IS 'Target active recommendation count, normally 20 for Top20 advisory pools.';
COMMENT ON COLUMN app.advisory_program.package_mode IS 'StrategyPackage binding mode: single_package, fusion_pool, or design-reserved sleeve_mode_future.';
COMMENT ON COLUMN app.advisory_program.package_ids IS 'JSON array of StrategyPackage ids bound to this Advisory Program version.';
COMMENT ON COLUMN app.advisory_program.package_weights IS 'JSON object of StrategyPackage weights used for weighted_rank_fusion; keys match package_ids.';
COMMENT ON COLUMN app.advisory_program.fusion_method IS 'Fusion method, currently weighted_rank_fusion for fusion_pool and NULL for single_package.';
COMMENT ON COLUMN app.advisory_program.package_set_hash IS 'SHA256 hash of package mode and package ids for audit.';
COMMENT ON COLUMN app.advisory_program.fusion_policy_sha256 IS 'SHA256 hash of fusion method and package weights; NULL for single-package programs.';
COMMENT ON COLUMN app.advisory_program.review_policy IS 'JSON review policy containing rank thresholds, replacement budget, stop-loss, take-profit, and time-stop rules.';
COMMENT ON COLUMN app.advisory_program.review_policy_sha256 IS 'SHA256 hash of the review policy used to audit daily review decisions.';
COMMENT ON COLUMN app.advisory_program.entry_price_basis IS 'Episode entry price basis; default next_open_executable avoids signal-close lookahead.';
COMMENT ON COLUMN app.advisory_program.exit_price_basis IS 'Episode exit price basis used for advisory return snapshots.';
COMMENT ON COLUMN app.advisory_program.review_schedule IS 'JSON schedule metadata for daily review automation; no broker/order side effects.';
COMMENT ON COLUMN app.advisory_program.version IS 'SCD-style configuration version; config edits increment version while status changes do not rewrite history.';
COMMENT ON COLUMN app.advisory_program.created_by IS 'Operator or system actor that created the Advisory Program.';
COMMENT ON COLUMN app.advisory_program.enabled_since IS 'Timestamp when the Advisory Program was first enabled.';
COMMENT ON COLUMN app.advisory_program.last_review_status IS 'Only automatically retained data-quality/status field for leaderboard display.';
COMMENT ON COLUMN app.advisory_program.latest_review_trade_date IS 'Latest trade date reviewed for this Advisory Program.';
COMMENT ON COLUMN app.advisory_program.program_payload_json IS 'Full service payload for forward-compatible readback; mirrors typed columns.';
COMMENT ON COLUMN app.advisory_program.created_at IS 'Program row creation timestamp.';
COMMENT ON COLUMN app.advisory_program.updated_at IS 'Program row update timestamp.';

COMMENT ON TABLE app.advisory_program_package IS 'Versioned package binding rows for Advisory Program package isolation and audit.';
COMMENT ON COLUMN app.advisory_program_package.program_id IS 'Advisory Program identifier.';
COMMENT ON COLUMN app.advisory_program_package.program_version IS 'Advisory Program configuration version.';
COMMENT ON COLUMN app.advisory_program_package.package_id IS 'StrategyPackage identifier included in this Advisory Program version.';
COMMENT ON COLUMN app.advisory_program_package.weight IS 'Positive fusion weight for the package; 1.0 for single-package programs.';
COMMENT ON COLUMN app.advisory_program_package.package_role IS 'Package role: primary or fusion_member.';
COMMENT ON COLUMN app.advisory_program_package.package_order IS 'Stable display order for package bindings.';
COMMENT ON COLUMN app.advisory_program_package.created_at IS 'Binding creation timestamp.';

COMMENT ON COLUMN app.advisory_daily_review.program_id IS 'Optional Advisory Program id for full lifecycle reviews; NULL preserves legacy watchlist review rows.';
COMMENT ON COLUMN app.advisory_daily_review.program_version IS 'Advisory Program config version used by this daily review.';
COMMENT ON COLUMN app.advisory_daily_review.episode_id IS 'Advisory episode id affected by this review decision; may be NULL for waiting candidate rows.';
COMMENT ON COLUMN app.advisory_daily_review.review_status IS 'Review status for the decision row: SUCCEEDED, WAITING_DATA, REVIEW_FAILED, or STALE.';
COMMENT ON COLUMN app.advisory_daily_review.fusion_evidence_json IS 'Per-package rank/score/fusion evidence used before the decision; future outcomes are forbidden.';
COMMENT ON COLUMN app.advisory_daily_review.decision_input_json IS 'Decision-time input payload for audit; must not contain future returns or future prices.';

COMMENT ON TABLE app.advisory_episode_return IS 'Append-only Advisory Program episode return snapshots; advisory diagnostics only, not validated PnL.';
COMMENT ON COLUMN app.advisory_episode_return.snapshot_id IS 'Surrogate id for each append-only episode snapshot.';
COMMENT ON COLUMN app.advisory_episode_return.episode_id IS 'Stable episode id from entry to exit; re-entry creates a new episode_id.';
COMMENT ON COLUMN app.advisory_episode_return.program_id IS 'Advisory Program identifier that owns this episode.';
COMMENT ON COLUMN app.advisory_episode_return.program_version IS 'Program version active when this snapshot was produced.';
COMMENT ON COLUMN app.advisory_episode_return.symbol IS 'A-share symbol under advisory review.';
COMMENT ON COLUMN app.advisory_episode_return.episode_status IS 'Episode status: ACTIVE or EXITED.';
COMMENT ON COLUMN app.advisory_episode_return.signal_date IS 'Trade date when the recommendation signal was selected.';
COMMENT ON COLUMN app.advisory_episode_return.effective_entry_date IS 'Date when entry becomes effective; default T+1 for next_open_executable.';
COMMENT ON COLUMN app.advisory_episode_return.entry_price IS 'Advisory entry basis price used for return calculation, not a broker fill.';
COMMENT ON COLUMN app.advisory_episode_return.entry_price_basis IS 'Entry price basis: next_open_executable, signal_close, or next_close.';
COMMENT ON COLUMN app.advisory_episode_return.entry_rank IS 'Canonical rank at episode entry; fusion_pool uses fusion_rank.';
COMMENT ON COLUMN app.advisory_episode_return.entry_score IS 'Canonical score at episode entry.';
COMMENT ON COLUMN app.advisory_episode_return.current_rank IS 'Latest canonical rank from daily review evidence.';
COMMENT ON COLUMN app.advisory_episode_return.current_score IS 'Latest canonical score from daily review evidence.';
COMMENT ON COLUMN app.advisory_episode_return.exit_signal_date IS 'Date when exit was signaled by stop, take, rank decay, or time-stop.';
COMMENT ON COLUMN app.advisory_episode_return.effective_exit_date IS 'Date when exit price basis becomes effective.';
COMMENT ON COLUMN app.advisory_episode_return.exit_price IS 'Advisory exit basis price used for return calculation.';
COMMENT ON COLUMN app.advisory_episode_return.exit_price_basis IS 'Exit price basis used by the snapshot.';
COMMENT ON COLUMN app.advisory_episode_return.exit_reason IS 'Exit reason such as STOP_LOSS, STOP_LOSS_DEFERRED_T1, TRAILING_TAKE_PROFIT, ALPHA_RANK_DROP_EXIT, or TIME_STOP.';
COMMENT ON COLUMN app.advisory_episode_return.holding_trading_days IS 'Approximate reviewed holding days counted by daily review snapshots.';
COMMENT ON COLUMN app.advisory_episode_return.return_bps IS 'Episode return in basis points using advisory entry/exit or mark price.';
COMMENT ON COLUMN app.advisory_episode_return.is_win IS 'Whether return_bps is greater than zero for win-rate calculation.';
COMMENT ON COLUMN app.advisory_episode_return.win_rate_inclusion_status IS 'Inclusion flag for internally derived win-rate denominator; denominator fields are not persisted separately.';
COMMENT ON COLUMN app.advisory_episode_return.max_runup_bps IS 'Maximum observed run-up in basis points during the episode.';
COMMENT ON COLUMN app.advisory_episode_return.max_drawdown_bps IS 'Maximum observed drawdown in basis points during the episode.';
COMMENT ON COLUMN app.advisory_episode_return.still_active_mark_price IS 'Latest mark price for active episodes.';
COMMENT ON COLUMN app.advisory_episode_return.price_quality_status IS 'Price quality status for this snapshot.';
COMMENT ON COLUMN app.advisory_episode_return.weak_rank_confirm_days IS 'Consecutive review days beyond rank_exit_threshold.';
COMMENT ON COLUMN app.advisory_episode_return.source_run_id IS 'Selection Center run id that supplied candidate evidence, if applicable.';
COMMENT ON COLUMN app.advisory_episode_return.evidence_json IS 'Decision evidence including package ranks, fusion hash, and policy hash.';
COMMENT ON COLUMN app.advisory_episode_return.episode_payload_json IS 'Full typed service payload for forward-compatible readback.';
COMMENT ON COLUMN app.advisory_episode_return.created_at IS 'Snapshot insertion timestamp.';
COMMENT ON COLUMN app.advisory_episode_return.updated_at IS 'Episode snapshot logical update timestamp.';

COMMENT ON TABLE app.advisory_replay_run IS 'Advisory lifecycle replay run metadata over historical trade dates; does not simulate accounts or orders.';
COMMENT ON COLUMN app.advisory_replay_run.replay_run_id IS 'Replay run identifier.';
COMMENT ON COLUMN app.advisory_replay_run.program_id IS 'Advisory Program replayed.';
COMMENT ON COLUMN app.advisory_replay_run.program_version IS 'Program version replayed.';
COMMENT ON COLUMN app.advisory_replay_run.start_date IS 'Replay start trade date.';
COMMENT ON COLUMN app.advisory_replay_run.end_date IS 'Replay end trade date.';
COMMENT ON COLUMN app.advisory_replay_run.entry_price_basis IS 'Entry basis used for replay; default next_open_executable.';
COMMENT ON COLUMN app.advisory_replay_run.exit_price_basis IS 'Exit basis used for replay.';
COMMENT ON COLUMN app.advisory_replay_run.status IS 'Replay status such as SUCCEEDED, WAITING_DATA, or REVIEW_FAILED.';
COMMENT ON COLUMN app.advisory_replay_run.replay_config_json IS 'Replay configuration and PIT cutoff metadata.';
COMMENT ON COLUMN app.advisory_replay_run.summary_json IS 'Replay summary metrics, including win rate and average return.';
COMMENT ON COLUMN app.advisory_replay_run.error_json IS 'Structured replay failure context if status is REVIEW_FAILED.';
COMMENT ON COLUMN app.advisory_replay_run.created_at IS 'Replay creation timestamp.';
COMMENT ON COLUMN app.advisory_replay_run.completed_at IS 'Replay completion timestamp.';

COMMENT ON TABLE app.advisory_program_metric_snapshot IS 'Program-level leaderboard metric snapshots; only last_review_status is retained as automatic quality/status field.';
COMMENT ON COLUMN app.advisory_program_metric_snapshot.snapshot_id IS 'Metric snapshot id.';
COMMENT ON COLUMN app.advisory_program_metric_snapshot.program_id IS 'Advisory Program identifier.';
COMMENT ON COLUMN app.advisory_program_metric_snapshot.snapshot_date IS 'Calendar date of metric snapshot creation.';
COMMENT ON COLUMN app.advisory_program_metric_snapshot.enabled_since IS 'Program enabled timestamp shown in leaderboard.';
COMMENT ON COLUMN app.advisory_program_metric_snapshot.entered_episode_count IS 'Cumulative recommendation episodes entered; re-entry counts as a new episode.';
COMMENT ON COLUMN app.advisory_program_metric_snapshot.active_count IS 'Current active recommendation count.';
COMMENT ON COLUMN app.advisory_program_metric_snapshot.take_profit_count IS 'Cumulative exited episode count with take-profit reasons.';
COMMENT ON COLUMN app.advisory_program_metric_snapshot.stop_loss_count IS 'Cumulative exited episode count with stop-loss reason.';
COMMENT ON COLUMN app.advisory_program_metric_snapshot.win_rate IS 'Win episode ratio computed from internally derived evaluable episodes; denominator fields are intentionally not recorded.';
COMMENT ON COLUMN app.advisory_program_metric_snapshot.avg_return_bps IS 'Average episode return in basis points.';
COMMENT ON COLUMN app.advisory_program_metric_snapshot.median_return_bps IS 'Median episode return in basis points.';
COMMENT ON COLUMN app.advisory_program_metric_snapshot.max_drawdown_bps IS 'Worst episode drawdown in basis points.';
COMMENT ON COLUMN app.advisory_program_metric_snapshot.avg_holding_days IS 'Average episode holding days.';
COMMENT ON COLUMN app.advisory_program_metric_snapshot.last_review_status IS 'Latest daily review status displayed by leaderboard.';
COMMENT ON COLUMN app.advisory_program_metric_snapshot.metrics_json IS 'Full metric payload for UI display; only last_review_status is retained as automatic quality/status metadata.';
COMMENT ON COLUMN app.advisory_program_metric_snapshot.created_at IS 'Metric snapshot creation timestamp.';

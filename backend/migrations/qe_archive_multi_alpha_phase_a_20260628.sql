-- Phase A: QE archive materialization for multi-alpha combine backtests.
-- Forward migration is additive and idempotent. Apply manually only after the
-- production DDL gate is approved; application services must not run this DDL.

BEGIN;

CREATE SCHEMA IF NOT EXISTS qe_archive;

ALTER TABLE IF EXISTS qe_archive.run
    DROP CONSTRAINT IF EXISTS ck_qear_run_status;

ALTER TABLE IF EXISTS qe_archive.run
    ADD CONSTRAINT ck_qear_run_status CHECK (
        status IN (
            'pending',
            'running',
            'completed',
            'failed',
            'interrupted',
            'partial_archived',
            'archived',
            'succeeded',
            'partial_failed'
        )
    );

CREATE INDEX IF NOT EXISTS idx_qear_run_type_status
    ON qe_archive.run(run_type, source_system, status);

CREATE TABLE IF NOT EXISTS qe_archive.multi_alpha_run (
    run_id TEXT PRIMARY KEY REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    roster_hash TEXT NOT NULL,
    oos_start DATE NOT NULL,
    oos_end DATE NOT NULL,
    normalize_method TEXT NOT NULL,
    walk_forward_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    baseline_leg_id TEXT,
    leg_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    logical_status TEXT,
    reason_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_created_at TIMESTAMPTZ,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_qear_macb_run_window CHECK (oos_end >= oos_start),
    CONSTRAINT ck_qear_macb_run_status CHECK (status IN ('succeeded','partial_failed','failed')),
    CONSTRAINT ck_qear_macb_run_logical_status CHECK (logical_status IS NULL OR logical_status IN ('succeeded','partial_failed','failed')),
    CONSTRAINT ck_qear_macb_run_leg_count CHECK (leg_count >= 0),
    CONSTRAINT ck_qear_macb_run_walk_forward_json CHECK (jsonb_typeof(walk_forward_json) = 'object'),
    CONSTRAINT ck_qear_macb_run_reason_json CHECK (jsonb_typeof(reason_json) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_qear_macb_run_roster
    ON qe_archive.multi_alpha_run(roster_hash, oos_start, oos_end);
CREATE INDEX IF NOT EXISTS idx_qear_macb_run_status
    ON qe_archive.multi_alpha_run(status, archived_at DESC);

CREATE TABLE IF NOT EXISTS qe_archive.multi_alpha_leg (
    run_id TEXT NOT NULL REFERENCES qe_archive.multi_alpha_run(run_id) ON DELETE CASCADE,
    leg_id TEXT NOT NULL,
    leg_order INTEGER NOT NULL,
    seed_run_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    factor_set_hash TEXT,
    factor_names JSONB NOT NULL DEFAULT '[]'::jsonb,
    factor_count INTEGER NOT NULL DEFAULT 0,
    model_type TEXT,
    model_family TEXT,
    freq TEXT,
    label_horizon INTEGER,
    seed_count INTEGER NOT NULL DEFAULT 0,
    source_run_meta JSONB NOT NULL DEFAULT '{}'::jsonb,
    provenance_complete BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (run_id, leg_id),
    CONSTRAINT ck_qear_macb_leg_order CHECK (leg_order >= 0),
    CONSTRAINT ck_qear_macb_leg_seed_run_ids CHECK (jsonb_typeof(seed_run_ids) = 'array'),
    CONSTRAINT ck_qear_macb_leg_factor_names CHECK (jsonb_typeof(factor_names) = 'array'),
    CONSTRAINT ck_qear_macb_leg_factor_count CHECK (factor_count >= 0),
    CONSTRAINT ck_qear_macb_leg_seed_count CHECK (seed_count >= 0),
    CONSTRAINT ck_qear_macb_leg_source_run_meta CHECK (jsonb_typeof(source_run_meta) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_qear_macb_leg_factor_hash
    ON qe_archive.multi_alpha_leg(factor_set_hash);

CREATE TABLE IF NOT EXISTS qe_archive.multi_alpha_leg_source (
    run_id TEXT NOT NULL,
    leg_id TEXT NOT NULL,
    source_seq INTEGER NOT NULL,
    seed_ref TEXT NOT NULL,
    seed_ref_kind TEXT NOT NULL,
    source_experiment_id TEXT,
    source_task_id TEXT,
    source_loop_id TEXT,
    source_loop_index INTEGER,
    source_run_type TEXT,
    source_model_type TEXT,
    source_factor_set_hash TEXT,
    resolved BOOLEAN NOT NULL DEFAULT FALSE,
    resolve_method TEXT NOT NULL,
    resolve_note TEXT,
    PRIMARY KEY (run_id, leg_id, source_seq),
    FOREIGN KEY (run_id, leg_id) REFERENCES qe_archive.multi_alpha_leg(run_id, leg_id) ON DELETE CASCADE,
    CONSTRAINT ck_qear_macb_leg_source_seq CHECK (source_seq >= 1),
    CONSTRAINT ck_qear_macb_leg_source_kind CHECK (seed_ref_kind IN ('archive_run_id','evolution_loop_id','unknown')),
    CONSTRAINT ck_qear_macb_leg_source_resolved CHECK (
        (resolved = TRUE AND source_experiment_id IS NOT NULL AND source_loop_id IS NOT NULL AND source_loop_index IS NOT NULL AND source_run_type IS NOT NULL)
        OR (resolved = FALSE AND resolve_note IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_qear_macb_leg_source_exp_loop
    ON qe_archive.multi_alpha_leg_source(source_experiment_id, source_loop_id, source_loop_index);
CREATE INDEX IF NOT EXISTS idx_qear_macb_leg_source_seed
    ON qe_archive.multi_alpha_leg_source(seed_ref);

CREATE TABLE IF NOT EXISTS qe_archive.multi_alpha_scheme (
    run_id TEXT NOT NULL REFERENCES qe_archive.multi_alpha_run(run_id) ON DELETE CASCADE,
    weighting_scheme TEXT NOT NULL,
    scheme_algorithm TEXT NOT NULL,
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
    is_best BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (run_id, weighting_scheme),
    CONSTRAINT ck_qear_macb_scheme_weights_json CHECK (jsonb_typeof(weights_json) = 'object'),
    CONSTRAINT ck_qear_macb_scheme_window_weights_json CHECK (jsonb_typeof(per_window_weights_json) = 'array'),
    CONSTRAINT ck_qear_macb_scheme_skip_reason CHECK ((skipped = FALSE) OR skipped_reason IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_qear_macb_scheme_best
    ON qe_archive.multi_alpha_scheme(is_best, sharpe DESC NULLS LAST);

CREATE TABLE IF NOT EXISTS qe_archive.multi_alpha_loo (
    run_id TEXT NOT NULL,
    weighting_scheme TEXT NOT NULL,
    dropped_leg_id TEXT NOT NULL,
    marginal_cagr DOUBLE PRECISION,
    marginal_sharpe DOUBLE PRECISION,
    marginal_calmar DOUBLE PRECISION,
    PRIMARY KEY (run_id, weighting_scheme, dropped_leg_id),
    FOREIGN KEY (run_id, weighting_scheme) REFERENCES qe_archive.multi_alpha_scheme(run_id, weighting_scheme) ON DELETE CASCADE,
    FOREIGN KEY (run_id, dropped_leg_id) REFERENCES qe_archive.multi_alpha_leg(run_id, leg_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_qear_macb_loo_leg
    ON qe_archive.multi_alpha_loo(run_id, dropped_leg_id);

ALTER TABLE IF EXISTS strategy_pkg.multi_alpha_combine_backtest_run
    DROP CONSTRAINT IF EXISTS ck_macb_run_status;

ALTER TABLE IF EXISTS strategy_pkg.multi_alpha_combine_backtest_run
    ADD CONSTRAINT ck_macb_run_status CHECK (status IN ('running', 'succeeded', 'failed', 'partial_failed'));

UPDATE strategy_pkg.multi_alpha_combine_backtest_run
SET status = 'partial_failed'
WHERE status = 'failed'
  AND reason->>'logical_status' = 'partial_failed';

COMMENT ON TABLE qe_archive.multi_alpha_run IS 'Archived multi-alpha combine-backtest run header and roster-level configuration snapshot.';
COMMENT ON COLUMN qe_archive.multi_alpha_run.run_id IS 'Stable QE archive run identifier used as the primary cross table join key.';
COMMENT ON COLUMN qe_archive.multi_alpha_run.roster_hash IS 'Deterministic hash of the multi-alpha roster used to build the combine-backtest run.';
COMMENT ON COLUMN qe_archive.multi_alpha_run.oos_start IS 'First out-of-sample trade date covered by the combine-backtest run.';
COMMENT ON COLUMN qe_archive.multi_alpha_run.oos_end IS 'Last out-of-sample trade date covered by the combine-backtest run.';
COMMENT ON COLUMN qe_archive.multi_alpha_run.normalize_method IS 'Score normalization method used before multi-alpha combination.';
COMMENT ON COLUMN qe_archive.multi_alpha_run.walk_forward_json IS 'Walk-forward weighting configuration such as window, min_periods, and expanding mode.';
COMMENT ON COLUMN qe_archive.multi_alpha_run.baseline_leg_id IS 'Optional leg id used as the baseline for scheme delta metrics.';
COMMENT ON COLUMN qe_archive.multi_alpha_run.leg_count IS 'Number of legs in the archived multi-alpha roster.';
COMMENT ON COLUMN qe_archive.multi_alpha_run.status IS 'Archive or source run status.';
COMMENT ON COLUMN qe_archive.multi_alpha_run.logical_status IS 'Business logical status preserved separately from legacy storage mappings.';
COMMENT ON COLUMN qe_archive.multi_alpha_run.reason_json IS 'Terminal reason and failure details copied from the source combine-backtest run.';
COMMENT ON COLUMN qe_archive.multi_alpha_run.source_created_at IS 'Creation timestamp reported by the upstream source system.';
COMMENT ON COLUMN qe_archive.multi_alpha_run.archived_at IS 'Timestamp when archive processing marked the run archived.';

COMMENT ON TABLE qe_archive.multi_alpha_leg IS 'Materialized per-leg snapshot for multi-alpha combine-backtest provenance and replay.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg.run_id IS 'Stable QE archive run identifier used as the primary cross table join key.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg.leg_id IS 'Stable leg identifier within one multi-alpha combine-backtest roster.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg.leg_order IS 'Deterministic zero-based order of the leg within the source roster.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg.seed_run_ids IS 'Original seed run identifiers used to build this leg.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg.factor_set_hash IS 'Deterministic hash of the exact ordered factor list or feature set.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg.factor_names IS 'Ordered factor names materialized for this leg when available.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg.factor_count IS 'Number of factors or features included in the archived run.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg.model_type IS 'Concrete model type or implementation name used by the run.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg.model_family IS 'High level model family such as tree, linear, LSTM, or deep model.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg.freq IS 'Data frequency used by the run, for example day or minute.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg.label_horizon IS 'Prediction label horizon used by the model or dataset.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg.seed_count IS 'Number of seed run identifiers listed for this leg.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg.source_run_meta IS 'JSON snapshot of resolved seed metadata, unresolved reasons, and leg metadata.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg.provenance_complete IS 'Whether every seed source was resolved and required leg metadata was materialized.';

COMMENT ON TABLE qe_archive.multi_alpha_leg_source IS 'Per-seed precise provenance from a multi-alpha leg to QE experiment, loop, and run coordinates.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg_source.run_id IS 'Stable QE archive run identifier used as the primary cross table join key.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg_source.leg_id IS 'Stable leg identifier within one multi-alpha combine-backtest roster.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg_source.source_seq IS 'One-based source sequence within a leg seed list.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg_source.seed_ref IS 'Original seed reference string from the source roster.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg_source.seed_ref_kind IS 'Parsed seed reference kind: archive_run_id, evolution_loop_id, or unknown.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg_source.source_experiment_id IS 'Resolved QE experiment id for this seed source.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg_source.source_task_id IS 'Resolved QE evolution task id for this seed source when applicable.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg_source.source_loop_id IS 'Resolved QE loop id for this seed source when applicable.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg_source.source_loop_index IS 'Resolved QE loop index for this seed source when applicable.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg_source.source_run_type IS 'Resolved QE archive run type, such as evolution_loop or single_experiment.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg_source.source_model_type IS 'Resolved model type copied from the source QE run.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg_source.source_factor_set_hash IS 'Resolved factor set hash copied from the source QE run.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg_source.resolved IS 'Whether the seed reference was resolved to precise QE coordinates.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg_source.resolve_method IS 'Resolution method used for the seed reference.';
COMMENT ON COLUMN qe_archive.multi_alpha_leg_source.resolve_note IS 'Resolution note or explicit unresolved reason.';

COMMENT ON TABLE qe_archive.multi_alpha_scheme IS 'Per-weighting-scheme weights, rolling weights, metrics, and baseline deltas for a multi-alpha run.';
COMMENT ON COLUMN qe_archive.multi_alpha_scheme.run_id IS 'Stable QE archive run identifier used as the primary cross table join key.';
COMMENT ON COLUMN qe_archive.multi_alpha_scheme.weighting_scheme IS 'Source weighting scheme identifier for multi-alpha combination.';
COMMENT ON COLUMN qe_archive.multi_alpha_scheme.scheme_algorithm IS 'Human-meaningful algorithm family for the weighting scheme.';
COMMENT ON COLUMN qe_archive.multi_alpha_scheme.weights_json IS 'Static leg weight payload for the weighting scheme.';
COMMENT ON COLUMN qe_archive.multi_alpha_scheme.per_window_weights_json IS 'Rolling or per-window weight trajectory payload for the weighting scheme.';
COMMENT ON COLUMN qe_archive.multi_alpha_scheme.cagr IS 'Compound annual growth rate metric.';
COMMENT ON COLUMN qe_archive.multi_alpha_scheme.max_drawdown IS 'Maximum drawdown metric.';
COMMENT ON COLUMN qe_archive.multi_alpha_scheme.sharpe IS 'Sharpe or information-ratio style risk-adjusted return metric.';
COMMENT ON COLUMN qe_archive.multi_alpha_scheme.calmar IS 'Calmar ratio metric.';
COMMENT ON COLUMN qe_archive.multi_alpha_scheme.topk_return_20 IS 'Top-20 selected stock return quality metric.';
COMMENT ON COLUMN qe_archive.multi_alpha_scheme.topk_hit_rate_20 IS 'Top-20 selected stock hit-rate quality metric.';
COMMENT ON COLUMN qe_archive.multi_alpha_scheme.turnover IS 'Turnover metric for the backtest or prediction list.';
COMMENT ON COLUMN qe_archive.multi_alpha_scheme.vs_baseline_sharpe_delta IS 'Sharpe difference versus the configured baseline leg.';
COMMENT ON COLUMN qe_archive.multi_alpha_scheme.vs_baseline_calmar_delta IS 'Calmar difference versus the configured baseline leg.';
COMMENT ON COLUMN qe_archive.multi_alpha_scheme.pred_persisted IS 'Whether the combined prediction artifact was persisted by the source run.';
COMMENT ON COLUMN qe_archive.multi_alpha_scheme.skipped IS 'Whether this scheme was explicitly skipped or failed before metrics were available.';
COMMENT ON COLUMN qe_archive.multi_alpha_scheme.skipped_reason IS 'Explicit reason for a skipped multi-alpha scheme.';
COMMENT ON COLUMN qe_archive.multi_alpha_scheme.is_best IS 'Whether this scheme is the best non-skipped scheme selected for the run.';

COMMENT ON TABLE qe_archive.multi_alpha_loo IS 'Leave-one-out marginal contribution metrics for multi-alpha weighting schemes.';
COMMENT ON COLUMN qe_archive.multi_alpha_loo.run_id IS 'Stable QE archive run identifier used as the primary cross table join key.';
COMMENT ON COLUMN qe_archive.multi_alpha_loo.weighting_scheme IS 'Source weighting scheme identifier for multi-alpha combination.';
COMMENT ON COLUMN qe_archive.multi_alpha_loo.dropped_leg_id IS 'Leg id dropped for leave-one-out marginal contribution measurement.';
COMMENT ON COLUMN qe_archive.multi_alpha_loo.marginal_cagr IS 'CAGR marginal contribution versus the full scheme.';
COMMENT ON COLUMN qe_archive.multi_alpha_loo.marginal_sharpe IS 'Sharpe marginal contribution versus the full scheme.';
COMMENT ON COLUMN qe_archive.multi_alpha_loo.marginal_calmar IS 'Calmar marginal contribution versus the full scheme.';

COMMENT ON CONSTRAINT ck_macb_run_status ON strategy_pkg.multi_alpha_combine_backtest_run IS 'Allows running, succeeded, failed, and Phase A partial_failed terminal status without rewriting old values.';

COMMIT;

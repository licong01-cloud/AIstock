-- Phase 4 Master Seed Contract additive migration draft.
-- Draft only: do not execute against production DB until reviewed and scheduled.
-- All objects live in strategy_pkg schema; no public schema changes.

CREATE SCHEMA IF NOT EXISTS strategy_pkg;

ALTER TABLE strategy_pkg.package ADD COLUMN IF NOT EXISTS seed_policy TEXT NULL;
ALTER TABLE strategy_pkg.package ADD COLUMN IF NOT EXISTS master_seed BIGINT NULL;
ALTER TABLE strategy_pkg.package ADD COLUMN IF NOT EXISTS seed_sequence JSONB NULL;
ALTER TABLE strategy_pkg.package ADD COLUMN IF NOT EXISTS seed_contract JSONB NULL;
ALTER TABLE strategy_pkg.package ADD COLUMN IF NOT EXISTS seed_contract_sha256 TEXT NULL;
ALTER TABLE strategy_pkg.package ADD COLUMN IF NOT EXISTS reproducibility_level TEXT NULL;
ALTER TABLE strategy_pkg.package ADD COLUMN IF NOT EXISTS nondeterministic_flags JSONB NULL;

-- PostgreSQL CHECK constraints do not have a native IF NOT EXISTS form,
-- so reruns must guard by table oid and constraint name before adding them.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint
        WHERE conname = 'package_seed_policy_check'
          AND conrelid = to_regclass('strategy_pkg.package')
    ) THEN
        ALTER TABLE strategy_pkg.package
            ADD CONSTRAINT package_seed_policy_check
            CHECK (seed_policy IS NULL OR seed_policy IN ('fixed', 'multi_seed', 'random_logged', 'unset_legacy'));
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint
        WHERE conname = 'package_master_seed_range_check'
          AND conrelid = to_regclass('strategy_pkg.package')
    ) THEN
        ALTER TABLE strategy_pkg.package
            ADD CONSTRAINT package_master_seed_range_check
            CHECK (master_seed IS NULL OR master_seed >= 0);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS strategy_pkg.seed_fragility_score (
    package_id TEXT PRIMARY KEY REFERENCES strategy_pkg.package(package_id),
    manifest_sha256 TEXT NOT NULL,
    seed_policy TEXT NOT NULL,
    master_seed BIGINT,
    seed_sequence JSONB NOT NULL DEFAULT '[]'::jsonb,
    metric_mean_by_seed JSONB NOT NULL DEFAULT '{}'::jsonb,
    metric_std_by_seed JSONB NOT NULL DEFAULT '{}'::jsonb,
    worst_seed_metric JSONB NOT NULL DEFAULT '{}'::jsonb,
    best_seed_metric JSONB NOT NULL DEFAULT '{}'::jsonb,
    seed_sensitivity_score DOUBLE PRECISION,
    rank_stability DOUBLE PRECISION,
    factor_importance_stability JSONB NOT NULL DEFAULT '{}'::jsonb,
    selection_overlap_by_seed JSONB NOT NULL DEFAULT '{}'::jsonb,
    seed_fragile BOOLEAN NOT NULL DEFAULT FALSE,
    reproducibility_level TEXT NOT NULL DEFAULT 'audit_only',
    nondeterministic_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT seed_fragility_score_policy_check CHECK (seed_policy IN ('fixed', 'multi_seed', 'random_logged', 'unset_legacy')),
    CONSTRAINT seed_fragility_score_master_seed_range_check CHECK (master_seed IS NULL OR master_seed >= 0),
    CONSTRAINT seed_fragility_score_sensitivity_nonnegative_check CHECK (seed_sensitivity_score IS NULL OR seed_sensitivity_score >= 0),
    CONSTRAINT seed_fragility_score_rank_stability_range_check CHECK (rank_stability IS NULL OR (rank_stability >= 0 AND rank_stability <= 1))
);

CREATE INDEX IF NOT EXISTS idx_seed_fragility_score_manifest ON strategy_pkg.seed_fragility_score(manifest_sha256);
CREATE INDEX IF NOT EXISTS idx_seed_fragility_score_policy ON strategy_pkg.seed_fragility_score(seed_policy, seed_fragile);

COMMENT ON COLUMN strategy_pkg.package.seed_policy IS 'Master Seed Contract policy for this StrategyPackage: fixed, multi_seed, random_logged, or unset_legacy; NULL only for pre-migration rows until classified.';
COMMENT ON COLUMN strategy_pkg.package.master_seed IS 'Signed 64-bit non-negative master seed used to deterministically derive Python, NumPy, Torch, LightGBM, XGBoost, CatBoost, and dataloader child seeds; NULL only for unset_legacy or unclassified historical rows.';
COMMENT ON COLUMN strategy_pkg.package.seed_sequence IS 'JSONB ordered seed list used for multi-seed stability evidence; schema is array<int64>, source is Phase 4 Master Seed Contract, empty only for unset_legacy.';
COMMENT ON COLUMN strategy_pkg.package.seed_contract IS 'JSONB normalized derived seed contract including child seeds, deterministic flags, library version evidence, hardware context, and quality semantics for reproducibility gates.';
COMMENT ON COLUMN strategy_pkg.package.seed_contract_sha256 IS 'SHA256 hash of canonical seed_contract JSONB payload for audit comparison and manifest/run parity checks.';
COMMENT ON COLUMN strategy_pkg.package.reproducibility_level IS 'Declared reproducibility level such as strict_retrain, artifact_only, or audit_only; consumers must fail fast when strict gates require stronger evidence.';
COMMENT ON COLUMN strategy_pkg.package.nondeterministic_flags IS 'JSONB array of explicit nondeterministic runtime flags or library/hardware caveats; schema is array<string> and empty means no known caveat recorded.';

COMMENT ON TABLE strategy_pkg.seed_fragility_score IS 'Per-package Phase 4 seed stability and fragility evidence derived from fixed or multi-seed validation runs without mutating frozen manifest metrics.';
COMMENT ON COLUMN strategy_pkg.seed_fragility_score.package_id IS 'StrategyPackage identifier whose seed stability evidence is summarized; references strategy_pkg.package.';
COMMENT ON COLUMN strategy_pkg.seed_fragility_score.manifest_sha256 IS 'Frozen StrategyPackage manifest SHA256 evaluated for this seed fragility summary.';
COMMENT ON COLUMN strategy_pkg.seed_fragility_score.seed_policy IS 'Seed policy evaluated for this row: fixed, multi_seed, random_logged, or unset_legacy.';
COMMENT ON COLUMN strategy_pkg.seed_fragility_score.master_seed IS 'Non-negative master seed used to derive child library seeds for this evidence row; NULL for unset_legacy evidence.';
COMMENT ON COLUMN strategy_pkg.seed_fragility_score.seed_sequence IS 'JSONB array<int64> of seeds actually evaluated for metric stability and overlap calculations.';
COMMENT ON COLUMN strategy_pkg.seed_fragility_score.metric_mean_by_seed IS 'JSONB map of metric name to mean value across seed runs; units follow each metric definition in the validation evidence.';
COMMENT ON COLUMN strategy_pkg.seed_fragility_score.metric_std_by_seed IS 'JSONB map of metric name to standard deviation across seed runs; units follow each metric definition.';
COMMENT ON COLUMN strategy_pkg.seed_fragility_score.worst_seed_metric IS 'JSONB map recording worst observed seed and metric value for each tracked metric.';
COMMENT ON COLUMN strategy_pkg.seed_fragility_score.best_seed_metric IS 'JSONB map recording best observed seed and metric value for each tracked metric.';
COMMENT ON COLUMN strategy_pkg.seed_fragility_score.seed_sensitivity_score IS 'Aggregate non-negative fragility score; larger values mean the candidate depends more strongly on seed choice.';
COMMENT ON COLUMN strategy_pkg.seed_fragility_score.rank_stability IS 'Normalized rank stability in [0,1] across seed runs where 1 means identical ranking.';
COMMENT ON COLUMN strategy_pkg.seed_fragility_score.factor_importance_stability IS 'JSONB factor-importance stability details across seed runs, including method and metric-specific quality notes.';
COMMENT ON COLUMN strategy_pkg.seed_fragility_score.selection_overlap_by_seed IS 'JSONB pairwise or per-date selected-symbol overlap by seed; used to review package fragility before Paper enablement.';
COMMENT ON COLUMN strategy_pkg.seed_fragility_score.seed_fragile IS 'Whether governance classified this package as seed-fragile and therefore requiring manual review before Paper enablement.';
COMMENT ON COLUMN strategy_pkg.seed_fragility_score.reproducibility_level IS 'Evidence strength reached by this row, for example strict_retrain, artifact_only, or audit_only.';
COMMENT ON COLUMN strategy_pkg.seed_fragility_score.nondeterministic_flags IS 'JSONB array<string> of known nondeterministic libraries, hardware paths, or runtime settings observed during validation.';
COMMENT ON COLUMN strategy_pkg.seed_fragility_score.evidence IS 'JSONB run evidence with schema_version, validation run IDs, source commands, artifact hashes, NAV/holding comparison summaries, and quality notes.';
COMMENT ON COLUMN strategy_pkg.seed_fragility_score.created_at IS 'Database timestamp when this seed fragility summary row was inserted.';

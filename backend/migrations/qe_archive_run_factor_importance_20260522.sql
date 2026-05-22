BEGIN;

CREATE TABLE IF NOT EXISTS qe_archive.run_factor_importance (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    factor_catalog_id BIGINT,
    factor_name TEXT NOT NULL,
    feature_name TEXT,
    feature_index INTEGER,
    model_family TEXT,
    model_type TEXT,
    method TEXT NOT NULL,
    method_version TEXT,
    split_name TEXT,
    time_bucket TEXT,
    epoch INTEGER,
    step INTEGER,
    importance_value DOUBLE PRECISION NOT NULL,
    normalized_value DOUBLE PRECISION,
    weight_pct DOUBLE PRECISION,
    signed_value DOUBLE PRECISION,
    rank_in_run INTEGER,
    sample_count INTEGER,
    reliability TEXT NOT NULL DEFAULT 'unknown',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE qe_archive.run_factor_importance
    ADD COLUMN IF NOT EXISTS weight_pct DOUBLE PRECISION;

CREATE INDEX IF NOT EXISTS idx_qear_importance_run
    ON qe_archive.run_factor_importance(run_id, method, split_name);

CREATE INDEX IF NOT EXISTS idx_qear_importance_factor
    ON qe_archive.run_factor_importance(factor_name, method, created_at DESC);

COMMENT ON TABLE qe_archive.run_factor_importance IS
    'Per factor or feature importance and attribution records for QE Archive runs; stores normalized percentages and artifact provenance without model blobs.';

COMMENT ON COLUMN qe_archive.run_factor_importance.id IS
    'Surrogate numeric row identifier.';
COMMENT ON COLUMN qe_archive.run_factor_importance.run_id IS
    'Stable qe_archive.run identifier; deletes cascade when an archived run is removed.';
COMMENT ON COLUMN qe_archive.run_factor_importance.factor_catalog_id IS
    'Optional AIstock factor catalog row identifier for the factor or feature.';
COMMENT ON COLUMN qe_archive.run_factor_importance.factor_name IS
    'Factor or model feature name used for attribution and cross-run comparison.';
COMMENT ON COLUMN qe_archive.run_factor_importance.feature_name IS
    'Model feature name when it differs from the canonical factor_name.';
COMMENT ON COLUMN qe_archive.run_factor_importance.feature_index IS
    'Model input feature index, if the source artifact exposes one.';
COMMENT ON COLUMN qe_archive.run_factor_importance.model_family IS
    'High-level model family reported by the source run, such as tree, linear, or deep.';
COMMENT ON COLUMN qe_archive.run_factor_importance.model_type IS
    'Concrete model type or implementation name reported by the source run.';
COMMENT ON COLUMN qe_archive.run_factor_importance.method IS
    'Importance or attribution method, such as pytorch_correlation, model_gain, or permutation_importance.';
COMMENT ON COLUMN qe_archive.run_factor_importance.method_version IS
    'Version of the importance method or extraction logic when available.';
COMMENT ON COLUMN qe_archive.run_factor_importance.split_name IS
    'Dataset split for the statistic, such as train, valid, test, or backtest.';
COMMENT ON COLUMN qe_archive.run_factor_importance.time_bucket IS
    'Optional time bucket or regime bucket used for grouped attribution.';
COMMENT ON COLUMN qe_archive.run_factor_importance.epoch IS
    'Training epoch associated with the attribution value when available.';
COMMENT ON COLUMN qe_archive.run_factor_importance.step IS
    'Training or evaluation step associated with the attribution value when available.';
COMMENT ON COLUMN qe_archive.run_factor_importance.importance_value IS
    'Raw source importance or attribution value, preserving the source method unit.';
COMMENT ON COLUMN qe_archive.run_factor_importance.normalized_value IS
    'Normalized importance fraction within the run and method, typically 0 to 1.';
COMMENT ON COLUMN qe_archive.run_factor_importance.weight_pct IS
    'Normalized factor importance percentage within the run and method, in percentage points from 0 to 100.';
COMMENT ON COLUMN qe_archive.run_factor_importance.signed_value IS
    'Signed contribution value when the method exposes directionality.';
COMMENT ON COLUMN qe_archive.run_factor_importance.rank_in_run IS
    'Rank of the factor within the run and importance method, where 1 is highest importance.';
COMMENT ON COLUMN qe_archive.run_factor_importance.sample_count IS
    'Sample count used to compute the importance statistic when reported by the source artifact.';
COMMENT ON COLUMN qe_archive.run_factor_importance.reliability IS
    'Reliability flag assigned by extraction or source metadata, such as unknown, estimated, or verified.';
COMMENT ON COLUMN qe_archive.run_factor_importance.metadata IS
    'JSONB extraction metadata including source payload path, source row index, method details, and artifact pointers; no model blob is stored here.';
COMMENT ON COLUMN qe_archive.run_factor_importance.created_at IS
    'Timestamp when this factor-importance row was written to QE Archive.';

INSERT INTO qe_archive.schema_version(version, description)
VALUES ('qe_archive_run_factor_importance_20260522', 'Add structured factor importance percentages for QE Archive loop analysis')
ON CONFLICT (version) DO NOTHING;

COMMIT;

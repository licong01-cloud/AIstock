BEGIN;

CREATE TABLE IF NOT EXISTS qe_archive.run_evaluation_metric (
    evaluation_metric_id BIGSERIAL PRIMARY KEY,
    evaluation_id TEXT NOT NULL REFERENCES qe_archive.run_evaluation(evaluation_id) ON DELETE CASCADE,
    metric_key TEXT NOT NULL,
    metric_scope TEXT NOT NULL,
    period_start DATE,
    period_end DATE,
    horizon INTEGER,
    sector_code TEXT,
    dimension_key TEXT NOT NULL,
    dimension_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    value_num DOUBLE PRECISION,
    value_text TEXT,
    value_json JSONB,
    unit TEXT,
    direction TEXT,
    source_payload_path TEXT NOT NULL,
    quality_flag TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_qear_run_evaluation_metric_dimension
        UNIQUE (evaluation_id, metric_key, dimension_key),
    CONSTRAINT ck_qear_run_evaluation_metric_scope CHECK (
        metric_scope IN ('signal_path', 'position_episode', 'portfolio_result',
                         'order_fill', 'execution_cause', 'sector_regime')
    ),
    CONSTRAINT ck_qear_run_evaluation_metric_quality CHECK (
        quality_flag IN ('ok', 'computed_with_limitations', 'insufficient_maturity',
                         'not_computable', 'not_verifiable', 'censored_only')
    ),
    CONSTRAINT ck_qear_run_evaluation_metric_horizon CHECK (
        horizon IS NULL OR horizon IN (20, 40, 60, 120, 180)
    ),
    CONSTRAINT ck_qear_run_evaluation_metric_values CHECK (
        value_text IS NULL OR (value_num IS NULL AND value_json IS NULL)
    ),
    CONSTRAINT ck_qear_run_evaluation_metric_dimension_key CHECK (
        dimension_key ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_qear_run_evaluation_metric_period CHECK (
        period_start IS NULL OR period_end IS NULL OR period_start <= period_end
    )
);

CREATE INDEX IF NOT EXISTS idx_qear_run_evaluation_metric_lookup
    ON qe_archive.run_evaluation_metric(evaluation_id, metric_scope, horizon, sector_code);

CREATE INDEX IF NOT EXISTS idx_qear_run_evaluation_metric_key_value
    ON qe_archive.run_evaluation_metric(metric_key, value_num DESC NULLS LAST);

CREATE TABLE IF NOT EXISTS qe_archive.run_evaluation_artifact (
    evaluation_artifact_id BIGSERIAL PRIMARY KEY,
    evaluation_id TEXT NOT NULL REFERENCES qe_archive.run_evaluation(evaluation_id) ON DELETE CASCADE,
    artifact_type TEXT NOT NULL,
    artifact_uri TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    schema_sha256 TEXT,
    size_bytes BIGINT,
    row_count BIGINT,
    status TEXT NOT NULL DEFAULT 'published',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_qear_run_evaluation_artifact_identity
        UNIQUE (evaluation_id, artifact_type, sha256),
    CONSTRAINT uq_qear_run_evaluation_artifact_type
        UNIQUE (evaluation_id, artifact_type),
    CONSTRAINT ck_qear_run_evaluation_artifact_hash CHECK (
        sha256 ~ '^[0-9a-f]{64}$'
        AND (schema_sha256 IS NULL OR schema_sha256 ~ '^[0-9a-f]{64}$')
    ),
    CONSTRAINT ck_qear_run_evaluation_artifact_size CHECK (
        (size_bytes IS NULL OR size_bytes >= 0)
        AND (row_count IS NULL OR row_count >= 0)
    ),
    CONSTRAINT ck_qear_run_evaluation_artifact_status CHECK (
        status IN ('staged', 'published', 'failed')
    ),
    CONSTRAINT ck_qear_run_evaluation_artifact_uri CHECK (
        artifact_uri LIKE 'aistock-qe-long-trend://evaluations/%'
    )
);

CREATE INDEX IF NOT EXISTS idx_qear_run_evaluation_artifact_lookup
    ON qe_archive.run_evaluation_artifact(evaluation_id, artifact_type, status);

CREATE INDEX IF NOT EXISTS idx_qear_run_evaluation_artifact_sha
    ON qe_archive.run_evaluation_artifact(sha256);

COMMENT ON TABLE qe_archive.run_evaluation_metric IS
    'Compact QE-only F-014 long-trend metrics. Missing evidence remains an explicit quality state and is not a research approval gate.';
COMMENT ON COLUMN qe_archive.run_evaluation_metric.evaluation_metric_id IS
    'Surrogate identifier for one compact long-trend metric row.';
COMMENT ON COLUMN qe_archive.run_evaluation_metric.evaluation_id IS
    'F-014 evaluation identity from qe_archive.run_evaluation.';
COMMENT ON COLUMN qe_archive.run_evaluation_metric.metric_key IS
    'Registered F-014 metric key; unknown keys are rejected by the receipt writer.';
COMMENT ON COLUMN qe_archive.run_evaluation_metric.metric_scope IS
    'Metric family scope such as signal_path, position_episode, portfolio_result or sector_regime.';
COMMENT ON COLUMN qe_archive.run_evaluation_metric.period_start IS
    'Optional inclusive evaluation slice start date from the immutable receipt.';
COMMENT ON COLUMN qe_archive.run_evaluation_metric.period_end IS
    'Optional inclusive evaluation slice end date from the immutable receipt.';
COMMENT ON COLUMN qe_archive.run_evaluation_metric.horizon IS
    'Optional long-trend evaluation horizon in trading days: 20, 40, 60, 120 or 180.';
COMMENT ON COLUMN qe_archive.run_evaluation_metric.sector_code IS
    'Optional signal-date PIT sector identity; null denotes a non-sector or all-market metric.';
COMMENT ON COLUMN qe_archive.run_evaluation_metric.dimension_key IS
    'SHA-256 of the versioned canonical dimension_json; generated and verified by the server-side repository.';
COMMENT ON COLUMN qe_archive.run_evaluation_metric.dimension_json IS
    'qelt_metric_dimension_v2: bounded server-generated scope, period, slice, horizon, sector and execution dimensions from the worker receipt.';
COMMENT ON COLUMN qe_archive.run_evaluation_metric.value_num IS
    'Primary sortable numeric value; it may coexist with bounded value_json diagnostics according to the registered metric schema.';
COMMENT ON COLUMN qe_archive.run_evaluation_metric.value_text IS
    'Optional enum or text value; mutually exclusive with value_num and value_json.';
COMMENT ON COLUMN qe_archive.run_evaluation_metric.value_json IS
    'Bounded registered diagnostic payload; never stores signal rows, episode rows, curves or arbitrary raw payloads.';
COMMENT ON COLUMN qe_archive.run_evaluation_metric.unit IS
    'Registered unit for value_num, for example ratio; null when the metric has no scalar unit.';
COMMENT ON COLUMN qe_archive.run_evaluation_metric.direction IS
    'Registered interpretation such as higher_better or lower_better; null for descriptive metrics.';
COMMENT ON COLUMN qe_archive.run_evaluation_metric.source_payload_path IS
    'Deterministic JSON path in qe_long_trend_worker_terminal_v1 used to reproduce the row.';
COMMENT ON COLUMN qe_archive.run_evaluation_metric.quality_flag IS
    'Evidence quality for this metric only; it does not approve, reject or eliminate a research direction.';
COMMENT ON COLUMN qe_archive.run_evaluation_metric.created_at IS
    'Database publication time; not the signal date, evaluation as-of or artifact creation time.';

COMMENT ON TABLE qe_archive.run_evaluation_artifact IS
    'Published manifest rows for the dedicated QE-only F-014 content-addressed artifact store.';
COMMENT ON COLUMN qe_archive.run_evaluation_artifact.evaluation_artifact_id IS
    'Surrogate identifier for one published F-014 artifact metadata row.';
COMMENT ON COLUMN qe_archive.run_evaluation_artifact.evaluation_id IS
    'F-014 evaluation identity from qe_archive.run_evaluation.';
COMMENT ON COLUMN qe_archive.run_evaluation_artifact.artifact_type IS
    'Allowlisted F-014 artifact type; one immutable type is allowed per evaluation.';
COMMENT ON COLUMN qe_archive.run_evaluation_artifact.artifact_uri IS
    'Allowlisted aistock-qe-long-trend URI verified against the immutable Phase 2 manifest before publication.';
COMMENT ON COLUMN qe_archive.run_evaluation_artifact.sha256 IS
    'SHA-256 of the immutable artifact bytes or canonical manifest content.';
COMMENT ON COLUMN qe_archive.run_evaluation_artifact.schema_sha256 IS
    'SHA-256 of the registered JSON schema identity or Parquet column schema; null only when unavailable by contract.';
COMMENT ON COLUMN qe_archive.run_evaluation_artifact.size_bytes IS
    'Non-negative artifact byte length from the immutable manifest; nullable for the canonical manifest envelope.';
COMMENT ON COLUMN qe_archive.run_evaluation_artifact.row_count IS
    'Non-negative Parquet row count when applicable; null for JSON receipts and manifest metadata.';
COMMENT ON COLUMN qe_archive.run_evaluation_artifact.metadata IS
    'qelt_evaluation_artifact_metadata_v1: bounded metadata sourced from the immutable Phase 2 manifest, with schema/row/column/quality evidence and no secrets or file-system paths.';
COMMENT ON COLUMN qe_archive.run_evaluation_artifact.status IS
    'Artifact publication lifecycle only; it has no research admission or promotion meaning.';
COMMENT ON COLUMN qe_archive.run_evaluation_artifact.created_at IS
    'Database publication time for the artifact metadata row.';

INSERT INTO qe_archive.schema_version(version, description)
VALUES (
    'qe_archive_v5_20260728',
    'F-014 long-trend compact metric/artifact persistence and bounded QE-only query contracts'
)
ON CONFLICT (version) DO NOTHING;

COMMIT;

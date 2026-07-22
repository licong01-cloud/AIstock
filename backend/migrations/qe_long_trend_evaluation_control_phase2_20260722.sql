BEGIN;

CREATE SCHEMA IF NOT EXISTS qe_archive;

CREATE TABLE IF NOT EXISTS qe_archive.run_evaluation (
    evaluation_id TEXT PRIMARY KEY,
    run_id TEXT REFERENCES qe_archive.run(run_id) ON DELETE CASCADE,
    parent_task_id TEXT NOT NULL,
    parent_loop_index INTEGER NOT NULL,
    evaluation_type TEXT NOT NULL DEFAULT 'long_trend',
    profile_id TEXT NOT NULL,
    profile_sha256 TEXT NOT NULL,
    evaluator_version TEXT NOT NULL,
    evaluator_source_sha256 TEXT NOT NULL,
    execution_environment_snapshot_id TEXT NOT NULL,
    execution_environment_manifest_sha256 TEXT NOT NULL,
    bundle_sha256 TEXT NOT NULL,
    qe_dataset_contract_id TEXT NOT NULL,
    feature_dataset_snapshot_id TEXT,
    feature_dataset_manifest_sha256 TEXT,
    outcome_dataset_snapshot_id TEXT,
    outcome_dataset_manifest_sha256 TEXT,
    input_manifest_sha256 TEXT NOT NULL,
    node_id TEXT NOT NULL,
    job_id TEXT,
    request_sha TEXT NOT NULL,
    request_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    current_attempt_id TEXT,
    resource_session_id TEXT,
    worker_terminal_sha256 TEXT,
    artifact_store_run_key TEXT,
    artifact_manifest_sha256 TEXT,
    status TEXT NOT NULL DEFAULT 'queued',
    family_status_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    platform_delivery_status_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    data_action_plan_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    reason_code TEXT,
    reason_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    stats_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    owner_id TEXT,
    fencing_token BIGINT NOT NULL DEFAULT 0,
    lease_expires_at TIMESTAMPTZ,
    row_version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_qear_run_evaluation_type CHECK (evaluation_type = 'long_trend'),
    CONSTRAINT ck_qear_run_evaluation_status CHECK (
        status IN ('queued', 'submitting', 'submitted', 'running', 'collecting',
                   'succeeded', 'partial', 'failed', 'cancelled', 'remote_state_unknown')
    ),
    CONSTRAINT ck_qear_run_evaluation_identity CHECK (
        evaluation_id ~ '^qelt_[0-9a-f]{64}$'
        AND profile_sha256 ~ '^[0-9a-f]{64}$'
        AND evaluator_source_sha256 ~ '^[0-9a-f]{64}$'
        AND execution_environment_manifest_sha256 ~ '^[0-9a-f]{64}$'
        AND bundle_sha256 ~ '^[0-9a-f]{64}$'
        AND input_manifest_sha256 ~ '^[0-9a-f]{64}$'
        AND request_sha ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_qear_run_evaluation_versions CHECK (
        fencing_token >= 0 AND row_version >= 1 AND parent_loop_index >= 1
    ),
    CONSTRAINT ck_qear_run_evaluation_lease CHECK (
        lease_expires_at IS NULL OR owner_id IS NOT NULL
    ),
    CONSTRAINT ck_qear_run_evaluation_terminal_time CHECK (
        status NOT IN ('succeeded', 'partial', 'failed', 'cancelled') OR completed_at IS NOT NULL
    ),
    CONSTRAINT uq_qear_run_evaluation_identity UNIQUE (
        parent_task_id, parent_loop_index, evaluation_type, profile_sha256, input_manifest_sha256,
        evaluator_source_sha256, execution_environment_manifest_sha256
    )
);

CREATE INDEX IF NOT EXISTS idx_qear_run_evaluation_recovery
    ON qe_archive.run_evaluation(status, lease_expires_at, created_at, evaluation_id)
    WHERE status NOT IN ('succeeded', 'partial', 'failed', 'cancelled');

CREATE INDEX IF NOT EXISTS idx_qear_run_evaluation_parent
    ON qe_archive.run_evaluation(parent_task_id, parent_loop_index, created_at DESC);

COMMENT ON TABLE qe_archive.run_evaluation IS
    'QE-only F-014 long-trend evaluation lifecycle control. Research evidence is not an approval gate.';
COMMENT ON COLUMN qe_archive.run_evaluation.status IS
    'Platform task lifecycle only; it does not approve, reject, or eliminate a research direction.';
COMMENT ON COLUMN qe_archive.run_evaluation.owner_id IS
    'Current AIstock reconciliation owner. Every mutation is fenced by owner, fencing_token, and row_version.';
COMMENT ON COLUMN qe_archive.run_evaluation.worker_terminal_sha256 IS
    'Immutable hash of the node worker terminal receipt; separate from the CAS published manifest.';
COMMENT ON COLUMN qe_archive.run_evaluation.run_id IS
    'Bound once qe.loop.completed archival creates qe_archive.run; normal registration is durably parented by task/Loop before that event.';
COMMENT ON COLUMN qe_archive.run_evaluation.request_json IS
    'Canonical secret-free node request used only for exact submit/reconcile recovery; the resource token is stored separately.';

COMMIT;

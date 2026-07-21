-- QE Archive v2 recovery snapshots for multi-alpha P0-2.
--
-- Additive and idempotent. Apply manually only after the sibling preflight in
-- DEV/production under the separately authorized DDL workflow. This migration
-- never creates an export and application code must not auto-apply it.

BEGIN;

DO $archive_p0_2_base$
BEGIN
    IF to_regclass('qe_archive.run') IS NULL
       OR to_regclass('qe_archive.multi_alpha_run') IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'qe_archive_multi_alpha_p0_2_base_schema_missing',
            DETAIL = 'Apply and verify qe_archive_multi_alpha_phase_a_20260628.sql first.';
    END IF;
END
$archive_p0_2_base$;

ALTER TABLE qe_archive.run
    DROP CONSTRAINT IF EXISTS ck_qear_run_status;
ALTER TABLE qe_archive.run
    ADD CONSTRAINT ck_qear_run_status CHECK (
        status IN (
            'pending', 'running', 'completed', 'failed', 'interrupted',
            'partial_archived', 'archived', 'succeeded', 'partial_failed',
            'cancelled', 'partial_recovered'
        )
    );

ALTER TABLE qe_archive.multi_alpha_run
    ADD COLUMN IF NOT EXISTS archive_schema_version TEXT NOT NULL DEFAULT 'v1',
    ADD COLUMN IF NOT EXISTS retry_of_run_id TEXT,
    ADD COLUMN IF NOT EXISTS recovery_kind TEXT,
    ADD COLUMN IF NOT EXISTS recovery_scope_json JSONB,
    ADD COLUMN IF NOT EXISTS recovery_scope_hash TEXT,
    ADD COLUMN IF NOT EXISTS execution_identity_json JSONB,
    ADD COLUMN IF NOT EXISTS execution_identity_hash TEXT,
    ADD COLUMN IF NOT EXISTS execution_identity_evidence_json JSONB;

ALTER TABLE qe_archive.multi_alpha_run
    DROP CONSTRAINT IF EXISTS ck_qear_macb_run_status;
ALTER TABLE qe_archive.multi_alpha_run
    ADD CONSTRAINT ck_qear_macb_run_status CHECK (
        status IN ('succeeded', 'partial_failed', 'partial_recovered', 'failed', 'cancelled')
    );
ALTER TABLE qe_archive.multi_alpha_run
    DROP CONSTRAINT IF EXISTS ck_qear_macb_run_logical_status;
ALTER TABLE qe_archive.multi_alpha_run
    ADD CONSTRAINT ck_qear_macb_run_logical_status CHECK (
        logical_status IS NULL
        OR logical_status IN ('succeeded', 'partial_failed', 'partial_recovered', 'failed', 'cancelled')
    );
ALTER TABLE qe_archive.multi_alpha_run
    DROP CONSTRAINT IF EXISTS ck_qear_macb_archive_schema_version;
ALTER TABLE qe_archive.multi_alpha_run
    ADD CONSTRAINT ck_qear_macb_archive_schema_version CHECK (
        archive_schema_version IN ('v1', 'v2')
    );
ALTER TABLE qe_archive.multi_alpha_run
    DROP CONSTRAINT IF EXISTS ck_qear_macb_recovery_kind;
ALTER TABLE qe_archive.multi_alpha_run
    ADD CONSTRAINT ck_qear_macb_recovery_kind CHECK (
        recovery_kind IS NULL OR recovery_kind = 'child_targeted'
    );
ALTER TABLE qe_archive.multi_alpha_run
    DROP CONSTRAINT IF EXISTS ck_qear_macb_recovery_scope;
ALTER TABLE qe_archive.multi_alpha_run
    ADD CONSTRAINT ck_qear_macb_recovery_scope CHECK (
        (recovery_scope_json IS NULL AND recovery_scope_hash IS NULL)
        OR (jsonb_typeof(recovery_scope_json) = 'object'
            AND recovery_scope_hash ~ '^[0-9a-f]{64}$')
    );
ALTER TABLE qe_archive.multi_alpha_run
    DROP CONSTRAINT IF EXISTS ck_qear_macb_execution_identity;
ALTER TABLE qe_archive.multi_alpha_run
    ADD CONSTRAINT ck_qear_macb_execution_identity CHECK (
        (execution_identity_json IS NULL AND execution_identity_hash IS NULL)
        OR (jsonb_typeof(execution_identity_json) = 'object'
            AND execution_identity_hash ~ '^[0-9a-f]{64}$')
    );
ALTER TABLE qe_archive.multi_alpha_run
    DROP CONSTRAINT IF EXISTS ck_qear_macb_execution_identity_evidence;
ALTER TABLE qe_archive.multi_alpha_run
    ADD CONSTRAINT ck_qear_macb_execution_identity_evidence CHECK (
        execution_identity_evidence_json IS NULL
        OR (jsonb_typeof(execution_identity_evidence_json) = 'object'
            AND jsonb_typeof(execution_identity_evidence_json->'complete') = 'boolean')
    );
ALTER TABLE qe_archive.multi_alpha_run
    DROP CONSTRAINT IF EXISTS ck_qear_macb_v2_recovery_tuple;
ALTER TABLE qe_archive.multi_alpha_run
    ADD CONSTRAINT ck_qear_macb_v2_recovery_tuple CHECK (
        archive_schema_version = 'v1'
        OR (
            archive_schema_version = 'v2'
            AND (
                (execution_identity_json IS NOT NULL
                 AND execution_identity_hash IS NOT NULL
                 AND execution_identity_evidence_json IS NOT NULL
                 AND execution_identity_evidence_json->>'complete' = 'true')
                OR
                (execution_identity_json IS NULL
                 AND execution_identity_hash IS NULL
                 AND execution_identity_evidence_json IS NOT NULL
                 AND execution_identity_evidence_json->>'complete' = 'false')
            )
            AND (
                (recovery_kind IS NULL AND retry_of_run_id IS NULL
                 AND recovery_scope_json IS NULL AND recovery_scope_hash IS NULL)
                OR
                (recovery_kind = 'child_targeted' AND retry_of_run_id IS NOT NULL
                 AND recovery_scope_json IS NOT NULL AND recovery_scope_hash IS NOT NULL)
            )
        )
    );
ALTER TABLE qe_archive.multi_alpha_run
    DROP CONSTRAINT IF EXISTS ck_qear_macb_partial_recovered_kind;
ALTER TABLE qe_archive.multi_alpha_run
    ADD CONSTRAINT ck_qear_macb_partial_recovered_kind CHECK (
        status <> 'partial_recovered' OR recovery_kind = 'child_targeted'
    );

CREATE INDEX IF NOT EXISTS idx_qear_macb_run_recovery_source
    ON qe_archive.multi_alpha_run(retry_of_run_id, archived_at DESC)
    WHERE recovery_kind = 'child_targeted';

CREATE TABLE IF NOT EXISTS qe_archive.multi_alpha_recovery_child (
    run_id TEXT NOT NULL REFERENCES qe_archive.multi_alpha_run(run_id) ON DELETE CASCADE,
    child_id TEXT NOT NULL,
    child_key TEXT NOT NULL,
    child_kind TEXT NOT NULL,
    status TEXT NOT NULL,
    execution_disposition TEXT NOT NULL,
    selected_attempt_id TEXT,
    source_child_id TEXT,
    source_lineage_json JSONB,
    source_lineage_hash TEXT,
    input_manifest_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    input_manifest_hash TEXT NOT NULL,
    prediction_artifact_uri TEXT,
    prediction_artifact_hash TEXT,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, child_id),
    CONSTRAINT ck_qear_macb_recovery_child_kind CHECK (child_kind IN ('baseline', 'scheme', 'loo')),
    CONSTRAINT ck_qear_macb_recovery_child_status CHECK (
        status IN ('pending', 'materializing', 'queued', 'running', 'reconciling',
                   'cancel_requested', 'cancelling', 'succeeded', 'not_computable',
                   'not_recovered', 'failed', 'cancelled')
    ),
    CONSTRAINT ck_qear_macb_recovery_child_disposition CHECK (
        execution_disposition IN ('execute', 'reuse_result', 'recompute_derived', 'preserve_unavailable')
    ),
    CONSTRAINT ck_qear_macb_recovery_child_lineage CHECK (
        (source_lineage_json IS NULL AND source_lineage_hash IS NULL)
        OR (jsonb_typeof(source_lineage_json) = 'object'
            AND source_lineage_hash ~ '^[0-9a-f]{64}$')
    ),
    CONSTRAINT ck_qear_macb_recovery_child_input_manifest CHECK (
        jsonb_typeof(input_manifest_json) = 'object'
    ),
    CONSTRAINT ck_qear_macb_recovery_child_input_hash CHECK (
        input_manifest_hash ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_qear_macb_recovery_child_prediction_hash CHECK (
        prediction_artifact_hash IS NULL OR prediction_artifact_hash ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_qear_macb_recovery_child_not_recovered CHECK (
        (status = 'not_recovered' AND execution_disposition = 'preserve_unavailable'
         AND selected_attempt_id IS NULL)
        OR status <> 'not_recovered'
    )
);

CREATE INDEX IF NOT EXISTS idx_qear_macb_recovery_child_source
    ON qe_archive.multi_alpha_recovery_child(source_child_id)
    WHERE source_child_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_qear_macb_recovery_child_status
    ON qe_archive.multi_alpha_recovery_child(run_id, status, execution_disposition);

CREATE TABLE IF NOT EXISTS qe_archive.multi_alpha_recovery_attempt (
    run_id TEXT NOT NULL,
    child_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    attempt_no INTEGER NOT NULL,
    retry_mode TEXT NOT NULL,
    execution_kind TEXT NOT NULL,
    status TEXT NOT NULL,
    source_attempt_id TEXT,
    artifact_manifest_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_manifest_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_manifest_hash TEXT,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, attempt_id),
    CONSTRAINT uq_qear_macb_recovery_attempt_child_no UNIQUE (run_id, child_id, attempt_no),
    CONSTRAINT uq_qear_macb_recovery_attempt_child_id UNIQUE (run_id, child_id, attempt_id),
    CONSTRAINT fk_qear_macb_recovery_attempt_child
        FOREIGN KEY (run_id, child_id)
        REFERENCES qe_archive.multi_alpha_recovery_child(run_id, child_id)
        ON DELETE CASCADE,
    CONSTRAINT ck_qear_macb_recovery_attempt_no CHECK (attempt_no >= 1),
    CONSTRAINT ck_qear_macb_recovery_attempt_mode CHECK (
        retry_mode IN ('initial', 'backtest_only', 'results_only', 'rematerialize_and_backtest')
    ),
    CONSTRAINT ck_qear_macb_recovery_attempt_kind CHECK (
        execution_kind IN ('remote_execution', 'reference_result', 'derived_result')
    ),
    CONSTRAINT ck_qear_macb_recovery_attempt_status CHECK (
        status IN ('queued', 'submitting', 'running', 'reconciling', 'succeeded', 'failed', 'cancelled')
    ),
    CONSTRAINT ck_qear_macb_recovery_attempt_artifact_json CHECK (
        jsonb_typeof(artifact_manifest_json) = 'object'
    ),
    CONSTRAINT ck_qear_macb_recovery_attempt_result_json CHECK (
        jsonb_typeof(result_manifest_json) = 'object'
    ),
    CONSTRAINT ck_qear_macb_recovery_attempt_result_hash CHECK (
        result_manifest_hash IS NULL OR result_manifest_hash ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_qear_macb_recovery_attempt_reference CHECK (
        execution_kind = 'remote_execution'
        OR (retry_mode = 'results_only' AND status = 'succeeded'
            AND source_attempt_id IS NOT NULL AND result_manifest_hash IS NOT NULL)
    )
);

ALTER TABLE qe_archive.multi_alpha_recovery_child
    DROP CONSTRAINT IF EXISTS fk_qear_macb_recovery_child_selected_attempt;
ALTER TABLE qe_archive.multi_alpha_recovery_child
    ADD CONSTRAINT fk_qear_macb_recovery_child_selected_attempt
    FOREIGN KEY (run_id, child_id, selected_attempt_id)
    REFERENCES qe_archive.multi_alpha_recovery_attempt(run_id, child_id, attempt_id)
    ON DELETE SET NULL (selected_attempt_id)
    DEFERRABLE INITIALLY DEFERRED;

CREATE INDEX IF NOT EXISTS idx_qear_macb_recovery_attempt_source
    ON qe_archive.multi_alpha_recovery_attempt(source_attempt_id)
    WHERE source_attempt_id IS NOT NULL;

COMMENT ON TABLE qe_archive.multi_alpha_recovery_child IS
    'P0-2 immutable archived child snapshot for multi-alpha recovery; preserves unavailable evidence without research approval semantics.';
COMMENT ON TABLE qe_archive.multi_alpha_recovery_attempt IS
    'P0-2 immutable archived actual child-attempt snapshot; reference and derived results are explicit non-remote execution kinds.';
COMMENT ON COLUMN qe_archive.multi_alpha_run.archive_schema_version IS
    'Archive payload/readback schema version. v1 remains readable; new P0-2 durable runs are v2.';
COMMENT ON COLUMN qe_archive.multi_alpha_run.execution_identity_json IS
    'Canonical content identity of dataset, prediction sources, runtime, materializer, and business formula; paths alone are not evidence.';

COMMIT;

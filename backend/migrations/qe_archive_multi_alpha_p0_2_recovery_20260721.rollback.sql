-- Guarded destructive rollback for qe_archive_multi_alpha_p0_2_recovery_20260721.sql.
--
-- Normal code rollback retains additive Archive v2 schema. Execute this only
-- when no v2 event/snapshot/new terminal status has ever been archived.

BEGIN;

DO $qear_macb_p0_2_rollback$
BEGIN
    IF to_regclass('qe_archive.multi_alpha_recovery_child') IS NULL
       OR to_regclass('qe_archive.multi_alpha_recovery_attempt') IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'qe_archive_multi_alpha_p0_2_rollback_schema_missing',
            DETAIL = 'Archive v2 is incomplete; repair schema explicitly instead of destructive rollback.';
    END IF;

    IF EXISTS (SELECT 1 FROM qe_archive.multi_alpha_recovery_child)
       OR EXISTS (SELECT 1 FROM qe_archive.multi_alpha_recovery_attempt)
       OR EXISTS (
            SELECT 1
            FROM qe_archive.multi_alpha_run
            WHERE archive_schema_version = 'v2'
               OR status IN ('cancelled', 'partial_recovered')
               OR logical_status IN ('cancelled', 'partial_recovered')
       ) OR EXISTS (
            SELECT 1
            FROM qe_archive.run
            WHERE status IN ('cancelled', 'partial_recovered')
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'qe_archive_multi_alpha_p0_2_rollback_v2_data_present',
            DETAIL = 'Keep additive Archive v2 schema; immutable recovery evidence must not be deleted.';
    END IF;
END
$qear_macb_p0_2_rollback$;

DROP TABLE qe_archive.multi_alpha_recovery_attempt;
DROP TABLE qe_archive.multi_alpha_recovery_child;
DROP INDEX IF EXISTS qe_archive.idx_qear_macb_run_recovery_source;

ALTER TABLE qe_archive.multi_alpha_run
    DROP CONSTRAINT IF EXISTS ck_qear_macb_partial_recovered_kind,
    DROP CONSTRAINT IF EXISTS ck_qear_macb_v2_recovery_tuple,
    DROP CONSTRAINT IF EXISTS ck_qear_macb_execution_identity_evidence,
    DROP CONSTRAINT IF EXISTS ck_qear_macb_execution_identity,
    DROP CONSTRAINT IF EXISTS ck_qear_macb_recovery_scope,
    DROP CONSTRAINT IF EXISTS ck_qear_macb_recovery_kind,
    DROP CONSTRAINT IF EXISTS ck_qear_macb_archive_schema_version,
    DROP CONSTRAINT IF EXISTS ck_qear_macb_run_logical_status,
    DROP CONSTRAINT IF EXISTS ck_qear_macb_run_status;

ALTER TABLE qe_archive.multi_alpha_run
    ADD CONSTRAINT ck_qear_macb_run_status CHECK (status IN ('succeeded', 'partial_failed', 'failed')),
    ADD CONSTRAINT ck_qear_macb_run_logical_status CHECK (
        logical_status IS NULL OR logical_status IN ('succeeded', 'partial_failed', 'failed')
    );

ALTER TABLE qe_archive.multi_alpha_run
    DROP COLUMN IF EXISTS archive_schema_version,
    DROP COLUMN IF EXISTS retry_of_run_id,
    DROP COLUMN IF EXISTS recovery_kind,
    DROP COLUMN IF EXISTS recovery_scope_json,
    DROP COLUMN IF EXISTS recovery_scope_hash,
    DROP COLUMN IF EXISTS execution_identity_json,
    DROP COLUMN IF EXISTS execution_identity_hash,
    DROP COLUMN IF EXISTS execution_identity_evidence_json;

ALTER TABLE qe_archive.run
    DROP CONSTRAINT IF EXISTS ck_qear_run_status;
ALTER TABLE qe_archive.run
    ADD CONSTRAINT ck_qear_run_status CHECK (
        status IN (
            'pending', 'running', 'completed', 'failed', 'interrupted',
            'partial_archived', 'archived', 'succeeded', 'partial_failed'
        )
    );

COMMIT;

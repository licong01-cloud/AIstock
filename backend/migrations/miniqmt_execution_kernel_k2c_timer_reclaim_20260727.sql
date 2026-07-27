-- MiniQMT K2-C timer reclaim compatibility migration.
BEGIN;

DO $$
DECLARE
    constraint_definition TEXT;
BEGIN
    SELECT pg_get_constraintdef(constraint_record.oid, true)
    INTO constraint_definition
    FROM pg_constraint AS constraint_record
    WHERE constraint_record.conrelid='qmt_strategy.execution_algo_timer_occurrence'::regclass
      AND constraint_record.conname='ck_miniqmt_k2_timer_occurrence_initial'
    FOR UPDATE;
    IF constraint_definition IS NULL THEN
        RAISE EXCEPTION 'K2-C forward: timer occurrence initial constraint is missing';
    END IF;
    IF constraint_definition = 'CHECK (status <> ''CLAIMED''::text OR lease_epoch = 1 AND row_version = 1 AND emitted_event_id IS NULL AND catch_up_receipt_sha256 IS NULL AND lease_owner IS NOT NULL AND lease_worker_id IS NOT NULL AND lease_process_incarnation_id IS NOT NULL AND lease_fence_token IS NOT NULL AND lease_expires_at_utc IS NOT NULL AND closed_at_utc IS NULL)' THEN
        ALTER TABLE qmt_strategy.execution_algo_timer_occurrence
            DROP CONSTRAINT ck_miniqmt_k2_timer_occurrence_initial;
        ALTER TABLE qmt_strategy.execution_algo_timer_occurrence
            ADD CONSTRAINT ck_miniqmt_k2_timer_occurrence_initial CHECK (
                status <> 'CLAIMED' OR (
                    lease_epoch >= 1 AND row_version = lease_epoch AND emitted_event_id IS NULL
                    AND catch_up_receipt_sha256 IS NULL AND lease_owner IS NOT NULL
                    AND lease_worker_id IS NOT NULL AND lease_process_incarnation_id IS NOT NULL
                    AND lease_fence_token IS NOT NULL AND lease_expires_at_utc IS NOT NULL
                    AND closed_at_utc IS NULL
                )
            ) NOT VALID;
        ALTER TABLE qmt_strategy.execution_algo_timer_occurrence
            VALIDATE CONSTRAINT ck_miniqmt_k2_timer_occurrence_initial;
    ELSIF constraint_definition <> 'CHECK (status <> ''CLAIMED''::text OR lease_epoch >= 1 AND row_version = lease_epoch AND emitted_event_id IS NULL AND catch_up_receipt_sha256 IS NULL AND lease_owner IS NOT NULL AND lease_worker_id IS NOT NULL AND lease_process_incarnation_id IS NOT NULL AND lease_fence_token IS NOT NULL AND lease_expires_at_utc IS NOT NULL AND closed_at_utc IS NULL)' THEN
        RAISE EXCEPTION 'K2-C forward: timer occurrence constraint drift: %', constraint_definition;
    END IF;
END $$;

COMMENT ON CONSTRAINT ck_miniqmt_k2_timer_occurrence_initial
ON qmt_strategy.execution_algo_timer_occurrence
IS 'K2-C CLAIMED occurrence starts at epoch/version 1 and each stale reclaim advances both exactly once.';

DO $$
DECLARE
    constraint_definition TEXT;
    actual_catalog_sha256 TEXT;
BEGIN
    SELECT pg_get_constraintdef(constraint_record.oid, true)
    INTO constraint_definition
    FROM pg_constraint AS constraint_record
    WHERE constraint_record.conrelid='qmt_strategy.execution_algo_timer_occurrence'::regclass
      AND constraint_record.conname='ck_miniqmt_k2_timer_occurrence_initial';
    IF constraint_definition <> 'CHECK (status <> ''CLAIMED''::text OR lease_epoch >= 1 AND row_version = lease_epoch AND emitted_event_id IS NULL AND catch_up_receipt_sha256 IS NULL AND lease_owner IS NOT NULL AND lease_worker_id IS NOT NULL AND lease_process_incarnation_id IS NOT NULL AND lease_fence_token IS NOT NULL AND lease_expires_at_utc IS NOT NULL AND closed_at_utc IS NULL)' THEN
        RAISE EXCEPTION 'K2-C post-commit readback drift: %', constraint_definition;
    END IF;
    SELECT qmt_strategy.miniqmt_k2_catalog_fingerprint() INTO actual_catalog_sha256;
    IF actual_catalog_sha256 <> '4c613f119a828c7ce3d1a9bac92113b803c93455802fb5b71b8a7ca2ac2743a5' THEN
        RAISE EXCEPTION 'K2-C schema catalog drift: expected 4c613f119a828c7ce3d1a9bac92113b803c93455802fb5b71b8a7ca2ac2743a5, got %', actual_catalog_sha256;
    END IF;
END $$;

COMMIT;

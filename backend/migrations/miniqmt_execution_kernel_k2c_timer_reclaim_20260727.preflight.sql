-- MiniQMT K2-C timer reclaim compatibility preflight (read-only).
BEGIN;
SET TRANSACTION READ ONLY;

DO $$
DECLARE
    constraint_definition TEXT;
    legacy_invalid_row_count BIGINT;
    actual_catalog_sha256 TEXT;
    expected_catalog_sha256 TEXT;
BEGIN
    IF to_regclass('qmt_strategy.execution_algo_timer_occurrence') IS NULL THEN
        RAISE EXCEPTION 'K2-C preflight: execution_algo_timer_occurrence is missing';
    END IF;
    SELECT pg_get_constraintdef(constraint_record.oid, true)
    INTO constraint_definition
    FROM pg_constraint AS constraint_record
    WHERE constraint_record.conrelid='qmt_strategy.execution_algo_timer_occurrence'::regclass
      AND constraint_record.conname='ck_miniqmt_k2_timer_occurrence_initial';
    IF constraint_definition IS NULL THEN
        RAISE EXCEPTION 'K2-C preflight: timer occurrence initial constraint is missing';
    END IF;
    IF constraint_definition NOT IN (
        'CHECK (status <> ''CLAIMED''::text OR lease_epoch = 1 AND row_version = 1 AND emitted_event_id IS NULL AND catch_up_receipt_sha256 IS NULL AND lease_owner IS NOT NULL AND lease_worker_id IS NOT NULL AND lease_process_incarnation_id IS NOT NULL AND lease_fence_token IS NOT NULL AND lease_expires_at_utc IS NOT NULL AND closed_at_utc IS NULL)',
        'CHECK (status <> ''CLAIMED''::text OR lease_epoch >= 1 AND row_version = lease_epoch AND emitted_event_id IS NULL AND catch_up_receipt_sha256 IS NULL AND lease_owner IS NOT NULL AND lease_worker_id IS NOT NULL AND lease_process_incarnation_id IS NOT NULL AND lease_fence_token IS NOT NULL AND lease_expires_at_utc IS NOT NULL AND closed_at_utc IS NULL)'
    ) THEN
        RAISE EXCEPTION 'K2-C preflight: timer occurrence initial/reclaim constraint drift: %', constraint_definition;
    END IF;
    expected_catalog_sha256 := CASE
        WHEN constraint_definition LIKE '%row_version = lease_epoch%'
            THEN '4c613f119a828c7ce3d1a9bac92113b803c93455802fb5b71b8a7ca2ac2743a5'
        ELSE '6e4fc4ae4c6e403d3316c124da6ae5933eb33184129569fd6bf1cf750e27f762'
    END;
    SELECT qmt_strategy.miniqmt_k2_catalog_fingerprint() INTO actual_catalog_sha256;
    IF actual_catalog_sha256 <> expected_catalog_sha256 THEN
        RAISE EXCEPTION 'K2-C preflight: exact schema catalog drift: expected %, got %',
            expected_catalog_sha256, actual_catalog_sha256;
    END IF;
    SELECT count(*) INTO legacy_invalid_row_count
    FROM qmt_strategy.execution_algo_timer_occurrence
    WHERE status='CLAIMED' AND NOT (
        lease_epoch >= 1 AND row_version = lease_epoch
        AND emitted_event_id IS NULL AND catch_up_receipt_sha256 IS NULL
        AND lease_owner IS NOT NULL AND lease_worker_id IS NOT NULL
        AND lease_process_incarnation_id IS NOT NULL AND lease_fence_token IS NOT NULL
        AND lease_expires_at_utc IS NOT NULL AND closed_at_utc IS NULL
    );
    IF legacy_invalid_row_count <> 0 THEN
        RAISE EXCEPTION 'K2-C preflight: legacy_invalid_row_count=%', legacy_invalid_row_count;
    END IF;
END $$;

SELECT
    'runner_must_verify_committed_forward_sha256' AS verification,
    '3552277b61c4035924bb787396565101a1403774a0c2c72ba5d8356965d3ec50'::TEXT AS expected_migration_sha256;

ROLLBACK;

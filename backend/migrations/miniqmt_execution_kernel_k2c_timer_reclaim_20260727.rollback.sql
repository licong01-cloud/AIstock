-- MiniQMT K2-C timer reclaim compatibility guarded rollback.
BEGIN;

DO $$
DECLARE
    reclaimed_claim_count BIGINT;
    constraint_definition TEXT;
BEGIN
    SELECT count(*) INTO reclaimed_claim_count
    FROM qmt_strategy.execution_algo_timer_occurrence
    WHERE status='CLAIMED' AND (lease_epoch <> 1 OR row_version <> 1);
    IF reclaimed_claim_count <> 0 THEN
        RAISE EXCEPTION 'K2-C destructive rollback refused: reclaimed_claim_count=%', reclaimed_claim_count;
    END IF;
    SELECT pg_get_constraintdef(constraint_record.oid, true)
    INTO constraint_definition
    FROM pg_constraint AS constraint_record
    WHERE constraint_record.conrelid='qmt_strategy.execution_algo_timer_occurrence'::regclass
      AND constraint_record.conname='ck_miniqmt_k2_timer_occurrence_initial'
    FOR UPDATE;
    IF constraint_definition IS NULL THEN
        RAISE EXCEPTION 'K2-C rollback: timer occurrence constraint is missing';
    END IF;
    IF constraint_definition = 'CHECK (status <> ''CLAIMED''::text OR lease_epoch >= 1 AND row_version = lease_epoch AND emitted_event_id IS NULL AND catch_up_receipt_sha256 IS NULL AND lease_owner IS NOT NULL AND lease_worker_id IS NOT NULL AND lease_process_incarnation_id IS NOT NULL AND lease_fence_token IS NOT NULL AND lease_expires_at_utc IS NOT NULL AND closed_at_utc IS NULL)' THEN
        ALTER TABLE qmt_strategy.execution_algo_timer_occurrence
            DROP CONSTRAINT ck_miniqmt_k2_timer_occurrence_initial;
        ALTER TABLE qmt_strategy.execution_algo_timer_occurrence
            ADD CONSTRAINT ck_miniqmt_k2_timer_occurrence_initial CHECK (
                status <> 'CLAIMED' OR (
                    lease_epoch=1 AND row_version=1 AND emitted_event_id IS NULL
                    AND catch_up_receipt_sha256 IS NULL AND lease_owner IS NOT NULL
                    AND lease_worker_id IS NOT NULL AND lease_process_incarnation_id IS NOT NULL
                    AND lease_fence_token IS NOT NULL AND lease_expires_at_utc IS NOT NULL
                    AND closed_at_utc IS NULL
                )
            ) NOT VALID;
        ALTER TABLE qmt_strategy.execution_algo_timer_occurrence
            VALIDATE CONSTRAINT ck_miniqmt_k2_timer_occurrence_initial;
    ELSIF constraint_definition <> 'CHECK (status <> ''CLAIMED''::text OR lease_epoch = 1 AND row_version = 1 AND emitted_event_id IS NULL AND catch_up_receipt_sha256 IS NULL AND lease_owner IS NOT NULL AND lease_worker_id IS NOT NULL AND lease_process_incarnation_id IS NOT NULL AND lease_fence_token IS NOT NULL AND lease_expires_at_utc IS NOT NULL AND closed_at_utc IS NULL)' THEN
        RAISE EXCEPTION 'K2-C rollback constraint drift: %', constraint_definition;
    END IF;
END $$;

COMMIT;


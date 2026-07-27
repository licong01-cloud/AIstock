-- MiniQMT K2-D append-only reconciliation history preflight (read-only).
BEGIN;
SET TRANSACTION READ ONLY;

DO $$
DECLARE
    existing_rows BIGINT;
    actual_catalog_sha256 TEXT;
BEGIN
    IF to_regclass('qmt_strategy.execution_algo_command_outbox') IS NULL THEN
        RAISE EXCEPTION 'K2-D preflight: execution_algo_command_outbox is missing';
    END IF;
    IF to_regclass('qmt_strategy.execution_runtime') IS NULL THEN
        RAISE EXCEPTION 'K2-D preflight: execution_runtime is missing';
    END IF;
    IF to_regclass('qmt_strategy.execution_broker_reconciliation_attempt') IS NOT NULL THEN
        SELECT count(*) INTO existing_rows
        FROM qmt_strategy.execution_broker_reconciliation_attempt
        WHERE reconcile_attempt NOT BETWEEN 1 AND 10
           OR outcome NOT IN ('NOT_FOUND','UNIQUE_ACCEPTED','UNIQUE_REJECTED','CONFLICT')
           OR receipt_sha256 !~ '^[0-9a-f]{64}$';
        IF existing_rows <> 0 THEN
            RAISE EXCEPTION 'K2-D preflight: legacy_invalid_row_count=%', existing_rows;
        END IF;
        SELECT qmt_strategy.miniqmt_k2_catalog_fingerprint() INTO actual_catalog_sha256;
        IF actual_catalog_sha256 <> '8f1534aae7b0362de2061fafec2f16e056f52fd66251c0d67698ef33a2915d9d' THEN
            RAISE EXCEPTION 'K2-D preflight: exact schema catalog drift: expected 8f1534aae7b0362de2061fafec2f16e056f52fd66251c0d67698ef33a2915d9d, got %', actual_catalog_sha256;
        END IF;
    END IF;
END $$;

SELECT
    'runner_must_verify_committed_forward_sha256' AS verification,
    'bb88b20556bd6e8fe847ad451abcf1496f9e1869ed94183756f3fda8e3015b98'::TEXT AS expected_migration_sha256;

ROLLBACK;

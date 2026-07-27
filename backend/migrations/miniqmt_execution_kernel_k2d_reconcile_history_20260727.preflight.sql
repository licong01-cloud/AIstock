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
    SELECT count(*) INTO existing_rows
    FROM qmt_strategy.execution_algo_command_outbox
    WHERE status IN ('DISPATCHING','OUTCOME_UNKNOWN','RECONCILING','ACKED','ACKED_REJECTED');
    IF existing_rows <> 0 AND NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='qmt_strategy' AND table_name='execution_algo_command_outbox'
          AND column_name='callback_watermark_before_call'
    ) THEN
        RAISE EXCEPTION 'K2-D preflight: post_call_rows_require_callback_watermark_backfill=%', existing_rows;
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
        IF to_regprocedure('qmt_strategy.miniqmt_k2d_catalog_fingerprint()') IS NULL THEN
            RAISE EXCEPTION 'K2-D preflight: exact schema catalog authority is missing';
        END IF;
        SELECT qmt_strategy.miniqmt_k2_catalog_fingerprint() INTO actual_catalog_sha256;
        IF actual_catalog_sha256 <> '2ae93a1e637f4232ea01fc80f7f7a4680679956cc428b12c56adb01f16efea6a' THEN
            RAISE EXCEPTION 'K2-D preflight: base schema catalog drift: expected 2ae93a1e637f4232ea01fc80f7f7a4680679956cc428b12c56adb01f16efea6a, got %', actual_catalog_sha256;
        END IF;
        SELECT qmt_strategy.miniqmt_k2d_catalog_fingerprint() INTO actual_catalog_sha256;
        IF actual_catalog_sha256 <> 'f9034e9e9680a12e335c5bdc0ac06e10dda73d34c8a65128df08c26b0f93725d' THEN
            RAISE EXCEPTION 'K2-D preflight: exact schema catalog drift: expected f9034e9e9680a12e335c5bdc0ac06e10dda73d34c8a65128df08c26b0f93725d, got %', actual_catalog_sha256;
        END IF;
    END IF;
END $$;

SELECT
    'runner_must_verify_committed_forward_sha256' AS verification,
    '23a7d6e19341cf69564719bc60a7c36d5b4daf94dca6cc963b03368e6f7a81c8'::TEXT AS expected_migration_sha256;

ROLLBACK;

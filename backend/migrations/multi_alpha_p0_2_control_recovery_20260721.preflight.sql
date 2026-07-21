-- Read-only preflight for multi_alpha_p0_2_control_recovery_20260721.sql.
--
-- This file performs no schema/data writes and does not create a DB export.
-- It proves that the P0-1B durable base is present and rejects a partially
-- applied P0-2 schema instead of letting application code guess its shape.

DO $p0_2_preflight$
DECLARE
    required_column RECORD;
    actual_type TEXT;
    p0_2_object_count INTEGER;
    expected_p0_2_object_count CONSTANT INTEGER := 7;
BEGIN
    IF to_regclass('strategy_pkg.multi_alpha_combine_backtest_run') IS NULL
       OR to_regclass('strategy_pkg.multi_alpha_combine_backtest_child') IS NULL
       OR to_regclass('strategy_pkg.multi_alpha_combine_backtest_child_attempt') IS NULL
       OR to_regclass('strategy_pkg.multi_alpha_combine_backtest_event') IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'multi_alpha_p0_2_durable_base_schema_missing',
            DETAIL = 'Apply and verify multi_alpha_durable_orchestration_20260718.sql first.';
    END IF;

    FOR required_column IN
        SELECT *
        FROM (VALUES
            ('multi_alpha_combine_backtest_run', 'id', 'text'),
            ('multi_alpha_combine_backtest_run', 'task_id', 'text'),
            ('multi_alpha_combine_backtest_run', 'retry_of_run_id', 'text'),
            ('multi_alpha_combine_backtest_run', 'status', 'text'),
            ('multi_alpha_combine_backtest_run', 'row_version', 'bigint'),
            ('multi_alpha_combine_backtest_child', 'child_id', 'text'),
            ('multi_alpha_combine_backtest_child', 'run_id', 'text'),
            ('multi_alpha_combine_backtest_child', 'status', 'text'),
            ('multi_alpha_combine_backtest_child', 'source_kind', 'text'),
            ('multi_alpha_combine_backtest_child_attempt', 'attempt_id', 'text'),
            ('multi_alpha_combine_backtest_child_attempt', 'child_id', 'text'),
            ('multi_alpha_combine_backtest_child_attempt', 'status', 'text'),
            ('multi_alpha_combine_backtest_child_attempt', 'retry_mode', 'text'),
            ('multi_alpha_combine_backtest_child_attempt', 'row_version', 'bigint')
        ) AS expected(table_name, column_name, data_type)
    LOOP
        SELECT data_type INTO actual_type
        FROM information_schema.columns
        WHERE table_schema = 'strategy_pkg'
          AND table_name = required_column.table_name
          AND column_name = required_column.column_name;

        IF actual_type IS NULL THEN
            RAISE EXCEPTION USING
                ERRCODE = 'P0001',
                MESSAGE = 'multi_alpha_p0_2_base_column_missing',
                DETAIL = format('%I.%I', required_column.table_name, required_column.column_name);
        END IF;
        IF actual_type <> required_column.data_type THEN
            RAISE EXCEPTION USING
                ERRCODE = 'P0001',
                MESSAGE = 'multi_alpha_p0_2_base_column_type_mismatch',
                DETAIL = format('%I.%I expected=%s actual=%s',
                                required_column.table_name,
                                required_column.column_name,
                                required_column.data_type,
                                actual_type);
        END IF;
    END LOOP;

    IF EXISTS (
        SELECT 1
        FROM (VALUES
            ('ck_macb_run_status'),
            ('uq_macb_child_key'),
            ('fk_macb_child_selected_attempt'),
            ('uq_macb_attempt_no'),
            ('ck_macb_attempt_lineage')
        ) AS expected(conname)
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_constraint AS actual
            JOIN pg_class AS cls ON cls.oid = actual.conrelid
            JOIN pg_namespace AS ns ON ns.oid = cls.relnamespace
            WHERE actual.conname = expected.conname
              AND ns.nspname = 'strategy_pkg'
              AND cls.relname LIKE 'multi_alpha_combine_backtest_%'
        )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'multi_alpha_p0_2_base_constraint_missing';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM strategy_pkg.multi_alpha_combine_backtest_run
        WHERE status NOT IN ('queued', 'preparing', 'running', 'pause_requested', 'paused',
                             'cancel_requested', 'cancelling', 'succeeded', 'partial_failed',
                             'failed', 'cancelled', 'partial_recovered')
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'multi_alpha_p0_2_unknown_existing_run_status';
    END IF;

    SELECT
        (CASE WHEN EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'strategy_pkg'
              AND table_name = 'multi_alpha_combine_backtest_run'
              AND column_name = 'recovery_kind'
        ) THEN 1 ELSE 0 END)
        + (CASE WHEN EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'strategy_pkg'
              AND table_name = 'multi_alpha_combine_backtest_child'
              AND column_name = 'source_child_id'
        ) THEN 1 ELSE 0 END)
        + (CASE WHEN EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'strategy_pkg'
              AND table_name = 'multi_alpha_combine_backtest_child_attempt'
              AND column_name = 'run_id'
        ) THEN 1 ELSE 0 END)
        + (CASE WHEN to_regclass('strategy_pkg.multi_alpha_combine_backtest_command') IS NOT NULL THEN 1 ELSE 0 END)
        + (CASE WHEN to_regclass('strategy_pkg.multi_alpha_combine_backtest_cancel_delivery') IS NOT NULL THEN 1 ELSE 0 END)
        + (CASE WHEN to_regclass('strategy_pkg.multi_alpha_combine_backtest_command_delivery') IS NOT NULL THEN 1 ELSE 0 END)
        + (CASE WHEN EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE schemaname = 'strategy_pkg'
              AND indexname = 'idx_macb_run_recovery_source'
        ) THEN 1 ELSE 0 END)
    INTO p0_2_object_count;

    IF p0_2_object_count NOT IN (0, expected_p0_2_object_count) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'multi_alpha_p0_2_partial_schema_detected',
            DETAIL = format('detected_components=%s expected_components=%s',
                            p0_2_object_count, expected_p0_2_object_count),
            HINT = 'Do not run application code against a partially applied P0-2 schema; repair the migration state explicitly.';
    END IF;

    IF p0_2_object_count = expected_p0_2_object_count THEN
        FOR required_column IN
            SELECT *
            FROM (VALUES
                ('multi_alpha_combine_backtest_run', 'recovery_kind', 'text'),
                ('multi_alpha_combine_backtest_run', 'recovery_scope_json', 'jsonb'),
                ('multi_alpha_combine_backtest_run', 'recovery_scope_hash', 'text'),
                ('multi_alpha_combine_backtest_run', 'execution_identity_json', 'jsonb'),
                ('multi_alpha_combine_backtest_run', 'execution_identity_hash', 'text'),
                ('multi_alpha_combine_backtest_run', 'execution_identity_evidence_json', 'jsonb'),
                ('multi_alpha_combine_backtest_child', 'source_child_id', 'text'),
                ('multi_alpha_combine_backtest_child', 'execution_disposition', 'text'),
                ('multi_alpha_combine_backtest_child', 'source_lineage_json', 'jsonb'),
                ('multi_alpha_combine_backtest_child', 'source_lineage_hash', 'text'),
                ('multi_alpha_combine_backtest_child_attempt', 'run_id', 'text'),
                ('multi_alpha_combine_backtest_child_attempt', 'source_attempt_id', 'text'),
                ('multi_alpha_combine_backtest_child_attempt', 'execution_kind', 'text'),
                ('multi_alpha_combine_backtest_child_attempt', 'result_manifest_hash', 'text'),
                ('multi_alpha_combine_backtest_command', 'command_id', 'text'),
                ('multi_alpha_combine_backtest_command', 'command_seq', 'bigint'),
                ('multi_alpha_combine_backtest_command', 'request_json', 'jsonb'),
                ('multi_alpha_combine_backtest_cancel_delivery', 'delivery_id', 'text'),
                ('multi_alpha_combine_backtest_cancel_delivery', 'expected_process_identity_json', 'jsonb')
            ) AS expected(table_name, column_name, data_type)
        LOOP
            SELECT data_type INTO actual_type
            FROM information_schema.columns
            WHERE table_schema = 'strategy_pkg'
              AND table_name = required_column.table_name
              AND column_name = required_column.column_name;
            IF actual_type IS DISTINCT FROM required_column.data_type THEN
                RAISE EXCEPTION USING
                    ERRCODE = 'P0001',
                    MESSAGE = 'multi_alpha_p0_2_existing_column_type_mismatch',
                    DETAIL = format('%I.%I expected=%s actual=%s',
                                    required_column.table_name,
                                    required_column.column_name,
                                    required_column.data_type,
                                    COALESCE(actual_type, '<missing>'));
            END IF;
        END LOOP;

        IF EXISTS (
            SELECT 1
            FROM (VALUES
                ('ck_macb_run_recovery_tuple'),
                ('ck_macb_run_execution_identity'),
                ('ck_macb_run_execution_identity_evidence'),
                ('ck_macb_run_execution_identity_evidence_alignment'),
                ('ck_macb_child_execution_disposition'),
                ('ck_macb_child_source_lineage'),
                ('ck_macb_child_not_recovered_disposition'),
                ('fk_macb_child_source_child'),
                ('uq_macb_child_run_child'),
                ('fk_macb_attempt_run_child'),
                ('fk_macb_attempt_source_attempt'),
                ('uq_macb_attempt_run_child_attempt'),
                ('ck_macb_attempt_execution_kind'),
                ('ck_macb_attempt_result_manifest_hash'),
                ('ck_macb_attempt_execution_remote_fields'),
                ('ck_macb_command_id'),
                ('ck_macb_command_action'),
                ('ck_macb_command_target'),
                ('ck_macb_command_target_key'),
                ('ck_macb_command_payload_hash'),
                ('ck_macb_command_scope_hash'),
                ('ck_macb_command_status'),
                ('ck_macb_command_request_json'),
                ('ck_macb_command_response_json'),
                ('ck_macb_command_error_json'),
                ('ck_macb_command_staging_manifest'),
                ('ck_macb_command_row_version'),
                ('ck_macb_command_fencing_token'),
                ('ck_macb_command_delivery_attempt_count'),
                ('uq_macb_command_idempotency'),
                ('fk_macb_command_child'),
                ('fk_macb_command_attempt'),
                ('ck_macb_cancel_delivery_id'),
                ('ck_macb_cancel_delivery_submission_hash'),
                ('ck_macb_cancel_delivery_target_key'),
                ('ck_macb_cancel_delivery_process_identity'),
                ('ck_macb_cancel_delivery_generation'),
                ('ck_macb_cancel_delivery_intent_hash'),
                ('ck_macb_cancel_delivery_status'),
                ('ck_macb_cancel_delivery_row_version'),
                ('ck_macb_cancel_delivery_fencing_token'),
                ('ck_macb_cancel_delivery_attempt_count'),
                ('ck_macb_cancel_delivery_receipt_json'),
                ('ck_macb_cancel_delivery_error_json'),
                ('uq_macb_cancel_delivery_target'),
                ('fk_macb_cancel_delivery_child'),
                ('fk_macb_cancel_delivery_attempt')
            ) AS expected(conname)
            WHERE NOT EXISTS (
                SELECT 1
                FROM pg_constraint AS actual
                JOIN pg_class AS cls ON cls.oid = actual.conrelid
                JOIN pg_namespace AS ns ON ns.oid = cls.relnamespace
                WHERE actual.conname = expected.conname
                  AND ns.nspname = 'strategy_pkg'
            )
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = 'P0001',
            MESSAGE = 'multi_alpha_p0_2_required_constraint_missing';
        END IF;

        IF EXISTS (
            SELECT 1
            FROM (VALUES
                ('idx_macb_run_recovery_source'),
                ('idx_macb_child_source_lineage'),
                ('idx_macb_child_recovery_disposition'),
                ('uq_macb_attempt_active_remote_execution'),
                ('idx_macb_attempt_source_attempt'),
                ('idx_macb_command_claim'),
                ('uq_macb_command_active_target'),
                ('idx_macb_command_run_seq'),
                ('uq_macb_cancel_delivery_active_attempt'),
                ('idx_macb_cancel_delivery_claim')
            ) AS expected(indexname)
            WHERE NOT EXISTS (
                SELECT 1
                FROM pg_indexes AS actual
                WHERE actual.schemaname = 'strategy_pkg'
                  AND actual.indexname = expected.indexname
            )
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = 'P0001',
                MESSAGE = 'multi_alpha_p0_2_required_index_missing';
        END IF;
    END IF;
END
$p0_2_preflight$;

SELECT
    'multi_alpha_p0_2_control_recovery_20260721' AS migration,
    CASE
        WHEN to_regclass('strategy_pkg.multi_alpha_combine_backtest_command') IS NULL THEN 'ready'
        ELSE 'already_applied'
    END AS preflight_status,
    COUNT(*) AS durable_run_count
FROM strategy_pkg.multi_alpha_combine_backtest_run;

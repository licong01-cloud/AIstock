-- Read-only preflight for multi_alpha_durable_orchestration_20260718.sql.
-- This file performs no schema/data writes and does not create a DB export.

DO $preflight$
DECLARE
    required_column RECORD;
    existing_type TEXT;
BEGIN
    IF to_regclass('strategy_pkg.multi_alpha_combine_backtest_run') IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'multi_alpha_durable_base_run_table_missing';
    END IF;
    IF to_regclass('strategy_pkg.multi_alpha_combine_backtest_scheme_result') IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'multi_alpha_durable_base_scheme_table_missing';
    END IF;
    IF to_regclass('strategy_pkg.multi_alpha_combine_backtest_loo') IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'multi_alpha_durable_base_loo_table_missing';
    END IF;

    FOR required_column IN
        SELECT *
        FROM (VALUES
            ('multi_alpha_combine_backtest_run', 'id', 'text'),
            ('multi_alpha_combine_backtest_run', 'roster_hash', 'text'),
            ('multi_alpha_combine_backtest_run', 'roster_json', 'jsonb'),
            ('multi_alpha_combine_backtest_run', 'walk_forward_json', 'jsonb'),
            ('multi_alpha_combine_backtest_run', 'backtest_config_json', 'jsonb'),
            ('multi_alpha_combine_backtest_run', 'status', 'text'),
            ('multi_alpha_combine_backtest_scheme_result', 'run_id', 'text'),
            ('multi_alpha_combine_backtest_scheme_result', 'weighting_scheme', 'text'),
            ('multi_alpha_combine_backtest_loo', 'run_id', 'text'),
            ('multi_alpha_combine_backtest_loo', 'weighting_scheme', 'text'),
            ('multi_alpha_combine_backtest_loo', 'dropped_leg_id', 'text')
        ) AS expected(table_name, column_name, data_type)
    LOOP
        SELECT c.data_type INTO existing_type
        FROM information_schema.columns AS c
        WHERE c.table_schema = 'strategy_pkg'
          AND c.table_name = required_column.table_name
          AND c.column_name = required_column.column_name;

        IF existing_type IS NULL THEN
            RAISE EXCEPTION USING
                ERRCODE = 'P0001',
                MESSAGE = 'multi_alpha_durable_required_column_missing',
                DETAIL = format('%I.%I', required_column.table_name, required_column.column_name);
        END IF;
        IF existing_type <> required_column.data_type THEN
            RAISE EXCEPTION USING
                ERRCODE = 'P0001',
                MESSAGE = 'multi_alpha_durable_required_column_type_mismatch',
                DETAIL = format('%I.%I expected=%s actual=%s', required_column.table_name,
                                required_column.column_name, required_column.data_type, existing_type);
        END IF;
    END LOOP;

    IF EXISTS (
        SELECT 1
        FROM strategy_pkg.multi_alpha_combine_backtest_run
        WHERE status NOT IN ('running', 'succeeded', 'failed', 'queued', 'preparing',
                             'pause_requested', 'paused', 'cancel_requested', 'cancelling',
                             'partial_failed', 'cancelled')
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'multi_alpha_durable_unknown_existing_run_status';
    END IF;

    -- If an earlier partial deployment created a durable column, it must have
    -- the exact type expected by this version. Missing additive columns are OK.
    FOR required_column IN
        SELECT *
        FROM (VALUES
            ('multi_alpha_combine_task', 'task_id', 'text', TRUE),
            ('multi_alpha_combine_task', 'task_name', 'text', TRUE),
            ('multi_alpha_combine_task', 'task_type', 'text', TRUE),
            ('multi_alpha_combine_task', 'description', 'text', TRUE),
            ('multi_alpha_combine_task', 'roster_hash', 'text', TRUE),
            ('multi_alpha_combine_task', 'roster_json', 'jsonb', TRUE),
            ('multi_alpha_combine_task', 'default_request_json', 'jsonb', TRUE),
            ('multi_alpha_combine_task', 'legacy_group_key', 'text', TRUE),
            ('multi_alpha_combine_task', 'source_kind', 'text', TRUE),
            ('multi_alpha_combine_task', 'created_by', 'text', TRUE),
            ('multi_alpha_combine_task', 'created_at', 'timestamp with time zone', TRUE),
            ('multi_alpha_combine_task', 'updated_at', 'timestamp with time zone', TRUE),
            ('multi_alpha_combine_backtest_run', 'task_id', 'text', FALSE),
            ('multi_alpha_combine_backtest_run', 'request_hash', 'text', FALSE),
            ('multi_alpha_combine_backtest_run', 'retry_of_run_id', 'text', FALSE),
            ('multi_alpha_combine_backtest_run', 'phase', 'text', FALSE),
            ('multi_alpha_combine_backtest_run', 'progress_json', 'jsonb', FALSE),
            ('multi_alpha_combine_backtest_run', 'row_version', 'bigint', FALSE),
            ('multi_alpha_combine_backtest_run', 'owner_id', 'text', FALSE),
            ('multi_alpha_combine_backtest_run', 'fencing_token', 'bigint', FALSE),
            ('multi_alpha_combine_backtest_run', 'lease_expires_at', 'timestamp with time zone', FALSE),
            ('multi_alpha_combine_backtest_run', 'heartbeat_at', 'timestamp with time zone', FALSE),
            ('multi_alpha_combine_backtest_run', 'pause_requested_at', 'timestamp with time zone', FALSE),
            ('multi_alpha_combine_backtest_run', 'pause_requested_by', 'text', FALSE),
            ('multi_alpha_combine_backtest_run', 'cancel_requested_at', 'timestamp with time zone', FALSE),
            ('multi_alpha_combine_backtest_run', 'cancel_requested_by', 'text', FALSE),
            ('multi_alpha_combine_backtest_run', 'node_parallelism_json', 'jsonb', FALSE),
            ('multi_alpha_combine_backtest_run', 'started_at', 'timestamp with time zone', FALSE),
            ('multi_alpha_combine_backtest_run', 'finished_at', 'timestamp with time zone', FALSE),
            ('multi_alpha_combine_backtest_run', 'updated_at', 'timestamp with time zone', FALSE),
            ('multi_alpha_combine_backtest_run', 'error_code', 'text', FALSE),
            ('multi_alpha_combine_backtest_run', 'error_json', 'jsonb', FALSE),
            ('multi_alpha_combine_backtest_child', 'child_id', 'text', TRUE),
            ('multi_alpha_combine_backtest_child', 'run_id', 'text', TRUE),
            ('multi_alpha_combine_backtest_child', 'child_key', 'text', TRUE),
            ('multi_alpha_combine_backtest_child', 'child_kind', 'text', TRUE),
            ('multi_alpha_combine_backtest_child', 'weighting_scheme', 'text', TRUE),
            ('multi_alpha_combine_backtest_child', 'dropped_leg_id', 'text', TRUE),
            ('multi_alpha_combine_backtest_child', 'ordinal', 'integer', TRUE),
            ('multi_alpha_combine_backtest_child', 'status', 'text', TRUE),
            ('multi_alpha_combine_backtest_child', 'input_manifest_json', 'jsonb', TRUE),
            ('multi_alpha_combine_backtest_child', 'input_manifest_hash', 'text', TRUE),
            ('multi_alpha_combine_backtest_child', 'prediction_artifact_uri', 'text', TRUE),
            ('multi_alpha_combine_backtest_child', 'prediction_artifact_hash', 'text', TRUE),
            ('multi_alpha_combine_backtest_child', 'selected_attempt_id', 'text', TRUE),
            ('multi_alpha_combine_backtest_child', 'source_kind', 'text', TRUE),
            ('multi_alpha_combine_backtest_child', 'created_at', 'timestamp with time zone', TRUE),
            ('multi_alpha_combine_backtest_child', 'updated_at', 'timestamp with time zone', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'attempt_id', 'text', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'child_id', 'text', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'attempt_no', 'integer', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'retry_mode', 'text', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'retry_of_attempt_id', 'text', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'node_id', 'text', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'qe_task_id', 'text', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'qe_loop_id', 'text', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'submission_intent_hash', 'text', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'remote_status', 'text', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'status', 'text', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'phase', 'text', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'row_version', 'bigint', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'owner_id', 'text', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'fencing_token', 'bigint', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'lease_expires_at', 'timestamp with time zone', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'heartbeat_at', 'timestamp with time zone', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'artifact_manifest_json', 'jsonb', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'result_manifest_json', 'jsonb', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'error_code', 'text', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'error_json', 'jsonb', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'queued_at', 'timestamp with time zone', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'submitted_at', 'timestamp with time zone', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'started_at', 'timestamp with time zone', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'finished_at', 'timestamp with time zone', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'created_at', 'timestamp with time zone', TRUE),
            ('multi_alpha_combine_backtest_child_attempt', 'updated_at', 'timestamp with time zone', TRUE),
            ('multi_alpha_combine_backtest_event', 'event_id', 'bigint', TRUE),
            ('multi_alpha_combine_backtest_event', 'run_id', 'text', TRUE),
            ('multi_alpha_combine_backtest_event', 'child_id', 'text', TRUE),
            ('multi_alpha_combine_backtest_event', 'attempt_id', 'text', TRUE),
            ('multi_alpha_combine_backtest_event', 'event_type', 'text', TRUE),
            ('multi_alpha_combine_backtest_event', 'phase', 'text', TRUE),
            ('multi_alpha_combine_backtest_event', 'reason_code', 'text', TRUE),
            ('multi_alpha_combine_backtest_event', 'payload_json', 'jsonb', TRUE),
            ('multi_alpha_combine_backtest_event', 'created_at', 'timestamp with time zone', TRUE)
        ) AS expected(table_name, column_name, data_type, required_if_table_exists)
    LOOP
        SELECT c.data_type INTO existing_type
        FROM information_schema.columns AS c
        WHERE c.table_schema = 'strategy_pkg'
          AND c.table_name = required_column.table_name
          AND c.column_name = required_column.column_name;
        IF existing_type IS NULL
           AND required_column.required_if_table_exists
           AND to_regclass(format('strategy_pkg.%I', required_column.table_name)) IS NOT NULL THEN
            RAISE EXCEPTION USING
                ERRCODE = 'P0001',
                MESSAGE = 'multi_alpha_durable_partial_table_missing_column',
                DETAIL = format('%I.%I', required_column.table_name, required_column.column_name);
        END IF;
        IF existing_type IS NOT NULL AND existing_type <> required_column.data_type THEN
            RAISE EXCEPTION USING
                ERRCODE = 'P0001',
                MESSAGE = 'multi_alpha_durable_existing_column_type_mismatch',
                DETAIL = format('%I.%I expected=%s actual=%s', required_column.table_name,
                                required_column.column_name, required_column.data_type, existing_type);
        END IF;
    END LOOP;

    IF to_regclass('strategy_pkg.multi_alpha_combine_task') IS NOT NULL
       AND to_regclass('strategy_pkg.multi_alpha_combine_backtest_child') IS NOT NULL
       AND to_regclass('strategy_pkg.multi_alpha_combine_backtest_child_attempt') IS NOT NULL
       AND to_regclass('strategy_pkg.multi_alpha_combine_backtest_event') IS NOT NULL THEN
        IF EXISTS (
            SELECT 1
            FROM (VALUES
                ('ck_macb_run_status'),
                ('fk_macb_run_task'),
                ('fk_macb_run_retry_of'),
                ('uq_macb_child_key'),
                ('fk_macb_child_selected_attempt'),
                ('uq_macb_attempt_no'),
                ('fk_macb_attempt_retry_of'),
                ('ck_macb_attempt_lineage'),
                ('ck_macb_event_type')
            ) AS expected(conname)
            WHERE NOT EXISTS (
                SELECT 1
                FROM pg_constraint AS actual
                JOIN pg_class AS cls ON cls.oid = actual.conrelid
                JOIN pg_namespace AS ns ON ns.oid = cls.relnamespace
                WHERE actual.conname = expected.conname
                  AND ns.nspname = 'strategy_pkg'
                  AND cls.relname LIKE 'multi_alpha_combine%'
            )
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = 'P0001',
                MESSAGE = 'multi_alpha_durable_required_constraint_missing';
        END IF;
        IF EXISTS (
            SELECT 1
            FROM (VALUES
                ('uq_mact_legacy_group_key'),
                ('idx_macb_run_claim'),
                ('idx_macb_child_run_ordinal'),
                ('uq_macb_attempt_remote_identity'),
                ('idx_macb_attempt_claim'),
                ('idx_macb_event_run_cursor')
            ) AS expected(indexname)
            WHERE NOT EXISTS (
                SELECT 1
                FROM pg_indexes AS actual
                WHERE actual.schemaname = 'strategy_pkg' AND actual.indexname = expected.indexname
            )
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = 'P0001',
                MESSAGE = 'multi_alpha_durable_required_index_missing';
        END IF;
        IF EXISTS (
            SELECT 1
            FROM (VALUES
                ('strategy_pkg.multi_alpha_combine_task'::regclass),
                ('strategy_pkg.multi_alpha_combine_backtest_child'::regclass),
                ('strategy_pkg.multi_alpha_combine_backtest_child_attempt'::regclass),
                ('strategy_pkg.multi_alpha_combine_backtest_event'::regclass)
            ) AS expected(table_oid)
            WHERE obj_description(expected.table_oid, 'pg_class') IS NULL
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = 'P0001',
                MESSAGE = 'multi_alpha_durable_required_table_comment_missing';
        END IF;
    END IF;
END
$preflight$;

SELECT
    'multi_alpha_durable_orchestration_20260718' AS migration,
    'ready' AS preflight_status,
    COUNT(*) AS existing_run_count,
    EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'strategy_pkg'
          AND table_name = 'multi_alpha_combine_backtest_run'
          AND column_name = 'task_id'
    ) AS durable_run_columns_present
FROM strategy_pkg.multi_alpha_combine_backtest_run;

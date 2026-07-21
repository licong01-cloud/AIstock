-- Read-only preflight for qe_archive_multi_alpha_p0_2_recovery_20260721.sql.
-- No schema/data writes and no DB export are performed here.

DO $qear_macb_p0_2_preflight$
DECLARE
    required_column RECORD;
    actual_type TEXT;
    p0_2_object_count INTEGER;
    expected_object_count CONSTANT INTEGER := 4;
BEGIN
    IF to_regclass('qe_archive.run') IS NULL
       OR to_regclass('qe_archive.multi_alpha_run') IS NULL
       OR to_regclass('qe_archive.multi_alpha_scheme') IS NULL
       OR to_regclass('qe_archive.multi_alpha_loo') IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'qe_archive_multi_alpha_p0_2_base_schema_missing',
            DETAIL = 'Apply qe_archive_multi_alpha_phase_a_20260628.sql first.';
    END IF;

    FOR required_column IN
        SELECT *
        FROM (VALUES
            ('multi_alpha_run', 'run_id', 'text'),
            ('multi_alpha_run', 'status', 'text'),
            ('multi_alpha_run', 'logical_status', 'text'),
            ('multi_alpha_run', 'reason_json', 'jsonb'),
            ('multi_alpha_run', 'archived_at', 'timestamp with time zone')
        ) AS expected(table_name, column_name, data_type)
    LOOP
        SELECT data_type INTO actual_type
        FROM information_schema.columns
        WHERE table_schema = 'qe_archive'
          AND table_name = required_column.table_name
          AND column_name = required_column.column_name;
        IF actual_type IS DISTINCT FROM required_column.data_type THEN
            RAISE EXCEPTION USING
                ERRCODE = 'P0001',
                MESSAGE = 'qe_archive_multi_alpha_p0_2_base_column_mismatch',
                DETAIL = format('%I.%I expected=%s actual=%s',
                                required_column.table_name,
                                required_column.column_name,
                                required_column.data_type,
                                COALESCE(actual_type, '<missing>'));
        END IF;
    END LOOP;

    IF EXISTS (
        SELECT 1
        FROM qe_archive.multi_alpha_run
        WHERE status NOT IN ('succeeded', 'partial_failed', 'partial_recovered', 'failed', 'cancelled')
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'qe_archive_multi_alpha_p0_2_unknown_existing_status';
    END IF;

    SELECT
        (CASE WHEN EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'qe_archive'
              AND table_name = 'multi_alpha_run'
              AND column_name = 'archive_schema_version'
        ) THEN 1 ELSE 0 END)
        + (CASE WHEN to_regclass('qe_archive.multi_alpha_recovery_child') IS NOT NULL THEN 1 ELSE 0 END)
        + (CASE WHEN to_regclass('qe_archive.multi_alpha_recovery_attempt') IS NOT NULL THEN 1 ELSE 0 END)
        + (CASE WHEN EXISTS (
            SELECT 1 FROM pg_indexes
            WHERE schemaname = 'qe_archive'
              AND indexname = 'idx_qear_macb_run_recovery_source'
        ) THEN 1 ELSE 0 END)
    INTO p0_2_object_count;

    IF p0_2_object_count NOT IN (0, expected_object_count) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'qe_archive_multi_alpha_p0_2_partial_schema_detected',
            DETAIL = format('detected_components=%s expected_components=%s',
                            p0_2_object_count, expected_object_count),
            HINT = 'Repair the partial Archive v2 schema explicitly; do not permit archive handler fallback.';
    END IF;

    IF p0_2_object_count = expected_object_count THEN
        FOR required_column IN
            SELECT *
            FROM (VALUES
                ('multi_alpha_run', 'archive_schema_version', 'text'),
                ('multi_alpha_run', 'retry_of_run_id', 'text'),
                ('multi_alpha_run', 'recovery_kind', 'text'),
                ('multi_alpha_run', 'recovery_scope_json', 'jsonb'),
                ('multi_alpha_run', 'recovery_scope_hash', 'text'),
                ('multi_alpha_run', 'execution_identity_json', 'jsonb'),
                ('multi_alpha_run', 'execution_identity_hash', 'text'),
                ('multi_alpha_run', 'execution_identity_evidence_json', 'jsonb'),
                ('multi_alpha_recovery_child', 'source_lineage_json', 'jsonb'),
                ('multi_alpha_recovery_attempt', 'result_manifest_json', 'jsonb')
            ) AS expected(table_name, column_name, data_type)
        LOOP
            SELECT data_type INTO actual_type
            FROM information_schema.columns
            WHERE table_schema = 'qe_archive'
              AND table_name = required_column.table_name
              AND column_name = required_column.column_name;
            IF actual_type IS DISTINCT FROM required_column.data_type THEN
                RAISE EXCEPTION USING
                    ERRCODE = 'P0001',
                    MESSAGE = 'qe_archive_multi_alpha_p0_2_existing_column_mismatch',
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
                ('ck_qear_macb_v2_recovery_tuple'),
                ('ck_qear_macb_execution_identity_evidence'),
                ('ck_qear_macb_archive_schema_version'),
                ('ck_qear_macb_recovery_kind'),
                ('ck_qear_macb_recovery_scope'),
                ('ck_qear_macb_execution_identity'),
                ('ck_qear_macb_partial_recovered_kind'),
                ('ck_qear_macb_recovery_child_kind'),
                ('ck_qear_macb_recovery_child_not_recovered'),
                ('ck_qear_macb_recovery_child_status'),
                ('ck_qear_macb_recovery_child_disposition'),
                ('ck_qear_macb_recovery_child_lineage'),
                ('ck_qear_macb_recovery_child_input_manifest'),
                ('ck_qear_macb_recovery_child_input_hash'),
                ('ck_qear_macb_recovery_child_prediction_hash'),
                ('fk_qear_macb_recovery_attempt_child'),
                ('uq_qear_macb_recovery_attempt_child_id'),
                ('uq_qear_macb_recovery_attempt_child_no'),
                ('ck_qear_macb_recovery_attempt_no'),
                ('ck_qear_macb_recovery_attempt_mode'),
                ('ck_qear_macb_recovery_attempt_kind'),
                ('ck_qear_macb_recovery_attempt_status'),
                ('ck_qear_macb_recovery_attempt_artifact_json'),
                ('ck_qear_macb_recovery_attempt_result_json'),
                ('ck_qear_macb_recovery_attempt_result_hash'),
                ('ck_qear_macb_recovery_attempt_reference'),
                ('fk_qear_macb_recovery_child_selected_attempt')
            ) AS expected(conname)
            WHERE NOT EXISTS (
                SELECT 1
                FROM pg_constraint AS actual
                JOIN pg_class AS cls ON cls.oid = actual.conrelid
                JOIN pg_namespace AS ns ON ns.oid = cls.relnamespace
                WHERE actual.conname = expected.conname
                  AND ns.nspname = 'qe_archive'
            )
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = 'P0001',
            MESSAGE = 'qe_archive_multi_alpha_p0_2_required_constraint_missing';
        END IF;

        IF EXISTS (
            SELECT 1
            FROM (VALUES
                ('idx_qear_macb_run_recovery_source'),
                ('idx_qear_macb_recovery_child_source'),
                ('idx_qear_macb_recovery_child_status'),
                ('idx_qear_macb_recovery_attempt_source')
            ) AS expected(indexname)
            WHERE NOT EXISTS (
                SELECT 1
                FROM pg_indexes AS actual
                WHERE actual.schemaname = 'qe_archive'
                  AND actual.indexname = expected.indexname
            )
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = 'P0001',
                MESSAGE = 'qe_archive_multi_alpha_p0_2_required_index_missing';
        END IF;
    END IF;
END
$qear_macb_p0_2_preflight$;

SELECT
    'qe_archive_multi_alpha_p0_2_recovery_20260721' AS migration,
    CASE
        WHEN to_regclass('qe_archive.multi_alpha_recovery_child') IS NULL THEN 'ready'
        ELSE 'already_applied'
    END AS preflight_status,
    COUNT(*) AS archived_multi_alpha_run_count
FROM qe_archive.multi_alpha_run;

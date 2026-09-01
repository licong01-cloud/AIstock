DO $$
DECLARE
    schema_errors TEXT[];
BEGIN
    IF to_regclass('qe_archive.run_evaluation') IS NULL THEN
        RAISE EXCEPTION 'QELT Phase 2 control table qe_archive.run_evaluation is missing';
    END IF;
    IF to_regclass('qe_archive.schema_version') IS NULL THEN
        RAISE EXCEPTION 'QE archive schema version table qe_archive.schema_version is missing';
    END IF;

    IF to_regclass('qe_archive.run_evaluation_metric') IS NOT NULL THEN
        SELECT array_agg(required.name ORDER BY required.ordinal)
        INTO schema_errors
        FROM (VALUES
            (1, 'evaluation_metric_id', 'int8', 'NO'),
            (2, 'evaluation_id', 'text', 'NO'),
            (3, 'metric_key', 'text', 'NO'),
            (4, 'metric_scope', 'text', 'NO'),
            (5, 'period_start', 'date', 'YES'),
            (6, 'period_end', 'date', 'YES'),
            (7, 'horizon', 'int4', 'YES'),
            (8, 'sector_code', 'text', 'YES'),
            (9, 'dimension_key', 'text', 'NO'),
            (10, 'dimension_json', 'jsonb', 'NO'),
            (11, 'value_num', 'float8', 'YES'),
            (12, 'value_text', 'text', 'YES'),
            (13, 'value_json', 'jsonb', 'YES'),
            (14, 'unit', 'text', 'YES'),
            (15, 'direction', 'text', 'YES'),
            (16, 'source_payload_path', 'text', 'NO'),
            (17, 'quality_flag', 'text', 'NO'),
            (18, 'created_at', 'timestamptz', 'NO')
        ) AS required(ordinal, name, udt_name, is_nullable)
        WHERE NOT EXISTS (
            SELECT 1 FROM information_schema.columns c
            WHERE c.table_schema = 'qe_archive'
              AND c.table_name = 'run_evaluation_metric'
              AND c.column_name = required.name
              AND c.udt_name = required.udt_name
              AND c.is_nullable = required.is_nullable
        );
        IF schema_errors IS NOT NULL THEN
            RAISE EXCEPTION 'existing run_evaluation_metric column contract differs: %', schema_errors;
        END IF;
        IF (SELECT count(*) FROM information_schema.columns
             WHERE table_schema = 'qe_archive' AND table_name = 'run_evaluation_metric') <> 18 THEN
            RAISE EXCEPTION 'existing run_evaluation_metric has unexpected columns';
        END IF;
        IF EXISTS (
            SELECT required.name
            FROM (VALUES
                ('run_evaluation_metric_pkey'),
                ('uq_qear_run_evaluation_metric_dimension'),
                ('ck_qear_run_evaluation_metric_scope'),
                ('ck_qear_run_evaluation_metric_quality'),
                ('ck_qear_run_evaluation_metric_horizon'),
                ('ck_qear_run_evaluation_metric_values'),
                ('ck_qear_run_evaluation_metric_dimension_key'),
                ('ck_qear_run_evaluation_metric_period'),
                ('run_evaluation_metric_evaluation_id_fkey')
            ) AS required(name)
            WHERE NOT EXISTS (
                SELECT 1 FROM pg_constraint c
                WHERE c.conrelid = 'qe_archive.run_evaluation_metric'::regclass
                  AND c.conname = required.name
            )
        ) THEN
            RAISE EXCEPTION 'existing run_evaluation_metric constraints are incomplete';
        END IF;
        IF to_regclass('qe_archive.idx_qear_run_evaluation_metric_lookup') IS NULL
           OR to_regclass('qe_archive.idx_qear_run_evaluation_metric_key_value') IS NULL THEN
            RAISE EXCEPTION 'existing run_evaluation_metric indexes are incomplete';
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint c
            WHERE c.conrelid = 'qe_archive.run_evaluation_metric'::regclass
              AND c.conname = 'run_evaluation_metric_evaluation_id_fkey'
              AND pg_get_constraintdef(c.oid) LIKE '%REFERENCES qe_archive.run_evaluation(evaluation_id) ON DELETE CASCADE%'
        ) THEN
            RAISE EXCEPTION 'existing run_evaluation_metric foreign key contract differs';
        END IF;
        IF obj_description('qe_archive.run_evaluation_metric'::regclass, 'pg_class') IS NULL
           OR col_description('qe_archive.run_evaluation_metric'::regclass, 10)
              NOT LIKE 'qelt_metric_dimension_v2:%' THEN
            RAISE EXCEPTION 'existing run_evaluation_metric comments are incomplete';
        END IF;
        IF (SELECT column_default FROM information_schema.columns
            WHERE table_schema = 'qe_archive' AND table_name = 'run_evaluation_metric'
              AND column_name = 'dimension_json') IS DISTINCT FROM '''{}''::jsonb'
           OR (SELECT column_default FROM information_schema.columns
               WHERE table_schema = 'qe_archive' AND table_name = 'run_evaluation_metric'
                 AND column_name = 'created_at') IS NULL
           OR (SELECT column_default FROM information_schema.columns
               WHERE table_schema = 'qe_archive' AND table_name = 'run_evaluation_metric'
                 AND column_name = 'created_at') NOT LIKE 'clock_timestamp()%' THEN
            RAISE EXCEPTION 'existing run_evaluation_metric defaults differ';
        END IF;
    END IF;

    IF to_regclass('qe_archive.run_evaluation_artifact') IS NOT NULL THEN
        SELECT array_agg(required.name ORDER BY required.ordinal)
        INTO schema_errors
        FROM (VALUES
            (1, 'evaluation_artifact_id', 'int8', 'NO'),
            (2, 'evaluation_id', 'text', 'NO'),
            (3, 'artifact_type', 'text', 'NO'),
            (4, 'artifact_uri', 'text', 'NO'),
            (5, 'sha256', 'text', 'NO'),
            (6, 'schema_sha256', 'text', 'YES'),
            (7, 'size_bytes', 'int8', 'YES'),
            (8, 'row_count', 'int8', 'YES'),
            (9, 'status', 'text', 'NO'),
            (10, 'metadata', 'jsonb', 'NO'),
            (11, 'created_at', 'timestamptz', 'NO')
        ) AS required(ordinal, name, udt_name, is_nullable)
        WHERE NOT EXISTS (
            SELECT 1 FROM information_schema.columns c
            WHERE c.table_schema = 'qe_archive'
              AND c.table_name = 'run_evaluation_artifact'
              AND c.column_name = required.name
              AND c.udt_name = required.udt_name
              AND c.is_nullable = required.is_nullable
        );
        IF schema_errors IS NOT NULL THEN
            RAISE EXCEPTION 'existing run_evaluation_artifact column contract differs: %', schema_errors;
        END IF;
        IF (SELECT count(*) FROM information_schema.columns
             WHERE table_schema = 'qe_archive' AND table_name = 'run_evaluation_artifact') <> 11 THEN
            RAISE EXCEPTION 'existing run_evaluation_artifact has unexpected columns';
        END IF;
        IF EXISTS (
            SELECT required.name
            FROM (VALUES
                ('run_evaluation_artifact_pkey'),
                ('uq_qear_run_evaluation_artifact_identity'),
                ('uq_qear_run_evaluation_artifact_type'),
                ('ck_qear_run_evaluation_artifact_hash'),
                ('ck_qear_run_evaluation_artifact_size'),
                ('ck_qear_run_evaluation_artifact_status'),
                ('ck_qear_run_evaluation_artifact_uri'),
                ('run_evaluation_artifact_evaluation_id_fkey')
            ) AS required(name)
            WHERE NOT EXISTS (
                SELECT 1 FROM pg_constraint c
                WHERE c.conrelid = 'qe_archive.run_evaluation_artifact'::regclass
                  AND c.conname = required.name
            )
        ) THEN
            RAISE EXCEPTION 'existing run_evaluation_artifact constraints are incomplete';
        END IF;
        IF to_regclass('qe_archive.idx_qear_run_evaluation_artifact_lookup') IS NULL
           OR to_regclass('qe_archive.idx_qear_run_evaluation_artifact_sha') IS NULL THEN
            RAISE EXCEPTION 'existing run_evaluation_artifact indexes are incomplete';
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint c
            WHERE c.conrelid = 'qe_archive.run_evaluation_artifact'::regclass
              AND c.conname = 'run_evaluation_artifact_evaluation_id_fkey'
              AND pg_get_constraintdef(c.oid) LIKE '%REFERENCES qe_archive.run_evaluation(evaluation_id) ON DELETE CASCADE%'
        ) THEN
            RAISE EXCEPTION 'existing run_evaluation_artifact foreign key contract differs';
        END IF;
        IF obj_description('qe_archive.run_evaluation_artifact'::regclass, 'pg_class') IS NULL
           OR col_description('qe_archive.run_evaluation_artifact'::regclass, 10)
              NOT LIKE 'qelt_evaluation_artifact_metadata_v1:%' THEN
            RAISE EXCEPTION 'existing run_evaluation_artifact comments are incomplete';
        END IF;
        IF (SELECT column_default FROM information_schema.columns
            WHERE table_schema = 'qe_archive' AND table_name = 'run_evaluation_artifact'
              AND column_name = 'status') IS DISTINCT FROM '''published''::text'
           OR (SELECT column_default FROM information_schema.columns
               WHERE table_schema = 'qe_archive' AND table_name = 'run_evaluation_artifact'
                 AND column_name = 'metadata') IS DISTINCT FROM '''{}''::jsonb'
           OR (SELECT column_default FROM information_schema.columns
               WHERE table_schema = 'qe_archive' AND table_name = 'run_evaluation_artifact'
                 AND column_name = 'created_at') IS NULL
           OR (SELECT column_default FROM information_schema.columns
               WHERE table_schema = 'qe_archive' AND table_name = 'run_evaluation_artifact'
                 AND column_name = 'created_at') NOT LIKE 'clock_timestamp()%' THEN
            RAISE EXCEPTION 'existing run_evaluation_artifact defaults differ';
        END IF;
    END IF;
END
$$;

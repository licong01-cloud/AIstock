DO $$
DECLARE
    missing_columns TEXT[];
    invalid_rows BIGINT;
BEGIN
    IF to_regclass('qe_archive.run') IS NULL THEN
        RAISE EXCEPTION 'qe_archive.run is required before F-014 Phase 2 control migration';
    END IF;
    IF to_regclass('qe_archive.run_evaluation') IS NULL THEN
        RAISE NOTICE 'qe_archive.run_evaluation is absent and ready for additive creation';
        RETURN;
    END IF;

    SELECT array_agg(required.column_name ORDER BY required.column_name)
    INTO missing_columns
    FROM (VALUES
        ('evaluation_id'), ('run_id'), ('parent_task_id'), ('parent_loop_index'), ('evaluation_type'), ('profile_id'),
        ('profile_sha256'), ('evaluator_version'), ('evaluator_source_sha256'),
        ('execution_environment_snapshot_id'), ('execution_environment_manifest_sha256'),
        ('bundle_sha256'), ('qe_dataset_contract_id'), ('input_manifest_sha256'),
        ('node_id'), ('request_sha'), ('request_json'), ('status'), ('owner_id'), ('fencing_token'),
        ('lease_expires_at'), ('row_version'), ('created_at'), ('updated_at')
    ) AS required(column_name)
    WHERE NOT EXISTS (
        SELECT 1
        FROM information_schema.columns actual
        WHERE actual.table_schema = 'qe_archive'
          AND actual.table_name = 'run_evaluation'
          AND actual.column_name = required.column_name
    );
    IF missing_columns IS NOT NULL THEN
        RAISE EXCEPTION 'existing qe_archive.run_evaluation schema is incomplete: %', missing_columns;
    END IF;

    SELECT count(*) INTO invalid_rows
    FROM qe_archive.run_evaluation
    WHERE evaluation_type <> 'long_trend'
       OR row_version < 1
       OR fencing_token < 0
       OR parent_task_id IS NULL
       OR parent_loop_index < 1;
    IF invalid_rows > 0 THEN
        RAISE EXCEPTION 'qe_archive.run_evaluation contains % rows violating the Phase 2 contract', invalid_rows;
    END IF;
END $$;

SELECT
    to_regclass('qe_archive.run_evaluation') AS table_name,
    obj_description(to_regclass('qe_archive.run_evaluation'), 'pg_class') AS table_comment;

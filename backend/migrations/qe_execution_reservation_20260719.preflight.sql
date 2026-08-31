-- Read-only preflight for qe_execution_reservation_20260719.sql.
-- This file performs no schema/data writes and does not create a database export.

DO $preflight$
DECLARE
    required_column RECORD;
    existing_type TEXT;
BEGIN
    IF to_regclass('infra.compute_nodes') IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'qe_execution_reservation_compute_nodes_missing';
    END IF;

    SELECT c.data_type INTO existing_type
    FROM information_schema.columns AS c
    WHERE c.table_schema = 'infra'
      AND c.table_name = 'compute_nodes'
      AND c.column_name = 'node_id';
    IF existing_type IS DISTINCT FROM 'text' THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'qe_execution_reservation_compute_node_identity_invalid',
            DETAIL = format('infra.compute_nodes.node_id expected=text actual=%s', existing_type);
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS con
        WHERE con.conrelid = 'infra.compute_nodes'::regclass
          AND con.contype IN ('p', 'u')
          AND con.conkey = ARRAY[
              (
                  SELECT attnum
                  FROM pg_attribute
                  WHERE attrelid = 'infra.compute_nodes'::regclass
                    AND attname = 'node_id'
                    AND NOT attisdropped
              )
          ]::smallint[]
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'qe_execution_reservation_compute_node_identity_not_unique';
    END IF;

    IF to_regclass('infra.qe_execution_reservation') IS NOT NULL THEN
        FOR required_column IN
            SELECT *
            FROM (VALUES
                ('reservation_id', 'text'),
                ('node_id', 'text'),
                ('source_kind', 'text'),
                ('source_execution_id', 'text'),
                ('qe_task_id', 'text'),
                ('qe_loop_id', 'text'),
                ('submission_intent_hash', 'text'),
                ('status', 'text'),
                ('remote_status', 'text'),
                ('release_reason_code', 'text'),
                ('owner_id', 'text'),
                ('lease_expires_at', 'timestamp with time zone'),
                ('fencing_token', 'bigint'),
                ('row_version', 'bigint'),
                ('reserved_at', 'timestamp with time zone'),
                ('heartbeat_at', 'timestamp with time zone'),
                ('released_at', 'timestamp with time zone'),
                ('created_at', 'timestamp with time zone'),
                ('updated_at', 'timestamp with time zone')
            ) AS expected(column_name, data_type)
        LOOP
            SELECT c.data_type INTO existing_type
            FROM information_schema.columns AS c
            WHERE c.table_schema = 'infra'
              AND c.table_name = 'qe_execution_reservation'
              AND c.column_name = required_column.column_name;
            IF existing_type IS NULL THEN
                RAISE EXCEPTION USING
                    ERRCODE = 'P0001',
                    MESSAGE = 'qe_execution_reservation_partial_table_missing_column',
                    DETAIL = required_column.column_name;
            END IF;
            IF existing_type <> required_column.data_type THEN
                RAISE EXCEPTION USING
                    ERRCODE = 'P0001',
                    MESSAGE = 'qe_execution_reservation_column_type_mismatch',
                    DETAIL = format('%I expected=%s actual=%s', required_column.column_name,
                                    required_column.data_type, existing_type);
            END IF;
        END LOOP;

        IF EXISTS (
            SELECT 1
            FROM (VALUES
                ('qe_execution_reservation_pkey'),
                ('fk_qeer_compute_node'),
                ('uq_qeer_source_execution'),
                ('ck_qeer_reservation_id'),
                ('ck_qeer_source_kind'),
                ('ck_qeer_nonempty_identity'),
                ('ck_qeer_submission_hash'),
                ('ck_qeer_status'),
                ('ck_qeer_versions'),
                ('ck_qeer_ownership'),
                ('ck_qeer_release_state')
            ) AS expected(conname)
            WHERE NOT EXISTS (
                SELECT 1
                FROM pg_constraint AS actual
                WHERE actual.conrelid = 'infra.qe_execution_reservation'::regclass
                  AND actual.conname = expected.conname
            )
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = 'P0001',
                MESSAGE = 'qe_execution_reservation_required_constraint_missing';
        END IF;

        IF EXISTS (
            SELECT 1
            FROM (VALUES
                ('uq_qeer_remote_identity_active'),
                ('idx_qeer_node_active'),
                ('idx_qeer_recoverable')
            ) AS expected(indexname)
            WHERE NOT EXISTS (
                SELECT 1
                FROM pg_indexes AS actual
                WHERE actual.schemaname = 'infra'
                  AND actual.tablename = 'qe_execution_reservation'
                  AND actual.indexname = expected.indexname
            )
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = 'P0001',
                MESSAGE = 'qe_execution_reservation_required_index_missing';
        END IF;

        IF obj_description('infra.qe_execution_reservation'::regclass, 'pg_class') IS NULL THEN
            RAISE EXCEPTION USING
                ERRCODE = 'P0001',
                MESSAGE = 'qe_execution_reservation_table_comment_missing';
        END IF;

        IF EXISTS (
            SELECT 1
            FROM information_schema.columns AS c
            WHERE c.table_schema = 'infra'
              AND c.table_name = 'qe_execution_reservation'
              AND col_description('infra.qe_execution_reservation'::regclass, c.ordinal_position) IS NULL
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = 'P0001',
                MESSAGE = 'qe_execution_reservation_column_comment_missing';
        END IF;

        IF EXISTS (
            SELECT 1
            FROM infra.qe_execution_reservation AS reservation
            LEFT JOIN infra.compute_nodes AS node ON node.node_id = reservation.node_id
            WHERE node.node_id IS NULL
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = 'P0001',
                MESSAGE = 'qe_execution_reservation_orphan_node_reference';
        END IF;
    END IF;
END
$preflight$;

SELECT
    'qe_execution_reservation_20260719' AS migration,
    'ready' AS preflight_status,
    to_regclass('infra.qe_execution_reservation') IS NOT NULL AS reservation_table_present;

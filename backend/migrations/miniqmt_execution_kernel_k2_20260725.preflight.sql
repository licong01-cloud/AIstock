-- K2-A read-only preflight. This script must never repair or mutate catalog/data.
BEGIN;
SET TRANSACTION READ ONLY;

DO $$
DECLARE
    required_table TEXT;
    installed_target_count BIGINT;
    actual_catalog_sha256 TEXT;
BEGIN
    FOREACH required_table IN ARRAY ARRAY[
        'qmt_strategy.execution_runtime',
        'qmt_strategy.execution_runtime_event',
        'qmt_strategy.execution_algo_instance',
        'qmt_strategy.execution_child_order'
    ] LOOP
        IF to_regclass(required_table) IS NULL THEN
            RAISE EXCEPTION 'K2 preflight missing required table: %', required_table;
        END IF;
    END LOOP;

    IF EXISTS (
        SELECT 1 FROM qmt_strategy.execution_runtime_event
        GROUP BY runtime_id, sequence HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'K2 preflight duplicate runtime sequence';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM qmt_strategy.execution_child_order AS child
        LEFT JOIN qmt_strategy.execution_algo_instance AS algo
          ON algo.algo_instance_id=child.algo_instance_id AND algo.runtime_id=child.runtime_id
        WHERE algo.algo_instance_id IS NULL
    ) THEN
        RAISE EXCEPTION 'K2 preflight cross-owner child row';
    END IF;

    SELECT COUNT(*) INTO installed_target_count
    FROM unnest(ARRAY[
        'qmt_strategy.execution_algo_event_delivery',
        'qmt_strategy.execution_algo_transition',
        'qmt_strategy.execution_algo_command_outbox',
        'qmt_strategy.execution_algo_command_dispatch_attempt',
        'qmt_strategy.execution_algo_timer_schedule',
        'qmt_strategy.execution_algo_timer_occurrence',
        'qmt_strategy.execution_kernel_worker_epoch',
        'qmt_strategy.execution_kernel_worker_incarnation',
        'qmt_strategy.execution_exchange_session_authority',
        'qmt_strategy.execution_algo_diagnostic_observation'
    ]::TEXT[]) AS target_table
    WHERE to_regclass(target_table) IS NOT NULL;
    IF installed_target_count NOT IN (0, 10) THEN
        RAISE EXCEPTION 'K2 preflight partial target catalog: % of 10 tables exist', installed_target_count;
    END IF;

    IF installed_target_count = 10 AND EXISTS (
        SELECT 1
        FROM unnest(ARRAY[
            'ck_miniqmt_k2_algo_active_child_closure',
            'ck_miniqmt_k2_algo_status',
            'ck_miniqmt_k2_child_mapping_contract',
            'ck_miniqmt_k2_child_mapping_initial',
            'ck_miniqmt_k2_delivery_lease',
            'ck_miniqmt_k2_delivery_initial',
            'ck_miniqmt_k2_delivery_predecessor',
            'ck_miniqmt_k2_delivery_receipt_presence',
            'ck_miniqmt_k2_delivery_sequence',
            'ck_miniqmt_k2_delivery_status',
            'ck_miniqmt_k2_diagnostic_hash',
            'ck_miniqmt_k2_dispatch_attempt_counter',
            'ck_miniqmt_k2_dispatch_attempt_stage',
            'ck_miniqmt_k2_event_composite',
            'ck_miniqmt_k2_event_contract',
            'ck_miniqmt_k2_exchange_session_authority_hash',
            'ck_miniqmt_k2_outbox_broker_called',
            'ck_miniqmt_k2_outbox_initial',
            'ck_miniqmt_k2_outbox_counter',
            'ck_miniqmt_k2_outbox_hash',
            'ck_miniqmt_k2_outbox_lease',
            'ck_miniqmt_k2_outbox_status',
            'ck_miniqmt_k2_timer_occurrence_lease',
            'ck_miniqmt_k2_timer_occurrence_initial',
            'ck_miniqmt_k2_timer_occurrence_receipt',
            'ck_miniqmt_k2_timer_occurrence_status',
            'ck_miniqmt_k2_timer_occurrence_version',
            'ck_miniqmt_k2_timer_schedule_lease',
            'ck_miniqmt_k2_timer_schedule_initial',
            'ck_miniqmt_k2_timer_schedule_receipt',
            'ck_miniqmt_k2_timer_schedule_status',
            'ck_miniqmt_k2_timer_schedule_version',
            'ck_miniqmt_k2_transition_hash',
            'ck_miniqmt_k2_transition_kind',
            'ck_miniqmt_k2_transition_receipt_presence',
            'ck_miniqmt_k2_worker_epoch_identity',
            'ck_miniqmt_k2_worker_epoch_sequence',
            'ck_miniqmt_k2_worker_incarnation_hash',
            'ck_miniqmt_k2_worker_incarnation_sequence',
            'fk_miniqmt_k2_child_algo_owner',
            'fk_miniqmt_k2_child_transition_owner',
            'fk_miniqmt_k2_delivery_algo_owner',
            'fk_miniqmt_k2_delivery_event_owner',
            'fk_miniqmt_k2_delivery_incarnation',
            'fk_miniqmt_k2_delivery_predecessor',
            'fk_miniqmt_k2_diagnostic_event_owner',
            'fk_miniqmt_k2_diagnostic_transition_owner',
            'fk_miniqmt_k2_dispatch_attempt_command',
            'fk_miniqmt_k2_dispatch_attempt_incarnation',
            'fk_miniqmt_k2_exchange_session_runtime',
            'fk_miniqmt_k2_outbox_algo_owner',
            'fk_miniqmt_k2_outbox_incarnation',
            'fk_miniqmt_k2_outbox_mapping',
            'fk_miniqmt_k2_outbox_transition_owner',
            'fk_miniqmt_k2_timer_occurrence_incarnation',
            'fk_miniqmt_k2_timer_occurrence_schedule',
            'fk_miniqmt_k2_timer_schedule_algo_owner',
            'fk_miniqmt_k2_timer_schedule_incarnation',
            'fk_miniqmt_k2_transition_delivery_owner',
            'fk_miniqmt_k2_worker_incarnation_epoch',
            'pk_miniqmt_k2_dispatch_attempt',
            'pk_miniqmt_k2_exchange_session_authority',
            'pk_miniqmt_k2_worker_epoch',
            'pk_miniqmt_k2_worker_incarnation',
            'uq_miniqmt_k2_algo_owner',
            'uq_miniqmt_k2_algo_runtime_identity',
            'uq_miniqmt_k2_child_mapping_id',
            'uq_miniqmt_k2_child_mapping_pair',
            'uq_miniqmt_k2_delivery_owner',
            'uq_miniqmt_k2_delivery_predecessor_target',
            'uq_miniqmt_k2_delivery_reference',
            'uq_miniqmt_k2_delivery_sequence',
            'uq_miniqmt_k2_event_owner',
            'uq_miniqmt_k2_exchange_session_authority_hash',
            'uq_miniqmt_k2_outbox_mapping',
            'uq_miniqmt_k2_outbox_transition_ordinal',
            'uq_miniqmt_k2_projection_owner',
            'uq_miniqmt_k2_timer_schedule_identity',
            'uq_miniqmt_k2_timer_schedule_occurrence',
            'uq_miniqmt_k2_transition_owner',
            'uq_miniqmt_k2_transition_reference',
            'uq_miniqmt_k2_worker_incarnation_owner',
            'uq_miniqmt_k2_worker_incarnation_sequence'
        ]::TEXT[]) AS expected_constraint(conname)
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_constraint AS actual_constraint
            JOIN pg_class AS target_table ON target_table.oid = actual_constraint.conrelid
            JOIN pg_namespace AS target_schema ON target_schema.oid = target_table.relnamespace
            WHERE target_schema.nspname = 'qmt_strategy'
              AND actual_constraint.conname = expected_constraint.conname
        )
    ) THEN
        RAISE EXCEPTION 'K2 preflight partial target constraint catalog';
    END IF;

    IF installed_target_count = 10 AND EXISTS (
        SELECT 1
        FROM unnest(ARRAY[
            'qmt_strategy.uq_miniqmt_k2_event_key',
            'qmt_strategy.uq_miniqmt_k2_event_owner',
            'qmt_strategy.uq_miniqmt_k2_algo_runtime_identity',
            'qmt_strategy.uq_miniqmt_k2_algo_owner',
            'qmt_strategy.uq_miniqmt_k2_child_mapping',
            'qmt_strategy.uq_miniqmt_k2_child_client_ref',
            'qmt_strategy.uq_miniqmt_k2_child_broker_order',
            'qmt_strategy.uq_miniqmt_k2_child_mapping_pair',
            'qmt_strategy.uq_miniqmt_k2_child_mapping_id'
        ]::TEXT[]) AS expected_index(index_name)
        WHERE to_regclass(expected_index.index_name) IS NULL
    ) THEN
        RAISE EXCEPTION 'K2 preflight partial target index catalog';
    END IF;

    IF installed_target_count = 10 THEN
        IF to_regprocedure('qmt_strategy.miniqmt_k2_catalog_fingerprint()') IS NULL THEN
            RAISE EXCEPTION 'K2 preflight missing schema catalog fingerprint authority';
        END IF;
        IF NOT EXISTS (
            SELECT 1
            FROM pg_proc AS function_record
            JOIN pg_namespace AS function_schema ON function_schema.oid=function_record.pronamespace
            JOIN pg_language AS function_language ON function_language.oid=function_record.prolang
            WHERE function_record.oid=to_regprocedure('qmt_strategy.miniqmt_k2_catalog_fingerprint()')
              AND function_language.lanname='sql'
              AND function_record.provolatile='s'
              AND pg_get_function_arguments(function_record.oid)=''
              AND pg_get_function_result(function_record.oid)='text'
              AND encode(
                    sha256(
                        convert_to(
                            btrim(
                                replace(function_record.prosrc,function_schema.nspname,'<schema>'),
                                E' \n\r\t;'
                            ),
                            'UTF8'
                        )
                    ),
                    'hex'
                  )='8d9c8b09b5c27a0b0caeeaea3663556b9876b0eea179057d691bbf2fce29c107'
        ) THEN
            RAISE EXCEPTION 'K2 preflight catalog function drift';
        END IF;
        WITH target_tables(relname) AS (
            VALUES
                ('execution_kernel_worker_epoch'),
                ('execution_kernel_worker_incarnation'),
                ('execution_algo_event_delivery'),
                ('execution_algo_transition'),
                ('execution_algo_command_outbox'),
                ('execution_algo_command_dispatch_attempt'),
                ('execution_algo_timer_schedule'),
                ('execution_algo_timer_occurrence'),
                ('execution_exchange_session_authority'),
                ('execution_algo_diagnostic_observation')
        ), additive_columns(relname,attname) AS (
            VALUES
                ('execution_runtime','runtime_id'),
                ('execution_runtime','trade_date'),
                ('execution_runtime_event','event_contract_version'),
                ('execution_runtime_event','event_schema_version'),
                ('execution_runtime_event','payload_schema_version'),
                ('execution_runtime_event','event_key_sha256'),
                ('execution_runtime_event','payload_sha256'),
                ('execution_runtime_event','observed_at_utc'),
                ('execution_runtime_event','logical_at_utc'),
                ('execution_runtime_event','source_identity_json'),
                ('execution_runtime_event','correlation_json'),
                ('execution_runtime_event','ingress_receipt_json'),
                ('execution_runtime_event','ingress_receipt_sha256'),
                ('execution_runtime_event','routing_rule_version'),
                ('execution_runtime_event','transaction_commit_identity'),
                ('execution_algo_instance','kernel_contract_version'),
                ('execution_algo_instance','traded_quantity'),
                ('execution_algo_instance','plugin_id'),
                ('execution_algo_instance','plugin_version'),
                ('execution_algo_instance','plugin_manifest_sha256'),
                ('execution_algo_instance','plugin_config_json'),
                ('execution_algo_instance','plugin_config_sha256'),
                ('execution_algo_instance','compatibility_receipt_sha256'),
                ('execution_algo_instance','state_schema_version'),
                ('execution_algo_instance','state_json'),
                ('execution_algo_instance','state_sha256'),
                ('execution_algo_instance','transition_sequence'),
                ('execution_algo_instance','last_applied_delivery_sequence'),
                ('execution_algo_instance','last_applied_delivery_id'),
                ('execution_algo_instance','last_closed_delivery_sequence'),
                ('execution_algo_instance','terminal_delivery_sequence'),
                ('execution_algo_instance','failure_receipt_id'),
                ('execution_algo_instance','active_child_closure_status'),
                ('execution_algo_instance','active_child_count'),
                ('execution_algo_instance','row_version'),
                ('execution_algo_instance','terminal_at_utc'),
                ('execution_algo_instance','kernel_carrier_json'),
                ('execution_child_order','kernel_contract_version'),
                ('execution_child_order','mapping_id'),
                ('execution_child_order','command_id'),
                ('execution_child_order','local_vt_orderid'),
                ('execution_child_order','deterministic_client_order_ref'),
                ('execution_child_order','order_remark'),
                ('execution_child_order','mapping_status'),
                ('execution_child_order','mapping_version'),
                ('execution_child_order','mapping_payload_sha256'),
                ('execution_child_order','mapping_receipt_sha256'),
                ('execution_child_order','broker_identity_source_event_id'),
                ('execution_child_order','last_order_event_id'),
                ('execution_child_order','last_trade_event_id'),
                ('execution_child_order','created_transition_id'),
                ('execution_child_order','updated_by_event_id'),
                ('execution_child_order','mapping_created_at_utc'),
                ('execution_child_order','mapping_updated_at_utc'),
                ('execution_child_order','mapping_json')
        ), catalog_items(sort_key,item) AS (
            SELECT
                format('column:%s:%05s', table_class.relname, attribute.attnum),
                jsonb_build_array(
                    'column', table_class.relname, attribute.attname,
                    format_type(attribute.atttypid, attribute.atttypmod),
                    attribute.attnotnull,
                    coalesce(pg_get_expr(attribute_default.adbin, attribute_default.adrelid), '')
                )
            FROM pg_class AS table_class
            JOIN pg_namespace AS table_schema ON table_schema.oid=table_class.relnamespace
            JOIN pg_attribute AS attribute
              ON attribute.attrelid=table_class.oid AND attribute.attnum > 0 AND NOT attribute.attisdropped
            LEFT JOIN pg_attrdef AS attribute_default
              ON attribute_default.adrelid=table_class.oid AND attribute_default.adnum=attribute.attnum
            WHERE table_schema.nspname='qmt_strategy'
              AND (
                  table_class.relname IN (SELECT relname FROM target_tables)
                  OR (table_class.relname,attribute.attname) IN (SELECT relname,attname FROM additive_columns)
              )

            UNION ALL

            SELECT
                format('constraint:%s:%s', table_class.relname, constraint_record.conname),
                jsonb_build_array(
                    'constraint', table_class.relname, constraint_record.conname,
                    constraint_record.contype, constraint_record.condeferrable,
                    constraint_record.condeferred, constraint_record.convalidated,
                    replace(
                        pg_get_constraintdef(constraint_record.oid, true),
                        table_schema.nspname || '.', '<schema>.'
                    )
                )
            FROM pg_constraint AS constraint_record
            JOIN pg_class AS table_class ON table_class.oid=constraint_record.conrelid
            JOIN pg_namespace AS table_schema ON table_schema.oid=table_class.relnamespace
            WHERE table_schema.nspname='qmt_strategy'
              AND (
                  table_class.relname IN (SELECT relname FROM target_tables)
                  OR constraint_record.conname LIKE '%miniqmt_k2%'
              )

            UNION ALL

            SELECT
                format('index:%s:%s', table_class.relname, index_class.relname),
                jsonb_build_array(
                    'index', table_class.relname, index_class.relname,
                    index_record.indisunique, index_record.indisprimary,
                    index_record.indisvalid, index_record.indisready,
                    replace(
                        pg_get_indexdef(index_record.indexrelid, 0, true),
                        table_schema.nspname || '.', '<schema>.'
                    ),
                    coalesce(
                        replace(
                            pg_get_expr(index_record.indpred, index_record.indrelid, true),
                            table_schema.nspname || '.', '<schema>.'
                        ),
                        ''
                    )
                )
            FROM pg_index AS index_record
            JOIN pg_class AS table_class ON table_class.oid=index_record.indrelid
            JOIN pg_class AS index_class ON index_class.oid=index_record.indexrelid
            JOIN pg_namespace AS table_schema ON table_schema.oid=table_class.relnamespace
            WHERE table_schema.nspname='qmt_strategy'
              AND (
                  table_class.relname IN (SELECT relname FROM target_tables)
                  OR index_class.relname LIKE '%miniqmt_k2%'
              )
        ), canonical_catalog AS (
            SELECT coalesce(jsonb_agg(item ORDER BY sort_key), '[]'::jsonb)::TEXT AS payload
            FROM catalog_items
        )
        SELECT encode(sha256(convert_to(payload, 'UTF8')), 'hex')
        INTO actual_catalog_sha256
        FROM canonical_catalog;
        IF actual_catalog_sha256 <> '6e4fc4ae4c6e403d3316c124da6ae5933eb33184129569fd6bf1cf750e27f762' THEN
            RAISE EXCEPTION 'K2 preflight exact schema catalog drift';
        END IF;
    END IF;
END $$;

SELECT
    'miniqmt_execution_kernel_k2_20260725'::TEXT AS migration_id,
    'edd5bac639c001a254842a96594f7ab93c2fcf92'::TEXT AS design_source_revision,
    '24b4e1894f93f1383d7690ff145c55e100a26cecfc9e60a9070b71a57524d083'::TEXT AS expected_migration_sha256,
    COUNT(*) FILTER (WHERE btrim(event_id) = '' OR btrim(runtime_id) = '') AS legacy_invalid_row_count,
    COUNT(*) AS legacy_event_count,
    COUNT(DISTINCT event_type) AS legacy_event_type_count,
    COUNT(DISTINCT source) AS legacy_event_source_count
FROM qmt_strategy.execution_runtime_event;

SELECT runtime_id, sequence, COUNT(*) AS duplicate_count
FROM qmt_strategy.execution_runtime_event
GROUP BY runtime_id, sequence
HAVING COUNT(*) > 1;

SELECT child.child_order_id, child.runtime_id, child.algo_instance_id
FROM qmt_strategy.execution_child_order AS child
LEFT JOIN qmt_strategy.execution_algo_instance AS algo
  ON algo.algo_instance_id = child.algo_instance_id
 AND algo.runtime_id = child.runtime_id
WHERE algo.algo_instance_id IS NULL;

SELECT
    COUNT(*) FILTER (WHERE archived_at IS NULL AND status IN ('ACTIVE', 'PAUSED')) AS legacy_active_algo_count,
    COUNT(*) FILTER (WHERE archived_at IS NULL) AS legacy_algo_count
FROM qmt_strategy.execution_algo_instance;

SELECT
    COUNT(*) FILTER (WHERE archived_at IS NULL AND status IN ('SUBMITTING', 'SUBMITTED', 'PARTIALLY_FILLED'))
        AS legacy_open_child_count,
    COUNT(*) FILTER (WHERE archived_at IS NULL) AS legacy_child_count
FROM qmt_strategy.execution_child_order;

SELECT
    to_regclass('qmt_strategy.execution_algo_event_delivery') AS delivery_table,
    to_regclass('qmt_strategy.execution_algo_transition') AS transition_table,
    to_regclass('qmt_strategy.execution_algo_command_outbox') AS outbox_table,
    to_regclass('qmt_strategy.execution_algo_timer_schedule') AS timer_schedule_table,
    to_regclass('qmt_strategy.execution_kernel_worker_incarnation') AS worker_incarnation_table;

COMMIT;

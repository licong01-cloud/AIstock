-- K2-A read-only preflight. This script must never repair or mutate catalog/data.
BEGIN;
SET TRANSACTION READ ONLY;

DO $$
DECLARE
    required_table TEXT;
    installed_target_count BIGINT;
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
            'ck_miniqmt_k2_delivery_lease',
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
            'ck_miniqmt_k2_outbox_counter',
            'ck_miniqmt_k2_outbox_hash',
            'ck_miniqmt_k2_outbox_lease',
            'ck_miniqmt_k2_outbox_status',
            'ck_miniqmt_k2_timer_occurrence_lease',
            'ck_miniqmt_k2_timer_occurrence_receipt',
            'ck_miniqmt_k2_timer_occurrence_status',
            'ck_miniqmt_k2_timer_occurrence_version',
            'ck_miniqmt_k2_timer_schedule_lease',
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
END $$;

SELECT
    'miniqmt_execution_kernel_k2_20260725'::TEXT AS migration_id,
    'edd5bac639c001a254842a96594f7ab93c2fcf92'::TEXT AS design_source_revision,
    'db0fa2140a11d795dcc903ac95ece64288645284ff68f495635adda5bdbc9e43'::TEXT AS expected_migration_sha256,
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

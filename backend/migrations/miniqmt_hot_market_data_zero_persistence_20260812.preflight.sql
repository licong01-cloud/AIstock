-- BUG-1041 read-only preflight. This script never changes database state.
BEGIN;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;
SET LOCAL statement_timeout = '30s';
SET LOCAL search_path = pg_catalog, qmt_strategy, pg_temp;
DO $$
DECLARE
    table_oid OID := to_regclass('qmt_strategy.execution_runtime_event');
    missing_columns TEXT[];
    unsupported_rows BIGINT;
    active_v3_count BIGINT;
    base_names TEXT[];
    validated_count INTEGER;
BEGIN
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'BUG-1041 preflight: execution_runtime_event is absent';
    END IF;
    SELECT array_agg(required.name ORDER BY required.name) INTO missing_columns
      FROM (VALUES
        ('event_id'),('runtime_id'),('sequence'),('event_type'),('source'),
        ('event_contract_version'),('payload_schema_version')
      ) AS required(name)
     WHERE NOT EXISTS (
        SELECT 1 FROM pg_attribute
         WHERE attrelid=table_oid AND attname=required.name AND attnum>0 AND NOT attisdropped
     );
    IF missing_columns IS NOT NULL THEN
        RAISE EXCEPTION 'BUG-1041 preflight: missing columns %',missing_columns;
    END IF;
    SELECT array_agg(conname ORDER BY conname COLLATE "C"),count(*) FILTER (WHERE convalidated)
      INTO base_names,validated_count
      FROM pg_constraint
     WHERE conrelid=table_oid AND contype='c' AND conname<>'ck_miniqmt_no_new_kernel_tick';
    IF base_names<>ARRAY[
        'ck_miniqmt_event_id','ck_miniqmt_event_sequence','ck_miniqmt_event_source',
        'ck_miniqmt_event_type','ck_miniqmt_k2_event_composite','ck_miniqmt_k2_event_contract'
       ]::TEXT[] OR validated_count<>6 THEN
        RAISE EXCEPTION 'BUG-1041 preflight: apply the exact BUG-1019 event-contract repair first';
    END IF;
    -- Historical KERNEL_V2 TICK rows are explicitly preserved read-only.
    SELECT count(*) INTO unsupported_rows
      FROM qmt_strategy.execution_runtime_event
     WHERE event_contract_version='KERNEL_V2' AND event_type<>'TICK'
       AND (event_type,source,payload_schema_version) NOT IN (
        ('ALGO_START','MINIQMT_EXECUTION_KERNEL','miniqmt_algo_start_v1'),
        ('ALGO_START','MINIQMT_EXECUTION_KERNEL','miniqmt_algo_start_v2'),
        ('COMMAND_OUTCOME','MINIQMT_EXECUTION_KERNEL','miniqmt_command_outcome_v1'),
        ('TIMER','EXCHANGE_SESSION_CLOCK','miniqmt_timer_due_v1'),
        ('SESSION','EXCHANGE_SESSION_CLOCK','miniqmt_session_event_v1'),
        ('EOD','EXCHANGE_SESSION_CLOCK','miniqmt_eod_event_v1'),
        ('ORDER','QMT_GATEWAY_CALLBACK','miniqmt_order_event_v1'),
        ('TRADE','QMT_GATEWAY_CALLBACK','miniqmt_trade_fact_v1'),
        ('ACCOUNT','QMT_OMS_PROJECTION','miniqmt_account_projection_v1'),
        ('RECONCILE','QMT_OMS_RECONCILIATION','miniqmt_reconciliation_receipt_v1'),
        ('OPERATOR','SIMULATION_RUNTIME_OPERATOR','miniqmt_operator_command_v1')
       );
    IF unsupported_rows<>0 THEN
        RAISE EXCEPTION 'BUG-1041 preflight: % non-TICK KERNEL_V2 rows violate the successor contract',unsupported_rows;
    END IF;
    SELECT count(*) INTO active_v3_count
      FROM qmt_strategy.execution_algo_instance
     WHERE kernel_contract_version='KERNEL_V2'
       AND status IN ('INITIALIZING','ACTIVE','PAUSED')
       AND plugin_version<>'4.0.0';
    IF active_v3_count<>0 THEN
        RAISE EXCEPTION 'BUG-1041 preflight: % active pre-V4 algorithm instances remain',active_v3_count;
    END IF;
END $$;
COMMIT;

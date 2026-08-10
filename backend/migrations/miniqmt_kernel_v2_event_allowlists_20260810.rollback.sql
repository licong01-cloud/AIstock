-- BUG-1004 guarded rollback: restore the P1-D quote allowlists and original
-- K2 composite only when no KERNEL_V2-only durable fact would be invalidated.

BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DO $$
DECLARE
    table_oid oid := to_regclass('qmt_strategy.execution_runtime_event');
    kernel_only_count bigint;
    min_sequence integer;
    max_sequence integer;
    type_def text;
    source_def text;
    composite_def text;
    type_values text[];
    source_values text[];
    type_validated boolean;
    source_validated boolean;
    composite_validated boolean;
    normalized_composite text;
    normalized_old_composite text;
    normalized_target_composite text;
    type_shape text;
    source_shape text;
    quote_types constant text[] := ARRAY[
        'ACCOUNT_EVENT','ALGO_ACTION_EMITTED','ALGO_INSTANCE_CREATED','BROKER_SYNCED','BROKER_SYNC_STARTED',
        'CHILD_ORDER_CANCEL_REQUESTED','CHILD_ORDER_REJECTED','CHILD_ORDER_SUBMITTED','GATEWAY_CONNECTED',
        'GATEWAY_DISCONNECTED','OPERATOR_COMMAND_EXECUTED','OPERATOR_COMMAND_RECEIVED','OPERATOR_COMMAND_REJECTED',
        'ORDER_EVENT','QUOTE_ELIGIBILITY_EVALUATED','QUOTE_INGRESS_HEALTH','QUOTE_MARK_CAPTURED','QUOTE_OBSERVED',
        'QUOTE_REJECTED','RECONCILE_COMPLETED','RECONCILE_STARTED','RISK_KILL_SWITCH_TRIGGERED','RUNTIME_CREATED',
        'RUNTIME_STOPPED','TICK','TIMER','TRADE_EVENT'
    ];
    quote_sources constant text[] := ARRAY['algo','gateway','oms','operator','quote_ingress','recovery','runtime'];
    target_types constant text[] := ARRAY[
        'ACCOUNT','ACCOUNT_EVENT','ALGO_ACTION_EMITTED','ALGO_INSTANCE_CREATED','ALGO_START','BROKER_SYNCED',
        'BROKER_SYNC_STARTED','CHILD_ORDER_CANCEL_REQUESTED','CHILD_ORDER_REJECTED','CHILD_ORDER_SUBMITTED',
        'COMMAND_OUTCOME','EOD','GATEWAY_CONNECTED','GATEWAY_DISCONNECTED','OPERATOR','OPERATOR_COMMAND_EXECUTED',
        'OPERATOR_COMMAND_RECEIVED','OPERATOR_COMMAND_REJECTED','ORDER','ORDER_EVENT','QUOTE_ELIGIBILITY_EVALUATED',
        'QUOTE_INGRESS_HEALTH','QUOTE_MARK_CAPTURED','QUOTE_OBSERVED','QUOTE_REJECTED','RECONCILE',
        'RECONCILE_COMPLETED','RECONCILE_STARTED','RISK_KILL_SWITCH_TRIGGERED','RUNTIME_CREATED','RUNTIME_STOPPED',
        'SESSION','TICK','TIMER','TRADE','TRADE_EVENT'
    ];
    target_sources constant text[] := ARRAY[
        'B0_QUOTE_V2','EXCHANGE_SESSION_CLOCK','MINIQMT_EXECUTION_KERNEL','QMT_GATEWAY_CALLBACK',
        'QMT_OMS_PROJECTION','QMT_OMS_RECONCILIATION','SIMULATION_RUNTIME_OPERATOR',
        'algo','gateway','oms','operator','quote_ingress','recovery','runtime'
    ];
    old_composite constant text := $old$CHECK (
        event_contract_version='LEGACY_V1' OR
        (event_type='ALGO_START' AND source='MINIQMT_EXECUTION_KERNEL' AND payload_schema_version='miniqmt_algo_start_v1') OR
        (event_type='TICK' AND source='B0_QUOTE_V2' AND payload_schema_version='miniqmt_market_data_view_v2') OR
        (event_type='TIMER' AND source='EXCHANGE_SESSION_CLOCK' AND payload_schema_version='miniqmt_timer_due_v1') OR
        (event_type='SESSION' AND source='EXCHANGE_SESSION_CLOCK' AND payload_schema_version='miniqmt_session_event_v1') OR
        (event_type='EOD' AND source='EXCHANGE_SESSION_CLOCK' AND payload_schema_version='miniqmt_eod_event_v1') OR
        (event_type='ORDER' AND source='QMT_GATEWAY_CALLBACK' AND payload_schema_version='miniqmt_order_event_v1') OR
        (event_type='TRADE' AND source='QMT_GATEWAY_CALLBACK' AND payload_schema_version='miniqmt_trade_fact_v1') OR
        (event_type='ACCOUNT' AND source='QMT_OMS_PROJECTION' AND payload_schema_version='miniqmt_account_projection_v1') OR
        (event_type='RECONCILE' AND source='QMT_OMS_RECONCILIATION' AND payload_schema_version='miniqmt_reconciliation_receipt_v1') OR
        (event_type='OPERATOR' AND source='SIMULATION_RUNTIME_OPERATOR' AND payload_schema_version='miniqmt_operator_command_v1')
    )$old$;
    target_composite constant text := $target$CHECK (
        event_contract_version='LEGACY_V1' OR
        (event_type='ALGO_START' AND source='MINIQMT_EXECUTION_KERNEL' AND payload_schema_version='miniqmt_algo_start_v1') OR
        (event_type='ALGO_START' AND source='MINIQMT_EXECUTION_KERNEL' AND payload_schema_version='miniqmt_algo_start_v2') OR
        (event_type='COMMAND_OUTCOME' AND source='MINIQMT_EXECUTION_KERNEL' AND payload_schema_version='miniqmt_command_outcome_v1') OR
        (event_type='TICK' AND source='B0_QUOTE_V2' AND payload_schema_version='miniqmt_market_data_view_v2') OR
        (event_type='TIMER' AND source='EXCHANGE_SESSION_CLOCK' AND payload_schema_version='miniqmt_timer_due_v1') OR
        (event_type='SESSION' AND source='EXCHANGE_SESSION_CLOCK' AND payload_schema_version='miniqmt_session_event_v1') OR
        (event_type='EOD' AND source='EXCHANGE_SESSION_CLOCK' AND payload_schema_version='miniqmt_eod_event_v1') OR
        (event_type='ORDER' AND source='QMT_GATEWAY_CALLBACK' AND payload_schema_version='miniqmt_order_event_v1') OR
        (event_type='TRADE' AND source='QMT_GATEWAY_CALLBACK' AND payload_schema_version='miniqmt_trade_fact_v1') OR
        (event_type='ACCOUNT' AND source='QMT_OMS_PROJECTION' AND payload_schema_version='miniqmt_account_projection_v1') OR
        (event_type='RECONCILE' AND source='QMT_OMS_RECONCILIATION' AND payload_schema_version='miniqmt_reconciliation_receipt_v1') OR
        (event_type='OPERATOR' AND source='SIMULATION_RUNTIME_OPERATOR' AND payload_schema_version='miniqmt_operator_command_v1')
    )$target$;
BEGIN
    IF table_oid IS NULL THEN RAISE EXCEPTION 'BUG-1004 rollback: execution_runtime_event does not exist'; END IF;
    LOCK TABLE qmt_strategy.execution_runtime_event IN SHARE ROW EXCLUSIVE MODE;
    SELECT pg_get_constraintdef(oid,true),convalidated INTO type_def,type_validated
      FROM pg_constraint WHERE conrelid=table_oid AND conname='ck_miniqmt_event_type' AND contype='c';
    SELECT pg_get_constraintdef(oid,true),convalidated INTO source_def,source_validated
      FROM pg_constraint WHERE conrelid=table_oid AND conname='ck_miniqmt_event_source' AND contype='c';
    SELECT pg_get_constraintdef(oid,true),convalidated INTO composite_def,composite_validated
      FROM pg_constraint WHERE conrelid=table_oid AND conname='ck_miniqmt_k2_event_composite' AND contype='c';
    IF type_def IS NULL OR source_def IS NULL OR composite_def IS NULL THEN
        RAISE EXCEPTION 'BUG-1004 rollback: all three named event CHECK constraints must exist exactly once';
    END IF;
    SELECT coalesce(array_agg(m[1] ORDER BY m[1]),ARRAY[]::text[]) INTO type_values
      FROM regexp_matches(type_def,'''([^'']+)''','g') AS m;
    SELECT coalesce(array_agg(m[1] ORDER BY m[1]),ARRAY[]::text[]) INTO source_values
      FROM regexp_matches(source_def,'''([^'']+)''','g') AS m;
    type_shape := regexp_replace(upper(type_def),'''[^'']+''','','g');
    type_shape := regexp_replace(type_shape,'::[A-Z_]+','','g');
    type_shape := regexp_replace(type_shape,'CHECK|EVENT_TYPE|IN|ANY|ARRAY|TEXT|VARCHAR|CHARACTER|[()\[\],=[:space:]]','','g');
    source_shape := regexp_replace(upper(source_def),'''[^'']+''','','g');
    source_shape := regexp_replace(source_shape,'::[A-Z_]+','','g');
    source_shape := regexp_replace(source_shape,'CHECK|SOURCE|IN|ANY|ARRAY|TEXT|VARCHAR|CHARACTER|[()\[\],=[:space:]]','','g');
    IF type_shape<>'' OR source_shape<>'' THEN
        RAISE EXCEPTION 'BUG-1004 rollback: event type/source CHECK expression drift';
    END IF;
    normalized_composite := regexp_replace(regexp_replace(upper(composite_def),'::TEXT','','g'),'[()[:space:]]','','g');
    normalized_old_composite := regexp_replace(regexp_replace(upper(old_composite),'::TEXT','','g'),'[()[:space:]]','','g');
    normalized_target_composite := regexp_replace(regexp_replace(upper(target_composite),'::TEXT','','g'),'[()[:space:]]','','g');
    IF type_values=quote_types AND source_values=quote_sources AND normalized_composite=normalized_old_composite THEN
        IF NOT type_validated THEN ALTER TABLE qmt_strategy.execution_runtime_event VALIDATE CONSTRAINT ck_miniqmt_event_type; END IF;
        IF NOT source_validated THEN ALTER TABLE qmt_strategy.execution_runtime_event VALIDATE CONSTRAINT ck_miniqmt_event_source; END IF;
        IF NOT composite_validated THEN ALTER TABLE qmt_strategy.execution_runtime_event VALIDATE CONSTRAINT ck_miniqmt_k2_event_composite; END IF;
        RAISE NOTICE 'BUG-1004 rollback no-op: exact P1-D/K2 predecessor CHECK authority already present';
        RETURN;
    END IF;
    IF type_values<>target_types OR source_values<>target_sources OR normalized_composite<>normalized_target_composite THEN
        RAISE EXCEPTION 'BUG-1004 rollback: current event CHECK authority is not the exact migration target';
    END IF;
    SELECT count(*),min(sequence),max(sequence)
      INTO kernel_only_count,min_sequence,max_sequence
      FROM qmt_strategy.execution_runtime_event
     WHERE event_type<>ALL(quote_types) OR source<>ALL(quote_sources);
    IF kernel_only_count>0 THEN
        RAISE EXCEPTION 'BUG-1004 rollback refused: kernel_only_count=%, min_sequence=%, max_sequence=%',
            kernel_only_count,min_sequence,max_sequence;
    END IF;

    ALTER TABLE qmt_strategy.execution_runtime_event
        DROP CONSTRAINT ck_miniqmt_event_type,
        DROP CONSTRAINT ck_miniqmt_event_source,
        DROP CONSTRAINT ck_miniqmt_k2_event_composite;
    ALTER TABLE qmt_strategy.execution_runtime_event ADD CONSTRAINT ck_miniqmt_event_type CHECK (event_type IN (
        'RUNTIME_CREATED','GATEWAY_CONNECTED','GATEWAY_DISCONNECTED','BROKER_SYNC_STARTED','BROKER_SYNCED',
        'ALGO_INSTANCE_CREATED','TIMER','TICK','ALGO_ACTION_EMITTED','CHILD_ORDER_SUBMITTED','CHILD_ORDER_REJECTED',
        'CHILD_ORDER_CANCEL_REQUESTED','ORDER_EVENT','TRADE_EVENT','ACCOUNT_EVENT','RISK_KILL_SWITCH_TRIGGERED',
        'RECONCILE_STARTED','RECONCILE_COMPLETED','OPERATOR_COMMAND_RECEIVED','OPERATOR_COMMAND_EXECUTED',
        'OPERATOR_COMMAND_REJECTED','RUNTIME_STOPPED','QUOTE_OBSERVED','QUOTE_REJECTED',
        'QUOTE_ELIGIBILITY_EVALUATED','QUOTE_MARK_CAPTURED','QUOTE_INGRESS_HEALTH'
    )) NOT VALID;
    ALTER TABLE qmt_strategy.execution_runtime_event ADD CONSTRAINT ck_miniqmt_event_source CHECK (source IN (
        'runtime','gateway','oms','algo','operator','recovery','quote_ingress'
    )) NOT VALID;
    ALTER TABLE qmt_strategy.execution_runtime_event ADD CONSTRAINT ck_miniqmt_k2_event_composite CHECK (
        event_contract_version='LEGACY_V1' OR
        (event_type='ALGO_START' AND source='MINIQMT_EXECUTION_KERNEL' AND payload_schema_version='miniqmt_algo_start_v1') OR
        (event_type='TICK' AND source='B0_QUOTE_V2' AND payload_schema_version='miniqmt_market_data_view_v2') OR
        (event_type='TIMER' AND source='EXCHANGE_SESSION_CLOCK' AND payload_schema_version='miniqmt_timer_due_v1') OR
        (event_type='SESSION' AND source='EXCHANGE_SESSION_CLOCK' AND payload_schema_version='miniqmt_session_event_v1') OR
        (event_type='EOD' AND source='EXCHANGE_SESSION_CLOCK' AND payload_schema_version='miniqmt_eod_event_v1') OR
        (event_type='ORDER' AND source='QMT_GATEWAY_CALLBACK' AND payload_schema_version='miniqmt_order_event_v1') OR
        (event_type='TRADE' AND source='QMT_GATEWAY_CALLBACK' AND payload_schema_version='miniqmt_trade_fact_v1') OR
        (event_type='ACCOUNT' AND source='QMT_OMS_PROJECTION' AND payload_schema_version='miniqmt_account_projection_v1') OR
        (event_type='RECONCILE' AND source='QMT_OMS_RECONCILIATION' AND payload_schema_version='miniqmt_reconciliation_receipt_v1') OR
        (event_type='OPERATOR' AND source='SIMULATION_RUNTIME_OPERATOR' AND payload_schema_version='miniqmt_operator_command_v1')
    ) NOT VALID;
    ALTER TABLE qmt_strategy.execution_runtime_event VALIDATE CONSTRAINT ck_miniqmt_event_type;
    ALTER TABLE qmt_strategy.execution_runtime_event VALIDATE CONSTRAINT ck_miniqmt_event_source;
    ALTER TABLE qmt_strategy.execution_runtime_event VALIDATE CONSTRAINT ck_miniqmt_k2_event_composite;

    SELECT pg_get_constraintdef(oid,true),convalidated INTO type_def,type_validated
      FROM pg_constraint WHERE conrelid=table_oid AND conname='ck_miniqmt_event_type' AND contype='c';
    SELECT pg_get_constraintdef(oid,true),convalidated INTO source_def,source_validated
      FROM pg_constraint WHERE conrelid=table_oid AND conname='ck_miniqmt_event_source' AND contype='c';
    SELECT pg_get_constraintdef(oid,true),convalidated INTO composite_def,composite_validated
      FROM pg_constraint WHERE conrelid=table_oid AND conname='ck_miniqmt_k2_event_composite' AND contype='c';
    SELECT coalesce(array_agg(m[1] ORDER BY m[1]),ARRAY[]::text[]) INTO type_values
      FROM regexp_matches(type_def,'''([^'']+)''','g') AS m;
    SELECT coalesce(array_agg(m[1] ORDER BY m[1]),ARRAY[]::text[]) INTO source_values
      FROM regexp_matches(source_def,'''([^'']+)''','g') AS m;
    normalized_composite := regexp_replace(regexp_replace(upper(composite_def),'::TEXT','','g'),'[()[:space:]]','','g');
    IF type_values<>quote_types OR source_values<>quote_sources OR normalized_composite<>normalized_old_composite
       OR NOT type_validated OR NOT source_validated OR NOT composite_validated THEN
        RAISE EXCEPTION 'BUG-1004 rollback: independent post-DDL predecessor readback drift';
    END IF;
END
$$;

COMMIT;

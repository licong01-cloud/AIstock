-- BUG-1019 guarded rollback to the immediate P1-D/K2 predecessor contract.
-- It never deletes, rewrites or hides a durable event.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
SET LOCAL search_path = pg_catalog, qmt_strategy, pg_temp;
LOCK TABLE qmt_strategy.execution_runtime_event IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE
    qmt_strategy.execution_runtime,
    qmt_strategy.execution_algo_instance,
    qmt_strategy.execution_child_order,
    qmt_strategy.execution_kernel_worker_epoch,
    qmt_strategy.execution_kernel_worker_incarnation,
    qmt_strategy.execution_algo_event_delivery,
    qmt_strategy.execution_algo_transition,
    qmt_strategy.execution_algo_command_outbox,
    qmt_strategy.execution_algo_command_dispatch_attempt,
    qmt_strategy.execution_algo_timer_schedule,
    qmt_strategy.execution_algo_timer_occurrence,
    qmt_strategy.execution_exchange_session_authority,
    qmt_strategy.execution_algo_diagnostic_observation,
    qmt_strategy.execution_broker_reconciliation_attempt,
    qmt_strategy.execution_dependent_buy_coordination,
    qmt_strategy.execution_dependent_buy_dependency,
    qmt_strategy.execution_dependent_buy_decision,
    qmt_strategy.execution_product_command_authority,
    qmt_strategy.execution_product_command_authority_item,
    qmt_strategy.execution_product_route_cutover,
    qmt_strategy.execution_product_route_owner
IN SHARE ROW EXCLUSIVE MODE;

DO $$
DECLARE
    table_oid OID := 'qmt_strategy.execution_runtime_event'::regclass;
    constraint_count INTEGER;
    validated_count INTEGER;
    type_def TEXT;
    source_def TEXT;
    composite_def TEXT;
    contract_def TEXT;
    constraint_names TEXT[];
    type_values TEXT[];
    source_values TEXT[];
    type_definition_sha256 TEXT;
    source_definition_sha256 TEXT;
    composite_definition_sha256 TEXT;
    contract_definition_sha256 TEXT;
    event_id_definition_sha256 TEXT;
    event_sequence_definition_sha256 TEXT;
    target_only_fact_count BIGINT;
    invalid_row_count BIGINT;
    state TEXT;
    function_body TEXT;
    function_schema TEXT;
    function_language TEXT;
    function_volatility "char";
    function_configuration TEXT[];
    function_arguments TEXT;
    function_result TEXT;
    function_body_sha256 TEXT;
    helper_catalog_sha256 TEXT;
    independent_catalog_sha256 TEXT;
    k2d_function_oid OID;
    k2d_function_body TEXT;
    k2d_function_schema TEXT;
    k2d_function_language TEXT;
    k2d_function_volatility "char";
    k2d_function_configuration TEXT[];
    k2d_function_arguments TEXT;
    k2d_function_result TEXT;
    k2d_function_body_sha256 TEXT;
    k2d_helper_catalog_sha256 TEXT;
    k2d_independent_catalog_sha256 TEXT;
    predecessor_catalog_sha256 CONSTANT TEXT := 'd0bf2e66443b46e60b9f931ac387b60cb84e2ce056c480e7c81aa2397a7ccdbe';
    target_catalog_sha256 CONSTANT TEXT := 'b5cceba58ef9646e441d1fcb346a47cd4648397ac4425a956d1b83b2fc81d473';
    k2d_catalog_sha256 CONSTANT TEXT := '2d5fcbf0151d9e5d2a9d8537f834aabfd056a42cc0eeb8c079add68c8964f59f';
    k2d_catalog_function_body_sha256 CONSTANT TEXT := '9e5236fdc17b79888c864871e71ed6613b12759bbe87e070bd5c1c1db0b95451';
    predecessor_catalog_function_body_sha256 CONSTANT TEXT := '8d9c8b09b5c27a0b0caeeaea3663556b9876b0eea179057d691bbf2fce29c107';
    target_catalog_function_body_sha256 CONSTANT TEXT := '81072c5de821a69e8ca3fab3bca63d8454beca0ebedb26a427f5117c1239e1ff';
    predecessor_type_sha256 CONSTANT TEXT := '148b6275debe87a7ebda2dc51385a6583a334f5a8dd6779e5124576758b4255e';
    predecessor_source_sha256 CONSTANT TEXT := '835ad788ea103d5f0e7cca878c810331a2f1b7fdb1377a554acefa30cd209697';
    predecessor_composite_sha256 CONSTANT TEXT := '907e964380874d06918981201685af0338bef13f034c7becd5e04a9a591b06b3';
    predecessor_contract_sha256 CONSTANT TEXT := '9d193860ed0de361ef590ba195b531c623afa09b42620f52e2c0938b9f6a1212';
    target_type_sha256 CONSTANT TEXT := '6ac3041d989166511127ec22d9379dd0ecdc09fb5055e72006100319026a6f24';
    target_source_sha256 CONSTANT TEXT := 'c2f8e672b140ec88f667e251bbb5ff812cd0bea2a24f31c45d74c3f8d32eb881';
    target_composite_sha256 CONSTANT TEXT := '4a2d33d3fc75a4b468661e1bdbf2ecce9cd13aaab491c7c4d7605a1df3af3857';
    target_contract_sha256 CONSTANT TEXT := '888bebaf7d9540ecadae15bfb7d2944db59177b4ed2ef5e8beb231b803f9faca';
    predecessor_event_id_sha256 CONSTANT TEXT := '55f2f3dd015fc42bed99754d426d434e62a3456295263bbbf42c3358d8257608';
    predecessor_event_sequence_sha256 CONSTANT TEXT := 'ddfd70c30577468691d352ae838281ec74c56efd9d5ec1c3e32967cf9ef5c6ed';
    target_event_id_sha256 CONSTANT TEXT := '836b7f7ebf14ee61ec94c9df82b300b42c96ff1046de0a2e0cfb8bc0f400642d';
    target_event_sequence_sha256 CONSTANT TEXT := 'a1b188a1431066f2e8f2d0d51107b8c0532830ca7b88567ba1903c4b3999a3d0';
    predecessor_constraint_names CONSTANT TEXT[] := ARRAY[
        'ck_miniqmt_event_id','ck_miniqmt_event_sequence','ck_miniqmt_event_source','ck_miniqmt_event_type',
        'ck_miniqmt_k2_event_composite','ck_miniqmt_k2_event_contract'
    ];
    target_constraint_names CONSTANT TEXT[] := ARRAY[
        'ck_miniqmt_event_id','ck_miniqmt_event_sequence','ck_miniqmt_event_source',
        'ck_miniqmt_event_type','ck_miniqmt_k2_event_composite','ck_miniqmt_k2_event_contract'
    ];
    predecessor_types CONSTANT TEXT[] := ARRAY[
        'ACCOUNT_EVENT','ALGO_ACTION_EMITTED','ALGO_INSTANCE_CREATED','BROKER_SYNCED','BROKER_SYNC_STARTED',
        'CHILD_ORDER_CANCEL_REQUESTED','CHILD_ORDER_REJECTED','CHILD_ORDER_SUBMITTED','GATEWAY_CONNECTED',
        'GATEWAY_DISCONNECTED','OPERATOR_COMMAND_EXECUTED','OPERATOR_COMMAND_RECEIVED',
        'OPERATOR_COMMAND_REJECTED','ORDER_EVENT','QUOTE_ELIGIBILITY_EVALUATED','QUOTE_INGRESS_HEALTH',
        'QUOTE_MARK_CAPTURED','QUOTE_OBSERVED','QUOTE_REJECTED','RECONCILE_COMPLETED','RECONCILE_STARTED',
        'RISK_KILL_SWITCH_TRIGGERED','RUNTIME_CREATED','RUNTIME_STOPPED','TICK','TIMER','TRADE_EVENT'
    ];
    target_types CONSTANT TEXT[] := ARRAY[
        'ACCOUNT','ACCOUNT_EVENT','ALGO_ACTION_EMITTED','ALGO_INSTANCE_CREATED','ALGO_START','BROKER_SYNCED',
        'BROKER_SYNC_STARTED','CHILD_ORDER_CANCEL_REQUESTED','CHILD_ORDER_REJECTED','CHILD_ORDER_SUBMITTED',
        'COMMAND_OUTCOME','EOD','GATEWAY_CONNECTED','GATEWAY_DISCONNECTED','OPERATOR',
        'OPERATOR_COMMAND_EXECUTED','OPERATOR_COMMAND_RECEIVED','OPERATOR_COMMAND_REJECTED','ORDER','ORDER_EVENT',
        'QUOTE_ELIGIBILITY_EVALUATED','QUOTE_INGRESS_HEALTH','QUOTE_MARK_CAPTURED','QUOTE_OBSERVED',
        'QUOTE_REJECTED','RECONCILE','RECONCILE_COMPLETED','RECONCILE_STARTED','RISK_KILL_SWITCH_TRIGGERED',
        'RUNTIME_CREATED','RUNTIME_STOPPED','SESSION','TICK','TIMER','TRADE','TRADE_EVENT'
    ];
    predecessor_sources CONSTANT TEXT[] := ARRAY['algo','gateway','oms','operator','quote_ingress','recovery','runtime'];
    target_sources CONSTANT TEXT[] := ARRAY[
        'B0_QUOTE_V2','EXCHANGE_SESSION_CLOCK','MINIQMT_EXECUTION_KERNEL','QMT_GATEWAY_CALLBACK',
        'QMT_OMS_PROJECTION','QMT_OMS_RECONCILIATION','SIMULATION_RUNTIME_OPERATOR',
        'algo','gateway','oms','operator','quote_ingress','recovery','runtime'
    ];
BEGIN
    SELECT count(*), count(*) FILTER (WHERE convalidated),
           array_agg(conname ORDER BY conname COLLATE "C")
      INTO constraint_count, validated_count, constraint_names
      FROM pg_constraint
     WHERE conrelid=table_oid AND contype='c';
    IF constraint_count<>validated_count
       OR constraint_names NOT IN (predecessor_constraint_names,target_constraint_names) THEN
        RAISE EXCEPTION 'BUG-1019 rollback preflight: exact validated predecessor/target CHECK names are required';
    END IF;
    SELECT pg_get_constraintdef(oid,true) INTO STRICT type_def FROM pg_constraint
     WHERE conrelid=table_oid AND conname='ck_miniqmt_event_type' AND contype='c';
    SELECT pg_get_constraintdef(oid,true) INTO STRICT source_def FROM pg_constraint
     WHERE conrelid=table_oid AND conname='ck_miniqmt_event_source' AND contype='c';
    SELECT pg_get_constraintdef(oid,true) INTO STRICT composite_def FROM pg_constraint
     WHERE conrelid=table_oid AND conname='ck_miniqmt_k2_event_composite' AND contype='c';
    SELECT pg_get_constraintdef(oid,true) INTO STRICT contract_def FROM pg_constraint
     WHERE conrelid=table_oid AND conname='ck_miniqmt_k2_event_contract' AND contype='c';
    type_definition_sha256 := encode(sha256(convert_to(type_def,'UTF8')),'hex');
    source_definition_sha256 := encode(sha256(convert_to(source_def,'UTF8')),'hex');
    composite_definition_sha256 := encode(sha256(convert_to(composite_def,'UTF8')),'hex');
    contract_definition_sha256 := encode(sha256(convert_to(contract_def,'UTF8')),'hex');
    SELECT encode(sha256(convert_to(pg_get_constraintdef(oid,true),'UTF8')),'hex')
      INTO STRICT event_id_definition_sha256 FROM pg_constraint
     WHERE conrelid=table_oid AND conname='ck_miniqmt_event_id' AND contype='c';
    SELECT encode(sha256(convert_to(pg_get_constraintdef(oid,true),'UTF8')),'hex')
      INTO STRICT event_sequence_definition_sha256 FROM pg_constraint
     WHERE conrelid=table_oid AND conname='ck_miniqmt_event_sequence' AND contype='c';
    SELECT coalesce(array_agg(match[1] ORDER BY match[1] COLLATE "C"),ARRAY[]::TEXT[]) INTO type_values
      FROM regexp_matches(type_def,'''([^'']+)''','g') AS match;
    SELECT coalesce(array_agg(match[1] ORDER BY match[1] COLLATE "C"),ARRAY[]::TEXT[]) INTO source_values
      FROM regexp_matches(source_def,'''([^'']+)''','g') AS match;
    IF type_values=predecessor_types AND source_values=predecessor_sources
       AND type_definition_sha256=predecessor_type_sha256
       AND source_definition_sha256=predecessor_source_sha256
       AND composite_definition_sha256=predecessor_composite_sha256
       AND contract_definition_sha256=predecessor_contract_sha256
       AND constraint_names=predecessor_constraint_names
       AND event_id_definition_sha256=predecessor_event_id_sha256
       AND event_sequence_definition_sha256=predecessor_event_sequence_sha256 THEN
        state := 'predecessor';
    ELSIF type_values=target_types AND source_values=target_sources
          AND type_definition_sha256=target_type_sha256
          AND source_definition_sha256=target_source_sha256
          AND composite_definition_sha256=target_composite_sha256
          AND contract_definition_sha256=target_contract_sha256
          AND event_id_definition_sha256=target_event_id_sha256
          AND event_sequence_definition_sha256=target_event_sequence_sha256
          AND constraint_names=target_constraint_names THEN
        state := 'target';
    ELSE
        RAISE EXCEPTION 'BUG-1019 rollback preflight: mixed or drifted predecessor/target definitions';
    END IF;

    SELECT function_record.prosrc,function_schema.nspname,function_language.lanname,
           function_record.provolatile,function_record.proconfig,pg_get_function_arguments(function_record.oid),
           pg_get_function_result(function_record.oid)
      INTO STRICT function_body,function_schema,function_language,function_volatility,function_configuration,
                  function_arguments,function_result
      FROM pg_proc AS function_record
      JOIN pg_namespace AS function_schema ON function_schema.oid=function_record.pronamespace
      JOIN pg_language AS function_language ON function_language.oid=function_record.prolang
     WHERE function_record.oid=to_regprocedure('qmt_strategy.miniqmt_k2_catalog_fingerprint()');
    function_body_sha256 := encode(
        sha256(convert_to(btrim(replace(function_body,function_schema,'<schema>'),E' \n\r\t;'),'UTF8')),
        'hex'
    );
    IF function_language<>'sql' OR function_volatility<>'s' OR function_arguments<>''
       OR function_result<>'text'
       OR (state='predecessor' AND function_configuration IS NOT NULL)
       OR (state='target' AND function_configuration IS DISTINCT FROM
           ARRAY['search_path=pg_catalog, qmt_strategy']::TEXT[])
       OR (state='predecessor' AND function_body_sha256<>predecessor_catalog_function_body_sha256)
       OR (state='target' AND function_body_sha256<>target_catalog_function_body_sha256) THEN
        RAISE EXCEPTION 'BUG-1019 rollback preflight: K2 catalog function definition drift';
    END IF;
    EXECUTE function_body INTO STRICT independent_catalog_sha256;
    SELECT qmt_strategy.miniqmt_k2_catalog_fingerprint() INTO STRICT helper_catalog_sha256;
    IF helper_catalog_sha256<>independent_catalog_sha256 THEN
        RAISE EXCEPTION 'BUG-1019 rollback preflight: helper and independent K2 catalog disagree';
    END IF;
    IF (state='predecessor' AND independent_catalog_sha256<>predecessor_catalog_sha256)
       OR (state='target' AND independent_catalog_sha256<>target_catalog_sha256) THEN
        RAISE EXCEPTION 'BUG-1019 rollback preflight: independent K2 catalog drift for state %, got %',
            state,independent_catalog_sha256;
    END IF;

    SELECT function_record.oid,function_record.prosrc,function_schema.nspname,function_language.lanname,
           function_record.provolatile,function_record.proconfig,pg_get_function_arguments(function_record.oid),
           pg_get_function_result(function_record.oid)
      INTO STRICT k2d_function_oid,k2d_function_body,k2d_function_schema,k2d_function_language,
                  k2d_function_volatility,k2d_function_configuration,k2d_function_arguments,k2d_function_result
      FROM pg_proc AS function_record
      JOIN pg_namespace AS function_schema ON function_schema.oid=function_record.pronamespace
      JOIN pg_language AS function_language ON function_language.oid=function_record.prolang
     WHERE function_record.oid=to_regprocedure('qmt_strategy.miniqmt_k2d_catalog_fingerprint()');
    k2d_function_body_sha256 := encode(
        sha256(convert_to(
            btrim(replace(k2d_function_body,k2d_function_schema,'<schema>'),E' \n\r\t;'),
            'UTF8'
        )),
        'hex'
    );
    IF k2d_function_oid IS NULL OR k2d_function_language<>'sql' OR k2d_function_volatility<>'s'
       OR k2d_function_configuration IS NOT NULL OR k2d_function_arguments<>''
       OR k2d_function_result<>'text'
       OR k2d_function_body_sha256<>k2d_catalog_function_body_sha256 THEN
        RAISE EXCEPTION 'BUG-1019 rollback preflight: K2-D catalog function definition drift';
    END IF;
    EXECUTE k2d_function_body INTO STRICT k2d_independent_catalog_sha256;
    SELECT qmt_strategy.miniqmt_k2d_catalog_fingerprint() INTO STRICT k2d_helper_catalog_sha256;
    IF k2d_helper_catalog_sha256<>k2d_independent_catalog_sha256
       OR k2d_independent_catalog_sha256<>k2d_catalog_sha256 THEN
        RAISE EXCEPTION 'BUG-1019 rollback preflight: independent K2-D catalog drift: helper=%, independent=%',
            k2d_helper_catalog_sha256,k2d_independent_catalog_sha256;
    END IF;

    IF state='predecessor' THEN
        SELECT count(*) INTO invalid_row_count FROM qmt_strategy.execution_runtime_event
         WHERE (event_contract_version='LEGACY_V1' AND event_type=ANY(predecessor_types)
                AND source=ANY(predecessor_sources)) IS NOT TRUE;
        IF invalid_row_count<>0 THEN
            RAISE EXCEPTION 'BUG-1019 rollback no-op: % durable rows violate predecessor',invalid_row_count;
        END IF;
        RAISE NOTICE 'BUG-1019 rollback no-op: exact immediate predecessor is already installed';
    ELSE
        SELECT count(*) INTO invalid_row_count FROM qmt_strategy.execution_runtime_event
         WHERE (
            (event_contract_version='LEGACY_V1' AND event_type=ANY(predecessor_types)
             AND source=ANY(predecessor_sources))
            OR (event_contract_version='KERNEL_V2' AND (
                (event_type='ALGO_START' AND source='MINIQMT_EXECUTION_KERNEL'
                 AND payload_schema_version IN ('miniqmt_algo_start_v1','miniqmt_algo_start_v2'))
                OR (event_type='COMMAND_OUTCOME' AND source='MINIQMT_EXECUTION_KERNEL'
                    AND payload_schema_version='miniqmt_command_outcome_v1')
                OR (event_type='TICK' AND source='B0_QUOTE_V2' AND payload_schema_version='miniqmt_market_data_view_v2')
                OR (event_type='TIMER' AND source='EXCHANGE_SESSION_CLOCK' AND payload_schema_version='miniqmt_timer_due_v1')
                OR (event_type='SESSION' AND source='EXCHANGE_SESSION_CLOCK' AND payload_schema_version='miniqmt_session_event_v1')
                OR (event_type='EOD' AND source='EXCHANGE_SESSION_CLOCK' AND payload_schema_version='miniqmt_eod_event_v1')
                OR (event_type='ORDER' AND source='QMT_GATEWAY_CALLBACK' AND payload_schema_version='miniqmt_order_event_v1')
                OR (event_type='TRADE' AND source='QMT_GATEWAY_CALLBACK' AND payload_schema_version='miniqmt_trade_fact_v1')
                OR (event_type='ACCOUNT' AND source='QMT_OMS_PROJECTION'
                    AND payload_schema_version='miniqmt_account_projection_v1')
                OR (event_type='RECONCILE' AND source='QMT_OMS_RECONCILIATION'
                    AND payload_schema_version='miniqmt_reconciliation_receipt_v1')
                OR (event_type='OPERATOR' AND source='SIMULATION_RUNTIME_OPERATOR'
                    AND payload_schema_version='miniqmt_operator_command_v1')
            ))
         ) IS NOT TRUE;
        IF invalid_row_count<>0 THEN
            RAISE EXCEPTION 'BUG-1019 rollback preflight: % durable rows violate successor',invalid_row_count;
        END IF;
        SELECT
            (SELECT count(*) FROM qmt_strategy.execution_runtime_event
              WHERE event_contract_version='KERNEL_V2'
                 OR event_type<>ALL(predecessor_types)
                 OR source<>ALL(predecessor_sources))
          + (SELECT count(*) FROM qmt_strategy.execution_algo_instance
              WHERE kernel_contract_version='KERNEL_V2')
          + (SELECT count(*) FROM qmt_strategy.execution_child_order
              WHERE kernel_contract_version='KERNEL_V2')
          + (SELECT count(*) FROM qmt_strategy.execution_kernel_worker_epoch)
          + (SELECT count(*) FROM qmt_strategy.execution_kernel_worker_incarnation)
          + (SELECT count(*) FROM qmt_strategy.execution_algo_event_delivery)
          + (SELECT count(*) FROM qmt_strategy.execution_algo_transition)
          + (SELECT count(*) FROM qmt_strategy.execution_algo_command_outbox)
          + (SELECT count(*) FROM qmt_strategy.execution_algo_command_dispatch_attempt)
          + (SELECT count(*) FROM qmt_strategy.execution_algo_timer_schedule)
          + (SELECT count(*) FROM qmt_strategy.execution_algo_timer_occurrence)
          + (SELECT count(*) FROM qmt_strategy.execution_exchange_session_authority)
          + (SELECT count(*) FROM qmt_strategy.execution_algo_diagnostic_observation)
          + (SELECT count(*) FROM qmt_strategy.execution_broker_reconciliation_attempt)
          + (SELECT count(*) FROM qmt_strategy.execution_dependent_buy_coordination)
          + (SELECT count(*) FROM qmt_strategy.execution_dependent_buy_dependency)
          + (SELECT count(*) FROM qmt_strategy.execution_dependent_buy_decision)
          + (SELECT count(*) FROM qmt_strategy.execution_product_command_authority)
          + (SELECT count(*) FROM qmt_strategy.execution_product_command_authority_item)
          + (SELECT count(*) FROM qmt_strategy.execution_product_route_cutover)
          + (SELECT count(*) FROM qmt_strategy.execution_product_route_owner)
          INTO target_only_fact_count;
        IF target_only_fact_count<>0 THEN
            RAISE EXCEPTION 'BUG-1019 destructive rollback refused: % successor/KERNEL_V2 durable facts exist',
                target_only_fact_count;
        END IF;

        ALTER TABLE qmt_strategy.execution_runtime_event DROP CONSTRAINT ck_miniqmt_event_id;
        ALTER TABLE qmt_strategy.execution_runtime_event DROP CONSTRAINT ck_miniqmt_event_sequence;
        ALTER TABLE qmt_strategy.execution_runtime_event DROP CONSTRAINT ck_miniqmt_event_type;
        ALTER TABLE qmt_strategy.execution_runtime_event DROP CONSTRAINT ck_miniqmt_event_source;
        ALTER TABLE qmt_strategy.execution_runtime_event DROP CONSTRAINT ck_miniqmt_k2_event_composite;
        ALTER TABLE qmt_strategy.execution_runtime_event DROP CONSTRAINT ck_miniqmt_k2_event_contract;
        ALTER TABLE qmt_strategy.execution_runtime_event ADD CONSTRAINT ck_miniqmt_event_id
            CHECK (btrim(event_id) <> '') NOT VALID;
        ALTER TABLE qmt_strategy.execution_runtime_event ADD CONSTRAINT ck_miniqmt_event_sequence
            CHECK (sequence > 0) NOT VALID;
        ALTER TABLE qmt_strategy.execution_runtime_event ADD CONSTRAINT ck_miniqmt_event_type CHECK (event_type IN (
            'RUNTIME_CREATED','GATEWAY_CONNECTED','GATEWAY_DISCONNECTED','BROKER_SYNC_STARTED','BROKER_SYNCED',
            'ALGO_INSTANCE_CREATED','TIMER','TICK','ALGO_ACTION_EMITTED','CHILD_ORDER_SUBMITTED',
            'CHILD_ORDER_REJECTED','CHILD_ORDER_CANCEL_REQUESTED','ORDER_EVENT','TRADE_EVENT','ACCOUNT_EVENT',
            'RISK_KILL_SWITCH_TRIGGERED','RECONCILE_STARTED','RECONCILE_COMPLETED','OPERATOR_COMMAND_RECEIVED',
            'OPERATOR_COMMAND_EXECUTED','OPERATOR_COMMAND_REJECTED','RUNTIME_STOPPED','QUOTE_OBSERVED','QUOTE_REJECTED',
            'QUOTE_ELIGIBILITY_EVALUATED','QUOTE_MARK_CAPTURED','QUOTE_INGRESS_HEALTH'
        )) NOT VALID;
        ALTER TABLE qmt_strategy.execution_runtime_event ADD CONSTRAINT ck_miniqmt_event_source CHECK (source IN (
            'runtime','gateway','oms','algo','operator','recovery','quote_ingress'
        )) NOT VALID;
        ALTER TABLE qmt_strategy.execution_runtime_event ADD CONSTRAINT ck_miniqmt_k2_event_composite CHECK (
            event_contract_version='LEGACY_V1'
            OR (event_type='ALGO_START' AND source='MINIQMT_EXECUTION_KERNEL'
                AND payload_schema_version='miniqmt_algo_start_v1')
            OR (event_type='TICK' AND source='B0_QUOTE_V2' AND payload_schema_version='miniqmt_market_data_view_v2')
            OR (event_type='TIMER' AND source='EXCHANGE_SESSION_CLOCK' AND payload_schema_version='miniqmt_timer_due_v1')
            OR (event_type='SESSION' AND source='EXCHANGE_SESSION_CLOCK' AND payload_schema_version='miniqmt_session_event_v1')
            OR (event_type='EOD' AND source='EXCHANGE_SESSION_CLOCK' AND payload_schema_version='miniqmt_eod_event_v1')
            OR (event_type='ORDER' AND source='QMT_GATEWAY_CALLBACK' AND payload_schema_version='miniqmt_order_event_v1')
            OR (event_type='TRADE' AND source='QMT_GATEWAY_CALLBACK' AND payload_schema_version='miniqmt_trade_fact_v1')
            OR (event_type='ACCOUNT' AND source='QMT_OMS_PROJECTION'
                AND payload_schema_version='miniqmt_account_projection_v1')
            OR (event_type='RECONCILE' AND source='QMT_OMS_RECONCILIATION'
                AND payload_schema_version='miniqmt_reconciliation_receipt_v1')
            OR (event_type='OPERATOR' AND source='SIMULATION_RUNTIME_OPERATOR'
                AND payload_schema_version='miniqmt_operator_command_v1')
        ) NOT VALID;
        ALTER TABLE qmt_strategy.execution_runtime_event ADD CONSTRAINT ck_miniqmt_k2_event_contract CHECK (
            (event_contract_version = 'LEGACY_V1'
             AND event_schema_version IS NULL AND payload_schema_version IS NULL
             AND event_key_sha256 IS NULL AND payload_sha256 IS NULL
             AND observed_at_utc IS NULL AND logical_at_utc IS NULL
             AND source_identity_json IS NULL AND correlation_json IS NULL
             AND ingress_receipt_json IS NULL AND ingress_receipt_sha256 IS NULL
             AND routing_rule_version IS NULL AND transaction_commit_identity IS NULL)
            OR
            (event_contract_version = 'KERNEL_V2'
             AND event_schema_version = 'miniqmt_runtime_event_envelope_v2'
             AND payload_schema_version IS NOT NULL
             AND event_key_sha256 ~ '^[0-9a-f]{64}$'
             AND payload_sha256 ~ '^[0-9a-f]{64}$'
             AND observed_at_utc IS NOT NULL AND logical_at_utc IS NOT NULL
             AND source_identity_json IS NOT NULL AND correlation_json IS NOT NULL
             AND ingress_receipt_json IS NOT NULL
             AND ingress_receipt_sha256 ~ '^[0-9a-f]{64}$'
             AND routing_rule_version = 'miniqmt_event_routing_v1'
             AND transaction_commit_identity IS NOT NULL)
        ) NOT VALID;
        ALTER TABLE qmt_strategy.execution_runtime_event VALIDATE CONSTRAINT ck_miniqmt_event_id;
        ALTER TABLE qmt_strategy.execution_runtime_event VALIDATE CONSTRAINT ck_miniqmt_event_sequence;
        ALTER TABLE qmt_strategy.execution_runtime_event VALIDATE CONSTRAINT ck_miniqmt_event_type;
        ALTER TABLE qmt_strategy.execution_runtime_event VALIDATE CONSTRAINT ck_miniqmt_event_source;
        ALTER TABLE qmt_strategy.execution_runtime_event VALIDATE CONSTRAINT ck_miniqmt_k2_event_composite;
        ALTER TABLE qmt_strategy.execution_runtime_event VALIDATE CONSTRAINT ck_miniqmt_k2_event_contract;
    END IF;

    IF state='target' THEN
        EXECUTE $catalog_ddl$
CREATE OR REPLACE FUNCTION qmt_strategy.miniqmt_k2_catalog_fingerprint()
RETURNS TEXT
LANGUAGE sql
STABLE
AS $catalog_body$
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
FROM canonical_catalog;
$catalog_body$
$catalog_ddl$;
    END IF;

    SELECT function_record.prosrc,function_schema.nspname,function_language.lanname,
           function_record.provolatile,function_record.proconfig,pg_get_function_arguments(function_record.oid),
           pg_get_function_result(function_record.oid)
      INTO STRICT function_body,function_schema,function_language,function_volatility,function_configuration,
                  function_arguments,function_result
      FROM pg_proc AS function_record
      JOIN pg_namespace AS function_schema ON function_schema.oid=function_record.pronamespace
      JOIN pg_language AS function_language ON function_language.oid=function_record.prolang
     WHERE function_record.oid=to_regprocedure('qmt_strategy.miniqmt_k2_catalog_fingerprint()');
    function_body_sha256 := encode(
        sha256(convert_to(btrim(replace(function_body,function_schema,'<schema>'),E' \n\r\t;'),'UTF8')),
        'hex'
    );
    IF function_language<>'sql' OR function_volatility<>'s' OR function_arguments<>''
       OR function_configuration IS NOT NULL
       OR function_result<>'text' OR function_body_sha256<>predecessor_catalog_function_body_sha256 THEN
        RAISE EXCEPTION 'BUG-1019 rollback post-DDL K2 catalog function definition drift';
    END IF;

    SELECT count(*), count(*) FILTER (WHERE convalidated),
           array_agg(conname ORDER BY conname COLLATE "C")
      INTO constraint_count, validated_count, constraint_names
      FROM pg_constraint
     WHERE conrelid=table_oid AND contype='c';
    SELECT encode(sha256(convert_to(pg_get_constraintdef(oid,true),'UTF8')),'hex') INTO STRICT type_definition_sha256
      FROM pg_constraint WHERE conrelid=table_oid AND conname='ck_miniqmt_event_type' AND contype='c';
    SELECT encode(sha256(convert_to(pg_get_constraintdef(oid,true),'UTF8')),'hex') INTO STRICT source_definition_sha256
      FROM pg_constraint WHERE conrelid=table_oid AND conname='ck_miniqmt_event_source' AND contype='c';
    SELECT encode(sha256(convert_to(pg_get_constraintdef(oid,true),'UTF8')),'hex') INTO STRICT composite_definition_sha256
      FROM pg_constraint WHERE conrelid=table_oid AND conname='ck_miniqmt_k2_event_composite' AND contype='c';
    SELECT encode(sha256(convert_to(pg_get_constraintdef(oid,true),'UTF8')),'hex') INTO STRICT contract_definition_sha256
      FROM pg_constraint WHERE conrelid=table_oid AND conname='ck_miniqmt_k2_event_contract' AND contype='c';
    SELECT encode(sha256(convert_to(pg_get_constraintdef(oid,true),'UTF8')),'hex')
      INTO STRICT event_id_definition_sha256 FROM pg_constraint
     WHERE conrelid=table_oid AND conname='ck_miniqmt_event_id' AND contype='c';
    SELECT encode(sha256(convert_to(pg_get_constraintdef(oid,true),'UTF8')),'hex')
      INTO STRICT event_sequence_definition_sha256 FROM pg_constraint
     WHERE conrelid=table_oid AND conname='ck_miniqmt_event_sequence' AND contype='c';
    IF constraint_count<>validated_count
       OR constraint_names<>predecessor_constraint_names
       OR type_definition_sha256<>predecessor_type_sha256
       OR source_definition_sha256<>predecessor_source_sha256
       OR composite_definition_sha256<>predecessor_composite_sha256
       OR contract_definition_sha256<>predecessor_contract_sha256
       OR event_id_definition_sha256<>predecessor_event_id_sha256
       OR event_sequence_definition_sha256<>predecessor_event_sequence_sha256 THEN
        RAISE EXCEPTION 'BUG-1019 rollback post-DDL exact constraint readback drift';
    END IF;
    EXECUTE function_body INTO STRICT independent_catalog_sha256;
    SELECT qmt_strategy.miniqmt_k2_catalog_fingerprint() INTO STRICT helper_catalog_sha256;
    IF helper_catalog_sha256<>independent_catalog_sha256
       OR independent_catalog_sha256<>predecessor_catalog_sha256 THEN
        RAISE EXCEPTION 'BUG-1019 rollback post-DDL independent K2 catalog drift: helper=%, independent=%',
            helper_catalog_sha256,independent_catalog_sha256;
    END IF;
    SELECT count(*) INTO invalid_row_count FROM qmt_strategy.execution_runtime_event
     WHERE (event_contract_version='LEGACY_V1' AND event_type=ANY(predecessor_types)
            AND source=ANY(predecessor_sources)) IS NOT TRUE;
    IF invalid_row_count<>0 THEN
        RAISE EXCEPTION 'BUG-1019 rollback post-DDL durable row drift: % invalid rows',invalid_row_count;
    END IF;
END $$;
COMMIT;

-- Independent post-commit predecessor assertion.  Any mismatch exits non-zero.
BEGIN;
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;
SET LOCAL statement_timeout = '30s';
SET LOCAL search_path = pg_catalog, qmt_strategy, pg_temp;
LOCK TABLE
    qmt_strategy.execution_runtime,
    qmt_strategy.execution_runtime_event,
    qmt_strategy.execution_algo_instance,
    qmt_strategy.execution_child_order,
    qmt_strategy.execution_kernel_worker_epoch,
    qmt_strategy.execution_kernel_worker_incarnation,
    qmt_strategy.execution_algo_event_delivery,
    qmt_strategy.execution_algo_transition,
    qmt_strategy.execution_algo_command_outbox,
    qmt_strategy.execution_algo_command_dispatch_attempt,
    qmt_strategy.execution_algo_timer_schedule,
    qmt_strategy.execution_algo_timer_occurrence,
    qmt_strategy.execution_exchange_session_authority,
    qmt_strategy.execution_algo_diagnostic_observation,
    qmt_strategy.execution_broker_reconciliation_attempt,
    qmt_strategy.execution_dependent_buy_coordination,
    qmt_strategy.execution_dependent_buy_dependency,
    qmt_strategy.execution_dependent_buy_decision,
    qmt_strategy.execution_product_command_authority,
    qmt_strategy.execution_product_command_authority_item,
    qmt_strategy.execution_product_route_cutover,
    qmt_strategy.execution_product_route_owner
IN ACCESS SHARE MODE;
DO $$
DECLARE
    table_oid OID := 'qmt_strategy.execution_runtime_event'::regclass;
    constraint_count INTEGER;
    validated_count INTEGER;
    type_sha256 TEXT;
    source_sha256 TEXT;
    composite_sha256 TEXT;
    contract_sha256 TEXT;
    event_id_sha256 TEXT;
    event_sequence_sha256 TEXT;
    constraint_names TEXT[];
    function_body TEXT;
    function_schema TEXT;
    function_body_sha256 TEXT;
    helper_catalog_sha256 TEXT;
    independent_catalog_sha256 TEXT;
    k2d_function_oid OID;
    k2d_function_body TEXT;
    k2d_function_schema TEXT;
    k2d_function_language TEXT;
    k2d_function_volatility "char";
    k2d_function_configuration TEXT[];
    k2d_function_arguments TEXT;
    k2d_function_result TEXT;
    k2d_function_body_sha256 TEXT;
    k2d_helper_catalog_sha256 TEXT;
    k2d_independent_catalog_sha256 TEXT;
    invalid_row_count BIGINT;
    target_only_fact_count BIGINT;
    predecessor_type_sha256 CONSTANT TEXT := '148b6275debe87a7ebda2dc51385a6583a334f5a8dd6779e5124576758b4255e';
    predecessor_source_sha256 CONSTANT TEXT := '835ad788ea103d5f0e7cca878c810331a2f1b7fdb1377a554acefa30cd209697';
    predecessor_composite_sha256 CONSTANT TEXT := '907e964380874d06918981201685af0338bef13f034c7becd5e04a9a591b06b3';
    predecessor_contract_sha256 CONSTANT TEXT := '9d193860ed0de361ef590ba195b531c623afa09b42620f52e2c0938b9f6a1212';
    expected_event_id_sha256 CONSTANT TEXT := '55f2f3dd015fc42bed99754d426d434e62a3456295263bbbf42c3358d8257608';
    expected_event_sequence_sha256 CONSTANT TEXT := 'ddfd70c30577468691d352ae838281ec74c56efd9d5ec1c3e32967cf9ef5c6ed';
    predecessor_catalog_sha256 CONSTANT TEXT := 'd0bf2e66443b46e60b9f931ac387b60cb84e2ce056c480e7c81aa2397a7ccdbe';
    predecessor_catalog_function_body_sha256 CONSTANT TEXT := '8d9c8b09b5c27a0b0caeeaea3663556b9876b0eea179057d691bbf2fce29c107';
    k2d_catalog_sha256 CONSTANT TEXT := '2d5fcbf0151d9e5d2a9d8537f834aabfd056a42cc0eeb8c079add68c8964f59f';
    k2d_catalog_function_body_sha256 CONSTANT TEXT := '9e5236fdc17b79888c864871e71ed6613b12759bbe87e070bd5c1c1db0b95451';
    predecessor_types CONSTANT TEXT[] := ARRAY[
        'ACCOUNT_EVENT','ALGO_ACTION_EMITTED','ALGO_INSTANCE_CREATED','BROKER_SYNCED','BROKER_SYNC_STARTED',
        'CHILD_ORDER_CANCEL_REQUESTED','CHILD_ORDER_REJECTED','CHILD_ORDER_SUBMITTED','GATEWAY_CONNECTED',
        'GATEWAY_DISCONNECTED','OPERATOR_COMMAND_EXECUTED','OPERATOR_COMMAND_RECEIVED',
        'OPERATOR_COMMAND_REJECTED','ORDER_EVENT','QUOTE_ELIGIBILITY_EVALUATED','QUOTE_INGRESS_HEALTH',
        'QUOTE_MARK_CAPTURED','QUOTE_OBSERVED','QUOTE_REJECTED','RECONCILE_COMPLETED','RECONCILE_STARTED',
        'RISK_KILL_SWITCH_TRIGGERED','RUNTIME_CREATED','RUNTIME_STOPPED','TICK','TIMER','TRADE_EVENT'
    ];
    predecessor_sources CONSTANT TEXT[] := ARRAY['algo','gateway','oms','operator','quote_ingress','recovery','runtime'];
    predecessor_constraint_names CONSTANT TEXT[] := ARRAY[
        'ck_miniqmt_event_id','ck_miniqmt_event_sequence','ck_miniqmt_event_source',
        'ck_miniqmt_event_type','ck_miniqmt_k2_event_composite','ck_miniqmt_k2_event_contract'
    ];
BEGIN
    SELECT count(*), count(*) FILTER (WHERE convalidated),
           array_agg(conname ORDER BY conname COLLATE "C")
      INTO constraint_count, validated_count, constraint_names
      FROM pg_constraint
     WHERE conrelid=table_oid AND contype='c';
    SELECT encode(sha256(convert_to(pg_get_constraintdef(oid,true),'UTF8')),'hex') INTO STRICT type_sha256
      FROM pg_constraint WHERE conrelid=table_oid AND conname='ck_miniqmt_event_type' AND contype='c';
    SELECT encode(sha256(convert_to(pg_get_constraintdef(oid,true),'UTF8')),'hex') INTO STRICT source_sha256
      FROM pg_constraint WHERE conrelid=table_oid AND conname='ck_miniqmt_event_source' AND contype='c';
    SELECT encode(sha256(convert_to(pg_get_constraintdef(oid,true),'UTF8')),'hex') INTO STRICT composite_sha256
      FROM pg_constraint WHERE conrelid=table_oid AND conname='ck_miniqmt_k2_event_composite' AND contype='c';
    SELECT encode(sha256(convert_to(pg_get_constraintdef(oid,true),'UTF8')),'hex') INTO STRICT contract_sha256
      FROM pg_constraint WHERE conrelid=table_oid AND conname='ck_miniqmt_k2_event_contract' AND contype='c';
    SELECT encode(sha256(convert_to(pg_get_constraintdef(oid,true),'UTF8')),'hex') INTO STRICT event_id_sha256
      FROM pg_constraint WHERE conrelid=table_oid AND conname='ck_miniqmt_event_id' AND contype='c';
    SELECT encode(sha256(convert_to(pg_get_constraintdef(oid,true),'UTF8')),'hex') INTO STRICT event_sequence_sha256
      FROM pg_constraint WHERE conrelid=table_oid AND conname='ck_miniqmt_event_sequence' AND contype='c';
    IF constraint_count<>validated_count
       OR constraint_names<>predecessor_constraint_names
       OR type_sha256<>predecessor_type_sha256
       OR source_sha256<>predecessor_source_sha256 OR composite_sha256<>predecessor_composite_sha256
       OR contract_sha256<>predecessor_contract_sha256
       OR event_id_sha256<>expected_event_id_sha256
       OR event_sequence_sha256<>expected_event_sequence_sha256 THEN
        RAISE EXCEPTION 'BUG-1019 rollback post-commit exact constraint readback drift';
    END IF;
    SELECT function_record.prosrc,function_namespace.nspname
      INTO STRICT function_body,function_schema
      FROM pg_proc AS function_record
      JOIN pg_namespace AS function_namespace ON function_namespace.oid=function_record.pronamespace
      JOIN pg_language AS function_language ON function_language.oid=function_record.prolang
     WHERE function_record.oid=to_regprocedure('qmt_strategy.miniqmt_k2_catalog_fingerprint()')
       AND function_language.lanname='sql' AND function_record.provolatile='s'
       AND function_record.proconfig IS NULL
       AND pg_get_function_arguments(function_record.oid)=''
       AND pg_get_function_result(function_record.oid)='text';
    function_body_sha256 := encode(
        sha256(convert_to(btrim(replace(function_body,function_schema,'<schema>'),E' \n\r\t;'),'UTF8')),'hex'
    );
    IF function_body_sha256<>predecessor_catalog_function_body_sha256 THEN
        RAISE EXCEPTION 'BUG-1019 rollback post-commit K2 catalog function definition drift';
    END IF;
    EXECUTE function_body INTO STRICT independent_catalog_sha256;
    SELECT qmt_strategy.miniqmt_k2_catalog_fingerprint() INTO STRICT helper_catalog_sha256;
    IF helper_catalog_sha256<>independent_catalog_sha256
       OR independent_catalog_sha256<>predecessor_catalog_sha256 THEN
        RAISE EXCEPTION 'BUG-1019 rollback post-commit independent K2 catalog drift: helper=%, independent=%',
            helper_catalog_sha256,independent_catalog_sha256;
    END IF;
    SELECT function_record.oid,function_record.prosrc,function_schema.nspname,function_language.lanname,
           function_record.provolatile,function_record.proconfig,pg_get_function_arguments(function_record.oid),
           pg_get_function_result(function_record.oid)
      INTO STRICT k2d_function_oid,k2d_function_body,k2d_function_schema,k2d_function_language,
                  k2d_function_volatility,k2d_function_configuration,k2d_function_arguments,k2d_function_result
      FROM pg_proc AS function_record
      JOIN pg_namespace AS function_schema ON function_schema.oid=function_record.pronamespace
      JOIN pg_language AS function_language ON function_language.oid=function_record.prolang
     WHERE function_record.oid=to_regprocedure('qmt_strategy.miniqmt_k2d_catalog_fingerprint()');
    k2d_function_body_sha256 := encode(
        sha256(convert_to(
            btrim(replace(k2d_function_body,k2d_function_schema,'<schema>'),E' \n\r\t;'),
            'UTF8'
        )),
        'hex'
    );
    IF k2d_function_oid IS NULL OR k2d_function_language<>'sql' OR k2d_function_volatility<>'s'
       OR k2d_function_configuration IS NOT NULL OR k2d_function_arguments<>''
       OR k2d_function_result<>'text'
       OR k2d_function_body_sha256<>k2d_catalog_function_body_sha256 THEN
        RAISE EXCEPTION 'BUG-1019 rollback post-commit K2-D catalog function definition drift';
    END IF;
    EXECUTE k2d_function_body INTO STRICT k2d_independent_catalog_sha256;
    SELECT qmt_strategy.miniqmt_k2d_catalog_fingerprint() INTO STRICT k2d_helper_catalog_sha256;
    IF k2d_helper_catalog_sha256<>k2d_independent_catalog_sha256
       OR k2d_independent_catalog_sha256<>k2d_catalog_sha256 THEN
        RAISE EXCEPTION 'BUG-1019 rollback post-commit independent K2-D catalog drift: helper=%, independent=%',
            k2d_helper_catalog_sha256,k2d_independent_catalog_sha256;
    END IF;
    SELECT (SELECT count(*) FROM qmt_strategy.execution_algo_instance
             WHERE kernel_contract_version='KERNEL_V2')
         + (SELECT count(*) FROM qmt_strategy.execution_child_order
             WHERE kernel_contract_version='KERNEL_V2')
         + (SELECT count(*) FROM qmt_strategy.execution_kernel_worker_epoch)
         + (SELECT count(*) FROM qmt_strategy.execution_kernel_worker_incarnation)
         + (SELECT count(*) FROM qmt_strategy.execution_algo_event_delivery)
         + (SELECT count(*) FROM qmt_strategy.execution_algo_transition)
         + (SELECT count(*) FROM qmt_strategy.execution_algo_command_outbox)
         + (SELECT count(*) FROM qmt_strategy.execution_algo_command_dispatch_attempt)
         + (SELECT count(*) FROM qmt_strategy.execution_algo_timer_schedule)
         + (SELECT count(*) FROM qmt_strategy.execution_algo_timer_occurrence)
         + (SELECT count(*) FROM qmt_strategy.execution_exchange_session_authority)
         + (SELECT count(*) FROM qmt_strategy.execution_algo_diagnostic_observation)
         + (SELECT count(*) FROM qmt_strategy.execution_broker_reconciliation_attempt)
         + (SELECT count(*) FROM qmt_strategy.execution_dependent_buy_coordination)
         + (SELECT count(*) FROM qmt_strategy.execution_dependent_buy_dependency)
         + (SELECT count(*) FROM qmt_strategy.execution_dependent_buy_decision)
         + (SELECT count(*) FROM qmt_strategy.execution_product_command_authority)
         + (SELECT count(*) FROM qmt_strategy.execution_product_command_authority_item)
         + (SELECT count(*) FROM qmt_strategy.execution_product_route_cutover)
         + (SELECT count(*) FROM qmt_strategy.execution_product_route_owner)
      INTO target_only_fact_count;
    IF target_only_fact_count<>0 THEN
        RAISE EXCEPTION 'BUG-1019 rollback post-commit successor fact drift: % facts remain',
            target_only_fact_count;
    END IF;
    SELECT count(*) INTO invalid_row_count FROM qmt_strategy.execution_runtime_event
     WHERE (event_contract_version='LEGACY_V1' AND event_type=ANY(predecessor_types)
            AND source=ANY(predecessor_sources)) IS NOT TRUE;
    IF invalid_row_count<>0 THEN
        RAISE EXCEPTION 'BUG-1019 rollback post-commit durable row drift: % invalid rows',invalid_row_count;
    END IF;
    PERFORM pg_catalog.set_config(
        'aistock.k2_independent_catalog_sha256',independent_catalog_sha256,true
    );
    PERFORM pg_catalog.set_config(
        'aistock.k2_code_owned_catalog_sha256',predecessor_catalog_sha256,true
    );
    PERFORM pg_catalog.set_config(
        'aistock.k2d_independent_catalog_sha256',k2d_independent_catalog_sha256,true
    );
    PERFORM pg_catalog.set_config(
        'aistock.k2d_code_owned_catalog_sha256',k2d_catalog_sha256,true
    );
END $$;
SELECT current_database() AS database_name,current_user AS database_user,
       'qmt_strategy.execution_runtime_event'::regclass::OID AS table_oid,
       current_setting('server_version_num') AS server_version_num,
       (SELECT datcollate FROM pg_database WHERE datname=current_database()) AS database_collation,
       (SELECT jsonb_agg(jsonb_build_object(
            'constraint_name',constraint_record.conname,
            'constraint_oid',constraint_record.oid,
            'validated',constraint_record.convalidated,
            'definition_sha256',encode(
                sha256(convert_to(pg_get_constraintdef(constraint_record.oid,true),'UTF8')),'hex'
            )
        ) ORDER BY constraint_record.conname COLLATE "C")
        FROM pg_constraint AS constraint_record
        WHERE constraint_record.conrelid='qmt_strategy.execution_runtime_event'::regclass
          AND constraint_record.contype='c') AS check_authority_json,
       (SELECT jsonb_build_object(
            'function_oid',function_record.oid,
            'body_sha256',encode(sha256(convert_to(
                btrim(replace(function_record.prosrc,function_schema.nspname,'<schema>'),E' \n\r\t;'),
                'UTF8'
            )),'hex'),
            'configuration',function_record.proconfig
        )
        FROM pg_proc AS function_record
        JOIN pg_namespace AS function_schema ON function_schema.oid=function_record.pronamespace
        WHERE function_record.oid=to_regprocedure(
            'qmt_strategy.miniqmt_k2_catalog_fingerprint()'
        )) AS k2_helper_authority_json,
       (SELECT jsonb_build_object(
            'function_oid',function_record.oid,
            'body_sha256',encode(sha256(convert_to(
                btrim(replace(function_record.prosrc,function_schema.nspname,'<schema>'),E' \n\r\t;'),
                'UTF8'
            )),'hex'),
            'configuration',function_record.proconfig
        )
        FROM pg_proc AS function_record
        JOIN pg_namespace AS function_schema ON function_schema.oid=function_record.pronamespace
        WHERE function_record.oid=to_regprocedure(
            'qmt_strategy.miniqmt_k2d_catalog_fingerprint()'
        )) AS k2d_helper_authority_json,
       qmt_strategy.miniqmt_k2_catalog_fingerprint() AS k2_catalog_sha256,
       pg_catalog.current_setting('aistock.k2_independent_catalog_sha256')
           AS k2_independent_catalog_sha256,
       pg_catalog.current_setting('aistock.k2_code_owned_catalog_sha256')
           AS k2_code_owned_catalog_sha256,
       (
           qmt_strategy.miniqmt_k2_catalog_fingerprint()
               = pg_catalog.current_setting('aistock.k2_independent_catalog_sha256')
           AND pg_catalog.current_setting('aistock.k2_independent_catalog_sha256')
               = pg_catalog.current_setting('aistock.k2_code_owned_catalog_sha256')
       ) AS k2_catalog_authority_verified,
       qmt_strategy.miniqmt_k2d_catalog_fingerprint() AS k2d_catalog_sha256,
       pg_catalog.current_setting('aistock.k2d_independent_catalog_sha256')
           AS k2d_independent_catalog_sha256,
       pg_catalog.current_setting('aistock.k2d_code_owned_catalog_sha256')
           AS k2d_code_owned_catalog_sha256,
       (
           qmt_strategy.miniqmt_k2d_catalog_fingerprint()
               = pg_catalog.current_setting('aistock.k2d_independent_catalog_sha256')
           AND pg_catalog.current_setting('aistock.k2d_independent_catalog_sha256')
               = pg_catalog.current_setting('aistock.k2d_code_owned_catalog_sha256')
       ) AS k2d_catalog_authority_verified,
       jsonb_build_object(
           'execution_algo_instance',(SELECT count(*) FROM qmt_strategy.execution_algo_instance
                                       WHERE kernel_contract_version='KERNEL_V2'),
           'execution_child_order',(SELECT count(*) FROM qmt_strategy.execution_child_order
                                     WHERE kernel_contract_version='KERNEL_V2'),
           'execution_kernel_worker_epoch',(SELECT count(*) FROM qmt_strategy.execution_kernel_worker_epoch),
           'execution_kernel_worker_incarnation',(SELECT count(*) FROM qmt_strategy.execution_kernel_worker_incarnation),
           'execution_algo_event_delivery',(SELECT count(*) FROM qmt_strategy.execution_algo_event_delivery),
           'execution_algo_transition',(SELECT count(*) FROM qmt_strategy.execution_algo_transition),
           'execution_algo_command_outbox',(SELECT count(*) FROM qmt_strategy.execution_algo_command_outbox),
           'execution_algo_command_dispatch_attempt',(SELECT count(*) FROM qmt_strategy.execution_algo_command_dispatch_attempt),
           'execution_algo_timer_schedule',(SELECT count(*) FROM qmt_strategy.execution_algo_timer_schedule),
           'execution_algo_timer_occurrence',(SELECT count(*) FROM qmt_strategy.execution_algo_timer_occurrence),
           'execution_exchange_session_authority',(SELECT count(*) FROM qmt_strategy.execution_exchange_session_authority),
           'execution_algo_diagnostic_observation',(SELECT count(*) FROM qmt_strategy.execution_algo_diagnostic_observation),
           'execution_broker_reconciliation_attempt',(SELECT count(*) FROM qmt_strategy.execution_broker_reconciliation_attempt),
           'execution_dependent_buy_coordination',(SELECT count(*) FROM qmt_strategy.execution_dependent_buy_coordination),
           'execution_dependent_buy_dependency',(SELECT count(*) FROM qmt_strategy.execution_dependent_buy_dependency),
           'execution_dependent_buy_decision',(SELECT count(*) FROM qmt_strategy.execution_dependent_buy_decision),
           'execution_product_command_authority',(SELECT count(*) FROM qmt_strategy.execution_product_command_authority),
           'execution_product_command_authority_item',(SELECT count(*) FROM qmt_strategy.execution_product_command_authority_item),
           'execution_product_route_cutover',(SELECT count(*) FROM qmt_strategy.execution_product_route_cutover),
           'execution_product_route_owner',(SELECT count(*) FROM qmt_strategy.execution_product_route_owner)
       ) AS successor_fact_counts_json,
       (SELECT count(*) FROM qmt_strategy.execution_runtime_event) AS durable_event_count,
       (SELECT count(*) FROM qmt_strategy.execution_runtime_event
         WHERE event_contract_version='KERNEL_V2') AS kernel_v2_event_count,
       clock_timestamp() AS queried_at_utc;
COMMIT;

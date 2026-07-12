-- P1-D rollback for miniqmt_quote_ingress_event_types_20260712.sql.
-- This is an allowlist rollback only.  It refuses to delete, rewrite, or hide
-- any quote-ingress rows.  Apply it only with the same explicit operator
-- authorization required for the forward DDL.

BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DO $$
DECLARE
    table_oid oid;
    type_def text;
    source_def text;
    type_validated boolean;
    source_validated boolean;
    type_shape text;
    source_shape text;
    type_values text[];
    source_values text[];
    new_type_count bigint;
    new_source_count bigint;
    min_sequence bigint;
    max_sequence bigint;
    old_types constant text[] := ARRAY[
        'ACCOUNT_EVENT', 'ALGO_ACTION_EMITTED', 'ALGO_INSTANCE_CREATED',
        'BROKER_SYNCED', 'BROKER_SYNC_STARTED', 'CHILD_ORDER_CANCEL_REQUESTED',
        'CHILD_ORDER_REJECTED', 'CHILD_ORDER_SUBMITTED', 'GATEWAY_CONNECTED',
        'GATEWAY_DISCONNECTED', 'OPERATOR_COMMAND_EXECUTED', 'OPERATOR_COMMAND_RECEIVED',
        'OPERATOR_COMMAND_REJECTED', 'ORDER_EVENT', 'RECONCILE_COMPLETED',
        'RECONCILE_STARTED', 'RISK_KILL_SWITCH_TRIGGERED', 'RUNTIME_CREATED',
        'RUNTIME_STOPPED', 'TICK', 'TIMER', 'TRADE_EVENT'
    ];
    target_types constant text[] := ARRAY[
        'ACCOUNT_EVENT', 'ALGO_ACTION_EMITTED', 'ALGO_INSTANCE_CREATED',
        'BROKER_SYNCED', 'BROKER_SYNC_STARTED', 'CHILD_ORDER_CANCEL_REQUESTED',
        'CHILD_ORDER_REJECTED', 'CHILD_ORDER_SUBMITTED', 'GATEWAY_CONNECTED',
        'GATEWAY_DISCONNECTED', 'OPERATOR_COMMAND_EXECUTED', 'OPERATOR_COMMAND_RECEIVED',
        'OPERATOR_COMMAND_REJECTED', 'ORDER_EVENT', 'QUOTE_ELIGIBILITY_EVALUATED',
        'QUOTE_INGRESS_HEALTH', 'QUOTE_MARK_CAPTURED', 'QUOTE_OBSERVED',
        'QUOTE_REJECTED', 'RECONCILE_COMPLETED', 'RECONCILE_STARTED',
        'RISK_KILL_SWITCH_TRIGGERED', 'RUNTIME_CREATED', 'RUNTIME_STOPPED',
        'TICK', 'TIMER', 'TRADE_EVENT'
    ];
    old_sources constant text[] := ARRAY['algo', 'gateway', 'oms', 'operator', 'recovery', 'runtime'];
    target_sources constant text[] := ARRAY['algo', 'gateway', 'oms', 'operator', 'quote_ingress', 'recovery', 'runtime'];
BEGIN
    table_oid := to_regclass('qmt_strategy.execution_runtime_event');
    IF table_oid IS NULL THEN
        RAISE EXCEPTION 'P1-D rollback preflight: qmt_strategy.execution_runtime_event does not exist';
    END IF;
    SELECT pg_get_constraintdef(oid, true), convalidated
      INTO type_def, type_validated
      FROM pg_constraint
     WHERE conrelid = table_oid AND conname = 'ck_miniqmt_event_type' AND contype = 'c';
    SELECT pg_get_constraintdef(oid, true), convalidated
      INTO source_def, source_validated
      FROM pg_constraint
     WHERE conrelid = table_oid AND conname = 'ck_miniqmt_event_source' AND contype = 'c';
    IF type_def IS NULL OR source_def IS NULL OR upper(type_def) !~ 'EVENT_TYPE' OR upper(source_def) !~ 'SOURCE' THEN
        RAISE EXCEPTION 'P1-D rollback preflight: named CHECK constraint drift';
    END IF;
    type_shape := regexp_replace(upper(type_def), '''[^'']+''', '', 'g');
    type_shape := regexp_replace(type_shape, '::[A-Z_]+', '', 'g');
    type_shape := regexp_replace(type_shape, 'CHECK|EVENT_TYPE|IN|ANY|ARRAY|[()\[\],=[:space:]]', '', 'g');
    source_shape := regexp_replace(upper(source_def), '''[^'']+''', '', 'g');
    source_shape := regexp_replace(source_shape, '::[A-Z_]+', '', 'g');
    source_shape := regexp_replace(source_shape, 'CHECK|SOURCE|IN|ANY|ARRAY|[()\[\],=[:space:]]', '', 'g');
    IF type_shape <> '' OR source_shape <> '' THEN
        RAISE EXCEPTION 'P1-D rollback preflight: CHECK expression drift';
    END IF;
    SELECT coalesce(array_agg(match[1] ORDER BY match[1]), ARRAY[]::text[])
      INTO type_values FROM regexp_matches(type_def, '''([^'']+)''', 'g') AS match;
    SELECT coalesce(array_agg(match[1] ORDER BY match[1]), ARRAY[]::text[])
      INTO source_values FROM regexp_matches(source_def, '''([^'']+)''', 'g') AS match;
    type_shape := regexp_replace(upper(type_def), '''[^'']+''', '', 'g');
    type_shape := regexp_replace(type_shape, '::[A-Z_]+', '', 'g');
    type_shape := regexp_replace(type_shape, 'CHECK|EVENT_TYPE|IN|ANY|ARRAY|[()\[\],=[:space:]]', '', 'g');
    source_shape := regexp_replace(upper(source_def), '''[^'']+''', '', 'g');
    source_shape := regexp_replace(source_shape, '::[A-Z_]+', '', 'g');
    source_shape := regexp_replace(source_shape, 'CHECK|SOURCE|IN|ANY|ARRAY|[()\[\],=[:space:]]', '', 'g');
    IF type_shape <> '' OR source_shape <> '' THEN
        RAISE EXCEPTION 'P1-D rollback: CHECK expression drift after lock';
    END IF;
    IF type_values <> old_types AND type_values <> target_types THEN
        RAISE EXCEPTION 'P1-D rollback preflight: event type allowlist is not exact old/target';
    END IF;
    IF source_values <> old_sources AND source_values <> target_sources THEN
        RAISE EXCEPTION 'P1-D rollback preflight: source allowlist is not exact old/target';
    END IF;
    IF type_values = old_types AND source_values = old_sources THEN
        IF NOT type_validated THEN
            ALTER TABLE qmt_strategy.execution_runtime_event VALIDATE CONSTRAINT ck_miniqmt_event_type;
        END IF;
        IF NOT source_validated THEN
            ALTER TABLE qmt_strategy.execution_runtime_event VALIDATE CONSTRAINT ck_miniqmt_event_source;
        END IF;
        RAISE NOTICE 'P1-D rollback no-op: exact old CHECK allowlists already present and validated';
        RETURN;
    END IF;

    SELECT count(*) FILTER (WHERE event_type IN (
               'QUOTE_OBSERVED', 'QUOTE_REJECTED', 'QUOTE_ELIGIBILITY_EVALUATED',
               'QUOTE_MARK_CAPTURED', 'QUOTE_INGRESS_HEALTH'
           )),
           count(*) FILTER (WHERE source = 'quote_ingress'),
           min(sequence) FILTER (WHERE event_type IN (
               'QUOTE_OBSERVED', 'QUOTE_REJECTED', 'QUOTE_ELIGIBILITY_EVALUATED',
               'QUOTE_MARK_CAPTURED', 'QUOTE_INGRESS_HEALTH'
           ) OR source = 'quote_ingress'),
           max(sequence) FILTER (WHERE event_type IN (
               'QUOTE_OBSERVED', 'QUOTE_REJECTED', 'QUOTE_ELIGIBILITY_EVALUATED',
               'QUOTE_MARK_CAPTURED', 'QUOTE_INGRESS_HEALTH'
           ) OR source = 'quote_ingress')
      INTO new_type_count, new_source_count, min_sequence, max_sequence
      FROM qmt_strategy.execution_runtime_event;
    IF new_type_count <> 0 OR new_source_count <> 0 THEN
        RAISE EXCEPTION 'P1-D rollback refused: quote-ingress rows exist (new_type_count=%, new_source_count=%, min_sequence=%, max_sequence=%)',
            new_type_count, new_source_count, min_sequence, max_sequence;
    END IF;

    LOCK TABLE qmt_strategy.execution_runtime_event IN SHARE ROW EXCLUSIVE MODE;
    -- Recheck after the lock: a concurrent forward migration or insert must
    -- turn this into a rollback, never a partial allowlist change.
    SELECT pg_get_constraintdef(oid, true)
      INTO type_def FROM pg_constraint
     WHERE conrelid = table_oid AND conname = 'ck_miniqmt_event_type' AND contype = 'c';
    SELECT pg_get_constraintdef(oid, true)
      INTO source_def FROM pg_constraint
     WHERE conrelid = table_oid AND conname = 'ck_miniqmt_event_source' AND contype = 'c';
    SELECT coalesce(array_agg(match[1] ORDER BY match[1]), ARRAY[]::text[])
      INTO type_values FROM regexp_matches(type_def, '''([^'']+)''', 'g') AS match;
    SELECT coalesce(array_agg(match[1] ORDER BY match[1]), ARRAY[]::text[])
      INTO source_values FROM regexp_matches(source_def, '''([^'']+)''', 'g') AS match;
    IF type_values <> target_types OR source_values <> target_sources THEN
        RAISE EXCEPTION 'P1-D rollback: CHECK definition drift after lock';
    END IF;
    SELECT count(*) FILTER (WHERE event_type IN (
               'QUOTE_OBSERVED', 'QUOTE_REJECTED', 'QUOTE_ELIGIBILITY_EVALUATED',
               'QUOTE_MARK_CAPTURED', 'QUOTE_INGRESS_HEALTH'
           )), count(*) FILTER (WHERE source = 'quote_ingress')
      INTO new_type_count, new_source_count
      FROM qmt_strategy.execution_runtime_event;
    IF new_type_count <> 0 OR new_source_count <> 0 THEN
        RAISE EXCEPTION 'P1-D rollback refused after lock: quote-ingress rows exist (new_type_count=%, new_source_count=%)',
            new_type_count, new_source_count;
    END IF;

    ALTER TABLE qmt_strategy.execution_runtime_event DROP CONSTRAINT ck_miniqmt_event_type;
    ALTER TABLE qmt_strategy.execution_runtime_event DROP CONSTRAINT ck_miniqmt_event_source;
    ALTER TABLE qmt_strategy.execution_runtime_event
        ADD CONSTRAINT ck_miniqmt_event_type CHECK (event_type IN (
            'RUNTIME_CREATED', 'GATEWAY_CONNECTED', 'GATEWAY_DISCONNECTED',
            'BROKER_SYNC_STARTED', 'BROKER_SYNCED', 'ALGO_INSTANCE_CREATED', 'TIMER', 'TICK',
            'ALGO_ACTION_EMITTED', 'CHILD_ORDER_SUBMITTED', 'CHILD_ORDER_REJECTED',
            'CHILD_ORDER_CANCEL_REQUESTED', 'ORDER_EVENT', 'TRADE_EVENT', 'ACCOUNT_EVENT',
            'RISK_KILL_SWITCH_TRIGGERED', 'RECONCILE_STARTED', 'RECONCILE_COMPLETED',
            'OPERATOR_COMMAND_RECEIVED', 'OPERATOR_COMMAND_EXECUTED', 'OPERATOR_COMMAND_REJECTED',
            'RUNTIME_STOPPED'
        )) NOT VALID;
    ALTER TABLE qmt_strategy.execution_runtime_event VALIDATE CONSTRAINT ck_miniqmt_event_type;
    ALTER TABLE qmt_strategy.execution_runtime_event
        ADD CONSTRAINT ck_miniqmt_event_source CHECK (source IN (
            'runtime', 'gateway', 'oms', 'algo', 'operator', 'recovery'
        )) NOT VALID;
    ALTER TABLE qmt_strategy.execution_runtime_event VALIDATE CONSTRAINT ck_miniqmt_event_source;

    SELECT coalesce(array_agg(match[1] ORDER BY match[1]), ARRAY[]::text[])
      INTO type_values
      FROM regexp_matches(
          (SELECT pg_get_constraintdef(oid, true) FROM pg_constraint
            WHERE conrelid = table_oid AND conname = 'ck_miniqmt_event_type' AND contype = 'c'),
          '''([^'']+)''', 'g'
      ) AS match;
    SELECT coalesce(array_agg(match[1] ORDER BY match[1]), ARRAY[]::text[])
      INTO source_values
      FROM regexp_matches(
          (SELECT pg_get_constraintdef(oid, true) FROM pg_constraint
            WHERE conrelid = table_oid AND conname = 'ck_miniqmt_event_source' AND contype = 'c'),
          '''([^'']+)''', 'g'
      ) AS match;
    IF type_values <> old_types OR source_values <> old_sources THEN
        RAISE EXCEPTION 'P1-D rollback: post-DDL CHECK readback does not match exact old allowlists';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = table_oid
           AND conname IN ('ck_miniqmt_event_type', 'ck_miniqmt_event_source')
           AND NOT convalidated
    ) THEN
        RAISE EXCEPTION 'P1-D rollback: post-DDL CHECK readback found an unvalidated constraint';
    END IF;
END $$;

COMMIT;

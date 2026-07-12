-- P1-D durable quote-evidence CHECK migration.
--
-- This file is intentionally an operator-applied migration.  Application
-- startup must never execute it.  It changes only the two existing CHECK
-- constraints on qmt_strategy.execution_runtime_event: no table, column,
-- index, role, data row, or other object is created or changed.

BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DO $$
DECLARE
    table_oid oid;
    type_oid oid;
    source_oid oid;
    type_def text;
    source_def text;
    type_validated boolean;
    source_validated boolean;
    type_shape text;
    source_shape text;
    type_values text[];
    source_values text[];
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
        RAISE EXCEPTION 'P1-D preflight: qmt_strategy.execution_runtime_event does not exist';
    END IF;

    SELECT oid, pg_get_constraintdef(oid, true), convalidated
      INTO type_oid, type_def, type_validated
      FROM pg_constraint
     WHERE conrelid = table_oid AND conname = 'ck_miniqmt_event_type' AND contype = 'c';
    SELECT oid, pg_get_constraintdef(oid, true), convalidated
      INTO source_oid, source_def, source_validated
      FROM pg_constraint
     WHERE conrelid = table_oid AND conname = 'ck_miniqmt_event_source' AND contype = 'c';
    IF type_oid IS NULL OR source_oid IS NULL THEN
        RAISE EXCEPTION 'P1-D preflight: both named CHECK constraints must exist exactly once';
    END IF;

    -- The literal extraction and column-token check intentionally reject an
    -- expression drift even when its allowed values happen to look familiar.
    IF upper(type_def) !~ 'EVENT_TYPE' OR upper(source_def) !~ 'SOURCE' THEN
        RAISE EXCEPTION 'P1-D preflight: CHECK definition column drift';
    END IF;
    type_shape := regexp_replace(upper(type_def), '''[^'']+''', '', 'g');
    type_shape := regexp_replace(type_shape, '::[A-Z_]+', '', 'g');
    type_shape := regexp_replace(type_shape, 'CHECK|EVENT_TYPE|IN|ANY|ARRAY|[()\[\],=[:space:]]', '', 'g');
    source_shape := regexp_replace(upper(source_def), '''[^'']+''', '', 'g');
    source_shape := regexp_replace(source_shape, '::[A-Z_]+', '', 'g');
    source_shape := regexp_replace(source_shape, 'CHECK|SOURCE|IN|ANY|ARRAY|[()\[\],=[:space:]]', '', 'g');
    IF type_shape <> '' OR source_shape <> '' THEN
        RAISE EXCEPTION 'P1-D preflight: CHECK expression drift';
    END IF;
    SELECT coalesce(array_agg(match[1] ORDER BY match[1]), ARRAY[]::text[])
      INTO type_values
      FROM regexp_matches(type_def, '''([^'']+)''', 'g') AS match;
    SELECT coalesce(array_agg(match[1] ORDER BY match[1]), ARRAY[]::text[])
      INTO source_values
      FROM regexp_matches(source_def, '''([^'']+)''', 'g') AS match;
    type_shape := regexp_replace(upper(type_def), '''[^'']+''', '', 'g');
    type_shape := regexp_replace(type_shape, '::[A-Z_]+', '', 'g');
    type_shape := regexp_replace(type_shape, 'CHECK|EVENT_TYPE|IN|ANY|ARRAY|[()\[\],=[:space:]]', '', 'g');
    source_shape := regexp_replace(upper(source_def), '''[^'']+''', '', 'g');
    source_shape := regexp_replace(source_shape, '::[A-Z_]+', '', 'g');
    source_shape := regexp_replace(source_shape, 'CHECK|SOURCE|IN|ANY|ARRAY|[()\[\],=[:space:]]', '', 'g');
    IF type_shape <> '' OR source_shape <> '' THEN
        RAISE EXCEPTION 'P1-D apply: CHECK expression drift after lock';
    END IF;
    IF type_values <> old_types AND type_values <> target_types THEN
        RAISE EXCEPTION 'P1-D preflight: ck_miniqmt_event_type allowed literal set is not exact old/target';
    END IF;
    IF source_values <> old_sources AND source_values <> target_sources THEN
        RAISE EXCEPTION 'P1-D preflight: ck_miniqmt_event_source allowed literal set is not exact old/target';
    END IF;
    IF EXISTS (
        SELECT 1
          FROM qmt_strategy.execution_runtime_event
         WHERE event_type <> ALL(target_types) OR source <> ALL(target_sources)
    ) THEN
        RAISE EXCEPTION 'P1-D preflight: existing rows contain values outside the target allowlists';
    END IF;

    -- A SHARE ROW EXCLUSIVE lock prevents a check/row TOCTOU window.  Re-read
    -- constraints after taking it; an already-target schema is a true no-op.
    LOCK TABLE qmt_strategy.execution_runtime_event IN SHARE ROW EXCLUSIVE MODE;
    SELECT pg_get_constraintdef(oid, true), convalidated
      INTO type_def, type_validated
      FROM pg_constraint
     WHERE conrelid = table_oid AND conname = 'ck_miniqmt_event_type' AND contype = 'c';
    SELECT pg_get_constraintdef(oid, true), convalidated
      INTO source_def, source_validated
      FROM pg_constraint
     WHERE conrelid = table_oid AND conname = 'ck_miniqmt_event_source' AND contype = 'c';
    SELECT coalesce(array_agg(match[1] ORDER BY match[1]), ARRAY[]::text[])
      INTO type_values
      FROM regexp_matches(type_def, '''([^'']+)''', 'g') AS match;
    SELECT coalesce(array_agg(match[1] ORDER BY match[1]), ARRAY[]::text[])
      INTO source_values
      FROM regexp_matches(source_def, '''([^'']+)''', 'g') AS match;
    IF type_values = target_types AND source_values = target_sources THEN
        IF NOT type_validated THEN
            ALTER TABLE qmt_strategy.execution_runtime_event VALIDATE CONSTRAINT ck_miniqmt_event_type;
        END IF;
        IF NOT source_validated THEN
            ALTER TABLE qmt_strategy.execution_runtime_event VALIDATE CONSTRAINT ck_miniqmt_event_source;
        END IF;
        SELECT bool_and(convalidated)
          INTO type_validated
          FROM pg_constraint
         WHERE conrelid = table_oid AND conname IN ('ck_miniqmt_event_type', 'ck_miniqmt_event_source');
        IF type_validated IS NOT TRUE THEN
            RAISE EXCEPTION 'P1-D apply: target CHECK constraints are not validated';
        END IF;
        RAISE NOTICE 'P1-D apply no-op: exact target CHECK allowlists already present and validated';
        RETURN;
    END IF;
    IF type_values <> old_types OR source_values <> old_sources THEN
        RAISE EXCEPTION 'P1-D apply: CHECK definition drift after lock';
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
            'RUNTIME_STOPPED', 'QUOTE_OBSERVED', 'QUOTE_REJECTED',
            'QUOTE_ELIGIBILITY_EVALUATED', 'QUOTE_MARK_CAPTURED', 'QUOTE_INGRESS_HEALTH'
        )) NOT VALID;
    ALTER TABLE qmt_strategy.execution_runtime_event VALIDATE CONSTRAINT ck_miniqmt_event_type;
    ALTER TABLE qmt_strategy.execution_runtime_event
        ADD CONSTRAINT ck_miniqmt_event_source CHECK (source IN (
            'runtime', 'gateway', 'oms', 'algo', 'operator', 'recovery', 'quote_ingress'
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
    IF type_values <> target_types OR source_values <> target_sources THEN
        RAISE EXCEPTION 'P1-D apply: post-DDL CHECK readback does not match target allowlists';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_constraint
         WHERE conrelid = table_oid
           AND conname IN ('ck_miniqmt_event_type', 'ck_miniqmt_event_source')
           AND NOT convalidated
    ) THEN
        RAISE EXCEPTION 'P1-D apply: post-DDL CHECK readback found an unvalidated constraint';
    END IF;
END $$;

COMMIT;

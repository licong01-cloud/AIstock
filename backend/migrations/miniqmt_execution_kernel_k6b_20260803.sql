-- K6-B allows only monotonic source-fact successors for one K6-C0 dependency.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
SELECT pg_advisory_xact_lock(hashtext('miniqmt_execution_kernel_k6b_20260803'));

DO $$
DECLARE
    durable_rows BIGINT;
    exact_successor_trigger BOOLEAN;
BEGIN
    SELECT count(*) INTO durable_rows FROM qmt_strategy.execution_dependent_buy_coordination;
    SELECT EXISTS (
        SELECT 1 FROM pg_trigger t
        JOIN pg_class c ON c.oid=t.tgrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='qmt_strategy' AND c.relname='execution_dependent_buy_dependency'
          AND t.tgname='trg_miniqmt_k6_dependency_successor' AND NOT t.tgisinternal
          AND position('miniqmt_k6b_validate_dependency_successor' IN pg_get_triggerdef(t.oid,true))>0
    ) INTO exact_successor_trigger;
    IF durable_rows<>0 AND (
       NOT exact_successor_trigger OR NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='qmt_strategy' AND table_name='execution_dependent_buy_coordination'
          AND column_name='virtual_account_id'
       ) OR NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='qmt_strategy' AND table_name='execution_dependent_buy_coordination'
          AND column_name='session_authority_sha256'
       )) THEN
        RAISE EXCEPTION 'K6-B coordination authority successor requires zero predecessor rows: rows=%',durable_rows;
    END IF;
END $$;

ALTER TABLE qmt_strategy.execution_dependent_buy_coordination
    ADD COLUMN IF NOT EXISTS virtual_account_id TEXT,
    ADD COLUMN IF NOT EXISTS session_authority_sha256 TEXT;
ALTER TABLE qmt_strategy.execution_dependent_buy_coordination
    ALTER COLUMN virtual_account_id SET NOT NULL,
    ALTER COLUMN session_authority_sha256 SET NOT NULL;
ALTER TABLE qmt_strategy.execution_dependent_buy_coordination
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k6b_coordination_authority;
ALTER TABLE qmt_strategy.execution_dependent_buy_coordination
    ADD CONSTRAINT ck_miniqmt_k6b_coordination_authority CHECK (
        btrim(virtual_account_id)<>'' AND session_authority_sha256 ~ '^[0-9a-f]{64}$'
    );

CREATE OR REPLACE FUNCTION qmt_strategy.miniqmt_k6_validate_coordination_update()
RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        IF NEW.status<>'DEFERRED_WAITING_SELL_PROCEEDS'
           OR NEW.decision_sequence<>0 OR NEW.last_decision_sha256 IS NOT NULL
           OR NEW.released_command_id IS NOT NULL OR NEW.released_outbox_id IS NOT NULL
           OR NEW.row_version<>1 OR NEW.lease_epoch<>0
           OR NEW.lease_worker_id IS NOT NULL OR NEW.lease_process_incarnation_id IS NOT NULL
           OR NEW.lease_expires_at_utc IS NOT NULL THEN
            RAISE EXCEPTION 'K6 dependent-BUY V2 first write requires exact waiting initial state';
        END IF;
        RETURN NEW;
    END IF;
    IF (NEW.coordination_id,NEW.runtime_id,NEW.binding_id,NEW.trade_date,NEW.strategy_id,
        NEW.buy_algo_instance_id,NEW.buy_parent_intent_id,NEW.required_cash,
        NEW.virtual_account_id,NEW.session_authority_sha256,
        NEW.release_command_id,NEW.release_transition_id,NEW.release_command_authority_item_sha256,
        NEW.release_command_payload_sha256,NEW.created_at_utc)
       IS DISTINCT FROM
       (OLD.coordination_id,OLD.runtime_id,OLD.binding_id,OLD.trade_date,OLD.strategy_id,
        OLD.buy_algo_instance_id,OLD.buy_parent_intent_id,OLD.required_cash,
        OLD.virtual_account_id,OLD.session_authority_sha256,
        OLD.release_command_id,OLD.release_transition_id,OLD.release_command_authority_item_sha256,
        OLD.release_command_payload_sha256,OLD.created_at_utc) THEN
        RAISE EXCEPTION 'K6 dependent-BUY V2 coordination immutable owner/payload drift';
    END IF;
    IF OLD.status IN ('RELEASED_TO_K2_OUTBOX','BLOCKED_SELL_PROCEEDS_UNAVAILABLE','EOD_RESIDUAL') THEN
        RAISE EXCEPTION 'K6 dependent-BUY terminal coordination cannot be updated';
    END IF;
    IF NEW.row_version<>OLD.row_version+1 OR NEW.decision_sequence<OLD.decision_sequence
       OR NEW.lease_epoch NOT IN (OLD.lease_epoch,OLD.lease_epoch+1)
       OR (NEW.lease_epoch=OLD.lease_epoch AND
           (NEW.lease_worker_id,NEW.lease_process_incarnation_id)
           IS DISTINCT FROM (OLD.lease_worker_id,OLD.lease_process_incarnation_id)) THEN
        RAISE EXCEPTION 'K6 dependent-BUY V2 coordination update is not a monotonic CAS successor';
    END IF;
    RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION qmt_strategy.miniqmt_k6b_validate_dependency_successor()
RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        RETURN NEW;
    END IF;
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'K6-B dependency rows are never deletable';
    END IF;
    IF (NEW.coordination_id,NEW.runtime_id,NEW.strategy_id,NEW.sell_parent_intent_id,
        NEW.sell_algo_instance_id)
       IS DISTINCT FROM
       (OLD.coordination_id,OLD.runtime_id,OLD.strategy_id,OLD.sell_parent_intent_id,
        OLD.sell_algo_instance_id) THEN
        RAISE EXCEPTION 'K6-B dependency immutable owner drift';
    END IF;
    IF OLD.dependency_status='TERMINAL_WITHOUT_SUFFICIENT_PROCEEDS' THEN
        RAISE EXCEPTION 'K6-B terminal dependency cannot reopen';
    END IF;
    IF (OLD.dependency_status='OPEN' AND NEW.dependency_status NOT IN
            ('OPEN','PROCEEDS_SETTLED','TERMINAL_WITHOUT_SUFFICIENT_PROCEEDS'))
       OR (OLD.dependency_status='PROCEEDS_SETTLED' AND NEW.dependency_status NOT IN
            ('PROCEEDS_SETTLED','TERMINAL_WITHOUT_SUFFICIENT_PROCEEDS')) THEN
        RAISE EXCEPTION 'K6-B dependency status is not monotonic';
    END IF;
    IF NOT (NEW.ordered_settled_proceeds_refs @> OLD.ordered_settled_proceeds_refs)
       OR jsonb_array_length(NEW.ordered_settled_proceeds_refs) < jsonb_array_length(OLD.ordered_settled_proceeds_refs) THEN
        RAISE EXCEPTION 'K6-B dependency removes settled proceeds evidence';
    END IF;
    IF (NEW.latest_order_fact_id,NEW.latest_order_fact_sha256)
       IS DISTINCT FROM (OLD.latest_order_fact_id,OLD.latest_order_fact_sha256)
       AND (NEW.dependency_status<>'TERMINAL_WITHOUT_SUFFICIENT_PROCEEDS'
            OR OLD.dependency_status='TERMINAL_WITHOUT_SUFFICIENT_PROCEEDS') THEN
        RAISE EXCEPTION 'K6-B dependency latest order fact may change only in terminal closure';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_miniqmt_k6_dependency_append_only
    ON qmt_strategy.execution_dependent_buy_dependency;
DROP TRIGGER IF EXISTS trg_miniqmt_k6_dependency_successor
    ON qmt_strategy.execution_dependent_buy_dependency;
CREATE TRIGGER trg_miniqmt_k6_dependency_successor
BEFORE UPDATE OR DELETE ON qmt_strategy.execution_dependent_buy_dependency
FOR EACH ROW EXECUTE FUNCTION qmt_strategy.miniqmt_k6b_validate_dependency_successor();

COMMENT ON FUNCTION qmt_strategy.miniqmt_k6b_validate_dependency_successor()
IS 'K6-B only: preserves frozen SELL owner and permits append-only proceeds/terminal successor facts.';
COMMENT ON TRIGGER trg_miniqmt_k6_dependency_successor
ON qmt_strategy.execution_dependent_buy_dependency
IS 'K6-B rejects source-fact removal, owner drift, status regression, and terminal reopen.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.virtual_account_id
IS 'K6-B frozen broker-account identity of the strategy virtual account.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.session_authority_sha256
IS 'K6-B frozen exchange-session authority used by every ledger observation and EOD decision.';

CREATE OR REPLACE FUNCTION qmt_strategy.miniqmt_k6b_catalog_fingerprint()
RETURNS TEXT LANGUAGE sql STABLE AS $$
WITH target_columns(relname,attname) AS (
    VALUES
        ('execution_dependent_buy_coordination','virtual_account_id'),
        ('execution_dependent_buy_coordination','session_authority_sha256')
), target_constraints(relname,conname) AS (
    VALUES ('execution_dependent_buy_coordination','ck_miniqmt_k6b_coordination_authority')
), target_triggers(relname,tgname) AS (
    VALUES
        ('execution_dependent_buy_coordination','trg_miniqmt_k6_coordination_cas'),
        ('execution_dependent_buy_dependency','trg_miniqmt_k6_dependency_successor')
), target_functions(proname) AS (
    VALUES
        ('miniqmt_k6_validate_coordination_update'),
        ('miniqmt_k6b_validate_dependency_successor')
), catalog_items(sort_key,item) AS (
    SELECT format('column:%s:%s',c.relname,a.attname),
           jsonb_build_array('column',c.relname,a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,
                             coalesce(pg_get_expr(d.adbin,d.adrelid),''),coalesce(col_description(c.oid,a.attnum),''))
    FROM target_columns tc JOIN pg_class c ON c.relname=tc.relname
    JOIN pg_namespace n ON n.oid=c.relnamespace AND n.nspname='qmt_strategy'
    JOIN pg_attribute a ON a.attrelid=c.oid AND a.attname=tc.attname AND a.attnum>0 AND NOT a.attisdropped
    LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
    UNION ALL
    SELECT format('constraint:%s:%s',c.relname,k.conname),
           jsonb_build_array('constraint',c.relname,k.conname,k.contype,k.condeferrable,k.condeferred,k.convalidated,
                             replace(pg_get_constraintdef(k.oid,true),n.nspname||'.','<schema>.'))
    FROM target_constraints tc JOIN pg_class c ON c.relname=tc.relname
    JOIN pg_namespace n ON n.oid=c.relnamespace AND n.nspname='qmt_strategy'
    JOIN pg_constraint k ON k.conrelid=c.oid AND k.conname=tc.conname
    UNION ALL
    SELECT format('trigger:%s:%s',c.relname,t.tgname),
           jsonb_build_array('trigger',c.relname,t.tgname,t.tgenabled,
                             replace(pg_get_triggerdef(t.oid,true),n.nspname||'.','<schema>.'))
    FROM target_triggers target JOIN pg_class c ON c.relname=target.relname
    JOIN pg_namespace n ON n.oid=c.relnamespace AND n.nspname='qmt_strategy'
    JOIN pg_trigger t ON t.tgrelid=c.oid AND t.tgname=target.tgname AND NOT t.tgisinternal
    UNION ALL
    SELECT format('function:%s',p.proname),
           jsonb_build_array('function',p.proname,l.lanname,p.provolatile,p.prokind,
                             pg_get_function_identity_arguments(p.oid),
                             replace(p.prosrc,n.nspname||'.','<schema>.'))
    FROM target_functions target JOIN pg_proc p ON p.proname=target.proname
    JOIN pg_namespace n ON n.oid=p.pronamespace AND n.nspname='qmt_strategy'
    JOIN pg_language l ON l.oid=p.prolang
), canonical_catalog AS (
    SELECT coalesce(jsonb_agg(item ORDER BY sort_key),'[]'::jsonb)::TEXT AS payload FROM catalog_items
)
SELECT encode(sha256(convert_to(payload,'UTF8')),'hex') FROM canonical_catalog
$$;

COMMENT ON FUNCTION qmt_strategy.miniqmt_k6b_catalog_fingerprint()
IS 'K6-B exact catalog fingerprint over successor columns, constraint, triggers, and function bodies.';

DO $$
DECLARE actual TEXT;
BEGIN
    SELECT qmt_strategy.miniqmt_k6b_catalog_fingerprint() INTO actual;
    IF actual <> '10ae5be030612f923f2fe23f17f1f8b4891358cc8bd9565d54ad27ee3d18393c' THEN
        RAISE EXCEPTION 'K6-B post-commit catalog drift: expected 10ae5be030612f923f2fe23f17f1f8b4891358cc8bd9565d54ad27ee3d18393c, got %',actual;
    END IF;
END $$;

COMMIT;

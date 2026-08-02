-- MiniQMT K6-C0 guarded rollback; only before any K6 durable use.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SELECT pg_advisory_xact_lock(hashtext('qmt_strategy.miniqmt_execution_kernel_k6c_20260802'));

DO $$
DECLARE table_name TEXT;
DECLARE durable_rows BIGINT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'execution_dependent_buy_coordination','execution_dependent_buy_dependency',
        'execution_dependent_buy_decision','execution_product_command_authority',
        'execution_product_command_authority_item','execution_product_route_cutover',
        'execution_product_route_owner'
    ] LOOP
        EXECUTE format('SELECT count(*) FROM qmt_strategy.%I',table_name) INTO durable_rows;
        IF durable_rows<>0 THEN
            RAISE EXCEPTION 'K6-C0 destructive rollback refused: table=%, rows=%',table_name,durable_rows;
        END IF;
    END LOOP;
    SELECT count(*) INTO durable_rows FROM qmt_strategy.execution_child_order
    WHERE mapping_status='DEFERRED_DEPENDENT_BUY'
       OR mapping_json->>'schema_version'='miniqmt_product_command_child_mapping_v1';
    IF durable_rows<>0 THEN
        RAISE EXCEPTION 'K6-C0 destructive rollback refused: product mapping rows=%',durable_rows;
    END IF;
END $$;

ALTER TABLE qmt_strategy.execution_dependent_buy_coordination
    DROP CONSTRAINT IF EXISTS fk_miniqmt_k6_coordination_release_item,
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k6_coordination_v2_release;
ALTER TABLE qmt_strategy.execution_product_command_authority_item
    DROP CONSTRAINT IF EXISTS fk_miniqmt_k6_authority_item_coordination,
    DROP CONSTRAINT IF EXISTS uq_miniqmt_k6_authority_item_sha256,
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k6_authority_item_presence,
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k6_authority_item_disposition;
ALTER TABLE qmt_strategy.execution_dependent_buy_dependency
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k6_dependency_v2_json;
ALTER TABLE qmt_strategy.execution_dependent_buy_decision
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k6_decision_v2_ledger,
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k6_decision_hash;
ALTER TABLE qmt_strategy.execution_product_command_authority
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k6_authority_counts,
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k6_authority_disposition;

ALTER TABLE qmt_strategy.execution_product_command_authority
    ADD CONSTRAINT ck_miniqmt_k6_authority_counts CHECK (
        materialize_count>=0 AND reject_count>=0 AND total_count BETWEEN 0 AND 256
        AND materialize_count+reject_count=total_count
    ),
    ADD CONSTRAINT ck_miniqmt_k6_authority_disposition CHECK (aggregate_disposition IN (
        'ZERO_COMMAND','ALL_REJECTED','MATERIALIZE_ALL_ACCEPTED_COMMANDS','MIXED_PER_COMMAND'
    ));
ALTER TABLE qmt_strategy.execution_product_command_authority_item
    ADD CONSTRAINT ck_miniqmt_k6_authority_item_disposition CHECK (disposition IN ('MATERIALIZE','REJECT_SYNCHRONOUS')),
    ADD CONSTRAINT ck_miniqmt_k6_authority_item_presence CHECK (
        (disposition='MATERIALIZE' AND mapping_id IS NOT NULL AND outbox_id IS NOT NULL AND child_order_id IS NOT NULL
            AND reject_reason_code IS NULL AND reject_context_sha256 IS NULL)
        OR (disposition='REJECT_SYNCHRONOUS' AND mapping_id IS NULL AND outbox_id IS NULL AND child_order_id IS NULL
            AND reject_reason_code IS NOT NULL AND reject_context_sha256 ~ '^[0-9a-f]{64}$')
    );

ALTER TABLE qmt_strategy.execution_child_order DROP CONSTRAINT IF EXISTS ck_miniqmt_k2_child_mapping_contract;
ALTER TABLE qmt_strategy.execution_child_order
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k6_product_mapping_state,
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k2_child_mapping_initial;
ALTER TABLE qmt_strategy.execution_child_order ADD CONSTRAINT ck_miniqmt_k2_child_mapping_contract CHECK (
    (kernel_contract_version = 'LEGACY_V1' AND mapping_id IS NULL AND command_id IS NULL AND local_vt_orderid IS NULL AND deterministic_client_order_ref IS NULL AND order_remark IS NULL AND mapping_status IS NULL AND mapping_version IS NULL AND mapping_payload_sha256 IS NULL AND mapping_receipt_sha256 IS NULL AND broker_identity_source_event_id IS NULL AND last_order_event_id IS NULL AND last_trade_event_id IS NULL AND created_transition_id IS NULL AND updated_by_event_id IS NULL AND mapping_created_at_utc IS NULL AND mapping_updated_at_utc IS NULL AND mapping_json IS NULL)
    OR (kernel_contract_version = 'KERNEL_V2' AND mapping_id IS NOT NULL AND command_id IS NOT NULL AND local_vt_orderid IS NOT NULL AND deterministic_client_order_ref IS NOT NULL AND order_remark = deterministic_client_order_ref AND mapping_status IN ('RESERVED','DISPATCHING','BROKER_ACCEPTED','BROKER_REJECTED','OUTCOME_UNKNOWN','TERMINAL') AND mapping_version > 0 AND mapping_payload_sha256 ~ '^[0-9a-f]{64}$' AND mapping_receipt_sha256 ~ '^[0-9a-f]{64}$' AND created_transition_id IS NOT NULL AND mapping_created_at_utc IS NOT NULL AND mapping_updated_at_utc >= mapping_created_at_utc AND mapping_json IS NOT NULL)
) NOT VALID;
ALTER TABLE qmt_strategy.execution_child_order VALIDATE CONSTRAINT ck_miniqmt_k2_child_mapping_contract;
ALTER TABLE qmt_strategy.execution_child_order ADD CONSTRAINT ck_miniqmt_k2_child_mapping_initial CHECK (
    kernel_contract_version <> 'KERNEL_V2' OR mapping_status <> 'RESERVED' OR (
        mapping_version=1 AND broker_order_id IS NULL AND broker_identity_source_event_id IS NULL
        AND last_order_event_id IS NULL AND last_trade_event_id IS NULL AND updated_by_event_id IS NULL
        AND mapping_created_at_utc=mapping_updated_at_utc
    )
) NOT VALID;
ALTER TABLE qmt_strategy.execution_child_order VALIDATE CONSTRAINT ck_miniqmt_k2_child_mapping_initial;

ALTER TABLE qmt_strategy.execution_product_command_authority_item
    DROP COLUMN IF EXISTS coordination_id,
    DROP COLUMN IF EXISTS evaluation_evidence_sha256,
    DROP COLUMN IF EXISTS evaluation_evidence_json,
    DROP COLUMN IF EXISTS command_json;
ALTER TABLE qmt_strategy.execution_product_command_authority DROP COLUMN IF EXISTS defer_count;
ALTER TABLE qmt_strategy.execution_dependent_buy_decision
    DROP COLUMN IF EXISTS ledger_revision_sha256,
    DROP COLUMN IF EXISTS ledger_latest_cash_sequence,
    DROP COLUMN IF EXISTS ledger_virtual_account_updated_at_utc,
    ALTER COLUMN ledger_row_version SET NOT NULL;
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.ledger_row_version IS 'Positive observed virtual-account row version.';
COMMENT ON COLUMN qmt_strategy.execution_child_order.mapping_status IS NULL;
ALTER TABLE qmt_strategy.execution_dependent_buy_decision
    ADD CONSTRAINT ck_miniqmt_k6_decision_hash CHECK (
        decision_id ~ '^[0-9a-f]{64}$' AND trigger_ref_sha256 ~ '^[0-9a-f]{64}$'
        AND ledger_observation_sha256 ~ '^[0-9a-f]{64}$' AND decision_sha256 ~ '^[0-9a-f]{64}$'
        AND lease_epoch>0 AND ledger_row_version>0
        AND jsonb_typeof(ordered_dependency_sha256s)='array'
        AND jsonb_typeof(trigger_ref_json)='object'
        AND jsonb_typeof(ledger_observation_json)='object'
    );
ALTER TABLE qmt_strategy.execution_dependent_buy_dependency
    DROP COLUMN IF EXISTS ordered_settled_proceeds_refs,
    DROP COLUMN IF EXISTS latest_order_fact_sha256,
    DROP COLUMN IF EXISTS latest_order_fact_id;
ALTER TABLE qmt_strategy.execution_dependent_buy_coordination
    DROP COLUMN IF EXISTS release_command_authority_item_sha256,
    DROP COLUMN IF EXISTS release_transition_id,
    DROP COLUMN IF EXISTS release_command_id;

DROP FUNCTION IF EXISTS qmt_strategy.miniqmt_k6c_catalog_fingerprint();
CREATE OR REPLACE FUNCTION qmt_strategy.miniqmt_k6_validate_coordination_update()
RETURNS trigger
LANGUAGE plpgsql
VOLATILE
AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        IF NEW.status<>'DEFERRED_WAITING_SELL_PROCEEDS'
           OR NEW.decision_sequence<>0 OR NEW.last_decision_sha256 IS NOT NULL
           OR NEW.released_command_id IS NOT NULL OR NEW.released_outbox_id IS NOT NULL
           OR NEW.row_version<>1 OR NEW.lease_epoch<>0
           OR NEW.lease_worker_id IS NOT NULL OR NEW.lease_process_incarnation_id IS NOT NULL
           OR NEW.lease_expires_at_utc IS NOT NULL THEN
            RAISE EXCEPTION 'K6 dependent-BUY first write requires exact waiting initial state';
        END IF;
        RETURN NEW;
    END IF;
    IF (NEW.coordination_id,NEW.runtime_id,NEW.binding_id,NEW.trade_date,NEW.strategy_id,
        NEW.buy_algo_instance_id,NEW.buy_parent_intent_id,NEW.required_cash,
        NEW.release_command_payload_sha256,NEW.created_at_utc)
       IS DISTINCT FROM
       (OLD.coordination_id,OLD.runtime_id,OLD.binding_id,OLD.trade_date,OLD.strategy_id,
        OLD.buy_algo_instance_id,OLD.buy_parent_intent_id,OLD.required_cash,
        OLD.release_command_payload_sha256,OLD.created_at_utc) THEN
        RAISE EXCEPTION 'K6 dependent-BUY coordination immutable owner/payload drift';
    END IF;
    IF OLD.status IN ('RELEASED_TO_K2_OUTBOX','BLOCKED_SELL_PROCEEDS_UNAVAILABLE','EOD_RESIDUAL') THEN
        RAISE EXCEPTION 'K6 dependent-BUY terminal coordination cannot be updated';
    END IF;
    IF NEW.row_version<>OLD.row_version+1 OR NEW.decision_sequence<OLD.decision_sequence
       OR NEW.lease_epoch NOT IN (OLD.lease_epoch,OLD.lease_epoch+1)
       OR (NEW.lease_epoch=OLD.lease_epoch AND
           (NEW.lease_worker_id,NEW.lease_process_incarnation_id)
           IS DISTINCT FROM (OLD.lease_worker_id,OLD.lease_process_incarnation_id)) THEN
        RAISE EXCEPTION 'K6 dependent-BUY coordination update is not a monotonic CAS successor';
    END IF;
    RETURN NEW;
END
$$;
COMMIT;

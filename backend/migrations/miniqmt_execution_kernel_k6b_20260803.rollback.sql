-- K6-B rollback is guarded: a successor fact is not safely representable by K6-C0.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DO $$
DECLARE
    durable_rows BIGINT;
BEGIN
    SELECT count(*) INTO durable_rows
    FROM qmt_strategy.execution_dependent_buy_coordination;
    IF durable_rows <> 0 THEN
        RAISE EXCEPTION 'K6-B guarded rollback refused: dependent-BUY durable rows exist';
    END IF;
END $$;

DROP TRIGGER IF EXISTS trg_miniqmt_k6_dependency_successor
    ON qmt_strategy.execution_dependent_buy_dependency;
DROP FUNCTION IF EXISTS qmt_strategy.miniqmt_k6b_validate_dependency_successor();
DROP FUNCTION IF EXISTS qmt_strategy.miniqmt_k6b_catalog_fingerprint();
CREATE TRIGGER trg_miniqmt_k6_dependency_append_only
BEFORE UPDATE OR DELETE ON qmt_strategy.execution_dependent_buy_dependency
FOR EACH ROW EXECUTE FUNCTION qmt_strategy.miniqmt_k6_reject_immutable_mutation();

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
        NEW.release_command_id,NEW.release_transition_id,NEW.release_command_authority_item_sha256,
        NEW.release_command_payload_sha256,NEW.created_at_utc)
       IS DISTINCT FROM
       (OLD.coordination_id,OLD.runtime_id,OLD.binding_id,OLD.trade_date,OLD.strategy_id,
        OLD.buy_algo_instance_id,OLD.buy_parent_intent_id,OLD.required_cash,
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

ALTER TABLE qmt_strategy.execution_dependent_buy_coordination
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k6b_coordination_authority,
    DROP COLUMN IF EXISTS session_authority_sha256,
    DROP COLUMN IF EXISTS virtual_account_id;

COMMIT;

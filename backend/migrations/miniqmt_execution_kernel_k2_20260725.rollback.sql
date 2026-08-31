-- Controlled K2-A schema rollback. Destructive rollback is forbidden after any KERNEL_V2 fact exists.
BEGIN;

DO $$
DECLARE
    kernel_v2_fact_count BIGINT;
BEGIN
    SELECT
        (SELECT COUNT(*) FROM qmt_strategy.execution_runtime_event WHERE event_contract_version = 'KERNEL_V2')
        + (SELECT COUNT(*) FROM qmt_strategy.execution_algo_instance WHERE kernel_contract_version = 'KERNEL_V2')
        + (SELECT COUNT(*) FROM qmt_strategy.execution_child_order WHERE kernel_contract_version = 'KERNEL_V2')
        + (SELECT COUNT(*) FROM qmt_strategy.execution_algo_event_delivery)
        + (SELECT COUNT(*) FROM qmt_strategy.execution_algo_transition)
        + (SELECT COUNT(*) FROM qmt_strategy.execution_algo_command_outbox)
        + (SELECT COUNT(*) FROM qmt_strategy.execution_algo_command_dispatch_attempt)
        + (SELECT COUNT(*) FROM qmt_strategy.execution_algo_timer_schedule)
        + (SELECT COUNT(*) FROM qmt_strategy.execution_algo_timer_occurrence)
        + (SELECT COUNT(*) FROM qmt_strategy.execution_kernel_worker_epoch)
        + (SELECT COUNT(*) FROM qmt_strategy.execution_kernel_worker_incarnation)
        + (SELECT COUNT(*) FROM qmt_strategy.execution_exchange_session_authority)
        + (SELECT COUNT(*) FROM qmt_strategy.execution_algo_diagnostic_observation)
    INTO kernel_v2_fact_count;

    IF kernel_v2_fact_count > 0 THEN
        RAISE EXCEPTION 'K2 destructive rollback refused: % durable KERNEL_V2 facts exist', kernel_v2_fact_count;
    END IF;
END $$;

DROP FUNCTION IF EXISTS qmt_strategy.miniqmt_k2_catalog_fingerprint();

ALTER TABLE qmt_strategy.execution_child_order
    DROP CONSTRAINT IF EXISTS fk_miniqmt_k2_child_transition_owner,
    DROP CONSTRAINT IF EXISTS fk_miniqmt_k2_child_algo_owner;

DROP TABLE IF EXISTS qmt_strategy.execution_algo_diagnostic_observation;
DROP TABLE IF EXISTS qmt_strategy.execution_exchange_session_authority;
DROP TABLE IF EXISTS qmt_strategy.execution_algo_timer_occurrence;
DROP TABLE IF EXISTS qmt_strategy.execution_algo_timer_schedule;
DROP TABLE IF EXISTS qmt_strategy.execution_algo_command_dispatch_attempt;
DROP TABLE IF EXISTS qmt_strategy.execution_algo_command_outbox;
DROP TABLE IF EXISTS qmt_strategy.execution_algo_transition;
DROP TABLE IF EXISTS qmt_strategy.execution_algo_event_delivery;
DROP TABLE IF EXISTS qmt_strategy.execution_kernel_worker_incarnation;
DROP TABLE IF EXISTS qmt_strategy.execution_kernel_worker_epoch;

ALTER TABLE qmt_strategy.execution_runtime_event
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k2_event_contract,
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k2_event_composite,
    DROP CONSTRAINT IF EXISTS uq_miniqmt_k2_event_owner,
    DROP COLUMN IF EXISTS transaction_commit_identity,
    DROP COLUMN IF EXISTS routing_rule_version,
    DROP COLUMN IF EXISTS ingress_receipt_sha256,
    DROP COLUMN IF EXISTS ingress_receipt_json,
    DROP COLUMN IF EXISTS correlation_json,
    DROP COLUMN IF EXISTS source_identity_json,
    DROP COLUMN IF EXISTS logical_at_utc,
    DROP COLUMN IF EXISTS observed_at_utc,
    DROP COLUMN IF EXISTS payload_sha256,
    DROP COLUMN IF EXISTS event_key_sha256,
    DROP COLUMN IF EXISTS event_schema_version,
    DROP COLUMN IF EXISTS payload_schema_version,
    DROP COLUMN IF EXISTS event_contract_version;

ALTER TABLE qmt_strategy.execution_algo_instance
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k2_algo_active_child_closure,
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k2_algo_status,
    DROP CONSTRAINT IF EXISTS uq_miniqmt_k2_algo_runtime_identity,
    DROP CONSTRAINT IF EXISTS uq_miniqmt_k2_algo_owner,
    DROP COLUMN IF EXISTS kernel_carrier_json,
    DROP COLUMN IF EXISTS terminal_at_utc,
    DROP COLUMN IF EXISTS row_version,
    DROP COLUMN IF EXISTS active_child_count,
    DROP COLUMN IF EXISTS active_child_closure_status,
    DROP COLUMN IF EXISTS failure_receipt_id,
    DROP COLUMN IF EXISTS terminal_delivery_sequence,
    DROP COLUMN IF EXISTS last_closed_delivery_sequence,
    DROP COLUMN IF EXISTS last_applied_delivery_id,
    DROP COLUMN IF EXISTS last_applied_delivery_sequence,
    DROP COLUMN IF EXISTS transition_sequence,
    DROP COLUMN IF EXISTS state_sha256,
    DROP COLUMN IF EXISTS state_json,
    DROP COLUMN IF EXISTS state_schema_version,
    DROP COLUMN IF EXISTS compatibility_receipt_sha256,
    DROP COLUMN IF EXISTS plugin_config_sha256,
    DROP COLUMN IF EXISTS plugin_config_json,
    DROP COLUMN IF EXISTS plugin_manifest_sha256,
    DROP COLUMN IF EXISTS plugin_version,
    DROP COLUMN IF EXISTS plugin_id,
    DROP COLUMN IF EXISTS traded_quantity,
    DROP COLUMN IF EXISTS kernel_contract_version;

ALTER TABLE qmt_strategy.execution_runtime
    DROP CONSTRAINT IF EXISTS uq_miniqmt_k2_runtime_trade_date;

ALTER TABLE qmt_strategy.execution_child_order
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k2_child_mapping_contract,
    DROP CONSTRAINT IF EXISTS uq_miniqmt_k2_child_mapping_pair,
    DROP CONSTRAINT IF EXISTS uq_miniqmt_k2_child_mapping_id,
    DROP COLUMN IF EXISTS mapping_json,
    DROP COLUMN IF EXISTS mapping_updated_at_utc,
    DROP COLUMN IF EXISTS mapping_created_at_utc,
    DROP COLUMN IF EXISTS updated_by_event_id,
    DROP COLUMN IF EXISTS created_transition_id,
    DROP COLUMN IF EXISTS last_trade_event_id,
    DROP COLUMN IF EXISTS last_order_event_id,
    DROP COLUMN IF EXISTS broker_identity_source_event_id,
    DROP COLUMN IF EXISTS mapping_receipt_sha256,
    DROP COLUMN IF EXISTS mapping_payload_sha256,
    DROP COLUMN IF EXISTS mapping_version,
    DROP COLUMN IF EXISTS mapping_status,
    DROP COLUMN IF EXISTS order_remark,
    DROP COLUMN IF EXISTS deterministic_client_order_ref,
    DROP COLUMN IF EXISTS local_vt_orderid,
    DROP COLUMN IF EXISTS command_id,
    DROP COLUMN IF EXISTS mapping_id,
    DROP COLUMN IF EXISTS kernel_contract_version;

ALTER TABLE qmt_strategy.execution_algo_instance
    ADD CONSTRAINT ck_miniqmt_algo_status CHECK (status IN ('ACTIVE','PAUSED','COMPLETED','CANCELLED','FAILED'));

COMMIT;

-- Stage 2: nontransactional concurrent index cleanup.
DROP INDEX CONCURRENTLY IF EXISTS qmt_strategy.uq_miniqmt_k2_event_key;
DROP INDEX CONCURRENTLY IF EXISTS qmt_strategy.uq_miniqmt_k2_event_owner;
DROP INDEX CONCURRENTLY IF EXISTS qmt_strategy.uq_miniqmt_k2_algo_runtime_identity;
DROP INDEX CONCURRENTLY IF EXISTS qmt_strategy.uq_miniqmt_k2_algo_owner;
DROP INDEX CONCURRENTLY IF EXISTS qmt_strategy.uq_miniqmt_k2_child_mapping;
DROP INDEX CONCURRENTLY IF EXISTS qmt_strategy.uq_miniqmt_k2_child_client_ref;
DROP INDEX CONCURRENTLY IF EXISTS qmt_strategy.uq_miniqmt_k2_child_broker_order;
DROP INDEX CONCURRENTLY IF EXISTS qmt_strategy.uq_miniqmt_k2_child_mapping_pair;
DROP INDEX CONCURRENTLY IF EXISTS qmt_strategy.uq_miniqmt_k2_child_mapping_id;

-- Stage 3: independent rollback readback.
SELECT
    to_regclass('qmt_strategy.execution_algo_event_delivery') IS NULL AS delivery_removed,
    to_regclass('qmt_strategy.execution_algo_command_outbox') IS NULL AS outbox_removed,
    to_regclass('qmt_strategy.execution_kernel_worker_incarnation') IS NULL AS worker_removed;

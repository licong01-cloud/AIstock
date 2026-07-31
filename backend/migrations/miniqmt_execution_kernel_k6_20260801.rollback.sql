-- MiniQMT K6-A guarded rollback; refuses any durable K6 fact.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SELECT pg_advisory_xact_lock(hashtext('qmt_strategy.miniqmt_execution_kernel_k6_20260801'));

DO $$
DECLARE durable_fact_count BIGINT;
BEGIN
    IF to_regclass('qmt_strategy.execution_dependent_buy_coordination') IS NOT NULL THEN
        SELECT
            (SELECT count(*) FROM qmt_strategy.execution_dependent_buy_coordination)
          + (SELECT count(*) FROM qmt_strategy.execution_dependent_buy_dependency)
          + (SELECT count(*) FROM qmt_strategy.execution_dependent_buy_decision)
          + (SELECT count(*) FROM qmt_strategy.execution_product_command_authority)
          + (SELECT count(*) FROM qmt_strategy.execution_product_command_authority_item)
          + (SELECT count(*) FROM qmt_strategy.execution_product_route_cutover)
          + (SELECT count(*) FROM qmt_strategy.execution_product_route_owner)
        INTO durable_fact_count;
        IF durable_fact_count<>0 THEN
            RAISE EXCEPTION 'K6-A destructive rollback refused: durable_fact_count=%', durable_fact_count;
        END IF;
    END IF;
END $$;

DROP TABLE IF EXISTS qmt_strategy.execution_product_route_owner;
DROP TABLE IF EXISTS qmt_strategy.execution_product_route_cutover;
DROP TABLE IF EXISTS qmt_strategy.execution_dependent_buy_decision;
DROP TABLE IF EXISTS qmt_strategy.execution_product_command_authority_item;
DROP TABLE IF EXISTS qmt_strategy.execution_product_command_authority;
DROP TABLE IF EXISTS qmt_strategy.execution_dependent_buy_dependency;
DROP TABLE IF EXISTS qmt_strategy.execution_dependent_buy_coordination;
DROP FUNCTION IF EXISTS qmt_strategy.miniqmt_k6_catalog_fingerprint();
DROP FUNCTION IF EXISTS qmt_strategy.miniqmt_k6_validate_route_owner();
DROP FUNCTION IF EXISTS qmt_strategy.miniqmt_k6_validate_coordination_update();
DROP FUNCTION IF EXISTS qmt_strategy.miniqmt_k6_reject_immutable_mutation();

COMMIT;

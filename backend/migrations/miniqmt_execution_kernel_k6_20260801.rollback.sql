-- MiniQMT K6-A guarded rollback; refuses any durable K6 fact.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SELECT pg_advisory_xact_lock(hashtext('qmt_strategy.miniqmt_execution_kernel_k6_20260801'));

DO $$
DECLARE
    durable_fact_count BIGINT := 0;
    table_fact_count BIGINT;
    target_table TEXT;
BEGIN
    FOREACH target_table IN ARRAY ARRAY[
        'execution_dependent_buy_coordination','execution_dependent_buy_dependency',
        'execution_dependent_buy_decision','execution_product_command_authority',
        'execution_product_command_authority_item','execution_product_route_cutover',
        'execution_product_route_owner'
    ] LOOP
        IF to_regclass('qmt_strategy.'||target_table) IS NOT NULL THEN
            EXECUTE format('SELECT count(*) FROM %I.%I','qmt_strategy',target_table)
            INTO table_fact_count;
            durable_fact_count := durable_fact_count + table_fact_count;
        END IF;
    END LOOP;
    IF durable_fact_count<>0 THEN
        RAISE EXCEPTION 'K6-A destructive rollback refused: durable_fact_count=%', durable_fact_count;
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
DROP FUNCTION IF EXISTS qmt_strategy.miniqmt_k6_validate_decision_closure();
DROP FUNCTION IF EXISTS qmt_strategy.miniqmt_k6_reject_immutable_mutation();

COMMIT;

-- MiniQMT K6-C0 read-only successor migration preflight.
BEGIN;
SET TRANSACTION READ ONLY;

DO $$
DECLARE table_name TEXT;
DECLARE durable_rows BIGINT;
DECLARE actual TEXT;
BEGIN
    IF to_regprocedure('qmt_strategy.miniqmt_k6_catalog_fingerprint()') IS NULL THEN
        RAISE EXCEPTION 'K6-C0 preflight: K6-A catalog authority is missing';
    END IF;
    FOREACH table_name IN ARRAY ARRAY[
        'execution_dependent_buy_coordination','execution_dependent_buy_dependency',
        'execution_dependent_buy_decision','execution_product_command_authority',
        'execution_product_command_authority_item','execution_product_route_cutover',
        'execution_product_route_owner'
    ] LOOP
        IF to_regclass('qmt_strategy.'||table_name) IS NULL THEN
            RAISE EXCEPTION 'K6-C0 preflight: required K6-A table is missing: %',table_name;
        END IF;
    END LOOP;
    IF to_regprocedure('qmt_strategy.miniqmt_k6c_catalog_fingerprint()') IS NULL THEN
        SELECT qmt_strategy.miniqmt_k6_catalog_fingerprint() INTO actual;
        IF actual <> '546a209dc2f8721ccee8b5e905117788486307147dfb4fc6bc396842f5cf84ad' THEN
            RAISE EXCEPTION 'K6-C0 preflight: base K6-A catalog drift: got %',actual;
        END IF;
        FOREACH table_name IN ARRAY ARRAY[
            'execution_dependent_buy_coordination','execution_dependent_buy_dependency',
            'execution_dependent_buy_decision','execution_product_command_authority',
            'execution_product_command_authority_item','execution_product_route_cutover',
            'execution_product_route_owner'
        ] LOOP
            EXECUTE format('SELECT count(*) FROM qmt_strategy.%I',table_name) INTO durable_rows;
            IF durable_rows<>0 THEN
                RAISE EXCEPTION 'K6-C0 preflight: successor requires zero K6-A durable rows: table=%, rows=%',table_name,durable_rows;
            END IF;
        END LOOP;
    ELSE
        SELECT qmt_strategy.miniqmt_k6c_catalog_fingerprint() INTO actual;
        IF actual <> '841717e7c9f998e5e197048877fa854db8e7469544d6b94682f73c730a7462fe' THEN
            RAISE EXCEPTION 'K6-C0 preflight: successor catalog drift: expected 841717e7c9f998e5e197048877fa854db8e7469544d6b94682f73c730a7462fe, got %',actual;
        END IF;
    END IF;
END $$;

SELECT 'canonical_lf_forward_sha256' AS verification,
       '782f3020a9de4917564d73626a6b099a27866a709d38ff6701f9313225bf5422'::TEXT AS expected_migration_sha256;
ROLLBACK;

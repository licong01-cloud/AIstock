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
        IF actual <> 'f4fc093c83642577009dc5ce8c03550bbb75e00f09ada7bf2489272ddd67bd7d' THEN
            RAISE EXCEPTION 'K6-C0 preflight: successor catalog drift: expected f4fc093c83642577009dc5ce8c03550bbb75e00f09ada7bf2489272ddd67bd7d, got %',actual;
        END IF;
    END IF;
END $$;

SELECT 'canonical_lf_forward_sha256' AS verification,
       '368fc29048ac40c7a9ca32f3ca76a214af2d6ba776e52b2490226ba341fb2ab4'::TEXT AS expected_migration_sha256;
ROLLBACK;

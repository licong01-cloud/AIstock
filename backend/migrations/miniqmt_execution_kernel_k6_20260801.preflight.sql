-- MiniQMT K6-A read-only additive migration preflight.
BEGIN;
SET TRANSACTION READ ONLY;

DO $$
DECLARE
    present_count INTEGER;
    present_function_count INTEGER;
    actual_catalog_sha256 TEXT;
    invalid_row_count BIGINT;
    actual_function_body_sha256 TEXT;
    actual_function_metadata TEXT;
BEGIN
    IF to_regclass('qmt_strategy.execution_runtime') IS NULL
       OR to_regclass('qmt_strategy.execution_algo_instance') IS NULL
       OR to_regclass('qmt_strategy.execution_algo_transition') IS NULL
       OR to_regclass('qmt_strategy.execution_algo_command_outbox') IS NULL
       OR to_regclass('qmt_strategy.execution_kernel_worker_incarnation') IS NULL THEN
        RAISE EXCEPTION 'K6-A preflight: required K2 durable kernel tables are missing';
    END IF;
    IF to_regprocedure('qmt_strategy.miniqmt_k2_catalog_fingerprint()') IS NULL
       OR to_regprocedure('qmt_strategy.miniqmt_k2d_catalog_fingerprint()') IS NULL THEN
        RAISE EXCEPTION 'K6-A preflight: required K2/K2-D catalog authority is missing';
    END IF;
    SELECT qmt_strategy.miniqmt_k2_catalog_fingerprint() INTO actual_catalog_sha256;
    IF actual_catalog_sha256 <> '2ae93a1e637f4232ea01fc80f7f7a4680679956cc428b12c56adb01f16efea6a' THEN
        RAISE EXCEPTION 'K6-A preflight: base K2 catalog drift: got %', actual_catalog_sha256;
    END IF;
    SELECT qmt_strategy.miniqmt_k2d_catalog_fingerprint() INTO actual_catalog_sha256;
    IF actual_catalog_sha256 <> 'f9034e9e9680a12e335c5bdc0ac06e10dda73d34c8a65128df08c26b0f93725d' THEN
        RAISE EXCEPTION 'K6-A preflight: K2-D catalog drift: got %', actual_catalog_sha256;
    END IF;

    SELECT count(*) INTO present_count
    FROM unnest(ARRAY[
        'execution_dependent_buy_coordination','execution_dependent_buy_dependency',
        'execution_dependent_buy_decision','execution_product_command_authority',
        'execution_product_command_authority_item','execution_product_route_cutover',
        'execution_product_route_owner'
    ]) AS table_name
    WHERE to_regclass('qmt_strategy.'||table_name) IS NOT NULL;
    IF present_count NOT IN (0,7) THEN
        RAISE EXCEPTION 'K6-A preflight: partial schema object count=%', present_count;
    END IF;
    SELECT count(*) INTO present_function_count
    FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
    WHERE n.nspname='qmt_strategy' AND p.proname IN (
        'miniqmt_k6_reject_immutable_mutation','miniqmt_k6_validate_route_owner',
        'miniqmt_k6_validate_coordination_update','miniqmt_k6_validate_decision_closure',
        'miniqmt_k6_catalog_fingerprint'
    );
    IF (present_count=0 AND present_function_count<>0)
       OR (present_count=7 AND present_function_count<>5) THEN
        RAISE EXCEPTION 'K6-A preflight: partial schema function count=% for table count=%',
            present_function_count,present_count;
    END IF;
    IF present_count=7 THEN
        IF to_regprocedure('qmt_strategy.miniqmt_k6_catalog_fingerprint()') IS NULL THEN
            RAISE EXCEPTION 'K6-A preflight: K6 catalog authority is missing';
        END IF;
        SELECT qmt_strategy.miniqmt_k6_catalog_fingerprint() INTO actual_catalog_sha256;
        IF actual_catalog_sha256 <> '546a209dc2f8721ccee8b5e905117788486307147dfb4fc6bc396842f5cf84ad' THEN
            RAISE EXCEPTION 'K6-A preflight: K6 catalog drift: expected 546a209dc2f8721ccee8b5e905117788486307147dfb4fc6bc396842f5cf84ad, got %', actual_catalog_sha256;
        END IF;
        SELECT encode(sha256(convert_to(replace(p.prosrc,n.nspname,'<schema>'),'UTF8')),'hex'),
               l.lanname||':'||p.provolatile::TEXT||':'||p.prokind::TEXT||':'||pg_get_function_identity_arguments(p.oid)
        INTO actual_function_body_sha256,actual_function_metadata
        FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang
        WHERE n.nspname='qmt_strategy' AND p.proname='miniqmt_k6_catalog_fingerprint';
        IF actual_function_body_sha256 <> 'bcb0b57b1cb425f4eb3d34b2ce5ca24c9f430986665871384482dfc056f5628a'
           OR actual_function_metadata <> 'sql:s:f:' THEN
            RAISE EXCEPTION 'K6-A preflight: catalog fingerprint function definition drift: metadata=%, body=%',
                actual_function_metadata,actual_function_body_sha256;
        END IF;
        SELECT count(*) INTO invalid_row_count
        FROM qmt_strategy.execution_dependent_buy_coordination
        WHERE coordination_sha256 !~ '^[0-9a-f]{64}$'
           OR status NOT IN ('DEFERRED_WAITING_SELL_PROCEEDS','RELEASED_TO_K2_OUTBOX',
                             'BLOCKED_SELL_PROCEEDS_UNAVAILABLE','EOD_RESIDUAL');
        IF invalid_row_count<>0 THEN
            RAISE EXCEPTION 'K6-A preflight: invalid coordination row count=%', invalid_row_count;
        END IF;
    END IF;
END $$;

SELECT 'canonical_lf_forward_sha256' AS verification,
       '4d5b6f251c84016765ce3c061e286e172a1685cf45a2d2629a361ae471adb75f'::TEXT AS expected_migration_sha256;

ROLLBACK;

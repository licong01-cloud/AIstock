-- K6-B dependent-BUY source-reader schema preflight. Read-only by contract.
-- canonical_lf_forward_sha256=e09fdd48362db5d957f4caede8dce9d1cb52863a0e965c1e662ad54700f0f885
BEGIN;
SET TRANSACTION READ ONLY;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

DO $$
DECLARE
    actual_trigger TEXT;
    actual_k6 TEXT;
    actual_k6c TEXT;
    actual_k6b TEXT;
BEGIN
    IF to_regclass('qmt_strategy.execution_dependent_buy_dependency') IS NULL
       OR to_regprocedure('qmt_strategy.miniqmt_k6_validate_coordination_update()') IS NULL
       OR to_regprocedure('qmt_strategy.miniqmt_k6_catalog_fingerprint()') IS NULL
       OR to_regprocedure('qmt_strategy.miniqmt_k6c_catalog_fingerprint()') IS NULL THEN
        RAISE EXCEPTION 'K6-B preflight: exact K6-C0 predecessor schema is unavailable';
    END IF;
    SELECT qmt_strategy.miniqmt_k6_catalog_fingerprint(),
           qmt_strategy.miniqmt_k6c_catalog_fingerprint()
    INTO actual_k6,actual_k6c;
    IF (actual_k6,actual_k6c) =
       ('6e33248ad909c59db11059f723adbe39c4c8a151c902e9af0fe0fd3637adacc9',
        'f4fc093c83642577009dc5ce8c03550bbb75e00f09ada7bf2489272ddd67bd7d') THEN
        NULL;
    ELSIF (actual_k6,actual_k6c) =
          ('6eeff2d2887049a7b3e3c93dd93e56e9af6241e0be1caf2c7ef535cbbde5d9f6',
           'ef09f8ab2f3e6a1563cd536327ee1d9c04273806c3fdfdea2e704600f330d912') THEN
        IF to_regprocedure('qmt_strategy.miniqmt_k6b_catalog_fingerprint()') IS NULL THEN
            RAISE EXCEPTION 'K6-B preflight: successor fingerprint function is unavailable';
        END IF;
        SELECT qmt_strategy.miniqmt_k6b_catalog_fingerprint() INTO actual_k6b;
        IF actual_k6b <> '10ae5be030612f923f2fe23f17f1f8b4891358cc8bd9565d54ad27ee3d18393c' THEN
            RAISE EXCEPTION 'K6-B preflight: successor catalog drift: actual=%',actual_k6b;
        END IF;
    ELSE
        RAISE EXCEPTION 'K6-B preflight: predecessor/successor catalog drift: k6=%, k6c=%',actual_k6,actual_k6c;
    END IF;
    SELECT pg_get_triggerdef(t.oid, true) INTO actual_trigger
    FROM pg_trigger t
    JOIN pg_class c ON c.oid=t.tgrelid
    JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='qmt_strategy' AND c.relname='execution_dependent_buy_dependency'
      AND t.tgname IN ('trg_miniqmt_k6_dependency_append_only','trg_miniqmt_k6_dependency_successor')
      AND NOT t.tgisinternal;
    IF actual_trigger IS NULL
       OR (position('miniqmt_k6_reject_immutable_mutation' IN actual_trigger)=0
           AND position('miniqmt_k6b_validate_dependency_successor' IN actual_trigger)=0) THEN
        RAISE EXCEPTION 'K6-B preflight: exact predecessor/successor dependency trigger is absent or drifted';
    END IF;
END $$;

COMMIT;

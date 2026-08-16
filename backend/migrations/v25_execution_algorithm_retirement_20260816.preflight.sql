BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;

DO $$
DECLARE
    target_count INTEGER;
BEGIN
    IF to_regclass('public.execution_algorithm_catalog') IS NULL THEN
        RAISE EXCEPTION 'execution_algorithm_catalog is required for V25 retirement';
    END IF;

    SELECT count(*)
      INTO target_count
      FROM public.execution_algorithm_catalog
     WHERE algo_code IN ('V25_TWO_STAGE', 'V25_1_SMALL_CAP');

    IF target_count <> 2 THEN
        RAISE EXCEPTION 'exact V25 retirement catalog identity is required: expected=2 actual=%', target_count;
    END IF;
END
$$;

SELECT algo_code, is_enabled, updated_at
  FROM public.execution_algorithm_catalog
 WHERE algo_code IN ('V25_TWO_STAGE', 'V25_1_SMALL_CAP')
 ORDER BY algo_code;

ROLLBACK;

BEGIN;

LOCK TABLE public.execution_algorithm_catalog IN SHARE ROW EXCLUSIVE MODE;

DO $$
DECLARE
    target_count INTEGER;
BEGIN
    SELECT count(*)
      INTO target_count
      FROM public.execution_algorithm_catalog
     WHERE algo_code IN ('V25_TWO_STAGE', 'V25_1_SMALL_CAP');

    IF target_count <> 2 THEN
        RAISE EXCEPTION 'exact V25 retirement catalog identity is required: expected=2 actual=%', target_count;
    END IF;
END
$$;

UPDATE public.execution_algorithm_catalog
   SET is_enabled = FALSE,
       updated_at = NOW()
 WHERE algo_code IN ('V25_TWO_STAGE', 'V25_1_SMALL_CAP');

DO $$
DECLARE
    enabled_count INTEGER;
BEGIN
    SELECT count(*)
      INTO enabled_count
      FROM public.execution_algorithm_catalog
     WHERE algo_code IN ('V25_TWO_STAGE', 'V25_1_SMALL_CAP')
       AND is_enabled IS TRUE;

    IF enabled_count <> 0 THEN
        RAISE EXCEPTION 'V25 retirement readback failed: enabled_count=%', enabled_count;
    END IF;
END
$$;

SELECT algo_code, is_enabled, updated_at
  FROM public.execution_algorithm_catalog
 WHERE algo_code IN ('V25_TWO_STAGE', 'V25_1_SMALL_CAP')
 ORDER BY algo_code;

COMMIT;

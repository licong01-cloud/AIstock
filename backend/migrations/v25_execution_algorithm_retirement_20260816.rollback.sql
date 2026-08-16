-- Safe no-op: source authority permanently rejects new V25 execution work.
-- Re-enabling historical catalog rows would create split authority and therefore
-- requires a separately approved successor design and migration.
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;

SELECT algo_code, is_enabled, updated_at
  FROM public.execution_algorithm_catalog
 WHERE algo_code IN ('V25_TWO_STAGE', 'V25_1_SMALL_CAP')
 ORDER BY algo_code;

ROLLBACK;

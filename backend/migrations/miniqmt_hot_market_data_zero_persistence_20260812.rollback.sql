-- BUG-1041 guarded rollback intentionally keeps the no-TICK successor.
-- Restoring the predecessor TICK writer is unsafe and therefore forbidden.
DO $$
DECLARE
    definition text;
BEGIN
    SELECT pg_get_constraintdef(oid,true) INTO definition
      FROM pg_constraint
     WHERE conrelid='qmt_strategy.execution_runtime_event'::regclass
       AND conname='ck_miniqmt_no_new_kernel_tick' AND contype='c';
    IF definition IS NULL THEN
        RAISE EXCEPTION 'BUG-1041 rollback: successor composite is absent';
    END IF;
    IF encode(sha256(convert_to(definition,'UTF8')),'hex')<>
       '9dd2d0274fe18ad4ab487f006e420e6f11b806818cd876ebabf3d3f286cc4bed' THEN
        RAISE EXCEPTION 'BUG-1041 rollback: no-TICK successor identity drift';
    END IF;
    RAISE NOTICE 'BUG-1041 rollback is a safe no-op; ordinary market-data persistence remains retired';
END $$;

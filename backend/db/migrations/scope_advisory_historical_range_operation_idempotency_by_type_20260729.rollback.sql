BEGIN;

DO $$
DECLARE
    old_definition TEXT;
    new_definition TEXT;
BEGIN
    IF to_regclass('app.advisory_historical_range_operation') IS NULL THEN
        RAISE EXCEPTION 'app.advisory_historical_range_operation does not exist';
    END IF;

    SELECT pg_get_constraintdef(oid)
      INTO old_definition
      FROM pg_constraint
     WHERE conrelid = 'app.advisory_historical_range_operation'::regclass
       AND conname = 'advisory_historical_range_ope_batch_id_operation_idempotenc_key';
    SELECT pg_get_constraintdef(oid)
      INTO new_definition
      FROM pg_constraint
     WHERE conrelid = 'app.advisory_historical_range_operation'::regclass
       AND conname = 'uq_advisory_hr_operation_type_idempotency';

    IF old_definition IS NOT NULL
       AND old_definition <> 'UNIQUE (batch_id, operation_idempotency_key)' THEN
        RAISE EXCEPTION 'legacy historical-range operation idempotency constraint has an unexpected definition: %', old_definition;
    END IF;
    IF new_definition IS NOT NULL
       AND new_definition <> 'UNIQUE (batch_id, operation_type, operation_idempotency_key)' THEN
        RAISE EXCEPTION 'scoped historical-range operation idempotency constraint has an unexpected definition: %', new_definition;
    END IF;
    IF old_definition IS NULL AND new_definition IS NULL THEN
        RAISE EXCEPTION 'historical-range operation idempotency constraint is missing';
    END IF;
    IF old_definition IS NOT NULL AND new_definition IS NOT NULL THEN
        RAISE EXCEPTION 'legacy and scoped historical-range operation idempotency constraints coexist';
    END IF;

    IF new_definition IS NOT NULL THEN
        IF EXISTS (
            SELECT 1
              FROM app.advisory_historical_range_operation
             GROUP BY batch_id, operation_idempotency_key
            HAVING COUNT(*) > 1
        ) THEN
            RAISE EXCEPTION 'rollback refused: parent and RUN operations already share an idempotency key';
        END IF;

        ALTER TABLE app.advisory_historical_range_operation
            DROP CONSTRAINT uq_advisory_hr_operation_type_idempotency;
        ALTER TABLE app.advisory_historical_range_operation
            ADD CONSTRAINT advisory_historical_range_ope_batch_id_operation_idempotenc_key
            UNIQUE (batch_id, operation_idempotency_key);
    END IF;
END;
$$;

COMMENT ON CONSTRAINT advisory_historical_range_ope_batch_id_operation_idempotenc_key
    ON app.advisory_historical_range_operation IS
    'Legacy batch-level operation idempotency restored only when no parent and RUN operation share a key.';

DO $$
DECLARE
    legacy_definition TEXT;
BEGIN
    SELECT pg_get_constraintdef(oid)
      INTO legacy_definition
      FROM pg_constraint
     WHERE conrelid = 'app.advisory_historical_range_operation'::regclass
       AND conname = 'advisory_historical_range_ope_batch_id_operation_idempotenc_key';
    IF legacy_definition <> 'UNIQUE (batch_id, operation_idempotency_key)' THEN
        RAISE EXCEPTION 'legacy historical-range operation idempotency constraint readback failed: %', legacy_definition;
    END IF;
    IF EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conrelid = 'app.advisory_historical_range_operation'::regclass
           AND conname = 'uq_advisory_hr_operation_type_idempotency'
    ) THEN
        RAISE EXCEPTION 'scoped historical-range operation idempotency constraint still exists';
    END IF;
END;
$$;

COMMIT;

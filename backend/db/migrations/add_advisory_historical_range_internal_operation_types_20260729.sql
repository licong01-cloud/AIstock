BEGIN;

DO $$
BEGIN
    IF to_regclass('app.advisory_historical_range_operation') IS NULL THEN
        RAISE EXCEPTION 'app.advisory_historical_range_operation does not exist';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'app.advisory_historical_range_operation'::regclass
          AND conname IN (
              'advisory_historical_range_operation_operation_type_check',
              'ck_advisory_hr_operation_type_v2'
          )
    ) THEN
        RAISE EXCEPTION 'authoritative historical-range operation type constraint is missing';
    END IF;
END;
$$;

ALTER TABLE app.advisory_historical_range_operation
    DROP CONSTRAINT IF EXISTS advisory_historical_range_operation_operation_type_check,
    DROP CONSTRAINT IF EXISTS ck_advisory_hr_operation_type_v2,
    ADD CONSTRAINT ck_advisory_hr_operation_type_v2 CHECK (
        operation_type IN (
            'CREATE',
            'BUILD_SOURCE_CATALOG',
            'RESUME',
            'CANCEL',
            'REFRESH_OUTCOMES',
            'REFRESH_OUTCOMES_RUN',
            'BUILD_DATASET_BRIDGE',
            'BUILD_DATASET_BRIDGE_RUN'
        )
    ) NOT VALID;

ALTER TABLE app.advisory_historical_range_operation
    VALIDATE CONSTRAINT ck_advisory_hr_operation_type_v2;

ALTER TABLE app.advisory_historical_range_operation
    DROP CONSTRAINT IF EXISTS ck_advisory_hr_r4_operation_result_kind,
    ADD CONSTRAINT ck_advisory_hr_r4_operation_result_kind CHECK (
        result_ref IS NULL
        OR (operation_type IN ('RESUME', 'CANCEL') AND result_ref->>'artifact_kind' = 'RANGE_RECEIPT')
        OR (operation_type = 'CREATE' AND result_ref->>'artifact_kind' = 'SOURCE_REQUIREMENT_PLAN')
        OR (operation_type = 'BUILD_SOURCE_CATALOG'
            AND result_ref->>'artifact_kind' = 'SOURCE_CATALOG_CHECKPOINT')
        OR (operation_type IN ('REFRESH_OUTCOMES', 'REFRESH_OUTCOMES_RUN')
            AND result_ref->>'artifact_kind' = 'OUTCOME_REFRESH_RECEIPT')
        OR (operation_type IN ('BUILD_DATASET_BRIDGE', 'BUILD_DATASET_BRIDGE_RUN')
            AND result_ref->>'artifact_kind' = 'DATASET_BRIDGE_RECEIPT')
    );

CREATE OR REPLACE FUNCTION app.verify_advisory_hr_r4_operation_attempt_kind()
RETURNS TRIGGER AS $$
DECLARE
    operation_kind TEXT;
    receipt_kind TEXT;
    expected_receipt_kind TEXT;
BEGIN
    IF NEW.attempt_receipt_ref IS NULL THEN
        RETURN NEW;
    END IF;
    SELECT operation_type INTO operation_kind
      FROM app.advisory_historical_range_operation
     WHERE operation_id = NEW.operation_id;
    receipt_kind := NEW.attempt_receipt_ref->>'artifact_kind';
    expected_receipt_kind := CASE operation_kind
        WHEN 'RESUME' THEN 'RANGE_RECEIPT'
        WHEN 'CANCEL' THEN 'RANGE_RECEIPT'
        WHEN 'CREATE' THEN 'SOURCE_REQUIREMENT_PLAN'
        WHEN 'BUILD_SOURCE_CATALOG' THEN 'SOURCE_CATALOG_CHECKPOINT'
        WHEN 'REFRESH_OUTCOMES' THEN 'OUTCOME_REFRESH_RECEIPT'
        WHEN 'REFRESH_OUTCOMES_RUN' THEN 'OUTCOME_REFRESH_RECEIPT'
        WHEN 'BUILD_DATASET_BRIDGE' THEN 'DATASET_BRIDGE_RECEIPT'
        WHEN 'BUILD_DATASET_BRIDGE_RUN' THEN 'DATASET_BRIDGE_RECEIPT'
        ELSE NULL
    END;
    IF expected_receipt_kind IS NULL
       OR receipt_kind IS DISTINCT FROM expected_receipt_kind THEN
        RAISE EXCEPTION 'ADVISORY_HR_R4_OPERATION_ATTEMPT_RECEIPT_KIND_INVALID';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON CONSTRAINT ck_advisory_hr_operation_type_v2
    ON app.advisory_historical_range_operation IS
    'Separates R5 batch-command operations from internal run-level Outcome and Dataset Bridge operations while preserving per-type single-active enforcement.';

DO $$
DECLARE
    type_definition TEXT;
    result_definition TEXT;
    attempt_function TEXT;
BEGIN
    SELECT pg_get_constraintdef(oid)
      INTO type_definition
      FROM pg_constraint
     WHERE conrelid = 'app.advisory_historical_range_operation'::regclass
       AND conname = 'ck_advisory_hr_operation_type_v2';
    SELECT pg_get_constraintdef(oid)
      INTO result_definition
      FROM pg_constraint
     WHERE conrelid = 'app.advisory_historical_range_operation'::regclass
       AND conname = 'ck_advisory_hr_r4_operation_result_kind';
    SELECT pg_get_functiondef('app.verify_advisory_hr_r4_operation_attempt_kind()'::regprocedure)
      INTO attempt_function;

    IF type_definition NOT LIKE '%REFRESH_OUTCOMES_RUN%'
       OR type_definition NOT LIKE '%BUILD_DATASET_BRIDGE_RUN%' THEN
        RAISE EXCEPTION 'internal operation types were not installed';
    END IF;
    IF result_definition NOT LIKE '%REFRESH_OUTCOMES_RUN%'
       OR result_definition NOT LIKE '%BUILD_DATASET_BRIDGE_RUN%' THEN
        RAISE EXCEPTION 'internal operation result-ref contract was not installed';
    END IF;
    IF attempt_function NOT LIKE '%REFRESH_OUTCOMES_RUN%'
       OR attempt_function NOT LIKE '%BUILD_DATASET_BRIDGE_RUN%' THEN
        RAISE EXCEPTION 'internal operation attempt receipt contract was not installed';
    END IF;
END;
$$;

COMMIT;

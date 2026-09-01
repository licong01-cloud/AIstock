BEGIN;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM app.advisory_historical_range_operation
        WHERE operation_type IN ('REFRESH_OUTCOMES_RUN', 'BUILD_DATASET_BRIDGE_RUN')
    ) THEN
        RAISE EXCEPTION 'rollback refused: internal run-level operation facts already exist';
    END IF;
END;
$$;

ALTER TABLE app.advisory_historical_range_operation
    DROP CONSTRAINT IF EXISTS ck_advisory_hr_operation_type_v2,
    ADD CONSTRAINT advisory_historical_range_operation_operation_type_check CHECK (
        operation_type IN (
            'CREATE',
            'BUILD_SOURCE_CATALOG',
            'RESUME',
            'CANCEL',
            'REFRESH_OUTCOMES',
            'BUILD_DATASET_BRIDGE'
        )
    ) NOT VALID;

ALTER TABLE app.advisory_historical_range_operation
    VALIDATE CONSTRAINT advisory_historical_range_operation_operation_type_check;

ALTER TABLE app.advisory_historical_range_operation
    DROP CONSTRAINT IF EXISTS ck_advisory_hr_r4_operation_result_kind,
    ADD CONSTRAINT ck_advisory_hr_r4_operation_result_kind CHECK (
        result_ref IS NULL
        OR (operation_type IN ('RESUME', 'CANCEL') AND result_ref->>'artifact_kind' = 'RANGE_RECEIPT')
        OR (operation_type = 'CREATE' AND result_ref->>'artifact_kind' = 'SOURCE_REQUIREMENT_PLAN')
        OR (operation_type = 'BUILD_SOURCE_CATALOG'
            AND result_ref->>'artifact_kind' = 'SOURCE_CATALOG_CHECKPOINT')
        OR (operation_type = 'REFRESH_OUTCOMES' AND result_ref->>'artifact_kind' = 'OUTCOME_REFRESH_RECEIPT')
        OR (operation_type = 'BUILD_DATASET_BRIDGE' AND result_ref->>'artifact_kind' = 'DATASET_BRIDGE_RECEIPT')
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
        WHEN 'BUILD_DATASET_BRIDGE' THEN 'DATASET_BRIDGE_RECEIPT'
        ELSE NULL
    END;
    IF expected_receipt_kind IS NULL
       OR receipt_kind IS DISTINCT FROM expected_receipt_kind THEN
        RAISE EXCEPTION 'ADVISORY_HR_R4_OPERATION_ATTEMPT_RECEIPT_KIND_INVALID';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMIT;

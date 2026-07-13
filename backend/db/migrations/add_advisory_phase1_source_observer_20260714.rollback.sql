DROP TRIGGER IF EXISTS trg_reject_advisory_source_observation_receipt_mutation
    ON app.advisory_source_observation_receipt;
DROP TRIGGER IF EXISTS trg_verify_advisory_source_observation_receipt
    ON app.advisory_source_observation_receipt;
DROP TRIGGER IF EXISTS trg_verify_advisory_source_observer_cursor_update
    ON app.advisory_source_observer_cursor;

DROP FUNCTION IF EXISTS app.reject_advisory_source_observation_receipt_mutation();
DROP FUNCTION IF EXISTS app.verify_advisory_source_observation_receipt();
DROP FUNCTION IF EXISTS app.verify_advisory_source_observer_cursor_update();

DROP TABLE IF EXISTS app.advisory_source_observation_receipt;
DROP TABLE IF EXISTS app.advisory_source_observer_cursor;

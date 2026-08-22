BEGIN;

DO $$
DECLARE
    evaluation_count BIGINT := 0;
    outcome_count BIGINT := 0;
BEGIN
    IF to_regclass('app.advisory_forward_model_evaluation') IS NOT NULL THEN
        EXECUTE 'SELECT COUNT(*) FROM app.advisory_forward_model_evaluation' INTO evaluation_count;
    END IF;
    IF to_regclass('app.advisory_forward_model_observation_outcome') IS NOT NULL THEN
        EXECUTE 'SELECT COUNT(*) FROM app.advisory_forward_model_observation_outcome' INTO outcome_count;
    END IF;
    IF evaluation_count > 0 OR outcome_count > 0 THEN
        RAISE EXCEPTION 'refusing forward model evaluation rollback with persisted facts: evaluations=% outcomes=%',
            evaluation_count, outcome_count;
    END IF;
END $$;

DROP TABLE IF EXISTS app.advisory_forward_model_observation_outcome;
DROP TABLE IF EXISTS app.advisory_forward_model_evaluation;

ALTER TABLE app.advisory_forward_model_observation
    DROP CONSTRAINT IF EXISTS advisory_forward_model_observation_evaluation_status_check,
    DROP COLUMN IF EXISTS evaluated_at,
    DROP COLUMN IF EXISTS evaluation_error_json,
    DROP COLUMN IF EXISTS evaluation_reason_code,
    DROP COLUMN IF EXISTS evaluation_status;

COMMIT;

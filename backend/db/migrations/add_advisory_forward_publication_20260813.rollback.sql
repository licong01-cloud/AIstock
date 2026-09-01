BEGIN;

DO $$
DECLARE
    has_rows BOOLEAN;
BEGIN
    IF to_regclass('app.advisory_forward_model_observation') IS NOT NULL THEN
        EXECUTE 'SELECT EXISTS (SELECT 1 FROM app.advisory_forward_model_observation LIMIT 1)'
            INTO has_rows;
        IF has_rows THEN
            RAISE EXCEPTION 'advisory forward rollback refused: durable model observations exist';
        END IF;
    END IF;
    IF to_regclass('app.advisory_forward_run') IS NOT NULL THEN
        EXECUTE 'SELECT EXISTS (SELECT 1 FROM app.advisory_forward_run LIMIT 1)'
            INTO has_rows;
        IF has_rows THEN
            RAISE EXCEPTION 'advisory forward rollback refused: durable forward facts exist';
        END IF;
    END IF;
END $$;

DROP TABLE IF EXISTS app.advisory_forward_model_observation;
DROP TABLE IF EXISTS app.advisory_forward_run;

COMMIT;

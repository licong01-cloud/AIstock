BEGIN;

DO $$
BEGIN
    IF to_regclass('qe_archive.run_evaluation_metric') IS NOT NULL THEN
        LOCK TABLE qe_archive.run_evaluation_metric IN ACCESS EXCLUSIVE MODE;
        IF EXISTS (SELECT 1 FROM qe_archive.run_evaluation_metric LIMIT 1) THEN
            RAISE EXCEPTION 'guarded rollback refused: qe_archive.run_evaluation_metric contains data';
        END IF;
    END IF;
    IF to_regclass('qe_archive.run_evaluation_artifact') IS NOT NULL THEN
        LOCK TABLE qe_archive.run_evaluation_artifact IN ACCESS EXCLUSIVE MODE;
        IF EXISTS (SELECT 1 FROM qe_archive.run_evaluation_artifact LIMIT 1) THEN
            RAISE EXCEPTION 'guarded rollback refused: qe_archive.run_evaluation_artifact contains data';
        END IF;
    END IF;
END
$$;

DROP TABLE IF EXISTS qe_archive.run_evaluation_artifact;
DROP TABLE IF EXISTS qe_archive.run_evaluation_metric;
DELETE FROM qe_archive.schema_version WHERE version = 'qe_archive_v5_20260728';
COMMIT;

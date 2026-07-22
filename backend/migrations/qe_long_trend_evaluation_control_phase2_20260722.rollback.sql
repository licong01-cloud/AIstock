DO $$
DECLARE
    row_count BIGINT;
BEGIN
    IF to_regclass('qe_archive.run_evaluation') IS NULL THEN
        RETURN;
    END IF;
    SELECT count(*) INTO row_count FROM qe_archive.run_evaluation;
    IF row_count > 0 THEN
        RAISE EXCEPTION
            'guarded rollback refused: qe_archive.run_evaluation contains % durable rows',
            row_count;
    END IF;
    DROP TABLE qe_archive.run_evaluation;
END $$;

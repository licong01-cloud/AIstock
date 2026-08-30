-- Guarded rollback for DEV only; retained successor rows block destructive rollback.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
DO $$
DECLARE
    row_count BIGINT;
    table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'paper_v2.localsim_replay_job_v1',
        'paper_v2.legacy_localsim_account_lineage_v1',
        'paper_v2.simulation_account_v1'
    ] LOOP
        IF to_regclass(table_name) IS NOT NULL THEN
            EXECUTE format('SELECT count(*) FROM %s', table_name) INTO row_count;
            IF row_count <> 0 THEN
                RAISE EXCEPTION 'SIM-LR-B rollback refuses non-empty table % with % rows', table_name, row_count;
            END IF;
        END IF;
    END LOOP;
END $$;
DROP TABLE IF EXISTS paper_v2.localsim_replay_job_v1;
DROP TABLE IF EXISTS paper_v2.legacy_localsim_account_lineage_v1;
DROP TABLE IF EXISTS paper_v2.simulation_account_v1;
DROP INDEX IF EXISTS paper_v2.uq_localsim_successor_open_binding;
COMMIT;

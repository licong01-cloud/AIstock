-- Phase A rollback for QE archive multi-alpha combine-backtest materialization.
-- Manual only. Before applying rollback, verify no production consumers need the
-- archived multi-alpha rows and no strategy_pkg runs still use partial_failed.

BEGIN;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'strategy_pkg'
          AND table_name = 'multi_alpha_combine_backtest_run'
    ) THEN
        IF EXISTS (
            SELECT 1
            FROM strategy_pkg.multi_alpha_combine_backtest_run
            WHERE status = 'partial_failed'
        ) THEN
            RAISE EXCEPTION 'Cannot rollback ck_macb_run_status while partial_failed rows exist';
        END IF;
    END IF;
END $$;

ALTER TABLE IF EXISTS strategy_pkg.multi_alpha_combine_backtest_run
    DROP CONSTRAINT IF EXISTS ck_macb_run_status;

ALTER TABLE IF EXISTS strategy_pkg.multi_alpha_combine_backtest_run
    ADD CONSTRAINT ck_macb_run_status CHECK (status IN ('running', 'succeeded', 'failed'));

DROP TABLE IF EXISTS qe_archive.multi_alpha_loo;
DROP TABLE IF EXISTS qe_archive.multi_alpha_scheme;
DROP TABLE IF EXISTS qe_archive.multi_alpha_leg_source;
DROP TABLE IF EXISTS qe_archive.multi_alpha_leg;
DROP TABLE IF EXISTS qe_archive.multi_alpha_run;
DROP INDEX IF EXISTS qe_archive.idx_qear_macb_loo_leg;
DROP INDEX IF EXISTS qe_archive.idx_qear_macb_scheme_best;
DROP INDEX IF EXISTS qe_archive.idx_qear_macb_leg_source_seed;
DROP INDEX IF EXISTS qe_archive.idx_qear_macb_leg_source_exp_loop;
DROP INDEX IF EXISTS qe_archive.idx_qear_macb_leg_factor_hash;
DROP INDEX IF EXISTS qe_archive.idx_qear_macb_run_status;
DROP INDEX IF EXISTS qe_archive.idx_qear_macb_run_roster;
DROP INDEX IF EXISTS qe_archive.idx_qear_run_type_status;

DELETE FROM qe_archive.run
WHERE run_type = 'multi_alpha_combine'
  AND source_system = 'multi_alpha';

ALTER TABLE IF EXISTS qe_archive.run
    DROP CONSTRAINT IF EXISTS ck_qear_run_status;

ALTER TABLE IF EXISTS qe_archive.run
    ADD CONSTRAINT ck_qear_run_status CHECK (
        status IN ('pending','running','completed','failed','interrupted','partial_archived','archived')
    );

COMMIT;

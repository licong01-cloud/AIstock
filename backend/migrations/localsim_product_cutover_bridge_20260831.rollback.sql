-- Guarded DEV rollback for the SIM-LR-C ledger-scope bridge.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
DO $$
DECLARE
    blocking_count BIGINT;
BEGIN
    IF to_regclass('paper_v2.simulation_ledger_scope_v1') IS NULL THEN
        RETURN;
    END IF;
    SELECT count(*) INTO blocking_count
    FROM paper_v2.simulation_ledger_scope_v1
    WHERE scope_kind = 'SUCCESSOR_NATIVE';
    IF blocking_count <> 0 THEN
        RAISE EXCEPTION 'SIM-LR-C rollback refuses % native successor ledger scopes', blocking_count;
    END IF;
    SELECT count(*) INTO blocking_count
    FROM paper_v2.legacy_localsim_account_lineage_v1;
    IF blocking_count <> 0 THEN
        RAISE EXCEPTION 'SIM-LR-C rollback refuses % retained lineage references', blocking_count;
    END IF;
    IF to_regclass('paper_v2.localsim_runtime_profile_version_v1') IS NOT NULL THEN
        SELECT count(*) INTO blocking_count
        FROM strategy_pkg.strategy_runtime_release AS release
        JOIN paper_v2.localsim_runtime_profile_version_v1 AS profile_version
          ON profile_version.profile_version_id = release.runtime_profile_version_id;
        IF blocking_count <> 0 THEN
            RAISE EXCEPTION 'SIM-LR-C rollback refuses % runtime releases that reference native profiles', blocking_count;
        END IF;
    END IF;
END $$;

ALTER TABLE paper_v2.run DROP CONSTRAINT IF EXISTS fk_paper_v2_run_ledger_scope_v1;
ALTER TABLE paper_v2.intraday_snapshots
    DROP CONSTRAINT IF EXISTS fk_paper_v2_intraday_snapshots_ledger_scope_v1;

ALTER TABLE paper_v2.run
    ADD CONSTRAINT run_portfolio_id_fkey
    FOREIGN KEY (portfolio_id) REFERENCES paper_v2.portfolio(portfolio_id);
ALTER TABLE paper_v2.intraday_snapshots
    ADD CONSTRAINT intraday_snapshots_portfolio_id_fkey
    FOREIGN KEY (portfolio_id) REFERENCES paper_v2.portfolio(portfolio_id);

DROP TABLE paper_v2.simulation_ledger_scope_v1;
DROP TABLE IF EXISTS paper_v2.localsim_runtime_profile_version_v1;
DROP TABLE IF EXISTS paper_v2.localsim_runtime_profile_v1;
COMMIT;

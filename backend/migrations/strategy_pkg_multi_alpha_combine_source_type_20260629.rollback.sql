-- Roll back StrategyPackage source_type CHECK to the pre multi_alpha_combine_run set.
-- Manual rollback only; verify no packages still use source_type='multi_alpha_combine_run' before applying.

CREATE SCHEMA IF NOT EXISTS strategy_pkg;

DO $$
BEGIN
    IF to_regclass('strategy_pkg.package') IS NOT NULL THEN
        ALTER TABLE strategy_pkg.package
            DROP CONSTRAINT IF EXISTS package_source_type_check;
        ALTER TABLE strategy_pkg.package
            ADD CONSTRAINT package_source_type_check
            CHECK (source_type IN ('qe_experiment', 'qe_evolution_loop', 'candidate_strategy_package'));

        COMMENT ON CONSTRAINT package_source_type_check ON strategy_pkg.package IS
            'Allowed StrategyPackage manifest source types before multi_alpha_combine_run lineage migration.';
    END IF;
END $$;

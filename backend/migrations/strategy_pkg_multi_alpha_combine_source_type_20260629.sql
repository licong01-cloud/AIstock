-- Allow StrategyPackage parent/component manifests sourced directly from multi-alpha combine-backtest runs.
-- Explicit migration only: business services must not run this DDL implicitly.

CREATE SCHEMA IF NOT EXISTS strategy_pkg;

DO $$
BEGIN
    IF to_regclass('strategy_pkg.package') IS NOT NULL THEN
        ALTER TABLE strategy_pkg.package
            DROP CONSTRAINT IF EXISTS package_source_type_check;
        ALTER TABLE strategy_pkg.package
            ADD CONSTRAINT package_source_type_check
            CHECK (source_type IN ('qe_experiment', 'qe_evolution_loop', 'candidate_strategy_package', 'multi_alpha_combine_run'));
    END IF;
END $$;

COMMENT ON CONSTRAINT package_source_type_check ON strategy_pkg.package IS
    'Allowed StrategyPackage manifest source types, including multi_alpha_combine_run for one-step Multi-Alpha combine export lineage.';

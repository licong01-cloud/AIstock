-- Allow rank-fusion combine-backtest schemes in result constraints.
-- Forward migration is idempotent and must be applied manually. Application
-- code must not execute this migration automatically.

BEGIN;

ALTER TABLE IF EXISTS strategy_pkg.multi_alpha_combine_backtest_scheme_result
    DROP CONSTRAINT IF EXISTS ck_macb_scheme_supported;

ALTER TABLE IF EXISTS strategy_pkg.multi_alpha_combine_backtest_scheme_result
    ADD CONSTRAINT ck_macb_scheme_supported CHECK (
        weighting_scheme IN (
            'equal',
            'orthogonality_aware',
            'ic_weighted',
            'risk_parity',
            'rank_fusion_rrf',
            'rank_fusion_borda'
        )
    );

ALTER TABLE IF EXISTS strategy_pkg.multi_alpha_combine_backtest_loo
    DROP CONSTRAINT IF EXISTS ck_macb_loo_scheme_supported;

ALTER TABLE IF EXISTS strategy_pkg.multi_alpha_combine_backtest_loo
    ADD CONSTRAINT ck_macb_loo_scheme_supported CHECK (
        weighting_scheme IN (
            'equal',
            'orthogonality_aware',
            'ic_weighted',
            'risk_parity',
            'rank_fusion_rrf',
            'rank_fusion_borda'
        )
    );

COMMIT;

-- Rollback (manual only):
-- BEGIN;
-- ALTER TABLE IF EXISTS strategy_pkg.multi_alpha_combine_backtest_scheme_result
--     DROP CONSTRAINT IF EXISTS ck_macb_scheme_supported;
-- ALTER TABLE IF EXISTS strategy_pkg.multi_alpha_combine_backtest_scheme_result
--     ADD CONSTRAINT ck_macb_scheme_supported CHECK (
--         weighting_scheme IN ('equal', 'orthogonality_aware', 'ic_weighted', 'risk_parity')
--     );
-- ALTER TABLE IF EXISTS strategy_pkg.multi_alpha_combine_backtest_loo
--     DROP CONSTRAINT IF EXISTS ck_macb_loo_scheme_supported;
-- ALTER TABLE IF EXISTS strategy_pkg.multi_alpha_combine_backtest_loo
--     ADD CONSTRAINT ck_macb_loo_scheme_supported CHECK (
--         weighting_scheme IN ('equal', 'orthogonality_aware', 'ic_weighted', 'risk_parity')
--     );
-- COMMIT;

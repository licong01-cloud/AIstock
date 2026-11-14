-- BUG-2226: persist the frozen_weighted scheme already supported by QE.
-- Validate on the existing DEV database first. Production application needs
-- authorization for these exact tables and this committed migration.
-- No application startup hook executes this file. Existing results are kept.

BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_scheme_result
    DROP CONSTRAINT ck_macb_scheme_supported,
    ADD CONSTRAINT ck_macb_scheme_supported CHECK (
        weighting_scheme IN (
            'equal',
            'orthogonality_aware',
            'ic_weighted',
            'risk_parity',
            'rank_fusion_rrf',
            'rank_fusion_borda',
            'frozen_weighted'
        )
    );

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_loo
    DROP CONSTRAINT ck_macb_loo_scheme_supported,
    ADD CONSTRAINT ck_macb_loo_scheme_supported CHECK (
        weighting_scheme IN (
            'equal',
            'orthogonality_aware',
            'ic_weighted',
            'risk_parity',
            'rank_fusion_rrf',
            'rank_fusion_borda',
            'frozen_weighted'
        )
    );

COMMIT;

-- Reapplying is safe: the two named constraints are replaced atomically.
-- Missing tables/constraints and lock timeouts fail instead of reporting a
-- successful no-op. No automatic downgrade is provided: once frozen results
-- exist, reverting the supported domain would reject preserved valid rows.

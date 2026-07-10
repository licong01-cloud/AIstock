-- Gate-0 sector-factor research: nullable 20-day companion metrics.
--
-- This migration is additive and idempotent. Existing 1-day payloads and rows
-- remain valid because every new column is nullable. Applying this file to a
-- production database is an explicit post-merge DDL gate; runtime code must not
-- execute it implicitly.

ALTER TABLE aistock_factor_metrics
    ADD COLUMN IF NOT EXISTS h20_return_horizon    TEXT,
    ADD COLUMN IF NOT EXISTS h20_ic_mean           DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS h20_ic_std            DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS h20_rank_ic_mean      DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS h20_rank_ic_std       DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS h20_icir              DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS h20_rank_icir         DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS h20_icir_hac          DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS h20_rank_icir_hac     DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS h20_ic_positive_ratio DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS h20_n_obs             INTEGER,
    ADD COLUMN IF NOT EXISTS h20_hac_lag           INTEGER;

COMMENT ON COLUMN aistock_factor_metrics.h20_return_horizon IS
    'h20 companion return interval; T21T1 means close(t+1) to close(t+21)';
COMMENT ON COLUMN aistock_factor_metrics.h20_ic_mean IS
    'Mean daily Pearson IC for the h20 companion horizon';
COMMENT ON COLUMN aistock_factor_metrics.h20_ic_std IS
    'Standard deviation of daily Pearson IC for the h20 companion horizon';
COMMENT ON COLUMN aistock_factor_metrics.h20_rank_ic_mean IS
    'Mean daily Spearman Rank IC for the h20 companion horizon';
COMMENT ON COLUMN aistock_factor_metrics.h20_rank_ic_std IS
    'Standard deviation of daily Spearman Rank IC for the h20 companion horizon';
COMMENT ON COLUMN aistock_factor_metrics.h20_icir IS
    'Unadjusted mean/std Pearson ICIR for h20';
COMMENT ON COLUMN aistock_factor_metrics.h20_rank_icir IS
    'Unadjusted mean/std Rank ICIR for h20';
COMMENT ON COLUMN aistock_factor_metrics.h20_icir_hac IS
    'Pearson ICIR using Newey-West long-run variance for overlapping h20 returns';
COMMENT ON COLUMN aistock_factor_metrics.h20_rank_icir_hac IS
    'Rank ICIR using Newey-West long-run variance for overlapping h20 returns';
COMMENT ON COLUMN aistock_factor_metrics.h20_ic_positive_ratio IS
    'Share of valid h20 Pearson IC observations above zero';
COMMENT ON COLUMN aistock_factor_metrics.h20_n_obs IS
    'Number of valid daily Pearson IC observations used for h20 statistics';
COMMENT ON COLUMN aistock_factor_metrics.h20_hac_lag IS
    'Maximum lag used by the h20 HAC long-run variance estimator; expected baseline is 19';

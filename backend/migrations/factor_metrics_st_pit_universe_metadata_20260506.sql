ALTER TABLE IF EXISTS aistock_factor_metrics
    ADD COLUMN IF NOT EXISTS coverage_numerator BIGINT,
    ADD COLUMN IF NOT EXISTS coverage_denominator BIGINT,
    ADD COLUMN IF NOT EXISTS coverage_semantics TEXT,
    ADD COLUMN IF NOT EXISTS universe_rule_version TEXT,
    ADD COLUMN IF NOT EXISTS universe_fingerprint_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS index_policy TEXT,
    ADD COLUMN IF NOT EXISTS eligible_sample_count BIGINT,
    ADD COLUMN IF NOT EXISTS suspended_excluded_count BIGINT,
    ADD COLUMN IF NOT EXISTS st_pit_excluded_count BIGINT;

COMMENT ON COLUMN aistock_factor_metrics.coverage_numerator IS 'Finite factor-value sample count inside the official ST PIT buy-eligible, market-valid, non-suspended, non-warmup denominator.';
COMMENT ON COLUMN aistock_factor_metrics.coverage_denominator IS 'Official coverage denominator: ST PIT buy-eligible samples with market data, excluding suspend_d and factor warmup.';
COMMENT ON COLUMN aistock_factor_metrics.coverage_semantics IS 'Versioned coverage definition used by qe_eval_v2, e.g. st_pit_buy_eligible_suspend_excluded_non_warmup_v1.';
COMMENT ON COLUMN aistock_factor_metrics.universe_rule_version IS 'Versioned ST PIT universe rule used for the metric calculation.';
COMMENT ON COLUMN aistock_factor_metrics.universe_fingerprint_sha256 IS 'SHA256 fingerprint of the derived ST PIT universe source state used for metric calculation.';
COMMENT ON COLUMN aistock_factor_metrics.index_policy IS 'Factor-value index/cache policy used by the official metric path.';
COMMENT ON COLUMN aistock_factor_metrics.eligible_sample_count IS 'Market-valid non-warmup sample count inside the ST PIT eligible universe before suspend exclusion.';
COMMENT ON COLUMN aistock_factor_metrics.suspended_excluded_count IS 'Sample count excluded by confirmed market.suspend_d suspension inside the ST PIT eligible universe.';
COMMENT ON COLUMN aistock_factor_metrics.st_pit_excluded_count IS 'Market-valid non-warmup sample count excluded because the instrument was outside ST PIT buy eligibility.';

ALTER TABLE IF EXISTS qe_factor_correlations
    ADD COLUMN IF NOT EXISTS universe TEXT,
    ADD COLUMN IF NOT EXISTS universe_rule_version TEXT,
    ADD COLUMN IF NOT EXISTS universe_fingerprint_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS index_policy TEXT;

COMMENT ON COLUMN qe_factor_correlations.universe IS 'Universe key for factor values used in this correlation row, e.g. shsz_st_pit_active_v1.';
COMMENT ON COLUMN qe_factor_correlations.universe_rule_version IS 'Versioned ST PIT universe rule for factor values used in this correlation row.';
COMMENT ON COLUMN qe_factor_correlations.universe_fingerprint_sha256 IS 'SHA256 fingerprint of the ST PIT universe source state used in this correlation row.';
COMMENT ON COLUMN qe_factor_correlations.index_policy IS 'Factor-value cache index policy used in this correlation row.';

ALTER TABLE IF EXISTS qe_correlation_metadata
    ADD COLUMN IF NOT EXISTS universe TEXT,
    ADD COLUMN IF NOT EXISTS universe_rule_version TEXT,
    ADD COLUMN IF NOT EXISTS universe_fingerprint_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS index_policy TEXT;

COMMENT ON COLUMN qe_correlation_metadata.universe IS 'Universe key for the factor-correlation matrix, e.g. shsz_st_pit_active_v1.';
COMMENT ON COLUMN qe_correlation_metadata.universe_rule_version IS 'Versioned ST PIT universe rule for the factor-correlation matrix.';
COMMENT ON COLUMN qe_correlation_metadata.universe_fingerprint_sha256 IS 'SHA256 fingerprint of the ST PIT universe source state for the factor-correlation matrix.';
COMMENT ON COLUMN qe_correlation_metadata.index_policy IS 'Factor-value cache index policy used by the factor-correlation matrix.';

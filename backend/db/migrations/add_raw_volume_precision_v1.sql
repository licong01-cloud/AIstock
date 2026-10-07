-- Additive and nullable: no rewriting BIGINT volume_hand or its unit contract.
-- Production requires separate target-specific authorization after DEV proof.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
ALTER TABLE market.kline_daily_raw
    ADD COLUMN IF NOT EXISTS volume_shares numeric,
    ADD COLUMN IF NOT EXISTS volume_shares_source varchar(32),
    ADD COLUMN IF NOT EXISTS volume_shares_sha256 varchar(64);
ALTER TABLE market.kline_minute_raw
    ADD COLUMN IF NOT EXISTS volume_shares numeric,
    ADD COLUMN IF NOT EXISTS volume_shares_source varchar(32),
    ADD COLUMN IF NOT EXISTS volume_shares_sha256 varchar(64);
COMMENT ON COLUMN market.kline_daily_raw.volume_shares IS
    'Provider-backed shares before whole-hand quantization; NULL means legacy precision only.';
COMMENT ON COLUMN market.kline_minute_raw.volume_shares IS
    'Provider-backed shares before whole-hand quantization; NULL means legacy precision only.';
COMMIT;

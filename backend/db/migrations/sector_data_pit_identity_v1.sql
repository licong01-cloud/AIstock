BEGIN;

LOCK TABLE market.sector_data IN SHARE ROW EXCLUSIVE MODE;

ALTER TABLE market.sector_data
    ADD COLUMN IF NOT EXISTS l1_code TEXT,
    ADD COLUMN IF NOT EXISTS l2_code TEXT,
    ADD COLUMN IF NOT EXISTS mapping_in_date DATE;

CREATE TEMP TABLE sector_data_pit_identity_backfill
ON COMMIT DROP
AS
WITH historical_candidates AS (
    SELECT
        sector.trade_date,
        sector.ts_code,
        member.l1_code,
        member.l2_code,
        member.in_date,
        ROW_NUMBER() OVER (
            PARTITION BY sector.trade_date, sector.ts_code, member.l2_code
            ORDER BY member.in_date DESC
        ) AS mapping_rank,
        daily.open,
        daily.high,
        daily.low,
        daily.close,
        daily.pct_change,
        daily.vol,
        daily.amount,
        daily.pe,
        daily.pb,
        daily.total_mv
    FROM market.sector_data AS sector
    JOIN market.sw_index_member AS member
      ON member.ts_code = sector.ts_code
     AND member.in_date <= sector.trade_date
    JOIN market.sw_daily AS daily
      ON daily.ts_code = member.l2_code
     AND daily.trade_date = sector.trade_date
    WHERE NULLIF(BTRIM(member.l1_code), '') IS NOT NULL
      AND NULLIF(BTRIM(member.l2_code), '') IS NOT NULL
      AND daily.open IS NOT NULL
      AND daily.high IS NOT NULL
      AND daily.low IS NOT NULL
      AND daily.close IS NOT NULL
      AND daily.pct_change IS NOT NULL
      AND daily.vol IS NOT NULL
      AND daily.amount IS NOT NULL
),
fact_matches AS (
    SELECT
        sector.trade_date,
        sector.ts_code,
        candidate.l1_code,
        candidate.l2_code,
        candidate.in_date
    FROM market.sector_data AS sector
    JOIN historical_candidates AS candidate
      ON candidate.trade_date = sector.trade_date
     AND candidate.ts_code = sector.ts_code
     AND candidate.mapping_rank = 1
    WHERE sector.sw2_open IS NOT DISTINCT FROM candidate.open
      AND sector.sw2_high IS NOT DISTINCT FROM candidate.high
      AND sector.sw2_low IS NOT DISTINCT FROM candidate.low
      AND sector.sw2_close IS NOT DISTINCT FROM candidate.close
      AND sector.sw2_pct_change IS NOT DISTINCT FROM candidate.pct_change
      AND sector.sw2_vol IS NOT DISTINCT FROM candidate.vol
      AND sector.sw2_amount IS NOT DISTINCT FROM candidate.amount
      AND sector.sw2_pe IS NOT DISTINCT FROM candidate.pe
      AND sector.sw2_pb IS NOT DISTINCT FROM candidate.pb
      AND sector.sw2_total_mv IS NOT DISTINCT FROM candidate.total_mv
),
unique_matches AS (
    SELECT trade_date, ts_code
    FROM fact_matches
    GROUP BY trade_date, ts_code
    HAVING COUNT(*) = 1
)
SELECT
    match.trade_date,
    match.ts_code,
    match.l1_code,
    match.l2_code,
    match.in_date AS mapping_in_date
FROM fact_matches AS match
JOIN unique_matches USING (trade_date, ts_code);

CREATE UNIQUE INDEX sector_data_pit_identity_backfill_pk
    ON sector_data_pit_identity_backfill (trade_date, ts_code);

DO $$
DECLARE
    source_count BIGINT;
    recovered_count BIGINT;
BEGIN
    SELECT COUNT(*) INTO source_count FROM market.sector_data;
    SELECT COUNT(*) INTO recovered_count FROM sector_data_pit_identity_backfill;

    IF source_count <> recovered_count THEN
        RAISE EXCEPTION
            'SECTOR_DATA_PIT_IDENTITY_BACKFILL_INCOMPLETE: source_rows=%, recovered_rows=%',
            source_count,
            recovered_count;
    END IF;
END;
$$;

UPDATE market.sector_data AS sector
SET l1_code = recovered.l1_code,
    l2_code = recovered.l2_code,
    mapping_in_date = recovered.mapping_in_date
FROM sector_data_pit_identity_backfill AS recovered
WHERE recovered.trade_date = sector.trade_date
  AND recovered.ts_code = sector.ts_code;

ALTER TABLE market.sector_data
    ALTER COLUMN l1_code SET NOT NULL,
    ALTER COLUMN l2_code SET NOT NULL,
    ALTER COLUMN mapping_in_date SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conrelid = 'market.sector_data'::regclass
          AND conname = 'ck_sector_data_mapping_in_date'
    ) THEN
        ALTER TABLE market.sector_data
            ADD CONSTRAINT ck_sector_data_mapping_in_date
            CHECK (mapping_in_date <= trade_date);
    END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS idx_sector_data_l1_date
    ON market.sector_data (l1_code, trade_date);
CREATE INDEX IF NOT EXISTS idx_sector_data_l2_date
    ON market.sector_data (l2_code, trade_date);

COMMENT ON COLUMN market.sector_data.l1_code IS
    'PIT mapping L1 code used when this row was built';
COMMENT ON COLUMN market.sector_data.l2_code IS
    'PIT mapping L2 code used when this row was built';
COMMENT ON COLUMN market.sector_data.mapping_in_date IS
    'in_date of the exact PIT mapping row used when this row was built';

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM market.sector_data
        GROUP BY trade_date, l2_code
        HAVING BOOL_OR(sw2_open IS NULL)
            OR BOOL_OR(sw2_high IS NULL)
            OR BOOL_OR(sw2_low IS NULL)
            OR BOOL_OR(sw2_close IS NULL)
            OR BOOL_OR(sw2_pct_change IS NULL)
            OR BOOL_OR(sw2_vol IS NULL)
            OR BOOL_OR(sw2_amount IS NULL)
            OR BOOL_OR(sw2_pe IS NULL)
            OR BOOL_OR(sw2_pb IS NULL)
            OR BOOL_OR(sw2_total_mv IS NULL)
            OR BOOL_OR(sw2_mf_buy_sm_amt IS NULL)
            OR BOOL_OR(sw2_mf_sell_sm_amt IS NULL)
            OR BOOL_OR(sw2_mf_buy_md_amt IS NULL)
            OR BOOL_OR(sw2_mf_sell_md_amt IS NULL)
            OR BOOL_OR(sw2_mf_buy_lg_amt IS NULL)
            OR BOOL_OR(sw2_mf_sell_lg_amt IS NULL)
            OR BOOL_OR(sw2_mf_buy_elg_amt IS NULL)
            OR BOOL_OR(sw2_mf_sell_elg_amt IS NULL)
            OR BOOL_OR(sw2_mf_net_amt IS NULL)
            OR BOOL_OR(sw2_mf_buy_elg_vol IS NULL)
            OR BOOL_OR(sw2_mf_sell_elg_vol IS NULL)
            OR BOOL_OR(sw2_mf_net_vol IS NULL)
            OR COUNT(DISTINCT sw2_open) > 1
            OR COUNT(DISTINCT sw2_high) > 1
            OR COUNT(DISTINCT sw2_low) > 1
            OR COUNT(DISTINCT sw2_close) > 1
            OR COUNT(DISTINCT sw2_pct_change) > 1
            OR COUNT(DISTINCT sw2_amount) > 1
            OR COUNT(DISTINCT sw2_vol) > 1
            OR COUNT(DISTINCT sw2_pe) > 1
            OR COUNT(DISTINCT sw2_pb) > 1
            OR COUNT(DISTINCT sw2_total_mv) > 1
            OR COUNT(DISTINCT sw2_mf_buy_sm_amt) > 1
            OR COUNT(DISTINCT sw2_mf_sell_sm_amt) > 1
            OR COUNT(DISTINCT sw2_mf_buy_md_amt) > 1
            OR COUNT(DISTINCT sw2_mf_sell_md_amt) > 1
            OR COUNT(DISTINCT sw2_mf_buy_lg_amt) > 1
            OR COUNT(DISTINCT sw2_mf_sell_lg_amt) > 1
            OR COUNT(DISTINCT sw2_mf_buy_elg_amt) > 1
            OR COUNT(DISTINCT sw2_mf_sell_elg_amt) > 1
            OR COUNT(DISTINCT sw2_mf_net_amt) > 1
            OR COUNT(DISTINCT sw2_mf_buy_elg_vol) > 1
            OR COUNT(DISTINCT sw2_mf_sell_elg_vol) > 1
            OR COUNT(DISTINCT sw2_mf_net_vol) > 1
    ) THEN
        RAISE EXCEPTION 'SECTOR_DATA_PERSISTED_L2_FACT_INCOMPLETE_OR_CONFLICTING';
    END IF;
END;
$$;

COMMIT;

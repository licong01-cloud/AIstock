BEGIN;

LOCK TABLE market.sector_data IN SHARE ROW EXCLUSIVE MODE;

DROP INDEX IF EXISTS market.idx_sector_data_l1_date;
DROP INDEX IF EXISTS market.idx_sector_data_l2_date;

ALTER TABLE market.sector_data
    DROP CONSTRAINT IF EXISTS ck_sector_data_mapping_in_date,
    DROP COLUMN IF EXISTS l1_code,
    DROP COLUMN IF EXISTS l2_code,
    DROP COLUMN IF EXISTS mapping_in_date;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'market'
          AND table_name = 'sector_data'
          AND column_name IN ('l1_code', 'l2_code', 'mapping_in_date')
    ) THEN
        RAISE EXCEPTION 'SECTOR_DATA_PERSISTED_PIT_IDENTITY_RETIREMENT_INCOMPLETE';
    END IF;
END;
$$;

COMMENT ON TABLE market.sector_data IS
    'Shenwan L2 stock/date facts; consumers resolve industry identity dynamically from sw_index_member and apply their authoritative PIT universe';

COMMIT;

BEGIN;

CREATE SCHEMA IF NOT EXISTS market;

CREATE TABLE IF NOT EXISTS market.core_index_membership_pit (
    pool_id TEXT NOT NULL,
    index_code TEXT NOT NULL,
    ts_code TEXT NOT NULL,
    effective_from DATE NOT NULL,
    effective_to_exclusive DATE,
    source_provider TEXT NOT NULL,
    source_reference TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (pool_id, ts_code, effective_from),
    CONSTRAINT ck_core_index_membership_dates CHECK (
        effective_to_exclusive IS NULL OR effective_to_exclusive > effective_from
    ),
    CONSTRAINT ck_core_index_membership_ts_code CHECK (
        ts_code ~ '^[0-9]{6}\.(SH|SZ)$'
    ),
    CONSTRAINT ck_core_index_membership_source_reference CHECK (
        btrim(source_reference) <> ''
    ),
    CONSTRAINT ck_core_index_membership_catalog CHECK (
        (pool_id = 'csi300' AND index_code = '000300.SH' AND source_provider = 'CSI') OR
        (pool_id = 'csi500' AND index_code = '000905.SH' AND source_provider = 'CSI') OR
        (pool_id = 'csi1000' AND index_code = '000852.SH' AND source_provider = 'CSI') OR
        (pool_id = 'star50' AND index_code = '000688.SH' AND source_provider = 'SSE') OR
        (pool_id = 'star100' AND index_code = '000698.SH' AND source_provider = 'SSE') OR
        (pool_id = 'sse50' AND index_code = '000016.SH' AND source_provider = 'SSE') OR
        (pool_id = 'chinext' AND index_code = '399006.SZ' AND source_provider = 'CNINDEX') OR
        (pool_id = 'csi_a500' AND index_code = '000510.SH' AND source_provider = 'CSI') OR
        (pool_id = 'csi2000' AND index_code = '932000.CSI' AND source_provider = 'CSI') OR
        (pool_id = 'csi800' AND index_code = '000906.SH' AND source_provider = 'CSI') OR
        (pool_id = 'szse_component' AND index_code = '399001.SZ' AND source_provider = 'CNINDEX') OR
        (pool_id = 'sse180' AND index_code = '000010.SH' AND source_provider = 'SSE') OR
        (pool_id = 'szse100' AND index_code = '399330.SZ' AND source_provider = 'CNINDEX') OR
        (pool_id = 'chinext50' AND index_code = '399673.SZ' AND source_provider = 'CNINDEX') OR
        (pool_id = 'csi_all_share' AND index_code = '000985.CSI' AND source_provider = 'CSI')
    )
);

CREATE INDEX IF NOT EXISTS idx_core_index_membership_pit_lookup
    ON market.core_index_membership_pit (pool_id, effective_from, effective_to_exclusive, ts_code);

CREATE OR REPLACE FUNCTION market.validate_core_index_membership_pit()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM market.core_index_membership_pit existing
         WHERE existing.pool_id = NEW.pool_id
           AND existing.ts_code = NEW.ts_code
           AND (existing.pool_id, existing.ts_code, existing.effective_from)
               <> (NEW.pool_id, NEW.ts_code, NEW.effective_from)
           AND daterange(
                   existing.effective_from,
                   COALESCE(existing.effective_to_exclusive, 'infinity'::date),
                   '[)'
               ) && daterange(
                   NEW.effective_from,
                   COALESCE(NEW.effective_to_exclusive, 'infinity'::date),
                   '[)'
               )
    ) THEN
        RAISE EXCEPTION 'overlapping core-index membership interval for %/%', NEW.pool_id, NEW.ts_code;
    END IF;
    NEW.updated_at := NOW();
    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS trg_validate_core_index_membership_pit
    ON market.core_index_membership_pit;
CREATE TRIGGER trg_validate_core_index_membership_pit
    BEFORE INSERT OR UPDATE ON market.core_index_membership_pit
    FOR EACH ROW EXECUTE FUNCTION market.validate_core_index_membership_pit();

COMMENT ON TABLE market.core_index_membership_pit IS
    'Official effective-date PIT membership for selectable core equity indices; consumers intersect rows with canonical equity PIT.';
COMMENT ON COLUMN market.core_index_membership_pit.pool_id IS
    'Stable consumer pool id such as csi300 or star50.';
COMMENT ON COLUMN market.core_index_membership_pit.index_code IS
    'Published index code corresponding to pool_id.';
COMMENT ON COLUMN market.core_index_membership_pit.ts_code IS
    'Canonical SH/SZ A-share code.';
COMMENT ON COLUMN market.core_index_membership_pit.effective_from IS
    'Inclusive official membership effective date.';
COMMENT ON COLUMN market.core_index_membership_pit.effective_to_exclusive IS
    'Exclusive official removal effective date; NULL for current membership.';
COMMENT ON COLUMN market.core_index_membership_pit.source_provider IS
    'Official index provider: CSI, SSE, or CNINDEX.';
COMMENT ON COLUMN market.core_index_membership_pit.source_reference IS
    'Compact official announcement or attachment locator; excluded from runtime sidecars.';
COMMENT ON COLUMN market.core_index_membership_pit.updated_at IS
    'Database write timestamp; never used as a PIT effective date.';

COMMIT;

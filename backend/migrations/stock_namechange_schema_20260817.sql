-- Authoritative Tushare historical security-name intervals used by issuer binding.

CREATE SCHEMA IF NOT EXISTS market;

CREATE TABLE IF NOT EXISTS market.stock_namechange (
    ts_code TEXT NOT NULL,
    name TEXT NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE,
    ann_date DATE,
    change_reason TEXT,
    source_api TEXT NOT NULL DEFAULT 'tushare.namechange',
    source_record_sha256 TEXT NOT NULL,
    source_payload JSONB NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ts_code, name, start_date),
    CONSTRAINT stock_namechange_interval_order_ck
        CHECK (end_date IS NULL OR end_date >= start_date),
    CONSTRAINT stock_namechange_source_api_ck
        CHECK (source_api = 'tushare.namechange'),
    CONSTRAINT stock_namechange_source_hash_ck
        CHECK (source_record_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE INDEX IF NOT EXISTS idx_stock_namechange_interval_lookup
    ON market.stock_namechange (ts_code, start_date, end_date, name);

COMMENT ON TABLE market.stock_namechange IS
    'Tushare namechange authoritative historical security-name intervals for PIT issuer binding';
COMMENT ON COLUMN market.stock_namechange.ts_code IS 'Tushare security code';
COMMENT ON COLUMN market.stock_namechange.name IS 'Security name effective in the interval';
COMMENT ON COLUMN market.stock_namechange.start_date IS 'Inclusive effective start date';
COMMENT ON COLUMN market.stock_namechange.end_date IS 'Inclusive effective end date; NULL means open-ended';
COMMENT ON COLUMN market.stock_namechange.ann_date IS 'Provider announcement date for the name change';
COMMENT ON COLUMN market.stock_namechange.change_reason IS 'Provider name-change reason';
COMMENT ON COLUMN market.stock_namechange.source_api IS 'Fixed provider API identity';
COMMENT ON COLUMN market.stock_namechange.source_record_sha256 IS 'SHA-256 of canonical provider fields';
COMMENT ON COLUMN market.stock_namechange.source_payload IS 'Canonical provider row retained for audit';
COMMENT ON COLUMN market.stock_namechange.ingested_at IS 'First local ingestion timestamp';
COMMENT ON COLUMN market.stock_namechange.updated_at IS 'Last provider reconciliation timestamp';

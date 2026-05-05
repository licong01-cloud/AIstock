-- Enhance market.dataset_date_refresh_audit for audit-first local data management.
-- Safe to run repeatedly.

ALTER TABLE market.dataset_date_refresh_audit
    ADD COLUMN IF NOT EXISTS data_max_at TIMESTAMPTZ;

ALTER TABLE market.dataset_date_refresh_audit
    ADD COLUMN IF NOT EXISTS written_rows BIGINT;

ALTER TABLE market.dataset_date_refresh_audit
    ADD COLUMN IF NOT EXISTS expected_rows BIGINT;

ALTER TABLE market.dataset_date_refresh_audit
    ADD COLUMN IF NOT EXISTS coverage_ratio NUMERIC(12, 8);

ALTER TABLE market.dataset_date_refresh_audit
    ADD COLUMN IF NOT EXISTS quality_status TEXT NOT NULL DEFAULT 'unknown';

ALTER TABLE market.dataset_date_refresh_audit
    ADD COLUMN IF NOT EXISTS failure_category TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'dataset_date_refresh_audit_written_rows_nonnegative'
          AND conrelid = 'market.dataset_date_refresh_audit'::regclass
    ) THEN
        ALTER TABLE market.dataset_date_refresh_audit
            ADD CONSTRAINT dataset_date_refresh_audit_written_rows_nonnegative
            CHECK (written_rows IS NULL OR written_rows >= 0);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'dataset_date_refresh_audit_expected_rows_nonnegative'
          AND conrelid = 'market.dataset_date_refresh_audit'::regclass
    ) THEN
        ALTER TABLE market.dataset_date_refresh_audit
            ADD CONSTRAINT dataset_date_refresh_audit_expected_rows_nonnegative
            CHECK (expected_rows IS NULL OR expected_rows >= 0);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'dataset_date_refresh_audit_coverage_ratio_range'
          AND conrelid = 'market.dataset_date_refresh_audit'::regclass
    ) THEN
        ALTER TABLE market.dataset_date_refresh_audit
            ADD CONSTRAINT dataset_date_refresh_audit_coverage_ratio_range
            CHECK (coverage_ratio IS NULL OR (coverage_ratio >= 0 AND coverage_ratio <= 1.5));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_dataset_refresh_audit_latest_success
    ON market.dataset_date_refresh_audit(dataset, status, trade_date DESC);

COMMENT ON TABLE market.dataset_date_refresh_audit IS 'AIstock dataset/date readiness ledger used by local data management, Selection Center, and Paper v2 fail-fast data gates.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.dataset IS 'Logical dataset key, for example suspend_d, stk_limit, kline_daily_raw, or sector_data.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.trade_date IS 'Trading date or effective dataset date that this readiness row describes.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.data_source IS 'Provider or process that produced the readiness row, such as tushare, tdx_api, sector_builder, or seed_existing_rows.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.job_id IS 'Optional market.ingestion_jobs.job_id that produced the latest readiness state for this dataset/date/source.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.status IS 'Readiness status; success means the dataset/date/source is usable, failed means it must not pass Paper v2/local data gates.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.row_count IS 'Final usable row count present for this dataset/date after the refresh attempt completed.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.refreshed_at IS 'Timestamp when the readiness row was written or updated by the refresh/audit process.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.error_message IS 'Provider, validation, or persistence error message for failed refresh attempts.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.data_max_at IS 'Precise maximum source data timestamp covered by this row for intraday or timestamped datasets; NULL for date-only datasets.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.written_rows IS 'Rows written or touched by the latest refresh attempt for this dataset/date/source; NULL when unknown.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.expected_rows IS 'Optional expected usable row count for coverage checks on this dataset/date.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.coverage_ratio IS 'Optional row_count divided by expected_rows; values below dataset policy thresholds indicate low coverage.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.quality_status IS 'Quality classification such as ok, unknown, empty_valid, empty_invalid, low_coverage, upstream_not_published, provider_unavailable, or error.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.failure_category IS 'Machine-readable failure reason for retry/self-healing decisions, for example audit_stale, empty_invalid, or provider_unavailable.';
COMMENT ON COLUMN market.dataset_date_refresh_audit.metadata IS 'Additional JSON context including API name, ingestion mode, table, source script, and validation notes.';

-- Tushare event-related source-only raw tables for unified event signals.
-- Safe to run repeatedly. Raw tables must never store derived event/risk/alpha fields.

CREATE SCHEMA IF NOT EXISTS market;

CREATE TABLE IF NOT EXISTS market.tushare_forecast_raw (
    raw_observation_id BIGSERIAL PRIMARY KEY,
    source_api TEXT NOT NULL,
    fetch_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_record_key TEXT NOT NULL,
    ts_code TEXT NOT NULL,
    ann_date DATE NOT NULL,
    report_period DATE NOT NULL,
    source_row_hash TEXT NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    first_seen_job_id UUID,
    last_seen_job_id UUID,
    job_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT tushare_forecast_raw_source_hash_uniq UNIQUE (source_record_key, source_row_hash),
    CONSTRAINT tushare_forecast_raw_payload_is_object CHECK (jsonb_typeof(raw_payload) = 'object'),
    CONSTRAINT tushare_forecast_raw_fetch_params_is_object CHECK (jsonb_typeof(fetch_params) = 'object')
);

CREATE TABLE IF NOT EXISTS market.tushare_express_raw (
    raw_observation_id BIGSERIAL PRIMARY KEY,
    source_api TEXT NOT NULL,
    fetch_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_record_key TEXT NOT NULL,
    ts_code TEXT NOT NULL,
    ann_date DATE NOT NULL,
    report_period DATE NOT NULL,
    source_row_hash TEXT NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    first_seen_job_id UUID,
    last_seen_job_id UUID,
    job_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT tushare_express_raw_source_hash_uniq UNIQUE (source_record_key, source_row_hash),
    CONSTRAINT tushare_express_raw_payload_is_object CHECK (jsonb_typeof(raw_payload) = 'object'),
    CONSTRAINT tushare_express_raw_fetch_params_is_object CHECK (jsonb_typeof(fetch_params) = 'object')
);

CREATE TABLE IF NOT EXISTS market.tushare_fina_indicator_raw (
    raw_observation_id BIGSERIAL PRIMARY KEY,
    source_api TEXT NOT NULL,
    fetch_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_record_key TEXT NOT NULL,
    ts_code TEXT NOT NULL,
    ann_date DATE NOT NULL,
    report_period DATE NOT NULL,
    source_row_hash TEXT NOT NULL,
    raw_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    first_seen_job_id UUID,
    last_seen_job_id UUID,
    job_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT tushare_fina_indicator_raw_source_hash_uniq UNIQUE (source_record_key, source_row_hash),
    CONSTRAINT tushare_fina_indicator_raw_payload_is_object CHECK (jsonb_typeof(raw_payload) = 'object'),
    CONSTRAINT tushare_fina_indicator_raw_fetch_params_is_object CHECK (jsonb_typeof(fetch_params) = 'object')
);

CREATE INDEX IF NOT EXISTS idx_tushare_forecast_raw_ts_ann
    ON market.tushare_forecast_raw(ts_code, ann_date);

CREATE INDEX IF NOT EXISTS idx_tushare_forecast_raw_report_period
    ON market.tushare_forecast_raw(report_period, ts_code);

CREATE INDEX IF NOT EXISTS idx_tushare_forecast_raw_observed_at
    ON market.tushare_forecast_raw(observed_at);

CREATE INDEX IF NOT EXISTS idx_tushare_forecast_raw_payload_gin
    ON market.tushare_forecast_raw USING GIN (raw_payload);

CREATE INDEX IF NOT EXISTS idx_tushare_express_raw_ts_ann
    ON market.tushare_express_raw(ts_code, ann_date);

CREATE INDEX IF NOT EXISTS idx_tushare_express_raw_report_period
    ON market.tushare_express_raw(report_period, ts_code);

CREATE INDEX IF NOT EXISTS idx_tushare_express_raw_observed_at
    ON market.tushare_express_raw(observed_at);

CREATE INDEX IF NOT EXISTS idx_tushare_express_raw_payload_gin
    ON market.tushare_express_raw USING GIN (raw_payload);

CREATE INDEX IF NOT EXISTS idx_tushare_fina_indicator_raw_ts_ann
    ON market.tushare_fina_indicator_raw(ts_code, ann_date);

CREATE INDEX IF NOT EXISTS idx_tushare_fina_indicator_raw_report_period
    ON market.tushare_fina_indicator_raw(report_period, ts_code);

CREATE INDEX IF NOT EXISTS idx_tushare_fina_indicator_raw_observed_at
    ON market.tushare_fina_indicator_raw(observed_at);

CREATE INDEX IF NOT EXISTS idx_tushare_fina_indicator_raw_payload_gin
    ON market.tushare_fina_indicator_raw USING GIN (raw_payload);

COMMENT ON TABLE market.tushare_forecast_raw IS 'Source-only raw observations from Tushare forecast or forecast_vip performance forecast APIs for unified event signal processing.';
COMMENT ON COLUMN market.tushare_forecast_raw.raw_observation_id IS 'AIstock local surrogate primary key for one immutable Tushare forecast raw observation version.';
COMMENT ON COLUMN market.tushare_forecast_raw.source_api IS 'Tushare API name used for this row, expected forecast or forecast_vip; records permission/path differences.';
COMMENT ON COLUMN market.tushare_forecast_raw.fetch_params IS 'JSONB parameters passed to Tushare for this fetch, such as period, ts_code, ann_date, start_date, or end_date.';
COMMENT ON COLUMN market.tushare_forecast_raw.source_record_key IS 'Stable source business key generated from Tushare forecast identifiers before hashing, normally ts_code, ann_date, report_period, and forecast type.';
COMMENT ON COLUMN market.tushare_forecast_raw.ts_code IS 'Tushare A-share security code reported by the forecast API.';
COMMENT ON COLUMN market.tushare_forecast_raw.ann_date IS 'Tushare forecast announcement date; date only, no exact publish time from this API.';
COMMENT ON COLUMN market.tushare_forecast_raw.report_period IS 'Financial reporting period end date from Tushare end_date.';
COMMENT ON COLUMN market.tushare_forecast_raw.source_row_hash IS 'SHA256 hash of normalized source row payload used to detect upstream forecast revisions without overwriting old raw payloads.';
COMMENT ON COLUMN market.tushare_forecast_raw.raw_payload IS 'Complete raw Tushare forecast row as JSONB, including type, p_change_min/max, net_profit_min/max, summary, and change_reason when supplied.';
COMMENT ON COLUMN market.tushare_forecast_raw.first_seen_at IS 'Local timestamp when AIstock first observed this exact source_record_key and source_row_hash version.';
COMMENT ON COLUMN market.tushare_forecast_raw.last_seen_at IS 'Local timestamp when AIstock most recently observed this exact source_record_key and source_row_hash version.';
COMMENT ON COLUMN market.tushare_forecast_raw.observed_at IS 'Local timestamp for the current fetch observation that inserted or refreshed this raw row.';
COMMENT ON COLUMN market.tushare_forecast_raw.first_seen_job_id IS 'Optional ingestion job UUID that first observed this exact raw version.';
COMMENT ON COLUMN market.tushare_forecast_raw.last_seen_job_id IS 'Optional ingestion job UUID that most recently observed this exact raw version.';
COMMENT ON COLUMN market.tushare_forecast_raw.job_id IS 'Optional ingestion job UUID for the fetch attempt that wrote or last refreshed this row.';
COMMENT ON COLUMN market.tushare_forecast_raw.created_at IS 'Database timestamp when this raw observation row was inserted.';
COMMENT ON COLUMN market.tushare_forecast_raw.updated_at IS 'Database timestamp when this raw observation row was last updated.';

COMMENT ON TABLE market.tushare_express_raw IS 'Source-only raw observations from Tushare express or express_vip performance express APIs for unified event signal processing.';
COMMENT ON COLUMN market.tushare_express_raw.raw_observation_id IS 'AIstock local surrogate primary key for one immutable Tushare express raw observation version.';
COMMENT ON COLUMN market.tushare_express_raw.source_api IS 'Tushare API name used for this row, expected express or express_vip; records permission/path differences.';
COMMENT ON COLUMN market.tushare_express_raw.fetch_params IS 'JSONB parameters passed to Tushare for this fetch, such as period, ts_code, ann_date, start_date, or end_date.';
COMMENT ON COLUMN market.tushare_express_raw.source_record_key IS 'Stable source business key generated from Tushare express identifiers before hashing, normally ts_code, ann_date, and report_period.';
COMMENT ON COLUMN market.tushare_express_raw.ts_code IS 'Tushare A-share security code reported by the express API.';
COMMENT ON COLUMN market.tushare_express_raw.ann_date IS 'Tushare performance express announcement date; date only, no exact publish time from this API.';
COMMENT ON COLUMN market.tushare_express_raw.report_period IS 'Financial reporting period end date from Tushare end_date.';
COMMENT ON COLUMN market.tushare_express_raw.source_row_hash IS 'SHA256 hash of normalized source row payload used to detect upstream express revisions without overwriting old raw payloads.';
COMMENT ON COLUMN market.tushare_express_raw.raw_payload IS 'Complete raw Tushare express row as JSONB, including revenue, profit, EPS, ROE, YoY metrics, audit flag, summary, and remarks when supplied.';
COMMENT ON COLUMN market.tushare_express_raw.first_seen_at IS 'Local timestamp when AIstock first observed this exact source_record_key and source_row_hash version.';
COMMENT ON COLUMN market.tushare_express_raw.last_seen_at IS 'Local timestamp when AIstock most recently observed this exact source_record_key and source_row_hash version.';
COMMENT ON COLUMN market.tushare_express_raw.observed_at IS 'Local timestamp for the current fetch observation that inserted or refreshed this raw row.';
COMMENT ON COLUMN market.tushare_express_raw.first_seen_job_id IS 'Optional ingestion job UUID that first observed this exact raw version.';
COMMENT ON COLUMN market.tushare_express_raw.last_seen_job_id IS 'Optional ingestion job UUID that most recently observed this exact raw version.';
COMMENT ON COLUMN market.tushare_express_raw.job_id IS 'Optional ingestion job UUID for the fetch attempt that wrote or last refreshed this row.';
COMMENT ON COLUMN market.tushare_express_raw.created_at IS 'Database timestamp when this raw observation row was inserted.';
COMMENT ON COLUMN market.tushare_express_raw.updated_at IS 'Database timestamp when this raw observation row was last updated.';

COMMENT ON TABLE market.tushare_fina_indicator_raw IS 'Source-only raw observations from Tushare fina_indicator or fina_indicator_vip financial indicator APIs for unified event signal processing.';
COMMENT ON COLUMN market.tushare_fina_indicator_raw.raw_observation_id IS 'AIstock local surrogate primary key for one immutable Tushare fina_indicator raw observation version.';
COMMENT ON COLUMN market.tushare_fina_indicator_raw.source_api IS 'Tushare API name used for this row, expected fina_indicator or fina_indicator_vip; records permission/path differences.';
COMMENT ON COLUMN market.tushare_fina_indicator_raw.fetch_params IS 'JSONB parameters passed to Tushare for this fetch, such as period, ts_code, ann_date, start_date, or end_date.';
COMMENT ON COLUMN market.tushare_fina_indicator_raw.source_record_key IS 'Stable source business key generated from Tushare fina_indicator identifiers before hashing, normally ts_code, ann_date, and report_period.';
COMMENT ON COLUMN market.tushare_fina_indicator_raw.ts_code IS 'Tushare A-share security code reported by the fina_indicator API.';
COMMENT ON COLUMN market.tushare_fina_indicator_raw.ann_date IS 'Tushare financial indicator announcement date; date only, no exact publish time from this API.';
COMMENT ON COLUMN market.tushare_fina_indicator_raw.report_period IS 'Financial reporting period end date from Tushare end_date.';
COMMENT ON COLUMN market.tushare_fina_indicator_raw.source_row_hash IS 'SHA256 hash of normalized source row payload used to detect upstream financial indicator revisions without overwriting old raw payloads.';
COMMENT ON COLUMN market.tushare_fina_indicator_raw.raw_payload IS 'Complete raw Tushare fina_indicator row as JSONB; typed financial metrics are derived later into event facts or feature snapshots, not stored as raw-table columns.';
COMMENT ON COLUMN market.tushare_fina_indicator_raw.first_seen_at IS 'Local timestamp when AIstock first observed this exact source_record_key and source_row_hash version.';
COMMENT ON COLUMN market.tushare_fina_indicator_raw.last_seen_at IS 'Local timestamp when AIstock most recently observed this exact source_record_key and source_row_hash version.';
COMMENT ON COLUMN market.tushare_fina_indicator_raw.observed_at IS 'Local timestamp for the current fetch observation that inserted or refreshed this raw row.';
COMMENT ON COLUMN market.tushare_fina_indicator_raw.first_seen_job_id IS 'Optional ingestion job UUID that first observed this exact raw version.';
COMMENT ON COLUMN market.tushare_fina_indicator_raw.last_seen_job_id IS 'Optional ingestion job UUID that most recently observed this exact raw version.';
COMMENT ON COLUMN market.tushare_fina_indicator_raw.job_id IS 'Optional ingestion job UUID for the fetch attempt that wrote or last refreshed this row.';
COMMENT ON COLUMN market.tushare_fina_indicator_raw.created_at IS 'Database timestamp when this raw observation row was inserted.';
COMMENT ON COLUMN market.tushare_fina_indicator_raw.updated_at IS 'Database timestamp when this raw observation row was last updated.';

DO $$
BEGIN
    IF to_regclass('market.data_stats_config') IS NOT NULL THEN
        INSERT INTO market.data_stats_config
            (data_kind, table_name, date_column, updated_column, enabled, extra_info)
        VALUES
            (
                'tushare_forecast_raw',
                'market.tushare_forecast_raw',
                'ann_date',
                'last_seen_at',
                TRUE,
                jsonb_build_object(
                    'desc', 'Tushare forecast/forecast_vip source-only performance forecast raw observations',
                    'source_api', jsonb_build_array('forecast', 'forecast_vip'),
                    'date_sequence', 'calendar',
                    'date_semantics', 'ann_date is source announcement date without exact publish time',
                    'cursor_source', 'refresh_audit',
                    'raw_layer', TRUE
                )
            ),
            (
                'tushare_express_raw',
                'market.tushare_express_raw',
                'ann_date',
                'last_seen_at',
                TRUE,
                jsonb_build_object(
                    'desc', 'Tushare express/express_vip source-only performance express raw observations',
                    'source_api', jsonb_build_array('express', 'express_vip'),
                    'date_sequence', 'calendar',
                    'date_semantics', 'ann_date is source announcement date without exact publish time',
                    'cursor_source', 'refresh_audit',
                    'raw_layer', TRUE
                )
            ),
            (
                'tushare_fina_indicator_raw',
                'market.tushare_fina_indicator_raw',
                'ann_date',
                'last_seen_at',
                TRUE,
                jsonb_build_object(
                    'desc', 'Tushare fina_indicator/fina_indicator_vip source-only financial indicator raw observations',
                    'source_api', jsonb_build_array('fina_indicator', 'fina_indicator_vip'),
                    'date_sequence', 'calendar',
                    'date_semantics', 'ann_date is source announcement date without exact publish time',
                    'cursor_source', 'refresh_audit',
                    'raw_layer', TRUE
                )
            )
        ON CONFLICT (data_kind) DO UPDATE
            SET table_name = EXCLUDED.table_name,
                date_column = EXCLUDED.date_column,
                updated_column = EXCLUDED.updated_column,
                enabled = EXCLUDED.enabled,
                extra_info = EXCLUDED.extra_info;
    END IF;
END $$;

-- Add local first/last observation timestamps for announcement metadata.
-- Safe to run repeatedly.

ALTER TABLE market.anns
    ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMPTZ;

ALTER TABLE market.anns
    ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ;

ALTER TABLE market.anns
    ADD COLUMN IF NOT EXISTS first_seen_source TEXT;

ALTER TABLE market.anns
    ADD COLUMN IF NOT EXISTS last_seen_source TEXT;

ALTER TABLE market.anns
    ADD COLUMN IF NOT EXISTS first_seen_job_id UUID;

ALTER TABLE market.anns
    ADD COLUMN IF NOT EXISTS last_seen_job_id UUID;

ALTER TABLE market.anns
    ADD COLUMN IF NOT EXISTS observed_time_quality TEXT NOT NULL DEFAULT 'BACKFILL_UNKNOWN';

ALTER TABLE market.ann_event_classification
    ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ;

ALTER TABLE market.ann_event_classification
    ADD COLUMN IF NOT EXISTS time_mode TEXT NOT NULL DEFAULT 'backtest';

ALTER TABLE market.ann_risk_signal
    ADD COLUMN IF NOT EXISTS available_at TIMESTAMPTZ;

ALTER TABLE market.ann_risk_signal
    ADD COLUMN IF NOT EXISTS time_mode TEXT NOT NULL DEFAULT 'backtest';

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'ann_event_classification_ann_rule_uniq'
           AND conrelid = 'market.ann_event_classification'::regclass
    ) THEN
        ALTER TABLE market.ann_event_classification
            DROP CONSTRAINT ann_event_classification_ann_rule_uniq;
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'ann_event_classification_ann_rule_mode_uniq'
           AND conrelid = 'market.ann_event_classification'::regclass
    ) THEN
        ALTER TABLE market.ann_event_classification
            ADD CONSTRAINT ann_event_classification_ann_rule_mode_uniq
            UNIQUE (ann_id, rule_version, time_mode);
    END IF;

    IF EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'ann_risk_signal_ann_rule_uniq'
           AND conrelid = 'market.ann_risk_signal'::regclass
    ) THEN
        ALTER TABLE market.ann_risk_signal
            DROP CONSTRAINT ann_risk_signal_ann_rule_uniq;
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'ann_risk_signal_ann_rule_mode_uniq'
           AND conrelid = 'market.ann_risk_signal'::regclass
    ) THEN
        ALTER TABLE market.ann_risk_signal
            ADD CONSTRAINT ann_risk_signal_ann_rule_mode_uniq
            UNIQUE (ann_id, rule_version, time_mode);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_anns_first_seen_at
    ON market.anns(first_seen_at);

CREATE INDEX IF NOT EXISTS idx_anns_last_seen_at
    ON market.anns(last_seen_at);

CREATE INDEX IF NOT EXISTS idx_anns_observed_time_quality
    ON market.anns(observed_time_quality);

CREATE INDEX IF NOT EXISTS idx_ann_event_classification_available
    ON market.ann_event_classification(available_at);

CREATE INDEX IF NOT EXISTS idx_ann_event_classification_time_mode
    ON market.ann_event_classification(time_mode, effective_trade_date, risk_level);

CREATE INDEX IF NOT EXISTS idx_ann_risk_signal_available
    ON market.ann_risk_signal(available_at);

CREATE INDEX IF NOT EXISTS idx_ann_risk_signal_time_mode
    ON market.ann_risk_signal(time_mode, effective_trade_date, risk_level, action);

COMMENT ON TABLE market.ann_event_classification IS 'One deterministic classification result per market.anns row, rule_version, and time_mode; keeps leakage-safe backtest rows separate from paper/live observed-time rows.';
COMMENT ON TABLE market.ann_risk_signal IS 'Announcement-derived risk overlay signals independent from alpha factors, keyed by rule_version and time_mode so backtest and live/paper alerts do not overwrite each other.';
COMMENT ON COLUMN market.anns.first_seen_at IS 'Local timestamp when AIstock first observed this announcement from an interface; NULL for rows inserted before this field existed to avoid historical backtest leakage.';
COMMENT ON COLUMN market.anns.last_seen_at IS 'Local timestamp when AIstock most recently observed or upserted this announcement from an interface.';
COMMENT ON COLUMN market.anns.first_seen_source IS 'Source name for first_seen_at, such as eastmoney, cninfo, or tushare; NULL for pre-observation-migration historical rows.';
COMMENT ON COLUMN market.anns.last_seen_source IS 'Source name for last_seen_at, such as eastmoney, cninfo, or tushare.';
COMMENT ON COLUMN market.anns.first_seen_job_id IS 'Optional market.ingestion_jobs.job_id that first observed this announcement locally; NULL when unknown.';
COMMENT ON COLUMN market.anns.last_seen_job_id IS 'Optional market.ingestion_jobs.job_id that most recently observed this announcement locally; NULL when unknown.';
COMMENT ON COLUMN market.anns.observed_time_quality IS 'Quality of local observation fields: BACKFILL_UNKNOWN for historical/pre-migration rows, LOCAL_FIRST_SEEN for announcements first inserted by live/incremental observation after this migration.';
COMMENT ON COLUMN market.ann_event_classification.available_at IS 'Timestamp used by the classification engine as the event availability time; rec_time for source-exact rows, first_seen_at for live/paper local-observed rows, NULL for date-only backtest rows.';
COMMENT ON COLUMN market.ann_event_classification.time_mode IS 'Availability semantics mode for this classification: backtest ignores local first_seen_at, while paper/live/observed can use local first_seen_at without overwriting backtest rows.';
COMMENT ON COLUMN market.ann_risk_signal.available_at IS 'Timestamp used by risk-signal consumers as the event availability time; copied from classification for live/paper alert timing.';
COMMENT ON COLUMN market.ann_risk_signal.time_mode IS 'Availability semantics mode for this risk signal: backtest, paper, live, or observed; included in uniqueness to keep research and live/paper signals separate.';

-- Announcement event classification and risk signal schema.
-- Safe to run repeatedly.  All new fields are documented with comments.

CREATE TABLE IF NOT EXISTS market.ann_event_taxonomy (
    event_type TEXT PRIMARY KEY,
    risk_level TEXT NOT NULL,
    default_action TEXT NOT NULL,
    needs_llm TEXT NOT NULL,
    description TEXT NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS market.ann_rule_set (
    rule_version TEXT PRIMARY KEY,
    engine_name TEXT NOT NULL,
    rule_source TEXT NOT NULL,
    rule_count INTEGER NOT NULL,
    config_hash TEXT NOT NULL,
    config JSONB NOT NULL DEFAULT '[]'::jsonb,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS market.ann_event_classification (
    classification_id BIGSERIAL PRIMARY KEY,
    ann_id BIGINT NOT NULL REFERENCES market.anns(id) ON DELETE CASCADE,
    ts_code TEXT NOT NULL,
    ann_date DATE NOT NULL,
    title_hash TEXT NOT NULL,
    rule_version TEXT NOT NULL REFERENCES market.ann_rule_set(rule_version),
    event_type TEXT NOT NULL REFERENCES market.ann_event_taxonomy(event_type),
    risk_level TEXT NOT NULL,
    action TEXT NOT NULL,
    needs_llm TEXT NOT NULL,
    matched_rule TEXT NOT NULL,
    matched_text TEXT,
    source_time_quality TEXT NOT NULL,
    effective_trade_date DATE NOT NULL,
    effective_rule TEXT NOT NULL,
    confidence NUMERIC(6, 4) NOT NULL,
    severity_score NUMERIC(6, 4) NOT NULL,
    classification_detail JSONB NOT NULL DEFAULT '{}'::jsonb,
    classified_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS market.ann_risk_signal (
    signal_id BIGSERIAL PRIMARY KEY,
    ann_id BIGINT NOT NULL REFERENCES market.anns(id) ON DELETE CASCADE,
    ts_code TEXT NOT NULL,
    ann_date DATE NOT NULL,
    rule_version TEXT NOT NULL REFERENCES market.ann_rule_set(rule_version),
    event_type TEXT NOT NULL REFERENCES market.ann_event_taxonomy(event_type),
    risk_level TEXT NOT NULL,
    action TEXT NOT NULL,
    source_time_quality TEXT NOT NULL,
    effective_trade_date DATE NOT NULL,
    signal_status TEXT NOT NULL DEFAULT 'ACTIVE',
    severity_score NUMERIC(6, 4) NOT NULL,
    confidence NUMERIC(6, 4) NOT NULL,
    reason TEXT NOT NULL,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'ann_event_classification_ann_rule_uniq'
           AND conrelid = 'market.ann_event_classification'::regclass
    ) THEN
        ALTER TABLE market.ann_event_classification
            ADD CONSTRAINT ann_event_classification_ann_rule_uniq
            UNIQUE (ann_id, rule_version);
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'ann_risk_signal_ann_rule_uniq'
           AND conrelid = 'market.ann_risk_signal'::regclass
    ) THEN
        ALTER TABLE market.ann_risk_signal
            ADD CONSTRAINT ann_risk_signal_ann_rule_uniq
            UNIQUE (ann_id, rule_version);
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'ann_event_classification_confidence_range'
           AND conrelid = 'market.ann_event_classification'::regclass
    ) THEN
        ALTER TABLE market.ann_event_classification
            ADD CONSTRAINT ann_event_classification_confidence_range
            CHECK (confidence >= 0 AND confidence <= 1);
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'ann_event_classification_severity_range'
           AND conrelid = 'market.ann_event_classification'::regclass
    ) THEN
        ALTER TABLE market.ann_event_classification
            ADD CONSTRAINT ann_event_classification_severity_range
            CHECK (severity_score >= 0 AND severity_score <= 1);
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'ann_risk_signal_severity_range'
           AND conrelid = 'market.ann_risk_signal'::regclass
    ) THEN
        ALTER TABLE market.ann_risk_signal
            ADD CONSTRAINT ann_risk_signal_severity_range
            CHECK (severity_score >= 0 AND severity_score <= 1);
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'ann_risk_signal_confidence_range'
           AND conrelid = 'market.ann_risk_signal'::regclass
    ) THEN
        ALTER TABLE market.ann_risk_signal
            ADD CONSTRAINT ann_risk_signal_confidence_range
            CHECK (confidence >= 0 AND confidence <= 1);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_ann_event_classification_date_level
    ON market.ann_event_classification(ann_date, risk_level);

CREATE INDEX IF NOT EXISTS idx_ann_event_classification_effective
    ON market.ann_event_classification(effective_trade_date, risk_level, action);

CREATE INDEX IF NOT EXISTS idx_ann_event_classification_symbol_effective
    ON market.ann_event_classification(ts_code, effective_trade_date);

CREATE INDEX IF NOT EXISTS idx_ann_event_classification_type
    ON market.ann_event_classification(event_type, ann_date);

CREATE INDEX IF NOT EXISTS idx_ann_risk_signal_effective
    ON market.ann_risk_signal(effective_trade_date, risk_level, action);

CREATE INDEX IF NOT EXISTS idx_ann_risk_signal_symbol_effective
    ON market.ann_risk_signal(ts_code, effective_trade_date);

CREATE INDEX IF NOT EXISTS idx_ann_risk_signal_status
    ON market.ann_risk_signal(signal_status, effective_trade_date);

COMMENT ON TABLE market.ann_event_taxonomy IS 'Announcement event taxonomy used by AIstock title/PDF/LLM classifiers; one row per stable event_type.';
COMMENT ON COLUMN market.ann_event_taxonomy.event_type IS 'Stable machine-readable announcement event type, for example delisting_or_risk_warning.';
COMMENT ON COLUMN market.ann_event_taxonomy.risk_level IS 'Default risk level for this event type, such as P0_BLOCK, P1_HIGH, P2_REVIEW, P3_POSITIVE_CANDIDATE, or P4_NEUTRAL.';
COMMENT ON COLUMN market.ann_event_taxonomy.default_action IS 'Default risk overlay action for this event type, such as block_buy, warn_high, warn_review, record_only, or discard_or_archive.';
COMMENT ON COLUMN market.ann_event_taxonomy.needs_llm IS 'Whether this event type needs PDF/LLM review in later stages: NO, OPTIONAL, YES, or SAMPLE_ONLY.';
COMMENT ON COLUMN market.ann_event_taxonomy.description IS 'Human-readable business meaning and intended first-stage treatment for this event type.';
COMMENT ON COLUMN market.ann_event_taxonomy.is_active IS 'Whether this taxonomy event type is active for new classifications.';
COMMENT ON COLUMN market.ann_event_taxonomy.created_at IS 'Database timestamp when this taxonomy row was first inserted.';
COMMENT ON COLUMN market.ann_event_taxonomy.updated_at IS 'Database timestamp when this taxonomy row was last updated.';

COMMENT ON TABLE market.ann_rule_set IS 'Versioned announcement classification rule-set metadata; same rule_version must be used for backtest and live parity.';
COMMENT ON COLUMN market.ann_rule_set.rule_version IS 'Stable classifier version identifier used in classification and signal rows.';
COMMENT ON COLUMN market.ann_rule_set.engine_name IS 'Classifier engine implementation name that produced this rule set.';
COMMENT ON COLUMN market.ann_rule_set.rule_source IS 'Source of the rule set, for example title_rules_v0_from_local_market_anns.';
COMMENT ON COLUMN market.ann_rule_set.rule_count IS 'Number of deterministic title rules contained in config for this version.';
COMMENT ON COLUMN market.ann_rule_set.config_hash IS 'SHA256 hash of normalized rule config for reproducibility and drift checks.';
COMMENT ON COLUMN market.ann_rule_set.config IS 'JSONB serialized deterministic rule definitions including event_type, regex pattern, action, and description.';
COMMENT ON COLUMN market.ann_rule_set.is_active IS 'Whether this rule version is active for new live/incremental classification.';
COMMENT ON COLUMN market.ann_rule_set.created_at IS 'Database timestamp when this rule-set row was first inserted.';
COMMENT ON COLUMN market.ann_rule_set.updated_at IS 'Database timestamp when this rule-set row was last updated.';

COMMENT ON TABLE market.ann_event_classification IS 'One deterministic classification result per market.anns row and rule_version; source for both backtest and live risk overlay.';
COMMENT ON COLUMN market.ann_event_classification.classification_id IS 'Local surrogate primary key for one classified announcement row.';
COMMENT ON COLUMN market.ann_event_classification.ann_id IS 'Foreign key to market.anns.id, preserving the canonical announcement metadata row.';
COMMENT ON COLUMN market.ann_event_classification.ts_code IS 'A-share security code copied from market.anns.ts_code for query speed and point-in-time joins.';
COMMENT ON COLUMN market.ann_event_classification.ann_date IS 'Announcement natural date copied from market.anns.ann_date; announcements can occur on non-trading days.';
COMMENT ON COLUMN market.ann_event_classification.title_hash IS 'SHA256 hash of market.anns.title at classification time, used to detect title text drift.';
COMMENT ON COLUMN market.ann_event_classification.rule_version IS 'Classifier rule version that produced this classification; required for backtest/live reproducibility.';
COMMENT ON COLUMN market.ann_event_classification.event_type IS 'Classified announcement event type from market.ann_event_taxonomy.';
COMMENT ON COLUMN market.ann_event_classification.risk_level IS 'Result risk level after applying title rules; first-stage production uses risk warning before alpha.';
COMMENT ON COLUMN market.ann_event_classification.action IS 'Result risk overlay action, for example block_buy, warn_high, warn_review, record_only, or discard_or_archive.';
COMMENT ON COLUMN market.ann_event_classification.needs_llm IS 'Whether this row should enter later PDF/LLM review: NO, OPTIONAL, YES, or SAMPLE_ONLY.';
COMMENT ON COLUMN market.ann_event_classification.matched_rule IS 'Matched deterministic rule identifier; DEFAULT means no title rule matched.';
COMMENT ON COLUMN market.ann_event_classification.matched_text IS 'Matched title substring or compact evidence phrase from the title rule; NULL/empty for DEFAULT.';
COMMENT ON COLUMN market.ann_event_classification.source_time_quality IS 'Announcement time quality: EXACT, MIDNIGHT_DEFAULT, or MISSING; controls leakage-safe effective dates.';
COMMENT ON COLUMN market.ann_event_classification.effective_trade_date IS 'First trading date when this classified event can be consumed by backtest/live decision logic.';
COMMENT ON COLUMN market.ann_event_classification.effective_rule IS 'Machine-readable rule used to derive effective_trade_date from ann_date/rec_time.';
COMMENT ON COLUMN market.ann_event_classification.confidence IS 'Classifier confidence in [0,1] for the deterministic title-level classification.';
COMMENT ON COLUMN market.ann_event_classification.severity_score IS 'Normalized risk severity in [0,1]; P0/P1 are risk overlay candidates, positive alpha remains disabled.';
COMMENT ON COLUMN market.ann_event_classification.classification_detail IS 'JSONB trace including title, description, matched rule, and engine metadata needed for audit.';
COMMENT ON COLUMN market.ann_event_classification.classified_at IS 'Database timestamp when this classification was generated.';
COMMENT ON COLUMN market.ann_event_classification.created_at IS 'Database timestamp when this classification row was first inserted.';
COMMENT ON COLUMN market.ann_event_classification.updated_at IS 'Database timestamp when this classification row was last updated.';

COMMENT ON TABLE market.ann_risk_signal IS 'Announcement-derived risk overlay signals independent from alpha factors; used for warnings and buy blocks.';
COMMENT ON COLUMN market.ann_risk_signal.signal_id IS 'Local surrogate primary key for one announcement risk signal.';
COMMENT ON COLUMN market.ann_risk_signal.ann_id IS 'Foreign key to market.anns.id that produced this signal.';
COMMENT ON COLUMN market.ann_risk_signal.ts_code IS 'A-share security code affected by this announcement risk signal.';
COMMENT ON COLUMN market.ann_risk_signal.ann_date IS 'Announcement natural date copied from market.anns.ann_date.';
COMMENT ON COLUMN market.ann_risk_signal.rule_version IS 'Classifier rule version that produced this signal; must match ann_event_classification.rule_version.';
COMMENT ON COLUMN market.ann_risk_signal.event_type IS 'Event type that produced this risk signal.';
COMMENT ON COLUMN market.ann_risk_signal.risk_level IS 'Risk level for signal action; P0/P1/P2 are first-stage risk controls, P3 is research-only record.';
COMMENT ON COLUMN market.ann_risk_signal.action IS 'Risk overlay action for consumers, such as block_buy, warn_high, warn_review, or record_only.';
COMMENT ON COLUMN market.ann_risk_signal.source_time_quality IS 'Announcement time quality: EXACT, MIDNIGHT_DEFAULT, or MISSING; copied from classification.';
COMMENT ON COLUMN market.ann_risk_signal.effective_trade_date IS 'First trading date when this risk signal is visible to trading/backtest consumers.';
COMMENT ON COLUMN market.ann_risk_signal.signal_status IS 'Signal lifecycle status, initially ACTIVE; future stages may set RESOLVED, EXPIRED, or SUPPRESSED.';
COMMENT ON COLUMN market.ann_risk_signal.severity_score IS 'Normalized risk severity in [0,1] used for overlay ranking and warning priority.';
COMMENT ON COLUMN market.ann_risk_signal.confidence IS 'Classifier confidence in [0,1] copied from deterministic title classification.';
COMMENT ON COLUMN market.ann_risk_signal.reason IS 'Short human-readable reason for alert displays and audit reports.';
COMMENT ON COLUMN market.ann_risk_signal.evidence IS 'JSONB evidence including matched title text, source announcement id, title, and effective-date trace.';
COMMENT ON COLUMN market.ann_risk_signal.generated_at IS 'Database timestamp when this signal was generated from classification.';
COMMENT ON COLUMN market.ann_risk_signal.created_at IS 'Database timestamp when this signal row was first inserted.';
COMMENT ON COLUMN market.ann_risk_signal.updated_at IS 'Database timestamp when this signal row was last updated.';

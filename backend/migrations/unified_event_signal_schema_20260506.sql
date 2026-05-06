-- Unified non-daily event fact/relation/signal schema.
-- Safe to run repeatedly for first-time creation.  Every table and column is commented.

CREATE SCHEMA IF NOT EXISTS market;

CREATE TABLE IF NOT EXISTS market.event_signal_rule_set (
    rule_version TEXT PRIMARY KEY,
    engine_name TEXT NOT NULL,
    rule_source TEXT NOT NULL,
    rule_scope TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_rule_versions JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS market.event_signal_run (
    run_id TEXT PRIMARY KEY,
    rule_version TEXT NOT NULL REFERENCES market.event_signal_rule_set(rule_version),
    run_mode TEXT NOT NULL,
    time_mode TEXT NOT NULL,
    source_scope JSONB NOT NULL DEFAULT '{}'::jsonb,
    date_from DATE,
    date_to DATE,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'RUNNING',
    source_input_rows BIGINT NOT NULL DEFAULT 0,
    fact_rows BIGINT NOT NULL DEFAULT 0,
    relation_rows BIGINT NOT NULL DEFAULT 0,
    signal_rows BIGINT NOT NULL DEFAULT 0,
    error_message TEXT,
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT event_signal_run_mode_check CHECK (run_mode IN ('backfill', 'incremental', 'smoke', 'repair', 'research')),
    CONSTRAINT event_signal_run_time_mode_check CHECK (time_mode IN ('backtest', 'paper', 'live', 'observed')),
    CONSTRAINT event_signal_run_status_check CHECK (status IN ('RUNNING', 'SUCCESS', 'PARTIAL', 'FAILED', 'CANCELLED'))
);

CREATE TABLE IF NOT EXISTS market.event_fact (
    event_id BIGSERIAL PRIMARY KEY,
    event_key TEXT NOT NULL,
    ts_code TEXT NOT NULL,
    event_family TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_status TEXT NOT NULL DEFAULT 'ACTIVE',
    source_type TEXT NOT NULL,
    source_pk TEXT NOT NULL,
    source_record_key TEXT,
    source_event_date DATE NOT NULL,
    source_available_at TIMESTAMPTZ,
    source_time_quality TEXT NOT NULL,
    available_at TIMESTAMPTZ,
    effective_trade_date DATE NOT NULL,
    time_mode TEXT NOT NULL,
    report_period DATE,
    rule_version TEXT NOT NULL REFERENCES market.event_signal_rule_set(rule_version),
    run_id TEXT REFERENCES market.event_signal_run(run_id),
    fact_confidence NUMERIC(6, 4) NOT NULL DEFAULT 1.0,
    facts JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_payload_hash TEXT,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT event_fact_event_key_uniq UNIQUE (event_key),
    CONSTRAINT event_fact_status_check CHECK (event_status IN ('ACTIVE', 'REVISED', 'SUPERSEDED', 'CANCELLED', 'UNKNOWN')),
    CONSTRAINT event_fact_time_mode_check CHECK (time_mode IN ('backtest', 'paper', 'live', 'observed')),
    CONSTRAINT event_fact_source_time_quality_check CHECK (source_time_quality IN ('EXACT', 'DATE_ONLY', 'MIDNIGHT_DEFAULT', 'MISSING', 'OBSERVED')),
    CONSTRAINT event_fact_confidence_range CHECK (fact_confidence >= 0 AND fact_confidence <= 1)
);

CREATE TABLE IF NOT EXISTS market.event_relation (
    relation_id BIGSERIAL PRIMARY KEY,
    relation_key TEXT NOT NULL,
    relation_type TEXT NOT NULL,
    ts_code TEXT NOT NULL,
    report_period DATE,
    left_event_id BIGINT NOT NULL REFERENCES market.event_fact(event_id) ON DELETE CASCADE,
    right_event_id BIGINT NOT NULL REFERENCES market.event_fact(event_id) ON DELETE CASCADE,
    relation_status TEXT NOT NULL DEFAULT 'ACTIVE',
    rule_version TEXT NOT NULL REFERENCES market.event_signal_rule_set(rule_version),
    run_id TEXT REFERENCES market.event_signal_run(run_id),
    strength_score NUMERIC(6, 4) NOT NULL DEFAULT 1.0,
    confidence NUMERIC(6, 4) NOT NULL DEFAULT 1.0,
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT event_relation_relation_key_uniq UNIQUE (relation_key),
    CONSTRAINT event_relation_status_check CHECK (relation_status IN ('ACTIVE', 'REVISED', 'SUPERSEDED', 'CANCELLED', 'UNKNOWN')),
    CONSTRAINT event_relation_distinct_events_check CHECK (left_event_id <> right_event_id),
    CONSTRAINT event_relation_strength_range CHECK (strength_score >= 0 AND strength_score <= 1),
    CONSTRAINT event_relation_confidence_range CHECK (confidence >= 0 AND confidence <= 1)
);

CREATE TABLE IF NOT EXISTS market.event_signal (
    signal_id BIGSERIAL PRIMARY KEY,
    signal_key TEXT NOT NULL,
    ts_code TEXT NOT NULL,
    event_id BIGINT REFERENCES market.event_fact(event_id) ON DELETE SET NULL,
    source_event_ids BIGINT[] NOT NULL DEFAULT '{}'::bigint[],
    relation_ids BIGINT[] NOT NULL DEFAULT '{}'::bigint[],
    source_type TEXT NOT NULL,
    source_pk TEXT NOT NULL,
    source_event_date DATE NOT NULL,
    source_time_quality TEXT NOT NULL,
    available_at TIMESTAMPTZ,
    effective_trade_date DATE NOT NULL,
    time_mode TEXT NOT NULL,
    event_family TEXT NOT NULL,
    event_type TEXT NOT NULL,
    risk_level TEXT NOT NULL,
    action TEXT NOT NULL,
    signal_type TEXT NOT NULL DEFAULT 'risk',
    signal_status TEXT NOT NULL DEFAULT 'ACTIVE',
    severity_score NUMERIC(6, 4) NOT NULL,
    confidence NUMERIC(6, 4) NOT NULL,
    alpha_score NUMERIC(6, 4) NOT NULL DEFAULT 0.0,
    reason TEXT NOT NULL,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    effective_rule TEXT NOT NULL,
    rule_version TEXT NOT NULL REFERENCES market.event_signal_rule_set(rule_version),
    run_id TEXT REFERENCES market.event_signal_run(run_id),
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT event_signal_signal_key_uniq UNIQUE (signal_key),
    CONSTRAINT event_signal_time_mode_check CHECK (time_mode IN ('backtest', 'paper', 'live', 'observed')),
    CONSTRAINT event_signal_source_time_quality_check CHECK (source_time_quality IN ('EXACT', 'DATE_ONLY', 'MIDNIGHT_DEFAULT', 'MISSING', 'OBSERVED')),
    CONSTRAINT event_signal_risk_level_check CHECK (risk_level IN ('P0_BLOCK', 'P1_HIGH', 'P2_REVIEW', 'P3_POSITIVE_CANDIDATE', 'P4_NEUTRAL')),
    CONSTRAINT event_signal_action_check CHECK (action IN ('block_buy', 'warn_high', 'warn_review', 'record_only', 'alpha_hint_disabled', 'discard_or_archive', 'force_exit')),
    CONSTRAINT event_signal_type_check CHECK (signal_type IN ('risk', 'alpha_hint', 'audit', 'research')),
    CONSTRAINT event_signal_status_check CHECK (signal_status IN ('ACTIVE', 'RESOLVED', 'EXPIRED', 'SUPPRESSED')),
    CONSTRAINT event_signal_severity_range CHECK (severity_score >= 0 AND severity_score <= 1),
    CONSTRAINT event_signal_confidence_range CHECK (confidence >= 0 AND confidence <= 1),
    CONSTRAINT event_signal_alpha_range CHECK (alpha_score >= -1 AND alpha_score <= 1)
);

CREATE INDEX IF NOT EXISTS idx_event_signal_rule_set_active
    ON market.event_signal_rule_set(is_active, rule_version);

CREATE INDEX IF NOT EXISTS idx_event_signal_run_rule_status
    ON market.event_signal_run(rule_version, status, started_at);

CREATE INDEX IF NOT EXISTS idx_event_signal_run_time_mode_date
    ON market.event_signal_run(time_mode, date_from, date_to);

CREATE INDEX IF NOT EXISTS idx_event_fact_ts_effective_type
    ON market.event_fact(ts_code, effective_trade_date, event_type);

CREATE INDEX IF NOT EXISTS idx_event_fact_source
    ON market.event_fact(source_type, source_pk);

CREATE INDEX IF NOT EXISTS idx_event_fact_report_period
    ON market.event_fact(report_period, ts_code, event_family);

CREATE INDEX IF NOT EXISTS idx_event_fact_time_mode_effective
    ON market.event_fact(time_mode, effective_trade_date);

CREATE INDEX IF NOT EXISTS idx_event_fact_facts_gin
    ON market.event_fact USING GIN (facts);

CREATE INDEX IF NOT EXISTS idx_event_relation_report_period
    ON market.event_relation(ts_code, report_period, relation_type);

CREATE INDEX IF NOT EXISTS idx_event_relation_left_event
    ON market.event_relation(left_event_id);

CREATE INDEX IF NOT EXISTS idx_event_relation_right_event
    ON market.event_relation(right_event_id);

CREATE INDEX IF NOT EXISTS idx_event_signal_symbol_effective_risk_action
    ON market.event_signal(ts_code, time_mode, effective_trade_date, risk_level, action);

CREATE INDEX IF NOT EXISTS idx_event_signal_effective_status
    ON market.event_signal(time_mode, effective_trade_date, risk_level, signal_status);

CREATE INDEX IF NOT EXISTS idx_event_signal_event_type_date
    ON market.event_signal(event_type, effective_trade_date);

CREATE INDEX IF NOT EXISTS idx_event_signal_source_date
    ON market.event_signal(source_type, source_event_date);

CREATE INDEX IF NOT EXISTS idx_event_signal_evidence_gin
    ON market.event_signal USING GIN (evidence);

COMMENT ON TABLE market.event_signal_rule_set IS 'Versioned unified event signal rule-set metadata for deterministic announcement and financial event signal generation.';
COMMENT ON COLUMN market.event_signal_rule_set.rule_version IS 'Stable rule version identifier, for example unified_event_signal_rules_v0_20260506.';
COMMENT ON COLUMN market.event_signal_rule_set.engine_name IS 'Signal engine implementation name that generated facts, relations, and signals.';
COMMENT ON COLUMN market.event_signal_rule_set.rule_source IS 'Human-readable origin of the rule set, including local announcement rules and financial rules.';
COMMENT ON COLUMN market.event_signal_rule_set.rule_scope IS 'Business scope covered by this rule set, such as announcement_adapter_only or announcement_and_financial_v0.';
COMMENT ON COLUMN market.event_signal_rule_set.config_hash IS 'SHA256 hash of the normalized JSON rule configuration for reproducibility and drift checks.';
COMMENT ON COLUMN market.event_signal_rule_set.config IS 'JSONB rule configuration used by the event signal engine, including thresholds, actions, and source adapters.';
COMMENT ON COLUMN market.event_signal_rule_set.source_rule_versions IS 'JSONB map of upstream rule versions referenced by this unified rule set, such as announcement title rule versions.';
COMMENT ON COLUMN market.event_signal_rule_set.is_active IS 'Whether this rule version is active for new incremental event signal generation.';
COMMENT ON COLUMN market.event_signal_rule_set.created_at IS 'Database timestamp when this rule-set row was first inserted.';
COMMENT ON COLUMN market.event_signal_rule_set.updated_at IS 'Database timestamp when this rule-set row was last updated.';

COMMENT ON TABLE market.event_signal_run IS 'Auditable run history for unified event fact, relation, and signal generation jobs.';
COMMENT ON COLUMN market.event_signal_run.run_id IS 'Stable externally generated run identifier used to link generated facts, relations, signals, and logs.';
COMMENT ON COLUMN market.event_signal_run.rule_version IS 'Unified event signal rule version used by this generation run.';
COMMENT ON COLUMN market.event_signal_run.run_mode IS 'Generation mode: backfill, incremental, smoke, repair, or research.';
COMMENT ON COLUMN market.event_signal_run.time_mode IS 'Point-in-time visibility mode for this run: backtest, paper, live, or observed.';
COMMENT ON COLUMN market.event_signal_run.source_scope IS 'JSONB description of source tables, source date windows, symbols, and adapters included in this run.';
COMMENT ON COLUMN market.event_signal_run.date_from IS 'Inclusive source or effective date lower bound requested for this run; NULL means not date-bounded.';
COMMENT ON COLUMN market.event_signal_run.date_to IS 'Inclusive source or effective date upper bound requested for this run; NULL means not date-bounded.';
COMMENT ON COLUMN market.event_signal_run.started_at IS 'Database timestamp when the signal generation run started.';
COMMENT ON COLUMN market.event_signal_run.finished_at IS 'Database timestamp when the signal generation run finished; NULL while running.';
COMMENT ON COLUMN market.event_signal_run.status IS 'Lifecycle status of this run: RUNNING, SUCCESS, PARTIAL, FAILED, or CANCELLED.';
COMMENT ON COLUMN market.event_signal_run.source_input_rows IS 'Number of source rows scanned or accepted as input by this run.';
COMMENT ON COLUMN market.event_signal_run.fact_rows IS 'Number of event_fact rows inserted or updated by this run.';
COMMENT ON COLUMN market.event_signal_run.relation_rows IS 'Number of event_relation rows inserted or updated by this run.';
COMMENT ON COLUMN market.event_signal_run.signal_rows IS 'Number of event_signal rows inserted or updated by this run.';
COMMENT ON COLUMN market.event_signal_run.error_message IS 'Compact failure message when status is FAILED or PARTIAL; NULL for successful runs.';
COMMENT ON COLUMN market.event_signal_run.metrics IS 'JSONB run metrics and quality counters, such as skipped rows, source revisions, and timing details.';
COMMENT ON COLUMN market.event_signal_run.created_at IS 'Database timestamp when this run row was first inserted.';
COMMENT ON COLUMN market.event_signal_run.updated_at IS 'Database timestamp when this run row was last updated.';

COMMENT ON TABLE market.event_fact IS 'Standardized point-in-time event facts derived from source-specific announcement and financial raw tables; facts do not encode trading actions.';
COMMENT ON COLUMN market.event_fact.event_id IS 'Local surrogate primary key for one standardized event fact.';
COMMENT ON COLUMN market.event_fact.event_key IS 'Stable idempotency key including source, source business key, rule version, and time_mode.';
COMMENT ON COLUMN market.event_fact.ts_code IS 'A-share Tushare security code affected by this event fact.';
COMMENT ON COLUMN market.event_fact.event_family IS 'Coarse event family such as announcement_risk, financial_forecast, financial_express, or financial_indicator.';
COMMENT ON COLUMN market.event_fact.event_type IS 'Fine-grained event type assigned by deterministic adapters, for example regulatory_penalty or financial_forecast_large_growth.';
COMMENT ON COLUMN market.event_fact.event_status IS 'Fact lifecycle status after source revisions: ACTIVE, REVISED, SUPERSEDED, CANCELLED, or UNKNOWN.';
COMMENT ON COLUMN market.event_fact.source_type IS 'Canonical source type that produced this fact, such as announcement, tushare_forecast, tushare_express, or tushare_fina_indicator.';
COMMENT ON COLUMN market.event_fact.source_pk IS 'Source-specific primary key or local raw observation identifier represented as text.';
COMMENT ON COLUMN market.event_fact.source_record_key IS 'Source business key used for source revision grouping, such as ts_code plus report_period plus ann_date.';
COMMENT ON COLUMN market.event_fact.source_event_date IS 'Natural source event date, usually announcement date or Tushare ann_date, before trading calendar adjustment.';
COMMENT ON COLUMN market.event_fact.source_available_at IS 'Timestamp when the source claims the event became available; NULL when the source only provides a date.';
COMMENT ON COLUMN market.event_fact.source_time_quality IS 'Quality of source timing: EXACT, DATE_ONLY, MIDNIGHT_DEFAULT, MISSING, or OBSERVED.';
COMMENT ON COLUMN market.event_fact.available_at IS 'Point-in-time timestamp visible to AIstock under the selected time_mode; backtests may keep this NULL for date-only rows.';
COMMENT ON COLUMN market.event_fact.effective_trade_date IS 'First trading date when this fact may be consumed without look-ahead under the selected time_mode.';
COMMENT ON COLUMN market.event_fact.time_mode IS 'Visibility mode used to compute available_at and effective_trade_date: backtest, paper, live, or observed.';
COMMENT ON COLUMN market.event_fact.report_period IS 'Financial reporting period end date when applicable; NULL for non-financial event facts.';
COMMENT ON COLUMN market.event_fact.rule_version IS 'Unified rule version that produced this standardized fact.';
COMMENT ON COLUMN market.event_fact.run_id IS 'event_signal_run.run_id that generated or last refreshed this fact.';
COMMENT ON COLUMN market.event_fact.fact_confidence IS 'Extraction confidence in [0,1] for this standardized fact, independent of trading risk severity.';
COMMENT ON COLUMN market.event_fact.facts IS 'JSONB source-derived structured fact payload, amounts, ratios, matched title terms, and adapter trace.';
COMMENT ON COLUMN market.event_fact.source_payload_hash IS 'Hash of the source raw payload or source title used to detect upstream source revisions.';
COMMENT ON COLUMN market.event_fact.generated_at IS 'Database timestamp when this fact was generated by the event signal engine.';
COMMENT ON COLUMN market.event_fact.created_at IS 'Database timestamp when this fact row was first inserted.';
COMMENT ON COLUMN market.event_fact.updated_at IS 'Database timestamp when this fact row was last updated.';

COMMENT ON TABLE market.event_relation IS 'Versioned relations between event facts, such as forecast-to-formal-result matching or source cross-validation.';
COMMENT ON COLUMN market.event_relation.relation_id IS 'Local surrogate primary key for one relation between two event facts.';
COMMENT ON COLUMN market.event_relation.relation_key IS 'Stable idempotency key including relation type, both event keys, rule version, and relation direction.';
COMMENT ON COLUMN market.event_relation.relation_type IS 'Machine-readable relation type, such as formalizes_forecast, formalizes_express, or misses_prior_expectation.';
COMMENT ON COLUMN market.event_relation.ts_code IS 'A-share Tushare security code shared by the related event facts.';
COMMENT ON COLUMN market.event_relation.report_period IS 'Financial reporting period shared by the relation when applicable; NULL for non-financial relations.';
COMMENT ON COLUMN market.event_relation.left_event_id IS 'Left-side event_fact.event_id in this directed relation.';
COMMENT ON COLUMN market.event_relation.right_event_id IS 'Right-side event_fact.event_id in this directed relation.';
COMMENT ON COLUMN market.event_relation.relation_status IS 'Relation lifecycle status after source revisions: ACTIVE, REVISED, SUPERSEDED, CANCELLED, or UNKNOWN.';
COMMENT ON COLUMN market.event_relation.rule_version IS 'Unified rule version that produced this relation.';
COMMENT ON COLUMN market.event_relation.run_id IS 'event_signal_run.run_id that generated or last refreshed this relation.';
COMMENT ON COLUMN market.event_relation.strength_score IS 'Normalized relation strength in [0,1], for example magnitude of forecast miss evidence after clipping.';
COMMENT ON COLUMN market.event_relation.confidence IS 'Relation confidence in [0,1] based on source keys, report period match, and data quality.';
COMMENT ON COLUMN market.event_relation.metrics IS 'JSONB relation metrics, such as forecast midpoint, actual result, and difference versus expectation.';
COMMENT ON COLUMN market.event_relation.evidence IS 'JSONB evidence describing both source facts and relation-building rules.';
COMMENT ON COLUMN market.event_relation.generated_at IS 'Database timestamp when this relation was generated by the event signal engine.';
COMMENT ON COLUMN market.event_relation.created_at IS 'Database timestamp when this relation row was first inserted.';
COMMENT ON COLUMN market.event_relation.updated_at IS 'Database timestamp when this relation row was last updated.';

COMMENT ON TABLE market.event_signal IS 'Unified non-daily event signals consumable by future risk overlays and warning dashboards; current phase generates data only and does not trade.';
COMMENT ON COLUMN market.event_signal.signal_id IS 'Local surrogate primary key for one generated event signal.';
COMMENT ON COLUMN market.event_signal.signal_key IS 'Stable idempotency key including source events, event type, rule version, action, and time_mode.';
COMMENT ON COLUMN market.event_signal.ts_code IS 'A-share Tushare security code affected by this event signal.';
COMMENT ON COLUMN market.event_signal.event_id IS 'Primary event_fact.event_id that produced this signal; NULL if a future compaction keeps only evidence arrays.';
COMMENT ON COLUMN market.event_signal.source_event_ids IS 'Array of event_fact ids used as direct evidence for this signal.';
COMMENT ON COLUMN market.event_signal.relation_ids IS 'Array of event_relation ids used as cross-source or cross-period evidence for this signal.';
COMMENT ON COLUMN market.event_signal.source_type IS 'Canonical source type for the primary signal source, such as announcement or tushare_forecast.';
COMMENT ON COLUMN market.event_signal.source_pk IS 'Primary source record identifier represented as text for traceability back to the raw or announcement table.';
COMMENT ON COLUMN market.event_signal.source_event_date IS 'Natural source event date before trading calendar adjustment.';
COMMENT ON COLUMN market.event_signal.source_time_quality IS 'Quality of source timing used for effective-date logic: EXACT, DATE_ONLY, MIDNIGHT_DEFAULT, MISSING, or OBSERVED.';
COMMENT ON COLUMN market.event_signal.available_at IS 'Point-in-time timestamp when this signal became visible under the selected time_mode; NULL when only date is known.';
COMMENT ON COLUMN market.event_signal.effective_trade_date IS 'First trading date when this signal can affect a future risk overlay without look-ahead.';
COMMENT ON COLUMN market.event_signal.time_mode IS 'Visibility mode used for this signal: backtest, paper, live, or observed; included in signal_key to prevent overwrites.';
COMMENT ON COLUMN market.event_signal.event_family IS 'Coarse event family behind this signal, used for dashboards and grouped research.';
COMMENT ON COLUMN market.event_signal.event_type IS 'Fine-grained event type behind this signal, used for risk policy mapping and event studies.';
COMMENT ON COLUMN market.event_signal.risk_level IS 'Risk priority level: P0_BLOCK, P1_HIGH, P2_REVIEW, P3_POSITIVE_CANDIDATE, or P4_NEUTRAL.';
COMMENT ON COLUMN market.event_signal.action IS 'Recommended deterministic action: block_buy, warn_high, warn_review, record_only, alpha_hint_disabled, discard_or_archive, or force_exit.';
COMMENT ON COLUMN market.event_signal.signal_type IS 'Signal purpose: risk, alpha_hint, audit, or research; first phase keeps alpha hints disabled or record-only.';
COMMENT ON COLUMN market.event_signal.signal_status IS 'Signal lifecycle status: ACTIVE, RESOLVED, EXPIRED, or SUPPRESSED.';
COMMENT ON COLUMN market.event_signal.severity_score IS 'Normalized risk severity in [0,1] used for alert priority and future risk policy ranking.';
COMMENT ON COLUMN market.event_signal.confidence IS 'Signal confidence in [0,1] from deterministic classification or structured financial source quality.';
COMMENT ON COLUMN market.event_signal.alpha_score IS 'Research-only alpha hint score in [-1,1]; first phase defaults to 0 and must not affect current alpha or trading programs.';
COMMENT ON COLUMN market.event_signal.reason IS 'Short human-readable reason for warning displays, sample review, and audit reports.';
COMMENT ON COLUMN market.event_signal.evidence IS 'JSONB evidence including source keys, matched rule, numeric thresholds, titles, and effective-date trace.';
COMMENT ON COLUMN market.event_signal.effective_rule IS 'Machine-readable rule used to derive effective_trade_date and action from source timing and event semantics.';
COMMENT ON COLUMN market.event_signal.rule_version IS 'Unified rule version that generated this signal.';
COMMENT ON COLUMN market.event_signal.run_id IS 'event_signal_run.run_id that generated or last refreshed this signal.';
COMMENT ON COLUMN market.event_signal.generated_at IS 'Database timestamp when this signal was generated by the event signal engine.';
COMMENT ON COLUMN market.event_signal.created_at IS 'Database timestamp when this signal row was first inserted.';
COMMENT ON COLUMN market.event_signal.updated_at IS 'Database timestamp when this signal row was last updated.';

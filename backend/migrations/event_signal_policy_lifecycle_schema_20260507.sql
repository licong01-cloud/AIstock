-- Event signal policy lifecycle and QE validation schema.
-- Safe to run repeatedly after unified_event_signal_schema_20260506.sql.
-- Every table and column is commented for local data-management auditability.

CREATE SCHEMA IF NOT EXISTS market;

CREATE TABLE IF NOT EXISTS market.event_signal_policy_profile (
    profile_id TEXT PRIMARY KEY,
    profile_name TEXT NOT NULL,
    profile_version TEXT NOT NULL,
    profile_status TEXT NOT NULL DEFAULT 'DRAFT',
    policy_scope TEXT NOT NULL DEFAULT 'research_overlay',
    time_mode TEXT NOT NULL DEFAULT 'backtest',
    base_rule_versions JSONB NOT NULL DEFAULT '{}'::jsonb,
    default_action_mode TEXT NOT NULL DEFAULT 'risk_first',
    positive_overlay_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    formal_st_removal_required BOOLEAN NOT NULL DEFAULT TRUE,
    st_removal_cooldown_trading_days INTEGER NOT NULL DEFAULT 5,
    allow_buy_on_st_removal_expectation BOOLEAN NOT NULL DEFAULT FALSE,
    max_positive_score_delta NUMERIC(8, 6) NOT NULL DEFAULT 0.0,
    max_negative_score_delta NUMERIC(8, 6) NOT NULL DEFAULT 0.0,
    config_hash TEXT NOT NULL,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_by TEXT NOT NULL DEFAULT 'codex',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT event_signal_policy_profile_status_check CHECK (profile_status IN ('DRAFT', 'RESEARCH_VALIDATED', 'ACTIVE', 'RETIRED')),
    CONSTRAINT event_signal_policy_profile_scope_check CHECK (policy_scope IN ('research_overlay', 'backtest_candidate', 'paper_candidate', 'live_candidate')),
    CONSTRAINT event_signal_policy_profile_time_mode_check CHECK (time_mode IN ('backtest', 'paper', 'live', 'observed')),
    CONSTRAINT event_signal_policy_profile_cooldown_check CHECK (st_removal_cooldown_trading_days >= 0),
    CONSTRAINT event_signal_policy_profile_positive_delta_check CHECK (max_positive_score_delta >= 0),
    CONSTRAINT event_signal_policy_profile_negative_delta_check CHECK (max_negative_score_delta <= 0)
);

CREATE TABLE IF NOT EXISTS market.event_signal_effect_rule (
    effect_rule_id BIGSERIAL PRIMARY KEY,
    profile_id TEXT NOT NULL REFERENCES market.event_signal_policy_profile(profile_id) ON DELETE CASCADE,
    rule_key TEXT NOT NULL,
    rule_status TEXT NOT NULL DEFAULT 'DISABLED',
    event_family TEXT NOT NULL,
    event_type TEXT NOT NULL,
    source_type TEXT,
    source_rule_version TEXT,
    match_expression JSONB NOT NULL DEFAULT '{}'::jsonb,
    lifecycle_kind TEXT NOT NULL,
    state_family TEXT,
    state_type TEXT,
    opens_state BOOLEAN NOT NULL DEFAULT FALSE,
    closes_state BOOLEAN NOT NULL DEFAULT FALSE,
    requires_formal_resolution BOOLEAN NOT NULL DEFAULT FALSE,
    resolution_event_types TEXT[] NOT NULL DEFAULT '{}'::text[],
    policy_risk_level TEXT NOT NULL,
    primary_action TEXT NOT NULL,
    block_buy BOOLEAN NOT NULL DEFAULT FALSE,
    block_add BOOLEAN NOT NULL DEFAULT FALSE,
    force_exit BOOLEAN NOT NULL DEFAULT FALSE,
    sell_only BOOLEAN NOT NULL DEFAULT FALSE,
    validity_trading_days INTEGER,
    decay_start_trading_days INTEGER,
    decay_half_life_trading_days INTEGER,
    cooldown_trading_days INTEGER NOT NULL DEFAULT 0,
    severity_weight NUMERIC(8, 6) NOT NULL DEFAULT 1.0,
    confidence_floor NUMERIC(8, 6) NOT NULL DEFAULT 0.0,
    score_delta NUMERIC(8, 6) NOT NULL DEFAULT 0.0,
    score_multiplier NUMERIC(8, 6) NOT NULL DEFAULT 1.0,
    score_overlay_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    priority INTEGER NOT NULL DEFAULT 100,
    is_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    effective_from DATE,
    effective_to DATE,
    rule_params JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT event_signal_effect_rule_profile_key_uniq UNIQUE (profile_id, rule_key),
    CONSTRAINT event_signal_effect_rule_status_check CHECK (rule_status IN ('DISABLED', 'RESEARCH', 'VALIDATED', 'ENABLED', 'RETIRED')),
    CONSTRAINT event_signal_effect_rule_lifecycle_kind_check CHECK (lifecycle_kind IN ('state', 'window', 'decay', 'record_only', 'close_state')),
    CONSTRAINT event_signal_effect_rule_policy_risk_check CHECK (policy_risk_level IN ('P0_FORCE_EXIT', 'P0_BLOCK', 'P1_HIGH', 'P2_REVIEW', 'P3_POSITIVE_CANDIDATE', 'P4_NEUTRAL')),
    CONSTRAINT event_signal_effect_rule_action_check CHECK (primary_action IN ('force_exit', 'block_buy', 'block_add', 'warn', 'record_only', 'score_down', 'score_up', 'none')),
    CONSTRAINT event_signal_effect_rule_validity_check CHECK (validity_trading_days IS NULL OR validity_trading_days >= 0),
    CONSTRAINT event_signal_effect_rule_decay_start_check CHECK (decay_start_trading_days IS NULL OR decay_start_trading_days >= 0),
    CONSTRAINT event_signal_effect_rule_decay_half_life_check CHECK (decay_half_life_trading_days IS NULL OR decay_half_life_trading_days > 0),
    CONSTRAINT event_signal_effect_rule_cooldown_check CHECK (cooldown_trading_days >= 0),
    CONSTRAINT event_signal_effect_rule_severity_weight_check CHECK (severity_weight >= 0),
    CONSTRAINT event_signal_effect_rule_confidence_floor_check CHECK (confidence_floor >= 0 AND confidence_floor <= 1),
    CONSTRAINT event_signal_effect_rule_score_multiplier_check CHECK (score_multiplier >= 0),
    CONSTRAINT event_signal_effect_rule_effective_date_check CHECK (effective_to IS NULL OR effective_from IS NULL OR effective_to >= effective_from)
);

CREATE TABLE IF NOT EXISTS market.event_signal_state_span (
    state_span_id BIGSERIAL PRIMARY KEY,
    state_key TEXT NOT NULL,
    profile_id TEXT NOT NULL REFERENCES market.event_signal_policy_profile(profile_id) ON DELETE CASCADE,
    ts_code TEXT NOT NULL,
    time_mode TEXT NOT NULL,
    state_family TEXT NOT NULL,
    state_type TEXT NOT NULL,
    state_status TEXT NOT NULL DEFAULT 'OPEN',
    opened_by_signal_id BIGINT REFERENCES market.event_signal(signal_id) ON DELETE SET NULL,
    closed_by_signal_id BIGINT REFERENCES market.event_signal(signal_id) ON DELETE SET NULL,
    open_event_type TEXT NOT NULL,
    close_event_type TEXT,
    start_trade_date DATE NOT NULL,
    end_trade_date DATE,
    expiry_trade_date DATE,
    cooldown_until_trade_date DATE,
    available_at_start TIMESTAMPTZ,
    available_at_end TIMESTAMPTZ,
    source_time_quality TEXT NOT NULL,
    policy_risk_level TEXT NOT NULL,
    primary_action TEXT NOT NULL,
    severity_score NUMERIC(8, 6) NOT NULL DEFAULT 0.0,
    confidence NUMERIC(8, 6) NOT NULL DEFAULT 0.0,
    score_delta NUMERIC(8, 6) NOT NULL DEFAULT 0.0,
    score_multiplier NUMERIC(8, 6) NOT NULL DEFAULT 1.0,
    effect_rule_id BIGINT REFERENCES market.event_signal_effect_rule(effect_rule_id) ON DELETE SET NULL,
    run_id TEXT REFERENCES market.event_signal_run(run_id) ON DELETE SET NULL,
    policy_snapshot_hash TEXT NOT NULL,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT event_signal_state_span_state_key_uniq UNIQUE (state_key),
    CONSTRAINT event_signal_state_span_time_mode_check CHECK (time_mode IN ('backtest', 'paper', 'live', 'observed')),
    CONSTRAINT event_signal_state_span_status_check CHECK (state_status IN ('OPEN', 'CLOSED', 'EXPIRED', 'SUPPRESSED')),
    CONSTRAINT event_signal_state_span_source_time_quality_check CHECK (source_time_quality IN ('EXACT', 'DATE_ONLY', 'MIDNIGHT_DEFAULT', 'MISSING', 'OBSERVED', 'LOCAL_FIRST_SEEN')),
    CONSTRAINT event_signal_state_span_policy_risk_check CHECK (policy_risk_level IN ('P0_FORCE_EXIT', 'P0_BLOCK', 'P1_HIGH', 'P2_REVIEW', 'P3_POSITIVE_CANDIDATE', 'P4_NEUTRAL')),
    CONSTRAINT event_signal_state_span_action_check CHECK (primary_action IN ('force_exit', 'block_buy', 'block_add', 'warn', 'record_only', 'score_down', 'score_up', 'none')),
    CONSTRAINT event_signal_state_span_date_order_check CHECK (end_trade_date IS NULL OR end_trade_date >= start_trade_date),
    CONSTRAINT event_signal_state_span_expiry_check CHECK (expiry_trade_date IS NULL OR expiry_trade_date >= start_trade_date),
    CONSTRAINT event_signal_state_span_cooldown_check CHECK (cooldown_until_trade_date IS NULL OR cooldown_until_trade_date >= start_trade_date),
    CONSTRAINT event_signal_state_span_severity_range CHECK (severity_score >= 0 AND severity_score <= 1),
    CONSTRAINT event_signal_state_span_confidence_range CHECK (confidence >= 0 AND confidence <= 1),
    CONSTRAINT event_signal_state_span_score_multiplier_check CHECK (score_multiplier >= 0)
);

CREATE TABLE IF NOT EXISTS market.event_signal_daily_overlay (
    overlay_id BIGSERIAL PRIMARY KEY,
    overlay_key TEXT NOT NULL,
    profile_id TEXT NOT NULL REFERENCES market.event_signal_policy_profile(profile_id) ON DELETE CASCADE,
    trade_date DATE NOT NULL,
    ts_code TEXT NOT NULL,
    time_mode TEXT NOT NULL,
    decision_status TEXT NOT NULL DEFAULT 'ACTIVE',
    can_buy BOOLEAN NOT NULL DEFAULT TRUE,
    can_add BOOLEAN NOT NULL DEFAULT TRUE,
    force_exit BOOLEAN NOT NULL DEFAULT FALSE,
    sell_only BOOLEAN NOT NULL DEFAULT FALSE,
    position_target_override NUMERIC(10, 6),
    policy_risk_level TEXT NOT NULL,
    primary_action TEXT NOT NULL,
    risk_score NUMERIC(8, 6) NOT NULL DEFAULT 0.0,
    alpha_score_delta NUMERIC(8, 6) NOT NULL DEFAULT 0.0,
    score_multiplier NUMERIC(8, 6) NOT NULL DEFAULT 1.0,
    score_overlay_enabled BOOLEAN NOT NULL DEFAULT FALSE,
    active_state_span_ids BIGINT[] NOT NULL DEFAULT '{}'::bigint[],
    active_signal_ids BIGINT[] NOT NULL DEFAULT '{}'::bigint[],
    reason_codes TEXT[] NOT NULL DEFAULT '{}'::text[],
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    run_id TEXT REFERENCES market.event_signal_run(run_id) ON DELETE SET NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT event_signal_daily_overlay_key_uniq UNIQUE (overlay_key),
    CONSTRAINT event_signal_daily_overlay_profile_date_symbol_uniq UNIQUE (profile_id, time_mode, trade_date, ts_code),
    CONSTRAINT event_signal_daily_overlay_time_mode_check CHECK (time_mode IN ('backtest', 'paper', 'live', 'observed')),
    CONSTRAINT event_signal_daily_overlay_status_check CHECK (decision_status IN ('ACTIVE', 'NO_EFFECT', 'SUPPRESSED')),
    CONSTRAINT event_signal_daily_overlay_policy_risk_check CHECK (policy_risk_level IN ('P0_FORCE_EXIT', 'P0_BLOCK', 'P1_HIGH', 'P2_REVIEW', 'P3_POSITIVE_CANDIDATE', 'P4_NEUTRAL')),
    CONSTRAINT event_signal_daily_overlay_action_check CHECK (primary_action IN ('force_exit', 'block_buy', 'block_add', 'warn', 'record_only', 'score_down', 'score_up', 'none')),
    CONSTRAINT event_signal_daily_overlay_target_range CHECK (position_target_override IS NULL OR (position_target_override >= 0 AND position_target_override <= 1)),
    CONSTRAINT event_signal_daily_overlay_risk_score_range CHECK (risk_score >= 0 AND risk_score <= 1),
    CONSTRAINT event_signal_daily_overlay_score_multiplier_check CHECK (score_multiplier >= 0)
);

CREATE TABLE IF NOT EXISTS market.event_signal_validation_result (
    validation_id BIGSERIAL PRIMARY KEY,
    validation_key TEXT NOT NULL,
    profile_id TEXT REFERENCES market.event_signal_policy_profile(profile_id) ON DELETE SET NULL,
    effect_rule_id BIGINT REFERENCES market.event_signal_effect_rule(effect_rule_id) ON DELETE SET NULL,
    candidate_signal_scope JSONB NOT NULL DEFAULT '{}'::jsonb,
    experiment_id TEXT NOT NULL,
    loop_id TEXT NOT NULL,
    loop_path TEXT NOT NULL,
    validation_mode TEXT NOT NULL,
    simulator_version TEXT NOT NULL,
    time_mode TEXT NOT NULL,
    date_from DATE NOT NULL,
    date_to DATE NOT NULL,
    policy_config_hash TEXT NOT NULL,
    input_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
    baseline_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    overlay_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    delta_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    hit_stats JSONB NOT NULL DEFAULT '{}'::jsonb,
    acceptance_gates JSONB NOT NULL DEFAULT '{}'::jsonb,
    decision TEXT NOT NULL DEFAULT 'REVIEW',
    decision_reason TEXT NOT NULL DEFAULT '',
    report_path TEXT,
    artifact_paths JSONB NOT NULL DEFAULT '{}'::jsonb,
    validated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT event_signal_validation_result_key_uniq UNIQUE (validation_key),
    CONSTRAINT event_signal_validation_result_mode_check CHECK (validation_mode IN ('single_signal', 'stacked_profile', 'parameter_sweep', 'event_study')),
    CONSTRAINT event_signal_validation_result_time_mode_check CHECK (time_mode IN ('backtest', 'paper', 'live', 'observed')),
    CONSTRAINT event_signal_validation_result_date_order_check CHECK (date_to >= date_from),
    CONSTRAINT event_signal_validation_result_decision_check CHECK (decision IN ('PASS', 'REVIEW', 'REJECT', 'DEFER'))
);

CREATE INDEX IF NOT EXISTS idx_event_signal_policy_profile_status
    ON market.event_signal_policy_profile(profile_status, policy_scope, time_mode);

CREATE INDEX IF NOT EXISTS idx_event_signal_effect_rule_profile_enabled
    ON market.event_signal_effect_rule(profile_id, is_enabled, priority);

CREATE INDEX IF NOT EXISTS idx_event_signal_effect_rule_event_type
    ON market.event_signal_effect_rule(event_family, event_type, rule_status);

CREATE INDEX IF NOT EXISTS idx_event_signal_effect_rule_match_gin
    ON market.event_signal_effect_rule USING GIN (match_expression);

CREATE INDEX IF NOT EXISTS idx_event_signal_state_span_active_lookup
    ON market.event_signal_state_span(profile_id, time_mode, ts_code, state_status, start_trade_date, end_trade_date);

CREATE INDEX IF NOT EXISTS idx_event_signal_state_span_date_range
    ON market.event_signal_state_span(profile_id, time_mode, start_trade_date, end_trade_date, cooldown_until_trade_date);

CREATE INDEX IF NOT EXISTS idx_event_signal_state_span_signal_refs
    ON market.event_signal_state_span(opened_by_signal_id, closed_by_signal_id);

CREATE INDEX IF NOT EXISTS idx_event_signal_state_span_evidence_gin
    ON market.event_signal_state_span USING GIN (evidence);

CREATE INDEX IF NOT EXISTS idx_event_signal_daily_overlay_profile_date
    ON market.event_signal_daily_overlay(profile_id, time_mode, trade_date);

CREATE INDEX IF NOT EXISTS idx_event_signal_daily_overlay_symbol_date
    ON market.event_signal_daily_overlay(ts_code, trade_date, profile_id);

CREATE INDEX IF NOT EXISTS idx_event_signal_daily_overlay_actions
    ON market.event_signal_daily_overlay(profile_id, time_mode, trade_date, force_exit, can_buy, policy_risk_level);

CREATE INDEX IF NOT EXISTS idx_event_signal_daily_overlay_evidence_gin
    ON market.event_signal_daily_overlay USING GIN (evidence);

CREATE INDEX IF NOT EXISTS idx_event_signal_validation_result_experiment
    ON market.event_signal_validation_result(experiment_id, loop_id, validation_mode, validated_at);

CREATE INDEX IF NOT EXISTS idx_event_signal_validation_result_profile_decision
    ON market.event_signal_validation_result(profile_id, decision, validated_at);

CREATE INDEX IF NOT EXISTS idx_event_signal_validation_result_scope_gin
    ON market.event_signal_validation_result USING GIN (candidate_signal_scope);

COMMENT ON TABLE market.event_signal_policy_profile IS 'Versioned policy profile that maps raw event_signal rows into lifecycle-aware risk and score overlay behavior.';
COMMENT ON COLUMN market.event_signal_policy_profile.profile_id IS 'Stable policy profile identifier used by state span, daily overlay, and validation result rows.';
COMMENT ON COLUMN market.event_signal_policy_profile.profile_name IS 'Human-readable policy profile name for reports, dashboards, and manual review.';
COMMENT ON COLUMN market.event_signal_policy_profile.profile_version IS 'Policy profile version string; any behavior-changing configuration update must create a new version.';
COMMENT ON COLUMN market.event_signal_policy_profile.profile_status IS 'Policy lifecycle status: DRAFT, RESEARCH_VALIDATED, ACTIVE, or RETIRED.';
COMMENT ON COLUMN market.event_signal_policy_profile.policy_scope IS 'Intended consumption scope: research_overlay, backtest_candidate, paper_candidate, or live_candidate.';
COMMENT ON COLUMN market.event_signal_policy_profile.time_mode IS 'Default point-in-time visibility mode for this profile: backtest, paper, live, or observed.';
COMMENT ON COLUMN market.event_signal_policy_profile.base_rule_versions IS 'JSONB map of upstream event_signal, announcement, and financial rule versions referenced by this profile.';
COMMENT ON COLUMN market.event_signal_policy_profile.default_action_mode IS 'Conflict-resolution mode for daily overlay generation; risk_first means hard risk overrides positive hints.';
COMMENT ON COLUMN market.event_signal_policy_profile.positive_overlay_enabled IS 'Whether positive alpha score overlays may affect consumers; first-stage research profiles keep this false.';
COMMENT ON COLUMN market.event_signal_policy_profile.formal_st_removal_required IS 'Whether ST hard-risk states require formal removal confirmation before buy eligibility can return.';
COMMENT ON COLUMN market.event_signal_policy_profile.st_removal_cooldown_trading_days IS 'Number of trading days to keep buy prohibition after formal ST removal; default is 5 for first validation.';
COMMENT ON COLUMN market.event_signal_policy_profile.allow_buy_on_st_removal_expectation IS 'Whether ST removal expectation or application events can allow buying before formal removal; current policy keeps this false.';
COMMENT ON COLUMN market.event_signal_policy_profile.max_positive_score_delta IS 'Profile-level cap for positive alpha score delta; unit is normalized score points and first-stage value is zero.';
COMMENT ON COLUMN market.event_signal_policy_profile.max_negative_score_delta IS 'Profile-level cap for negative alpha score delta; unit is normalized score points and non-positive by constraint.';
COMMENT ON COLUMN market.event_signal_policy_profile.config_hash IS 'SHA256 hash of the normalized full policy configuration for reproducible backtest/live parity checks.';
COMMENT ON COLUMN market.event_signal_policy_profile.config IS 'JSONB full profile configuration including risk priorities, default decay settings, and future consumer switches.';
COMMENT ON COLUMN market.event_signal_policy_profile.created_by IS 'Tool or operator that created the policy profile row.';
COMMENT ON COLUMN market.event_signal_policy_profile.created_at IS 'Database timestamp when this policy profile row was first inserted.';
COMMENT ON COLUMN market.event_signal_policy_profile.updated_at IS 'Database timestamp when this policy profile row was last updated.';

COMMENT ON TABLE market.event_signal_effect_rule IS 'Per-profile rule that converts source event_signal rows into state spans, daily overlay actions, and future score adjustments.';
COMMENT ON COLUMN market.event_signal_effect_rule.effect_rule_id IS 'Local surrogate primary key for one event signal effect rule.';
COMMENT ON COLUMN market.event_signal_effect_rule.profile_id IS 'Policy profile that owns this effect rule; deleting the profile deletes its rules.';
COMMENT ON COLUMN market.event_signal_effect_rule.rule_key IS 'Stable rule key unique within one profile, for example st_imposed_force_exit_v1.';
COMMENT ON COLUMN market.event_signal_effect_rule.rule_status IS 'Research lifecycle for this rule: DISABLED, RESEARCH, VALIDATED, ENABLED, or RETIRED.';
COMMENT ON COLUMN market.event_signal_effect_rule.event_family IS 'event_signal.event_family value matched by this rule.';
COMMENT ON COLUMN market.event_signal_effect_rule.event_type IS 'event_signal.event_type value matched by this rule.';
COMMENT ON COLUMN market.event_signal_effect_rule.source_type IS 'Optional event_signal.source_type filter, such as announcement or tushare_forecast; NULL means any source.';
COMMENT ON COLUMN market.event_signal_effect_rule.source_rule_version IS 'Optional upstream event_signal.rule_version filter; NULL means any source rule version accepted by the profile.';
COMMENT ON COLUMN market.event_signal_effect_rule.match_expression IS 'JSONB additional match conditions, such as risk_level, action, evidence flags, or source quality requirements.';
COMMENT ON COLUMN market.event_signal_effect_rule.lifecycle_kind IS 'Lifecycle resolver kind: state, window, decay, record_only, or close_state.';
COMMENT ON COLUMN market.event_signal_effect_rule.state_family IS 'State family opened or closed by this rule, such as st_hard_risk or financial_warning.';
COMMENT ON COLUMN market.event_signal_effect_rule.state_type IS 'Fine-grained state type opened or closed by this rule, such as st_active_block or st_removal_cooldown.';
COMMENT ON COLUMN market.event_signal_effect_rule.opens_state IS 'Whether matching source signals open a persistent state span.';
COMMENT ON COLUMN market.event_signal_effect_rule.closes_state IS 'Whether matching source signals close an existing persistent state span.';
COMMENT ON COLUMN market.event_signal_effect_rule.requires_formal_resolution IS 'Whether this rule-created state can only be closed by formal resolution events instead of expectation events.';
COMMENT ON COLUMN market.event_signal_effect_rule.resolution_event_types IS 'Event types allowed to resolve the state created by this rule; empty means no explicit resolver list.';
COMMENT ON COLUMN market.event_signal_effect_rule.policy_risk_level IS 'Policy-layer risk level, including P0_FORCE_EXIT which is intentionally not stored in raw event_signal.risk_level.';
COMMENT ON COLUMN market.event_signal_effect_rule.primary_action IS 'Primary overlay action produced by this rule: force_exit, block_buy, block_add, warn, record_only, score_down, score_up, or none.';
COMMENT ON COLUMN market.event_signal_effect_rule.block_buy IS 'Whether the rule blocks new buys while active.';
COMMENT ON COLUMN market.event_signal_effect_rule.block_add IS 'Whether the rule blocks adding to an existing position while active.';
COMMENT ON COLUMN market.event_signal_effect_rule.force_exit IS 'Whether the rule forces existing positions to exit while active.';
COMMENT ON COLUMN market.event_signal_effect_rule.sell_only IS 'Whether the rule allows selling only and disallows buy or add operations while active.';
COMMENT ON COLUMN market.event_signal_effect_rule.validity_trading_days IS 'Fixed active window length measured in trading days; NULL means state resolver controls the end.';
COMMENT ON COLUMN market.event_signal_effect_rule.decay_start_trading_days IS 'Trading-day offset when score or severity decay starts; NULL means no decay start is configured.';
COMMENT ON COLUMN market.event_signal_effect_rule.decay_half_life_trading_days IS 'Half-life for decay measured in trading days; NULL means no half-life decay is configured.';
COMMENT ON COLUMN market.event_signal_effect_rule.cooldown_trading_days IS 'Trading-day cooldown after a close event before the symbol can return to normal eligibility.';
COMMENT ON COLUMN market.event_signal_effect_rule.severity_weight IS 'Multiplier applied to source signal severity when producing state and daily risk_score.';
COMMENT ON COLUMN market.event_signal_effect_rule.confidence_floor IS 'Minimum source signal confidence required for this rule to produce state or overlay output.';
COMMENT ON COLUMN market.event_signal_effect_rule.score_delta IS 'Configured normalized score delta for future alpha overlay; current first-stage hard-risk rules keep zero.';
COMMENT ON COLUMN market.event_signal_effect_rule.score_multiplier IS 'Configured score multiplier for future alpha overlay; current first-stage rules keep one.';
COMMENT ON COLUMN market.event_signal_effect_rule.score_overlay_enabled IS 'Whether score_delta or score_multiplier may be consumed by future selection or trading systems.';
COMMENT ON COLUMN market.event_signal_effect_rule.priority IS 'Conflict resolution priority within a profile; lower numbers override higher numbers.';
COMMENT ON COLUMN market.event_signal_effect_rule.is_enabled IS 'Whether this rule participates in production overlay generation for its profile.';
COMMENT ON COLUMN market.event_signal_effect_rule.effective_from IS 'First source or trade date where the rule is valid; NULL means no lower date bound.';
COMMENT ON COLUMN market.event_signal_effect_rule.effective_to IS 'Last source or trade date where the rule is valid; NULL means no upper date bound.';
COMMENT ON COLUMN market.event_signal_effect_rule.rule_params IS 'JSONB extension parameters, including future switches such as allowing buy on ST removal expectation.';
COMMENT ON COLUMN market.event_signal_effect_rule.created_at IS 'Database timestamp when this effect rule row was first inserted.';
COMMENT ON COLUMN market.event_signal_effect_rule.updated_at IS 'Database timestamp when this effect rule row was last updated.';

COMMENT ON TABLE market.event_signal_state_span IS 'Lifecycle-expanded per-symbol event risk or alpha state spans derived from source event_signal rows and policy effect rules.';
COMMENT ON COLUMN market.event_signal_state_span.state_span_id IS 'Local surrogate primary key for one lifecycle-expanded state span.';
COMMENT ON COLUMN market.event_signal_state_span.state_key IS 'Stable idempotency key including profile, time_mode, symbol, state type, and opening source signal.';
COMMENT ON COLUMN market.event_signal_state_span.profile_id IS 'Policy profile used to derive this state span.';
COMMENT ON COLUMN market.event_signal_state_span.ts_code IS 'A-share Tushare security code affected by this state span.';
COMMENT ON COLUMN market.event_signal_state_span.time_mode IS 'Point-in-time visibility mode for the state span: backtest, paper, live, or observed.';
COMMENT ON COLUMN market.event_signal_state_span.state_family IS 'Coarse state family such as st_hard_risk, st_removal_cooldown, or financial_warning.';
COMMENT ON COLUMN market.event_signal_state_span.state_type IS 'Fine-grained state type used by daily overlay generation.';
COMMENT ON COLUMN market.event_signal_state_span.state_status IS 'State lifecycle status: OPEN, CLOSED, EXPIRED, or SUPPRESSED.';
COMMENT ON COLUMN market.event_signal_state_span.opened_by_signal_id IS 'event_signal.signal_id that opened this state span; NULL if the source signal was later deleted.';
COMMENT ON COLUMN market.event_signal_state_span.closed_by_signal_id IS 'event_signal.signal_id that closed this state span; NULL while open or if closed by expiry.';
COMMENT ON COLUMN market.event_signal_state_span.open_event_type IS 'Event type that opened this state span.';
COMMENT ON COLUMN market.event_signal_state_span.close_event_type IS 'Event type that closed this state span, if any.';
COMMENT ON COLUMN market.event_signal_state_span.start_trade_date IS 'First trading date when this state can affect overlay decisions without look-ahead.';
COMMENT ON COLUMN market.event_signal_state_span.end_trade_date IS 'Last trading date when this state affects overlay decisions; NULL for open-ended active state.';
COMMENT ON COLUMN market.event_signal_state_span.expiry_trade_date IS 'Rule-derived expiry trading date for fixed-window states; NULL for formal-resolution states.';
COMMENT ON COLUMN market.event_signal_state_span.cooldown_until_trade_date IS 'Last trading date of post-resolution cooldown, such as five trading days after formal ST removal.';
COMMENT ON COLUMN market.event_signal_state_span.available_at_start IS 'Timestamp when the opening state became visible under the selected time_mode.';
COMMENT ON COLUMN market.event_signal_state_span.available_at_end IS 'Timestamp when the closing state became visible under the selected time_mode.';
COMMENT ON COLUMN market.event_signal_state_span.source_time_quality IS 'Timing quality inherited from the opening source signal: EXACT, DATE_ONLY, MIDNIGHT_DEFAULT, MISSING, OBSERVED, or LOCAL_FIRST_SEEN.';
COMMENT ON COLUMN market.event_signal_state_span.policy_risk_level IS 'Policy-layer risk level active for this state span, including P0_FORCE_EXIT for hard sell-and-block states.';
COMMENT ON COLUMN market.event_signal_state_span.primary_action IS 'Primary action active during this state span.';
COMMENT ON COLUMN market.event_signal_state_span.severity_score IS 'Normalized state severity in [0,1] after applying effect-rule severity weight.';
COMMENT ON COLUMN market.event_signal_state_span.confidence IS 'State confidence in [0,1] inherited or derived from source signal and rule confidence floor.';
COMMENT ON COLUMN market.event_signal_state_span.score_delta IS 'Normalized score delta active during this state span; first-stage profiles keep zero.';
COMMENT ON COLUMN market.event_signal_state_span.score_multiplier IS 'Score multiplier active during this state span; first-stage profiles keep one.';
COMMENT ON COLUMN market.event_signal_state_span.effect_rule_id IS 'Effect rule that generated this state span; NULL if the rule was deleted after generation.';
COMMENT ON COLUMN market.event_signal_state_span.run_id IS 'event_signal_run.run_id or lifecycle generation run identifier that created or refreshed this state span.';
COMMENT ON COLUMN market.event_signal_state_span.policy_snapshot_hash IS 'Hash of policy profile and effect rule configuration used to generate this state span.';
COMMENT ON COLUMN market.event_signal_state_span.evidence IS 'JSONB evidence for opening, closing, expiry, cooldown, and source timing trace.';
COMMENT ON COLUMN market.event_signal_state_span.created_at IS 'Database timestamp when this state span row was first inserted.';
COMMENT ON COLUMN market.event_signal_state_span.updated_at IS 'Database timestamp when this state span row was last updated.';

COMMENT ON TABLE market.event_signal_daily_overlay IS 'Per-trade-date per-symbol decision overlay generated from active event signal states and effect rules for research validation and future consumers.';
COMMENT ON COLUMN market.event_signal_daily_overlay.overlay_id IS 'Local surrogate primary key for one daily event signal overlay decision.';
COMMENT ON COLUMN market.event_signal_daily_overlay.overlay_key IS 'Stable idempotency key including profile, time_mode, trade_date, and symbol.';
COMMENT ON COLUMN market.event_signal_daily_overlay.profile_id IS 'Policy profile used to generate this daily overlay row.';
COMMENT ON COLUMN market.event_signal_daily_overlay.trade_date IS 'Trading date when this overlay row is effective.';
COMMENT ON COLUMN market.event_signal_daily_overlay.ts_code IS 'A-share Tushare security code affected by this daily overlay row.';
COMMENT ON COLUMN market.event_signal_daily_overlay.time_mode IS 'Point-in-time visibility mode for this overlay: backtest, paper, live, or observed.';
COMMENT ON COLUMN market.event_signal_daily_overlay.decision_status IS 'Overlay row status: ACTIVE means a decision applies, NO_EFFECT means explicit no-risk row, SUPPRESSED means ignored by policy.';
COMMENT ON COLUMN market.event_signal_daily_overlay.can_buy IS 'Whether new buy orders are allowed for this symbol on this trade_date under the profile.';
COMMENT ON COLUMN market.event_signal_daily_overlay.can_add IS 'Whether adding to an existing position is allowed for this symbol on this trade_date.';
COMMENT ON COLUMN market.event_signal_daily_overlay.force_exit IS 'Whether existing positions must be sold or reduced to the configured target on this trade_date.';
COMMENT ON COLUMN market.event_signal_daily_overlay.sell_only IS 'Whether only sell actions are allowed for this symbol on this trade_date.';
COMMENT ON COLUMN market.event_signal_daily_overlay.position_target_override IS 'Optional target position weight override; zero represents full forced exit for hard-risk states.';
COMMENT ON COLUMN market.event_signal_daily_overlay.policy_risk_level IS 'Highest policy-layer risk level after combining active states and direct event signals.';
COMMENT ON COLUMN market.event_signal_daily_overlay.primary_action IS 'Final primary action after risk-priority conflict resolution.';
COMMENT ON COLUMN market.event_signal_daily_overlay.risk_score IS 'Combined normalized risk score in [0,1] for alert ordering and validation diagnostics.';
COMMENT ON COLUMN market.event_signal_daily_overlay.alpha_score_delta IS 'Combined normalized alpha score delta; first-stage profiles keep zero and disabled.';
COMMENT ON COLUMN market.event_signal_daily_overlay.score_multiplier IS 'Combined score multiplier; first-stage profiles keep one and disabled.';
COMMENT ON COLUMN market.event_signal_daily_overlay.score_overlay_enabled IS 'Whether alpha_score_delta and score_multiplier are allowed to affect future consumers.';
COMMENT ON COLUMN market.event_signal_daily_overlay.active_state_span_ids IS 'Array of event_signal_state_span ids active on this trade_date and used by the overlay.';
COMMENT ON COLUMN market.event_signal_daily_overlay.active_signal_ids IS 'Array of event_signal.signal_id values directly used by this overlay row.';
COMMENT ON COLUMN market.event_signal_daily_overlay.reason_codes IS 'Machine-readable reason codes explaining the final overlay decision.';
COMMENT ON COLUMN market.event_signal_daily_overlay.evidence IS 'JSONB conflict-resolution evidence, active state summaries, and source trace for this overlay row.';
COMMENT ON COLUMN market.event_signal_daily_overlay.run_id IS 'event_signal_run.run_id or overlay generation run identifier that created or refreshed this row.';
COMMENT ON COLUMN market.event_signal_daily_overlay.created_at IS 'Database timestamp when this daily overlay row was first inserted.';
COMMENT ON COLUMN market.event_signal_daily_overlay.updated_at IS 'Database timestamp when this daily overlay row was last updated.';

COMMENT ON TABLE market.event_signal_validation_result IS 'Research validation records for single event-signal rules and stacked policy profiles against fixed QE loop artifacts.';
COMMENT ON COLUMN market.event_signal_validation_result.validation_id IS 'Local surrogate primary key for one event signal validation result.';
COMMENT ON COLUMN market.event_signal_validation_result.validation_key IS 'Stable idempotency key including experiment, loop, profile, candidate rule, and simulator version.';
COMMENT ON COLUMN market.event_signal_validation_result.profile_id IS 'Policy profile validated by this row; NULL is allowed for legacy or ad-hoc event-study validations.';
COMMENT ON COLUMN market.event_signal_validation_result.effect_rule_id IS 'Single effect rule validated by this row; NULL for stacked profile or event-study validations.';
COMMENT ON COLUMN market.event_signal_validation_result.candidate_signal_scope IS 'JSONB description of event types, source rule versions, signal ids, and date range included in validation.';
COMMENT ON COLUMN market.event_signal_validation_result.experiment_id IS 'QE experiment identifier used as the fixed validation baseline.';
COMMENT ON COLUMN market.event_signal_validation_result.loop_id IS 'QE loop identifier used as the fixed validation baseline.';
COMMENT ON COLUMN market.event_signal_validation_result.loop_path IS 'Local filesystem path to the QE loop artifacts consumed by the offline simulator.';
COMMENT ON COLUMN market.event_signal_validation_result.validation_mode IS 'Validation mode: single_signal, stacked_profile, parameter_sweep, or event_study.';
COMMENT ON COLUMN market.event_signal_validation_result.simulator_version IS 'Offline event-signal simulator version used to produce overlay metrics.';
COMMENT ON COLUMN market.event_signal_validation_result.time_mode IS 'Point-in-time mode of the source signal and overlay used in this validation.';
COMMENT ON COLUMN market.event_signal_validation_result.date_from IS 'Inclusive first trade date covered by the validation.';
COMMENT ON COLUMN market.event_signal_validation_result.date_to IS 'Inclusive last trade date covered by the validation.';
COMMENT ON COLUMN market.event_signal_validation_result.policy_config_hash IS 'Hash of the exact policy profile and effect rule configuration used during validation.';
COMMENT ON COLUMN market.event_signal_validation_result.input_snapshot IS 'JSONB input manifest with QE artifact paths, hashes, source table row counts, and simulator parameters.';
COMMENT ON COLUMN market.event_signal_validation_result.baseline_metrics IS 'JSONB metrics from the unmodified QE baseline, such as final account, CAGR, and max drawdown.';
COMMENT ON COLUMN market.event_signal_validation_result.overlay_metrics IS 'JSONB metrics from the simulated event-signal overlay counterfactual.';
COMMENT ON COLUMN market.event_signal_validation_result.delta_metrics IS 'JSONB differences between overlay metrics and baseline metrics.';
COMMENT ON COLUMN market.event_signal_validation_result.hit_stats IS 'JSONB hit counts, unique symbols, original PnL distribution, and freshness diagnostics.';
COMMENT ON COLUMN market.event_signal_validation_result.acceptance_gates IS 'JSONB validation gates and pass/fail flags used to decide whether a signal may be promoted.';
COMMENT ON COLUMN market.event_signal_validation_result.decision IS 'Validation decision: PASS, REVIEW, REJECT, or DEFER.';
COMMENT ON COLUMN market.event_signal_validation_result.decision_reason IS 'Human-readable validation conclusion and promotion or rejection rationale.';
COMMENT ON COLUMN market.event_signal_validation_result.report_path IS 'Path to the human-readable validation report generated for this result.';
COMMENT ON COLUMN market.event_signal_validation_result.artifact_paths IS 'JSONB map of generated CSV, JSON, chart, and cache artifact paths.';
COMMENT ON COLUMN market.event_signal_validation_result.validated_at IS 'Timestamp when validation completed.';
COMMENT ON COLUMN market.event_signal_validation_result.created_at IS 'Database timestamp when this validation result row was first inserted.';
COMMENT ON COLUMN market.event_signal_validation_result.updated_at IS 'Database timestamp when this validation result row was last updated.';

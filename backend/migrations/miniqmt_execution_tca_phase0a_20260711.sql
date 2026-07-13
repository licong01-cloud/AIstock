-- MiniQMT adaptive-IS Phase 0A execution TCA evidence schema.
-- Controlled forward DDL only. Never execute from service startup.

CREATE SCHEMA IF NOT EXISTS qmt_strategy;

CREATE UNIQUE INDEX IF NOT EXISTS ux_tca_execution_plan_id_hash
    ON paper_v2.execution_plan(plan_id, plan_hash);
CREATE UNIQUE INDEX IF NOT EXISTS ux_tca_simulation_run_plan
    ON paper_v2.simulation_daily_run(run_id, execution_plan_id, execution_plan_hash);

ALTER TABLE qmt_strategy.trade_ledger
    ADD COLUMN IF NOT EXISTS first_ingest_source TEXT,
    ADD COLUMN IF NOT EXISTS first_ingested_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS canonical_trade_fact_sha256 TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_tca_trade_ledger_provenance'
          AND conrelid = 'qmt_strategy.trade_ledger'::regclass
    ) THEN
        ALTER TABLE qmt_strategy.trade_ledger
            ADD CONSTRAINT ck_tca_trade_ledger_provenance CHECK (
                (first_ingest_source IS NULL AND first_ingested_at IS NULL AND canonical_trade_fact_sha256 IS NULL)
                OR (first_ingest_source IN ('BROKER_CALLBACK','BROKER_SNAPSHOT_SYNC')
                    AND first_ingested_at IS NOT NULL
                    AND canonical_trade_fact_sha256 ~ '^[0-9a-f]{64}$')
            );
    END IF;
END;
$$;

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_planning_subject (
    planning_subject_id TEXT PRIMARY KEY,
    trading_rule_decision_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    execution_plan_id TEXT NOT NULL,
    execution_plan_hash TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    binding_hash TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    portfolio_id TEXT NOT NULL,
    package_id TEXT NOT NULL,
    release_id TEXT NOT NULL,
    selection_evidence_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    planning_requested_quantity BIGINT NOT NULL,
    trading_rule_legal_quantity BIGINT NOT NULL,
    decision TEXT NOT NULL,
    planning_class TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    emitted_parent_intent_id TEXT,
    trading_rule_version TEXT NOT NULL,
    evidence JSONB NOT NULL,
    evidence_sha256 TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_tca_planning_subject_plan_decision UNIQUE (execution_plan_id, trading_rule_decision_id),
    CONSTRAINT uq_tca_planning_subject_parent UNIQUE (planning_subject_id, emitted_parent_intent_id),
    CONSTRAINT fk_tca_planning_subject_run_plan FOREIGN KEY (run_id, execution_plan_id, execution_plan_hash)
        REFERENCES paper_v2.simulation_daily_run(run_id, execution_plan_id, execution_plan_hash) ON DELETE NO ACTION,
    CONSTRAINT fk_tca_planning_subject_plan FOREIGN KEY (execution_plan_id, execution_plan_hash)
        REFERENCES paper_v2.execution_plan(plan_id, plan_hash) ON DELETE NO ACTION,
    CONSTRAINT ck_tca_planning_subject_side CHECK (side IN ('BUY', 'SELL')),
    CONSTRAINT ck_tca_planning_subject_decision CHECK (decision IN ('EMIT', 'ADJUST', 'REJECT')),
    CONSTRAINT ck_tca_planning_subject_quantity CHECK (
        planning_requested_quantity >= 0 AND trading_rule_legal_quantity >= 0
        AND trading_rule_legal_quantity <= planning_requested_quantity
    ),
    CONSTRAINT ck_tca_planning_subject_parent CHECK (
        (decision = 'REJECT' AND emitted_parent_intent_id IS NULL)
        OR (decision IN ('EMIT', 'ADJUST'))
    ),
    CONSTRAINT ck_tca_planning_subject_hash CHECK (evidence_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_parent_benchmark (
    parent_intent_id TEXT NOT NULL,
    parent_revision INTEGER NOT NULL DEFAULT 1,
    supersedes_parent_revision INTEGER,
    run_id TEXT NOT NULL,
    execution_plan_id TEXT NOT NULL,
    execution_plan_hash TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    binding_hash TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    portfolio_id TEXT NOT NULL,
    package_id TEXT NOT NULL,
    release_id TEXT NOT NULL,
    selection_evidence_id TEXT NOT NULL,
    runtime_id TEXT,
    logical_tca_scope_hash TEXT NOT NULL,
    qmt_order_intent_id TEXT,
    account_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    environment TEXT NOT NULL DEFAULT 'SIM',
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    currency TEXT NOT NULL DEFAULT 'CNY',
    planning_requested_quantity BIGINT NOT NULL,
    trading_rule_legal_quantity BIGINT NOT NULL,
    emitted_parent_quantity BIGINT NOT NULL,
    managed_request_quantity_before_cash BIGINT,
    managed_request_quantity_after_cash BIGINT,
    eligible_now_quantity BIGINT,
    conditional_eligible_quantity BIGINT,
    eligible_quantity BIGINT,
    execution_ineligible_quantity BIGINT,
    planning_excluded_quantity BIGINT NOT NULL,
    decision_benchmark_type TEXT NOT NULL,
    decision_capture_fetch_started_at TIMESTAMPTZ,
    decision_event_at TIMESTAMPTZ,
    decision_market_time TIMESTAMPTZ,
    decision_received_at TIMESTAMPTZ,
    decision_persisted_at TIMESTAMPTZ,
    decision_bid_price_1 NUMERIC(20,8),
    decision_ask_price_1 NUMERIC(20,8),
    decision_mid_price NUMERIC(20,8),
    decision_quote_source TEXT,
    decision_quote_age_ms BIGINT,
    decision_transport_latency_ms BIGINT,
    decision_quality TEXT NOT NULL,
    decision_raw_quote_sha256 TEXT,
    strategy_decision_price NUMERIC(20,8),
    strategy_decision_time TIMESTAMPTZ,
    strategy_decision_source TEXT,
    strategy_decision_quality TEXT,
    arrival_time TIMESTAMPTZ,
    arrival_benchmark_type TEXT NOT NULL,
    arrival_quote_market_time TIMESTAMPTZ,
    arrival_quote_received_at TIMESTAMPTZ,
    arrival_persisted_at TIMESTAMPTZ,
    arrival_bid_price_1 NUMERIC(20,8),
    arrival_ask_price_1 NUMERIC(20,8),
    arrival_mid_price NUMERIC(20,8),
    arrival_quote_source TEXT,
    arrival_quote_offset_ms BIGINT,
    arrival_transport_latency_ms BIGINT,
    arrival_quality TEXT NOT NULL,
    arrival_raw_quote_sha256 TEXT,
    eligibility_as_of TIMESTAMPTZ,
    eligibility_class TEXT NOT NULL,
    eligibility_quality TEXT NOT NULL,
    eligibility_rule_version TEXT,
    trading_rule_decision_id TEXT NOT NULL,
    preflight_result_hash TEXT,
    dependency_parent_ids TEXT[] NOT NULL DEFAULT '{}',
    eligibility_evidence JSONB NOT NULL DEFAULT '{}',
    deadline TIMESTAMPTZ,
    calendar_version TEXT NOT NULL,
    deadline_mark_policy_version TEXT NOT NULL,
    deadline_mark_max_age_ms BIGINT NOT NULL,
    arrival_forward_window_ms BIGINT NOT NULL,
    clock_skew_tolerance_ms BIGINT NOT NULL,
    benchmark_max_transport_latency_ms BIGINT NOT NULL,
    tail_sweep_time TIMESTAMPTZ,
    continuous_cancel_cutoff TIMESTAMPTZ,
    benchmark_schema_version TEXT NOT NULL,
    benchmark_policy_version TEXT NOT NULL,
    capture_code_version TEXT NOT NULL,
    execution_policy_id TEXT NOT NULL,
    execution_policy_sha256 TEXT NOT NULL,
    runtime_config_sha256 TEXT NOT NULL,
    time_parser_version TEXT NOT NULL,
    unit_mapping_version TEXT NOT NULL,
    hard_cost_limit_bps NUMERIC(20,8),
    hard_cost_benchmark_type TEXT,
    hard_cost_benchmark_price NUMERIC(20,8),
    raw_evidence JSONB NOT NULL DEFAULT '{}',
    evidence_sha256 TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (parent_intent_id, parent_revision),
    CONSTRAINT uq_tca_parent_benchmark_plan_parent UNIQUE (execution_plan_id, parent_intent_id),
    CONSTRAINT uq_tca_parent_benchmark_identity UNIQUE (parent_intent_id, parent_revision, account_id, trade_date),
    CONSTRAINT fk_tca_parent_benchmark_run_plan FOREIGN KEY (run_id, execution_plan_id, execution_plan_hash)
        REFERENCES paper_v2.simulation_daily_run(run_id, execution_plan_id, execution_plan_hash) ON DELETE NO ACTION,
    CONSTRAINT fk_tca_parent_benchmark_plan FOREIGN KEY (execution_plan_id, execution_plan_hash)
        REFERENCES paper_v2.execution_plan(plan_id, plan_hash) ON DELETE NO ACTION,
    CONSTRAINT fk_tca_parent_benchmark_intent FOREIGN KEY (qmt_order_intent_id)
        REFERENCES qmt_strategy.order_intent(intent_id) ON DELETE NO ACTION,
    CONSTRAINT fk_tca_parent_benchmark_runtime FOREIGN KEY (runtime_id)
        REFERENCES qmt_strategy.execution_runtime(runtime_id) ON DELETE NO ACTION,
    CONSTRAINT ck_tca_parent_revision CHECK (
        parent_revision > 0
        AND ((parent_revision = 1 AND supersedes_parent_revision IS NULL)
             OR (parent_revision > 1 AND supersedes_parent_revision = parent_revision - 1))
    ),
    CONSTRAINT ck_tca_parent_identity CHECK (
        environment = 'SIM' AND side IN ('BUY', 'SELL') AND currency = 'CNY'
        AND (qmt_order_intent_id IS NULL OR qmt_order_intent_id = parent_intent_id)
    ),
    CONSTRAINT ck_tca_parent_quantity_funnel CHECK (
        planning_requested_quantity >= 0
        AND trading_rule_legal_quantity BETWEEN 0 AND planning_requested_quantity
        AND emitted_parent_quantity BETWEEN 0 AND trading_rule_legal_quantity
        AND planning_excluded_quantity = planning_requested_quantity - trading_rule_legal_quantity
        AND (managed_request_quantity_before_cash IS NULL OR managed_request_quantity_before_cash >= 0)
        AND (managed_request_quantity_after_cash IS NULL OR managed_request_quantity_after_cash BETWEEN 0 AND managed_request_quantity_before_cash)
        AND (eligible_quantity IS NULL OR eligible_quantity BETWEEN 0 AND managed_request_quantity_after_cash)
        AND (execution_ineligible_quantity IS NULL OR execution_ineligible_quantity = managed_request_quantity_after_cash - eligible_quantity)
    ),
    CONSTRAINT ck_tca_parent_quality CHECK (
        decision_quality IN ('VALID','STALE','FUTURE_SKEW','CLOCK_SKEW','ONE_SIDED','CROSSED','MISSING_TIME','MISSING','CAPTURE_FAILED','LEGACY_UNRECOVERABLE','MARKET_SESSION_ENDED')
        AND arrival_quality IN ('VALID','STALE','FUTURE_SKEW','CLOCK_SKEW','ONE_SIDED','CROSSED','MISSING_TIME','MISSING','CAPTURE_FAILED','LEGACY_UNRECOVERABLE','MARKET_SESSION_ENDED')
        AND eligibility_quality IN ('VALID','PARTIAL','MISSING','CAPTURE_FAILED','LEGACY_UNRECOVERABLE')
        AND eligibility_class IN ('ELIGIBLE_NOW','ELIGIBLE_CONDITIONAL','MIXED','INELIGIBLE_PREFLIGHT','NO_ELIGIBLE_QUANTITY','CAPTURE_FAILED','LEGACY_UNRECOVERABLE')
    ),
    CONSTRAINT ck_tca_parent_valid_eligibility CHECK (
        eligibility_quality <> 'VALID' OR (
            eligibility_as_of IS NOT NULL AND eligibility_rule_version IS NOT NULL
            AND preflight_result_hash ~ '^[0-9a-f]{64}$'
            AND managed_request_quantity_before_cash IS NOT NULL
            AND managed_request_quantity_after_cash IS NOT NULL
            AND eligible_now_quantity IS NOT NULL AND conditional_eligible_quantity IS NOT NULL
            AND eligible_quantity = eligible_now_quantity + conditional_eligible_quantity
            AND execution_ineligible_quantity = managed_request_quantity_after_cash - eligible_quantity
        )
    ),
    CONSTRAINT ck_tca_parent_valid_bbo CHECK (
        (decision_quality <> 'VALID' OR (decision_bid_price_1 > 0 AND decision_ask_price_1 >= decision_bid_price_1 AND decision_mid_price = (decision_bid_price_1 + decision_ask_price_1) / 2))
        AND (arrival_quality <> 'VALID' OR (arrival_bid_price_1 > 0 AND arrival_ask_price_1 >= arrival_bid_price_1 AND arrival_mid_price = (arrival_bid_price_1 + arrival_ask_price_1) / 2))
    ),
    CONSTRAINT ck_tca_parent_hashes CHECK (
        evidence_sha256 ~ '^[0-9a-f]{64}$' AND execution_plan_hash ~ '^[0-9a-f]{64}$'
        AND execution_policy_sha256 ~ '^[0-9a-f]{64}$' AND runtime_config_sha256 ~ '^[0-9a-f]{64}$'
    )
);

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_tca_trade_observation (
    trade_observation_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    trade_id TEXT NOT NULL,
    intent_id TEXT,
    qmt_order_id TEXT,
    child_order_id TEXT,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    ingest_source TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    broker_trade_time TIMESTAMPTZ,
    price NUMERIC(20,8) NOT NULL,
    quantity BIGINT NOT NULL,
    amount NUMERIC(30,8) NOT NULL,
    commission NUMERIC(30,8),
    fee_evidence_level TEXT NOT NULL,
    canonical_trade_fact_sha256 TEXT NOT NULL,
    timing_observation_sha256 TEXT NOT NULL,
    attribution_sha256 TEXT NOT NULL,
    fee_observation_sha256 TEXT NOT NULL,
    raw_observation_sha256 TEXT NOT NULL,
    normalized_payload JSONB NOT NULL,
    raw_payload JSONB NOT NULL,
    reconciliation_run_id TEXT,
    normalization_version TEXT NOT NULL,
    broker_time_parser_version TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (account_id, trade_date, trade_id, ingest_source, raw_observation_sha256),
    CONSTRAINT uq_tca_trade_observation_identity UNIQUE (trade_observation_id, account_id, trade_date, trade_id),
    CONSTRAINT fk_tca_trade_observation_ledger FOREIGN KEY (account_id, trade_date, trade_id)
        REFERENCES qmt_strategy.trade_ledger(account_id, trade_date, trade_id) ON DELETE NO ACTION,
    CONSTRAINT fk_tca_trade_observation_reconciliation FOREIGN KEY (reconciliation_run_id)
        REFERENCES qmt_strategy.reconciliation_run(run_id) ON DELETE NO ACTION,
    CONSTRAINT fk_tca_trade_observation_child FOREIGN KEY (child_order_id)
        REFERENCES qmt_strategy.execution_child_order(child_order_id) ON DELETE NO ACTION,
    CONSTRAINT ck_tca_trade_observation_values CHECK (
        side IN ('BUY','SELL') AND ingest_source IN ('BROKER_CALLBACK','BROKER_SNAPSHOT_SYNC')
        AND fee_evidence_level IN ('TRADE_LEVEL','ORDER_LEVEL','MISSING')
        AND price > 0 AND quantity > 0 AND amount = price * quantity AND (commission IS NULL OR commission >= 0)
    ),
    CONSTRAINT ck_tca_trade_observation_hashes CHECK (
        canonical_trade_fact_sha256 ~ '^[0-9a-f]{64}$'
        AND timing_observation_sha256 ~ '^[0-9a-f]{64}$'
        AND attribution_sha256 ~ '^[0-9a-f]{64}$'
        AND fee_observation_sha256 ~ '^[0-9a-f]{64}$'
        AND raw_observation_sha256 ~ '^[0-9a-f]{64}$'
    )
);

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_tca_trade_conflict (
    trade_conflict_fact_id TEXT PRIMARY KEY,
    conflict_series_key TEXT NOT NULL,
    conflict_generation INTEGER NOT NULL,
    supersedes_conflict_fact_id TEXT,
    account_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    trade_id TEXT NOT NULL,
    conflict_type TEXT NOT NULL,
    conflict_status TEXT NOT NULL,
    existing_observation_id TEXT,
    incoming_observation_id TEXT NOT NULL,
    existing_ingest_source TEXT NOT NULL,
    incoming_ingest_source TEXT NOT NULL,
    existing_canonical_sha256 TEXT NOT NULL,
    incoming_canonical_sha256 TEXT NOT NULL,
    existing_timing_sha256 TEXT NOT NULL,
    incoming_timing_sha256 TEXT NOT NULL,
    existing_ledger_evidence_sha256 TEXT,
    resolution_authority TEXT,
    resolution_reason TEXT,
    resolution_evidence_sha256 TEXT,
    detected_at TIMESTAMPTZ NOT NULL,
    resolved_at TIMESTAMPTZ,
    fact_sha256 TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (conflict_series_key, conflict_generation),
    CONSTRAINT fk_tca_trade_conflict_previous FOREIGN KEY (supersedes_conflict_fact_id)
        REFERENCES qmt_strategy.execution_tca_trade_conflict(trade_conflict_fact_id) ON DELETE NO ACTION,
    CONSTRAINT fk_tca_trade_conflict_existing FOREIGN KEY (existing_observation_id)
        REFERENCES qmt_strategy.execution_tca_trade_observation(trade_observation_id) ON DELETE NO ACTION,
    CONSTRAINT fk_tca_trade_conflict_incoming FOREIGN KEY (incoming_observation_id, account_id, trade_date, trade_id)
        REFERENCES qmt_strategy.execution_tca_trade_observation(trade_observation_id, account_id, trade_date, trade_id) ON DELETE NO ACTION,
    CONSTRAINT ck_tca_trade_conflict_identity CHECK (
        conflict_generation > 0 AND conflict_type IN ('CORE_FACT','AUTHORITATIVE_TIME')
        AND conflict_status IN ('OPEN','RESOLVED')
        AND existing_ingest_source IN ('BROKER_CALLBACK','BROKER_SNAPSHOT_SYNC','LEGACY_LEDGER_BASELINE')
        AND incoming_ingest_source IN ('BROKER_CALLBACK','BROKER_SNAPSHOT_SYNC')
        AND (supersedes_conflict_fact_id IS NULL OR supersedes_conflict_fact_id <> trade_conflict_fact_id)
    ),
    CONSTRAINT ck_tca_trade_conflict_state CHECK (
        (conflict_status = 'OPEN' AND resolution_authority IS NULL AND resolution_reason IS NULL AND resolution_evidence_sha256 IS NULL AND resolved_at IS NULL)
        OR (conflict_status = 'RESOLVED' AND supersedes_conflict_fact_id IS NOT NULL AND resolution_authority IS NOT NULL AND resolution_reason IS NOT NULL AND resolution_evidence_sha256 IS NOT NULL AND resolved_at IS NOT NULL)
    ),
    CONSTRAINT ck_tca_trade_conflict_legacy CHECK (
        (existing_observation_id IS NULL AND existing_ingest_source = 'LEGACY_LEDGER_BASELINE' AND existing_ledger_evidence_sha256 IS NOT NULL)
        OR (existing_observation_id IS NOT NULL AND existing_ingest_source <> 'LEGACY_LEDGER_BASELINE' AND existing_ledger_evidence_sha256 IS NULL)
    ),
    CONSTRAINT ck_tca_trade_conflict_hashes CHECK (
        existing_canonical_sha256 ~ '^[0-9a-f]{64}$' AND incoming_canonical_sha256 ~ '^[0-9a-f]{64}$'
        AND existing_timing_sha256 ~ '^[0-9a-f]{64}$' AND incoming_timing_sha256 ~ '^[0-9a-f]{64}$'
        AND fact_sha256 ~ '^[0-9a-f]{64}$'
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_tca_trade_conflict_successor
    ON qmt_strategy.execution_tca_trade_conflict(supersedes_conflict_fact_id)
    WHERE supersedes_conflict_fact_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_tca_mark (
    mark_id TEXT PRIMARY KEY,
    mark_series_key TEXT NOT NULL,
    mark_revision INTEGER NOT NULL,
    supersedes_mark_id TEXT,
    parent_intent_id TEXT NOT NULL,
    parent_revision INTEGER NOT NULL,
    mark_scope_key TEXT NOT NULL,
    mark_type TEXT NOT NULL,
    trade_account_id TEXT,
    trade_date DATE,
    trade_id TEXT,
    child_order_id TEXT,
    horizon_ms BIGINT,
    target_time TIMESTAMPTZ NOT NULL,
    source_snapshot_started_at TIMESTAMPTZ NOT NULL,
    source_snapshot_completed_at TIMESTAMPTZ NOT NULL,
    market_time TIMESTAMPTZ,
    received_at TIMESTAMPTZ,
    persisted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    bid_price_1 NUMERIC(20,8),
    ask_price_1 NUMERIC(20,8),
    mid_price NUMERIC(20,8),
    last_price NUMERIC(20,8),
    quote_source TEXT,
    age_or_lag_ms BIGINT,
    quality TEXT NOT NULL,
    market_phase TEXT,
    stock_status TEXT,
    raw_quote_sha256 TEXT,
    market_data_id TEXT,
    mark_policy_version TEXT NOT NULL,
    source_input_sha256 TEXT NOT NULL,
    evidence_sha256 TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (mark_series_key, mark_revision),
    UNIQUE (mark_series_key, mark_policy_version, source_input_sha256),
    CONSTRAINT uq_tca_mark_parent_role UNIQUE (mark_id, parent_intent_id, parent_revision, mark_type),
    CONSTRAINT uq_tca_mark_parent UNIQUE (mark_id, parent_intent_id, parent_revision, mark_type, horizon_ms),
    CONSTRAINT fk_tca_mark_previous FOREIGN KEY (supersedes_mark_id)
        REFERENCES qmt_strategy.execution_tca_mark(mark_id) ON DELETE NO ACTION,
    CONSTRAINT fk_tca_mark_parent FOREIGN KEY (parent_intent_id, parent_revision)
        REFERENCES qmt_strategy.execution_parent_benchmark(parent_intent_id, parent_revision) ON DELETE NO ACTION,
    CONSTRAINT fk_tca_mark_trade FOREIGN KEY (trade_account_id, trade_date, trade_id)
        REFERENCES qmt_strategy.trade_ledger(account_id, trade_date, trade_id) ON DELETE NO ACTION,
    CONSTRAINT fk_tca_mark_child FOREIGN KEY (child_order_id)
        REFERENCES qmt_strategy.execution_child_order(child_order_id) ON DELETE NO ACTION,
    CONSTRAINT ck_tca_mark_identity CHECK (
        mark_revision > 0 AND mark_type IN ('DEADLINE','CHILD_RECEIPT','FILL_MARKOUT_60S','FILL_MARKOUT_300S','FILL_MARKOUT_900S')
        AND quality IN ('VALID','STALE','FUTURE_SKEW','CLOCK_SKEW','ONE_SIDED','CROSSED','MISSING_TIME','MISSING','CAPTURE_FAILED','LEGACY_UNRECOVERABLE','MARKET_SESSION_ENDED')
        AND (supersedes_mark_id IS NULL OR supersedes_mark_id <> mark_id)
    ),
    CONSTRAINT ck_tca_mark_trade_key CHECK (
        (trade_account_id IS NULL AND trade_date IS NULL AND trade_id IS NULL)
        OR (trade_account_id IS NOT NULL AND trade_date IS NOT NULL AND trade_id IS NOT NULL)
    ),
    CONSTRAINT ck_tca_mark_horizon CHECK (
        (mark_type = 'FILL_MARKOUT_60S' AND horizon_ms = 60000)
        OR (mark_type = 'FILL_MARKOUT_300S' AND horizon_ms = 300000)
        OR (mark_type = 'FILL_MARKOUT_900S' AND horizon_ms = 900000)
        OR (mark_type IN ('DEADLINE','CHILD_RECEIPT') AND horizon_ms IS NULL)
    ),
    CONSTRAINT ck_tca_mark_hashes CHECK (source_input_sha256 ~ '^[0-9a-f]{64}$' AND evidence_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_tca_mark_successor
    ON qmt_strategy.execution_tca_mark(supersedes_mark_id) WHERE supersedes_mark_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_tca_rebuild_receipt (
    receipt_id TEXT PRIMARY KEY,
    receipt_scope_hash TEXT NOT NULL,
    receipt_generation INTEGER NOT NULL,
    supersedes_receipt_id TEXT,
    receipt_status TEXT NOT NULL,
    snapshot_kind TEXT NOT NULL,
    environment TEXT NOT NULL DEFAULT 'SIM',
    binding_ids TEXT[] NOT NULL,
    account_pseudonyms TEXT[] NOT NULL,
    account_pseudonym_key_version TEXT NOT NULL,
    trade_date_from DATE NOT NULL,
    trade_date_to DATE NOT NULL,
    selection_predicates JSONB NOT NULL,
    db_snapshot_identity JSONB NOT NULL,
    source_snapshot_started_at TIMESTAMPTZ,
    source_snapshot_completed_at TIMESTAMPTZ,
    source_snapshot_complete BOOLEAN NOT NULL,
    source_watermarks JSONB NOT NULL,
    source_row_counts JSONB NOT NULL,
    source_content_hashes JSONB NOT NULL,
    calculator_version TEXT NOT NULL,
    formula_version TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    query_version TEXT NOT NULL,
    benchmark_policy_version TEXT NOT NULL,
    mark_policy_version TEXT NOT NULL,
    fee_policy_version TEXT NOT NULL,
    trade_provenance_policy_version TEXT NOT NULL,
    code_commit TEXT NOT NULL,
    canonical_query_sha256 TEXT NOT NULL,
    parent_count BIGINT,
    planning_subject_count BIGINT,
    planning_excluded_count BIGINT,
    order_event_count BIGINT,
    trade_count BIGINT,
    trade_observation_count BIGINT,
    trade_conflict_count BIGINT,
    mark_count BIGINT,
    eligible_quantity BIGINT,
    deadline_filled_quantity BIGINT,
    terminal_filled_quantity BIGINT,
    eligible_notional_cny NUMERIC(30,8),
    deadline_filled_notional_cny NUMERIC(30,8),
    terminal_filled_notional_cny NUMERIC(30,8),
    coverage JSONB NOT NULL,
    orphan_counts JSONB NOT NULL,
    duplicate_counts JSONB NOT NULL,
    conflict_counts JSONB NOT NULL,
    invalid_counts JSONB NOT NULL,
    invariant_results JSONB NOT NULL,
    numeric_tolerances JSONB NOT NULL,
    canonical_input_sha256 TEXT,
    canonical_output_sha256 TEXT NOT NULL,
    failure_attempt_sha256 TEXT,
    final_parent_count BIGINT,
    provisional_parent_count BIGINT,
    invalid_parent_count BIGINT,
    failure_reason_code TEXT,
    failure_stage TEXT,
    failure_class TEXT,
    failure_context JSONB NOT NULL DEFAULT '{}',
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    operator_pseudonym TEXT NOT NULL,
    source_snapshot_read_only BOOLEAN NOT NULL DEFAULT TRUE,
    broker_side_effect BOOLEAN NOT NULL DEFAULT FALSE,
    source_mutation BOOLEAN NOT NULL DEFAULT FALSE,
    evidence_write_performed BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (receipt_scope_hash, receipt_generation),
    UNIQUE (receipt_id, receipt_status),
    CONSTRAINT fk_tca_receipt_previous FOREIGN KEY (supersedes_receipt_id)
        REFERENCES qmt_strategy.execution_tca_rebuild_receipt(receipt_id) ON DELETE NO ACTION,
    CONSTRAINT ck_tca_receipt_identity CHECK (
        receipt_generation > 0 AND receipt_status IN ('COMPLETED','FAILED')
        AND snapshot_kind IN ('DEADLINE','RECONCILED_FINAL') AND environment = 'SIM'
        AND trade_date_from <= trade_date_to AND completed_at >= started_at
        AND source_snapshot_read_only AND NOT broker_side_effect AND NOT source_mutation AND evidence_write_performed
        AND (supersedes_receipt_id IS NULL OR supersedes_receipt_id <> receipt_id)
    ),
    CONSTRAINT ck_tca_receipt_state CHECK (
        (receipt_status = 'COMPLETED' AND source_snapshot_complete
            AND source_snapshot_started_at IS NOT NULL AND source_snapshot_completed_at IS NOT NULL
            AND canonical_input_sha256 IS NOT NULL AND parent_count IS NOT NULL AND planning_subject_count IS NOT NULL
            AND failure_attempt_sha256 IS NULL AND failure_reason_code IS NULL AND failure_stage IS NULL AND failure_class IS NULL)
        OR (receipt_status = 'FAILED' AND failure_attempt_sha256 IS NOT NULL
            AND failure_reason_code IS NOT NULL AND failure_stage IS NOT NULL
            AND failure_class IN ('DOMAIN','OPERATIONAL'))
    ),
    CONSTRAINT ck_tca_receipt_hashes CHECK (
        receipt_scope_hash ~ '^[0-9a-f]{64}$' AND canonical_query_sha256 ~ '^[0-9a-f]{64}$'
        AND canonical_output_sha256 ~ '^[0-9a-f]{64}$'
        AND (canonical_input_sha256 IS NULL OR canonical_input_sha256 ~ '^[0-9a-f]{64}$')
        AND (failure_attempt_sha256 IS NULL OR failure_attempt_sha256 ~ '^[0-9a-f]{64}$')
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_tca_receipt_successor
    ON qmt_strategy.execution_tca_rebuild_receipt(supersedes_receipt_id) WHERE supersedes_receipt_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS ux_tca_receipt_completed_input
    ON qmt_strategy.execution_tca_rebuild_receipt(
        receipt_scope_hash, snapshot_kind, calculator_version, formula_version, schema_version,
        query_version, benchmark_policy_version, mark_policy_version, fee_policy_version,
        trade_provenance_policy_version, canonical_input_sha256
    ) WHERE receipt_status = 'COMPLETED';

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_parent_tca (
    tca_result_id TEXT PRIMARY KEY,
    result_series_key TEXT NOT NULL,
    result_generation INTEGER NOT NULL,
    supersedes_tca_result_id TEXT,
    parent_intent_id TEXT NOT NULL,
    parent_revision INTEGER NOT NULL,
    snapshot_kind TEXT NOT NULL,
    result_status TEXT NOT NULL,
    as_of_time TIMESTAMPTZ NOT NULL,
    source_snapshot_started_at TIMESTAMPTZ NOT NULL,
    source_snapshot_completed_at TIMESTAMPTZ NOT NULL,
    deadline TIMESTAMPTZ,
    terminal_as_of TIMESTAMPTZ,
    reconciliation_run_id TEXT,
    eligible_quantity BIGINT,
    deadline_filled_quantity BIGINT,
    terminal_filled_quantity BIGINT,
    post_deadline_filled_quantity BIGINT,
    deadline_residual_quantity BIGINT,
    terminal_residual_quantity BIGINT,
    deadline_fill_count BIGINT,
    deadline_fill_notional_cny NUMERIC(30,8),
    deadline_fill_vwap NUMERIC(20,8),
    terminal_fill_count BIGINT,
    terminal_fill_notional_cny NUMERIC(30,8),
    terminal_fill_vwap NUMERIC(20,8),
    delay_cost_cny NUMERIC(30,8),
    execution_cost_cny NUMERIC(30,8),
    opportunity_cost_cny NUMERIC(30,8),
    decision_calculation_mode TEXT,
    decision_is_direct_check_gross_cny NUMERIC(30,8),
    decision_is_gross_cny NUMERIC(30,8),
    decision_is_net_actual_cny NUMERIC(30,8),
    decision_is_net_estimated_cny NUMERIC(30,8),
    decision_is_gross_bps NUMERIC(20,8),
    decision_is_net_actual_bps NUMERIC(20,8),
    decision_is_net_estimated_bps NUMERIC(20,8),
    arrival_is_gross_cny NUMERIC(30,8),
    arrival_is_net_actual_cny NUMERIC(30,8),
    arrival_is_net_estimated_cny NUMERIC(30,8),
    arrival_is_gross_bps NUMERIC(20,8),
    arrival_is_net_actual_bps NUMERIC(20,8),
    arrival_is_net_estimated_bps NUMERIC(20,8),
    deadline_fee_actual_cny NUMERIC(30,8),
    deadline_fee_estimated_cny NUMERIC(30,8),
    post_deadline_fee_actual_cny NUMERIC(30,8),
    post_deadline_fee_estimated_cny NUMERIC(30,8),
    deadline_fee_quality TEXT NOT NULL,
    post_deadline_fee_quality TEXT NOT NULL,
    fee_breakdown JSONB NOT NULL,
    fee_schedule_version TEXT,
    account_fee_profile_version TEXT,
    fee_allocation_version TEXT NOT NULL,
    completion_by_deadline_quantity NUMERIC(20,12),
    terminal_completion_quantity NUMERIC(20,12),
    completion_by_deadline_notional NUMERIC(20,12),
    effective_spread_bps NUMERIC(20,8),
    effective_spread_partial_bps NUMERIC(20,8),
    effective_spread_coverage_notional_ratio NUMERIC(20,12),
    cost_markout_60s_bps NUMERIC(20,8),
    cost_markout_300s_bps NUMERIC(20,8),
    cost_markout_900s_bps NUMERIC(20,8),
    markout_partial_metrics JSONB NOT NULL,
    markout_coverage JSONB NOT NULL,
    post_deadline_execution_cost_cny NUMERIC(30,8),
    residual_reason TEXT NOT NULL,
    residual_executability_class TEXT NOT NULL,
    metric_validity JSONB NOT NULL,
    join_coverage JSONB NOT NULL,
    benchmark_coverage JSONB NOT NULL,
    mark_coverage JSONB NOT NULL,
    fee_coverage JSONB NOT NULL,
    finality_evidence JSONB NOT NULL,
    invariant_results JSONB NOT NULL,
    formula_version TEXT NOT NULL,
    calculator_version TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    query_version TEXT NOT NULL,
    benchmark_policy_version TEXT NOT NULL,
    mark_policy_version TEXT NOT NULL,
    fee_policy_version TEXT NOT NULL,
    trade_provenance_policy_version TEXT NOT NULL,
    canonical_input_sha256 TEXT NOT NULL,
    canonical_output_sha256 TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (result_series_key, result_generation),
    UNIQUE (result_series_key, calculator_version, formula_version, schema_version, query_version,
            benchmark_policy_version, mark_policy_version, fee_policy_version,
            trade_provenance_policy_version, canonical_input_sha256),
    CONSTRAINT uq_tca_parent_result_parent UNIQUE (tca_result_id, parent_intent_id, parent_revision),
    CONSTRAINT uq_tca_parent_result_identity UNIQUE (tca_result_id, parent_intent_id, parent_revision, snapshot_kind),
    CONSTRAINT fk_tca_parent_result_previous FOREIGN KEY (supersedes_tca_result_id)
        REFERENCES qmt_strategy.execution_parent_tca(tca_result_id) ON DELETE NO ACTION,
    CONSTRAINT fk_tca_parent_result_parent FOREIGN KEY (parent_intent_id, parent_revision)
        REFERENCES qmt_strategy.execution_parent_benchmark(parent_intent_id, parent_revision) ON DELETE NO ACTION,
    CONSTRAINT fk_tca_parent_result_reconciliation FOREIGN KEY (reconciliation_run_id)
        REFERENCES qmt_strategy.reconciliation_run(run_id) ON DELETE NO ACTION,
    CONSTRAINT ck_tca_parent_result_identity CHECK (
        result_generation > 0 AND snapshot_kind IN ('DEADLINE','RECONCILED_FINAL')
        AND result_status IN ('PROVISIONAL','FINAL','INVALID')
        AND (decision_calculation_mode IS NULL OR decision_calculation_mode IN ('DECOMPOSED','DIRECT'))
        AND deadline_fee_quality IN ('ACTUAL_COMPLETE','ACTUAL_PARTIAL','ESTIMATED','PROVISIONAL_ORDER_FEE_ALLOCATION','MISSING','UNKNOWN_LEGACY')
        AND post_deadline_fee_quality IN ('ACTUAL_COMPLETE','ACTUAL_PARTIAL','ESTIMATED','PROVISIONAL_ORDER_FEE_ALLOCATION','MISSING','UNKNOWN_LEGACY')
        AND residual_executability_class IN ('COMPLETED','POLICY_BLOCKED','MARKET_EXTERNAL_BLOCKED','BROKER_REJECTED','DEPENDENCY_UNSATISFIED','BATCH_ABORTED_BY_PEER','UNKNOWN','INVALID')
        AND (supersedes_tca_result_id IS NULL OR supersedes_tca_result_id <> tca_result_id)
    ),
    CONSTRAINT ck_tca_parent_result_finality CHECK (
        (result_status <> 'FINAL') OR (snapshot_kind = 'RECONCILED_FINAL' AND terminal_as_of IS NOT NULL AND reconciliation_run_id IS NOT NULL)
    ),
    CONSTRAINT ck_tca_parent_result_quantities CHECK (
        eligible_quantity IS NULL OR (
            eligible_quantity >= 0 AND deadline_filled_quantity BETWEEN 0 AND eligible_quantity
            AND terminal_filled_quantity BETWEEN deadline_filled_quantity AND eligible_quantity
            AND post_deadline_filled_quantity = terminal_filled_quantity - deadline_filled_quantity
            AND deadline_residual_quantity = eligible_quantity - deadline_filled_quantity
            AND terminal_residual_quantity = eligible_quantity - terminal_filled_quantity
        )
    ),
    CONSTRAINT ck_tca_parent_result_hashes CHECK (
        result_series_key ~ '^[0-9a-f]{64}$' AND canonical_input_sha256 ~ '^[0-9a-f]{64}$'
        AND canonical_output_sha256 ~ '^[0-9a-f]{64}$'
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_tca_parent_result_successor
    ON qmt_strategy.execution_parent_tca(supersedes_tca_result_id) WHERE supersedes_tca_result_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_tca_receipt_planning_subject (
    receipt_id TEXT NOT NULL,
    receipt_status TEXT NOT NULL,
    planning_subject_id TEXT NOT NULL,
    classification TEXT NOT NULL,
    membership_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (receipt_id, planning_subject_id),
    CONSTRAINT fk_tca_receipt_subject_receipt FOREIGN KEY (receipt_id, receipt_status)
        REFERENCES qmt_strategy.execution_tca_rebuild_receipt(receipt_id, receipt_status) ON DELETE NO ACTION,
    CONSTRAINT fk_tca_receipt_subject_subject FOREIGN KEY (planning_subject_id)
        REFERENCES qmt_strategy.execution_planning_subject(planning_subject_id) ON DELETE NO ACTION,
    CONSTRAINT ck_tca_receipt_subject CHECK (
        receipt_status = 'COMPLETED'
        AND classification IN ('EMITTED_PARENT','PLANNING_RULE_EXCLUDED','INVALID_SOURCE')
        AND membership_hash ~ '^[0-9a-f]{64}$'
    )
);

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_tca_receipt_result (
    receipt_id TEXT NOT NULL,
    receipt_status TEXT NOT NULL,
    tca_result_id TEXT NOT NULL,
    parent_intent_id TEXT NOT NULL,
    parent_revision INTEGER NOT NULL,
    snapshot_kind TEXT NOT NULL,
    membership_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (receipt_id, tca_result_id),
    UNIQUE (receipt_id, parent_intent_id, parent_revision, snapshot_kind),
    CONSTRAINT fk_tca_receipt_result_receipt FOREIGN KEY (receipt_id, receipt_status)
        REFERENCES qmt_strategy.execution_tca_rebuild_receipt(receipt_id, receipt_status) ON DELETE NO ACTION,
    CONSTRAINT fk_tca_receipt_result_result FOREIGN KEY (tca_result_id, parent_intent_id, parent_revision, snapshot_kind)
        REFERENCES qmt_strategy.execution_parent_tca(tca_result_id, parent_intent_id, parent_revision, snapshot_kind) ON DELETE NO ACTION,
    CONSTRAINT ck_tca_receipt_result CHECK (receipt_status = 'COMPLETED' AND membership_hash ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_tca_result_mark (
    tca_result_id TEXT NOT NULL,
    mark_id TEXT NOT NULL,
    parent_intent_id TEXT NOT NULL,
    parent_revision INTEGER NOT NULL,
    mark_role TEXT NOT NULL,
    membership_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tca_result_id, mark_id, mark_role),
    CONSTRAINT fk_tca_result_mark_result FOREIGN KEY (tca_result_id, parent_intent_id, parent_revision)
        REFERENCES qmt_strategy.execution_parent_tca(tca_result_id, parent_intent_id, parent_revision) ON DELETE NO ACTION,
    CONSTRAINT fk_tca_result_mark_mark FOREIGN KEY (mark_id, parent_intent_id, parent_revision, mark_role)
        REFERENCES qmt_strategy.execution_tca_mark(mark_id, parent_intent_id, parent_revision, mark_type) ON DELETE NO ACTION,
    CONSTRAINT ck_tca_result_mark CHECK (
        mark_role IN ('DEADLINE','CHILD_RECEIPT','FILL_MARKOUT_60S','FILL_MARKOUT_300S','FILL_MARKOUT_900S')
        AND membership_hash ~ '^[0-9a-f]{64}$'
    )
);

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_tca_result_trade_observation (
    tca_result_id TEXT NOT NULL,
    trade_observation_id TEXT NOT NULL,
    parent_intent_id TEXT NOT NULL,
    parent_revision INTEGER NOT NULL,
    trade_account_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    trade_id TEXT NOT NULL,
    observation_role TEXT NOT NULL,
    selected_content_sha256 TEXT NOT NULL,
    membership_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tca_result_id, trade_observation_id, observation_role),
    UNIQUE (tca_result_id, trade_account_id, trade_date, trade_id, observation_role),
    CONSTRAINT fk_tca_result_observation_result FOREIGN KEY (tca_result_id, parent_intent_id, parent_revision)
        REFERENCES qmt_strategy.execution_parent_tca(tca_result_id, parent_intent_id, parent_revision) ON DELETE NO ACTION,
    CONSTRAINT fk_tca_result_observation_observation FOREIGN KEY (trade_observation_id, trade_account_id, trade_date, trade_id)
        REFERENCES qmt_strategy.execution_tca_trade_observation(trade_observation_id, account_id, trade_date, trade_id) ON DELETE NO ACTION,
    CONSTRAINT ck_tca_result_observation CHECK (
        observation_role IN ('CORE','TIMING','FEE','ATTRIBUTION')
        AND selected_content_sha256 ~ '^[0-9a-f]{64}$' AND membership_hash ~ '^[0-9a-f]{64}$'
    )
);

CREATE INDEX IF NOT EXISTS ix_tca_algo_parent_runtime
    ON qmt_strategy.execution_algo_instance(parent_intent_id, runtime_id, algo_instance_id);
CREATE INDEX IF NOT EXISTS ix_tca_child_parent_algo
    ON qmt_strategy.execution_child_order(parent_intent_id, algo_instance_id, child_order_id);
CREATE INDEX IF NOT EXISTS ix_tca_order_ledger_intent_sync
    ON qmt_strategy.order_ledger(intent_id, last_synced_at) WHERE intent_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_tca_order_event_intent_time
    ON qmt_strategy.order_status_event(intent_id, event_time, event_id) WHERE intent_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_tca_trade_ledger_intent_time
    ON qmt_strategy.trade_ledger(intent_id, trade_time, trade_id);
CREATE INDEX IF NOT EXISTS ix_tca_planning_subject_binding_date
    ON qmt_strategy.execution_planning_subject(binding_id, trade_date, planning_subject_id);
CREATE INDEX IF NOT EXISTS ix_tca_parent_binding_date
    ON qmt_strategy.execution_parent_benchmark(binding_id, trade_date, parent_intent_id);
CREATE INDEX IF NOT EXISTS ix_tca_trade_observation_key_time
    ON qmt_strategy.execution_tca_trade_observation(account_id, trade_date, trade_id, observed_at);
CREATE INDEX IF NOT EXISTS ix_tca_trade_observation_canonical
    ON qmt_strategy.execution_tca_trade_observation(canonical_trade_fact_sha256);
CREATE INDEX IF NOT EXISTS ix_tca_trade_conflict_open
    ON qmt_strategy.execution_tca_trade_conflict(account_id, trade_date, trade_id, conflict_series_key, conflict_generation DESC)
    WHERE conflict_status = 'OPEN';
CREATE INDEX IF NOT EXISTS ix_tca_result_parent_read
    ON qmt_strategy.execution_parent_tca(parent_intent_id, parent_revision, snapshot_kind, result_series_key, result_generation DESC);
CREATE INDEX IF NOT EXISTS ix_tca_result_asof
    ON qmt_strategy.execution_parent_tca(result_series_key, source_snapshot_started_at, result_generation DESC);
CREATE INDEX IF NOT EXISTS ix_tca_receipt_asof
    ON qmt_strategy.execution_tca_rebuild_receipt(receipt_status, source_snapshot_started_at, receipt_id);
CREATE INDEX IF NOT EXISTS ix_tca_receipt_result_reverse
    ON qmt_strategy.execution_tca_receipt_result(tca_result_id, receipt_id);
CREATE INDEX IF NOT EXISTS ix_tca_result_mark_reverse
    ON qmt_strategy.execution_tca_result_mark(mark_id, tca_result_id);
CREATE INDEX IF NOT EXISTS ix_tca_result_observation_reverse
    ON qmt_strategy.execution_tca_result_trade_observation(trade_observation_id, tca_result_id);
CREATE INDEX IF NOT EXISTS ix_tca_reconciliation_scope
    ON qmt_strategy.reconciliation_run(account_id, trade_date, completed_at, status);
CREATE INDEX IF NOT EXISTS ix_tca_reconciliation_issue_run
    ON qmt_strategy.reconciliation_issue(run_id);

CREATE OR REPLACE FUNCTION qmt_strategy.reject_execution_tca_mutation()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'immutable execution TCA evidence rejects % on %.%', TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME
        USING ERRCODE = '55000';
END;
$$;

DO $$
DECLARE
    table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'execution_planning_subject','execution_parent_benchmark','execution_tca_trade_observation',
        'execution_tca_trade_conflict','execution_tca_mark','execution_tca_rebuild_receipt',
        'execution_parent_tca','execution_tca_receipt_planning_subject','execution_tca_receipt_result',
        'execution_tca_result_mark','execution_tca_result_trade_observation'
    ] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS reject_tca_mutation ON qmt_strategy.%I', table_name);
        EXECUTE format(
            'CREATE TRIGGER reject_tca_mutation BEFORE UPDATE OR DELETE ON qmt_strategy.%I '
            'FOR EACH ROW EXECUTE FUNCTION qmt_strategy.reject_execution_tca_mutation()', table_name
        );
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION qmt_strategy.validate_tca_result_observation_role()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    observation_row qmt_strategy.execution_tca_trade_observation%ROWTYPE;
    expected_hash TEXT;
BEGIN
    SELECT * INTO STRICT observation_row
    FROM qmt_strategy.execution_tca_trade_observation
    WHERE trade_observation_id = NEW.trade_observation_id;
    expected_hash := CASE NEW.observation_role
        WHEN 'CORE' THEN observation_row.canonical_trade_fact_sha256
        WHEN 'TIMING' THEN observation_row.timing_observation_sha256
        WHEN 'FEE' THEN observation_row.fee_observation_sha256
        WHEN 'ATTRIBUTION' THEN observation_row.attribution_sha256
    END;
    IF expected_hash IS DISTINCT FROM NEW.selected_content_sha256 THEN
        RAISE EXCEPTION 'TCA observation role hash mismatch' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS validate_tca_result_observation_role
    ON qmt_strategy.execution_tca_result_trade_observation;
CREATE CONSTRAINT TRIGGER validate_tca_result_observation_role
AFTER INSERT ON qmt_strategy.execution_tca_result_trade_observation
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION qmt_strategy.validate_tca_result_observation_role();

CREATE OR REPLACE FUNCTION qmt_strategy.validate_tca_subject_parent()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE
    subject_row qmt_strategy.execution_planning_subject%ROWTYPE;
    benchmark_row qmt_strategy.execution_parent_benchmark%ROWTYPE;
BEGIN
    SELECT * INTO STRICT subject_row FROM qmt_strategy.execution_planning_subject
    WHERE planning_subject_id = NEW.planning_subject_id;
    IF NEW.classification = 'EMITTED_PARENT' THEN
        IF subject_row.emitted_parent_intent_id IS NULL THEN
            RAISE EXCEPTION 'EMITTED_PARENT membership lacks emitted parent' USING ERRCODE = '23514';
        END IF;
        SELECT * INTO STRICT benchmark_row FROM qmt_strategy.execution_parent_benchmark
        WHERE parent_intent_id = subject_row.emitted_parent_intent_id AND parent_revision = 1;
        IF benchmark_row.execution_plan_id <> subject_row.execution_plan_id
           OR benchmark_row.symbol <> subject_row.symbol OR benchmark_row.side <> subject_row.side THEN
            RAISE EXCEPTION 'planning subject and parent benchmark lineage mismatch' USING ERRCODE = '23514';
        END IF;
    ELSIF NEW.classification = 'PLANNING_RULE_EXCLUDED' AND subject_row.decision <> 'REJECT' THEN
        RAISE EXCEPTION 'planning exclusion membership requires REJECT decision' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS validate_tca_subject_parent
    ON qmt_strategy.execution_tca_receipt_planning_subject;
CREATE CONSTRAINT TRIGGER validate_tca_subject_parent
AFTER INSERT ON qmt_strategy.execution_tca_receipt_planning_subject
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION qmt_strategy.validate_tca_subject_parent();

COMMENT ON TABLE qmt_strategy.execution_planning_subject IS 'Immutable projection of every execution-plan trading-rule decision, including rejected decisions.';
COMMENT ON TABLE qmt_strategy.execution_parent_benchmark IS 'Immutable per-parent decision, arrival, eligibility, deadline, and lineage benchmark evidence.';
COMMENT ON TABLE qmt_strategy.execution_tca_trade_observation IS 'Append-only broker callback or snapshot transport observation for a canonical trade fact.';
COMMENT ON TABLE qmt_strategy.execution_tca_trade_conflict IS 'Append-only loud conflict fact for canonical trade or authoritative trade-time disagreement.';
COMMENT ON TABLE qmt_strategy.execution_tca_mark IS 'Append-only selected deadline, child receipt, or fill markout quote evidence.';
COMMENT ON TABLE qmt_strategy.execution_tca_rebuild_receipt IS 'Immutable deterministic rebuild receipt with source watermarks, coverage, versions, and failure evidence.';
COMMENT ON TABLE qmt_strategy.execution_parent_tca IS 'Immutable per-parent TCA result fact; calculator population begins in Phase 0A-3.';
COMMENT ON TABLE qmt_strategy.execution_tca_receipt_planning_subject IS 'Exact per-receipt classification membership for every scoped planning subject.';
COMMENT ON TABLE qmt_strategy.execution_tca_receipt_result IS 'Exact per-receipt membership of scope-independent parent TCA results.';
COMMENT ON TABLE qmt_strategy.execution_tca_result_mark IS 'Exact role-qualified mark evidence selected by one parent TCA result.';
COMMENT ON TABLE qmt_strategy.execution_tca_result_trade_observation IS 'Exact role-qualified trade observation evidence selected by one parent TCA result.';
COMMENT ON COLUMN qmt_strategy.trade_ledger.first_ingest_source IS 'Prospective first broker ingest source; NULL means UNKNOWN_LEGACY and is never guessed.';
COMMENT ON COLUMN qmt_strategy.trade_ledger.first_ingested_at IS 'UTC time the prospective canonical trade fact first entered the local ledger; NULL for legacy rows.';
COMMENT ON COLUMN qmt_strategy.trade_ledger.canonical_trade_fact_sha256 IS 'Versioned canonical economic fact hash; NULL for unrecoverable legacy provenance.';

DO $$
DECLARE
    column_row RECORD;
BEGIN
    FOR column_row IN
        SELECT table_schema, table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'qmt_strategy'
          AND table_name IN (
              'execution_planning_subject','execution_parent_benchmark','execution_tca_trade_observation',
              'execution_tca_trade_conflict','execution_tca_mark','execution_tca_rebuild_receipt',
              'execution_parent_tca','execution_tca_receipt_planning_subject','execution_tca_receipt_result',
              'execution_tca_result_mark','execution_tca_result_trade_observation'
          )
    LOOP
        EXECUTE format(
            'COMMENT ON COLUMN %I.%I.%I IS %L',
            column_row.table_schema,
            column_row.table_name,
            column_row.column_name,
            'Phase 0A immutable execution TCA field: ' || column_row.column_name || '.'
        );
    END LOOP;
END;
$$;

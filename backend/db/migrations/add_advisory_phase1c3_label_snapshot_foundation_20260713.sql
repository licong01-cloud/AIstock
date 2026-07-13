-- Advisory Phase 1C-3 Batch C PostgreSQL foundation.
-- Apply only through an explicit DEV/release migration workflow.  Runtime
-- services must never execute this migration or create partitions.

BEGIN;

ALTER TABLE app.advisory_capture_batch
    ADD COLUMN IF NOT EXISTS capture_request_schema_version TEXT NOT NULL
        DEFAULT 'advisory_phase1_capture_batch_v1',
    ADD COLUMN IF NOT EXISTS capture_purpose TEXT NOT NULL
        DEFAULT 'OBSERVATION_CAPTURE_V1';

ALTER TABLE app.advisory_capture_batch
    DROP CONSTRAINT IF EXISTS advisory_capture_batch_schema_purpose_check;
ALTER TABLE app.advisory_capture_batch
    ADD CONSTRAINT advisory_capture_batch_schema_purpose_check CHECK (
        (capture_request_schema_version = 'advisory_phase1_capture_batch_v1'
            AND capture_purpose = 'OBSERVATION_CAPTURE_V1')
        OR
        (capture_request_schema_version = 'advisory_phase1_capture_batch_v2'
            AND capture_purpose = 'LABEL_CAPTURE_V1')
    );

CREATE OR REPLACE FUNCTION app.verify_advisory_capture_batch_transition()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.capture_status <> 'PLANNED' OR NEW.row_version <> 1 OR NEW.fencing_token <> 1 THEN
            RAISE EXCEPTION 'ADVISORY_PHASE1_CAPTURE_BATCH_INITIAL_STATE_INVALID';
        END IF;
        IF NEW.capture_request_schema_version = 'advisory_phase1_capture_batch_v2' AND (
            NEW.request_payload_jsonb->>'schema_version' <> 'advisory_phase1_capture_batch_v2'
            OR NEW.request_payload_jsonb->>'capture_purpose' <> 'LABEL_CAPTURE_V1'
            OR NEW.binding_jsonb->>'schema_version' <> 'advisory_phase1_label_capture_binding_v1'
        ) THEN
            RAISE EXCEPTION 'ADVISORY_PHASE1C3_CAPTURE_PAYLOAD_CLOSURE_INVALID';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.capture_request_hash <> OLD.capture_request_hash
       OR NEW.request_payload_jsonb <> OLD.request_payload_jsonb
       OR NEW.binding_jsonb <> OLD.binding_jsonb
       OR NEW.control_binding_event_hash <> OLD.control_binding_event_hash
       OR NEW.handoff_readiness_hash <> OLD.handoff_readiness_hash
       OR NEW.admission_scope_id <> OLD.admission_scope_id
       OR NEW.admission_scope_hash <> OLD.admission_scope_hash
       OR NEW.capture_request_schema_version <> OLD.capture_request_schema_version
       OR NEW.capture_purpose <> OLD.capture_purpose
       OR NEW.capture_attempt_no <> OLD.capture_attempt_no
       OR NEW.predecessor_capture_batch_id IS DISTINCT FROM OLD.predecessor_capture_batch_id THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1_CAPTURE_BATCH_IMMUTABLE';
    END IF;
    IF NEW.row_version <> OLD.row_version + 1 OR NEW.fencing_token < OLD.fencing_token THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1_CAPTURE_BATCH_CAS_INVALID';
    END IF;
    IF OLD.capture_status = 'PLANNED' AND NEW.capture_status <> 'RUNNING' THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1_CAPTURE_BATCH_TRANSITION_INVALID';
    ELSIF OLD.capture_status = 'RUNNING' AND NEW.capture_status NOT IN ('RUNNING', 'COMPLETE', 'FAILED', 'EXPIRED', 'ABORTED') THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1_CAPTURE_BATCH_TRANSITION_INVALID';
    ELSIF OLD.capture_status IN ('COMPLETE', 'FAILED', 'EXPIRED', 'ABORTED') THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1_CAPTURE_BATCH_TERMINAL';
    END IF;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION app.verify_advisory_capture_plan_closure()
RETURNS TRIGGER AS $$
DECLARE
    target_batch_id TEXT := COALESCE(NEW.capture_batch_id, OLD.capture_batch_id);
    batch_schema TEXT;
    batch_purpose TEXT;
    plan_count INTEGER;
BEGIN
    SELECT capture_request_schema_version, capture_purpose
      INTO batch_schema, batch_purpose
      FROM app.advisory_capture_batch
     WHERE capture_batch_id = target_batch_id;
    SELECT count(*) INTO plan_count
      FROM app.advisory_capture_plan
     WHERE capture_batch_id = target_batch_id;
    IF batch_schema = 'advisory_phase1_capture_batch_v1' AND (batch_purpose <> 'OBSERVATION_CAPTURE_V1' OR plan_count < 1) THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_CAPTURE_PAYLOAD_CLOSURE_INVALID';
    END IF;
    IF batch_schema = 'advisory_phase1_capture_batch_v2' AND (batch_purpose <> 'LABEL_CAPTURE_V1' OR plan_count <> 0) THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_CAPTURE_PAYLOAD_CLOSURE_INVALID';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_capture_plan_closure_batch ON app.advisory_capture_batch;
CREATE CONSTRAINT TRIGGER trg_verify_advisory_capture_plan_closure_batch
AFTER INSERT ON app.advisory_capture_batch
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_capture_plan_closure();
DROP TRIGGER IF EXISTS trg_verify_advisory_capture_plan_closure_plan ON app.advisory_capture_plan;
CREATE CONSTRAINT TRIGGER trg_verify_advisory_capture_plan_closure_plan
AFTER INSERT OR DELETE ON app.advisory_capture_plan
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_capture_plan_closure();

CREATE TABLE IF NOT EXISTS app.advisory_dataset_blob (
    store_backend_hash TEXT NOT NULL,
    blob_sha256 TEXT NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes > 0),
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (store_backend_hash, blob_sha256),
    UNIQUE (store_backend_hash, blob_sha256, size_bytes)
);

CREATE TABLE IF NOT EXISTS app.advisory_outcome_label (
    label_version_id TEXT PRIMARY KEY,
    label_content_hash TEXT NOT NULL UNIQUE,
    label_key_hash TEXT NOT NULL,
    label_revision_no INTEGER NOT NULL CHECK (label_revision_no >= 1),
    supersedes_label_version_id TEXT UNIQUE REFERENCES app.advisory_outcome_label(label_version_id),
    supersedes_label_version_hash TEXT,
    label_append_request_hash TEXT NOT NULL UNIQUE,
    label_policy_bundle_id TEXT NOT NULL,
    label_policy_bundle_hash TEXT NOT NULL,
    label_policy_hash TEXT NOT NULL,
    label_source_revision_set_id TEXT NOT NULL,
    label_source_revision_set_hash TEXT NOT NULL,
    owner_type TEXT NOT NULL CHECK (owner_type IN ('CANDIDATE', 'UNIVERSE')),
    owner_key TEXT NOT NULL,
    canonical_signal_id TEXT NOT NULL REFERENCES app.advisory_signal_observation(canonical_signal_id),
    observation_version_id TEXT REFERENCES app.advisory_signal_observation_version(observation_version_id),
    candidate_stage_evidence_id TEXT REFERENCES app.advisory_signal_stage_evidence(stage_evidence_id),
    symbol TEXT NOT NULL,
    universe_layer TEXT,
    decision_as_of_trade_date DATE NOT NULL,
    evidence_scope TEXT NOT NULL CHECK (evidence_scope = 'RETROSPECTIVE_RESEARCH_ONLY'),
    horizon_trading_days INTEGER NOT NULL CHECK (horizon_trading_days >= 0),
    projection TEXT NOT NULL CHECK (projection IN ('GAP_1D','RETURN_GROSS','RETURN_NET_ABSOLUTE','RETURN_NET_EXCESS','PATH_MFE','PATH_MAE','EXECUTABLE_MFE','EXECUTABLE_MAE','BARRIER','SURVIVAL')),
    projection_schema_version TEXT NOT NULL,
    intended_entry_trade_date DATE NOT NULL,
    earliest_sell_eligible_trade_date DATE NOT NULL,
    exit_trade_date DATE,
    maturity_status TEXT NOT NULL CHECK (maturity_status IN ('PENDING', 'MATURED', 'RIGHT_CENSORED', 'UNAVAILABLE')),
    outcome_event_status TEXT NOT NULL CHECK (outcome_event_status IN ('NONE', 'TERMINAL', 'BARRIER')),
    entry_status TEXT NOT NULL,
    projection_payload_hash TEXT NOT NULL,
    calculation_evidence_sha256 TEXT NOT NULL,
    calculation_evidence_size_bytes BIGINT NOT NULL CHECK (calculation_evidence_size_bytes > 0),
    calculation_evidence_store_backend_hash TEXT NOT NULL,
    created_by_capture_batch_id TEXT NOT NULL REFERENCES app.advisory_capture_batch(capture_batch_id),
    computed_at TIMESTAMPTZ NOT NULL,
    UNIQUE (label_key_hash, label_revision_no),
    UNIQUE (label_version_id, decision_as_of_trade_date),
    FOREIGN KEY (calculation_evidence_store_backend_hash, calculation_evidence_sha256, calculation_evidence_size_bytes)
        REFERENCES app.advisory_dataset_blob(store_backend_hash, blob_sha256, size_bytes),
    CHECK (
        (label_revision_no = 1 AND supersedes_label_version_id IS NULL AND supersedes_label_version_hash IS NULL)
        OR (label_revision_no > 1 AND supersedes_label_version_id IS NOT NULL AND supersedes_label_version_hash IS NOT NULL)
    ),
    CHECK (
        (owner_type = 'CANDIDATE' AND observation_version_id IS NOT NULL AND candidate_stage_evidence_id IS NOT NULL AND universe_layer IS NULL)
        OR (owner_type = 'UNIVERSE' AND observation_version_id IS NULL AND candidate_stage_evidence_id IS NULL AND universe_layer IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS app.advisory_outcome_label_payload (
    decision_as_of_trade_date DATE NOT NULL,
    label_version_id TEXT NOT NULL,
    label_content_hash TEXT NOT NULL,
    projection TEXT NOT NULL CHECK (projection IN ('GAP_1D','RETURN_GROSS','RETURN_NET_ABSOLUTE','RETURN_NET_EXCESS','PATH_MFE','PATH_MAE','EXECUTABLE_MFE','EXECUTABLE_MAE','BARRIER','SURVIVAL')),
    projection_schema_version TEXT NOT NULL,
    horizon_trading_days INTEGER NOT NULL CHECK (horizon_trading_days >= 0),
    scheduled_maturity_ts TIMESTAMPTZ NOT NULL,
    source_closed_at TIMESTAMPTZ,
    event_closed_at TIMESTAMPTZ,
    failure_observed_at TIMESTAMPTZ,
    maturity_status TEXT NOT NULL CHECK (maturity_status IN ('PENDING', 'MATURED', 'RIGHT_CENSORED', 'UNAVAILABLE')),
    outcome_event_status TEXT NOT NULL CHECK (outcome_event_status IN ('NONE', 'TERMINAL', 'BARRIER')),
    entry_status TEXT NOT NULL CHECK (entry_status IN ('EXECUTABLE', 'NOT_EXECUTABLE', 'EXECUTION_AMBIGUOUS', 'UNAVAILABLE')),
    missing_source_receipt_hash TEXT,
    projection_value_decimal NUMERIC(38, 12),
    projection_event_code TEXT,
    projection_payload_hash TEXT NOT NULL,
    entry_price_raw_yuan NUMERIC(38, 12),
    entry_adj_factor NUMERIC(38, 12),
    exit_price_raw_yuan NUMERIC(38, 12),
    exit_adj_factor NUMERIC(38, 12),
    entry_quantity NUMERIC(38, 12),
    exit_quantity NUMERIC(38, 12),
    buy_execution_price_yuan NUMERIC(38, 12),
    sell_execution_price_yuan NUMERIC(38, 12),
    buy_notional_yuan NUMERIC(38, 12),
    sell_notional_yuan NUMERIC(38, 12),
    buy_fee_yuan NUMERIC(38, 12),
    sell_fee_yuan NUMERIC(38, 12),
    entry_cash_yuan NUMERIC(38, 12),
    residual_cash_yuan NUMERIC(38, 12),
    exit_cash_yuan NUMERIC(38, 12),
    terminal_value_yuan NUMERIC(38, 12),
    cost_breakdown_hash TEXT,
    benchmark_gross_total_return NUMERIC(38, 12),
    benchmark_net_total_return NUMERIC(38, 12),
    entry_day_touch_status TEXT,
    executable_barrier_status TEXT,
    executable_event_trade_date DATE,
    time_to_executable_hit_trading_days INTEGER,
    observed_holding_trading_days INTEGER,
    terminal_disposition TEXT,
    terminal_symbol TEXT,
    terminal_event_trade_date DATE,
    terminal_event_closed_at TIMESTAMPTZ,
    terminal_source_hash TEXT,
    terminal_settlement_raw_li NUMERIC(38, 12),
    terminal_settlement_adj_factor NUMERIC(38, 12),
    terminal_settlement_quantity_multiplier NUMERIC(38, 12),
    terminal_settlement_cashflow_yuan_per_share NUMERIC(38, 12),
    censor_reason_code TEXT,
    policy_bundle_hash TEXT NOT NULL,
    price_path_hash TEXT NOT NULL,
    corporate_actions_hash TEXT NOT NULL,
    benchmark_bundle_hash TEXT,
    formula_schema_version TEXT NOT NULL,
    calculation_evidence_schema_version TEXT NOT NULL,
    calculation_evidence_uri TEXT NOT NULL,
    calculation_evidence_sha256 TEXT NOT NULL,
    calculation_evidence_size_bytes BIGINT NOT NULL CHECK (calculation_evidence_size_bytes > 0),
    calculation_evidence_store_backend_hash TEXT NOT NULL,
    reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(reason_codes) = 'array'),
    PRIMARY KEY (decision_as_of_trade_date, label_version_id),
    FOREIGN KEY (label_version_id, decision_as_of_trade_date)
        REFERENCES app.advisory_outcome_label(label_version_id, decision_as_of_trade_date),
    FOREIGN KEY (calculation_evidence_store_backend_hash, calculation_evidence_sha256, calculation_evidence_size_bytes)
        REFERENCES app.advisory_dataset_blob(store_backend_hash, blob_sha256, size_bytes),
    CHECK (
        (maturity_status = 'PENDING' AND projection_value_decimal IS NULL AND projection_event_code IS NULL
            AND failure_observed_at IS NULL AND missing_source_receipt_hash IS NULL)
        OR (maturity_status = 'UNAVAILABLE' AND failure_observed_at IS NOT NULL
            AND missing_source_receipt_hash IS NOT NULL AND projection_value_decimal IS NULL
            AND projection_event_code IS NULL)
        OR (maturity_status = 'RIGHT_CENSORED' AND event_closed_at IS NOT NULL
            AND projection_event_code = 'RIGHT_CENSORED'
            AND ((projection = 'SURVIVAL' AND projection_value_decimal IS NOT NULL)
                OR (projection <> 'SURVIVAL' AND projection_value_decimal IS NULL)))
        OR (maturity_status = 'MATURED' AND source_closed_at IS NOT NULL
            AND ((projection = 'BARRIER' AND projection_value_decimal IS NULL AND projection_event_code IS NOT NULL)
                OR (projection = 'SURVIVAL' AND projection_value_decimal IS NOT NULL AND projection_event_code IS NOT NULL)
                OR (projection NOT IN ('BARRIER','SURVIVAL') AND projection_value_decimal IS NOT NULL AND projection_event_code IS NULL)))
    ),
    CHECK (
        (terminal_disposition = 'NONE' AND terminal_symbol IS NULL AND terminal_event_trade_date IS NULL
            AND terminal_event_closed_at IS NULL AND terminal_source_hash IS NULL
            AND terminal_settlement_raw_li IS NULL AND terminal_settlement_adj_factor IS NULL
            AND terminal_settlement_quantity_multiplier IS NULL
            AND terminal_settlement_cashflow_yuan_per_share IS NULL AND censor_reason_code IS NULL)
        OR
        (terminal_disposition = 'RIGHT_CENSORED' AND terminal_symbol IS NOT NULL
            AND terminal_event_trade_date IS NOT NULL AND terminal_event_closed_at IS NOT NULL
            AND terminal_source_hash IS NOT NULL AND censor_reason_code IS NOT NULL
            AND terminal_settlement_raw_li IS NULL AND terminal_settlement_adj_factor IS NULL
            AND terminal_settlement_quantity_multiplier IS NULL
            AND terminal_settlement_cashflow_yuan_per_share IS NULL)
        OR
        (terminal_disposition = 'TERMINAL' AND terminal_symbol IS NOT NULL
            AND terminal_event_trade_date IS NOT NULL AND terminal_event_closed_at IS NOT NULL
            AND terminal_source_hash IS NOT NULL AND censor_reason_code IS NULL)
    )
) PARTITION BY RANGE (decision_as_of_trade_date);

CREATE TABLE IF NOT EXISTS app.advisory_outcome_label_payload_202606
    PARTITION OF app.advisory_outcome_label_payload
    FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE IF NOT EXISTS app.advisory_outcome_label_payload_202607
    PARTITION OF app.advisory_outcome_label_payload
    FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');
CREATE TABLE IF NOT EXISTS app.advisory_outcome_label_payload_202608
    PARTITION OF app.advisory_outcome_label_payload
    FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');

CREATE OR REPLACE FUNCTION app.verify_advisory_outcome_label_closure()
RETURNS TRIGGER AS $$
DECLARE
    target_id TEXT := COALESCE(NEW.label_version_id, OLD.label_version_id);
    header_count INTEGER;
    payload_count INTEGER;
BEGIN
    SELECT count(*) INTO header_count FROM app.advisory_outcome_label WHERE label_version_id = target_id;
    SELECT count(*) INTO payload_count FROM app.advisory_outcome_label_payload WHERE label_version_id = target_id;
    IF header_count <> 1 OR payload_count <> 1 THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_LABEL_HEADER_PAYLOAD_CLOSURE_INVALID';
    END IF;
    IF EXISTS (
        SELECT 1 FROM app.advisory_outcome_label h
        JOIN app.advisory_outcome_label_payload p
          ON p.label_version_id = h.label_version_id AND p.decision_as_of_trade_date = h.decision_as_of_trade_date
        WHERE h.label_version_id = target_id
          AND (h.label_content_hash <> p.label_content_hash OR h.projection <> p.projection
            OR h.projection_schema_version <> p.projection_schema_version OR h.horizon_trading_days <> p.horizon_trading_days
            OR h.maturity_status <> p.maturity_status OR h.outcome_event_status <> p.outcome_event_status
            OR h.entry_status <> p.entry_status OR h.projection_payload_hash <> p.projection_payload_hash
            OR h.calculation_evidence_sha256 <> p.calculation_evidence_sha256
            OR h.calculation_evidence_size_bytes <> p.calculation_evidence_size_bytes
            OR h.calculation_evidence_store_backend_hash <> p.calculation_evidence_store_backend_hash)
    ) THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_LABEL_HEADER_PAYLOAD_CLOSURE_INVALID';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION app.verify_advisory_outcome_label_predecessor()
RETURNS TRIGGER AS $$
DECLARE
    previous_row app.advisory_outcome_label%ROWTYPE;
BEGIN
    IF NEW.label_revision_no = 1 THEN
        RETURN NEW;
    END IF;
    SELECT * INTO previous_row FROM app.advisory_outcome_label
     WHERE label_version_id = NEW.supersedes_label_version_id FOR KEY SHARE;
    IF NOT FOUND OR previous_row.label_key_hash <> NEW.label_key_hash
       OR previous_row.label_revision_no <> NEW.label_revision_no - 1
       OR previous_row.label_content_hash <> NEW.supersedes_label_version_hash THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_LABEL_PREDECESSOR_INVALID';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION app.verify_advisory_outcome_label_owner()
RETURNS TRIGGER AS $$
DECLARE
    creator_status TEXT;
    creator_schema TEXT;
    creator_purpose TEXT;
    creator_lease_expires_at TIMESTAMPTZ;
    database_now TIMESTAMPTZ;
    observed_signal_id TEXT;
    stage_observation_id TEXT;
    candidate_membership TEXT;
BEGIN
    SELECT capture_status, capture_request_schema_version, capture_purpose, lease_expires_at, clock_timestamp()
      INTO creator_status, creator_schema, creator_purpose, creator_lease_expires_at, database_now
      FROM app.advisory_capture_batch WHERE capture_batch_id = NEW.created_by_capture_batch_id FOR KEY SHARE;
    IF creator_status IS DISTINCT FROM 'RUNNING'
       OR creator_schema IS DISTINCT FROM 'advisory_phase1_capture_batch_v2'
       OR creator_purpose IS DISTINCT FROM 'LABEL_CAPTURE_V1'
       OR creator_lease_expires_at IS NULL OR creator_lease_expires_at <= database_now THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_LABEL_CREATOR_CAPTURE_INVALID';
    END IF;
    IF NEW.owner_type = 'CANDIDATE' THEN
        SELECT canonical_signal_id INTO observed_signal_id
          FROM app.advisory_signal_observation_version
         WHERE observation_version_id = NEW.observation_version_id FOR KEY SHARE;
        SELECT observation_version_id INTO stage_observation_id
          FROM app.advisory_signal_stage_evidence
         WHERE stage_evidence_id = NEW.candidate_stage_evidence_id AND stage = 'alpha_raw' FOR KEY SHARE;
        SELECT membership_status INTO candidate_membership
          FROM app.advisory_signal_stage_candidate
         WHERE stage_evidence_id = NEW.candidate_stage_evidence_id AND symbol = NEW.symbol FOR KEY SHARE;
        IF observed_signal_id IS DISTINCT FROM NEW.canonical_signal_id
           OR stage_observation_id IS DISTINCT FROM NEW.observation_version_id
           OR candidate_membership IS DISTINCT FROM 'INCLUDED' THEN
            RAISE EXCEPTION 'ADVISORY_PHASE1C3_LABEL_OWNER_MEMBERSHIP_INVALID';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_outcome_label_predecessor ON app.advisory_outcome_label;
CREATE TRIGGER trg_verify_advisory_outcome_label_predecessor
BEFORE INSERT ON app.advisory_outcome_label
FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_outcome_label_predecessor();
DROP TRIGGER IF EXISTS trg_verify_advisory_outcome_label_owner ON app.advisory_outcome_label;
CREATE TRIGGER trg_verify_advisory_outcome_label_owner
BEFORE INSERT ON app.advisory_outcome_label
FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_outcome_label_owner();
DROP TRIGGER IF EXISTS trg_verify_advisory_outcome_label_closure_header ON app.advisory_outcome_label;
CREATE CONSTRAINT TRIGGER trg_verify_advisory_outcome_label_closure_header
AFTER INSERT ON app.advisory_outcome_label DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_outcome_label_closure();
DROP TRIGGER IF EXISTS trg_verify_advisory_outcome_label_closure_payload ON app.advisory_outcome_label_payload;
CREATE CONSTRAINT TRIGGER trg_verify_advisory_outcome_label_closure_payload
AFTER INSERT ON app.advisory_outcome_label_payload DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_outcome_label_closure();

CREATE TABLE IF NOT EXISTS app.advisory_dataset_build (
    build_id TEXT PRIMARY KEY,
    logical_build_key_sha256 TEXT NOT NULL,
    build_generation INTEGER NOT NULL CHECK (build_generation >= 1),
    predecessor_build_id TEXT REFERENCES app.advisory_dataset_build(build_id),
    build_request_hash TEXT NOT NULL,
    build_request_payload_jsonb JSONB NOT NULL,
    snapshot_source_revision_set_hash TEXT NOT NULL,
    capture_set_hash TEXT NOT NULL,
    handoff_readiness_hash TEXT NOT NULL,
    admission_scope_set_hash TEXT NOT NULL,
    query_registry_hash TEXT NOT NULL,
    date_start DATE NOT NULL,
    date_end DATE NOT NULL CHECK (date_end >= date_start),
    base_snapshot_id TEXT,
    base_snapshot_content_hash TEXT,
    base_manifest_sha256 TEXT,
    base_policy_compatibility_hash TEXT,
    builder_version TEXT NOT NULL,
    code_commit TEXT NOT NULL,
    writer_version TEXT NOT NULL,
    partition_policy_hash TEXT NOT NULL,
    compression_config_hash TEXT NOT NULL,
    lifecycle_status TEXT NOT NULL CHECK (lifecycle_status IN ('ACTIVE', 'SEALED', 'FAILED_TERMINAL', 'ABORTED')),
    checkpoint TEXT NOT NULL CHECK (checkpoint IN ('REQUESTED', 'MATERIALIZED', 'VERIFIED', 'PROMOTED', 'SEALED')),
    current_fencing_token BIGINT NOT NULL CHECK (current_fencing_token >= 1),
    current_attempt_id TEXT,
    materialized_attempt_id TEXT,
    materialize_receipt_hash TEXT,
    materialized_file_set_hash TEXT,
    verified_attempt_id TEXT,
    verify_receipt_hash TEXT,
    verified_file_set_hash TEXT,
    verification_contract_version TEXT,
    promoted_attempt_id TEXT,
    promotion_receipt_hash TEXT,
    promoted_manifest_hash TEXT,
    sealed_attempt_id TEXT,
    seal_receipt_hash TEXT,
    sealed_snapshot_id TEXT,
    terminated_at TIMESTAMPTZ,
    termination_receipt_hash TEXT,
    terminal_reason_code TEXT,
    terminal_payload_hash TEXT,
    row_version BIGINT NOT NULL CHECK (row_version >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (logical_build_key_sha256, build_generation),
    CHECK ((base_snapshot_id IS NULL AND base_snapshot_content_hash IS NULL AND base_manifest_sha256 IS NULL AND base_policy_compatibility_hash IS NULL)
        OR (base_snapshot_id IS NOT NULL AND base_snapshot_content_hash IS NOT NULL AND base_manifest_sha256 IS NOT NULL AND base_policy_compatibility_hash IS NOT NULL)),
    CHECK ((build_generation = 1 AND predecessor_build_id IS NULL)
        OR (build_generation > 1 AND predecessor_build_id IS NOT NULL)),
    CHECK ((checkpoint IN ('REQUESTED') AND materialized_attempt_id IS NULL AND verified_attempt_id IS NULL)
        OR (checkpoint IN ('MATERIALIZED','VERIFIED','PROMOTED','SEALED') AND materialized_attempt_id IS NOT NULL
            AND materialize_receipt_hash IS NOT NULL AND materialized_file_set_hash IS NOT NULL)),
    CHECK ((checkpoint IN ('REQUESTED','MATERIALIZED') AND verified_attempt_id IS NULL AND verification_contract_version IS NULL)
        OR (checkpoint IN ('VERIFIED','PROMOTED','SEALED') AND verified_attempt_id IS NOT NULL
            AND verify_receipt_hash IS NOT NULL AND verified_file_set_hash IS NOT NULL AND verification_contract_version IS NOT NULL)),
    CHECK ((checkpoint IN ('REQUESTED','MATERIALIZED','VERIFIED') AND promoted_attempt_id IS NULL
            AND promotion_receipt_hash IS NULL AND promoted_manifest_hash IS NULL)
        OR (checkpoint IN ('PROMOTED','SEALED') AND promoted_attempt_id IS NOT NULL
            AND promotion_receipt_hash IS NOT NULL AND promoted_manifest_hash IS NOT NULL)),
    CHECK ((checkpoint <> 'SEALED' AND sealed_attempt_id IS NULL AND seal_receipt_hash IS NULL AND sealed_snapshot_id IS NULL)
        OR (checkpoint = 'SEALED' AND sealed_attempt_id IS NOT NULL AND seal_receipt_hash IS NOT NULL AND sealed_snapshot_id IS NOT NULL)),
    CHECK ((lifecycle_status = 'SEALED') = (checkpoint = 'SEALED')),
    CHECK (lifecycle_status = 'ACTIVE' OR current_attempt_id IS NULL)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_advisory_dataset_build_active_key
ON app.advisory_dataset_build(logical_build_key_sha256) WHERE lifecycle_status = 'ACTIVE';

CREATE TABLE IF NOT EXISTS app.advisory_dataset_build_attempt (
    attempt_id TEXT PRIMARY KEY,
    build_id TEXT NOT NULL REFERENCES app.advisory_dataset_build(build_id),
    attempt_no INTEGER NOT NULL CHECK (attempt_no >= 1),
    operation TEXT NOT NULL CHECK (operation IN ('MATERIALIZE', 'VERIFY', 'PROMOTE', 'SEAL', 'RECOVER')),
    attempt_state TEXT NOT NULL CHECK (attempt_state IN ('ACTIVE', 'SUCCEEDED', 'FAILED', 'EXPIRED', 'ABORTED')),
    lease_owner_id TEXT NOT NULL,
    lease_token TEXT NOT NULL,
    fencing_token BIGINT NOT NULL CHECK (fencing_token >= 1),
    expected_build_row_version BIGINT NOT NULL,
    expected_checkpoint TEXT NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL,
    heartbeat_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    staging_uri TEXT,
    operation_request_hash TEXT NOT NULL,
    predecessor_attempt_id TEXT UNIQUE REFERENCES app.advisory_dataset_build_attempt(attempt_id),
    error_code TEXT,
    error_hash TEXT,
    UNIQUE (build_id, attempt_no),
    CHECK ((attempt_state = 'ACTIVE' AND finished_at IS NULL) OR (attempt_state <> 'ACTIVE' AND finished_at IS NOT NULL)),
    CHECK ((operation = 'RECOVER' AND predecessor_attempt_id IS NOT NULL)
        OR (operation <> 'RECOVER' AND predecessor_attempt_id IS NULL))
);

CREATE TABLE IF NOT EXISTS app.advisory_dataset_attempt_file (
    attempt_id TEXT NOT NULL REFERENCES app.advisory_dataset_build_attempt(attempt_id),
    fencing_token BIGINT NOT NULL,
    logical_path TEXT NOT NULL,
    logical_role TEXT NOT NULL,
    partition_key_hash TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    staging_uri TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes > 0),
    row_count BIGINT NOT NULL CHECK (row_count >= 0),
    schema_fingerprint TEXT NOT NULL,
    partition_content_hash TEXT NOT NULL,
    min_decision_date DATE,
    max_decision_date DATE,
    min_sort_key TEXT,
    max_sort_key TEXT,
    compression TEXT NOT NULL,
    writer_version TEXT NOT NULL,
    PRIMARY KEY (attempt_id, logical_path),
    UNIQUE (attempt_id, logical_role, partition_key_hash, ordinal)
);

CREATE TABLE IF NOT EXISTS app.advisory_dataset_build_event (
    event_id TEXT PRIMARY KEY,
    build_id TEXT NOT NULL REFERENCES app.advisory_dataset_build(build_id),
    attempt_id TEXT REFERENCES app.advisory_dataset_build_attempt(attempt_id),
    fencing_token BIGINT,
    event_type TEXT NOT NULL CHECK (event_type IN ('REQUESTED','READINESS_PASSED','ATTEMPT_STARTED','SOURCE_VIEW_OPENED','MATERIALIZED','VERIFIED','PROMOTED','SEALED','ATTEMPT_FAILED','ATTEMPT_EXPIRED','RECOVERY_STARTED','BUILD_TERMINATED','ABORTED')),
    event_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    actor TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(reason_codes) = 'array'),
    UNIQUE (build_id, event_type, payload_hash)
);

CREATE TABLE IF NOT EXISTS app.advisory_dataset_build_gap (
    gap_id TEXT PRIMARY KEY,
    capture_batch_id TEXT REFERENCES app.advisory_capture_batch(capture_batch_id),
    canonical_signal_id TEXT,
    audit_target_id TEXT NOT NULL,
    program_id TEXT NOT NULL,
    package_id TEXT NOT NULL,
    decision_as_of_trade_date DATE NOT NULL,
    signal_capability TEXT NOT NULL,
    gap_class TEXT NOT NULL CHECK (gap_class IN ('NO_CANDIDATE_EVIDENCE','MISSING_SOURCE','MISSING_RUNTIME','CONFLICT','NOT_REPLAYABLE','CAPTURE_FAILED')),
    evidence_scope TEXT NOT NULL,
    missing_evidence_hashes JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(missing_evidence_hashes) = 'array'),
    reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(reason_codes) = 'array'),
    gap_content_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS app.advisory_dataset_snapshot (
    snapshot_id TEXT PRIMARY KEY,
    snapshot_content_hash TEXT NOT NULL UNIQUE,
    snapshot_state TEXT NOT NULL CHECK (snapshot_state = 'SEALED'),
    manifest_core_sha256 TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL UNIQUE,
    promotion_receipt_uri TEXT NOT NULL,
    promotion_receipt_hash TEXT NOT NULL,
    build_id TEXT NOT NULL UNIQUE REFERENCES app.advisory_dataset_build(build_id),
    snapshot_schema_version TEXT NOT NULL,
    snapshot_source_revision_set_hash TEXT NOT NULL,
    capture_set_hash TEXT NOT NULL,
    base_snapshot_id TEXT,
    base_snapshot_content_hash TEXT,
    base_manifest_sha256 TEXT,
    base_policy_compatibility_hash TEXT,
    handoff_readiness_hash TEXT NOT NULL,
    admission_scope_set_hash TEXT NOT NULL,
    query_registry_hash TEXT NOT NULL,
    builder_version TEXT NOT NULL,
    code_commit TEXT NOT NULL,
    writer_version TEXT NOT NULL,
    partition_policy_hash TEXT NOT NULL,
    policy_compatibility_hash TEXT NOT NULL,
    dataset_capability_manifest JSONB NOT NULL,
    dataset_capability_manifest_hash TEXT NOT NULL,
    schema_fingerprint TEXT NOT NULL,
    file_count BIGINT NOT NULL CHECK (file_count >= 0),
    row_count BIGINT NOT NULL CHECK (row_count >= 0),
    total_bytes BIGINT NOT NULL CHECK (total_bytes >= 0),
    label_maturity_event_summary JSONB NOT NULL,
    sealed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS app.advisory_dataset_snapshot_file (
    snapshot_id TEXT NOT NULL REFERENCES app.advisory_dataset_snapshot(snapshot_id),
    logical_path TEXT NOT NULL,
    logical_role TEXT NOT NULL,
    partition_key_hash TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    content_uri TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes > 0),
    row_count BIGINT NOT NULL CHECK (row_count >= 0),
    schema_fingerprint TEXT NOT NULL,
    partition_content_hash TEXT NOT NULL,
    store_backend_hash TEXT NOT NULL,
    blob_sha256 TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, logical_path),
    UNIQUE (snapshot_id, logical_role, partition_key_hash, ordinal),
    UNIQUE (snapshot_id, logical_path, store_backend_hash, blob_sha256),
    FOREIGN KEY (store_backend_hash, blob_sha256) REFERENCES app.advisory_dataset_blob(store_backend_hash, blob_sha256),
    CHECK (sha256 = blob_sha256)
);

CREATE TABLE IF NOT EXISTS app.advisory_dataset_snapshot_observation (
    snapshot_id TEXT NOT NULL REFERENCES app.advisory_dataset_snapshot(snapshot_id),
    canonical_signal_id TEXT NOT NULL,
    observation_version_id TEXT NOT NULL REFERENCES app.advisory_signal_observation_version(observation_version_id),
    evidence_scope TEXT NOT NULL,
    oos_interval_id TEXT NOT NULL,
    selector_policy_hash TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, canonical_signal_id),
    UNIQUE (snapshot_id, observation_version_id)
);

CREATE TABLE IF NOT EXISTS app.advisory_dataset_snapshot_label (
    snapshot_id TEXT NOT NULL REFERENCES app.advisory_dataset_snapshot(snapshot_id),
    label_key_hash TEXT NOT NULL,
    label_version_id TEXT NOT NULL REFERENCES app.advisory_outcome_label(label_version_id),
    canonical_signal_id TEXT NOT NULL,
    observation_version_id TEXT NOT NULL REFERENCES app.advisory_signal_observation_version(observation_version_id),
    candidate_stage_evidence_id TEXT NOT NULL REFERENCES app.advisory_signal_stage_evidence(stage_evidence_id),
    symbol TEXT NOT NULL,
    selector_policy_hash TEXT NOT NULL,
    PRIMARY KEY (snapshot_id, label_key_hash),
    UNIQUE (snapshot_id, label_version_id),
    FOREIGN KEY (snapshot_id, canonical_signal_id) REFERENCES app.advisory_dataset_snapshot_observation(snapshot_id, canonical_signal_id)
);

CREATE TABLE IF NOT EXISTS app.advisory_dataset_snapshot_invalidation (
    invalidation_id TEXT PRIMARY KEY,
    snapshot_id TEXT NOT NULL REFERENCES app.advisory_dataset_snapshot(snapshot_id),
    manifest_sha256 TEXT NOT NULL,
    invalidated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    invalidated_by TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    reason_hash TEXT NOT NULL,
    invalidation_request_hash TEXT NOT NULL UNIQUE,
    replacement_snapshot_id TEXT REFERENCES app.advisory_dataset_snapshot(snapshot_id),
    invalidation_content_hash TEXT NOT NULL UNIQUE,
    UNIQUE (snapshot_id)
);

CREATE TABLE IF NOT EXISTS app.advisory_dataset_snapshot_blob_ref (
    snapshot_id TEXT NOT NULL,
    logical_path TEXT NOT NULL,
    logical_role TEXT NOT NULL,
    partition_key_hash TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    store_backend_hash TEXT NOT NULL,
    blob_sha256 TEXT NOT NULL,
    ref_content_hash TEXT NOT NULL UNIQUE,
    PRIMARY KEY (snapshot_id, logical_path),
    UNIQUE (snapshot_id, logical_role, partition_key_hash, ordinal),
    FOREIGN KEY (snapshot_id, logical_path, store_backend_hash, blob_sha256)
        REFERENCES app.advisory_dataset_snapshot_file(snapshot_id, logical_path, store_backend_hash, blob_sha256),
    FOREIGN KEY (store_backend_hash, blob_sha256) REFERENCES app.advisory_dataset_blob(store_backend_hash, blob_sha256)
);

CREATE OR REPLACE FUNCTION app.verify_advisory_dataset_build_transition()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.build_id <> OLD.build_id
       OR NEW.logical_build_key_sha256 <> OLD.logical_build_key_sha256
       OR NEW.build_generation <> OLD.build_generation
       OR NEW.predecessor_build_id IS DISTINCT FROM OLD.predecessor_build_id
       OR NEW.build_request_hash <> OLD.build_request_hash
       OR NEW.build_request_payload_jsonb <> OLD.build_request_payload_jsonb
       OR NEW.snapshot_source_revision_set_hash <> OLD.snapshot_source_revision_set_hash
       OR NEW.capture_set_hash <> OLD.capture_set_hash
       OR NEW.handoff_readiness_hash <> OLD.handoff_readiness_hash
       OR NEW.admission_scope_set_hash <> OLD.admission_scope_set_hash
       OR NEW.query_registry_hash <> OLD.query_registry_hash
       OR NEW.date_start <> OLD.date_start OR NEW.date_end <> OLD.date_end
       OR NEW.base_snapshot_id IS DISTINCT FROM OLD.base_snapshot_id
       OR NEW.base_snapshot_content_hash IS DISTINCT FROM OLD.base_snapshot_content_hash
       OR NEW.base_manifest_sha256 IS DISTINCT FROM OLD.base_manifest_sha256
       OR NEW.builder_version <> OLD.builder_version OR NEW.code_commit <> OLD.code_commit
       OR NEW.writer_version <> OLD.writer_version OR NEW.partition_policy_hash <> OLD.partition_policy_hash
       OR NEW.compression_config_hash <> OLD.compression_config_hash THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_BUILD_IMMUTABLE';
    END IF;
    IF NEW.row_version <> OLD.row_version + 1 THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_BUILD_CAS_INVALID';
    END IF;
    IF OLD.lifecycle_status <> 'ACTIVE' THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_BUILD_TERMINAL';
    END IF;
    IF NEW.lifecycle_status NOT IN ('ACTIVE', 'ABORTED', 'FAILED_TERMINAL', 'SEALED') THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_BUILD_TRANSITION_INVALID';
    END IF;
    IF NEW.checkpoint NOT IN ('REQUESTED', 'MATERIALIZED', 'VERIFIED', 'PROMOTED', 'SEALED') THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_BUILD_TRANSITION_INVALID';
    END IF;
    IF array_position(ARRAY['REQUESTED','MATERIALIZED','VERIFIED','PROMOTED','SEALED'], NEW.checkpoint)
       < array_position(ARRAY['REQUESTED','MATERIALIZED','VERIFIED','PROMOTED','SEALED'], OLD.checkpoint) THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_BUILD_TRANSITION_INVALID';
    END IF;
    IF array_position(ARRAY['REQUESTED','MATERIALIZED','VERIFIED','PROMOTED','SEALED'], NEW.checkpoint)
       > array_position(ARRAY['REQUESTED','MATERIALIZED','VERIFIED','PROMOTED','SEALED'], OLD.checkpoint) + 1 THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_BUILD_TRANSITION_INVALID';
    END IF;
    IF (OLD.materialized_attempt_id IS NOT NULL AND NEW.materialized_attempt_id IS DISTINCT FROM OLD.materialized_attempt_id)
       OR (OLD.materialize_receipt_hash IS NOT NULL AND NEW.materialize_receipt_hash IS DISTINCT FROM OLD.materialize_receipt_hash)
       OR (OLD.materialized_file_set_hash IS NOT NULL AND NEW.materialized_file_set_hash IS DISTINCT FROM OLD.materialized_file_set_hash)
       OR (OLD.verified_attempt_id IS NOT NULL AND NEW.verified_attempt_id IS DISTINCT FROM OLD.verified_attempt_id)
       OR (OLD.verify_receipt_hash IS NOT NULL AND NEW.verify_receipt_hash IS DISTINCT FROM OLD.verify_receipt_hash)
       OR (OLD.verified_file_set_hash IS NOT NULL AND NEW.verified_file_set_hash IS DISTINCT FROM OLD.verified_file_set_hash)
       OR (OLD.verification_contract_version IS NOT NULL AND NEW.verification_contract_version IS DISTINCT FROM OLD.verification_contract_version)
       OR (OLD.promoted_attempt_id IS NOT NULL AND NEW.promoted_attempt_id IS DISTINCT FROM OLD.promoted_attempt_id)
       OR (OLD.promotion_receipt_hash IS NOT NULL AND NEW.promotion_receipt_hash IS DISTINCT FROM OLD.promotion_receipt_hash)
       OR (OLD.promoted_manifest_hash IS NOT NULL AND NEW.promoted_manifest_hash IS DISTINCT FROM OLD.promoted_manifest_hash) THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_CHECKPOINT_CONFLICT';
    END IF;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION app.verify_advisory_dataset_attempt_transition()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.attempt_id <> OLD.attempt_id OR NEW.build_id <> OLD.build_id OR NEW.attempt_no <> OLD.attempt_no
       OR NEW.operation <> OLD.operation OR NEW.lease_owner_id <> OLD.lease_owner_id
       OR NEW.lease_token <> OLD.lease_token OR NEW.fencing_token <> OLD.fencing_token
       OR NEW.expected_build_row_version <> OLD.expected_build_row_version
       OR NEW.expected_checkpoint <> OLD.expected_checkpoint OR NEW.acquired_at <> OLD.acquired_at
       OR NEW.started_at <> OLD.started_at
       OR NEW.predecessor_attempt_id IS DISTINCT FROM OLD.predecessor_attempt_id
       OR NEW.operation_request_hash <> OLD.operation_request_hash THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_ATTEMPT_IMMUTABLE';
    END IF;
    IF OLD.attempt_state = 'ACTIVE' AND NEW.attempt_state = 'ACTIVE' THEN
        IF NEW.finished_at IS NOT NULL OR NEW.heartbeat_at <= OLD.heartbeat_at
           OR NEW.expires_at < OLD.expires_at OR NEW.heartbeat_at > NEW.expires_at THEN
            RAISE EXCEPTION 'ADVISORY_PHASE1C3_ATTEMPT_HEARTBEAT_INVALID';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.attempt_state <> 'ACTIVE' OR NEW.attempt_state NOT IN ('SUCCEEDED','FAILED','EXPIRED','ABORTED')
       OR NEW.finished_at IS NULL THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_ATTEMPT_TRANSITION_INVALID';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION app.verify_advisory_dataset_attempt_file_admission()
RETURNS TRIGGER AS $$
DECLARE
    attempt app.advisory_dataset_build_attempt%ROWTYPE;
    build app.advisory_dataset_build%ROWTYPE;
BEGIN
    SELECT * INTO attempt FROM app.advisory_dataset_build_attempt WHERE attempt_id = NEW.attempt_id FOR KEY SHARE;
    SELECT * INTO build FROM app.advisory_dataset_build WHERE build_id = attempt.build_id FOR KEY SHARE;
    IF NOT FOUND OR attempt.attempt_state <> 'ACTIVE' OR attempt.operation <> 'MATERIALIZE'
       OR attempt.fencing_token <> NEW.fencing_token OR build.current_attempt_id <> NEW.attempt_id
       OR build.current_fencing_token <> NEW.fencing_token OR attempt.expires_at <= clock_timestamp() THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_ATTEMPT_FILE_ADMISSION_INVALID';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION app.verify_advisory_dataset_build_attempt_closure()
RETURNS TRIGGER AS $$
DECLARE
    target_build_id TEXT := COALESCE(NEW.build_id, OLD.build_id);
BEGIN
    IF EXISTS (
        SELECT 1 FROM app.advisory_dataset_build b
         WHERE b.build_id = target_build_id AND b.current_attempt_id IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM app.advisory_dataset_build_attempt a
                WHERE a.attempt_id = b.current_attempt_id AND a.build_id = b.build_id
                  AND a.attempt_state = 'ACTIVE' AND a.fencing_token = b.current_fencing_token
           )
    ) OR EXISTS (
        SELECT 1 FROM app.advisory_dataset_build_attempt a
         WHERE a.build_id = target_build_id AND a.attempt_state = 'ACTIVE'
           AND NOT EXISTS (
               SELECT 1 FROM app.advisory_dataset_build b
                WHERE b.build_id = a.build_id AND b.current_attempt_id = a.attempt_id
                  AND b.current_fencing_token = a.fencing_token
           )
    ) OR EXISTS (
        SELECT 1 FROM app.advisory_dataset_build b
         WHERE b.build_id = target_build_id AND b.materialized_attempt_id IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM app.advisory_dataset_build_attempt a
                WHERE a.attempt_id = b.materialized_attempt_id AND a.build_id = b.build_id
                  AND a.operation = 'MATERIALIZE' AND a.attempt_state = 'SUCCEEDED'
           )
    ) OR EXISTS (
        SELECT 1 FROM app.advisory_dataset_build b
         WHERE b.build_id = target_build_id AND b.verified_attempt_id IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM app.advisory_dataset_build_attempt a
                WHERE a.attempt_id = b.verified_attempt_id AND a.build_id = b.build_id
                  AND a.operation = 'VERIFY' AND a.attempt_state = 'SUCCEEDED'
           )
    ) OR EXISTS (
        SELECT 1 FROM app.advisory_dataset_build b
         WHERE b.build_id = target_build_id AND b.promoted_attempt_id IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM app.advisory_dataset_build_attempt a
                WHERE a.attempt_id = b.promoted_attempt_id AND a.build_id = b.build_id
                  AND a.operation = 'PROMOTE' AND a.attempt_state = 'SUCCEEDED'
           )
    ) OR EXISTS (
        SELECT 1 FROM app.advisory_dataset_build b
         WHERE b.build_id = target_build_id AND b.sealed_attempt_id IS NOT NULL
           AND NOT EXISTS (
               SELECT 1 FROM app.advisory_dataset_build_attempt a
                WHERE a.attempt_id = b.sealed_attempt_id AND a.build_id = b.build_id
                  AND a.operation = 'SEAL' AND a.attempt_state = 'SUCCEEDED'
           )
    ) THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_BUILD_ATTEMPT_CLOSURE_INVALID';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION app.verify_advisory_dataset_build_predecessor()
RETURNS TRIGGER AS $$
DECLARE
    predecessor app.advisory_dataset_build%ROWTYPE;
BEGIN
    IF NEW.build_generation = 1 THEN
        RETURN NEW;
    END IF;
    SELECT * INTO predecessor FROM app.advisory_dataset_build
     WHERE build_id = NEW.predecessor_build_id FOR KEY SHARE;
    IF NOT FOUND OR predecessor.logical_build_key_sha256 <> NEW.logical_build_key_sha256
       OR predecessor.build_generation <> NEW.build_generation - 1
       OR predecessor.lifecycle_status <> 'ABORTED'
       OR predecessor.termination_receipt_hash IS NULL THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_BUILD_PREDECESSOR_INVALID';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION app.verify_advisory_snapshot_label_membership()
RETURNS TRIGGER AS $$
DECLARE
    label_row app.advisory_outcome_label%ROWTYPE;
    observation_exists BOOLEAN;
BEGIN
    SELECT * INTO label_row FROM app.advisory_outcome_label WHERE label_version_id = NEW.label_version_id FOR KEY SHARE;
    SELECT EXISTS(
        SELECT 1 FROM app.advisory_dataset_snapshot_observation o
         WHERE o.snapshot_id = NEW.snapshot_id
           AND o.canonical_signal_id = NEW.canonical_signal_id
           AND o.observation_version_id = NEW.observation_version_id
    ) INTO observation_exists;
    IF label_row.label_version_id IS NULL OR NOT observation_exists
       OR label_row.label_key_hash <> NEW.label_key_hash
       OR label_row.canonical_signal_id <> NEW.canonical_signal_id
       OR label_row.observation_version_id <> NEW.observation_version_id
       OR label_row.candidate_stage_evidence_id <> NEW.candidate_stage_evidence_id
       OR label_row.symbol <> NEW.symbol THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_SNAPSHOT_LABEL_MEMBERSHIP_INVALID';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION app.verify_advisory_dataset_snapshot_closure()
RETURNS TRIGGER AS $$
DECLARE
    target_snapshot_id TEXT := COALESCE(NEW.snapshot_id, OLD.snapshot_id);
    snapshot_row app.advisory_dataset_snapshot%ROWTYPE;
    actual_file_count BIGINT;
    actual_row_count BIGINT;
    actual_total_bytes BIGINT;
    actual_ref_count BIGINT;
    base_row app.advisory_dataset_snapshot%ROWTYPE;
    build_row app.advisory_dataset_build%ROWTYPE;
BEGIN
    SELECT * INTO snapshot_row FROM app.advisory_dataset_snapshot
     WHERE snapshot_id = target_snapshot_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_SNAPSHOT_CLOSURE_INVALID';
    END IF;
    SELECT * INTO build_row FROM app.advisory_dataset_build WHERE build_id = snapshot_row.build_id;
    IF NOT FOUND OR build_row.lifecycle_status <> 'SEALED' OR build_row.checkpoint <> 'SEALED'
       OR build_row.sealed_snapshot_id IS DISTINCT FROM snapshot_row.snapshot_id
       OR build_row.promotion_receipt_hash IS DISTINCT FROM snapshot_row.promotion_receipt_hash
       OR build_row.promoted_manifest_hash IS DISTINCT FROM snapshot_row.manifest_sha256 THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_SNAPSHOT_BUILD_CLOSURE_INVALID';
    END IF;
    SELECT count(*), COALESCE(sum(row_count), 0), COALESCE(sum(size_bytes), 0)
      INTO actual_file_count, actual_row_count, actual_total_bytes
      FROM app.advisory_dataset_snapshot_file WHERE snapshot_id = target_snapshot_id;
    SELECT count(*) INTO actual_ref_count
      FROM app.advisory_dataset_snapshot_blob_ref WHERE snapshot_id = target_snapshot_id;
    IF actual_file_count <> snapshot_row.file_count
       OR actual_row_count <> snapshot_row.row_count
       OR actual_total_bytes <> snapshot_row.total_bytes
       OR actual_ref_count <> actual_file_count THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_SNAPSHOT_CLOSURE_INVALID';
    END IF;
    IF EXISTS (
        SELECT 1 FROM app.advisory_dataset_snapshot_file f
         WHERE f.snapshot_id = target_snapshot_id
           AND NOT EXISTS (
               SELECT 1 FROM app.advisory_dataset_snapshot_blob_ref r
                WHERE r.snapshot_id = f.snapshot_id AND r.logical_path = f.logical_path
                  AND r.logical_role = f.logical_role AND r.partition_key_hash = f.partition_key_hash
                  AND r.ordinal = f.ordinal AND r.store_backend_hash = f.store_backend_hash
                  AND r.blob_sha256 = f.blob_sha256
           )
    ) THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_SNAPSHOT_CLOSURE_INVALID';
    END IF;
    IF snapshot_row.base_snapshot_id IS NOT NULL THEN
        PERFORM pg_advisory_xact_lock_shared(hashtext('advisory_snapshot:' || snapshot_row.base_snapshot_id));
        SELECT * INTO base_row FROM app.advisory_dataset_snapshot
         WHERE snapshot_id = snapshot_row.base_snapshot_id AND snapshot_state = 'SEALED';
        IF NOT FOUND OR base_row.snapshot_content_hash <> snapshot_row.base_snapshot_content_hash
           OR base_row.manifest_sha256 <> snapshot_row.base_manifest_sha256
           OR base_row.policy_compatibility_hash <> snapshot_row.base_policy_compatibility_hash
           OR EXISTS (SELECT 1 FROM app.advisory_dataset_snapshot_invalidation i WHERE i.snapshot_id = base_row.snapshot_id) THEN
            RAISE EXCEPTION 'ADVISORY_PHASE1C3_BASE_SNAPSHOT_INVALID';
        END IF;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION app.verify_advisory_dataset_snapshot_invalidation()
RETURNS TRIGGER AS $$
DECLARE
    authoritative_manifest TEXT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('advisory_snapshot:' || NEW.snapshot_id));
    SELECT manifest_sha256 INTO authoritative_manifest
      FROM app.advisory_dataset_snapshot
     WHERE snapshot_id = NEW.snapshot_id AND snapshot_state = 'SEALED' FOR KEY SHARE;
    IF authoritative_manifest IS DISTINCT FROM NEW.manifest_sha256
       OR NEW.replacement_snapshot_id = NEW.snapshot_id THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_SNAPSHOT_INVALIDATION_INVALID';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION app.reject_advisory_dataset_build_delete()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'ADVISORY_PHASE1C3_APPEND_ONLY';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_dataset_build_transition ON app.advisory_dataset_build;
CREATE TRIGGER trg_verify_advisory_dataset_build_transition
BEFORE UPDATE ON app.advisory_dataset_build FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_dataset_build_transition();
DROP TRIGGER IF EXISTS trg_verify_advisory_dataset_build_predecessor ON app.advisory_dataset_build;
CREATE TRIGGER trg_verify_advisory_dataset_build_predecessor
BEFORE INSERT ON app.advisory_dataset_build FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_dataset_build_predecessor();
DROP TRIGGER IF EXISTS trg_reject_advisory_dataset_build_delete ON app.advisory_dataset_build;
CREATE TRIGGER trg_reject_advisory_dataset_build_delete
BEFORE DELETE ON app.advisory_dataset_build FOR EACH ROW EXECUTE FUNCTION app.reject_advisory_dataset_build_delete();
DROP TRIGGER IF EXISTS trg_verify_advisory_dataset_attempt_transition ON app.advisory_dataset_build_attempt;
CREATE TRIGGER trg_verify_advisory_dataset_attempt_transition
BEFORE UPDATE ON app.advisory_dataset_build_attempt FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_dataset_attempt_transition();
DROP TRIGGER IF EXISTS trg_verify_advisory_dataset_build_attempt_closure_build ON app.advisory_dataset_build;
CREATE CONSTRAINT TRIGGER trg_verify_advisory_dataset_build_attempt_closure_build
AFTER INSERT OR UPDATE ON app.advisory_dataset_build DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_dataset_build_attempt_closure();
DROP TRIGGER IF EXISTS trg_verify_advisory_dataset_build_attempt_closure_attempt ON app.advisory_dataset_build_attempt;
CREATE CONSTRAINT TRIGGER trg_verify_advisory_dataset_build_attempt_closure_attempt
AFTER INSERT OR UPDATE ON app.advisory_dataset_build_attempt DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_dataset_build_attempt_closure();
DROP TRIGGER IF EXISTS trg_verify_advisory_dataset_attempt_file_admission ON app.advisory_dataset_attempt_file;
CREATE TRIGGER trg_verify_advisory_dataset_attempt_file_admission
BEFORE INSERT ON app.advisory_dataset_attempt_file FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_dataset_attempt_file_admission();
DROP TRIGGER IF EXISTS trg_reject_advisory_dataset_attempt_delete ON app.advisory_dataset_build_attempt;
CREATE TRIGGER trg_reject_advisory_dataset_attempt_delete
BEFORE DELETE ON app.advisory_dataset_build_attempt FOR EACH ROW EXECUTE FUNCTION app.reject_advisory_dataset_build_delete();
DROP TRIGGER IF EXISTS trg_verify_advisory_snapshot_label_membership ON app.advisory_dataset_snapshot_label;
CREATE TRIGGER trg_verify_advisory_snapshot_label_membership
BEFORE INSERT ON app.advisory_dataset_snapshot_label FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_snapshot_label_membership();
DROP TRIGGER IF EXISTS trg_verify_advisory_dataset_snapshot_invalidation ON app.advisory_dataset_snapshot_invalidation;
CREATE TRIGGER trg_verify_advisory_dataset_snapshot_invalidation
BEFORE INSERT ON app.advisory_dataset_snapshot_invalidation
FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_dataset_snapshot_invalidation();

DO $$
DECLARE relation_name TEXT;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'advisory_dataset_snapshot', 'advisory_dataset_snapshot_file',
        'advisory_dataset_snapshot_observation', 'advisory_dataset_snapshot_label',
        'advisory_dataset_snapshot_blob_ref'
    ] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON app.%I', 'trg_verify_' || relation_name || '_closure', relation_name);
        EXECUTE format(
            'CREATE CONSTRAINT TRIGGER %I AFTER INSERT ON app.%I DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_dataset_snapshot_closure()',
            'trg_verify_' || relation_name || '_closure', relation_name
        );
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION app.reject_advisory_phase1c3_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'ADVISORY_PHASE1C3_APPEND_ONLY';
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE relation_name TEXT;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'advisory_dataset_blob', 'advisory_outcome_label', 'advisory_outcome_label_payload',
        'advisory_dataset_attempt_file', 'advisory_dataset_build_event', 'advisory_dataset_build_gap',
        'advisory_dataset_snapshot', 'advisory_dataset_snapshot_file',
        'advisory_dataset_snapshot_observation', 'advisory_dataset_snapshot_label',
        'advisory_dataset_snapshot_invalidation', 'advisory_dataset_snapshot_blob_ref'
    ] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON app.%I', 'trg_reject_' || relation_name || '_mutation', relation_name);
        EXECUTE format('CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON app.%I FOR EACH ROW EXECUTE FUNCTION app.reject_advisory_phase1c3_mutation()', 'trg_reject_' || relation_name || '_mutation', relation_name);
    END LOOP;
END;
$$;

CREATE INDEX IF NOT EXISTS ix_advisory_outcome_label_key_revision
    ON app.advisory_outcome_label(label_key_hash, label_revision_no);
CREATE INDEX IF NOT EXISTS ix_advisory_dataset_build_checkpoint
    ON app.advisory_dataset_build(lifecycle_status, checkpoint, logical_build_key_sha256);
CREATE INDEX IF NOT EXISTS ix_advisory_dataset_attempt_lease
    ON app.advisory_dataset_build_attempt(build_id, attempt_state, expires_at);

COMMENT ON TABLE app.advisory_outcome_label IS
    'Append-only global label authority header. It is a research-data invariant, not approval, authorization, or runtime DDL.';
COMMENT ON TABLE app.advisory_dataset_build IS
    'Historical-research build state machine. Legal frozen input advances automatically; no role or manual approval is involved.';

COMMIT;

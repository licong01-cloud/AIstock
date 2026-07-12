-- Phase 1C-1 capture foundation. Apply only through the explicit DEV/release
-- migration workflow; no runtime service is permitted to execute this file.

BEGIN;

CREATE TABLE IF NOT EXISTS app.advisory_capture_batch (
    capture_batch_id TEXT PRIMARY KEY,
    capture_request_hash TEXT NOT NULL,
    request_payload_jsonb JSONB NOT NULL,
    binding_jsonb JSONB NOT NULL,
    control_binding_event_hash TEXT NOT NULL
        REFERENCES app.advisory_phase1_control_binding_event(binding_event_hash),
    handoff_readiness_hash TEXT NOT NULL,
    admission_scope_id TEXT NOT NULL,
    admission_scope_hash TEXT NOT NULL,
    capture_status TEXT NOT NULL CHECK (capture_status IN ('PLANNED', 'RUNNING', 'COMPLETE', 'FAILED', 'EXPIRED', 'ABORTED')),
    row_version BIGINT NOT NULL CHECK (row_version >= 1),
    fencing_token BIGINT NOT NULL CHECK (fencing_token >= 1),
    lease_expires_at TIMESTAMPTZ,
    capture_attempt_no INTEGER NOT NULL CHECK (capture_attempt_no >= 1),
    predecessor_capture_batch_id TEXT REFERENCES app.advisory_capture_batch(capture_batch_id),
    membership_count INTEGER,
    membership_hash TEXT,
    capture_receipt_hash TEXT UNIQUE,
    reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (capture_status = 'RUNNING' AND lease_expires_at IS NOT NULL AND membership_count IS NULL AND membership_hash IS NULL AND capture_receipt_hash IS NULL)
        OR (capture_status = 'COMPLETE' AND lease_expires_at IS NULL AND membership_count IS NOT NULL AND membership_count >= 0 AND membership_hash IS NOT NULL AND capture_receipt_hash IS NOT NULL)
        OR (capture_status IN ('PLANNED', 'FAILED', 'EXPIRED', 'ABORTED') AND lease_expires_at IS NULL AND membership_count IS NULL AND membership_hash IS NULL AND capture_receipt_hash IS NULL)
    ),
    UNIQUE (capture_request_hash, capture_attempt_no),
    UNIQUE (predecessor_capture_batch_id)
);

CREATE TABLE IF NOT EXISTS app.advisory_capture_plan (
    capture_batch_id TEXT NOT NULL REFERENCES app.advisory_capture_batch(capture_batch_id),
    plan_hash TEXT NOT NULL,
    plan_payload_jsonb JSONB NOT NULL,
    selection_run_id TEXT NOT NULL,
    package_id TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    decision_as_of_trade_date DATE NOT NULL,
    stable_signal_semantics_hash TEXT NOT NULL,
    canonical_signal_scope_hash TEXT NOT NULL,
    phase0a_audit_id TEXT NOT NULL,
    phase0a_audit_manifest_hash TEXT NOT NULL,
    handoff_readiness_hash TEXT NOT NULL,
    admission_scope_id TEXT NOT NULL,
    admission_scope_hash TEXT NOT NULL,
    signal_source_revision_set_id TEXT NOT NULL,
    signal_source_revision_set_hash TEXT NOT NULL,
    program_id TEXT NOT NULL,
    binding_version_id TEXT NOT NULL,
    source_run_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (capture_batch_id, plan_hash),
    UNIQUE (capture_batch_id, selection_run_id, package_id, manifest_sha256, decision_as_of_trade_date)
);

CREATE TABLE IF NOT EXISTS app.advisory_capture_batch_evidence_membership (
    capture_batch_id TEXT NOT NULL REFERENCES app.advisory_capture_batch(capture_batch_id),
    evidence_role TEXT NOT NULL,
    evidence_id TEXT NOT NULL,
    evidence_content_hash TEXT NOT NULL,
    fencing_token BIGINT NOT NULL CHECK (fencing_token >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (capture_batch_id, evidence_role, evidence_id)
);

CREATE TABLE IF NOT EXISTS app.advisory_capture_gap (
    capture_gap_id TEXT PRIMARY KEY,
    selection_run_id TEXT NOT NULL,
    package_id TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    decision_as_of_trade_date DATE NOT NULL,
    capture_policy_hash TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    gap_content_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (selection_run_id, package_id, manifest_sha256, decision_as_of_trade_date, capture_policy_hash, reason_code)
);

CREATE TABLE IF NOT EXISTS app.advisory_signal_observation (
    canonical_signal_id TEXT PRIMARY KEY,
    signal_schema_version TEXT NOT NULL CHECK (signal_schema_version = 'advisory_canonical_signal_v1'),
    stable_signal_semantics_hash TEXT NOT NULL,
    canonical_signal_scope_hash TEXT NOT NULL UNIQUE,
    decision_as_of_trade_date DATE NOT NULL,
    selection_as_of_trade_date DATE NOT NULL,
    target_trade_date DATE NOT NULL,
    decision_cutoff_ts TIMESTAMPTZ NOT NULL,
    package_id TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    alpha_mode TEXT NOT NULL CHECK (alpha_mode IN ('single_alpha', 'multi_alpha')),
    selection_runtime_semantics_hash TEXT NOT NULL,
    package_effective_config_hash TEXT NOT NULL,
    calendar_version TEXT NOT NULL,
    calendar_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (selection_as_of_trade_date = decision_as_of_trade_date),
    CHECK (target_trade_date > decision_as_of_trade_date)
);

CREATE TABLE IF NOT EXISTS app.advisory_signal_observation_version (
    observation_version_id TEXT PRIMARY KEY,
    canonical_signal_id TEXT NOT NULL REFERENCES app.advisory_signal_observation(canonical_signal_id),
    observation_schema_version TEXT NOT NULL CHECK (observation_schema_version = 'advisory_signal_observation_version_v1'),
    observation_revision_no INTEGER NOT NULL CHECK (observation_revision_no >= 1),
    supersedes_observation_version_id TEXT UNIQUE REFERENCES app.advisory_signal_observation_version(observation_version_id),
    signal_source_revision_set_id TEXT NOT NULL,
    signal_source_revision_set_hash TEXT NOT NULL,
    phase0a_signal_context_hash TEXT NOT NULL,
    evidence_bundle_hash TEXT NOT NULL,
    stage_evidence_bundle_hash TEXT NOT NULL,
    selection_evidence_id TEXT NOT NULL,
    selection_evidence_hash TEXT NOT NULL,
    selection_run_id TEXT NOT NULL,
    selection_run_content_hash TEXT NOT NULL,
    selection_score_artifact_id TEXT NOT NULL,
    selection_score_artifact_hash TEXT NOT NULL,
    runtime_profile_version_id TEXT NOT NULL,
    runtime_profile_version_hash TEXT NOT NULL,
    hmm_snapshot_id TEXT,
    hmm_snapshot_hash TEXT,
    hmm_snapshot_status TEXT NOT NULL,
    risk_policy_hash TEXT NOT NULL,
    universe_policy_hash TEXT NOT NULL,
    symbol_normalization_policy_hash TEXT NOT NULL,
    valid_no_candidate BOOLEAN NOT NULL,
    observation_status TEXT NOT NULL CHECK (observation_status IN ('COMPLETE', 'PARTIAL', 'CAPTURE_FAILED')),
    evidence_available_at TIMESTAMPTZ NOT NULL,
    observation_content_hash TEXT NOT NULL UNIQUE,
    reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_by_capture_batch_id TEXT NOT NULL REFERENCES app.advisory_capture_batch(capture_batch_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (canonical_signal_id, observation_revision_no),
    CHECK (
        (observation_revision_no = 1 AND supersedes_observation_version_id IS NULL)
        OR (observation_revision_no > 1 AND supersedes_observation_version_id IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS app.advisory_signal_observation_lineage (
    lineage_id TEXT PRIMARY KEY,
    canonical_signal_id TEXT NOT NULL REFERENCES app.advisory_signal_observation(canonical_signal_id),
    observation_version_id TEXT NOT NULL REFERENCES app.advisory_signal_observation_version(observation_version_id),
    phase0a_audit_id TEXT NOT NULL,
    phase0a_audit_manifest_hash TEXT NOT NULL,
    handoff_readiness_hash TEXT NOT NULL,
    admission_scope_id TEXT NOT NULL,
    admission_scope_hash TEXT NOT NULL,
    audit_target_id TEXT NOT NULL,
    target_scope_hash TEXT NOT NULL,
    capability TEXT NOT NULL,
    stable_signal_semantics_hash TEXT NOT NULL,
    canonical_signal_scope_hash TEXT NOT NULL,
    phase0a_signal_context_hash TEXT NOT NULL,
    oos_interval_id TEXT NOT NULL,
    oos_interval_hash TEXT NOT NULL,
    evidence_scope TEXT NOT NULL CHECK (evidence_scope IN ('RETROSPECTIVE_RESEARCH_ONLY', 'GAP_ONLY')),
    signal_evidence_level TEXT NOT NULL,
    effective_cutoff_date DATE NOT NULL,
    program_id TEXT NOT NULL,
    binding_version_id TEXT NOT NULL,
    lineage_source_type TEXT NOT NULL CHECK (lineage_source_type IN ('PHASE0A_AUDIT', 'ONLINE_REVIEW', 'ONLINE_LIST', 'HISTORICAL_REPLAY')),
    source_run_id TEXT NOT NULL,
    review_run_id TEXT,
    list_version_id TEXT,
    lineage_content_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (observation_version_id, phase0a_audit_id, admission_scope_id, program_id, binding_version_id, lineage_source_type, source_run_id)
);

CREATE TABLE IF NOT EXISTS app.advisory_signal_stage_evidence (
    stage_evidence_id TEXT PRIMARY KEY,
    observation_version_id TEXT NOT NULL REFERENCES app.advisory_signal_observation_version(observation_version_id),
    stage TEXT NOT NULL CHECK (stage IN ('alpha_raw', 'hmm_adjusted', 'risk_policy_adjusted', 'selection_effective', 'advisory_model')),
    capability_status TEXT NOT NULL CHECK (capability_status IN ('FULL', 'PARTIAL', 'UNAVAILABLE', 'NOT_APPLICABLE')),
    input_count INTEGER NOT NULL CHECK (input_count >= 0),
    output_count INTEGER NOT NULL CHECK (output_count >= 0),
    excluded_count INTEGER NOT NULL CHECK (excluded_count >= 0),
    observed_max_rank INTEGER,
    source_artifact_id TEXT,
    source_artifact_hash TEXT,
    content_hash TEXT NOT NULL UNIQUE,
    semantic_hash TEXT NOT NULL,
    score_direction TEXT NOT NULL,
    tie_break_policy_id TEXT NOT NULL,
    tie_break_policy_hash TEXT NOT NULL,
    reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (observation_version_id, stage)
);

CREATE TABLE IF NOT EXISTS app.advisory_signal_stage_candidate (
    stage_evidence_id TEXT NOT NULL REFERENCES app.advisory_signal_stage_evidence(stage_evidence_id),
    symbol TEXT NOT NULL,
    membership_status TEXT NOT NULL CHECK (membership_status IN ('INCLUDED', 'EXCLUDED')),
    rank INTEGER,
    score_decimal NUMERIC(38, 12),
    input_rank INTEGER,
    input_score_decimal NUMERIC(38, 12),
    exclusion_reason_code TEXT,
    component_capability TEXT NOT NULL CHECK (component_capability IN ('FULL', 'PARTIAL', 'UNAVAILABLE', 'NOT_APPLICABLE')),
    component_evidence_schema_version TEXT,
    component_evidence_json JSONB,
    component_evidence_hash TEXT,
    component_reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(component_reason_codes) = 'array'),
    candidate_content_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (stage_evidence_id, symbol),
    CHECK ((membership_status = 'INCLUDED' AND rank IS NOT NULL AND score_decimal IS NOT NULL) OR membership_status = 'EXCLUDED'),
    CHECK (
        (component_capability = 'FULL' AND component_evidence_schema_version = 'multi_alpha_component_evidence_v1' AND component_evidence_json IS NOT NULL AND component_evidence_hash IS NOT NULL AND component_reason_codes = '[]'::jsonb)
        OR (component_capability <> 'FULL' AND component_evidence_schema_version IS NULL AND component_evidence_json IS NULL AND component_evidence_hash IS NULL)
    )
);

CREATE OR REPLACE FUNCTION app.verify_advisory_capture_batch_transition()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.capture_status <> 'PLANNED' OR NEW.row_version <> 1 OR NEW.fencing_token <> 1 THEN
            RAISE EXCEPTION 'ADVISORY_PHASE1_CAPTURE_BATCH_INITIAL_STATE_INVALID';
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

DROP TRIGGER IF EXISTS trg_verify_advisory_capture_batch_transition ON app.advisory_capture_batch;
CREATE TRIGGER trg_verify_advisory_capture_batch_transition
    BEFORE INSERT OR UPDATE ON app.advisory_capture_batch
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_capture_batch_transition();

CREATE OR REPLACE FUNCTION app.verify_advisory_capture_membership()
RETURNS TRIGGER AS $$
DECLARE
    batch app.advisory_capture_batch%ROWTYPE;
BEGIN
    SELECT * INTO batch FROM app.advisory_capture_batch
    WHERE capture_batch_id = NEW.capture_batch_id FOR KEY SHARE;
    IF NOT FOUND
       OR batch.capture_status <> 'RUNNING'
       OR batch.fencing_token <> NEW.fencing_token
       OR batch.lease_expires_at IS NULL
       OR batch.lease_expires_at <= clock_timestamp() THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1_CAPTURE_MEMBERSHIP_ADMISSION_INVALID';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION app.verify_advisory_signal_calendar_adjacency()
RETURNS TRIGGER AS $$
DECLARE
    expected_target_trade_date DATE;
BEGIN
    SELECT MIN(cal_date) INTO expected_target_trade_date
    FROM market.trading_calendar
    WHERE is_trading = TRUE AND cal_date > NEW.decision_as_of_trade_date;
    IF expected_target_trade_date IS NULL OR NEW.target_trade_date <> expected_target_trade_date THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1_OBSERVATION_CALENDAR_INVALID';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_signal_calendar_adjacency ON app.advisory_signal_observation;
CREATE TRIGGER trg_verify_advisory_signal_calendar_adjacency
    BEFORE INSERT ON app.advisory_signal_observation
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_signal_calendar_adjacency();

CREATE OR REPLACE FUNCTION app.verify_advisory_observation_revision_chain()
RETURNS TRIGGER AS $$
DECLARE
    predecessor_signal_id TEXT;
    predecessor_revision_no INTEGER;
BEGIN
    IF NEW.observation_revision_no = 1 THEN
        RETURN NEW;
    END IF;
    SELECT canonical_signal_id, observation_revision_no
      INTO predecessor_signal_id, predecessor_revision_no
      FROM app.advisory_signal_observation_version
     WHERE observation_version_id = NEW.supersedes_observation_version_id
     FOR KEY SHARE;
    IF NOT FOUND
       OR predecessor_signal_id <> NEW.canonical_signal_id
       OR predecessor_revision_no <> NEW.observation_revision_no - 1 THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1_OBSERVATION_REVISION_CHAIN_INVALID';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_observation_revision_chain ON app.advisory_signal_observation_version;
CREATE TRIGGER trg_verify_advisory_observation_revision_chain
    BEFORE INSERT ON app.advisory_signal_observation_version
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_observation_revision_chain();

DROP TRIGGER IF EXISTS trg_verify_advisory_capture_membership ON app.advisory_capture_batch_evidence_membership;
CREATE TRIGGER trg_verify_advisory_capture_membership
    BEFORE INSERT ON app.advisory_capture_batch_evidence_membership
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_capture_membership();

CREATE OR REPLACE FUNCTION app.reject_advisory_phase1_capture_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'ADVISORY_PHASE1_CAPTURE_IMMUTABLE';
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    relation_name TEXT;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'advisory_capture_plan',
        'advisory_capture_batch_evidence_membership',
        'advisory_capture_gap',
        'advisory_signal_observation',
        'advisory_signal_observation_version',
        'advisory_signal_observation_lineage',
        'advisory_signal_stage_evidence',
        'advisory_signal_stage_candidate'
    ] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON app.%I', 'trg_reject_' || relation_name || '_mutation', relation_name);
        EXECUTE format(
            'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON app.%I FOR EACH ROW EXECUTE FUNCTION app.reject_advisory_phase1_capture_mutation()',
            'trg_reject_' || relation_name || '_mutation',
            relation_name
        );
    END LOOP;
END;
$$;

COMMENT ON TABLE app.advisory_capture_batch IS
    'Phase 1C mutable control header for a historical research capture state machine. It is not an approval, authorization, or runtime DDL mechanism.';
COMMENT ON TABLE app.advisory_capture_plan IS
    'Immutable explicit Phase 0A canonical identity plan. Capture writers must not fill its fields from latest records.';
COMMENT ON TABLE app.advisory_capture_batch_evidence_membership IS
    'Append-only frozen evidence membership admitted only by the current RUNNING capture batch lease and fencing token.';
COMMENT ON TABLE app.advisory_capture_gap IS
    'Append-only evidence of a missing historical trace capture. TRACE_CAPTURE_LOST is distinct from a delivery write failure.';
COMMENT ON TABLE app.advisory_signal_observation IS
    'Stable canonical historical signal header. Evidence revisions are stored separately and never overwrite this economic identity.';
COMMENT ON TABLE app.advisory_signal_observation_version IS
    'Append-only immutable evidence revision for one canonical historical signal.';
COMMENT ON TABLE app.advisory_signal_observation_lineage IS
    'Append-only Phase 0A and Program lineage for one immutable observation version.';
COMMENT ON TABLE app.advisory_signal_stage_evidence IS
    'Immutable per-stage rank and score summary captured from a copied historical trace envelope.';
COMMENT ON TABLE app.advisory_signal_stage_candidate IS
    'Immutable per-symbol candidate or exclusion evidence for one captured selection stage.';

DO $$
DECLARE
    column_record RECORD;
BEGIN
    FOR column_record IN
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = 'app'
          AND table_name = ANY (ARRAY[
              'advisory_capture_batch',
              'advisory_capture_plan',
              'advisory_capture_batch_evidence_membership',
              'advisory_capture_gap',
              'advisory_signal_observation',
              'advisory_signal_observation_version',
              'advisory_signal_observation_lineage',
              'advisory_signal_stage_evidence',
              'advisory_signal_stage_candidate'
          ])
    LOOP
        EXECUTE format(
            'COMMENT ON COLUMN app.%I.%I IS %L',
            column_record.table_name,
            column_record.column_name,
            'Phase 1C capture-foundation field. Its historical-research source, immutable identity, quality constraints, and lifecycle semantics are defined by advisory_phase1c_capture_foundation_f2_design_20260713.'
        );
    END LOOP;
END;
$$;

COMMIT;

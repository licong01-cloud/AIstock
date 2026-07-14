BEGIN;

CREATE TABLE IF NOT EXISTS app.advisory_signal_observation_lineage_identity (
    lineage_id TEXT PRIMARY KEY,
    decision_as_of_trade_date DATE NOT NULL,
    observation_version_id TEXT NOT NULL REFERENCES app.advisory_signal_observation_version(observation_version_id),
    phase0a_audit_id TEXT NOT NULL,
    admission_scope_id TEXT NOT NULL,
    program_id TEXT NOT NULL,
    binding_version_id TEXT NOT NULL,
    lineage_source_type TEXT NOT NULL CHECK (lineage_source_type IN ('PHASE0A_AUDIT', 'ONLINE_REVIEW', 'ONLINE_LIST', 'HISTORICAL_REPLAY')),
    source_run_id TEXT NOT NULL,
    lineage_content_hash TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (lineage_id, decision_as_of_trade_date),
    UNIQUE (observation_version_id, phase0a_audit_id, admission_scope_id, program_id, binding_version_id, lineage_source_type, source_run_id)
);

CREATE TABLE IF NOT EXISTS app.advisory_signal_observation_lineage_payload (
    decision_as_of_trade_date DATE NOT NULL,
    lineage_id TEXT NOT NULL,
    canonical_signal_id TEXT NOT NULL REFERENCES app.advisory_signal_observation(canonical_signal_id),
    phase0a_audit_manifest_hash TEXT NOT NULL,
    handoff_readiness_hash TEXT NOT NULL,
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
    review_run_id TEXT,
    list_version_id TEXT,
    PRIMARY KEY (decision_as_of_trade_date, lineage_id),
    FOREIGN KEY (lineage_id, decision_as_of_trade_date)
        REFERENCES app.advisory_signal_observation_lineage_identity(lineage_id, decision_as_of_trade_date)
) PARTITION BY RANGE (decision_as_of_trade_date);

CREATE TABLE IF NOT EXISTS app.advisory_signal_stage_candidate_identity (
    stage_evidence_id TEXT NOT NULL REFERENCES app.advisory_signal_stage_evidence(stage_evidence_id),
    symbol TEXT NOT NULL,
    decision_as_of_trade_date DATE NOT NULL,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (stage_evidence_id, symbol),
    UNIQUE (stage_evidence_id, symbol, decision_as_of_trade_date)
);

CREATE TABLE IF NOT EXISTS app.advisory_signal_stage_candidate_payload (
    decision_as_of_trade_date DATE NOT NULL,
    stage_evidence_id TEXT NOT NULL,
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
    candidate_content_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (decision_as_of_trade_date, stage_evidence_id, symbol),
    FOREIGN KEY (stage_evidence_id, symbol, decision_as_of_trade_date)
        REFERENCES app.advisory_signal_stage_candidate_identity(stage_evidence_id, symbol, decision_as_of_trade_date),
    CHECK ((membership_status = 'INCLUDED' AND rank IS NOT NULL AND score_decimal IS NOT NULL) OR membership_status = 'EXCLUDED'),
    CHECK (
        (component_capability = 'FULL' AND component_evidence_schema_version = 'multi_alpha_component_evidence_v1' AND component_evidence_json IS NOT NULL AND component_evidence_hash IS NOT NULL AND component_reason_codes = '[]'::jsonb)
        OR (component_capability <> 'FULL' AND component_evidence_schema_version IS NULL AND component_evidence_json IS NULL AND component_evidence_hash IS NULL)
    )
) PARTITION BY RANGE (decision_as_of_trade_date);

CREATE INDEX IF NOT EXISTS idx_adv_p1f1_stage_candidate_payload_content_hash
    ON app.advisory_signal_stage_candidate_payload (candidate_content_hash);

CREATE OR REPLACE FUNCTION app.verify_advisory_phase1f1_lineage_identity_date()
RETURNS TRIGGER AS $$
DECLARE
    resolved_date DATE;
BEGIN
    SELECT observation.decision_as_of_trade_date
      INTO resolved_date
      FROM app.advisory_signal_observation_version observation_version
      JOIN app.advisory_signal_observation observation
        ON observation.canonical_signal_id = observation_version.canonical_signal_id
     WHERE observation_version.observation_version_id = NEW.observation_version_id
     FOR KEY SHARE;
    IF NOT FOUND OR NEW.decision_as_of_trade_date <> resolved_date THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1F1_LINEAGE_DECISION_DATE_INVALID';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION app.verify_advisory_phase1f1_stage_candidate_identity_date()
RETURNS TRIGGER AS $$
DECLARE
    resolved_date DATE;
BEGIN
    SELECT observation.decision_as_of_trade_date
      INTO resolved_date
      FROM app.advisory_signal_stage_evidence stage_evidence
      JOIN app.advisory_signal_observation_version observation_version
        ON observation_version.observation_version_id = stage_evidence.observation_version_id
      JOIN app.advisory_signal_observation observation
        ON observation.canonical_signal_id = observation_version.canonical_signal_id
     WHERE stage_evidence.stage_evidence_id = NEW.stage_evidence_id
     FOR KEY SHARE;
    IF NOT FOUND OR NEW.decision_as_of_trade_date <> resolved_date THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1F1_CANDIDATE_DECISION_DATE_INVALID';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_adv_p1f1_lineage_identity_date ON app.advisory_signal_observation_lineage_identity;
CREATE TRIGGER trg_adv_p1f1_lineage_identity_date
    BEFORE INSERT ON app.advisory_signal_observation_lineage_identity
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_phase1f1_lineage_identity_date();

DROP TRIGGER IF EXISTS trg_adv_p1f1_candidate_identity_date ON app.advisory_signal_stage_candidate_identity;
CREATE TRIGGER trg_adv_p1f1_candidate_identity_date
    BEFORE INSERT ON app.advisory_signal_stage_candidate_identity
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_phase1f1_stage_candidate_identity_date();

DROP TRIGGER IF EXISTS trg_adv_p1f1_lineage_identity_immutable ON app.advisory_signal_observation_lineage_identity;
CREATE TRIGGER trg_adv_p1f1_lineage_identity_immutable
    BEFORE UPDATE OR DELETE ON app.advisory_signal_observation_lineage_identity
    FOR EACH ROW EXECUTE FUNCTION app.reject_advisory_phase1_capture_mutation();

DROP TRIGGER IF EXISTS trg_adv_p1f1_lineage_payload_immutable ON app.advisory_signal_observation_lineage_payload;
CREATE TRIGGER trg_adv_p1f1_lineage_payload_immutable
    BEFORE UPDATE OR DELETE ON app.advisory_signal_observation_lineage_payload
    FOR EACH ROW EXECUTE FUNCTION app.reject_advisory_phase1_capture_mutation();

DROP TRIGGER IF EXISTS trg_adv_p1f1_candidate_identity_immutable ON app.advisory_signal_stage_candidate_identity;
CREATE TRIGGER trg_adv_p1f1_candidate_identity_immutable
    BEFORE UPDATE OR DELETE ON app.advisory_signal_stage_candidate_identity
    FOR EACH ROW EXECUTE FUNCTION app.reject_advisory_phase1_capture_mutation();

DROP TRIGGER IF EXISTS trg_adv_p1f1_candidate_payload_immutable ON app.advisory_signal_stage_candidate_payload;
CREATE TRIGGER trg_adv_p1f1_candidate_payload_immutable
    BEFORE UPDATE OR DELETE ON app.advisory_signal_stage_candidate_payload
    FOR EACH ROW EXECUTE FUNCTION app.reject_advisory_phase1_capture_mutation();

COMMENT ON TABLE app.advisory_signal_observation_lineage_identity IS
    'Phase 1F.1 global lineage identity. Payload is range partitioned by canonical decision month.';
COMMENT ON TABLE app.advisory_signal_observation_lineage_payload IS
    'Phase 1F.1 lineage payload, range partitioned by decision_as_of_trade_date. No default partition.';
COMMENT ON TABLE app.advisory_signal_stage_candidate_identity IS
    'Phase 1F.1 global stage-candidate identity. Payload is range partitioned by canonical decision month.';
COMMENT ON TABLE app.advisory_signal_stage_candidate_payload IS
    'Phase 1F.1 stage-candidate payload, range partitioned by decision_as_of_trade_date. No default partition.';

COMMIT;

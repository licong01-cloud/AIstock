-- Phase 0A.1 authority bootstrap. Apply only through the separately approved
-- production_authority_bootstrap_ddl_gate; this migration never writes business data.

CREATE SCHEMA IF NOT EXISTS app;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'aistock_advisory_phase0a_approver') THEN
        CREATE ROLE aistock_advisory_phase0a_approver NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'aistock_advisory_phase0a_finalizer') THEN
        CREATE ROLE aistock_advisory_phase0a_finalizer NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'aistock_advisory_phase1_operation_authorizer') THEN
        CREATE ROLE aistock_advisory_phase1_operation_authorizer NOLOGIN;
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS app.advisory_phase0a_approval_event (
    decision_id TEXT PRIMARY KEY,
    decision_kind TEXT NOT NULL CHECK (decision_kind IN ('GLOBAL', 'ADMISSION_SCOPE')),
    event_type TEXT NOT NULL CHECK (event_type IN ('APPROVE', 'REJECT', 'REVOKE')),
    audit_id TEXT NOT NULL,
    audit_manifest_hash TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    initial_approval_receipt_hash TEXT NOT NULL,
    phase1_handoff_bundle_hash TEXT NOT NULL,
    audit_target_id TEXT NULL,
    target_handoff_hash TEXT NULL,
    admission_scope_id TEXT NULL,
    admission_scope_hash TEXT NULL,
    previous_terminal_decision_hash TEXT NULL,
    revokes_decision_hash TEXT NULL,
    decision_hash TEXT NOT NULL UNIQUE,
    actor_principal TEXT NOT NULL,
    authority_backend_id TEXT NOT NULL,
    decision_at TIMESTAMPTZ NOT NULL,
    payload_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (
        (decision_kind = 'GLOBAL' AND admission_scope_id IS NULL AND admission_scope_hash IS NULL)
        OR (decision_kind = 'ADMISSION_SCOPE' AND admission_scope_id IS NOT NULL AND admission_scope_hash IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_advisory_phase0a_approval_event_successor
    ON app.advisory_phase0a_approval_event (
        audit_id,
        phase1_handoff_bundle_hash,
        decision_kind,
        COALESCE(admission_scope_id, '__GLOBAL__'),
        previous_terminal_decision_hash
    )
    WHERE previous_terminal_decision_hash IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_advisory_phase0a_approval_event_revocation
    ON app.advisory_phase0a_approval_event (
        audit_id,
        phase1_handoff_bundle_hash,
        decision_kind,
        COALESCE(admission_scope_id, '__GLOBAL__'),
        revokes_decision_hash
    )
    WHERE revokes_decision_hash IS NOT NULL;

CREATE TABLE IF NOT EXISTS app.advisory_phase0a_approval_bundle (
    approval_bundle_id TEXT PRIMARY KEY,
    schema_version TEXT NOT NULL,
    audit_id TEXT NOT NULL,
    audit_manifest_hash TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    initial_approval_receipt_hash TEXT NOT NULL,
    phase1_handoff_bundle_hash TEXT NOT NULL,
    global_terminal_decision_hash TEXT NOT NULL,
    admission_scope_set_hash TEXT NOT NULL,
    scope_member_count INTEGER NOT NULL CHECK (scope_member_count >= 0),
    authority_backend_id TEXT NOT NULL,
    authority_backend_hash TEXT NOT NULL,
    approval_bundle_content_hash TEXT NOT NULL UNIQUE,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    payload_json JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS app.advisory_phase0a_approval_bundle_scope (
    approval_bundle_id TEXT NOT NULL REFERENCES app.advisory_phase0a_approval_bundle(approval_bundle_id),
    admission_scope_id TEXT NOT NULL,
    admission_scope_hash TEXT NOT NULL,
    terminal_decision_hash TEXT NOT NULL,
    allowed_evidence_scope TEXT NOT NULL CHECK (
        allowed_evidence_scope IN ('FORMAL_OOS', 'RETROSPECTIVE_RESEARCH_ONLY', 'GAP_ONLY')
    ),
    scope_member_content_hash TEXT NOT NULL UNIQUE,
    PRIMARY KEY (approval_bundle_id, admission_scope_id)
);

CREATE TABLE IF NOT EXISTS app.advisory_phase1_operation_authorization_event (
    authorization_id TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type IN ('AUTHORIZE', 'REVOKE')),
    operation_type TEXT NOT NULL,
    environment TEXT NOT NULL,
    approval_bundle_hash TEXT NULL,
    admission_scope_set_hash TEXT NULL,
    governance_scope_hash TEXT NULL,
    operation_payload_hash TEXT NOT NULL,
    max_rows BIGINT NULL CHECK (max_rows IS NULL OR max_rows >= 0),
    max_bytes BIGINT NULL CHECK (max_bytes IS NULL OR max_bytes >= 0),
    valid_from TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    previous_event_hash TEXT NULL,
    revokes_event_hash TEXT NULL,
    actor_principal TEXT NOT NULL,
    event_at TIMESTAMPTZ NOT NULL,
    approval_reference TEXT NOT NULL,
    authorization_event_hash TEXT PRIMARY KEY,
    payload_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (expires_at > valid_from)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_advisory_phase1_operation_authorization_successor
    ON app.advisory_phase1_operation_authorization_event (authorization_id, previous_event_hash)
    WHERE previous_event_hash IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_advisory_phase1_operation_authorization_revocation
    ON app.advisory_phase1_operation_authorization_event (authorization_id, revokes_event_hash)
    WHERE revokes_event_hash IS NOT NULL;

CREATE OR REPLACE FUNCTION app.reject_advisory_phase0a_authority_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'Phase 0A.1 authority evidence is append-only';
END;
$$;

DROP TRIGGER IF EXISTS trg_advisory_phase0a_approval_event_append_only ON app.advisory_phase0a_approval_event;
CREATE TRIGGER trg_advisory_phase0a_approval_event_append_only
BEFORE UPDATE OR DELETE ON app.advisory_phase0a_approval_event
FOR EACH ROW EXECUTE FUNCTION app.reject_advisory_phase0a_authority_mutation();

DROP TRIGGER IF EXISTS trg_advisory_phase0a_approval_bundle_append_only ON app.advisory_phase0a_approval_bundle;
CREATE TRIGGER trg_advisory_phase0a_approval_bundle_append_only
BEFORE UPDATE OR DELETE ON app.advisory_phase0a_approval_bundle
FOR EACH ROW EXECUTE FUNCTION app.reject_advisory_phase0a_authority_mutation();

DROP TRIGGER IF EXISTS trg_advisory_phase0a_approval_bundle_scope_append_only ON app.advisory_phase0a_approval_bundle_scope;
CREATE TRIGGER trg_advisory_phase0a_approval_bundle_scope_append_only
BEFORE UPDATE OR DELETE ON app.advisory_phase0a_approval_bundle_scope
FOR EACH ROW EXECUTE FUNCTION app.reject_advisory_phase0a_authority_mutation();

DROP TRIGGER IF EXISTS trg_advisory_phase1_operation_authorization_append_only ON app.advisory_phase1_operation_authorization_event;
CREATE TRIGGER trg_advisory_phase1_operation_authorization_append_only
BEFORE UPDATE OR DELETE ON app.advisory_phase1_operation_authorization_event
FOR EACH ROW EXECUTE FUNCTION app.reject_advisory_phase0a_authority_mutation();

GRANT USAGE ON SCHEMA app TO aistock_advisory_phase0a_approver, aistock_advisory_phase0a_finalizer, aistock_advisory_phase1_operation_authorizer;
GRANT SELECT, INSERT ON app.advisory_phase0a_approval_event TO aistock_advisory_phase0a_approver;
GRANT SELECT, INSERT ON app.advisory_phase0a_approval_bundle, app.advisory_phase0a_approval_bundle_scope TO aistock_advisory_phase0a_finalizer;
GRANT SELECT, INSERT ON app.advisory_phase1_operation_authorization_event TO aistock_advisory_phase1_operation_authorizer;

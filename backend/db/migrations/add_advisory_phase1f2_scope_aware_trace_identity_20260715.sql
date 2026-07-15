ALTER TABLE app.advisory_capture_gap
    ADD COLUMN admission_scope_id TEXT;

ALTER TABLE app.advisory_capture_gap
    ADD COLUMN admission_scope_hash TEXT;

ALTER TABLE app.advisory_capture_gap
    ADD CONSTRAINT ck_advisory_capture_gap_scope_pair
    CHECK (
        (admission_scope_id IS NULL AND admission_scope_hash IS NULL)
        OR (admission_scope_id IS NOT NULL AND admission_scope_hash IS NOT NULL)
    ) NOT VALID;

ALTER TABLE app.advisory_capture_gap
    VALIDATE CONSTRAINT ck_advisory_capture_gap_scope_pair;

ALTER TABLE app.advisory_capture_gap
    DROP CONSTRAINT advisory_capture_gap_selection_run_id_package_id_manifest_s_key;

CREATE UNIQUE INDEX ux_advisory_capture_gap_legacy_identity
    ON app.advisory_capture_gap (
        selection_run_id,
        package_id,
        manifest_sha256,
        decision_as_of_trade_date,
        capture_policy_hash,
        reason_code
    )
    WHERE admission_scope_hash IS NULL;

CREATE UNIQUE INDEX ux_advisory_capture_gap_scope_v2_identity
    ON app.advisory_capture_gap (
        selection_run_id,
        package_id,
        manifest_sha256,
        decision_as_of_trade_date,
        capture_policy_hash,
        admission_scope_hash,
        reason_code
    )
    WHERE admission_scope_hash IS NOT NULL;

ALTER TABLE app.advisory_selection_stage_trace_outbox
    DROP CONSTRAINT advisory_selection_stage_trac_selection_run_id_package_id_m_key;

ALTER TABLE app.advisory_selection_stage_trace_outbox
    ADD CONSTRAINT uq_advisory_stage_trace_outbox_scope_identity
    UNIQUE (
        selection_run_id,
        package_id,
        manifest_sha256,
        decision_as_of_trade_date,
        capture_policy_hash,
        admission_scope_hash
    );

COMMENT ON TABLE app.advisory_capture_gap IS
    'Append-only Phase 1 trace capture gap evidence. NULL scope is reserved for immutable pre-v2 legacy rows; new writes are scope-aware.';
COMMENT ON COLUMN app.advisory_capture_gap.admission_scope_id IS
    'Phase 0A handoff admission scope identity. NULL is reserved for immutable pre-v2 legacy rows.';
COMMENT ON COLUMN app.advisory_capture_gap.admission_scope_hash IS
    'Canonical Phase 0A handoff admission scope hash. NULL is reserved for immutable pre-v2 legacy rows.';
COMMENT ON TABLE app.advisory_selection_stage_trace_outbox IS
    'Append-only Phase 1 trace envelope outbox. Natural identity includes admission scope; writes never modify Selection, Advisory, simulation, Paper or market-source rows.';

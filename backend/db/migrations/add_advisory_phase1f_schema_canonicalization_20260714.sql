-- Phase 1F canonicalization: converge legacy/fresh variants without business-row DML.

ALTER TABLE app.advisory_source_availability_event
    DROP CONSTRAINT IF EXISTS advisory_source_availability_event_check;
ALTER TABLE app.advisory_source_availability_event
    DROP CONSTRAINT IF EXISTS advisory_source_availability_event_check1;
ALTER TABLE app.advisory_source_availability_event
    DROP CONSTRAINT IF EXISTS advisory_source_availability_event_check2;
ALTER TABLE app.advisory_source_availability_event
    ADD CONSTRAINT advisory_source_availability_event_check
    CHECK (formal_available_at >= first_observed_at);
ALTER TABLE app.advisory_source_availability_event
    ADD CONSTRAINT advisory_source_availability_event_check1
    CHECK (event_content_hash <> predecessor_event_hash);
ALTER TABLE app.advisory_source_availability_event
    ADD CONSTRAINT advisory_source_availability_event_check2
    CHECK (
        (event_revision_no = 1 AND event_type = 'INGESTED' AND predecessor_event_hash IS NULL)
        OR (event_revision_no > 1 AND predecessor_event_hash IS NOT NULL)
    );

ALTER TABLE app.advisory_source_revision_member
    DROP CONSTRAINT IF EXISTS advisory_source_revision_member_check2;
ALTER TABLE app.advisory_source_revision_member
    ADD CONSTRAINT advisory_source_revision_member_check2
    CHECK (availability_event_hash IS NOT NULL OR research_only IS TRUE);

COMMENT ON COLUMN app.advisory_capture_batch.capture_request_schema_version IS
    'Phase 1C capture-foundation field. Its historical-research source, immutable identity, quality constraints, and lifecycle semantics are defined by advisory_phase1c_capture_foundation_f2_design_20260713.';
COMMENT ON COLUMN app.advisory_capture_batch.capture_purpose IS
    'Phase 1C capture-foundation field. Its historical-research source, immutable identity, quality constraints, and lifecycle semantics are defined by advisory_phase1c_capture_foundation_f2_design_20260713.';

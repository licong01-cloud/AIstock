-- DEV/test rollback only. Refuse to remove any persisted Batch C evidence.

BEGIN;

DO $$
DECLARE relation_name TEXT; row_count BIGINT;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'advisory_dataset_snapshot_blob_ref', 'advisory_dataset_snapshot_invalidation',
        'advisory_dataset_snapshot_label', 'advisory_dataset_snapshot_observation',
        'advisory_dataset_snapshot_file', 'advisory_dataset_snapshot', 'advisory_dataset_build_gap',
        'advisory_dataset_build_event', 'advisory_dataset_attempt_file', 'advisory_dataset_build_attempt',
        'advisory_dataset_build', 'advisory_outcome_label_payload', 'advisory_outcome_label', 'advisory_dataset_blob'
    ] LOOP
        IF to_regclass('app.' || relation_name) IS NOT NULL THEN
            EXECUTE format('SELECT count(*) FROM app.%I', relation_name) INTO row_count;
            IF row_count <> 0 THEN
                RAISE EXCEPTION 'ADVISORY_PHASE1C3_ROLLBACK_EVIDENCE_NOT_EMPTY: % (%)', relation_name, row_count;
            END IF;
        END IF;
    END LOOP;
    IF EXISTS (SELECT 1 FROM app.advisory_capture_batch WHERE capture_request_schema_version = 'advisory_phase1_capture_batch_v2') THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_ROLLBACK_EVIDENCE_NOT_EMPTY: v2 capture batches exist';
    END IF;
END;
$$;

DROP TABLE IF EXISTS app.advisory_dataset_snapshot_blob_ref;
DROP TABLE IF EXISTS app.advisory_dataset_snapshot_invalidation;
DROP TABLE IF EXISTS app.advisory_dataset_snapshot_label;
DROP TABLE IF EXISTS app.advisory_dataset_snapshot_observation;
DROP TABLE IF EXISTS app.advisory_dataset_snapshot_file;
DROP TABLE IF EXISTS app.advisory_dataset_snapshot;
DROP TABLE IF EXISTS app.advisory_dataset_build_gap;
DROP TABLE IF EXISTS app.advisory_dataset_build_event;
DROP TABLE IF EXISTS app.advisory_dataset_attempt_file;
DROP TABLE IF EXISTS app.advisory_dataset_build_attempt;
DROP TABLE IF EXISTS app.advisory_dataset_build;
DROP TABLE IF EXISTS app.advisory_outcome_label_payload_202608;
DROP TABLE IF EXISTS app.advisory_outcome_label_payload_202607;
DROP TABLE IF EXISTS app.advisory_outcome_label_payload_202606;
DROP TABLE IF EXISTS app.advisory_outcome_label_payload;
DROP TABLE IF EXISTS app.advisory_outcome_label;
DROP TABLE IF EXISTS app.advisory_dataset_blob;

DROP FUNCTION IF EXISTS app.verify_advisory_outcome_label_closure();
DROP FUNCTION IF EXISTS app.verify_advisory_outcome_label_predecessor();
DROP FUNCTION IF EXISTS app.verify_advisory_outcome_label_owner();
DROP FUNCTION IF EXISTS app.verify_advisory_dataset_build_transition();
DROP FUNCTION IF EXISTS app.verify_advisory_dataset_attempt_transition();
DROP FUNCTION IF EXISTS app.verify_advisory_dataset_attempt_file_admission();
DROP FUNCTION IF EXISTS app.verify_advisory_dataset_build_attempt_closure();
DROP FUNCTION IF EXISTS app.verify_advisory_dataset_build_predecessor();
DROP FUNCTION IF EXISTS app.verify_advisory_snapshot_label_membership();
DROP FUNCTION IF EXISTS app.verify_advisory_dataset_snapshot_closure();
DROP FUNCTION IF EXISTS app.verify_advisory_dataset_snapshot_invalidation();
DROP FUNCTION IF EXISTS app.reject_advisory_dataset_build_delete();
DROP TRIGGER IF EXISTS trg_verify_advisory_capture_plan_closure_batch ON app.advisory_capture_batch;
DROP TRIGGER IF EXISTS trg_verify_advisory_capture_plan_closure_plan ON app.advisory_capture_plan;
DROP FUNCTION IF EXISTS app.verify_advisory_capture_plan_closure();
DROP FUNCTION IF EXISTS app.reject_advisory_phase1c3_mutation();

ALTER TABLE app.advisory_capture_batch
    DROP CONSTRAINT IF EXISTS advisory_capture_batch_schema_purpose_check,
    DROP COLUMN IF EXISTS capture_purpose,
    DROP COLUMN IF EXISTS capture_request_schema_version;

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

COMMIT;

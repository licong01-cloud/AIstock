-- Phase 1R R4 outcome, summary, and retrospective dataset bridge.
-- Additive/forward-only. Apply and verify in the existing DEV database first.
-- Runtime services must never execute this migration.

BEGIN;

CREATE OR REPLACE FUNCTION app.advisory_historical_range_artifact_ref_is_valid(
    value JSONB,
    expected_kind TEXT,
    expected_hash TEXT
)
RETURNS BOOLEAN AS $$
DECLARE
    namespace TEXT;
BEGIN
    namespace := CASE expected_kind
        WHEN 'SOURCE_REQUIREMENT_PLAN' THEN 'source-requirement-plans'
        WHEN 'SOURCE_CATALOG_CHECKPOINT' THEN 'source-catalog-checkpoints'
        WHEN 'HMM_BINDING_SET' THEN 'hmm-binding-sets'
        WHEN 'REQUEST' THEN 'requests'
        WHEN 'DATE_PLAN' THEN 'date-plans'
        WHEN 'FROZEN_PROGRAM' THEN 'frozen-programs'
        WHEN 'CANDIDATE_ARTIFACT' THEN 'candidate-artifacts'
        WHEN 'DECISION_MARK_SET' THEN 'decision-mark-sets'
        WHEN 'DAY_RECEIPT' THEN 'day-receipts'
        WHEN 'RANGE_RECEIPT' THEN 'range-receipts'
        WHEN 'OUTCOME_REFRESH_RECEIPT' THEN 'outcome-refresh-receipts'
        WHEN 'DATASET_BRIDGE_RECEIPT' THEN 'dataset-bridge-receipts'
        WHEN 'OUTCOME' THEN 'outcomes'
        WHEN 'SUMMARY' THEN 'summaries'
        WHEN 'DATASET_BRIDGE' THEN 'dataset-bridges'
        ELSE NULL
    END;
    RETURN namespace IS NOT NULL
       AND jsonb_typeof(value) = 'object'
       AND value->>'schema_version' = 'advisory_historical_range_artifact_ref_v1'
       AND value->>'artifact_kind' = expected_kind
       AND COALESCE(value->>'relative_path', '') <> ''
       AND COALESCE(value->>'producer_contract_version', '') <> ''
       AND COALESCE(value->>'payload_schema_version', '') <> ''
       AND app.advisory_historical_range_is_sha256(value->>'semantic_content_hash')
       AND app.advisory_historical_range_is_sha256(value->>'payload_sha256')
       AND app.advisory_historical_range_is_sha256(value->>'file_sha256')
       AND value->>'relative_path' = namespace || '/' || (value->>'semantic_content_hash') || '.json'
       AND (expected_hash IS NULL OR value->>'semantic_content_hash' = expected_hash);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

ALTER TABLE app.advisory_historical_range_outcome
    ADD COLUMN IF NOT EXISTS evaluation_window_type TEXT,
    ADD COLUMN IF NOT EXISTS historical_range_policy_bundle_hash TEXT,
    ADD COLUMN IF NOT EXISTS outcome_input_hash TEXT,
    ADD COLUMN IF NOT EXISTS revision_reason TEXT,
    ADD COLUMN IF NOT EXISTS producer_code_hash TEXT,
    ADD COLUMN IF NOT EXISTS outcome_contract_version TEXT,
    ADD COLUMN IF NOT EXISTS revision_evidence_ref JSONB,
    ADD COLUMN IF NOT EXISTS revision_evidence_hash TEXT;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM app.advisory_historical_range_outcome
        WHERE evaluation_window_type IS NULL
           OR historical_range_policy_bundle_hash IS NULL
           OR outcome_input_hash IS NULL
           OR revision_reason IS NULL
           OR producer_code_hash IS NULL
           OR outcome_contract_version IS NULL
    ) THEN
        RAISE EXCEPTION 'ADVISORY_HR_R4_OUTCOME_EXPLICIT_BACKFILL_REQUIRED';
    END IF;
END;
$$;

ALTER TABLE app.advisory_historical_range_outcome
    ALTER COLUMN evaluation_window_type SET NOT NULL,
    ALTER COLUMN historical_range_policy_bundle_hash SET NOT NULL,
    ALTER COLUMN outcome_input_hash SET NOT NULL,
    ALTER COLUMN revision_reason SET NOT NULL,
    ALTER COLUMN producer_code_hash SET NOT NULL,
    ALTER COLUMN outcome_contract_version SET NOT NULL;

DO $$
DECLARE
    item RECORD;
BEGIN
    FOR item IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'app.advisory_historical_range_outcome'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%horizon_trade_days >= 1%'
    LOOP
        EXECUTE format('ALTER TABLE app.advisory_historical_range_outcome DROP CONSTRAINT %I', item.conname);
    END LOOP;
END;
$$;

ALTER TABLE app.advisory_historical_range_outcome
    DROP CONSTRAINT IF EXISTS ck_advisory_hr_r4_outcome_window,
    DROP CONSTRAINT IF EXISTS ck_advisory_hr_r4_outcome_hashes,
    DROP CONSTRAINT IF EXISTS ck_advisory_hr_r4_outcome_revision,
    ADD CONSTRAINT ck_advisory_hr_r4_outcome_window CHECK (
        (evaluation_window_type = 'FIXED_HORIZON'
         AND subject_type IN ('CANDIDATE', 'LIST_VERSION', 'RANGE')
         AND horizon_trade_days >= 1)
        OR
        (evaluation_window_type = 'EPISODE_LIFECYCLE'
         AND subject_type = 'EPISODE'
         AND horizon_trade_days = 0)
    ),
    ADD CONSTRAINT ck_advisory_hr_r4_outcome_hashes CHECK (
        app.advisory_historical_range_is_sha256(historical_range_policy_bundle_hash)
        AND app.advisory_historical_range_is_sha256(outcome_input_hash)
        AND app.advisory_historical_range_is_sha256(producer_code_hash)
        AND (revision_evidence_hash IS NULL OR app.advisory_historical_range_is_sha256(revision_evidence_hash))
        AND ((revision_evidence_ref IS NULL) = (revision_evidence_hash IS NULL))
        AND (revision_evidence_ref IS NULL OR app.advisory_historical_range_artifact_ref_is_valid(
            revision_evidence_ref, revision_evidence_ref->>'artifact_kind', revision_evidence_hash
        ))
    ),
    ADD CONSTRAINT ck_advisory_hr_r4_outcome_revision CHECK (
        revision_reason IN ('INITIAL', 'MATURITY_ADVANCE', 'SOURCE_CORRECTION', 'CALCULATION_CORRECTION')
        AND ((outcome_version = 1) = (revision_reason = 'INITIAL'))
        AND ((revision_reason IN ('SOURCE_CORRECTION', 'CALCULATION_CORRECTION')) = (revision_evidence_ref IS NOT NULL))
    );

CREATE UNIQUE INDEX IF NOT EXISTS ux_advisory_hr_r4_outcome_input
    ON app.advisory_historical_range_outcome(outcome_logical_id, outcome_input_hash);

CREATE OR REPLACE FUNCTION app.verify_advisory_historical_range_outcome_revision()
RETURNS TRIGGER AS $$
DECLARE
    predecessor app.advisory_historical_range_outcome%ROWTYPE;
BEGIN
    IF NEW.outcome_version = 1 THEN
        RETURN NEW;
    END IF;
    SELECT * INTO predecessor
      FROM app.advisory_historical_range_outcome
     WHERE outcome_version_id = NEW.predecessor_outcome_version_id
     FOR KEY SHARE;
    IF NOT FOUND
       OR predecessor.outcome_logical_id <> NEW.outcome_logical_id
       OR predecessor.outcome_version <> NEW.outcome_version - 1
       OR predecessor.outcome_content_hash <> NEW.predecessor_outcome_hash
       OR predecessor.outcome_artifact_ref IS DISTINCT FROM NEW.outcome_json->'predecessor_outcome_ref' THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OUTCOME_REVISION_CHAIN_INVALID';
    END IF;
    IF NEW.outcome_input_hash = predecessor.outcome_input_hash THEN
        RAISE EXCEPTION 'ADVISORY_HR_OUTCOME_INPUT_CONFLICT';
    END IF;
    IF NEW.revision_reason = 'MATURITY_ADVANCE' THEN
        IF predecessor.maturity_status = 'NOT_DUE'
           AND NEW.maturity_status NOT IN ('MATURING', 'COMPLETE', 'CENSORED', 'TERMINAL') THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OUTCOME_TRANSITION_INVALID';
        ELSIF predecessor.maturity_status = 'MATURING'
           AND NEW.maturity_status NOT IN ('COMPLETE', 'CENSORED', 'TERMINAL') THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OUTCOME_TRANSITION_INVALID';
        ELSIF predecessor.maturity_status NOT IN ('NOT_DUE', 'MATURING') THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OUTCOME_TERMINAL_IMMUTABLE';
        END IF;
    ELSIF NEW.revision_reason = 'SOURCE_CORRECTION' THEN
        IF NEW.source_revision_set_hash = predecessor.source_revision_set_hash THEN
            RAISE EXCEPTION 'ADVISORY_HR_OUTCOME_SOURCE_REVISION_CONFLICT';
        END IF;
    ELSIF NEW.revision_reason = 'CALCULATION_CORRECTION' THEN
        IF NEW.producer_code_hash = predecessor.producer_code_hash
           AND NEW.outcome_contract_version = predecessor.outcome_contract_version THEN
            RAISE EXCEPTION 'ADVISORY_HR_OUTCOME_CALCULATION_CORRECTION_INVALID';
        END IF;
    ELSE
        RAISE EXCEPTION 'ADVISORY_HR_OUTCOME_REVISION_INVALID';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION app.verify_advisory_hr_r4_outcome_artifact_columns()
RETURNS TRIGGER AS $$
BEGIN
    IF jsonb_typeof(NEW.outcome_json) IS DISTINCT FROM 'object'
       OR NEW.outcome_json->>'outcome_logical_id' IS DISTINCT FROM NEW.outcome_logical_id
       OR NEW.outcome_json->>'outcome_version_id' IS DISTINCT FROM NEW.outcome_version_id
       OR NEW.outcome_json->>'outcome_input_hash' IS DISTINCT FROM NEW.outcome_input_hash
       OR NEW.outcome_json->>'projection_group' IS DISTINCT FROM NEW.projection
       OR NEW.outcome_json->>'evaluation_window_type' IS DISTINCT FROM NEW.evaluation_window_type
       OR (NEW.outcome_json->>'horizon_trade_days')::INTEGER IS DISTINCT FROM NEW.horizon_trade_days
       OR NEW.outcome_json->>'policy_bundle_hash' IS DISTINCT FROM NEW.historical_range_policy_bundle_hash
       OR NEW.outcome_json->>'label_as_of_trade_date' IS DISTINCT FROM NEW.label_as_of_trade_date::TEXT
       OR NEW.outcome_json->>'source_revision_set_hash' IS DISTINCT FROM NEW.source_revision_set_hash
       OR NEW.outcome_json->>'maturity_status' IS DISTINCT FROM NEW.maturity_status
       OR NEW.outcome_json->>'next_refresh_trade_date' IS DISTINCT FROM NEW.next_refresh_trade_date::TEXT
       OR NEW.outcome_json->>'producer_code_hash' IS DISTINCT FROM NEW.producer_code_hash THEN
        RAISE EXCEPTION 'ADVISORY_HR_R4_OUTCOME_ARTIFACT_COLUMNS_CONFLICT';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_hr_r4_outcome_artifact_columns
    ON app.advisory_historical_range_outcome;
CREATE TRIGGER trg_verify_advisory_hr_r4_outcome_artifact_columns
BEFORE INSERT OR UPDATE ON app.advisory_historical_range_outcome
FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_hr_r4_outcome_artifact_columns();

COMMENT ON FUNCTION app.verify_advisory_hr_r4_outcome_artifact_columns() IS
    'Rejects split-brain R4 outcome rows whose indexed fact columns differ from the embedded immutable outcome artifact.';

ALTER TABLE app.advisory_historical_range_summary
    ADD COLUMN IF NOT EXISTS summary_policy_hash TEXT,
    ADD COLUMN IF NOT EXISTS summary_input_hash TEXT,
    ADD COLUMN IF NOT EXISTS recall_denominator_set_hash TEXT,
    ADD COLUMN IF NOT EXISTS recall_denominator_evidence_json JSONB,
    ADD COLUMN IF NOT EXISTS producer_code_hash TEXT,
    ADD COLUMN IF NOT EXISTS maturity_coverage_json JSONB,
    ADD COLUMN IF NOT EXISTS maturity_coverage_hash TEXT;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM app.advisory_historical_range_summary
        WHERE summary_policy_hash IS NULL OR summary_input_hash IS NULL
           OR recall_denominator_set_hash IS NULL
           OR recall_denominator_evidence_json IS NULL
           OR producer_code_hash IS NULL OR maturity_coverage_json IS NULL
           OR maturity_coverage_hash IS NULL
    ) THEN
        RAISE EXCEPTION 'ADVISORY_HR_R4_SUMMARY_EXPLICIT_BACKFILL_REQUIRED';
    END IF;
END;
$$;

ALTER TABLE app.advisory_historical_range_summary
    ALTER COLUMN summary_policy_hash SET NOT NULL,
    ALTER COLUMN summary_input_hash SET NOT NULL,
    ALTER COLUMN recall_denominator_set_hash SET NOT NULL,
    ALTER COLUMN recall_denominator_evidence_json SET NOT NULL,
    ALTER COLUMN producer_code_hash SET NOT NULL,
    ALTER COLUMN maturity_coverage_json SET NOT NULL,
    ALTER COLUMN maturity_coverage_hash SET NOT NULL,
    DROP CONSTRAINT IF EXISTS ck_advisory_hr_r4_summary_hashes,
    ADD CONSTRAINT ck_advisory_hr_r4_summary_hashes CHECK (
        app.advisory_historical_range_is_sha256(summary_policy_hash)
        AND app.advisory_historical_range_is_sha256(summary_input_hash)
        AND app.advisory_historical_range_is_sha256(recall_denominator_set_hash)
        AND jsonb_typeof(recall_denominator_evidence_json) = 'object'
        AND recall_denominator_evidence_json->>'denominator_set_hash'
            = recall_denominator_set_hash
        AND app.advisory_historical_range_is_sha256(producer_code_hash)
        AND app.advisory_historical_range_is_sha256(maturity_coverage_hash)
        AND jsonb_typeof(maturity_coverage_json) = 'object'
    );

CREATE UNIQUE INDEX IF NOT EXISTS ux_advisory_hr_r4_summary_input
    ON app.advisory_historical_range_summary(range_run_id, summary_input_hash);

CREATE OR REPLACE FUNCTION app.verify_advisory_historical_range_summary_revision()
RETURNS TRIGGER AS $$
DECLARE
    predecessor app.advisory_historical_range_summary%ROWTYPE;
BEGIN
    IF NEW.summary_version = 1 THEN
        RETURN NEW;
    END IF;
    SELECT * INTO predecessor
      FROM app.advisory_historical_range_summary
     WHERE summary_id = NEW.predecessor_summary_id
     FOR KEY SHARE;
    IF NOT FOUND
       OR predecessor.range_run_id <> NEW.range_run_id
       OR predecessor.summary_version <> NEW.summary_version - 1
       OR predecessor.summary_content_hash <> NEW.predecessor_summary_hash
       OR predecessor.summary_artifact_ref IS DISTINCT FROM NEW.summary_json->'predecessor_summary_ref' THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_SUMMARY_REVISION_CHAIN_INVALID';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION app.verify_advisory_hr_r4_summary_artifact_columns()
RETURNS TRIGGER AS $$
BEGIN
    IF jsonb_typeof(NEW.summary_json) IS DISTINCT FROM 'object'
       OR NEW.summary_json->>'range_run_id' IS DISTINCT FROM NEW.range_run_id
       OR NEW.summary_json->>'summary_input_hash' IS DISTINCT FROM NEW.summary_input_hash
       OR NEW.summary_json->>'summary_policy_hash' IS DISTINCT FROM NEW.summary_policy_hash
       OR NEW.summary_json->>'covered_outcome_set_hash' IS DISTINCT FROM NEW.covered_outcome_set_hash
       OR NEW.summary_json->>'recall_denominator_set_hash' IS DISTINCT FROM NEW.recall_denominator_set_hash
       OR NEW.summary_json->'recall_denominator_evidence' IS DISTINCT FROM NEW.recall_denominator_evidence_json
       OR NEW.summary_json->>'producer_code_hash' IS DISTINCT FROM NEW.producer_code_hash
       OR NEW.summary_json->'maturity_coverage' IS DISTINCT FROM NEW.maturity_coverage_json
       OR NEW.summary_json->>'maturity_coverage_hash' IS DISTINCT FROM NEW.maturity_coverage_hash THEN
        RAISE EXCEPTION 'ADVISORY_HR_R4_SUMMARY_ARTIFACT_COLUMNS_CONFLICT';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_hr_r4_summary_artifact_columns
    ON app.advisory_historical_range_summary;
CREATE TRIGGER trg_verify_advisory_hr_r4_summary_artifact_columns
BEFORE INSERT OR UPDATE ON app.advisory_historical_range_summary
FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_hr_r4_summary_artifact_columns();

COMMENT ON FUNCTION app.verify_advisory_hr_r4_summary_artifact_columns() IS
    'Rejects split-brain R4 summary rows whose indexed fact columns differ from the embedded immutable summary artifact.';

DO $$
DECLARE
    item RECORD;
BEGIN
    FOR item IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'app.advisory_historical_range_operation_attempt'::regclass
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%SOURCE_CATALOG_CHECKPOINT%'
          AND pg_get_constraintdef(oid) LIKE '%RANGE_RECEIPT%'
    LOOP
        EXECUTE format('ALTER TABLE app.advisory_historical_range_operation_attempt DROP CONSTRAINT %I', item.conname);
    END LOOP;
END;
$$;

ALTER TABLE app.advisory_historical_range_operation_attempt
    DROP CONSTRAINT IF EXISTS ck_advisory_hr_r4_operation_attempt_receipt,
    ADD CONSTRAINT ck_advisory_hr_r4_operation_attempt_receipt CHECK (
        attempt_receipt_ref IS NULL
        OR app.advisory_historical_range_artifact_ref_is_valid(attempt_receipt_ref, 'RANGE_RECEIPT', attempt_receipt_hash)
        OR app.advisory_historical_range_artifact_ref_is_valid(attempt_receipt_ref, 'SOURCE_REQUIREMENT_PLAN', attempt_receipt_hash)
        OR app.advisory_historical_range_artifact_ref_is_valid(attempt_receipt_ref, 'SOURCE_CATALOG_CHECKPOINT', attempt_receipt_hash)
        OR app.advisory_historical_range_artifact_ref_is_valid(attempt_receipt_ref, 'OUTCOME_REFRESH_RECEIPT', attempt_receipt_hash)
        OR app.advisory_historical_range_artifact_ref_is_valid(attempt_receipt_ref, 'DATASET_BRIDGE_RECEIPT', attempt_receipt_hash)
    );

ALTER TABLE app.advisory_historical_range_operation
    DROP CONSTRAINT IF EXISTS ck_advisory_hr_r4_operation_result_kind,
    ADD CONSTRAINT ck_advisory_hr_r4_operation_result_kind CHECK (
        result_ref IS NULL
        OR (operation_type IN ('RESUME', 'CANCEL') AND result_ref->>'artifact_kind' = 'RANGE_RECEIPT')
        OR (operation_type = 'CREATE' AND result_ref->>'artifact_kind' = 'SOURCE_REQUIREMENT_PLAN')
        OR (operation_type = 'BUILD_SOURCE_CATALOG'
            AND result_ref->>'artifact_kind' = 'SOURCE_CATALOG_CHECKPOINT')
        OR (operation_type = 'REFRESH_OUTCOMES' AND result_ref->>'artifact_kind' = 'OUTCOME_REFRESH_RECEIPT')
        OR (operation_type = 'BUILD_DATASET_BRIDGE' AND result_ref->>'artifact_kind' = 'DATASET_BRIDGE_RECEIPT')
    );

CREATE OR REPLACE FUNCTION app.verify_advisory_hr_r4_operation_attempt_kind()
RETURNS TRIGGER AS $$
DECLARE
    operation_kind TEXT;
    receipt_kind TEXT;
    expected_receipt_kind TEXT;
BEGIN
    IF NEW.attempt_receipt_ref IS NULL THEN
        RETURN NEW;
    END IF;
    SELECT operation_type INTO operation_kind
      FROM app.advisory_historical_range_operation
     WHERE operation_id = NEW.operation_id;
    receipt_kind := NEW.attempt_receipt_ref->>'artifact_kind';
    expected_receipt_kind := CASE operation_kind
        WHEN 'RESUME' THEN 'RANGE_RECEIPT'
        WHEN 'CANCEL' THEN 'RANGE_RECEIPT'
        WHEN 'CREATE' THEN 'SOURCE_REQUIREMENT_PLAN'
        WHEN 'BUILD_SOURCE_CATALOG' THEN 'SOURCE_CATALOG_CHECKPOINT'
        WHEN 'REFRESH_OUTCOMES' THEN 'OUTCOME_REFRESH_RECEIPT'
        WHEN 'BUILD_DATASET_BRIDGE' THEN 'DATASET_BRIDGE_RECEIPT'
        ELSE NULL
    END;
    IF expected_receipt_kind IS NULL
       OR receipt_kind IS DISTINCT FROM expected_receipt_kind THEN
        RAISE EXCEPTION 'ADVISORY_HR_R4_OPERATION_ATTEMPT_RECEIPT_KIND_INVALID';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_hr_r4_operation_attempt_kind
    ON app.advisory_historical_range_operation_attempt;
CREATE TRIGGER trg_verify_advisory_hr_r4_operation_attempt_kind
    BEFORE INSERT ON app.advisory_historical_range_operation_attempt
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_hr_r4_operation_attempt_kind();

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.views
        WHERE table_schema = 'app' AND table_name = 'advisory_signal_observation_lineage'
    ) THEN
        DROP VIEW app.advisory_signal_observation_lineage;
    END IF;
END;
$$;

ALTER TABLE app.advisory_signal_observation_lineage_identity
    ALTER COLUMN phase0a_audit_id DROP NOT NULL,
    ALTER COLUMN admission_scope_id DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS historical_range_request_ref JSONB,
    ADD COLUMN IF NOT EXISTS historical_range_request_hash TEXT,
    ADD COLUMN IF NOT EXISTS historical_range_frozen_program_ref JSONB,
    ADD COLUMN IF NOT EXISTS historical_range_frozen_program_hash TEXT,
    ADD COLUMN IF NOT EXISTS range_run_id TEXT,
    ADD COLUMN IF NOT EXISTS range_day_run_id TEXT,
    ADD COLUMN IF NOT EXISTS candidate_artifact_ref JSONB,
    ADD COLUMN IF NOT EXISTS candidate_artifact_hash TEXT,
    ADD COLUMN IF NOT EXISTS range_lineage_identity_hash TEXT;

ALTER TABLE app.advisory_signal_observation_lineage_payload
    ALTER COLUMN phase0a_audit_manifest_hash DROP NOT NULL,
    ALTER COLUMN handoff_readiness_hash DROP NOT NULL,
    ALTER COLUMN admission_scope_hash DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS range_signal_context_hash TEXT;

DO $$
DECLARE
    item RECORD;
BEGIN
    FOR item IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'app.advisory_signal_observation_lineage_identity'::regclass
          AND (
              pg_get_constraintdef(oid) LIKE '%lineage_source_type%'
              OR (contype = 'u' AND pg_get_constraintdef(oid) LIKE '%phase0a_audit_id%')
          )
    LOOP
        EXECUTE format('ALTER TABLE app.advisory_signal_observation_lineage_identity DROP CONSTRAINT %I', item.conname);
    END LOOP;
END;
$$;

ALTER TABLE app.advisory_signal_observation_lineage_identity
    DROP CONSTRAINT IF EXISTS ck_advisory_phase1_r4_lineage_union,
    ADD CONSTRAINT ck_advisory_phase1_r4_lineage_union CHECK (
        (
            lineage_source_type IN ('PHASE0A_AUDIT', 'ONLINE_REVIEW', 'ONLINE_LIST', 'HISTORICAL_REPLAY')
            AND phase0a_audit_id IS NOT NULL AND admission_scope_id IS NOT NULL
            AND program_id IS NOT NULL AND binding_version_id IS NOT NULL
            AND source_run_id IS NOT NULL
            AND historical_range_request_ref IS NULL AND historical_range_request_hash IS NULL
            AND historical_range_frozen_program_ref IS NULL AND historical_range_frozen_program_hash IS NULL
            AND range_run_id IS NULL AND range_day_run_id IS NULL
            AND candidate_artifact_ref IS NULL AND candidate_artifact_hash IS NULL
            AND range_lineage_identity_hash IS NULL
        )
        OR
        (
            lineage_source_type = 'HISTORICAL_RANGE_RESEARCH'
            AND historical_range_request_ref IS NOT NULL AND historical_range_request_hash IS NOT NULL
            AND historical_range_frozen_program_ref IS NOT NULL AND historical_range_frozen_program_hash IS NOT NULL
            AND range_run_id IS NOT NULL AND range_day_run_id IS NOT NULL
            AND source_run_id IS NOT NULL AND source_run_id = range_day_run_id
            AND candidate_artifact_ref IS NOT NULL AND candidate_artifact_hash IS NOT NULL
            AND range_lineage_identity_hash IS NOT NULL
            AND phase0a_audit_id IS NULL AND admission_scope_id IS NULL
            AND program_id IS NULL AND binding_version_id IS NULL
            AND app.advisory_historical_range_artifact_ref_is_valid(
                historical_range_request_ref, 'REQUEST', historical_range_request_hash
            )
            AND app.advisory_historical_range_artifact_ref_is_valid(
                historical_range_frozen_program_ref, 'FROZEN_PROGRAM', historical_range_frozen_program_hash
            )
            AND app.advisory_historical_range_artifact_ref_is_valid(
                candidate_artifact_ref, 'CANDIDATE_ARTIFACT', candidate_artifact_hash
            )
            AND app.advisory_historical_range_is_sha256(range_lineage_identity_hash)
        )
    );

CREATE UNIQUE INDEX IF NOT EXISTS ux_advisory_phase1_r4_lineage_identity
    ON app.advisory_signal_observation_lineage_identity(
        observation_version_id, lineage_source_type,
        COALESCE(phase0a_audit_id, range_run_id),
        COALESCE(admission_scope_id, range_day_run_id), source_run_id
    );

CREATE VIEW app.advisory_signal_observation_lineage AS
SELECT identity.lineage_id,
       payload.canonical_signal_id,
       identity.observation_version_id,
       identity.phase0a_audit_id,
       payload.phase0a_audit_manifest_hash,
       payload.handoff_readiness_hash,
       identity.admission_scope_id,
       payload.admission_scope_hash,
       payload.audit_target_id,
       payload.target_scope_hash,
       payload.capability,
       payload.stable_signal_semantics_hash,
       payload.canonical_signal_scope_hash,
       payload.phase0a_signal_context_hash,
       payload.range_signal_context_hash,
       payload.oos_interval_id,
       payload.oos_interval_hash,
       payload.evidence_scope,
       payload.signal_evidence_level,
       payload.effective_cutoff_date,
       identity.program_id,
       identity.binding_version_id,
       identity.lineage_source_type,
       identity.source_run_id,
       payload.review_run_id,
       payload.list_version_id,
       identity.historical_range_request_ref,
       identity.historical_range_request_hash,
       identity.historical_range_frozen_program_ref,
       identity.historical_range_frozen_program_hash,
       identity.range_run_id,
       identity.range_day_run_id,
       identity.candidate_artifact_ref,
       identity.candidate_artifact_hash,
       identity.range_lineage_identity_hash,
       identity.lineage_content_hash,
       identity.created_at
  FROM app.advisory_signal_observation_lineage_identity identity
  JOIN app.advisory_signal_observation_lineage_payload payload
    ON payload.lineage_id = identity.lineage_id
   AND payload.decision_as_of_trade_date = identity.decision_as_of_trade_date;

ALTER TABLE app.advisory_dataset_build
    ALTER COLUMN handoff_readiness_hash DROP NOT NULL,
    ALTER COLUMN admission_scope_set_hash DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS lineage_identity_type TEXT NOT NULL DEFAULT 'PHASE0A',
    ADD COLUMN IF NOT EXISTS range_lineage_scope_set_hash TEXT,
    ADD COLUMN IF NOT EXISTS selector_policy_hash TEXT,
    ADD COLUMN IF NOT EXISTS execution_origin TEXT NOT NULL DEFAULT 'ADVISORY_RUN',
    ADD COLUMN IF NOT EXISTS research_scope TEXT NOT NULL DEFAULT 'HISTORICAL_RESEARCH_ONLY',
    ADD COLUMN IF NOT EXISTS evidence_scope TEXT NOT NULL DEFAULT 'RETROSPECTIVE_RESEARCH_ONLY',
    ADD COLUMN IF NOT EXISTS historical_range_policy_bundle_ref JSONB,
    ADD COLUMN IF NOT EXISTS historical_range_policy_bundle_hash TEXT,
    ADD COLUMN IF NOT EXISTS selected_range_day_outcome_set_hash TEXT,
    ADD COLUMN IF NOT EXISTS policy_component_set_hash TEXT;

ALTER TABLE app.advisory_dataset_build
    DROP CONSTRAINT IF EXISTS ck_advisory_phase1_r4_dataset_origin_scope,
    ADD CONSTRAINT ck_advisory_phase1_r4_dataset_origin_scope CHECK (
        (
            lineage_identity_type = 'PHASE0A'
            AND execution_origin = 'ADVISORY_RUN'
            AND research_scope = 'HISTORICAL_RESEARCH_ONLY'
            AND handoff_readiness_hash IS NOT NULL AND admission_scope_set_hash IS NOT NULL
            AND range_lineage_scope_set_hash IS NULL
            AND selector_policy_hash IS NULL
            AND historical_range_policy_bundle_ref IS NULL
            AND historical_range_policy_bundle_hash IS NULL
            AND selected_range_day_outcome_set_hash IS NULL
            AND policy_component_set_hash IS NULL
        )
        OR
        (
            lineage_identity_type = 'HISTORICAL_RANGE'
            AND execution_origin = 'HISTORICAL_RANGE_RESEARCH'
            AND research_scope = 'RETROSPECTIVE_RESEARCH_ONLY'
            AND evidence_scope = 'RETROSPECTIVE_RESEARCH_ONLY'
            AND handoff_readiness_hash IS NULL AND admission_scope_set_hash IS NULL
            AND app.advisory_historical_range_is_sha256(range_lineage_scope_set_hash)
            AND app.advisory_historical_range_is_sha256(selector_policy_hash)
            AND app.advisory_historical_range_is_sha256(selected_range_day_outcome_set_hash)
            AND app.advisory_historical_range_is_sha256(policy_component_set_hash)
            AND app.advisory_historical_range_artifact_ref_is_valid(
                historical_range_policy_bundle_ref,
                'REQUEST',
                NULL
            )
            AND historical_range_policy_bundle_ref->>'payload_sha256' = historical_range_policy_bundle_hash
        )
    );

ALTER TABLE app.advisory_dataset_snapshot
    ALTER COLUMN handoff_readiness_hash DROP NOT NULL,
    ALTER COLUMN admission_scope_set_hash DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS lineage_identity_type TEXT NOT NULL DEFAULT 'PHASE0A',
    ADD COLUMN IF NOT EXISTS execution_origin TEXT NOT NULL DEFAULT 'ADVISORY_RUN',
    ADD COLUMN IF NOT EXISTS research_scope TEXT NOT NULL DEFAULT 'HISTORICAL_RESEARCH_ONLY',
    ADD COLUMN IF NOT EXISTS range_lineage_scope_set_hash TEXT,
    ADD COLUMN IF NOT EXISTS evidence_scope TEXT NOT NULL DEFAULT 'RETROSPECTIVE_RESEARCH_ONLY',
    ADD COLUMN IF NOT EXISTS selector_policy_hash TEXT,
    ADD COLUMN IF NOT EXISTS selected_range_day_outcome_set_hash TEXT,
    ADD COLUMN IF NOT EXISTS policy_lineage_type TEXT NOT NULL DEFAULT 'PHASE1_LABEL_POLICY',
    ADD COLUMN IF NOT EXISTS historical_range_policy_bundle_hash TEXT,
    ADD COLUMN IF NOT EXISTS policy_component_set_hash TEXT,
    ADD COLUMN IF NOT EXISTS selected_observation_mapping_set_hash TEXT,
    ADD COLUMN IF NOT EXISTS selected_label_mapping_set_hash TEXT,
    ADD COLUMN IF NOT EXISTS source_revision_closure_hash TEXT,
    ADD COLUMN IF NOT EXISTS maturity_coverage_hash TEXT;

ALTER TABLE app.advisory_dataset_snapshot
    DROP CONSTRAINT IF EXISTS ck_advisory_phase1_r4_snapshot_lineage,
    ADD CONSTRAINT ck_advisory_phase1_r4_snapshot_lineage CHECK (
        (lineage_identity_type = 'PHASE0A'
         AND execution_origin = 'ADVISORY_RUN'
         AND research_scope = 'HISTORICAL_RESEARCH_ONLY'
         AND handoff_readiness_hash IS NOT NULL AND admission_scope_set_hash IS NOT NULL
         AND range_lineage_scope_set_hash IS NULL
         AND selector_policy_hash IS NULL
         AND selected_range_day_outcome_set_hash IS NULL
         AND policy_lineage_type = 'PHASE1_LABEL_POLICY'
         AND historical_range_policy_bundle_hash IS NULL
         AND policy_component_set_hash IS NULL
         AND selected_observation_mapping_set_hash IS NULL
         AND selected_label_mapping_set_hash IS NULL
         AND source_revision_closure_hash IS NULL
         AND maturity_coverage_hash IS NULL)
        OR
        (lineage_identity_type = 'HISTORICAL_RANGE'
         AND execution_origin = 'HISTORICAL_RANGE_RESEARCH'
         AND research_scope = 'RETROSPECTIVE_RESEARCH_ONLY'
         AND handoff_readiness_hash IS NULL AND admission_scope_set_hash IS NULL
         AND app.advisory_historical_range_is_sha256(range_lineage_scope_set_hash)
         AND app.advisory_historical_range_is_sha256(selector_policy_hash)
         AND app.advisory_historical_range_is_sha256(selected_range_day_outcome_set_hash)
         AND policy_lineage_type = 'HISTORICAL_RANGE_OUTCOME_POLICY'
         AND app.advisory_historical_range_is_sha256(historical_range_policy_bundle_hash)
         AND app.advisory_historical_range_is_sha256(policy_component_set_hash)
         AND app.advisory_historical_range_is_sha256(selected_observation_mapping_set_hash)
         AND app.advisory_historical_range_is_sha256(selected_label_mapping_set_hash)
         AND app.advisory_historical_range_is_sha256(source_revision_closure_hash)
         AND app.advisory_historical_range_is_sha256(maturity_coverage_hash)
         AND evidence_scope = 'RETROSPECTIVE_RESEARCH_ONLY')
    );

COMMENT ON COLUMN app.advisory_historical_range_outcome.evaluation_window_type IS
    'FIXED_HORIZON or EPISODE_LIFECYCLE; episode uses horizon_trade_days=0 sentinel.';
COMMENT ON COLUMN app.advisory_historical_range_outcome.outcome_input_hash IS
    'Canonical exact-retry identity over subject, policy, label-as-of, source revisions, and producer code.';
COMMENT ON COLUMN app.advisory_historical_range_outcome.revision_reason IS
    'INITIAL, MATURITY_ADVANCE, SOURCE_CORRECTION, or CALCULATION_CORRECTION.';
COMMENT ON COLUMN app.advisory_historical_range_summary.summary_input_hash IS
    'Hash of covered_outcome_set_hash, summary_policy_hash, recall_denominator_set_hash, and producer_code_hash.';
COMMENT ON COLUMN app.advisory_historical_range_summary.recall_denominator_set_hash IS
    'Typed Phase 1 PIT universe Recall evidence identity, including explicit unavailable state.';
COMMENT ON COLUMN app.advisory_signal_observation_lineage_identity.range_lineage_identity_hash IS
    'Exact range-native lineage; never a synthetic Phase 0A audit or admission identity.';
COMMENT ON COLUMN app.advisory_dataset_build.selector_policy_hash IS
    'Exact formal or retrospective selector policy propagated into the sealed manifest.';

-- Phase 1 capture/observation/label tagged-union extensions. Formal rows keep
-- their existing canonical payload; range rows never manufacture Phase 0A ids.
ALTER TABLE app.advisory_capture_batch
    ALTER COLUMN control_binding_event_hash DROP NOT NULL,
    ALTER COLUMN handoff_readiness_hash DROP NOT NULL,
    ALTER COLUMN admission_scope_id DROP NOT NULL,
    ALTER COLUMN admission_scope_hash DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS lineage_identity_type TEXT NOT NULL DEFAULT 'PHASE0A',
    ADD COLUMN IF NOT EXISTS range_lineage_scope_id TEXT,
    ADD COLUMN IF NOT EXISTS range_lineage_scope_hash TEXT,
    ADD COLUMN IF NOT EXISTS execution_origin TEXT NOT NULL DEFAULT 'ADVISORY_RUN',
    ADD COLUMN IF NOT EXISTS research_scope TEXT NOT NULL DEFAULT 'HISTORICAL_RESEARCH_ONLY',
    ADD COLUMN IF NOT EXISTS evidence_scope TEXT NOT NULL DEFAULT 'RETROSPECTIVE_RESEARCH_ONLY',
    ADD COLUMN IF NOT EXISTS selector_policy_hash TEXT,
    ADD COLUMN IF NOT EXISTS historical_range_policy_bundle_ref JSONB,
    ADD COLUMN IF NOT EXISTS historical_range_policy_bundle_hash TEXT;

ALTER TABLE app.advisory_capture_batch
    DROP CONSTRAINT IF EXISTS advisory_capture_batch_schema_purpose_check,
    ADD CONSTRAINT advisory_capture_batch_schema_purpose_check CHECK (
        (capture_request_schema_version = 'advisory_phase1_capture_batch_v1'
         AND capture_purpose = 'OBSERVATION_CAPTURE_V1')
        OR
        (capture_request_schema_version = 'advisory_phase1_capture_batch_v2'
         AND capture_purpose = 'LABEL_CAPTURE_V1')
        OR
        (capture_request_schema_version = 'advisory_phase1_retrospective_capture_batch_v1'
         AND capture_purpose = 'OBSERVATION_CAPTURE_V1')
        OR
        (capture_request_schema_version = 'advisory_phase1_retrospective_label_capture_batch_v1'
         AND capture_purpose = 'LABEL_CAPTURE_V1')
    );

ALTER TABLE app.advisory_capture_batch
    DROP CONSTRAINT IF EXISTS ck_advisory_phase1_r4_capture_union,
    ADD CONSTRAINT ck_advisory_phase1_r4_capture_union CHECK (
        (
            lineage_identity_type = 'PHASE0A'
            AND execution_origin = 'ADVISORY_RUN'
            AND research_scope = 'HISTORICAL_RESEARCH_ONLY'
            AND control_binding_event_hash IS NOT NULL
            AND handoff_readiness_hash IS NOT NULL
            AND admission_scope_id IS NOT NULL
            AND admission_scope_hash IS NOT NULL
            AND range_lineage_scope_id IS NULL
            AND range_lineage_scope_hash IS NULL
            AND selector_policy_hash IS NULL
            AND historical_range_policy_bundle_ref IS NULL
            AND historical_range_policy_bundle_hash IS NULL
        )
        OR
        (
            lineage_identity_type = 'HISTORICAL_RANGE'
            AND execution_origin = 'HISTORICAL_RANGE_RESEARCH'
            AND research_scope = 'RETROSPECTIVE_RESEARCH_ONLY'
            AND evidence_scope = 'RETROSPECTIVE_RESEARCH_ONLY'
            AND control_binding_event_hash IS NULL
            AND handoff_readiness_hash IS NULL
            AND admission_scope_id IS NULL
            AND admission_scope_hash IS NULL
            AND range_lineage_scope_id IS NOT NULL
            AND app.advisory_historical_range_is_sha256(range_lineage_scope_hash)
            AND app.advisory_historical_range_is_sha256(selector_policy_hash)
            AND app.advisory_historical_range_is_sha256(historical_range_policy_bundle_hash)
            AND app.advisory_historical_range_artifact_ref_is_valid(
                historical_range_policy_bundle_ref,
                'REQUEST',
                NULL
            )
            AND historical_range_policy_bundle_ref->>'payload_sha256' = historical_range_policy_bundle_hash
        )
    );

ALTER TABLE app.advisory_capture_plan
    ALTER COLUMN selection_run_id DROP NOT NULL,
    ALTER COLUMN phase0a_audit_id DROP NOT NULL,
    ALTER COLUMN phase0a_audit_manifest_hash DROP NOT NULL,
    ALTER COLUMN handoff_readiness_hash DROP NOT NULL,
    ALTER COLUMN admission_scope_id DROP NOT NULL,
    ALTER COLUMN admission_scope_hash DROP NOT NULL,
    ALTER COLUMN program_id DROP NOT NULL,
    ALTER COLUMN binding_version_id DROP NOT NULL,
    ALTER COLUMN source_run_id DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS lineage_identity_type TEXT NOT NULL DEFAULT 'PHASE0A',
    ADD COLUMN IF NOT EXISTS range_lineage_scope_id TEXT,
    ADD COLUMN IF NOT EXISTS range_lineage_scope_hash TEXT,
    ADD COLUMN IF NOT EXISTS historical_range_request_ref JSONB,
    ADD COLUMN IF NOT EXISTS historical_range_request_hash TEXT,
    ADD COLUMN IF NOT EXISTS historical_range_frozen_program_ref JSONB,
    ADD COLUMN IF NOT EXISTS historical_range_frozen_program_hash TEXT,
    ADD COLUMN IF NOT EXISTS range_run_id TEXT,
    ADD COLUMN IF NOT EXISTS range_day_run_id TEXT,
    ADD COLUMN IF NOT EXISTS candidate_artifact_ref JSONB,
    ADD COLUMN IF NOT EXISTS candidate_artifact_hash TEXT,
    ADD COLUMN IF NOT EXISTS range_lineage_identity_hash TEXT,
    ADD COLUMN IF NOT EXISTS range_signal_context_hash TEXT,
    ADD COLUMN IF NOT EXISTS selector_policy_hash TEXT;

ALTER TABLE app.advisory_capture_plan
    DROP CONSTRAINT IF EXISTS ck_advisory_phase1_r4_capture_plan_union,
    ADD CONSTRAINT ck_advisory_phase1_r4_capture_plan_union CHECK (
        (
            lineage_identity_type = 'PHASE0A'
            AND selection_run_id IS NOT NULL
            AND phase0a_audit_id IS NOT NULL
            AND phase0a_audit_manifest_hash IS NOT NULL
            AND handoff_readiness_hash IS NOT NULL
            AND admission_scope_id IS NOT NULL
            AND admission_scope_hash IS NOT NULL
            AND program_id IS NOT NULL
            AND binding_version_id IS NOT NULL
            AND source_run_id IS NOT NULL
            AND range_lineage_scope_id IS NULL
            AND range_lineage_scope_hash IS NULL
            AND historical_range_request_ref IS NULL
            AND historical_range_request_hash IS NULL
            AND historical_range_frozen_program_ref IS NULL
            AND historical_range_frozen_program_hash IS NULL
            AND range_run_id IS NULL
            AND range_day_run_id IS NULL
            AND candidate_artifact_ref IS NULL
            AND candidate_artifact_hash IS NULL
            AND range_lineage_identity_hash IS NULL
            AND range_signal_context_hash IS NULL
            AND selector_policy_hash IS NULL
        )
        OR
        (
            lineage_identity_type = 'HISTORICAL_RANGE'
            AND selection_run_id IS NULL
            AND phase0a_audit_id IS NULL
            AND phase0a_audit_manifest_hash IS NULL
            AND handoff_readiness_hash IS NULL
            AND admission_scope_id IS NULL
            AND admission_scope_hash IS NULL
            AND program_id IS NULL
            AND binding_version_id IS NULL
            AND source_run_id IS NULL
            AND range_lineage_scope_id IS NOT NULL
            AND app.advisory_historical_range_is_sha256(range_lineage_scope_hash)
            AND range_run_id IS NOT NULL
            AND range_day_run_id IS NOT NULL
            AND app.advisory_historical_range_artifact_ref_is_valid(
                historical_range_request_ref, 'REQUEST', historical_range_request_hash
            )
            AND app.advisory_historical_range_artifact_ref_is_valid(
                historical_range_frozen_program_ref, 'FROZEN_PROGRAM', historical_range_frozen_program_hash
            )
            AND app.advisory_historical_range_artifact_ref_is_valid(
                candidate_artifact_ref, 'CANDIDATE_ARTIFACT', candidate_artifact_hash
            )
            AND app.advisory_historical_range_is_sha256(range_lineage_identity_hash)
            AND app.advisory_historical_range_is_sha256(range_signal_context_hash)
            AND app.advisory_historical_range_is_sha256(selector_policy_hash)
        )
    );

ALTER TABLE app.advisory_signal_observation_version
    ALTER COLUMN phase0a_signal_context_hash DROP NOT NULL,
    ALTER COLUMN selection_evidence_id DROP NOT NULL,
    ALTER COLUMN selection_evidence_hash DROP NOT NULL,
    ALTER COLUMN selection_run_id DROP NOT NULL,
    ALTER COLUMN selection_run_content_hash DROP NOT NULL,
    ALTER COLUMN selection_score_artifact_id DROP NOT NULL,
    ALTER COLUMN selection_score_artifact_hash DROP NOT NULL,
    ALTER COLUMN runtime_profile_version_id DROP NOT NULL,
    ALTER COLUMN runtime_profile_version_hash DROP NOT NULL,
    ALTER COLUMN hmm_snapshot_id DROP NOT NULL,
    ALTER COLUMN hmm_snapshot_hash DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS lineage_identity_type TEXT NOT NULL DEFAULT 'PHASE0A',
    ADD COLUMN IF NOT EXISTS range_signal_context_hash TEXT,
    ADD COLUMN IF NOT EXISTS range_run_id TEXT,
    ADD COLUMN IF NOT EXISTS range_day_run_id TEXT,
    ADD COLUMN IF NOT EXISTS candidate_artifact_ref JSONB,
    ADD COLUMN IF NOT EXISTS candidate_artifact_hash TEXT;

ALTER TABLE app.advisory_signal_observation_version
    DROP CONSTRAINT IF EXISTS ck_advisory_phase1_r4_observation_union,
    ADD CONSTRAINT ck_advisory_phase1_r4_observation_union CHECK (
        (
            lineage_identity_type = 'PHASE0A'
            AND phase0a_signal_context_hash IS NOT NULL
            AND selection_evidence_id IS NOT NULL
            AND selection_evidence_hash IS NOT NULL
            AND selection_run_id IS NOT NULL
            AND selection_run_content_hash IS NOT NULL
            AND selection_score_artifact_id IS NOT NULL
            AND selection_score_artifact_hash IS NOT NULL
            AND runtime_profile_version_id IS NOT NULL
            AND runtime_profile_version_hash IS NOT NULL
            AND hmm_snapshot_status IS NOT NULL
            AND (
                (hmm_snapshot_status = 'NOT_APPLICABLE'
                 AND hmm_snapshot_id IS NULL AND hmm_snapshot_hash IS NULL)
                OR
                (hmm_snapshot_status <> 'NOT_APPLICABLE'
                 AND hmm_snapshot_id IS NOT NULL AND hmm_snapshot_hash IS NOT NULL)
            )
            AND range_signal_context_hash IS NULL
            AND range_run_id IS NULL
            AND range_day_run_id IS NULL
            AND candidate_artifact_ref IS NULL
            AND candidate_artifact_hash IS NULL
        )
        OR
        (
            lineage_identity_type = 'HISTORICAL_RANGE'
            AND phase0a_signal_context_hash IS NULL
            AND selection_evidence_id IS NULL
            AND selection_evidence_hash IS NULL
            AND selection_run_id IS NULL
            AND selection_run_content_hash IS NULL
            AND selection_score_artifact_id IS NULL
            AND selection_score_artifact_hash IS NULL
            AND runtime_profile_version_id IS NOT NULL
            AND runtime_profile_version_hash IS NOT NULL
            AND hmm_snapshot_status IS NOT NULL
            AND (
                (hmm_snapshot_status = 'NOT_APPLICABLE'
                 AND hmm_snapshot_id IS NULL AND hmm_snapshot_hash IS NULL)
                OR
                (hmm_snapshot_status <> 'NOT_APPLICABLE'
                 AND hmm_snapshot_id IS NOT NULL AND hmm_snapshot_hash IS NOT NULL)
            )
            AND app.advisory_historical_range_is_sha256(range_signal_context_hash)
            AND range_run_id IS NOT NULL
            AND range_day_run_id IS NOT NULL
            AND app.advisory_historical_range_artifact_ref_is_valid(
                candidate_artifact_ref, 'CANDIDATE_ARTIFACT', candidate_artifact_hash
            )
        )
    );

ALTER TABLE app.advisory_signal_observation_lineage_identity
    ALTER COLUMN program_id DROP NOT NULL,
    ALTER COLUMN binding_version_id DROP NOT NULL;

ALTER TABLE app.advisory_signal_observation_lineage_payload
    ALTER COLUMN phase0a_signal_context_hash DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS range_signal_context_hash TEXT;

DO $$
DECLARE
    lineage_partition REGCLASS;
BEGIN
    FOR lineage_partition IN
        SELECT inhrelid::REGCLASS
          FROM pg_inherits
         WHERE inhparent = 'app.advisory_signal_observation_lineage_payload'::REGCLASS
         ORDER BY inhrelid::REGCLASS::TEXT
    LOOP
        EXECUTE format(
            'ALTER TABLE %s ALTER COLUMN phase0a_signal_context_hash DROP NOT NULL',
            lineage_partition
        );
    END LOOP;
END;
$$;

ALTER TABLE app.advisory_signal_observation_lineage_payload
    DROP CONSTRAINT IF EXISTS ck_advisory_phase1_r4_lineage_payload_union,
    ADD CONSTRAINT ck_advisory_phase1_r4_lineage_payload_union CHECK (
        (
            phase0a_signal_context_hash IS NOT NULL
            AND range_signal_context_hash IS NULL
        )
        OR
        (
            phase0a_signal_context_hash IS NULL
            AND app.advisory_historical_range_is_sha256(range_signal_context_hash)
        )
    );

CREATE OR REPLACE FUNCTION app.verify_advisory_phase1_r4_lineage_payload_union()
RETURNS TRIGGER AS $$
DECLARE
    identity_source_type TEXT;
BEGIN
    SELECT lineage_source_type
      INTO identity_source_type
      FROM app.advisory_signal_observation_lineage_identity
     WHERE lineage_id = NEW.lineage_id
       AND decision_as_of_trade_date = NEW.decision_as_of_trade_date
     FOR KEY SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1_R4_LINEAGE_IDENTITY_MISSING';
    END IF;
    IF identity_source_type = 'HISTORICAL_RANGE_RESEARCH' THEN
        IF NEW.phase0a_audit_manifest_hash IS NOT NULL
           OR NEW.handoff_readiness_hash IS NOT NULL
           OR NEW.admission_scope_hash IS NOT NULL
           OR NEW.phase0a_signal_context_hash IS NOT NULL
           OR NEW.range_signal_context_hash IS NULL THEN
            RAISE EXCEPTION 'ADVISORY_PHASE1_R4_LINEAGE_PAYLOAD_IDENTITY_MISMATCH';
        END IF;
    ELSIF identity_source_type IN ('PHASE0A_AUDIT', 'ONLINE_REVIEW', 'ONLINE_LIST', 'HISTORICAL_REPLAY') THEN
        IF NEW.phase0a_audit_manifest_hash IS NULL
           OR NEW.handoff_readiness_hash IS NULL
           OR NEW.admission_scope_hash IS NULL
           OR NEW.phase0a_signal_context_hash IS NULL
           OR NEW.range_signal_context_hash IS NOT NULL THEN
            RAISE EXCEPTION 'ADVISORY_PHASE1_R4_LINEAGE_PAYLOAD_IDENTITY_MISMATCH';
        END IF;
    ELSE
        RAISE EXCEPTION 'ADVISORY_PHASE1_R4_LINEAGE_IDENTITY_SOURCE_INVALID';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_phase1_r4_lineage_payload_union
    ON app.advisory_signal_observation_lineage_payload;
CREATE TRIGGER trg_verify_advisory_phase1_r4_lineage_payload_union
    BEFORE INSERT ON app.advisory_signal_observation_lineage_payload
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_phase1_r4_lineage_payload_union();

ALTER TABLE app.advisory_outcome_label
    ALTER COLUMN label_policy_bundle_id DROP NOT NULL,
    ALTER COLUMN label_policy_bundle_hash DROP NOT NULL,
    ADD COLUMN IF NOT EXISTS policy_lineage_type TEXT NOT NULL DEFAULT 'PHASE1_LABEL_POLICY',
    ADD COLUMN IF NOT EXISTS historical_range_policy_bundle_ref JSONB,
    ADD COLUMN IF NOT EXISTS historical_range_policy_bundle_hash TEXT,
    ADD COLUMN IF NOT EXISTS policy_component_set_hash TEXT;

ALTER TABLE app.advisory_outcome_label
    DROP CONSTRAINT IF EXISTS ck_advisory_phase1_r4_label_policy_union,
    ADD CONSTRAINT ck_advisory_phase1_r4_label_policy_union CHECK (
        (
            policy_lineage_type = 'PHASE1_LABEL_POLICY'
            AND label_policy_bundle_id IS NOT NULL
            AND app.advisory_historical_range_is_sha256(label_policy_bundle_hash)
            AND historical_range_policy_bundle_ref IS NULL
            AND historical_range_policy_bundle_hash IS NULL
            AND policy_component_set_hash IS NULL
        )
        OR
        (
            policy_lineage_type = 'HISTORICAL_RANGE_OUTCOME_POLICY'
            AND label_policy_bundle_id IS NULL
            AND label_policy_bundle_hash IS NULL
            AND app.advisory_historical_range_is_sha256(historical_range_policy_bundle_hash)
            AND app.advisory_historical_range_is_sha256(policy_component_set_hash)
            AND app.advisory_historical_range_artifact_ref_is_valid(
                historical_range_policy_bundle_ref,
                'REQUEST', NULL
            )
            AND historical_range_policy_bundle_ref->>'payload_sha256' = historical_range_policy_bundle_hash
        )
    );

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
    required_candidate_stage TEXT;
BEGIN
    SELECT capture_status, capture_request_schema_version, capture_purpose,
           lease_expires_at, clock_timestamp()
      INTO creator_status, creator_schema, creator_purpose,
           creator_lease_expires_at, database_now
      FROM app.advisory_capture_batch
     WHERE capture_batch_id = NEW.created_by_capture_batch_id
     FOR KEY SHARE;
    IF creator_status IS DISTINCT FROM 'RUNNING'
       OR creator_purpose IS DISTINCT FROM 'LABEL_CAPTURE_V1'
       OR creator_lease_expires_at IS NULL
       OR creator_lease_expires_at <= database_now
       OR (NEW.policy_lineage_type = 'PHASE1_LABEL_POLICY'
           AND creator_schema IS DISTINCT FROM 'advisory_phase1_capture_batch_v2')
       OR (NEW.policy_lineage_type = 'HISTORICAL_RANGE_OUTCOME_POLICY'
           AND creator_schema IS DISTINCT FROM 'advisory_phase1_retrospective_label_capture_batch_v1') THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_LABEL_CREATOR_CAPTURE_INVALID';
    END IF;
    IF NEW.owner_type = 'CANDIDATE' THEN
        required_candidate_stage := CASE NEW.policy_lineage_type
            WHEN 'PHASE1_LABEL_POLICY' THEN 'alpha_raw'
            WHEN 'HISTORICAL_RANGE_OUTCOME_POLICY' THEN 'selection_effective'
            ELSE NULL
        END;
        SELECT canonical_signal_id
          INTO observed_signal_id
          FROM app.advisory_signal_observation_version
         WHERE observation_version_id = NEW.observation_version_id
         FOR KEY SHARE;
        SELECT observation_version_id
          INTO stage_observation_id
          FROM app.advisory_signal_stage_evidence
         WHERE stage_evidence_id = NEW.candidate_stage_evidence_id
           AND stage = required_candidate_stage
         FOR KEY SHARE;
        SELECT membership_status
          INTO candidate_membership
          FROM app.advisory_signal_stage_candidate
         WHERE stage_evidence_id = NEW.candidate_stage_evidence_id
           AND symbol = NEW.symbol
         FOR KEY SHARE;
        IF required_candidate_stage IS NULL
           OR observed_signal_id IS DISTINCT FROM NEW.canonical_signal_id
           OR stage_observation_id IS DISTINCT FROM NEW.observation_version_id
           OR candidate_membership IS DISTINCT FROM 'INCLUDED' THEN
            RAISE EXCEPTION 'ADVISORY_PHASE1C3_LABEL_OWNER_MEMBERSHIP_INVALID';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION app.verify_advisory_capture_batch_transition()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.capture_status <> 'PLANNED'
           OR NEW.row_version <> 1
           OR NEW.fencing_token <> 1 THEN
            RAISE EXCEPTION 'ADVISORY_PHASE1_CAPTURE_BATCH_INITIAL_STATE_INVALID';
        END IF;
        IF NEW.capture_request_schema_version = 'advisory_phase1_capture_batch_v2'
           AND (
               NEW.request_payload_jsonb->>'schema_version' <> 'advisory_phase1_capture_batch_v2'
               OR NEW.request_payload_jsonb->>'capture_purpose' <> 'LABEL_CAPTURE_V1'
               OR NEW.binding_jsonb->>'schema_version' <> 'advisory_phase1_label_capture_binding_v1'
           ) THEN
            RAISE EXCEPTION 'ADVISORY_PHASE1C3_CAPTURE_PAYLOAD_CLOSURE_INVALID';
        ELSIF NEW.capture_request_schema_version = 'advisory_phase1_retrospective_capture_batch_v1'
           AND (
               NEW.request_payload_jsonb->>'schema_version' <> 'advisory_phase1_retrospective_capture_batch_v1'
               OR NEW.request_payload_jsonb->>'capture_purpose' <> 'OBSERVATION_CAPTURE_V1'
               OR NEW.binding_jsonb->>'schema_version' <> 'advisory_phase1_retrospective_capture_binding_v1'
           ) THEN
            RAISE EXCEPTION 'ADVISORY_PHASE1C3_CAPTURE_PAYLOAD_CLOSURE_INVALID';
        ELSIF NEW.capture_request_schema_version = 'advisory_phase1_retrospective_label_capture_batch_v1'
           AND (
               NEW.request_payload_jsonb->>'schema_version' <> 'advisory_phase1_retrospective_label_capture_batch_v1'
               OR NEW.request_payload_jsonb->>'capture_purpose' <> 'LABEL_CAPTURE_V1'
               OR NEW.binding_jsonb->>'schema_version' <> 'advisory_phase1_retrospective_label_capture_binding_v1'
           ) THEN
            RAISE EXCEPTION 'ADVISORY_PHASE1C3_CAPTURE_PAYLOAD_CLOSURE_INVALID';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.capture_request_hash IS DISTINCT FROM OLD.capture_request_hash
       OR NEW.request_payload_jsonb IS DISTINCT FROM OLD.request_payload_jsonb
       OR NEW.binding_jsonb IS DISTINCT FROM OLD.binding_jsonb
       OR NEW.control_binding_event_hash IS DISTINCT FROM OLD.control_binding_event_hash
       OR NEW.handoff_readiness_hash IS DISTINCT FROM OLD.handoff_readiness_hash
       OR NEW.admission_scope_id IS DISTINCT FROM OLD.admission_scope_id
       OR NEW.admission_scope_hash IS DISTINCT FROM OLD.admission_scope_hash
       OR NEW.capture_attempt_no IS DISTINCT FROM OLD.capture_attempt_no
       OR NEW.predecessor_capture_batch_id IS DISTINCT FROM OLD.predecessor_capture_batch_id
       OR NEW.capture_request_schema_version IS DISTINCT FROM OLD.capture_request_schema_version
       OR NEW.capture_purpose IS DISTINCT FROM OLD.capture_purpose
       OR NEW.lineage_identity_type IS DISTINCT FROM OLD.lineage_identity_type
       OR NEW.range_lineage_scope_id IS DISTINCT FROM OLD.range_lineage_scope_id
       OR NEW.range_lineage_scope_hash IS DISTINCT FROM OLD.range_lineage_scope_hash
       OR NEW.execution_origin IS DISTINCT FROM OLD.execution_origin
       OR NEW.research_scope IS DISTINCT FROM OLD.research_scope
       OR NEW.evidence_scope IS DISTINCT FROM OLD.evidence_scope
       OR NEW.selector_policy_hash IS DISTINCT FROM OLD.selector_policy_hash
       OR NEW.historical_range_policy_bundle_ref IS DISTINCT FROM OLD.historical_range_policy_bundle_ref
       OR NEW.historical_range_policy_bundle_hash IS DISTINCT FROM OLD.historical_range_policy_bundle_hash THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1_CAPTURE_BATCH_IMMUTABLE';
    END IF;
    IF NEW.row_version <> OLD.row_version + 1
       OR NEW.fencing_token < OLD.fencing_token THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1_CAPTURE_BATCH_CAS_INVALID';
    END IF;
    IF OLD.capture_status = 'PLANNED' AND NEW.capture_status <> 'RUNNING' THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1_CAPTURE_BATCH_TRANSITION_INVALID';
    ELSIF OLD.capture_status = 'RUNNING'
          AND NEW.capture_status NOT IN ('RUNNING', 'COMPLETE', 'FAILED', 'EXPIRED', 'ABORTED') THEN
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
    IF batch_schema IN ('advisory_phase1_capture_batch_v1', 'advisory_phase1_retrospective_capture_batch_v1')
       AND (batch_purpose <> 'OBSERVATION_CAPTURE_V1' OR plan_count < 1) THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_CAPTURE_PAYLOAD_CLOSURE_INVALID';
    END IF;
    IF batch_schema IN ('advisory_phase1_capture_batch_v2', 'advisory_phase1_retrospective_label_capture_batch_v1')
       AND ((batch_purpose = 'LABEL_CAPTURE_V1' AND plan_count <> 0)
            OR (batch_purpose <> 'LABEL_CAPTURE_V1' AND plan_count < 1)) THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C3_CAPTURE_PAYLOAD_CLOSURE_INVALID';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

COMMIT;

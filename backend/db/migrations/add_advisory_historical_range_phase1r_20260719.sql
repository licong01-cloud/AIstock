-- Phase 1R historical-range Advisory research foundation.
-- Additive only. Apply through the explicit DEV/release migration workflow.
-- Runtime services must never execute this file. This schema introduces no
-- role, approval, authorization, backup, scheduler, or package re-admission.

BEGIN;

CREATE SCHEMA IF NOT EXISTS app;

CREATE OR REPLACE FUNCTION app.advisory_historical_range_is_sha256(value TEXT)
RETURNS BOOLEAN AS $$
BEGIN
    RETURN value ~ '^[0-9a-f]{64}$';
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;

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
        WHEN 'DAY_RECEIPT' THEN 'day-receipts'
        WHEN 'RANGE_RECEIPT' THEN 'range-receipts'
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

CREATE TABLE IF NOT EXISTS app.advisory_historical_range_batch (
    batch_id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    client_idempotency_key TEXT NOT NULL UNIQUE,
    user_request_semantic_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(user_request_semantic_hash)),
    planning_identity_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(planning_identity_hash)),
    requirement_plan_ref JSONB NOT NULL,
    requirement_plan_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(requirement_plan_hash)),
    requirement_plan_artifact_hash TEXT NOT NULL CHECK (
        app.advisory_historical_range_is_sha256(requirement_plan_artifact_hash)
    ),
    request_payload_sha256 TEXT CHECK (request_payload_sha256 IS NULL OR app.advisory_historical_range_is_sha256(request_payload_sha256)),
    request_artifact_ref JSONB,
    request_artifact_hash TEXT CHECK (request_artifact_hash IS NULL OR app.advisory_historical_range_is_sha256(request_artifact_hash)),
    supersedes_batch_id TEXT REFERENCES app.advisory_historical_range_batch(batch_id) ON DELETE RESTRICT,
    canonical_batch_id TEXT REFERENCES app.advisory_historical_range_batch(batch_id) ON DELETE RESTRICT,
    deduplicated_request_payload_sha256 TEXT CHECK (
        deduplicated_request_payload_sha256 IS NULL
        OR app.advisory_historical_range_is_sha256(deduplicated_request_payload_sha256)
    ),
    dedup_receipt_ref JSONB,
    dedup_receipt_hash TEXT CHECK (dedup_receipt_hash IS NULL OR app.advisory_historical_range_is_sha256(dedup_receipt_hash)),
    start_trade_date DATE NOT NULL,
    end_trade_date DATE NOT NULL,
    calendar_id TEXT NOT NULL,
    calendar_version TEXT NOT NULL,
    ordered_trade_dates_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(ordered_trade_dates_hash)),
    date_plan_ref JSONB,
    date_plan_hash TEXT CHECK (date_plan_hash IS NULL OR app.advisory_historical_range_is_sha256(date_plan_hash)),
    source_revision_catalog_ref JSONB,
    source_revision_catalog_hash TEXT CHECK (
        source_revision_catalog_hash IS NULL OR app.advisory_historical_range_is_sha256(source_revision_catalog_hash)
    ),
    selection_semantics_version TEXT NOT NULL,
    selection_semantics_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(selection_semantics_hash)),
    list_semantics_version TEXT NOT NULL,
    list_semantics_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(list_semantics_hash)),
    per_program_input_warmup_ranges_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(per_program_input_warmup_ranges_hash)),
    program_count INTEGER NOT NULL CHECK (program_count >= 1),
    trade_date_count INTEGER NOT NULL CHECK (trade_date_count >= 1),
    planned_day_count BIGINT NOT NULL CHECK (planned_day_count >= 1),
    status TEXT NOT NULL CHECK (status IN ('PLANNING', 'QUEUED', 'RUNNING', 'PARTIAL', 'WAITING_INPUT', 'COMPLETED', 'FAILED', 'CANCELLING', 'CANCELLED', 'DEDUPLICATED')),
    waiting_stage TEXT CHECK (waiting_stage IS NULL OR waiting_stage IN ('CATALOG', 'DAY_INPUT')),
    catalog_generation INTEGER NOT NULL DEFAULT 1 CHECK (catalog_generation >= 1),
    catalog_phase TEXT NOT NULL DEFAULT 'DISCOVER' CHECK (catalog_phase IN ('DISCOVER', 'VERIFY')),
    catalog_cursor_ordinal INTEGER NOT NULL DEFAULT 1 CHECK (catalog_cursor_ordinal >= 1),
    catalog_resolved_count BIGINT NOT NULL DEFAULT 0 CHECK (catalog_resolved_count >= 0),
    catalog_unresolved_count BIGINT NOT NULL DEFAULT 0 CHECK (catalog_unresolved_count >= 0),
    catalog_member_chain_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(catalog_member_chain_hash)),
    latest_catalog_checkpoint_ref JSONB,
    latest_catalog_checkpoint_hash TEXT CHECK (
        latest_catalog_checkpoint_hash IS NULL OR app.advisory_historical_range_is_sha256(latest_catalog_checkpoint_hash)
    ),
    row_version BIGINT NOT NULL DEFAULT 1 CHECK (row_version >= 1),
    successful_day_count BIGINT NOT NULL DEFAULT 0 CHECK (successful_day_count >= 0),
    terminal_failed_day_count BIGINT NOT NULL DEFAULT 0 CHECK (terminal_failed_day_count >= 0),
    completed_program_count INTEGER NOT NULL DEFAULT 0 CHECK (completed_program_count >= 0),
    failed_program_count INTEGER NOT NULL DEFAULT 0 CHECK (failed_program_count >= 0),
    waiting_program_count INTEGER NOT NULL DEFAULT 0 CHECK (waiting_program_count >= 0),
    retryable_program_count INTEGER NOT NULL DEFAULT 0 CHECK (retryable_program_count >= 0),
    partial_program_count INTEGER NOT NULL DEFAULT 0 CHECK (partial_program_count >= 0),
    recoverable_program_count INTEGER NOT NULL DEFAULT 0 CHECK (recoverable_program_count >= 0),
    artifact_root_identity_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(artifact_root_identity_hash)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    error_json JSONB,
    request_payload_json JSONB NOT NULL,
    sealed_at TIMESTAMPTZ,
    CHECK (start_trade_date <= end_trade_date),
    CHECK (planned_day_count = program_count::BIGINT * trade_date_count::BIGINT),
    CHECK (successful_day_count + terminal_failed_day_count <= planned_day_count),
    CHECK (completed_program_count <= program_count),
    CHECK (failed_program_count <= program_count),
    CHECK (waiting_program_count <= program_count),
    CHECK (retryable_program_count <= program_count),
    CHECK (partial_program_count <= program_count),
    CHECK (recoverable_program_count <= program_count),
    CHECK (app.advisory_historical_range_artifact_ref_is_valid(
        requirement_plan_ref, 'SOURCE_REQUIREMENT_PLAN', requirement_plan_artifact_hash
    )),
    CHECK ((request_artifact_ref IS NULL) = (request_artifact_hash IS NULL)),
    CHECK ((date_plan_ref IS NULL) = (date_plan_hash IS NULL)),
    CHECK ((source_revision_catalog_ref IS NULL) = (source_revision_catalog_hash IS NULL)),
    CHECK ((latest_catalog_checkpoint_ref IS NULL) = (latest_catalog_checkpoint_hash IS NULL)),
    CHECK ((dedup_receipt_ref IS NULL) = (dedup_receipt_hash IS NULL)),
    CHECK (
        request_artifact_ref IS NULL
        OR app.advisory_historical_range_artifact_ref_is_valid(request_artifact_ref, 'REQUEST', request_artifact_hash)
    ),
    CHECK (
        date_plan_ref IS NULL
        OR app.advisory_historical_range_artifact_ref_is_valid(date_plan_ref, 'DATE_PLAN', date_plan_hash)
    ),
    CHECK (
        source_revision_catalog_ref IS NULL
        OR app.advisory_historical_range_artifact_ref_is_valid(
            source_revision_catalog_ref, 'REQUEST', NULL
        )
    ),
    CHECK (
        latest_catalog_checkpoint_ref IS NULL
        OR app.advisory_historical_range_artifact_ref_is_valid(
            latest_catalog_checkpoint_ref, 'SOURCE_CATALOG_CHECKPOINT', latest_catalog_checkpoint_hash
        )
    ),
    CHECK (
        dedup_receipt_ref IS NULL
        OR app.advisory_historical_range_artifact_ref_is_valid(dedup_receipt_ref, 'RANGE_RECEIPT', dedup_receipt_hash)
    ),
    CHECK (
        (sealed_at IS NULL AND request_payload_sha256 IS NULL AND request_artifact_ref IS NULL
         AND date_plan_ref IS NULL AND source_revision_catalog_ref IS NULL)
        OR
        (sealed_at IS NOT NULL AND request_payload_sha256 IS NOT NULL AND request_artifact_ref IS NOT NULL
         AND date_plan_ref IS NOT NULL AND source_revision_catalog_ref IS NOT NULL)
    ),
    CHECK (
        (status = 'DEDUPLICATED' AND canonical_batch_id IS NOT NULL
         AND deduplicated_request_payload_sha256 IS NOT NULL AND dedup_receipt_ref IS NOT NULL
         AND sealed_at IS NULL)
        OR
        (status <> 'DEDUPLICATED' AND canonical_batch_id IS NULL
         AND deduplicated_request_payload_sha256 IS NULL AND dedup_receipt_ref IS NULL)
    ),
    CHECK (
        (status = 'WAITING_INPUT' AND waiting_stage IS NOT NULL)
        OR (status <> 'WAITING_INPUT' AND waiting_stage IS NULL)
    ),
    CHECK (
        waiting_stage <> 'CATALOG'
        OR (sealed_at IS NULL AND status = 'WAITING_INPUT')
    ),
    CHECK (
        (finished_at IS NOT NULL) = (
            status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'DEDUPLICATED')
            OR (status = 'PARTIAL' AND recoverable_program_count = 0)
        )
    ),
    CHECK (finished_at IS NULL OR started_at IS NOT NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_advisory_historical_range_batch_request_payload
    ON app.advisory_historical_range_batch(request_payload_sha256)
    WHERE request_payload_sha256 IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_advisory_historical_range_batch_supersedes
    ON app.advisory_historical_range_batch(supersedes_batch_id)
    WHERE supersedes_batch_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS app.advisory_historical_range_request_key (
    client_idempotency_key TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL REFERENCES app.advisory_historical_range_batch(batch_id) ON DELETE RESTRICT,
    request_id TEXT NOT NULL,
    user_request_semantic_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(user_request_semantic_hash)),
    planning_identity_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(planning_identity_hash)),
    requirement_plan_ref JSONB NOT NULL,
    requirement_plan_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(requirement_plan_hash)),
    requirement_plan_artifact_hash TEXT NOT NULL CHECK (
        app.advisory_historical_range_is_sha256(requirement_plan_artifact_hash)
    ),
    request_payload_sha256 TEXT CHECK (request_payload_sha256 IS NULL OR app.advisory_historical_range_is_sha256(request_payload_sha256)),
    request_artifact_ref JSONB,
    request_artifact_hash TEXT CHECK (request_artifact_hash IS NULL OR app.advisory_historical_range_is_sha256(request_artifact_hash)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (app.advisory_historical_range_artifact_ref_is_valid(
        requirement_plan_ref, 'SOURCE_REQUIREMENT_PLAN', requirement_plan_artifact_hash
    )),
    CHECK ((request_payload_sha256 IS NULL) = (request_artifact_ref IS NULL)),
    CHECK ((request_artifact_ref IS NULL) = (request_artifact_hash IS NULL)),
    CHECK (
        request_artifact_ref IS NULL
        OR app.advisory_historical_range_artifact_ref_is_valid(request_artifact_ref, 'REQUEST', request_artifact_hash)
    )
);

CREATE TABLE IF NOT EXISTS app.advisory_historical_range_run (
    range_run_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL REFERENCES app.advisory_historical_range_batch(batch_id) ON DELETE RESTRICT,
    research_program_id TEXT NOT NULL,
    source_program_id TEXT,
    source_program_version BIGINT CHECK (source_program_version IS NULL OR source_program_version >= 1),
    source_binding_version_id TEXT,
    package_id TEXT NOT NULL,
    package_version TEXT NOT NULL CHECK (length(btrim(package_version)) BETWEEN 1 AND 80),
    manifest_sha256 TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(manifest_sha256)),
    alpha_mode TEXT NOT NULL CHECK (alpha_mode IN ('single_alpha', 'multi_alpha')),
    program_config_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(program_config_hash)),
    runtime_config_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(runtime_config_hash)),
    review_policy_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(review_policy_hash)),
    style_profile_hash TEXT CHECK (style_profile_hash IS NULL OR app.advisory_historical_range_is_sha256(style_profile_hash)),
    code_release_id TEXT NOT NULL,
    code_release_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(code_release_hash)),
    target_package_asset_root_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(target_package_asset_root_hash)),
    input_warmup_contract_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(input_warmup_contract_hash)),
    admitted_package_projection_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(admitted_package_projection_hash)),
    status TEXT NOT NULL CHECK (status IN ('QUEUED', 'RUNNING', 'WAITING_INPUT', 'RETRYABLE_FAILED', 'PARTIAL', 'COMPLETED', 'FAILED', 'CANCELLED')),
    row_version BIGINT NOT NULL DEFAULT 1 CHECK (row_version >= 1),
    resume_trade_date DATE,
    completed_day_count INTEGER NOT NULL DEFAULT 0 CHECK (completed_day_count >= 0),
    failed_day_count INTEGER NOT NULL DEFAULT 0 CHECK (failed_day_count >= 0),
    waiting_day_count INTEGER NOT NULL DEFAULT 0 CHECK (waiting_day_count >= 0),
    retryable_day_count INTEGER NOT NULL DEFAULT 0 CHECK (retryable_day_count >= 0),
    day_plan_ref JSONB NOT NULL,
    day_plan_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(day_plan_hash)),
    materialized_day_count INTEGER NOT NULL DEFAULT 0 CHECK (materialized_day_count >= 0),
    day_plan_cursor_ordinal INTEGER NOT NULL DEFAULT 0 CHECK (day_plan_cursor_ordinal >= 0),
    cancelled_from_ordinal INTEGER CHECK (cancelled_from_ordinal IS NULL OR cancelled_from_ordinal >= 1),
    first_list_hash TEXT CHECK (first_list_hash IS NULL OR app.advisory_historical_range_is_sha256(first_list_hash)),
    latest_list_hash TEXT CHECK (latest_list_hash IS NULL OR app.advisory_historical_range_is_sha256(latest_list_hash)),
    final_receipt_ref JSONB,
    final_receipt_hash TEXT CHECK (final_receipt_hash IS NULL OR app.advisory_historical_range_is_sha256(final_receipt_hash)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    error_json JSONB,
    frozen_program_json JSONB NOT NULL,
    UNIQUE(batch_id, research_program_id),
    CHECK (
        (source_program_id IS NULL AND source_program_version IS NULL AND source_binding_version_id IS NULL AND research_program_id LIKE 'hrp_%')
        OR
        (source_program_id IS NOT NULL AND source_program_version IS NOT NULL AND source_binding_version_id IS NOT NULL AND research_program_id = source_program_id)
    ),
    CHECK (materialized_day_count = day_plan_cursor_ordinal),
    CHECK (app.advisory_historical_range_artifact_ref_is_valid(day_plan_ref, 'DATE_PLAN', day_plan_hash)),
    CHECK ((final_receipt_ref IS NULL) = (final_receipt_hash IS NULL)),
    CHECK (
        (status IN ('COMPLETED', 'FAILED', 'CANCELLED')) =
        (finished_at IS NOT NULL AND final_receipt_ref IS NOT NULL)
    ),
    CHECK (
        final_receipt_ref IS NULL
        OR app.advisory_historical_range_artifact_ref_is_valid(final_receipt_ref, 'RANGE_RECEIPT', final_receipt_hash)
    ),
    CHECK (finished_at IS NULL OR started_at IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS app.advisory_historical_range_day_run (
    day_run_id TEXT PRIMARY KEY,
    range_run_id TEXT NOT NULL REFERENCES app.advisory_historical_range_run(range_run_id) ON DELETE RESTRICT,
    decision_trade_date DATE NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 1),
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'WAITING_PREVIOUS_DAY', 'RUNNING', 'COMPLETE', 'VALID_NO_CANDIDATE', 'WAITING_INPUT', 'RETRYABLE_FAILED', 'FAILED', 'CANCELLED')),
    row_version BIGINT NOT NULL DEFAULT 1 CHECK (row_version >= 1),
    attempt_no INTEGER NOT NULL DEFAULT 0 CHECK (attempt_no >= 0),
    lease_expires_at TIMESTAMPTZ,
    current_fencing_token BIGINT CHECK (current_fencing_token IS NULL OR current_fencing_token >= 1),
    previous_day_run_id TEXT REFERENCES app.advisory_historical_range_day_run(day_run_id) ON DELETE RESTRICT,
    previous_day_run_hash TEXT CHECK (previous_day_run_hash IS NULL OR app.advisory_historical_range_is_sha256(previous_day_run_hash)),
    previous_list_version_id TEXT,
    previous_list_version_hash TEXT CHECK (previous_list_version_hash IS NULL OR app.advisory_historical_range_is_sha256(previous_list_version_hash)),
    day_input_hash TEXT CHECK (day_input_hash IS NULL OR app.advisory_historical_range_is_sha256(day_input_hash)),
    candidate_artifact_ref JSONB,
    candidate_artifact_hash TEXT CHECK (candidate_artifact_hash IS NULL OR app.advisory_historical_range_is_sha256(candidate_artifact_hash)),
    list_version_id TEXT,
    list_version_hash TEXT CHECK (list_version_hash IS NULL OR app.advisory_historical_range_is_sha256(list_version_hash)),
    day_receipt_ref JSONB,
    day_receipt_hash TEXT CHECK (day_receipt_hash IS NULL OR app.advisory_historical_range_is_sha256(day_receipt_hash)),
    reason_codes_json JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(reason_codes_json) = 'array'),
    error_json JSONB,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(range_run_id, decision_trade_date),
    UNIQUE(range_run_id, ordinal),
    UNIQUE(previous_day_run_id),
    CHECK ((ordinal = 1 AND previous_day_run_id IS NULL) OR (ordinal > 1 AND previous_day_run_id IS NOT NULL)),
    CHECK ((candidate_artifact_ref IS NULL) = (candidate_artifact_hash IS NULL)),
    CHECK ((list_version_id IS NULL) = (list_version_hash IS NULL)),
    CHECK ((day_receipt_ref IS NULL) = (day_receipt_hash IS NULL)),
    CHECK ((status = 'RUNNING' AND lease_expires_at IS NOT NULL AND current_fencing_token IS NOT NULL AND attempt_no >= 1) OR status <> 'RUNNING'),
    CHECK (
        candidate_artifact_ref IS NULL
        OR (
            app.advisory_historical_range_artifact_ref_is_valid(
                candidate_artifact_ref, 'CANDIDATE_ARTIFACT', candidate_artifact_hash
            )
            AND candidate_artifact_ref->>'payload_schema_version' = 'advisory_historical_range_candidate_artifact_payload_v2'
        )
    ),
    CHECK (
        day_receipt_ref IS NULL
        OR app.advisory_historical_range_artifact_ref_is_valid(day_receipt_ref, 'DAY_RECEIPT', day_receipt_hash)
    ),
    CHECK (
        (status IN ('COMPLETE', 'VALID_NO_CANDIDATE')) =
        (candidate_artifact_ref IS NOT NULL AND list_version_id IS NOT NULL AND day_receipt_ref IS NOT NULL)
    ),
    CHECK (
        (finished_at IS NOT NULL) = (status IN ('COMPLETE', 'VALID_NO_CANDIDATE', 'FAILED', 'CANCELLED'))
    ),
    CHECK (finished_at IS NULL OR started_at IS NOT NULL)
);

CREATE TABLE IF NOT EXISTS app.advisory_historical_range_day_attempt (
    attempt_id TEXT PRIMARY KEY,
    day_run_id TEXT NOT NULL REFERENCES app.advisory_historical_range_day_run(day_run_id) ON DELETE RESTRICT,
    attempt_no INTEGER NOT NULL CHECK (attempt_no >= 1),
    worker_id TEXT NOT NULL,
    lease_token TEXT NOT NULL,
    fencing_token BIGINT NOT NULL CHECK (fencing_token >= 1),
    status TEXT NOT NULL CHECK (status IN ('RUNNING', 'COMPLETE', 'VALID_NO_CANDIDATE', 'WAITING_INPUT', 'RETRYABLE_FAILED', 'FAILED', 'CANCELLED')),
    input_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(input_hash)),
    result_hash TEXT CHECK (result_hash IS NULL OR app.advisory_historical_range_is_sha256(result_hash)),
    candidate_artifact_ref JSONB,
    candidate_artifact_hash TEXT CHECK (candidate_artifact_hash IS NULL OR app.advisory_historical_range_is_sha256(candidate_artifact_hash)),
    attempt_receipt_ref JSONB,
    attempt_receipt_hash TEXT CHECK (attempt_receipt_hash IS NULL OR app.advisory_historical_range_is_sha256(attempt_receipt_hash)),
    reason_codes_json JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(reason_codes_json) = 'array'),
    error_json JSONB,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(day_run_id, attempt_no),
    CHECK ((candidate_artifact_ref IS NULL) = (candidate_artifact_hash IS NULL)),
    CHECK ((attempt_receipt_ref IS NULL) = (attempt_receipt_hash IS NULL)),
    CHECK (
        candidate_artifact_ref IS NULL
        OR (
            app.advisory_historical_range_artifact_ref_is_valid(
                candidate_artifact_ref, 'CANDIDATE_ARTIFACT', candidate_artifact_hash
            )
            AND candidate_artifact_ref->>'payload_schema_version' = 'advisory_historical_range_candidate_artifact_payload_v2'
        )
    ),
    CHECK (
        attempt_receipt_ref IS NULL
        OR app.advisory_historical_range_artifact_ref_is_valid(attempt_receipt_ref, 'DAY_RECEIPT', attempt_receipt_hash)
    ),
    CHECK ((status = 'RUNNING' AND finished_at IS NULL) OR (status <> 'RUNNING' AND finished_at IS NOT NULL AND attempt_receipt_ref IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS app.advisory_historical_range_operation (
    operation_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL REFERENCES app.advisory_historical_range_batch(batch_id) ON DELETE RESTRICT,
    operation_type TEXT NOT NULL CHECK (operation_type IN ('CREATE', 'BUILD_SOURCE_CATALOG', 'RESUME', 'CANCEL', 'REFRESH_OUTCOMES', 'BUILD_DATASET_BRIDGE')),
    operation_idempotency_key TEXT NOT NULL,
    request_payload_sha256 TEXT CHECK (
        request_payload_sha256 IS NULL OR app.advisory_historical_range_is_sha256(request_payload_sha256)
    ),
    planning_identity_hash TEXT CHECK (
        planning_identity_hash IS NULL OR app.advisory_historical_range_is_sha256(planning_identity_hash)
    ),
    expected_row_version BIGINT CHECK (expected_row_version IS NULL OR expected_row_version >= 1),
    status TEXT NOT NULL CHECK (status IN ('QUEUED', 'RUNNING', 'WAITING_INPUT', 'COMPLETED', 'RETRYABLE_FAILED', 'FAILED')),
    row_version BIGINT NOT NULL DEFAULT 1 CHECK (row_version >= 1),
    attempt_no INTEGER NOT NULL DEFAULT 0 CHECK (attempt_no >= 0),
    worker_id TEXT,
    lease_token TEXT,
    lease_expires_at TIMESTAMPTZ,
    fencing_token BIGINT CHECK (fencing_token IS NULL OR fencing_token >= 1),
    stable_keyset_cursor_json JSONB,
    catalog_generation INTEGER CHECK (catalog_generation IS NULL OR catalog_generation >= 1),
    catalog_phase TEXT CHECK (catalog_phase IS NULL OR catalog_phase IN ('DISCOVER', 'VERIFY')),
    latest_checkpoint_ref JSONB,
    latest_checkpoint_hash TEXT CHECK (
        latest_checkpoint_hash IS NULL OR app.advisory_historical_range_is_sha256(latest_checkpoint_hash)
    ),
    cumulative_resolved_count BIGINT CHECK (cumulative_resolved_count IS NULL OR cumulative_resolved_count >= 0),
    cumulative_unresolved_count BIGINT CHECK (cumulative_unresolved_count IS NULL OR cumulative_unresolved_count >= 0),
    cumulative_member_chain_hash TEXT CHECK (
        cumulative_member_chain_hash IS NULL OR app.advisory_historical_range_is_sha256(cumulative_member_chain_hash)
    ),
    result_row_version BIGINT CHECK (result_row_version IS NULL OR result_row_version >= 1),
    result_status TEXT,
    result_ref JSONB,
    result_hash TEXT CHECK (result_hash IS NULL OR app.advisory_historical_range_is_sha256(result_hash)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    error_json JSONB,
    UNIQUE(batch_id, operation_idempotency_key),
    CHECK (
        (operation_type IN ('CREATE', 'BUILD_SOURCE_CATALOG') AND planning_identity_hash IS NOT NULL AND request_payload_sha256 IS NULL)
        OR
        (operation_type NOT IN ('CREATE', 'BUILD_SOURCE_CATALOG') AND request_payload_sha256 IS NOT NULL)
    ),
    CHECK ((latest_checkpoint_ref IS NULL) = (latest_checkpoint_hash IS NULL)),
    CHECK (
        latest_checkpoint_ref IS NULL
        OR app.advisory_historical_range_artifact_ref_is_valid(
            latest_checkpoint_ref, 'SOURCE_CATALOG_CHECKPOINT', latest_checkpoint_hash
        )
    ),
    CHECK (
        (operation_type = 'BUILD_SOURCE_CATALOG' AND catalog_generation IS NOT NULL AND catalog_phase IS NOT NULL
         AND cumulative_resolved_count IS NOT NULL AND cumulative_unresolved_count IS NOT NULL
         AND cumulative_member_chain_hash IS NOT NULL)
        OR
        (operation_type <> 'BUILD_SOURCE_CATALOG' AND catalog_generation IS NULL AND catalog_phase IS NULL
         AND latest_checkpoint_ref IS NULL AND cumulative_resolved_count IS NULL
         AND cumulative_unresolved_count IS NULL AND cumulative_member_chain_hash IS NULL)
    ),
    CHECK (
        (status = 'RUNNING' AND worker_id IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL AND fencing_token IS NOT NULL AND attempt_no >= 1)
        OR status <> 'RUNNING'
    ),
    CHECK ((result_ref IS NULL) = (result_hash IS NULL)),
    CHECK ((status IN ('WAITING_INPUT', 'COMPLETED', 'FAILED')) = (result_ref IS NOT NULL)),
    CHECK ((status IN ('COMPLETED', 'FAILED')) = (finished_at IS NOT NULL)),
    CHECK (result_ref IS NULL OR app.advisory_historical_range_artifact_ref_is_valid(result_ref, result_ref->>'artifact_kind', result_hash)),
    CHECK (finished_at IS NULL OR started_at IS NOT NULL)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_advisory_historical_range_operation_running_type
    ON app.advisory_historical_range_operation(batch_id, operation_type)
    WHERE status = 'RUNNING';

CREATE TABLE IF NOT EXISTS app.advisory_historical_range_operation_attempt (
    attempt_id TEXT PRIMARY KEY,
    operation_id TEXT NOT NULL REFERENCES app.advisory_historical_range_operation(operation_id) ON DELETE RESTRICT,
    attempt_no INTEGER NOT NULL CHECK (attempt_no >= 1),
    worker_id TEXT NOT NULL,
    lease_token TEXT NOT NULL,
    fencing_token BIGINT NOT NULL CHECK (fencing_token >= 1),
    status TEXT NOT NULL CHECK (status IN ('RUNNING', 'WAITING_INPUT', 'COMPLETED', 'RETRYABLE_FAILED', 'FAILED')),
    input_cursor_json JSONB,
    result_cursor_json JSONB,
    input_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(input_hash)),
    result_hash TEXT CHECK (result_hash IS NULL OR app.advisory_historical_range_is_sha256(result_hash)),
    attempt_receipt_ref JSONB,
    attempt_receipt_hash TEXT CHECK (attempt_receipt_hash IS NULL OR app.advisory_historical_range_is_sha256(attempt_receipt_hash)),
    reason_codes_json JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(reason_codes_json) = 'array'),
    error_json JSONB,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(operation_id, attempt_no),
    CHECK ((attempt_receipt_ref IS NULL) = (attempt_receipt_hash IS NULL)),
    CHECK (
        attempt_receipt_ref IS NULL
        OR (
            app.advisory_historical_range_artifact_ref_is_valid(
                attempt_receipt_ref, 'RANGE_RECEIPT', attempt_receipt_hash
            )
            OR app.advisory_historical_range_artifact_ref_is_valid(
                attempt_receipt_ref, 'SOURCE_REQUIREMENT_PLAN', attempt_receipt_hash
            )
            OR app.advisory_historical_range_artifact_ref_is_valid(
                attempt_receipt_ref, 'SOURCE_CATALOG_CHECKPOINT', attempt_receipt_hash
            )
        )
    ),
    CHECK ((status = 'RUNNING' AND finished_at IS NULL) OR (status <> 'RUNNING' AND finished_at IS NOT NULL AND attempt_receipt_ref IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS app.advisory_historical_range_candidate (
    candidate_id TEXT PRIMARY KEY,
    day_run_id TEXT NOT NULL REFERENCES app.advisory_historical_range_day_run(day_run_id) ON DELETE RESTRICT,
    symbol TEXT NOT NULL,
    membership_status TEXT NOT NULL CHECK (membership_status IN ('INCLUDED', 'EXCLUDED')),
    alpha_raw_rank INTEGER CHECK (alpha_raw_rank IS NULL OR alpha_raw_rank >= 1),
    alpha_raw_score NUMERIC(38, 12),
    hmm_adjusted_rank INTEGER CHECK (hmm_adjusted_rank IS NULL OR hmm_adjusted_rank >= 1),
    hmm_adjusted_score NUMERIC(38, 12),
    risk_policy_adjusted_rank INTEGER CHECK (risk_policy_adjusted_rank IS NULL OR risk_policy_adjusted_rank >= 1),
    risk_policy_adjusted_score NUMERIC(38, 12),
    selection_effective_rank INTEGER CHECK (selection_effective_rank IS NULL OR selection_effective_rank >= 1),
    selection_effective_score NUMERIC(38, 12),
    advisory_model_rank INTEGER CHECK (advisory_model_rank IS NULL OR advisory_model_rank >= 1),
    advisory_model_score NUMERIC(38, 12),
    component_lineage_json JSONB NOT NULL,
    component_lineage_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(component_lineage_hash)),
    artifact_ref JSONB NOT NULL,
    artifact_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(artifact_hash)),
    candidate_content_hash TEXT NOT NULL UNIQUE CHECK (app.advisory_historical_range_is_sha256(candidate_content_hash)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(day_run_id, symbol),
    CHECK (
        app.advisory_historical_range_artifact_ref_is_valid(artifact_ref, 'CANDIDATE_ARTIFACT', artifact_hash)
        AND artifact_ref->>'payload_schema_version' = 'advisory_historical_range_candidate_artifact_payload_v2'
    ),
    CHECK (
        membership_status = 'EXCLUDED'
        OR (selection_effective_rank IS NOT NULL AND selection_effective_score IS NOT NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_advisory_historical_range_candidate_alpha_rank
    ON app.advisory_historical_range_candidate(day_run_id, alpha_raw_rank)
    WHERE alpha_raw_rank IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_advisory_historical_range_candidate_hmm_rank
    ON app.advisory_historical_range_candidate(day_run_id, hmm_adjusted_rank)
    WHERE hmm_adjusted_rank IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_advisory_historical_range_candidate_risk_rank
    ON app.advisory_historical_range_candidate(day_run_id, risk_policy_adjusted_rank)
    WHERE risk_policy_adjusted_rank IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_advisory_historical_range_candidate_selection_rank
    ON app.advisory_historical_range_candidate(day_run_id, selection_effective_rank)
    WHERE selection_effective_rank IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_advisory_historical_range_candidate_model_rank
    ON app.advisory_historical_range_candidate(day_run_id, advisory_model_rank)
    WHERE advisory_model_rank IS NOT NULL;

CREATE TABLE IF NOT EXISTS app.advisory_historical_range_list_version (
    list_version_id TEXT PRIMARY KEY,
    day_run_id TEXT NOT NULL UNIQUE REFERENCES app.advisory_historical_range_day_run(day_run_id) ON DELETE RESTRICT,
    range_run_id TEXT NOT NULL REFERENCES app.advisory_historical_range_run(range_run_id) ON DELETE RESTRICT,
    previous_list_version_id TEXT UNIQUE REFERENCES app.advisory_historical_range_list_version(list_version_id) ON DELETE RESTRICT,
    previous_list_hash TEXT CHECK (previous_list_hash IS NULL OR app.advisory_historical_range_is_sha256(previous_list_hash)),
    previous_day_receipt_hash TEXT CHECK (previous_day_receipt_hash IS NULL OR app.advisory_historical_range_is_sha256(previous_day_receipt_hash)),
    target_count INTEGER NOT NULL CHECK (target_count >= 1),
    active_count INTEGER NOT NULL CHECK (active_count >= 0),
    enter_count INTEGER NOT NULL CHECK (enter_count >= 0),
    hold_count INTEGER NOT NULL CHECK (hold_count >= 0),
    exit_count INTEGER NOT NULL CHECK (exit_count >= 0),
    watch_count INTEGER NOT NULL CHECK (watch_count >= 0),
    price_timing_policy TEXT NOT NULL CHECK (price_timing_policy = 'PIT_DECISION_THEN_MATURE'),
    summary_json JSONB NOT NULL,
    list_content_hash TEXT NOT NULL UNIQUE CHECK (app.advisory_historical_range_is_sha256(list_content_hash)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (active_count <= target_count),
    CHECK (active_count = enter_count + hold_count),
    CHECK (
        (previous_list_version_id IS NULL AND previous_list_hash IS NULL AND previous_day_receipt_hash IS NULL)
        OR
        (previous_list_version_id IS NOT NULL AND previous_list_hash IS NOT NULL AND previous_day_receipt_hash IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS app.advisory_historical_range_list_item (
    list_item_id TEXT PRIMARY KEY,
    list_version_id TEXT NOT NULL REFERENCES app.advisory_historical_range_list_version(list_version_id) ON DELETE RESTRICT,
    symbol TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('ENTER', 'HOLD', 'EXIT', 'WATCH')),
    rank INTEGER CHECK (rank IS NULL OR rank >= 1),
    score NUMERIC(38, 12),
    reason_codes_json JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(reason_codes_json) = 'array'),
    episode_id TEXT,
    rule_guidance_json JSONB NOT NULL,
    intended_execution_trade_date DATE,
    intended_execution_basis TEXT,
    execution_status TEXT NOT NULL,
    evidence_hash TEXT NOT NULL UNIQUE CHECK (app.advisory_historical_range_is_sha256(evidence_hash)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(list_version_id, symbol),
    CHECK ((action = 'WATCH' AND episode_id IS NULL) OR (action <> 'WATCH' AND episode_id IS NOT NULL)),
    CHECK ((intended_execution_trade_date IS NULL) = (intended_execution_basis IS NULL))
);

CREATE TABLE IF NOT EXISTS app.advisory_historical_range_episode_snapshot (
    episode_snapshot_id TEXT PRIMARY KEY,
    range_run_id TEXT NOT NULL REFERENCES app.advisory_historical_range_run(range_run_id) ON DELETE RESTRICT,
    list_version_id TEXT NOT NULL REFERENCES app.advisory_historical_range_list_version(list_version_id) ON DELETE RESTRICT,
    episode_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    decision_trade_date DATE NOT NULL,
    entry_sequence INTEGER NOT NULL CHECK (entry_sequence >= 1),
    enter_decision_trade_date DATE NOT NULL,
    exit_decision_trade_date DATE,
    recommendation_state TEXT NOT NULL CHECK (recommendation_state IN ('ACTIVE', 'EXITED', 'ACTIVE_AT_RANGE_END')),
    action TEXT NOT NULL CHECK (action IN ('ENTER', 'HOLD', 'EXIT')),
    execution_status TEXT NOT NULL,
    price_quality TEXT NOT NULL,
    weak_rank_confirmation_count INTEGER NOT NULL CHECK (weak_rank_confirmation_count >= 0),
    mark_json JSONB NOT NULL,
    evidence_hash TEXT NOT NULL UNIQUE CHECK (app.advisory_historical_range_is_sha256(evidence_hash)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(range_run_id, episode_id, decision_trade_date),
    UNIQUE(list_version_id, symbol),
    CHECK (enter_decision_trade_date <= decision_trade_date),
    CHECK (exit_decision_trade_date IS NULL OR exit_decision_trade_date = decision_trade_date),
    CHECK ((action = 'EXIT' AND recommendation_state = 'EXITED' AND exit_decision_trade_date IS NOT NULL) OR (action <> 'EXIT' AND recommendation_state <> 'EXITED' AND exit_decision_trade_date IS NULL))
);

CREATE TABLE IF NOT EXISTS app.advisory_historical_range_outcome (
    outcome_version_id TEXT PRIMARY KEY,
    outcome_logical_id TEXT NOT NULL,
    outcome_version INTEGER NOT NULL CHECK (outcome_version >= 1),
    subject_type TEXT NOT NULL CHECK (subject_type IN ('CANDIDATE', 'EPISODE', 'LIST_VERSION', 'RANGE')),
    subject_id TEXT NOT NULL,
    projection TEXT NOT NULL CHECK (projection IN ('RECOMMENDATION', 'EXECUTABLE')),
    horizon_trade_days INTEGER NOT NULL CHECK (horizon_trade_days >= 1),
    label_policy_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(label_policy_hash)),
    source_revision_set_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(source_revision_set_hash)),
    predecessor_outcome_version_id TEXT UNIQUE REFERENCES app.advisory_historical_range_outcome(outcome_version_id) ON DELETE RESTRICT,
    predecessor_outcome_hash TEXT CHECK (predecessor_outcome_hash IS NULL OR app.advisory_historical_range_is_sha256(predecessor_outcome_hash)),
    maturity_status TEXT NOT NULL CHECK (maturity_status IN ('NOT_DUE', 'MATURING', 'COMPLETE', 'CENSORED', 'TERMINAL', 'FAILED')),
    label_as_of_trade_date DATE,
    next_refresh_trade_date DATE,
    entry_execution_evidence_json JSONB,
    exit_execution_evidence_json JSONB,
    benchmark_hash TEXT CHECK (benchmark_hash IS NULL OR app.advisory_historical_range_is_sha256(benchmark_hash)),
    cost_policy_hash TEXT CHECK (cost_policy_hash IS NULL OR app.advisory_historical_range_is_sha256(cost_policy_hash)),
    corporate_action_hash TEXT CHECK (corporate_action_hash IS NULL OR app.advisory_historical_range_is_sha256(corporate_action_hash)),
    calculation_evidence_ref JSONB,
    calculation_evidence_hash TEXT CHECK (calculation_evidence_hash IS NULL OR app.advisory_historical_range_is_sha256(calculation_evidence_hash)),
    outcome_artifact_ref JSONB NOT NULL,
    outcome_artifact_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(outcome_artifact_hash)),
    outcome_json JSONB NOT NULL,
    outcome_content_hash TEXT NOT NULL UNIQUE CHECK (app.advisory_historical_range_is_sha256(outcome_content_hash)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(outcome_logical_id, outcome_version),
    UNIQUE(outcome_logical_id, outcome_version, source_revision_set_hash),
    CHECK ((calculation_evidence_ref IS NULL) = (calculation_evidence_hash IS NULL)),
    CHECK (
        calculation_evidence_ref IS NULL
        OR app.advisory_historical_range_artifact_ref_is_valid(
            calculation_evidence_ref, calculation_evidence_ref->>'artifact_kind', calculation_evidence_hash
        )
    ),
    CHECK (app.advisory_historical_range_artifact_ref_is_valid(outcome_artifact_ref, 'OUTCOME', outcome_artifact_hash)),
    CHECK (
        (outcome_version = 1 AND predecessor_outcome_version_id IS NULL AND predecessor_outcome_hash IS NULL)
        OR
        (outcome_version > 1 AND predecessor_outcome_version_id IS NOT NULL AND predecessor_outcome_hash IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS app.advisory_historical_range_summary (
    summary_id TEXT PRIMARY KEY,
    range_run_id TEXT NOT NULL REFERENCES app.advisory_historical_range_run(range_run_id) ON DELETE RESTRICT,
    summary_version INTEGER NOT NULL CHECK (summary_version >= 1),
    covered_outcome_set_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(covered_outcome_set_hash)),
    predecessor_summary_id TEXT UNIQUE REFERENCES app.advisory_historical_range_summary(summary_id) ON DELETE RESTRICT,
    predecessor_summary_hash TEXT CHECK (predecessor_summary_hash IS NULL OR app.advisory_historical_range_is_sha256(predecessor_summary_hash)),
    summary_artifact_ref JSONB NOT NULL,
    summary_artifact_hash TEXT NOT NULL CHECK (app.advisory_historical_range_is_sha256(summary_artifact_hash)),
    summary_json JSONB NOT NULL,
    summary_content_hash TEXT NOT NULL UNIQUE CHECK (app.advisory_historical_range_is_sha256(summary_content_hash)),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(range_run_id, summary_version),
    CHECK (app.advisory_historical_range_artifact_ref_is_valid(summary_artifact_ref, 'SUMMARY', summary_artifact_hash)),
    CHECK (
        (summary_version = 1 AND predecessor_summary_id IS NULL AND predecessor_summary_hash IS NULL)
        OR
        (summary_version > 1 AND predecessor_summary_id IS NOT NULL AND predecessor_summary_hash IS NOT NULL)
    )
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_advisory_historical_range_day_previous_list'
          AND conrelid = 'app.advisory_historical_range_day_run'::regclass
    ) THEN
        ALTER TABLE app.advisory_historical_range_day_run
            ADD CONSTRAINT fk_advisory_historical_range_day_previous_list
            FOREIGN KEY (previous_list_version_id)
            REFERENCES app.advisory_historical_range_list_version(list_version_id)
            ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_advisory_historical_range_day_list'
          AND conrelid = 'app.advisory_historical_range_day_run'::regclass
    ) THEN
        ALTER TABLE app.advisory_historical_range_day_run
            ADD CONSTRAINT fk_advisory_historical_range_day_list
            FOREIGN KEY (list_version_id)
            REFERENCES app.advisory_historical_range_list_version(list_version_id)
            ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;
    END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS idx_advisory_historical_range_batch_status_updated
    ON app.advisory_historical_range_batch(status, updated_at);
CREATE INDEX IF NOT EXISTS idx_advisory_historical_range_run_batch_status_resume
    ON app.advisory_historical_range_run(batch_id, status, resume_trade_date);
CREATE INDEX IF NOT EXISTS idx_advisory_historical_range_day_run_ordinal_status
    ON app.advisory_historical_range_day_run(range_run_id, ordinal, status);
CREATE INDEX IF NOT EXISTS idx_advisory_historical_range_day_claim
    ON app.advisory_historical_range_day_run(status, lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_advisory_historical_range_operation_claim
    ON app.advisory_historical_range_operation(batch_id, operation_type, status, lease_expires_at);
CREATE INDEX IF NOT EXISTS idx_advisory_historical_range_outcome_refresh
    ON app.advisory_historical_range_outcome(maturity_status, next_refresh_trade_date);
CREATE INDEX IF NOT EXISTS idx_advisory_historical_range_summary_latest
    ON app.advisory_historical_range_summary(range_run_id, summary_version DESC);

CREATE OR REPLACE FUNCTION app.verify_advisory_historical_range_batch_transition()
RETURNS TRIGGER AS $$
DECLARE
    actual_successful_day_count BIGINT;
    actual_terminal_failed_day_count BIGINT;
    actual_completed_program_count INTEGER;
    actual_failed_program_count INTEGER;
    actual_waiting_program_count INTEGER;
    actual_retryable_program_count INTEGER;
    actual_partial_program_count INTEGER;
    actual_recoverable_program_count INTEGER;
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'PLANNING' OR NEW.row_version <> 1 OR NEW.sealed_at IS NOT NULL THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_BATCH_INITIAL_STATE_INVALID';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.batch_id <> OLD.batch_id
       OR NEW.request_id <> OLD.request_id
       OR NEW.client_idempotency_key <> OLD.client_idempotency_key
       OR NEW.user_request_semantic_hash <> OLD.user_request_semantic_hash
       OR NEW.planning_identity_hash <> OLD.planning_identity_hash
       OR NEW.requirement_plan_ref <> OLD.requirement_plan_ref
       OR NEW.requirement_plan_hash <> OLD.requirement_plan_hash
       OR NEW.requirement_plan_artifact_hash <> OLD.requirement_plan_artifact_hash
       OR NEW.start_trade_date <> OLD.start_trade_date
       OR NEW.end_trade_date <> OLD.end_trade_date
       OR NEW.calendar_id <> OLD.calendar_id
       OR NEW.calendar_version <> OLD.calendar_version
       OR NEW.ordered_trade_dates_hash <> OLD.ordered_trade_dates_hash
       OR NEW.selection_semantics_version <> OLD.selection_semantics_version
       OR NEW.selection_semantics_hash <> OLD.selection_semantics_hash
       OR NEW.list_semantics_version <> OLD.list_semantics_version
       OR NEW.list_semantics_hash <> OLD.list_semantics_hash
       OR NEW.per_program_input_warmup_ranges_hash <> OLD.per_program_input_warmup_ranges_hash
       OR NEW.program_count <> OLD.program_count
       OR NEW.trade_date_count <> OLD.trade_date_count
       OR NEW.planned_day_count <> OLD.planned_day_count
       OR NEW.artifact_root_identity_hash <> OLD.artifact_root_identity_hash THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_BATCH_IDENTITY_IMMUTABLE';
    END IF;
    IF OLD.supersedes_batch_id IS NOT NULL
       AND NEW.supersedes_batch_id IS DISTINCT FROM OLD.supersedes_batch_id THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_BATCH_SUPERSEDES_IMMUTABLE';
    END IF;
    IF OLD.supersedes_batch_id IS NULL AND NEW.supersedes_batch_id IS NOT NULL
       AND NOT (OLD.status = 'PLANNING' AND NEW.status = 'QUEUED') THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_BATCH_SUPERSEDES_ASSIGNMENT_INVALID';
    END IF;
    IF OLD.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'DEDUPLICATED') THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_BATCH_TERMINAL_IMMUTABLE';
    END IF;
    IF OLD.status = 'PARTIAL' AND OLD.recoverable_program_count = 0 AND OLD.finished_at IS NOT NULL THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_BATCH_FINISHED_PARTIAL_IMMUTABLE';
    END IF;
    IF NEW.row_version <> OLD.row_version + 1 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_BATCH_ROW_VERSION_INVALID';
    END IF;
    IF OLD.sealed_at IS NOT NULL AND (
        NEW.request_payload_sha256 IS DISTINCT FROM OLD.request_payload_sha256
        OR NEW.request_artifact_ref IS DISTINCT FROM OLD.request_artifact_ref
        OR NEW.request_artifact_hash IS DISTINCT FROM OLD.request_artifact_hash
        OR NEW.date_plan_ref IS DISTINCT FROM OLD.date_plan_ref
        OR NEW.date_plan_hash IS DISTINCT FROM OLD.date_plan_hash
        OR NEW.source_revision_catalog_ref IS DISTINCT FROM OLD.source_revision_catalog_ref
        OR NEW.source_revision_catalog_hash IS DISTINCT FROM OLD.source_revision_catalog_hash
        OR NEW.sealed_at IS DISTINCT FROM OLD.sealed_at
        OR NEW.request_payload_json IS DISTINCT FROM OLD.request_payload_json
    ) THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_BATCH_SEALED_IDENTITY_IMMUTABLE';
    END IF;
    IF OLD.canonical_batch_id IS NOT NULL AND (
        NEW.canonical_batch_id IS DISTINCT FROM OLD.canonical_batch_id
        OR NEW.deduplicated_request_payload_sha256 IS DISTINCT FROM OLD.deduplicated_request_payload_sha256
        OR NEW.dedup_receipt_ref IS DISTINCT FROM OLD.dedup_receipt_ref
        OR NEW.dedup_receipt_hash IS DISTINCT FROM OLD.dedup_receipt_hash
    ) THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_BATCH_DEDUP_IDENTITY_IMMUTABLE';
    END IF;
    IF NEW.catalog_generation < OLD.catalog_generation THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_CATALOG_GENERATION_INVALID';
    END IF;
    IF NEW.catalog_generation = OLD.catalog_generation THEN
        IF NEW.catalog_phase IS DISTINCT FROM OLD.catalog_phase THEN
            IF OLD.catalog_phase <> 'DISCOVER'
               OR NEW.catalog_phase <> 'VERIFY'
               OR NEW.catalog_cursor_ordinal <> 1
               OR NEW.catalog_resolved_count <> 0
               OR NEW.catalog_unresolved_count <> 0
               OR NEW.catalog_member_chain_hash <> '4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945' THEN
                RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_CATALOG_PHASE_INVALID';
            END IF;
        ELSE
            IF NEW.catalog_cursor_ordinal < OLD.catalog_cursor_ordinal
               OR NEW.catalog_resolved_count < OLD.catalog_resolved_count THEN
                RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_CATALOG_CURSOR_INVALID';
            END IF;
            IF OLD.latest_catalog_checkpoint_ref IS NOT NULL
               AND NEW.latest_catalog_checkpoint_ref IS DISTINCT FROM OLD.latest_catalog_checkpoint_ref
               AND NEW.catalog_cursor_ordinal = OLD.catalog_cursor_ordinal
               AND NOT (NEW.status = 'WAITING_INPUT' AND NEW.catalog_unresolved_count > 0) THEN
                RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_CATALOG_CHECKPOINT_INVALID';
            END IF;
        END IF;
    ELSIF NEW.catalog_generation <> OLD.catalog_generation + 1
          OR NEW.catalog_phase <> 'DISCOVER'
          OR NEW.catalog_cursor_ordinal <> 1
          OR NEW.catalog_resolved_count <> 0
          OR NEW.latest_catalog_checkpoint_ref IS NOT NULL THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_CATALOG_RESTART_INVALID';
    END IF;
    IF NEW.status <> OLD.status THEN
        IF OLD.status = 'PLANNING' AND NEW.status NOT IN ('QUEUED', 'WAITING_INPUT', 'DEDUPLICATED', 'FAILED', 'CANCELLED') THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_BATCH_TRANSITION_INVALID';
        ELSIF OLD.status = 'QUEUED' AND NEW.status NOT IN ('RUNNING', 'CANCELLED') THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_BATCH_TRANSITION_INVALID';
        ELSIF OLD.status = 'RUNNING' AND NEW.status NOT IN ('PARTIAL', 'WAITING_INPUT', 'COMPLETED', 'FAILED', 'CANCELLING') THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_BATCH_TRANSITION_INVALID';
        ELSIF OLD.status = 'PARTIAL' AND NEW.status NOT IN ('RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED') THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_BATCH_TRANSITION_INVALID';
        ELSIF OLD.status = 'WAITING_INPUT' AND NEW.status NOT IN ('PLANNING', 'RUNNING', 'FAILED', 'CANCELLED') THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_BATCH_TRANSITION_INVALID';
        ELSIF OLD.status = 'CANCELLING' AND NEW.status <> 'CANCELLED' THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_BATCH_TRANSITION_INVALID';
        END IF;
    END IF;
    IF NEW.status = 'QUEUED' AND (OLD.status <> 'PLANNING' OR NEW.sealed_at IS NULL) THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_BATCH_SEAL_INVALID';
    END IF;
    IF NEW.status = 'DEDUPLICATED' AND OLD.status <> 'PLANNING' THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_BATCH_DEDUP_INVALID';
    END IF;
    IF NEW.sealed_at IS NULL AND NEW.status IN ('PLANNING', 'WAITING_INPUT', 'FAILED', 'CANCELLED') THEN
        NEW.successful_day_count := 0;
        NEW.terminal_failed_day_count := 0;
        NEW.completed_program_count := 0;
        NEW.failed_program_count := 0;
        NEW.waiting_program_count := 0;
        NEW.retryable_program_count := 0;
        NEW.partial_program_count := 0;
        NEW.recoverable_program_count := 0;
        IF NEW.status IN ('FAILED', 'CANCELLED') AND NEW.finished_at IS NULL THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_BATCH_FINISHED_AT_REQUIRED';
        END IF;
        NEW.updated_at := clock_timestamp();
        RETURN NEW;
    END IF;
    IF NEW.status = 'DEDUPLICATED' THEN
        NEW.updated_at := clock_timestamp();
        RETURN NEW;
    END IF;
    SELECT
        COUNT(*) FILTER (WHERE day.status IN ('COMPLETE', 'VALID_NO_CANDIDATE')),
        COUNT(*) FILTER (WHERE day.status = 'FAILED'),
        COUNT(DISTINCT run.range_run_id) FILTER (WHERE run.status = 'COMPLETED'),
        COUNT(DISTINCT run.range_run_id) FILTER (WHERE run.status = 'FAILED'),
        COUNT(DISTINCT run.range_run_id) FILTER (WHERE run.status = 'WAITING_INPUT'),
        COUNT(DISTINCT run.range_run_id) FILTER (WHERE run.status = 'RETRYABLE_FAILED'),
        COUNT(DISTINCT run.range_run_id) FILTER (WHERE run.status = 'PARTIAL'),
        COUNT(DISTINCT run.range_run_id) FILTER (
            WHERE run.status IN ('QUEUED', 'RUNNING', 'WAITING_INPUT', 'RETRYABLE_FAILED', 'PARTIAL')
        )
      INTO actual_successful_day_count, actual_terminal_failed_day_count,
           actual_completed_program_count, actual_failed_program_count,
           actual_waiting_program_count, actual_retryable_program_count,
           actual_partial_program_count, actual_recoverable_program_count
      FROM app.advisory_historical_range_run AS run
      LEFT JOIN app.advisory_historical_range_day_run AS day
        ON day.range_run_id = run.range_run_id
     WHERE run.batch_id = NEW.batch_id;
    IF NEW.successful_day_count <> actual_successful_day_count
       OR NEW.terminal_failed_day_count <> actual_terminal_failed_day_count
       OR NEW.completed_program_count <> actual_completed_program_count
       OR NEW.failed_program_count <> actual_failed_program_count
       OR NEW.waiting_program_count <> actual_waiting_program_count
       OR NEW.retryable_program_count <> actual_retryable_program_count
       OR NEW.partial_program_count <> actual_partial_program_count
       OR NEW.recoverable_program_count <> actual_recoverable_program_count THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_BATCH_CHILD_AGGREGATE_INVALID';
    END IF;
    IF NEW.status = 'FAILED' AND (
        NEW.successful_day_count <> 0
        OR NEW.failed_program_count <> NEW.program_count
        OR NEW.recoverable_program_count <> 0
    ) THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_BATCH_FAILED_AGGREGATE_INVALID';
    END IF;
    IF NEW.status = 'COMPLETED' AND (
        NEW.completed_program_count <> NEW.program_count
        OR NEW.successful_day_count <> NEW.planned_day_count
        OR NEW.failed_program_count <> 0
        OR NEW.waiting_program_count <> 0
        OR NEW.retryable_program_count <> 0
        OR NEW.partial_program_count <> 0
    ) THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_BATCH_COMPLETE_AGGREGATE_INVALID';
    END IF;
    IF NEW.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'DEDUPLICATED') AND NEW.finished_at IS NULL THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_BATCH_FINISHED_AT_REQUIRED';
    END IF;
    IF NEW.status = 'PARTIAL' AND NEW.recoverable_program_count = 0 AND NEW.finished_at IS NULL THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_BATCH_FINISHED_PARTIAL_REQUIRED';
    END IF;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_historical_range_batch_transition ON app.advisory_historical_range_batch;
CREATE TRIGGER trg_verify_advisory_historical_range_batch_transition
    BEFORE INSERT OR UPDATE ON app.advisory_historical_range_batch
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_historical_range_batch_transition();

CREATE OR REPLACE FUNCTION app.verify_advisory_historical_range_supersedes_chain()
RETURNS TRIGGER AS $$
DECLARE
    predecessor_semantic_hash TEXT;
    cursor_id TEXT;
BEGIN
    IF NEW.supersedes_batch_id IS NULL THEN
        RETURN NEW;
    END IF;
    IF NEW.supersedes_batch_id = NEW.batch_id THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_SUPERSEDES_CYCLE';
    END IF;
    SELECT user_request_semantic_hash INTO predecessor_semantic_hash
      FROM app.advisory_historical_range_batch
     WHERE batch_id = NEW.supersedes_batch_id
     FOR KEY SHARE;
    IF NOT FOUND OR predecessor_semantic_hash <> NEW.user_request_semantic_hash THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_SUPERSEDES_SEMANTICS_INVALID';
    END IF;
    cursor_id := NEW.supersedes_batch_id;
    WHILE cursor_id IS NOT NULL LOOP
        IF cursor_id = NEW.batch_id THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_SUPERSEDES_CYCLE';
        END IF;
        SELECT supersedes_batch_id INTO cursor_id
          FROM app.advisory_historical_range_batch
         WHERE batch_id = cursor_id;
    END LOOP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_historical_range_supersedes_chain ON app.advisory_historical_range_batch;
CREATE TRIGGER trg_verify_advisory_historical_range_supersedes_chain
    BEFORE INSERT OR UPDATE OF supersedes_batch_id ON app.advisory_historical_range_batch
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_historical_range_supersedes_chain();

CREATE OR REPLACE FUNCTION app.verify_advisory_historical_range_request_key()
RETURNS TRIGGER AS $$
DECLARE
    batch_row app.advisory_historical_range_batch%ROWTYPE;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF NEW.client_idempotency_key <> OLD.client_idempotency_key
           OR NEW.batch_id <> OLD.batch_id
           OR NEW.request_id <> OLD.request_id
           OR NEW.user_request_semantic_hash <> OLD.user_request_semantic_hash
           OR NEW.planning_identity_hash <> OLD.planning_identity_hash
           OR NEW.requirement_plan_ref <> OLD.requirement_plan_ref
           OR NEW.requirement_plan_hash <> OLD.requirement_plan_hash
           OR NEW.requirement_plan_artifact_hash <> OLD.requirement_plan_artifact_hash THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_REQUEST_KEY_IDENTITY_IMMUTABLE';
        END IF;
        IF OLD.request_payload_sha256 IS NOT NULL AND (
            NEW.request_payload_sha256 IS DISTINCT FROM OLD.request_payload_sha256
            OR NEW.request_artifact_ref IS DISTINCT FROM OLD.request_artifact_ref
            OR NEW.request_artifact_hash IS DISTINCT FROM OLD.request_artifact_hash
        ) THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_REQUEST_KEY_SEALED_IMMUTABLE';
        END IF;
    END IF;
    SELECT * INTO batch_row
      FROM app.advisory_historical_range_batch
     WHERE batch_id = NEW.batch_id
     FOR KEY SHARE;
    IF NOT FOUND
       OR batch_row.request_id <> NEW.request_id
       OR batch_row.user_request_semantic_hash <> NEW.user_request_semantic_hash
       OR batch_row.planning_identity_hash <> NEW.planning_identity_hash
       OR batch_row.requirement_plan_ref <> NEW.requirement_plan_ref
       OR batch_row.requirement_plan_hash <> NEW.requirement_plan_hash
       OR batch_row.requirement_plan_artifact_hash <> NEW.requirement_plan_artifact_hash
       OR batch_row.request_payload_sha256 IS DISTINCT FROM NEW.request_payload_sha256
       OR batch_row.request_artifact_ref IS DISTINCT FROM NEW.request_artifact_ref
       OR batch_row.request_artifact_hash IS DISTINCT FROM NEW.request_artifact_hash THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_REQUEST_KEY_PAYLOAD_INVALID';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_historical_range_request_key ON app.advisory_historical_range_request_key;
CREATE TRIGGER trg_verify_advisory_historical_range_request_key
    BEFORE INSERT OR UPDATE ON app.advisory_historical_range_request_key
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_historical_range_request_key();

CREATE OR REPLACE FUNCTION app.verify_advisory_historical_range_run_transition()
RETURNS TRIGGER AS $$
DECLARE
    expected_day_count INTEGER;
    parent_batch_status TEXT;
    actual_materialized_day_count INTEGER;
    actual_completed_day_count INTEGER;
    actual_failed_day_count INTEGER;
    actual_waiting_day_count INTEGER;
    actual_retryable_day_count INTEGER;
    actual_nonterminal_day_count INTEGER;
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'QUEUED' OR NEW.row_version <> 1 OR NEW.materialized_day_count <> 0 OR NEW.day_plan_cursor_ordinal <> 0 THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_RUN_INITIAL_STATE_INVALID';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.range_run_id <> OLD.range_run_id
       OR NEW.batch_id <> OLD.batch_id
       OR NEW.research_program_id <> OLD.research_program_id
       OR NEW.source_program_id IS DISTINCT FROM OLD.source_program_id
       OR NEW.source_program_version IS DISTINCT FROM OLD.source_program_version
       OR NEW.source_binding_version_id IS DISTINCT FROM OLD.source_binding_version_id
       OR NEW.package_id <> OLD.package_id
       OR NEW.package_version <> OLD.package_version
       OR NEW.manifest_sha256 <> OLD.manifest_sha256
       OR NEW.alpha_mode <> OLD.alpha_mode
       OR NEW.program_config_hash <> OLD.program_config_hash
       OR NEW.runtime_config_hash <> OLD.runtime_config_hash
       OR NEW.review_policy_hash <> OLD.review_policy_hash
       OR NEW.style_profile_hash IS DISTINCT FROM OLD.style_profile_hash
       OR NEW.code_release_id <> OLD.code_release_id
       OR NEW.code_release_hash <> OLD.code_release_hash
       OR NEW.target_package_asset_root_hash <> OLD.target_package_asset_root_hash
       OR NEW.input_warmup_contract_hash <> OLD.input_warmup_contract_hash
       OR NEW.admitted_package_projection_hash <> OLD.admitted_package_projection_hash
       OR NEW.day_plan_ref <> OLD.day_plan_ref
       OR NEW.day_plan_hash <> OLD.day_plan_hash
       OR NEW.frozen_program_json <> OLD.frozen_program_json THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_RUN_IDENTITY_IMMUTABLE';
    END IF;
    IF OLD.status IN ('COMPLETED', 'FAILED', 'CANCELLED') THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_RUN_TERMINAL_IMMUTABLE';
    END IF;
    IF OLD.final_receipt_ref IS NOT NULL AND (
        NEW.final_receipt_ref IS DISTINCT FROM OLD.final_receipt_ref
        OR NEW.final_receipt_hash IS DISTINCT FROM OLD.final_receipt_hash
    ) THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_RUN_RECEIPT_IMMUTABLE';
    END IF;
    IF NEW.row_version <> OLD.row_version + 1 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_RUN_ROW_VERSION_INVALID';
    END IF;
    IF NEW.materialized_day_count < OLD.materialized_day_count
       OR NEW.day_plan_cursor_ordinal < OLD.day_plan_cursor_ordinal
       OR NEW.materialized_day_count <> NEW.day_plan_cursor_ordinal THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_PLAN_CURSOR_INVALID';
    END IF;
    IF OLD.cancelled_from_ordinal IS NOT NULL AND NEW.cancelled_from_ordinal IS DISTINCT FROM OLD.cancelled_from_ordinal THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_CANCELLED_TAIL_IMMUTABLE';
    END IF;
    IF NEW.cancelled_from_ordinal IS NOT NULL AND NEW.cancelled_from_ordinal <= NEW.day_plan_cursor_ordinal THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_CANCELLED_TAIL_INVALID';
    END IF;
    IF NEW.status <> OLD.status THEN
        IF OLD.status = 'QUEUED' AND NEW.status NOT IN ('RUNNING', 'CANCELLED') THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_RUN_TRANSITION_INVALID';
        ELSIF OLD.status = 'RUNNING' AND NEW.status NOT IN ('WAITING_INPUT', 'RETRYABLE_FAILED', 'PARTIAL', 'COMPLETED', 'FAILED', 'CANCELLED') THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_RUN_TRANSITION_INVALID';
        ELSIF OLD.status IN ('WAITING_INPUT', 'RETRYABLE_FAILED', 'PARTIAL') AND NEW.status NOT IN ('RUNNING', 'CANCELLED') THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_RUN_TRANSITION_INVALID';
        END IF;
    END IF;
    SELECT trade_date_count, status INTO expected_day_count, parent_batch_status
      FROM app.advisory_historical_range_batch
     WHERE batch_id = NEW.batch_id;
    IF NEW.materialized_day_count > expected_day_count THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_PLAN_CURSOR_INVALID';
    END IF;
    SELECT COUNT(*)::INTEGER,
           COUNT(*) FILTER (WHERE status IN ('COMPLETE', 'VALID_NO_CANDIDATE'))::INTEGER,
           COUNT(*) FILTER (WHERE status = 'FAILED')::INTEGER,
           COUNT(*) FILTER (WHERE status = 'WAITING_INPUT')::INTEGER,
           COUNT(*) FILTER (WHERE status = 'RETRYABLE_FAILED')::INTEGER,
           COUNT(*) FILTER (
               WHERE status IN ('PENDING', 'WAITING_PREVIOUS_DAY', 'RUNNING', 'WAITING_INPUT', 'RETRYABLE_FAILED')
           )::INTEGER
      INTO actual_materialized_day_count, actual_completed_day_count,
           actual_failed_day_count, actual_waiting_day_count,
           actual_retryable_day_count, actual_nonterminal_day_count
      FROM app.advisory_historical_range_day_run
     WHERE range_run_id = NEW.range_run_id;
    IF NEW.materialized_day_count <> actual_materialized_day_count
       OR NEW.day_plan_cursor_ordinal <> actual_materialized_day_count
       OR NEW.completed_day_count <> actual_completed_day_count
       OR NEW.failed_day_count <> actual_failed_day_count
       OR NEW.waiting_day_count <> actual_waiting_day_count
       OR NEW.retryable_day_count <> actual_retryable_day_count THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_RUN_CHILD_AGGREGATE_INVALID';
    END IF;
    IF NEW.status = 'COMPLETED' AND (
        NEW.materialized_day_count <> expected_day_count
        OR NEW.completed_day_count <> expected_day_count
        OR NEW.failed_day_count <> 0
        OR NEW.waiting_day_count <> 0
        OR NEW.retryable_day_count <> 0
    ) THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_RUN_COMPLETE_AGGREGATE_INVALID';
    END IF;
    IF NEW.status = 'FAILED' AND NEW.completed_day_count <> 0 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_RUN_FAILED_AGGREGATE_INVALID';
    END IF;
    IF NEW.status IN ('COMPLETED', 'FAILED', 'CANCELLED') AND (
        NEW.finished_at IS NULL OR NEW.final_receipt_ref IS NULL OR NEW.final_receipt_hash IS NULL
    ) THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_RUN_TERMINAL_RECEIPT_REQUIRED';
    END IF;
    IF NEW.status IN ('FAILED', 'CANCELLED') AND actual_nonterminal_day_count <> 0 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_RUN_TERMINAL_CHILD_STATE_INVALID';
    END IF;
    IF NEW.status = 'RUNNING' AND parent_batch_status NOT IN ('RUNNING', 'PARTIAL') THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_RUN_PARENT_STATE_INVALID';
    END IF;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_historical_range_run_transition ON app.advisory_historical_range_run;
CREATE TRIGGER trg_verify_advisory_historical_range_run_transition
    BEFORE INSERT OR UPDATE ON app.advisory_historical_range_run
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_historical_range_run_transition();

CREATE OR REPLACE FUNCTION app.verify_advisory_historical_range_day_identity()
RETURNS TRIGGER AS $$
DECLARE
    predecessor app.advisory_historical_range_day_run%ROWTYPE;
    max_ordinal INTEGER;
BEGIN
    SELECT trade_date_count INTO max_ordinal
      FROM app.advisory_historical_range_batch AS b
      JOIN app.advisory_historical_range_run AS r ON r.batch_id = b.batch_id
     WHERE r.range_run_id = NEW.range_run_id;
    IF max_ordinal IS NULL OR NEW.ordinal > max_ordinal THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_PLAN_ENTRY_INVALID';
    END IF;
    IF NEW.ordinal = 1 THEN
        IF NEW.previous_day_run_id IS NOT NULL THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_CHAIN_INVALID';
        END IF;
        RETURN NEW;
    END IF;
    SELECT * INTO predecessor
      FROM app.advisory_historical_range_day_run
     WHERE day_run_id = NEW.previous_day_run_id
     FOR KEY SHARE;
    IF NOT FOUND OR predecessor.range_run_id <> NEW.range_run_id OR predecessor.ordinal <> NEW.ordinal - 1 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_CHAIN_INVALID';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_historical_range_day_identity ON app.advisory_historical_range_day_run;
CREATE TRIGGER trg_verify_advisory_historical_range_day_identity
    BEFORE INSERT ON app.advisory_historical_range_day_run
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_historical_range_day_identity();

CREATE OR REPLACE FUNCTION app.verify_advisory_historical_range_day_transition()
RETURNS TRIGGER AS $$
DECLARE
    predecessor app.advisory_historical_range_day_run%ROWTYPE;
    parent_run_status TEXT;
    parent_batch_status TEXT;
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'PENDING' OR NEW.row_version <> 1 OR NEW.attempt_no <> 0 THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_INITIAL_STATE_INVALID';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.day_run_id <> OLD.day_run_id
       OR NEW.range_run_id <> OLD.range_run_id
       OR NEW.decision_trade_date <> OLD.decision_trade_date
       OR NEW.ordinal <> OLD.ordinal
       OR NEW.previous_day_run_id IS DISTINCT FROM OLD.previous_day_run_id THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_IDENTITY_IMMUTABLE';
    END IF;
    IF OLD.status IN ('COMPLETE', 'VALID_NO_CANDIDATE', 'FAILED', 'CANCELLED') THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_TERMINAL_IMMUTABLE';
    END IF;
    IF NEW.row_version <> OLD.row_version + 1 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_ROW_VERSION_INVALID';
    END IF;
    IF NEW.attempt_no < OLD.attempt_no OR NEW.attempt_no > OLD.attempt_no + 1 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_ATTEMPT_INVALID';
    END IF;
    IF OLD.current_fencing_token IS NOT NULL
       AND NEW.current_fencing_token IS NOT NULL
       AND NEW.current_fencing_token < OLD.current_fencing_token THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_FENCING_INVALID';
    END IF;
    IF OLD.status = 'RUNNING' AND NEW.status = 'RUNNING' THEN
        IF NEW.attempt_no = OLD.attempt_no THEN
            IF NEW.current_fencing_token IS DISTINCT FROM OLD.current_fencing_token
               OR NEW.lease_expires_at IS NULL
               OR NEW.lease_expires_at <= OLD.lease_expires_at THEN
                RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_HEARTBEAT_INVALID';
            END IF;
        ELSIF NEW.attempt_no = OLD.attempt_no + 1 THEN
            IF OLD.lease_expires_at IS NULL
               OR OLD.lease_expires_at > clock_timestamp()
               OR NEW.current_fencing_token IS NULL
               OR OLD.current_fencing_token IS NULL
               OR NEW.current_fencing_token <= OLD.current_fencing_token THEN
                RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_TAKEOVER_INVALID';
            END IF;
            IF NOT EXISTS (
                SELECT 1
                FROM app.advisory_historical_range_day_attempt AS attempt
                WHERE attempt.day_run_id = OLD.day_run_id
                  AND attempt.attempt_no = OLD.attempt_no
                  AND attempt.fencing_token = OLD.current_fencing_token
                  AND attempt.status = 'RETRYABLE_FAILED'
                  AND attempt.attempt_receipt_ref IS NOT NULL
            ) THEN
                RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_TAKEOVER_RECEIPT_REQUIRED';
            END IF;
        ELSE
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_ATTEMPT_INVALID';
        END IF;
    END IF;
    IF OLD.previous_day_run_hash IS NOT NULL AND NEW.previous_day_run_hash IS DISTINCT FROM OLD.previous_day_run_hash THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_CHAIN_IMMUTABLE';
    END IF;
    IF OLD.previous_list_version_id IS NOT NULL AND NEW.previous_list_version_id IS DISTINCT FROM OLD.previous_list_version_id THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_CHAIN_IMMUTABLE';
    END IF;
    IF OLD.previous_list_version_hash IS NOT NULL AND NEW.previous_list_version_hash IS DISTINCT FROM OLD.previous_list_version_hash THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_CHAIN_IMMUTABLE';
    END IF;
    IF OLD.day_input_hash IS NOT NULL AND NEW.day_input_hash IS DISTINCT FROM OLD.day_input_hash THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_INPUT_IMMUTABLE';
    END IF;
    IF OLD.candidate_artifact_ref IS NOT NULL AND (
        NEW.candidate_artifact_ref IS DISTINCT FROM OLD.candidate_artifact_ref
        OR NEW.candidate_artifact_hash IS DISTINCT FROM OLD.candidate_artifact_hash
    ) THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_RESULT_IMMUTABLE';
    END IF;
    IF OLD.list_version_id IS NOT NULL AND (
        NEW.list_version_id IS DISTINCT FROM OLD.list_version_id
        OR NEW.list_version_hash IS DISTINCT FROM OLD.list_version_hash
    ) THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_RESULT_IMMUTABLE';
    END IF;
    IF OLD.day_receipt_ref IS NOT NULL AND (
        NEW.day_receipt_ref IS DISTINCT FROM OLD.day_receipt_ref
        OR NEW.day_receipt_hash IS DISTINCT FROM OLD.day_receipt_hash
    ) THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_RESULT_IMMUTABLE';
    END IF;
    IF NEW.status <> OLD.status THEN
        IF OLD.status = 'PENDING' AND NEW.status NOT IN ('WAITING_PREVIOUS_DAY', 'CANCELLED') THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_TRANSITION_INVALID';
        ELSIF OLD.status = 'WAITING_PREVIOUS_DAY' AND NEW.status NOT IN ('RUNNING', 'CANCELLED') THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_TRANSITION_INVALID';
        ELSIF OLD.status = 'RUNNING' AND NEW.status NOT IN ('COMPLETE', 'VALID_NO_CANDIDATE', 'WAITING_INPUT', 'RETRYABLE_FAILED', 'FAILED', 'CANCELLED') THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_TRANSITION_INVALID';
        ELSIF OLD.status IN ('WAITING_INPUT', 'RETRYABLE_FAILED') AND NEW.status NOT IN ('WAITING_PREVIOUS_DAY', 'CANCELLED') THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_TRANSITION_INVALID';
        END IF;
    END IF;
    IF NEW.status = 'RUNNING' THEN
        IF NEW.lease_expires_at IS NULL OR NEW.current_fencing_token IS NULL THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_RUNNING_CONTRACT_INVALID';
        END IF;
        IF NEW.day_input_hash IS NOT NULL
           OR NEW.candidate_artifact_ref IS NOT NULL
           OR NEW.list_version_id IS NOT NULL
           OR NEW.day_receipt_ref IS NOT NULL THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_CANONICAL_INPUT_PREMATURE';
        END IF;
        SELECT r.status, b.status INTO parent_run_status, parent_batch_status
          FROM app.advisory_historical_range_run AS r
          JOIN app.advisory_historical_range_batch AS b ON b.batch_id = r.batch_id
         WHERE r.range_run_id = NEW.range_run_id
         FOR KEY SHARE OF r, b;
        IF parent_run_status <> 'RUNNING' OR parent_batch_status NOT IN ('RUNNING', 'PARTIAL') THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_PARENT_STATE_INVALID';
        END IF;
        IF NEW.ordinal = 1 THEN
            IF NEW.previous_day_run_hash IS NOT NULL OR NEW.previous_list_version_id IS NOT NULL OR NEW.previous_list_version_hash IS NOT NULL THEN
                RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_CHAIN_INVALID';
            END IF;
        ELSE
            SELECT * INTO predecessor
              FROM app.advisory_historical_range_day_run
             WHERE day_run_id = NEW.previous_day_run_id
             FOR KEY SHARE;
            IF NOT FOUND
               OR predecessor.range_run_id <> NEW.range_run_id
               OR predecessor.ordinal <> NEW.ordinal - 1
               OR predecessor.status NOT IN ('COMPLETE', 'VALID_NO_CANDIDATE')
               OR NEW.previous_day_run_hash IS DISTINCT FROM predecessor.day_receipt_hash
               OR NEW.previous_list_version_id IS DISTINCT FROM predecessor.list_version_id
               OR NEW.previous_list_version_hash IS DISTINCT FROM predecessor.list_version_hash THEN
                RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_CHAIN_INVALID';
            END IF;
        END IF;
    ELSE
        NEW.lease_expires_at := NULL;
    END IF;
    IF NEW.status IN ('COMPLETE', 'VALID_NO_CANDIDATE') AND (
        NEW.day_input_hash IS NULL
        OR NEW.candidate_artifact_ref IS NULL
        OR NEW.candidate_artifact_hash IS NULL
        OR NEW.list_version_id IS NULL
        OR NEW.list_version_hash IS NULL
        OR NEW.day_receipt_ref IS NULL
        OR NEW.day_receipt_hash IS NULL
        OR NEW.finished_at IS NULL
    ) THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_SUCCESS_CONTRACT_INVALID';
    END IF;
    IF NEW.status NOT IN ('COMPLETE', 'VALID_NO_CANDIDATE') AND (
        NEW.day_input_hash IS NOT NULL
        OR NEW.candidate_artifact_ref IS NOT NULL
        OR NEW.list_version_id IS NOT NULL
        OR NEW.day_receipt_ref IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_CANONICAL_RESULT_PREMATURE';
    END IF;
    IF NEW.status IN ('COMPLETE', 'VALID_NO_CANDIDATE', 'FAILED', 'CANCELLED') AND NEW.finished_at IS NULL THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_FINISHED_AT_REQUIRED';
    END IF;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_historical_range_day_transition ON app.advisory_historical_range_day_run;
CREATE TRIGGER trg_verify_advisory_historical_range_day_transition
    BEFORE INSERT OR UPDATE ON app.advisory_historical_range_day_run
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_historical_range_day_transition();

CREATE OR REPLACE FUNCTION app.verify_advisory_historical_range_operation_transition()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.status <> 'QUEUED' OR NEW.row_version <> 1 OR NEW.attempt_no <> 0 THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OPERATION_INITIAL_STATE_INVALID';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.operation_id <> OLD.operation_id
       OR NEW.batch_id <> OLD.batch_id
       OR NEW.operation_type <> OLD.operation_type
       OR NEW.operation_idempotency_key <> OLD.operation_idempotency_key
       OR NEW.request_payload_sha256 IS DISTINCT FROM OLD.request_payload_sha256
       OR NEW.planning_identity_hash IS DISTINCT FROM OLD.planning_identity_hash
       OR NEW.expected_row_version IS DISTINCT FROM OLD.expected_row_version THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OPERATION_IDENTITY_IMMUTABLE';
    END IF;
    IF OLD.status IN ('COMPLETED', 'FAILED') THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OPERATION_TERMINAL_IMMUTABLE';
    END IF;
    IF NEW.row_version <> OLD.row_version + 1 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OPERATION_ROW_VERSION_INVALID';
    END IF;
    IF NEW.attempt_no < OLD.attempt_no OR NEW.attempt_no > OLD.attempt_no + 1 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OPERATION_ATTEMPT_INVALID';
    END IF;
    IF OLD.fencing_token IS NOT NULL AND NEW.fencing_token IS NOT NULL AND NEW.fencing_token < OLD.fencing_token THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OPERATION_FENCING_INVALID';
    END IF;
    IF OLD.status = 'RUNNING' AND NEW.status = 'RUNNING' THEN
        IF NEW.attempt_no = OLD.attempt_no THEN
            IF NEW.fencing_token IS DISTINCT FROM OLD.fencing_token
               OR NEW.worker_id IS DISTINCT FROM OLD.worker_id
               OR NEW.lease_token IS DISTINCT FROM OLD.lease_token
               OR NEW.lease_expires_at IS NULL
               OR NEW.lease_expires_at <= OLD.lease_expires_at THEN
                RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OPERATION_HEARTBEAT_INVALID';
            END IF;
        ELSIF NEW.attempt_no = OLD.attempt_no + 1 THEN
            IF NEW.fencing_token IS NULL
               OR OLD.fencing_token IS NULL
               OR NEW.fencing_token <= OLD.fencing_token THEN
                RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OPERATION_FENCING_INVALID';
            END IF;
            IF EXISTS (
                SELECT 1
                FROM app.advisory_historical_range_operation_attempt AS attempt
                WHERE attempt.operation_id = OLD.operation_id
                  AND attempt.attempt_no = OLD.attempt_no
                  AND attempt.fencing_token = OLD.fencing_token
                  AND attempt.worker_id = OLD.worker_id
                  AND attempt.lease_token = OLD.lease_token
                  AND attempt.status = 'COMPLETED'
                  AND attempt.attempt_receipt_ref IS NOT NULL
            ) THEN
                IF NEW.latest_checkpoint_ref IS NULL
                   OR NEW.latest_checkpoint_ref IS NOT DISTINCT FROM OLD.latest_checkpoint_ref THEN
                    RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OPERATION_ROLLOVER_CHECKPOINT_REQUIRED';
                END IF;
            ELSIF NOT EXISTS (
                SELECT 1
                FROM app.advisory_historical_range_operation_attempt AS attempt
                WHERE attempt.operation_id = OLD.operation_id
                  AND attempt.attempt_no = OLD.attempt_no
                  AND attempt.fencing_token = OLD.fencing_token
                  AND attempt.worker_id = OLD.worker_id
                  AND attempt.lease_token = OLD.lease_token
                  AND attempt.status = 'RETRYABLE_FAILED'
                  AND attempt.attempt_receipt_ref IS NOT NULL
            ) OR (
                NEW.catalog_generation = OLD.catalog_generation
                AND (OLD.lease_expires_at IS NULL OR OLD.lease_expires_at > clock_timestamp())
            ) THEN
                RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OPERATION_TAKEOVER_RECEIPT_REQUIRED';
            END IF;
        ELSE
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OPERATION_ATTEMPT_INVALID';
        END IF;
    END IF;
    IF OLD.status IN ('COMPLETED', 'FAILED') AND OLD.result_ref IS NOT NULL AND (
        NEW.result_ref IS DISTINCT FROM OLD.result_ref
        OR NEW.result_hash IS DISTINCT FROM OLD.result_hash
        OR NEW.result_row_version IS DISTINCT FROM OLD.result_row_version
        OR NEW.result_status IS DISTINCT FROM OLD.result_status
    ) THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OPERATION_RESULT_IMMUTABLE';
    END IF;
    IF NEW.catalog_generation IS DISTINCT FROM OLD.catalog_generation THEN
        IF OLD.catalog_generation IS NULL
           OR NEW.catalog_generation <> OLD.catalog_generation + 1
           OR NEW.catalog_phase <> 'DISCOVER'
           OR NEW.stable_keyset_cursor_json IS NOT NULL
           OR NEW.latest_checkpoint_ref IS NOT NULL
           OR NEW.cumulative_resolved_count <> 0
           OR NEW.cumulative_unresolved_count <> 0 THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OPERATION_CATALOG_RESTART_INVALID';
        END IF;
    ELSIF NEW.operation_type = 'BUILD_SOURCE_CATALOG' THEN
        IF NEW.catalog_phase IS DISTINCT FROM OLD.catalog_phase THEN
            IF OLD.catalog_phase <> 'DISCOVER'
               OR NEW.catalog_phase <> 'VERIFY'
               OR NEW.stable_keyset_cursor_json <> '{"next_requirement_ordinal": 1}'::jsonb
               OR NEW.cumulative_resolved_count <> 0
               OR NEW.cumulative_unresolved_count <> 0
               OR NEW.cumulative_member_chain_hash <> '4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945' THEN
                RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OPERATION_CATALOG_PHASE_INVALID';
            END IF;
        ELSE
            IF COALESCE(NEW.cumulative_resolved_count, 0) < COALESCE(OLD.cumulative_resolved_count, 0) THEN
                RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OPERATION_CATALOG_PROGRESS_INVALID';
            END IF;
            IF OLD.latest_checkpoint_ref IS NOT NULL
               AND NEW.latest_checkpoint_ref IS DISTINCT FROM OLD.latest_checkpoint_ref
               AND NEW.stable_keyset_cursor_json IS NOT DISTINCT FROM OLD.stable_keyset_cursor_json
               AND NOT (NEW.status = 'WAITING_INPUT' AND NEW.cumulative_unresolved_count > 0) THEN
                RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OPERATION_CHECKPOINT_INVALID';
            END IF;
        END IF;
    END IF;
    IF NEW.status <> OLD.status THEN
        IF OLD.status = 'QUEUED' AND NEW.status <> 'RUNNING' THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OPERATION_TRANSITION_INVALID';
        ELSIF OLD.status = 'RUNNING' AND NEW.status NOT IN ('WAITING_INPUT', 'COMPLETED', 'RETRYABLE_FAILED', 'FAILED') THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OPERATION_TRANSITION_INVALID';
        ELSIF OLD.status = 'WAITING_INPUT' AND NEW.status <> 'RUNNING' THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OPERATION_TRANSITION_INVALID';
        ELSIF OLD.status = 'RETRYABLE_FAILED' AND NEW.status <> 'RUNNING' THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OPERATION_TRANSITION_INVALID';
        END IF;
    END IF;
    IF NEW.status = 'RUNNING' THEN
        IF NEW.worker_id IS NULL OR NEW.lease_token IS NULL OR NEW.lease_expires_at IS NULL OR NEW.fencing_token IS NULL THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OPERATION_RUNNING_CONTRACT_INVALID';
        END IF;
    ELSE
        NEW.worker_id := NULL;
        NEW.lease_token := NULL;
        NEW.lease_expires_at := NULL;
    END IF;
    IF NEW.status IN ('WAITING_INPUT', 'COMPLETED', 'FAILED') AND (NEW.result_ref IS NULL OR NEW.result_hash IS NULL) THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OPERATION_TERMINAL_RECEIPT_REQUIRED';
    END IF;
    IF NEW.status IN ('COMPLETED', 'FAILED') AND NEW.finished_at IS NULL THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OPERATION_FINISHED_AT_REQUIRED';
    END IF;
    IF NEW.status NOT IN ('WAITING_INPUT', 'COMPLETED', 'FAILED') AND NEW.result_ref IS NOT NULL THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OPERATION_RESULT_PREMATURE';
    END IF;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_historical_range_operation_transition ON app.advisory_historical_range_operation;
CREATE TRIGGER trg_verify_advisory_historical_range_operation_transition
    BEFORE INSERT OR UPDATE ON app.advisory_historical_range_operation
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_historical_range_operation_transition();

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
       OR predecessor.outcome_content_hash <> NEW.predecessor_outcome_hash THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OUTCOME_REVISION_CHAIN_INVALID';
    END IF;
    IF predecessor.maturity_status NOT IN ('NOT_DUE', 'MATURING') THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OUTCOME_TERMINAL_IMMUTABLE';
    END IF;
    IF predecessor.maturity_status = 'NOT_DUE' AND NEW.maturity_status NOT IN ('MATURING', 'COMPLETE', 'CENSORED', 'TERMINAL', 'FAILED') THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OUTCOME_TRANSITION_INVALID';
    ELSIF predecessor.maturity_status = 'MATURING' AND NEW.maturity_status NOT IN ('COMPLETE', 'CENSORED', 'TERMINAL', 'FAILED') THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OUTCOME_TRANSITION_INVALID';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_historical_range_outcome_revision ON app.advisory_historical_range_outcome;
CREATE TRIGGER trg_verify_advisory_historical_range_outcome_revision
    BEFORE INSERT ON app.advisory_historical_range_outcome
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_historical_range_outcome_revision();

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
       OR predecessor.summary_content_hash <> NEW.predecessor_summary_hash THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_SUMMARY_REVISION_CHAIN_INVALID';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_historical_range_summary_revision ON app.advisory_historical_range_summary;
CREATE TRIGGER trg_verify_advisory_historical_range_summary_revision
    BEFORE INSERT ON app.advisory_historical_range_summary
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_historical_range_summary_revision();

CREATE OR REPLACE FUNCTION app.verify_advisory_historical_range_episode_chain()
RETURNS TRIGGER AS $$
DECLARE
    prior_exit_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO prior_exit_count
      FROM app.advisory_historical_range_episode_snapshot
     WHERE range_run_id = NEW.range_run_id
       AND episode_id = NEW.episode_id
       AND decision_trade_date < NEW.decision_trade_date
       AND recommendation_state = 'EXITED';
    IF prior_exit_count > 0 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_EPISODE_REVIVAL_FORBIDDEN';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_historical_range_episode_chain ON app.advisory_historical_range_episode_snapshot;
CREATE TRIGGER trg_verify_advisory_historical_range_episode_chain
    BEFORE INSERT ON app.advisory_historical_range_episode_snapshot
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_historical_range_episode_chain();

CREATE OR REPLACE FUNCTION app.verify_advisory_historical_range_list_counts()
RETURNS TRIGGER AS $$
DECLARE
    target_id TEXT;
    version app.advisory_historical_range_list_version%ROWTYPE;
    actual_enter INTEGER;
    actual_hold INTEGER;
    actual_exit INTEGER;
    actual_watch INTEGER;
    invalid_episode_projection INTEGER;
    orphan_episode_projection INTEGER;
    invalid_candidate_projection INTEGER;
    included_candidate_count INTEGER;
BEGIN
    target_id := NEW.list_version_id;
    SELECT * INTO version
      FROM app.advisory_historical_range_list_version
     WHERE list_version_id = target_id;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    SELECT
        COUNT(*) FILTER (WHERE action = 'ENTER'),
        COUNT(*) FILTER (WHERE action = 'HOLD'),
        COUNT(*) FILTER (WHERE action = 'EXIT'),
        COUNT(*) FILTER (WHERE action = 'WATCH')
      INTO actual_enter, actual_hold, actual_exit, actual_watch
      FROM app.advisory_historical_range_list_item
     WHERE list_version_id = target_id;
    IF version.enter_count <> actual_enter
       OR version.hold_count <> actual_hold
       OR version.exit_count <> actual_exit
       OR version.watch_count <> actual_watch
       OR version.active_count <> actual_enter + actual_hold
       OR version.active_count > version.target_count THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_LIST_COUNTS_INVALID';
    END IF;
    SELECT COUNT(*) INTO invalid_episode_projection
      FROM app.advisory_historical_range_list_item AS item
      LEFT JOIN app.advisory_historical_range_episode_snapshot AS episode
        ON episode.list_version_id = item.list_version_id
       AND episode.symbol = item.symbol
       AND episode.episode_id = item.episode_id
     WHERE item.list_version_id = target_id
       AND item.action <> 'WATCH'
       AND (
            episode.episode_snapshot_id IS NULL
            OR episode.action <> item.action
            OR (item.action = 'EXIT' AND episode.recommendation_state <> 'EXITED')
            OR (item.action IN ('ENTER', 'HOLD') AND episode.recommendation_state = 'EXITED')
       );
    IF invalid_episode_projection > 0 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_LIST_EPISODE_PROJECTION_INVALID';
    END IF;
    SELECT COUNT(*) INTO orphan_episode_projection
      FROM app.advisory_historical_range_episode_snapshot AS episode
      LEFT JOIN app.advisory_historical_range_list_item AS item
        ON item.list_version_id = episode.list_version_id
       AND item.symbol = episode.symbol
       AND item.episode_id = episode.episode_id
       AND item.action = episode.action
     WHERE episode.list_version_id = target_id
       AND item.list_item_id IS NULL;
    IF orphan_episode_projection > 0 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_ORPHAN_EPISODE_PROJECTION_INVALID';
    END IF;
    SELECT COUNT(*) INTO invalid_candidate_projection
      FROM app.advisory_historical_range_list_item AS item
      LEFT JOIN app.advisory_historical_range_candidate AS candidate
        ON candidate.day_run_id = version.day_run_id
       AND candidate.symbol = item.symbol
       AND candidate.membership_status = 'INCLUDED'
     WHERE item.list_version_id = target_id
       AND item.action IN ('ENTER', 'WATCH')
       AND candidate.candidate_id IS NULL;
    IF invalid_candidate_projection > 0 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_LIST_CANDIDATE_PROJECTION_INVALID';
    END IF;
    SELECT COUNT(*) INTO included_candidate_count
      FROM app.advisory_historical_range_candidate
     WHERE day_run_id = version.day_run_id
       AND membership_status = 'INCLUDED';
    IF version.watch_count > included_candidate_count THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_WATCH_DEPTH_INVALID';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_historical_range_list_counts_version ON app.advisory_historical_range_list_version;
CREATE CONSTRAINT TRIGGER trg_verify_advisory_historical_range_list_counts_version
    AFTER INSERT ON app.advisory_historical_range_list_version
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_historical_range_list_counts();

DROP TRIGGER IF EXISTS trg_verify_advisory_historical_range_list_counts_item ON app.advisory_historical_range_list_item;
CREATE CONSTRAINT TRIGGER trg_verify_advisory_historical_range_list_counts_item
    AFTER INSERT ON app.advisory_historical_range_list_item
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_historical_range_list_counts();

CREATE OR REPLACE FUNCTION app.verify_advisory_historical_range_day_success_facts()
RETURNS TRIGGER AS $$
DECLARE
    persisted_list app.advisory_historical_range_list_version%ROWTYPE;
    included_count INTEGER;
    matching_attempt_count INTEGER;
BEGIN
    IF NEW.status NOT IN ('COMPLETE', 'VALID_NO_CANDIDATE') THEN
        RETURN NULL;
    END IF;
    SELECT * INTO persisted_list
      FROM app.advisory_historical_range_list_version
     WHERE list_version_id = NEW.list_version_id;
    IF NOT FOUND
       OR persisted_list.day_run_id <> NEW.day_run_id
       OR persisted_list.range_run_id <> NEW.range_run_id
       OR persisted_list.list_content_hash <> NEW.list_version_hash THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_LIST_FACT_INVALID';
    END IF;
    IF NEW.status = 'VALID_NO_CANDIDATE' THEN
        SELECT COUNT(*) INTO included_count
          FROM app.advisory_historical_range_candidate
         WHERE day_run_id = NEW.day_run_id
           AND membership_status = 'INCLUDED';
        IF included_count <> 0 THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_VALID_EMPTY_CANDIDATE_INVALID';
        END IF;
    ELSE
        SELECT COUNT(*) INTO included_count
          FROM app.advisory_historical_range_candidate
         WHERE day_run_id = NEW.day_run_id
           AND membership_status = 'INCLUDED';
        IF included_count = 0 THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_COMPLETE_CANDIDATE_INVALID';
        END IF;
    END IF;
    SELECT COUNT(*) INTO matching_attempt_count
      FROM app.advisory_historical_range_day_attempt
     WHERE day_run_id = NEW.day_run_id
       AND attempt_no = NEW.attempt_no
       AND fencing_token = NEW.current_fencing_token
       AND status = NEW.status
       AND input_hash = NEW.day_input_hash
       AND candidate_artifact_ref = NEW.candidate_artifact_ref
       AND candidate_artifact_hash = NEW.candidate_artifact_hash
       AND attempt_receipt_ref = NEW.day_receipt_ref
       AND attempt_receipt_hash = NEW.day_receipt_hash;
    IF matching_attempt_count <> 1 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_ATTEMPT_CLOSURE_INVALID';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_historical_range_day_success_facts ON app.advisory_historical_range_day_run;
CREATE CONSTRAINT TRIGGER trg_verify_advisory_historical_range_day_success_facts
    AFTER INSERT OR UPDATE ON app.advisory_historical_range_day_run
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_historical_range_day_success_facts();

CREATE OR REPLACE FUNCTION app.verify_advisory_historical_range_day_attempt_closure()
RETURNS TRIGGER AS $$
DECLARE
    target_day_id TEXT;
    day app.advisory_historical_range_day_run%ROWTYPE;
    matching_attempt_count INTEGER;
BEGIN
    target_day_id := NEW.day_run_id;
    SELECT * INTO day
      FROM app.advisory_historical_range_day_run
     WHERE day_run_id = target_day_id;
    IF NOT FOUND OR day.status NOT IN (
        'COMPLETE', 'VALID_NO_CANDIDATE', 'WAITING_INPUT',
        'RETRYABLE_FAILED', 'FAILED', 'CANCELLED'
    ) THEN
        RETURN NULL;
    END IF;
    SELECT COUNT(*) INTO matching_attempt_count
      FROM app.advisory_historical_range_day_attempt AS attempt
     WHERE attempt.day_run_id = day.day_run_id
       AND attempt.attempt_no = day.attempt_no
       AND attempt.fencing_token = day.current_fencing_token
       AND attempt.status = day.status
       AND attempt.attempt_receipt_ref IS NOT NULL;
    IF matching_attempt_count <> 1 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_ATTEMPT_CLOSURE_INVALID';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_historical_range_day_attempt_closure_day
    ON app.advisory_historical_range_day_run;
CREATE CONSTRAINT TRIGGER trg_verify_advisory_historical_range_day_attempt_closure_day
    AFTER INSERT OR UPDATE ON app.advisory_historical_range_day_run
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_historical_range_day_attempt_closure();

DROP TRIGGER IF EXISTS trg_ahr_day_attempt_closure_attempt
    ON app.advisory_historical_range_day_attempt;
CREATE CONSTRAINT TRIGGER trg_ahr_day_attempt_closure_attempt
    AFTER INSERT ON app.advisory_historical_range_day_attempt
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_historical_range_day_attempt_closure();

CREATE OR REPLACE FUNCTION app.verify_advisory_historical_range_operation_attempt_closure()
RETURNS TRIGGER AS $$
DECLARE
    target_operation_id TEXT;
    operation app.advisory_historical_range_operation%ROWTYPE;
    matching_attempt_count INTEGER;
BEGIN
    target_operation_id := NEW.operation_id;
    SELECT * INTO operation
      FROM app.advisory_historical_range_operation
     WHERE operation_id = target_operation_id;
    IF NOT FOUND OR operation.status NOT IN ('WAITING_INPUT', 'COMPLETED', 'RETRYABLE_FAILED', 'FAILED') THEN
        RETURN NULL;
    END IF;
    SELECT COUNT(*) INTO matching_attempt_count
      FROM app.advisory_historical_range_operation_attempt AS attempt
     WHERE attempt.operation_id = operation.operation_id
       AND attempt.attempt_no = operation.attempt_no
       AND attempt.fencing_token = operation.fencing_token
       AND attempt.status = operation.status
       AND attempt.attempt_receipt_ref IS NOT NULL;
    IF matching_attempt_count <> 1 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_OPERATION_ATTEMPT_CLOSURE_INVALID';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_ahr_operation_attempt_closure_operation
    ON app.advisory_historical_range_operation;
CREATE CONSTRAINT TRIGGER trg_ahr_operation_attempt_closure_operation
    AFTER INSERT OR UPDATE ON app.advisory_historical_range_operation
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_historical_range_operation_attempt_closure();

DROP TRIGGER IF EXISTS trg_ahr_operation_attempt_closure_attempt
    ON app.advisory_historical_range_operation_attempt;
CREATE CONSTRAINT TRIGGER trg_ahr_operation_attempt_closure_attempt
    AFTER INSERT ON app.advisory_historical_range_operation_attempt
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_historical_range_operation_attempt_closure();

CREATE OR REPLACE FUNCTION app.verify_advisory_historical_range_run_child_aggregate()
RETURNS TRIGGER AS $$
DECLARE
    target_range_run_id TEXT;
    run app.advisory_historical_range_run%ROWTYPE;
    actual_materialized INTEGER;
    actual_completed INTEGER;
    actual_failed INTEGER;
    actual_waiting INTEGER;
    actual_retryable INTEGER;
BEGIN
    IF TG_TABLE_NAME = 'advisory_historical_range_day_run' THEN
        target_range_run_id := NEW.range_run_id;
    ELSE
        target_range_run_id := NEW.range_run_id;
    END IF;
    SELECT * INTO run
      FROM app.advisory_historical_range_run
     WHERE range_run_id = target_range_run_id;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    SELECT COUNT(*)::INTEGER,
           COUNT(*) FILTER (WHERE status IN ('COMPLETE', 'VALID_NO_CANDIDATE'))::INTEGER,
           COUNT(*) FILTER (WHERE status = 'FAILED')::INTEGER,
           COUNT(*) FILTER (WHERE status = 'WAITING_INPUT')::INTEGER,
           COUNT(*) FILTER (WHERE status = 'RETRYABLE_FAILED')::INTEGER
      INTO actual_materialized, actual_completed, actual_failed, actual_waiting, actual_retryable
      FROM app.advisory_historical_range_day_run
     WHERE range_run_id = target_range_run_id;
    IF run.materialized_day_count <> actual_materialized
       OR run.day_plan_cursor_ordinal <> actual_materialized
       OR run.completed_day_count <> actual_completed
       OR run.failed_day_count <> actual_failed
       OR run.waiting_day_count <> actual_waiting
       OR run.retryable_day_count <> actual_retryable THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_RUN_CHILD_AGGREGATE_INVALID';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_historical_range_run_child_aggregate_run
    ON app.advisory_historical_range_run;
CREATE CONSTRAINT TRIGGER trg_verify_advisory_historical_range_run_child_aggregate_run
    AFTER INSERT OR UPDATE ON app.advisory_historical_range_run
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_historical_range_run_child_aggregate();

DROP TRIGGER IF EXISTS trg_verify_advisory_historical_range_run_child_aggregate_day
    ON app.advisory_historical_range_day_run;
CREATE CONSTRAINT TRIGGER trg_verify_advisory_historical_range_run_child_aggregate_day
    AFTER INSERT OR UPDATE ON app.advisory_historical_range_day_run
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_historical_range_run_child_aggregate();

CREATE OR REPLACE FUNCTION app.verify_advisory_historical_range_batch_child_aggregate()
RETURNS TRIGGER AS $$
DECLARE
    target_batch_id TEXT;
    batch app.advisory_historical_range_batch%ROWTYPE;
    actual_successful BIGINT;
    actual_failed_days BIGINT;
    actual_completed_programs INTEGER;
    actual_failed_programs INTEGER;
    actual_waiting_programs INTEGER;
    actual_retryable_programs INTEGER;
    actual_partial_programs INTEGER;
    actual_recoverable_programs INTEGER;
BEGIN
    IF TG_TABLE_NAME = 'advisory_historical_range_batch' THEN
        target_batch_id := NEW.batch_id;
    ELSIF TG_TABLE_NAME = 'advisory_historical_range_run' THEN
        target_batch_id := NEW.batch_id;
    ELSE
        SELECT run.batch_id INTO target_batch_id
          FROM app.advisory_historical_range_run AS run
         WHERE run.range_run_id = NEW.range_run_id;
    END IF;
    SELECT * INTO batch
      FROM app.advisory_historical_range_batch
     WHERE batch_id = target_batch_id;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    SELECT
        COUNT(*) FILTER (WHERE day.status IN ('COMPLETE', 'VALID_NO_CANDIDATE')),
        COUNT(*) FILTER (WHERE day.status = 'FAILED'),
        COUNT(DISTINCT run.range_run_id) FILTER (WHERE run.status = 'COMPLETED'),
        COUNT(DISTINCT run.range_run_id) FILTER (WHERE run.status = 'FAILED'),
        COUNT(DISTINCT run.range_run_id) FILTER (WHERE run.status = 'WAITING_INPUT'),
        COUNT(DISTINCT run.range_run_id) FILTER (WHERE run.status = 'RETRYABLE_FAILED'),
        COUNT(DISTINCT run.range_run_id) FILTER (WHERE run.status = 'PARTIAL'),
        COUNT(DISTINCT run.range_run_id) FILTER (
            WHERE run.status IN ('QUEUED', 'RUNNING', 'WAITING_INPUT', 'RETRYABLE_FAILED', 'PARTIAL')
        )
      INTO actual_successful, actual_failed_days,
           actual_completed_programs, actual_failed_programs,
           actual_waiting_programs, actual_retryable_programs,
           actual_partial_programs, actual_recoverable_programs
      FROM app.advisory_historical_range_run AS run
      LEFT JOIN app.advisory_historical_range_day_run AS day
        ON day.range_run_id = run.range_run_id
     WHERE run.batch_id = target_batch_id;
    IF batch.successful_day_count <> actual_successful
       OR batch.terminal_failed_day_count <> actual_failed_days
       OR batch.completed_program_count <> actual_completed_programs
       OR batch.failed_program_count <> actual_failed_programs
       OR batch.waiting_program_count <> actual_waiting_programs
       OR batch.retryable_program_count <> actual_retryable_programs
       OR batch.partial_program_count <> actual_partial_programs
       OR batch.recoverable_program_count <> actual_recoverable_programs THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_BATCH_CHILD_AGGREGATE_INVALID';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_ahr_batch_child_aggregate_batch
    ON app.advisory_historical_range_batch;
CREATE CONSTRAINT TRIGGER trg_ahr_batch_child_aggregate_batch
    AFTER INSERT OR UPDATE ON app.advisory_historical_range_batch
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_historical_range_batch_child_aggregate();

DROP TRIGGER IF EXISTS trg_verify_advisory_historical_range_batch_child_aggregate_run
    ON app.advisory_historical_range_run;
CREATE CONSTRAINT TRIGGER trg_verify_advisory_historical_range_batch_child_aggregate_run
    AFTER INSERT OR UPDATE ON app.advisory_historical_range_run
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_historical_range_batch_child_aggregate();

DROP TRIGGER IF EXISTS trg_verify_advisory_historical_range_batch_child_aggregate_day
    ON app.advisory_historical_range_day_run;
CREATE CONSTRAINT TRIGGER trg_verify_advisory_historical_range_batch_child_aggregate_day
    AFTER INSERT OR UPDATE ON app.advisory_historical_range_day_run
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_historical_range_batch_child_aggregate();

CREATE OR REPLACE FUNCTION app.reject_advisory_historical_range_fact_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_FACT_IMMUTABLE';
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    relation_name TEXT;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'advisory_historical_range_day_attempt',
        'advisory_historical_range_operation_attempt',
        'advisory_historical_range_candidate',
        'advisory_historical_range_list_version',
        'advisory_historical_range_list_item',
        'advisory_historical_range_episode_snapshot',
        'advisory_historical_range_outcome',
        'advisory_historical_range_summary'
    ] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON app.%I', 'trg_reject_' || relation_name || '_mutation', relation_name);
        EXECUTE format(
            'CREATE TRIGGER %I BEFORE UPDATE OR DELETE ON app.%I FOR EACH ROW EXECUTE FUNCTION app.reject_advisory_historical_range_fact_mutation()',
            'trg_reject_' || relation_name || '_mutation',
            relation_name
        );
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION app.reject_advisory_historical_range_orchestration_delete()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_ORCHESTRATION_DELETE_FORBIDDEN';
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    relation_name TEXT;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'advisory_historical_range_batch',
        'advisory_historical_range_request_key',
        'advisory_historical_range_run',
        'advisory_historical_range_day_run',
        'advisory_historical_range_operation'
    ] LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON app.%I', 'trg_reject_' || relation_name || '_delete', relation_name);
        EXECUTE format(
            'CREATE TRIGGER %I BEFORE DELETE ON app.%I FOR EACH ROW EXECUTE FUNCTION app.reject_advisory_historical_range_orchestration_delete()',
            'trg_reject_' || relation_name || '_delete',
            relation_name
        );
    END LOOP;
END;
$$;

COMMENT ON TABLE app.advisory_historical_range_batch IS
    'Phase 1R finite historical-range research batch. It is not a scheduler, approval, authorization, or trading runtime.';
COMMENT ON TABLE app.advisory_historical_range_request_key IS
    'Append-only client idempotency alias bound to one immutable resolved historical-range request.';
COMMENT ON TABLE app.advisory_historical_range_run IS
    'One isolated Program range run bound to one admitted single-alpha package or one admitted native multi-alpha parent.';
COMMENT ON TABLE app.advisory_historical_range_day_run IS
    'Materialized ordinal day state for one frozen Program/date chain. It never writes ordinary Selection or Advisory rows.';
COMMENT ON TABLE app.advisory_historical_range_day_attempt IS
    'Append-only final or recovery receipt for one historical day attempt and fencing token.';
COMMENT ON TABLE app.advisory_historical_range_operation IS
    'Finite idempotent create/resume/cancel/outcome/dataset operation state; no approval or scheduler semantics.';
COMMENT ON TABLE app.advisory_historical_range_operation_attempt IS
    'Append-only operation attempt receipt, cursor, lease identity, and fencing evidence.';
COMMENT ON TABLE app.advisory_historical_range_candidate IS
    'Append-only Phase 1R candidate depth and five-stage ranking evidence owned by one historical day.';
COMMENT ON TABLE app.advisory_historical_range_list_version IS
    'Append-only bounded ENTER/HOLD/EXIT/WATCH research list. It does not represent positions, orders, or cash.';
COMMENT ON TABLE app.advisory_historical_range_list_item IS
    'Append-only symbol action projection for one bounded historical-range list version.';
COMMENT ON TABLE app.advisory_historical_range_episode_snapshot IS
    'Append-only recommendation episode state at one historical decision date; never a position or order.';
COMMENT ON TABLE app.advisory_historical_range_outcome IS
    'Append-only recommendation/executable outcome revisions; future prices cannot rewrite decision-time list facts.';
COMMENT ON TABLE app.advisory_historical_range_summary IS
    'Append-only versioned summary over an immutable covered outcome set for one Program range run.';

DO $$
DECLARE
    column_row RECORD;
    semantic_comment TEXT;
BEGIN
    FOR column_row IN
        SELECT table_name, column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'app'
          AND table_name LIKE 'advisory_historical_range_%'
        ORDER BY table_name, ordinal_position
    LOOP
        semantic_comment := CASE
            WHEN column_row.column_name LIKE '%_ref' THEN
                'Exact Phase 1R artifact ref JSONB: schema/version/kind/path/payload hash/file hash; nullable only when the owning lifecycle state has no canonical artifact.'
            WHEN column_row.column_name LIKE '%_json' THEN
                'Typed Phase 1R JSONB payload; schema/version/source/quality semantics are closed by the owning Python contract and database invariants.'
            WHEN column_row.column_name LIKE '%_hash' OR column_row.column_name LIKE '%sha256' THEN
                'Immutable lowercase SHA-256 identity or content digest for the named Phase 1R fact.'
            WHEN column_row.column_name LIKE '%_at' OR column_row.column_name LIKE '%_expires_at' THEN
                'Timezone-aware lifecycle timestamp stored as PostgreSQL timestamptz; NULL means the named lifecycle event has not occurred.'
            WHEN column_row.column_name LIKE '%_count' OR column_row.column_name LIKE '%_ordinal' OR column_row.column_name LIKE '%_version' THEN
                'Non-negative Phase 1R count, ordinal, or version governed by table and deferred child-aggregate constraints.'
            WHEN column_row.column_name = 'status' OR column_row.column_name LIKE '%_status' THEN
                'Explicit Phase 1R business state; no implicit success, fallback, approval, or trading meaning.'
            WHEN column_row.column_name LIKE '%_id' THEN
                'Stable Phase 1R identity or foreign-key reference for the named immutable or orchestration entity.'
            ELSE
                'Phase 1R ' || column_row.column_name || ' contract field; source and nullability are defined by the owning table and typed service model.'
        END;
        EXECUTE format(
            'COMMENT ON COLUMN app.%I.%I IS %L',
            column_row.table_name,
            column_row.column_name,
            semantic_comment
        );
    END LOOP;
END;
$$;

COMMIT;

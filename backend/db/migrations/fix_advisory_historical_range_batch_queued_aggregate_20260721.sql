BEGIN;

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
    IF NEW.status = 'QUEUED' AND (
        (NEW.status IS DISTINCT FROM OLD.status AND OLD.status <> 'PLANNING')
        OR NEW.sealed_at IS NULL
    ) THEN
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

COMMENT ON FUNCTION app.verify_advisory_historical_range_batch_transition() IS
    'Validates Phase 1R batch transitions and permits aggregate refresh while a sealed batch remains QUEUED.';

COMMIT;

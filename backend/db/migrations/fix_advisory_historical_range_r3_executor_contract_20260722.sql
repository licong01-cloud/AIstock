BEGIN;

DO $$
BEGIN
    IF to_regclass('app.advisory_historical_range_day_run') IS NULL
       OR to_regclass('app.advisory_historical_range_run') IS NULL
       OR to_regclass('app.advisory_historical_range_batch') IS NULL THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_BASE_RELATION_MISSING';
    END IF;
    IF to_regprocedure('app.verify_advisory_historical_range_day_transition()') IS NULL
       OR to_regprocedure('app.verify_advisory_historical_range_run_transition()') IS NULL
       OR to_regprocedure('app.verify_advisory_historical_range_run_child_aggregate()') IS NULL
       OR to_regprocedure('app.verify_advisory_historical_range_batch_transition()') IS NULL
       OR to_regprocedure('app.verify_advisory_historical_range_batch_child_aggregate()') IS NULL THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_BASE_FUNCTION_MISSING';
    END IF;
END;
$$;

ALTER TABLE app.advisory_historical_range_day_run
    ADD COLUMN IF NOT EXISTS worker_id TEXT,
    ADD COLUMN IF NOT EXISTS lease_token TEXT;

COMMENT ON COLUMN app.advisory_historical_range_day_run.worker_id IS
    'Durable owner of the current R3 day attempt; NULL outside RUNNING.';
COMMENT ON COLUMN app.advisory_historical_range_day_run.lease_token IS
    'Opaque durable lease token for the current R3 day attempt; NULL outside RUNNING.';

DO $$
DECLARE
    exact_column_count INTEGER;
BEGIN
    SELECT COUNT(*)
      INTO exact_column_count
      FROM information_schema.columns
     WHERE table_schema = 'app'
       AND table_name = 'advisory_historical_range_day_run'
       AND column_name IN ('worker_id', 'lease_token')
       AND data_type = 'text'
       AND is_nullable = 'YES'
       AND column_default IS NULL;
    IF exact_column_count <> 2 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_DAY_LEASE_COLUMN_CONTRACT_INVALID:%', exact_column_count;
    END IF;
END;
$$;

DO $$
DECLARE
    candidate_name TEXT;
    candidate_count INTEGER;
BEGIN
    SELECT COUNT(*), MIN(con.conname)
      INTO candidate_count, candidate_name
      FROM pg_constraint AS con
      JOIN pg_class AS rel ON rel.oid = con.conrelid
      JOIN pg_namespace AS ns ON ns.oid = rel.relnamespace
     WHERE ns.nspname = 'app'
       AND rel.relname = 'advisory_historical_range_day_run'
       AND con.contype = 'c'
       AND pg_get_constraintdef(con.oid) LIKE '%status = ''RUNNING''%lease_expires_at IS NOT NULL%current_fencing_token IS NOT NULL%';
    IF candidate_count <> 1 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_DAY_RUNNING_CONSTRAINT_AMBIGUOUS:%', candidate_count;
    END IF;
    EXECUTE format('ALTER TABLE app.advisory_historical_range_day_run DROP CONSTRAINT %I', candidate_name);

    SELECT COUNT(*), MIN(con.conname)
      INTO candidate_count, candidate_name
      FROM pg_constraint AS con
      JOIN pg_class AS rel ON rel.oid = con.conrelid
      JOIN pg_namespace AS ns ON ns.oid = rel.relnamespace
     WHERE ns.nspname = 'app'
       AND rel.relname = 'advisory_historical_range_run'
       AND con.contype = 'c'
       AND pg_get_constraintdef(con.oid) LIKE '%COMPLETED%FAILED%CANCELLED%finished_at IS NOT NULL%final_receipt_ref IS NOT NULL%';
    IF candidate_count <> 1 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_RUN_TERMINAL_CONSTRAINT_AMBIGUOUS:%', candidate_count;
    END IF;
    EXECUTE format('ALTER TABLE app.advisory_historical_range_run DROP CONSTRAINT %I', candidate_name);
END;
$$;

ALTER TABLE app.advisory_historical_range_day_run
    ADD CONSTRAINT ck_ahr_day_r3_running_lease_identity
    CHECK (
        (
            status = 'RUNNING' AND attempt_no >= 1
            AND worker_id IS NOT NULL AND lease_token IS NOT NULL
            AND lease_expires_at IS NOT NULL AND current_fencing_token IS NOT NULL
        )
        OR
        (
            status <> 'RUNNING' AND worker_id IS NULL
            AND lease_token IS NULL AND lease_expires_at IS NULL
        )
    );

ALTER TABLE app.advisory_historical_range_run
    ADD CONSTRAINT ck_ahr_run_r3_terminal_receipt
    CHECK (
        (
            status IN ('COMPLETED', 'FAILED', 'CANCELLED')
            OR (status = 'PARTIAL' AND finished_at IS NOT NULL)
        ) = (
            finished_at IS NOT NULL
            AND final_receipt_ref IS NOT NULL
            AND final_receipt_hash IS NOT NULL
        )
    );

DO $$
DECLARE
    definition TEXT;
    old_fragment TEXT;
    new_fragment TEXT;
    occurrence_count INTEGER;
BEGIN
    definition := pg_get_functiondef('app.verify_advisory_historical_range_day_transition()'::regprocedure);

    old_fragment := $old$IF NEW.status <> 'PENDING' OR NEW.row_version <> 1 OR NEW.attempt_no <> 0 THEN$old$;
    new_fragment := $new$IF NEW.status <> 'PENDING' OR NEW.row_version <> 1 OR NEW.attempt_no <> 0
           OR NEW.worker_id IS NOT NULL OR NEW.lease_token IS NOT NULL OR NEW.lease_expires_at IS NOT NULL THEN$new$;
    IF position(old_fragment IN definition) > 0 THEN
        occurrence_count := (length(definition) - length(replace(definition, old_fragment, ''))) / length(old_fragment);
        IF occurrence_count <> 1 THEN RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_DAY_INSERT_FRAGMENT_AMBIGUOUS'; END IF;
        definition := replace(definition, old_fragment, new_fragment);
    ELSIF position(new_fragment IN definition) = 0 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_DAY_INSERT_PREDECESSOR_UNEXPECTED';
    END IF;
    occurrence_count := (length(definition) - length(replace(definition, new_fragment, ''))) / length(new_fragment);
    IF occurrence_count <> 1 THEN RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_DAY_INSERT_RESULT_AMBIGUOUS:%', occurrence_count; END IF;

    old_fragment := $old$IF NEW.attempt_no = OLD.attempt_no THEN
            IF NEW.current_fencing_token IS DISTINCT FROM OLD.current_fencing_token
               OR NEW.lease_expires_at IS NULL
               OR NEW.lease_expires_at <= OLD.lease_expires_at THEN$old$;
    new_fragment := $new$IF NEW.attempt_no = OLD.attempt_no THEN
            IF NEW.worker_id IS DISTINCT FROM OLD.worker_id
               OR NEW.lease_token IS DISTINCT FROM OLD.lease_token
               OR NEW.current_fencing_token IS DISTINCT FROM OLD.current_fencing_token
               OR NEW.lease_expires_at IS NULL
               OR NEW.lease_expires_at <= OLD.lease_expires_at THEN$new$;
    IF position(old_fragment IN definition) > 0 THEN
        definition := replace(definition, old_fragment, new_fragment);
    ELSIF position(new_fragment IN definition) = 0 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_DAY_HEARTBEAT_PREDECESSOR_UNEXPECTED';
    END IF;
    occurrence_count := (length(definition) - length(replace(definition, new_fragment, ''))) / length(new_fragment);
    IF occurrence_count <> 1 THEN RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_DAY_HEARTBEAT_RESULT_AMBIGUOUS:%', occurrence_count; END IF;

    old_fragment := $old$OR NEW.current_fencing_token <= OLD.current_fencing_token THEN
                RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_TAKEOVER_INVALID';$old$;
    new_fragment := $new$OR NEW.current_fencing_token <= OLD.current_fencing_token
               OR NEW.worker_id IS NULL OR NEW.lease_token IS NULL THEN
                RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_DAY_TAKEOVER_INVALID';$new$;
    IF position(old_fragment IN definition) > 0 THEN
        definition := replace(definition, old_fragment, new_fragment);
    ELSIF position(new_fragment IN definition) = 0 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_DAY_TAKEOVER_PREDECESSOR_UNEXPECTED';
    END IF;
    occurrence_count := (length(definition) - length(replace(definition, new_fragment, ''))) / length(new_fragment);
    IF occurrence_count <> 1 THEN RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_DAY_TAKEOVER_RESULT_AMBIGUOUS:%', occurrence_count; END IF;

    old_fragment := $old$AND attempt.fencing_token = OLD.current_fencing_token
                  AND attempt.status = 'RETRYABLE_FAILED'$old$;
    new_fragment := $new$AND attempt.fencing_token = OLD.current_fencing_token
                  AND attempt.worker_id = OLD.worker_id
                  AND attempt.lease_token = OLD.lease_token
                  AND attempt.status = 'RETRYABLE_FAILED'$new$;
    IF position(old_fragment IN definition) > 0 THEN
        definition := replace(definition, old_fragment, new_fragment);
    ELSIF position(new_fragment IN definition) = 0 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_DAY_TAKEOVER_RECEIPT_PREDECESSOR_UNEXPECTED';
    END IF;
    occurrence_count := (length(definition) - length(replace(definition, new_fragment, ''))) / length(new_fragment);
    IF occurrence_count <> 1 THEN RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_DAY_TAKEOVER_RECEIPT_RESULT_AMBIGUOUS:%', occurrence_count; END IF;

    old_fragment := $old$IF NEW.lease_expires_at IS NULL OR NEW.current_fencing_token IS NULL THEN$old$;
    new_fragment := $new$IF NEW.worker_id IS NULL OR NEW.lease_token IS NULL
           OR NEW.lease_expires_at IS NULL OR NEW.current_fencing_token IS NULL THEN$new$;
    IF position(old_fragment IN definition) > 0 THEN
        definition := replace(definition, old_fragment, new_fragment);
    ELSIF position(new_fragment IN definition) = 0 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_DAY_RUNNING_PREDECESSOR_UNEXPECTED';
    END IF;
    occurrence_count := (length(definition) - length(replace(definition, new_fragment, ''))) / length(new_fragment);
    IF occurrence_count <> 1 THEN RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_DAY_RUNNING_RESULT_AMBIGUOUS:%', occurrence_count; END IF;

    old_fragment := $old$ELSE
        NEW.lease_expires_at := NULL;
    END IF;$old$;
    new_fragment := $new$ELSE
        NEW.worker_id := NULL;
        NEW.lease_token := NULL;
        NEW.lease_expires_at := NULL;
    END IF;$new$;
    IF position(old_fragment IN definition) > 0 THEN
        definition := replace(definition, old_fragment, new_fragment);
    ELSIF position(new_fragment IN definition) = 0 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_DAY_LEASE_CLEAR_PREDECESSOR_UNEXPECTED';
    END IF;
    occurrence_count := (length(definition) - length(replace(definition, new_fragment, ''))) / length(new_fragment);
    IF occurrence_count <> 1 THEN RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_DAY_LEASE_CLEAR_RESULT_AMBIGUOUS:%', occurrence_count; END IF;

    EXECUTE definition;

    definition := pg_get_functiondef('app.verify_advisory_historical_range_run_transition()'::regprocedure);
    old_fragment := $old$IF OLD.status IN ('COMPLETED', 'FAILED', 'CANCELLED') THEN$old$;
    new_fragment := $new$IF OLD.status IN ('COMPLETED', 'FAILED', 'CANCELLED')
       OR (OLD.status = 'PARTIAL' AND OLD.finished_at IS NOT NULL AND OLD.final_receipt_ref IS NOT NULL) THEN$new$;
    IF position(old_fragment IN definition) > 0 THEN
        definition := replace(definition, old_fragment, new_fragment);
    ELSIF position(new_fragment IN definition) = 0 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_RUN_TERMINAL_PREDECESSOR_UNEXPECTED';
    END IF;
    occurrence_count := (length(definition) - length(replace(definition, new_fragment, ''))) / length(new_fragment);
    IF occurrence_count <> 1 THEN RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_RUN_TERMINAL_RESULT_AMBIGUOUS:%', occurrence_count; END IF;

    old_fragment := $old$ELSIF OLD.status IN ('WAITING_INPUT', 'RETRYABLE_FAILED', 'PARTIAL') AND NEW.status NOT IN ('RUNNING', 'CANCELLED') THEN$old$;
    new_fragment := $new$ELSIF OLD.status IN ('WAITING_INPUT', 'RETRYABLE_FAILED') AND NEW.status NOT IN ('RUNNING', 'CANCELLED') THEN
            RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_RUN_TRANSITION_INVALID';
        ELSIF OLD.status = 'PARTIAL' AND NEW.status NOT IN ('RUNNING', 'PARTIAL', 'CANCELLED') THEN$new$;
    IF position(old_fragment IN definition) > 0 THEN
        definition := replace(definition, old_fragment, new_fragment);
    ELSIF position(new_fragment IN definition) = 0 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_RUN_PARTIAL_PREDECESSOR_UNEXPECTED';
    END IF;
    occurrence_count := (length(definition) - length(replace(definition, new_fragment, ''))) / length(new_fragment);
    IF occurrence_count <> 1 THEN RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_RUN_PARTIAL_RESULT_AMBIGUOUS:%', occurrence_count; END IF;

    old_fragment := $old$WHERE status IN ('PENDING', 'WAITING_PREVIOUS_DAY', 'RUNNING', 'WAITING_INPUT', 'RETRYABLE_FAILED')$old$;
    new_fragment := $new$WHERE status IN ('RUNNING', 'WAITING_INPUT', 'RETRYABLE_FAILED')$new$;
    IF position(old_fragment IN definition) > 0 THEN
        definition := replace(definition, old_fragment, new_fragment);
    ELSIF position(new_fragment IN definition) = 0 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_RUN_ACTIVE_CHILD_PREDECESSOR_UNEXPECTED';
    END IF;
    occurrence_count := (length(definition) - length(replace(definition, new_fragment, ''))) / length(new_fragment);
    IF occurrence_count <> 1 THEN RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_RUN_ACTIVE_CHILD_RESULT_AMBIGUOUS:%', occurrence_count; END IF;

    old_fragment := $old$IF NEW.status = 'FAILED' AND NEW.completed_day_count <> 0 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_RUN_FAILED_AGGREGATE_INVALID';
    END IF;$old$;
    new_fragment := $new$IF NEW.status = 'FAILED' AND (
        NEW.completed_day_count <> 0 OR NEW.failed_day_count <> 1 OR actual_nonterminal_day_count <> 0
    ) THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_RUN_FAILED_AGGREGATE_INVALID';
    END IF;
    IF NEW.status = 'PARTIAL' AND NEW.finished_at IS NOT NULL AND (
        NEW.completed_day_count = 0 OR NEW.failed_day_count <> 1 OR actual_nonterminal_day_count <> 0
    ) THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_RUN_TERMINAL_PARTIAL_AGGREGATE_INVALID';
    END IF;$new$;
    IF position(old_fragment IN definition) > 0 THEN
        definition := replace(definition, old_fragment, new_fragment);
    ELSIF position(new_fragment IN definition) = 0 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_RUN_FAILURE_PREDECESSOR_UNEXPECTED';
    END IF;
    occurrence_count := (length(definition) - length(replace(definition, new_fragment, ''))) / length(new_fragment);
    IF occurrence_count <> 1 THEN RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_RUN_FAILURE_RESULT_AMBIGUOUS:%', occurrence_count; END IF;

    old_fragment := $old$IF NEW.status IN ('COMPLETED', 'FAILED', 'CANCELLED') AND (
        NEW.finished_at IS NULL OR NEW.final_receipt_ref IS NULL OR NEW.final_receipt_hash IS NULL
    ) THEN$old$;
    new_fragment := $new$IF (
        NEW.status IN ('COMPLETED', 'FAILED', 'CANCELLED')
        OR (NEW.status = 'PARTIAL' AND NEW.finished_at IS NOT NULL)
    ) AND (NEW.finished_at IS NULL OR NEW.final_receipt_ref IS NULL OR NEW.final_receipt_hash IS NULL) THEN$new$;
    IF position(old_fragment IN definition) > 0 THEN
        definition := replace(definition, old_fragment, new_fragment);
    ELSIF position(new_fragment IN definition) = 0 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_RUN_RECEIPT_PREDECESSOR_UNEXPECTED';
    END IF;
    occurrence_count := (length(definition) - length(replace(definition, new_fragment, ''))) / length(new_fragment);
    IF occurrence_count <> 1 THEN RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_RUN_RECEIPT_RESULT_AMBIGUOUS:%', occurrence_count; END IF;

    old_fragment := $old$IF NEW.status IN ('FAILED', 'CANCELLED') AND actual_nonterminal_day_count <> 0 THEN$old$;
    new_fragment := $new$IF (
        NEW.status IN ('FAILED', 'CANCELLED')
        OR (NEW.status = 'PARTIAL' AND NEW.finished_at IS NOT NULL)
    ) AND actual_nonterminal_day_count <> 0 THEN$new$;
    IF position(old_fragment IN definition) > 0 THEN
        definition := replace(definition, old_fragment, new_fragment);
    ELSIF position(new_fragment IN definition) = 0 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_RUN_CHILD_PREDECESSOR_UNEXPECTED';
    END IF;
    occurrence_count := (length(definition) - length(replace(definition, new_fragment, ''))) / length(new_fragment);
    IF occurrence_count <> 1 THEN RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_RUN_CHILD_RESULT_AMBIGUOUS:%', occurrence_count; END IF;
    EXECUTE definition;

    definition := pg_get_functiondef('app.verify_advisory_historical_range_batch_transition()'::regprocedure);
    old_fragment := $old$ELSIF OLD.status = 'QUEUED' AND NEW.status NOT IN ('RUNNING', 'CANCELLED') THEN$old$;
    new_fragment := $new$ELSIF OLD.status = 'QUEUED' AND NEW.status NOT IN ('RUNNING', 'CANCELLING', 'CANCELLED') THEN$new$;
    IF position(old_fragment IN definition) > 0 THEN
        definition := replace(definition, old_fragment, new_fragment);
    ELSIF position(new_fragment IN definition) = 0 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_BATCH_QUEUED_PREDECESSOR_UNEXPECTED';
    END IF;
    occurrence_count := (length(definition) - length(replace(definition, new_fragment, ''))) / length(new_fragment);
    IF occurrence_count <> 1 THEN RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_BATCH_QUEUED_RESULT_AMBIGUOUS:%', occurrence_count; END IF;
    old_fragment := $old$ELSIF OLD.status = 'PARTIAL' AND NEW.status NOT IN ('RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED') THEN$old$;
    new_fragment := $new$ELSIF OLD.status = 'PARTIAL' AND NEW.status NOT IN ('RUNNING', 'COMPLETED', 'FAILED', 'CANCELLING', 'CANCELLED') THEN$new$;
    IF position(old_fragment IN definition) > 0 THEN
        definition := replace(definition, old_fragment, new_fragment);
    ELSIF position(new_fragment IN definition) = 0 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_BATCH_PARTIAL_PREDECESSOR_UNEXPECTED';
    END IF;
    occurrence_count := (length(definition) - length(replace(definition, new_fragment, ''))) / length(new_fragment);
    IF occurrence_count <> 1 THEN RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_BATCH_PARTIAL_RESULT_AMBIGUOUS:%', occurrence_count; END IF;
    old_fragment := $old$ELSIF OLD.status = 'WAITING_INPUT' AND NEW.status NOT IN ('PLANNING', 'RUNNING', 'FAILED', 'CANCELLED') THEN$old$;
    new_fragment := $new$ELSIF OLD.status = 'WAITING_INPUT' AND NEW.status NOT IN ('PLANNING', 'RUNNING', 'FAILED', 'CANCELLING', 'CANCELLED') THEN$new$;
    IF position(old_fragment IN definition) > 0 THEN
        definition := replace(definition, old_fragment, new_fragment);
    ELSIF position(new_fragment IN definition) = 0 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_BATCH_WAITING_PREDECESSOR_UNEXPECTED';
    END IF;
    occurrence_count := (length(definition) - length(replace(definition, new_fragment, ''))) / length(new_fragment);
    IF occurrence_count <> 1 THEN RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_BATCH_WAITING_RESULT_AMBIGUOUS:%', occurrence_count; END IF;
    old_fragment := $old$WHERE run.status IN ('QUEUED', 'RUNNING', 'WAITING_INPUT', 'RETRYABLE_FAILED', 'PARTIAL')$old$;
    new_fragment := $new$WHERE run.status IN ('QUEUED', 'RUNNING', 'WAITING_INPUT', 'RETRYABLE_FAILED')
               OR (run.status = 'PARTIAL' AND run.finished_at IS NULL)$new$;
    IF position(old_fragment IN definition) > 0 THEN
        definition := replace(definition, old_fragment, new_fragment);
    ELSIF position(new_fragment IN definition) = 0 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_BATCH_RECOVERABLE_PREDECESSOR_UNEXPECTED';
    END IF;
    occurrence_count := (length(definition) - length(replace(definition, new_fragment, ''))) / length(new_fragment);
    IF occurrence_count <> 1 THEN RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_BATCH_RECOVERABLE_RESULT_AMBIGUOUS:%', occurrence_count; END IF;
    EXECUTE definition;

    definition := pg_get_functiondef('app.verify_advisory_historical_range_batch_child_aggregate()'::regprocedure);
    IF position(old_fragment IN definition) > 0 THEN
        definition := replace(definition, old_fragment, new_fragment);
    ELSIF position(new_fragment IN definition) = 0 THEN
        RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_BATCH_CHILD_PREDECESSOR_UNEXPECTED';
    END IF;
    occurrence_count := (length(definition) - length(replace(definition, new_fragment, ''))) / length(new_fragment);
    IF occurrence_count <> 1 THEN RAISE EXCEPTION 'ADVISORY_HISTORICAL_RANGE_R3_BATCH_CHILD_RESULT_AMBIGUOUS:%', occurrence_count; END IF;
    EXECUTE definition;

    -- Its aggregate columns remain unchanged; recreate it so the corrective
    -- migration verifies and replaces all five design-listed functions.
    EXECUTE pg_get_functiondef('app.verify_advisory_historical_range_run_child_aggregate()'::regprocedure);
END;
$$;

COMMENT ON FUNCTION app.verify_advisory_historical_range_day_transition() IS
    'R3 ordered-day worker, lease, fencing, takeover, and terminal transition contract.';
COMMENT ON FUNCTION app.verify_advisory_historical_range_run_transition() IS
    'R3 recoverable versus terminal PARTIAL range-run contract.';

COMMIT;

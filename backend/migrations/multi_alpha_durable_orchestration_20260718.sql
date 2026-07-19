-- Multi-alpha durable orchestration schema (F2 P0-1A).
--
-- This migration is additive and idempotent. It extends the existing
-- combine-backtest result schema; it does not replace the current metrics,
-- scheme-result, LOO, Archive, or QE execution tables.
--
-- Operational rules:
--   * Apply manually after running the sibling .preflight.sql script.
--   * Application code must never auto-apply this file.
--   * Do not export/backup the database as part of this migration workflow;
--     AIstock production already has its normal backup policy.

BEGIN;

DO $preflight$
BEGIN
    IF to_regclass('strategy_pkg.multi_alpha_combine_backtest_run') IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'multi_alpha_durable_base_run_table_missing',
            DETAIL = 'Apply multi_alpha_combine_backtest_result_20260620.sql before this migration.';
    END IF;
    IF to_regclass('strategy_pkg.multi_alpha_combine_backtest_scheme_result') IS NULL
       OR to_regclass('strategy_pkg.multi_alpha_combine_backtest_loo') IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'multi_alpha_durable_base_result_tables_missing',
            DETAIL = 'The existing scheme-result and LOO tables are required for parity and historical backfill.';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM strategy_pkg.multi_alpha_combine_backtest_run
        WHERE status NOT IN ('running', 'succeeded', 'failed', 'queued', 'preparing',
                             'pause_requested', 'paused', 'cancel_requested', 'cancelling',
                             'partial_failed', 'cancelled')
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'multi_alpha_durable_unknown_existing_run_status',
            DETAIL = 'Existing run rows contain a status outside the durable state contract.';
    END IF;
END
$preflight$;

CREATE TABLE IF NOT EXISTS strategy_pkg.multi_alpha_combine_task (
    task_id TEXT PRIMARY KEY,
    task_name TEXT NOT NULL,
    task_type TEXT NOT NULL DEFAULT 'multi_alpha_combine',
    description TEXT,
    roster_hash TEXT NOT NULL,
    roster_json JSONB NOT NULL,
    default_request_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    legacy_group_key TEXT,
    source_kind TEXT NOT NULL,
    created_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_mact_id CHECK (task_id ~ '^mact_'),
    CONSTRAINT ck_mact_type CHECK (task_type = 'multi_alpha_combine'),
    CONSTRAINT ck_mact_source CHECK (source_kind IN ('ui', 'api', 'mcp', 'legacy_backfill')),
    CONSTRAINT ck_mact_roster_json CHECK (jsonb_typeof(roster_json) = 'array'),
    CONSTRAINT ck_mact_default_request_json CHECK (jsonb_typeof(default_request_json) = 'object')
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_mact_legacy_group_key
    ON strategy_pkg.multi_alpha_combine_task(legacy_group_key)
    WHERE legacy_group_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_mact_created_at
    ON strategy_pkg.multi_alpha_combine_task(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_mact_roster_hash
    ON strategy_pkg.multi_alpha_combine_task(roster_hash, created_at DESC);

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    ADD COLUMN IF NOT EXISTS task_id TEXT,
    ADD COLUMN IF NOT EXISTS request_hash TEXT,
    ADD COLUMN IF NOT EXISTS retry_of_run_id TEXT,
    ADD COLUMN IF NOT EXISTS phase TEXT,
    ADD COLUMN IF NOT EXISTS progress_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS row_version BIGINT NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS owner_id TEXT,
    ADD COLUMN IF NOT EXISTS fencing_token BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS pause_requested_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS pause_requested_by TEXT,
    ADD COLUMN IF NOT EXISTS cancel_requested_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS cancel_requested_by TEXT,
    ADD COLUMN IF NOT EXISTS node_parallelism_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS error_code TEXT,
    ADD COLUMN IF NOT EXISTS error_json JSONB;

UPDATE strategy_pkg.multi_alpha_combine_backtest_run
SET progress_json = '{}'::jsonb
WHERE progress_json IS NULL;
UPDATE strategy_pkg.multi_alpha_combine_backtest_run
SET node_parallelism_json = '{}'::jsonb
WHERE node_parallelism_json IS NULL;
UPDATE strategy_pkg.multi_alpha_combine_backtest_run
SET row_version = 1
WHERE row_version IS NULL OR row_version < 1;
UPDATE strategy_pkg.multi_alpha_combine_backtest_run
SET fencing_token = 0
WHERE fencing_token IS NULL OR fencing_token < 0;
UPDATE strategy_pkg.multi_alpha_combine_backtest_run
SET updated_at = created_at
WHERE updated_at IS NULL;

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    ALTER COLUMN progress_json SET DEFAULT '{}'::jsonb,
    ALTER COLUMN progress_json SET NOT NULL,
    ALTER COLUMN node_parallelism_json SET DEFAULT '{}'::jsonb,
    ALTER COLUMN node_parallelism_json SET NOT NULL,
    ALTER COLUMN row_version SET DEFAULT 1,
    ALTER COLUMN row_version SET NOT NULL,
    ALTER COLUMN fencing_token SET DEFAULT 0,
    ALTER COLUMN fencing_token SET NOT NULL,
    ALTER COLUMN updated_at SET DEFAULT NOW(),
    ALTER COLUMN updated_at SET NOT NULL;

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    DROP CONSTRAINT IF EXISTS ck_macb_run_status;
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    ADD CONSTRAINT ck_macb_run_status CHECK (
        status IN ('queued', 'preparing', 'running', 'pause_requested', 'paused',
                   'cancel_requested', 'cancelling', 'succeeded', 'partial_failed',
                   'failed', 'cancelled')
    );

DO $constraints$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_macb_run_task'
          AND conrelid = 'strategy_pkg.multi_alpha_combine_backtest_run'::regclass
    ) THEN
        ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
            ADD CONSTRAINT fk_macb_run_task
            FOREIGN KEY (task_id)
            REFERENCES strategy_pkg.multi_alpha_combine_task(task_id)
            ON DELETE RESTRICT;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_macb_run_retry_of'
          AND conrelid = 'strategy_pkg.multi_alpha_combine_backtest_run'::regclass
    ) THEN
        ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
            ADD CONSTRAINT fk_macb_run_retry_of
            FOREIGN KEY (retry_of_run_id)
            REFERENCES strategy_pkg.multi_alpha_combine_backtest_run(id)
            ON DELETE SET NULL;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_macb_run_progress_json'
          AND conrelid = 'strategy_pkg.multi_alpha_combine_backtest_run'::regclass
    ) THEN
        ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
            ADD CONSTRAINT ck_macb_run_progress_json CHECK (jsonb_typeof(progress_json) = 'object');
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_macb_run_parallelism_json'
          AND conrelid = 'strategy_pkg.multi_alpha_combine_backtest_run'::regclass
    ) THEN
        ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
            ADD CONSTRAINT ck_macb_run_parallelism_json CHECK (jsonb_typeof(node_parallelism_json) = 'object');
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_macb_run_error_json'
          AND conrelid = 'strategy_pkg.multi_alpha_combine_backtest_run'::regclass
    ) THEN
        ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
            ADD CONSTRAINT ck_macb_run_error_json CHECK (error_json IS NULL OR jsonb_typeof(error_json) = 'object');
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_macb_run_row_version'
          AND conrelid = 'strategy_pkg.multi_alpha_combine_backtest_run'::regclass
    ) THEN
        ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
            ADD CONSTRAINT ck_macb_run_row_version CHECK (row_version >= 1);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_macb_run_fencing_token'
          AND conrelid = 'strategy_pkg.multi_alpha_combine_backtest_run'::regclass
    ) THEN
        ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
            ADD CONSTRAINT ck_macb_run_fencing_token CHECK (fencing_token >= 0);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'ck_macb_run_request_hash'
          AND conrelid = 'strategy_pkg.multi_alpha_combine_backtest_run'::regclass
    ) THEN
        ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
            ADD CONSTRAINT ck_macb_run_request_hash CHECK (request_hash IS NULL OR request_hash ~ '^[0-9a-f]{64}$');
    END IF;
END
$constraints$;

CREATE INDEX IF NOT EXISTS idx_macb_run_task_created_at
    ON strategy_pkg.multi_alpha_combine_backtest_run(task_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_macb_run_request_hash
    ON strategy_pkg.multi_alpha_combine_backtest_run(request_hash)
    WHERE request_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_macb_run_claim
    ON strategy_pkg.multi_alpha_combine_backtest_run(status, lease_expires_at, created_at)
    WHERE status IN ('queued', 'preparing', 'running', 'cancel_requested', 'cancelling');

CREATE TABLE IF NOT EXISTS strategy_pkg.multi_alpha_combine_backtest_child (
    child_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES strategy_pkg.multi_alpha_combine_backtest_run(id) ON DELETE CASCADE,
    child_key TEXT NOT NULL,
    child_kind TEXT NOT NULL,
    weighting_scheme TEXT,
    dropped_leg_id TEXT,
    ordinal INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    input_manifest_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    input_manifest_hash TEXT NOT NULL,
    prediction_artifact_uri TEXT,
    prediction_artifact_hash TEXT,
    selected_attempt_id TEXT,
    source_kind TEXT NOT NULL DEFAULT 'runtime',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_macb_child_key UNIQUE (run_id, child_key),
    CONSTRAINT ck_macb_child_id CHECK (child_id ~ '^macbc_'),
    CONSTRAINT ck_macb_child_kind CHECK (child_kind IN ('baseline', 'scheme', 'loo')),
    CONSTRAINT ck_macb_child_status CHECK (
        status IN ('pending', 'materializing', 'queued', 'running', 'reconciling',
                   'cancel_requested', 'cancelling', 'succeeded', 'not_computable',
                   'failed', 'cancelled')
    ),
    CONSTRAINT ck_macb_child_source CHECK (source_kind IN ('runtime', 'legacy_result_backfill')),
    CONSTRAINT ck_macb_child_ordinal CHECK (ordinal >= 0),
    CONSTRAINT ck_macb_child_manifest CHECK (jsonb_typeof(input_manifest_json) = 'object'),
    CONSTRAINT ck_macb_child_manifest_hash CHECK (input_manifest_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_macb_child_prediction_hash CHECK (
        prediction_artifact_hash IS NULL OR prediction_artifact_hash ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_macb_child_kind_fields CHECK (
        (child_kind = 'baseline' AND weighting_scheme IS NULL AND dropped_leg_id IS NULL)
        OR (child_kind = 'scheme' AND weighting_scheme IS NOT NULL AND dropped_leg_id IS NULL)
        OR (child_kind = 'loo' AND weighting_scheme IS NOT NULL AND dropped_leg_id IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_macb_child_run_ordinal
    ON strategy_pkg.multi_alpha_combine_backtest_child(run_id, ordinal, child_id);
CREATE INDEX IF NOT EXISTS idx_macb_child_status
    ON strategy_pkg.multi_alpha_combine_backtest_child(status, updated_at);

CREATE TABLE IF NOT EXISTS strategy_pkg.multi_alpha_combine_backtest_child_attempt (
    attempt_id TEXT PRIMARY KEY,
    child_id TEXT NOT NULL REFERENCES strategy_pkg.multi_alpha_combine_backtest_child(child_id) ON DELETE CASCADE,
    attempt_no INTEGER NOT NULL,
    retry_mode TEXT NOT NULL,
    retry_of_attempt_id TEXT,
    node_id TEXT,
    qe_task_id TEXT,
    qe_loop_id TEXT,
    submission_intent_hash TEXT,
    remote_status TEXT,
    status TEXT NOT NULL DEFAULT 'queued',
    phase TEXT,
    row_version BIGINT NOT NULL DEFAULT 1,
    owner_id TEXT,
    fencing_token BIGINT NOT NULL DEFAULT 0,
    lease_expires_at TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    artifact_manifest_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_manifest_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    error_code TEXT,
    error_json JSONB,
    queued_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    submitted_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_macb_attempt_no UNIQUE (child_id, attempt_no),
    CONSTRAINT ck_macb_attempt_id CHECK (attempt_id ~ '^macba_'),
    CONSTRAINT ck_macb_attempt_no CHECK (attempt_no >= 1),
    CONSTRAINT ck_macb_attempt_retry_mode CHECK (
        retry_mode IN ('initial', 'backtest_only', 'results_only', 'rematerialize_and_backtest')
    ),
    CONSTRAINT ck_macb_attempt_status CHECK (
        status IN ('queued', 'submitting', 'running', 'reconciling', 'succeeded', 'failed', 'cancelled')
    ),
    CONSTRAINT ck_macb_attempt_lineage CHECK (
        (retry_mode = 'initial' AND attempt_no = 1 AND retry_of_attempt_id IS NULL)
        OR (retry_mode <> 'initial' AND attempt_no > 1 AND retry_of_attempt_id IS NOT NULL)
    ),
    CONSTRAINT ck_macb_attempt_remote_identity CHECK (
        (qe_task_id IS NULL AND qe_loop_id IS NULL)
        OR (qe_task_id IS NOT NULL AND qe_loop_id IS NOT NULL)
    ),
    CONSTRAINT ck_macb_attempt_submission_hash CHECK (
        submission_intent_hash IS NULL OR submission_intent_hash ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_macb_attempt_row_version CHECK (row_version >= 1),
    CONSTRAINT ck_macb_attempt_fencing_token CHECK (fencing_token >= 0),
    CONSTRAINT ck_macb_attempt_artifact_manifest CHECK (jsonb_typeof(artifact_manifest_json) = 'object'),
    CONSTRAINT ck_macb_attempt_result_manifest CHECK (jsonb_typeof(result_manifest_json) = 'object'),
    CONSTRAINT ck_macb_attempt_error_json CHECK (error_json IS NULL OR jsonb_typeof(error_json) = 'object')
);

DO $attempt_constraints$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_macb_attempt_retry_of'
          AND conrelid = 'strategy_pkg.multi_alpha_combine_backtest_child_attempt'::regclass
    ) THEN
        ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child_attempt
            ADD CONSTRAINT fk_macb_attempt_retry_of
            FOREIGN KEY (retry_of_attempt_id)
            REFERENCES strategy_pkg.multi_alpha_combine_backtest_child_attempt(attempt_id)
            ON DELETE RESTRICT;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_macb_child_selected_attempt'
          AND conrelid = 'strategy_pkg.multi_alpha_combine_backtest_child'::regclass
    ) THEN
        ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child
            ADD CONSTRAINT fk_macb_child_selected_attempt
            FOREIGN KEY (selected_attempt_id)
            REFERENCES strategy_pkg.multi_alpha_combine_backtest_child_attempt(attempt_id)
            ON DELETE SET NULL;
    END IF;
END
$attempt_constraints$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_macb_attempt_remote_identity
    ON strategy_pkg.multi_alpha_combine_backtest_child_attempt(qe_task_id, qe_loop_id)
    WHERE qe_task_id IS NOT NULL AND qe_loop_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_macb_attempt_child_created
    ON strategy_pkg.multi_alpha_combine_backtest_child_attempt(child_id, attempt_no DESC);
CREATE INDEX IF NOT EXISTS idx_macb_attempt_claim
    ON strategy_pkg.multi_alpha_combine_backtest_child_attempt(status, lease_expires_at, queued_at)
    WHERE status IN ('queued', 'submitting', 'running', 'reconciling');
CREATE INDEX IF NOT EXISTS idx_macb_attempt_node_active
    ON strategy_pkg.multi_alpha_combine_backtest_child_attempt(node_id, status, updated_at)
    WHERE node_id IS NOT NULL AND status IN ('submitting', 'running', 'reconciling');

CREATE TABLE IF NOT EXISTS strategy_pkg.multi_alpha_combine_backtest_event (
    event_id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES strategy_pkg.multi_alpha_combine_backtest_run(id) ON DELETE CASCADE,
    child_id TEXT REFERENCES strategy_pkg.multi_alpha_combine_backtest_child(child_id) ON DELETE CASCADE,
    attempt_id TEXT REFERENCES strategy_pkg.multi_alpha_combine_backtest_child_attempt(attempt_id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    phase TEXT,
    reason_code TEXT,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_macb_event_type CHECK (
        event_type IN ('created', 'claimed', 'submitted', 'status', 'log', 'reconciled',
                       'control', 'result', 'error', 'terminal')
    ),
    CONSTRAINT ck_macb_event_payload CHECK (jsonb_typeof(payload_json) = 'object'),
    CONSTRAINT ck_macb_event_attempt_scope CHECK (attempt_id IS NULL OR child_id IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_macb_event_run_cursor
    ON strategy_pkg.multi_alpha_combine_backtest_event(run_id, event_id);
CREATE INDEX IF NOT EXISTS idx_macb_event_child_cursor
    ON strategy_pkg.multi_alpha_combine_backtest_event(child_id, event_id)
    WHERE child_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_macb_event_attempt_cursor
    ON strategy_pkg.multi_alpha_combine_backtest_event(attempt_id, event_id)
    WHERE attempt_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_macb_event_created_at
    ON strategy_pkg.multi_alpha_combine_backtest_event(created_at, event_id);

COMMENT ON TABLE strategy_pkg.multi_alpha_combine_task IS
    'First-class QE-only multi-alpha combine research task. Status and metrics are derived from runs; no approval semantics.';
COMMENT ON COLUMN strategy_pkg.multi_alpha_combine_task.legacy_group_key IS
    'Stable mapping from the historical task_key_for_run grouping; populated only for legacy backfill.';
COMMENT ON COLUMN strategy_pkg.multi_alpha_combine_task.source_kind IS
    'Creation channel ui/api/mcp/legacy_backfill; it is provenance, not approval.';

COMMENT ON COLUMN strategy_pkg.multi_alpha_combine_backtest_run.task_id IS
    'Durable parent task; nullable only during additive rollout and historical backfill.';
COMMENT ON COLUMN strategy_pkg.multi_alpha_combine_backtest_run.request_hash IS
    'Canonical SHA-256 of the immutable run request. The same run identity may not map to another hash.';
COMMENT ON COLUMN strategy_pkg.multi_alpha_combine_backtest_run.row_version IS
    'Optimistic CAS version incremented by each authoritative durable write.';
COMMENT ON COLUMN strategy_pkg.multi_alpha_combine_backtest_run.fencing_token IS
    'Monotonic ownership generation. A stale worker token cannot commit state.';
COMMENT ON COLUMN strategy_pkg.multi_alpha_combine_backtest_run.reason IS
    'Legacy compatibility summary; structured durable columns and event rows are authoritative.';

COMMENT ON TABLE strategy_pkg.multi_alpha_combine_backtest_child IS
    'Deterministic baseline/scheme/LOO child identity for an existing multi-alpha run; metrics remain in existing result tables or selected attempt manifest.';
COMMENT ON COLUMN strategy_pkg.multi_alpha_combine_backtest_child.input_manifest_hash IS
    'Canonical immutable child input identity. Conflicting payloads must fail instead of overwriting.';
COMMENT ON COLUMN strategy_pkg.multi_alpha_combine_backtest_child.source_kind IS
    'runtime or legacy_result_backfill; historical mappings do not imply a fabricated remote attempt.';

COMMENT ON TABLE strategy_pkg.multi_alpha_combine_backtest_child_attempt IS
    'Append-only execution attempts, including remote QE identity, artifact/result manifests, lease, fencing and CAS version.';
COMMENT ON COLUMN strategy_pkg.multi_alpha_combine_backtest_child_attempt.submission_intent_hash IS
    'Canonical identity persisted before the remote QE side effect; never silently replaced.';
COMMENT ON COLUMN strategy_pkg.multi_alpha_combine_backtest_child_attempt.retry_mode IS
    'Explicit retry semantics; modes are never silently interchanged.';

COMMENT ON TABLE strategy_pkg.multi_alpha_combine_backtest_event IS
    'Authoritative DB event stream for QE multi-alpha state, restart recovery and UI/SSE cursor reads.';
COMMENT ON COLUMN strategy_pkg.multi_alpha_combine_backtest_event.payload_json IS
    'Structured context without credentials; inserted in the same transaction as its authoritative state transition.';

COMMIT;

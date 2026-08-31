-- Multi-alpha P0-2 durable control and child-recovery schema.
--
-- Additive only. Apply manually after the sibling preflight in DEV/production
-- under the separately authorized DDL workflow. Application code must never
-- auto-apply this migration and this workflow never requires a DB export.

BEGIN;

DO $p0_2_base_preflight$
BEGIN
    IF to_regclass('strategy_pkg.multi_alpha_combine_backtest_run') IS NULL
       OR to_regclass('strategy_pkg.multi_alpha_combine_backtest_child') IS NULL
       OR to_regclass('strategy_pkg.multi_alpha_combine_backtest_child_attempt') IS NULL
       OR to_regclass('strategy_pkg.multi_alpha_combine_backtest_event') IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'multi_alpha_p0_2_durable_base_schema_missing',
            DETAIL = 'Apply and verify multi_alpha_durable_orchestration_20260718.sql first.';
    END IF;
END
$p0_2_base_preflight$;

-- Recovery identity belongs to the existing run row. Whole-run retry rows may
-- still have retry_of_run_id with recovery_kind NULL; only child-targeted
-- successor rows carry the non-empty scope tuple below.
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    ADD COLUMN IF NOT EXISTS recovery_kind TEXT,
    ADD COLUMN IF NOT EXISTS recovery_scope_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS recovery_scope_hash TEXT,
    ADD COLUMN IF NOT EXISTS execution_identity_json JSONB,
    ADD COLUMN IF NOT EXISTS execution_identity_hash TEXT,
    ADD COLUMN IF NOT EXISTS execution_identity_evidence_json JSONB;

UPDATE strategy_pkg.multi_alpha_combine_backtest_run
SET recovery_scope_json = '{}'::jsonb
WHERE recovery_scope_json IS NULL;

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    ALTER COLUMN recovery_scope_json SET DEFAULT '{}'::jsonb,
    ALTER COLUMN recovery_scope_json SET NOT NULL;

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    DROP CONSTRAINT IF EXISTS ck_macb_run_status;
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    ADD CONSTRAINT ck_macb_run_status CHECK (
        status IN ('queued', 'preparing', 'running', 'pause_requested', 'paused',
                   'cancel_requested', 'cancelling', 'succeeded', 'partial_failed',
                   'partial_recovered', 'failed', 'cancelled')
    );

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    DROP CONSTRAINT IF EXISTS ck_macb_run_recovery_kind;
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    ADD CONSTRAINT ck_macb_run_recovery_kind CHECK (
        recovery_kind IS NULL OR recovery_kind = 'child_targeted'
    );
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    DROP CONSTRAINT IF EXISTS ck_macb_run_recovery_scope_json;
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    ADD CONSTRAINT ck_macb_run_recovery_scope_json CHECK (
        jsonb_typeof(recovery_scope_json) = 'object'
    );
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    DROP CONSTRAINT IF EXISTS ck_macb_run_recovery_scope_hash;
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    ADD CONSTRAINT ck_macb_run_recovery_scope_hash CHECK (
        recovery_scope_hash IS NULL OR recovery_scope_hash ~ '^[0-9a-f]{64}$'
    );
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    DROP CONSTRAINT IF EXISTS ck_macb_run_recovery_tuple;
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    ADD CONSTRAINT ck_macb_run_recovery_tuple CHECK (
        (recovery_kind IS NULL
         AND recovery_scope_json = '{}'::jsonb
         AND recovery_scope_hash IS NULL)
        OR
        (recovery_kind = 'child_targeted'
         AND retry_of_run_id IS NOT NULL
         AND recovery_scope_json <> '{}'::jsonb
         AND recovery_scope_hash IS NOT NULL)
    );
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    DROP CONSTRAINT IF EXISTS ck_macb_run_partial_recovered_kind;
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    ADD CONSTRAINT ck_macb_run_partial_recovered_kind CHECK (
        status <> 'partial_recovered' OR recovery_kind = 'child_targeted'
    );
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    DROP CONSTRAINT IF EXISTS ck_macb_run_execution_identity;
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    ADD CONSTRAINT ck_macb_run_execution_identity CHECK (
        (execution_identity_json IS NULL AND execution_identity_hash IS NULL)
        OR (jsonb_typeof(execution_identity_json) = 'object'
            AND execution_identity_hash ~ '^[0-9a-f]{64}$')
    );
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    DROP CONSTRAINT IF EXISTS ck_macb_run_execution_identity_evidence;
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    ADD CONSTRAINT ck_macb_run_execution_identity_evidence CHECK (
        execution_identity_evidence_json IS NULL
        OR (jsonb_typeof(execution_identity_evidence_json) = 'object'
            AND jsonb_typeof(execution_identity_evidence_json->'complete') = 'boolean')
    );
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    DROP CONSTRAINT IF EXISTS ck_macb_run_execution_identity_evidence_alignment;
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    ADD CONSTRAINT ck_macb_run_execution_identity_evidence_alignment CHECK (
        execution_identity_evidence_json IS NULL
        OR (
            (execution_identity_json IS NOT NULL
             AND execution_identity_evidence_json->>'complete' = 'true')
            OR
            (execution_identity_json IS NULL
             AND execution_identity_evidence_json->>'complete' = 'false')
        )
    );

CREATE INDEX IF NOT EXISTS idx_macb_run_recovery_source
    ON strategy_pkg.multi_alpha_combine_backtest_run(retry_of_run_id, created_at DESC)
    WHERE recovery_kind = 'child_targeted';

-- Source lineage freezes recovery provenance without keeping the deleted
-- source row alive. The direct FK is navigation only and is deliberately
-- SET NULL on an explicit terminal DELETE.
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child
    ADD COLUMN IF NOT EXISTS source_child_id TEXT,
    ADD COLUMN IF NOT EXISTS execution_disposition TEXT NOT NULL DEFAULT 'execute',
    ADD COLUMN IF NOT EXISTS source_lineage_json JSONB,
    ADD COLUMN IF NOT EXISTS source_lineage_hash TEXT;

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child
    DROP CONSTRAINT IF EXISTS ck_macb_child_status;
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child
    ADD CONSTRAINT ck_macb_child_status CHECK (
        status IN ('pending', 'materializing', 'queued', 'running', 'reconciling',
                   'cancel_requested', 'cancelling', 'succeeded', 'not_computable',
                   'not_recovered', 'failed', 'cancelled')
    );
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child
    DROP CONSTRAINT IF EXISTS ck_macb_child_source;
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child
    ADD CONSTRAINT ck_macb_child_source CHECK (
        source_kind IN ('runtime', 'legacy_result_backfill', 'recovery_reference')
    );
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child
    DROP CONSTRAINT IF EXISTS ck_macb_child_execution_disposition;
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child
    ADD CONSTRAINT ck_macb_child_execution_disposition CHECK (
        execution_disposition IN ('execute', 'reuse_result', 'recompute_derived', 'preserve_unavailable')
    );
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child
    DROP CONSTRAINT IF EXISTS ck_macb_child_source_lineage;
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child
    ADD CONSTRAINT ck_macb_child_source_lineage CHECK (
        (source_lineage_json IS NULL AND source_lineage_hash IS NULL)
        OR
        (jsonb_typeof(source_lineage_json) = 'object'
         AND source_lineage_hash ~ '^[0-9a-f]{64}$')
    );
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child
    DROP CONSTRAINT IF EXISTS ck_macb_child_not_recovered_disposition;
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child
    ADD CONSTRAINT ck_macb_child_not_recovered_disposition CHECK (
        (status = 'not_recovered' AND execution_disposition = 'preserve_unavailable')
        OR status <> 'not_recovered'
    );

DO $p0_2_child_constraints$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_macb_child_source_child'
          AND conrelid = 'strategy_pkg.multi_alpha_combine_backtest_child'::regclass
    ) THEN
        ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child
            ADD CONSTRAINT fk_macb_child_source_child
            FOREIGN KEY (source_child_id)
            REFERENCES strategy_pkg.multi_alpha_combine_backtest_child(child_id)
            ON DELETE SET NULL;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_macb_child_run_child'
          AND conrelid = 'strategy_pkg.multi_alpha_combine_backtest_child'::regclass
    ) THEN
        ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child
            ADD CONSTRAINT uq_macb_child_run_child UNIQUE (run_id, child_id);
    END IF;
END
$p0_2_child_constraints$;

CREATE INDEX IF NOT EXISTS idx_macb_child_source_lineage
    ON strategy_pkg.multi_alpha_combine_backtest_child(source_child_id)
    WHERE source_child_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_macb_child_recovery_disposition
    ON strategy_pkg.multi_alpha_combine_backtest_child(run_id, execution_disposition, status);

-- Attempt run_id enables composite scope FKs and is populated from its child.
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child_attempt
    ADD COLUMN IF NOT EXISTS run_id TEXT,
    ADD COLUMN IF NOT EXISTS source_attempt_id TEXT,
    ADD COLUMN IF NOT EXISTS execution_kind TEXT NOT NULL DEFAULT 'remote_execution',
    ADD COLUMN IF NOT EXISTS result_manifest_hash TEXT;

UPDATE strategy_pkg.multi_alpha_combine_backtest_child_attempt AS attempt
SET run_id = child.run_id
FROM strategy_pkg.multi_alpha_combine_backtest_child AS child
WHERE child.child_id = attempt.child_id
  AND attempt.run_id IS NULL;

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child_attempt
    ALTER COLUMN run_id SET NOT NULL;

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child_attempt
    DROP CONSTRAINT IF EXISTS ck_macb_attempt_lineage;
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child_attempt
    ADD CONSTRAINT ck_macb_attempt_lineage CHECK (
        -- Original initial execution.
        (execution_kind = 'remote_execution'
         AND retry_mode = 'initial'
         AND attempt_no = 1
         AND retry_of_attempt_id IS NULL
         AND source_attempt_id IS NULL)
        OR
        -- Same-child remote retry.
        (execution_kind = 'remote_execution'
         AND retry_mode <> 'initial'
         AND attempt_no > 1
         AND retry_of_attempt_id IS NOT NULL
         AND source_attempt_id IS NULL)
        OR
        -- First remote attempt of a successor child.
        (execution_kind = 'remote_execution'
         AND retry_mode <> 'initial'
         AND attempt_no = 1
         AND retry_of_attempt_id IS NULL
         AND source_attempt_id IS NOT NULL)
        OR
        -- Successor reference/derived results are terminal and never dispatch.
        (execution_kind IN ('reference_result', 'derived_result')
         AND retry_mode = 'results_only'
         AND attempt_no = 1
         AND retry_of_attempt_id IS NULL
         AND source_attempt_id IS NOT NULL
         AND status = 'succeeded')
        OR
        -- Narrow in-place results-only collection references the previous
        -- same-child attempt without inventing a remote submit.
        (execution_kind = 'reference_result'
         AND retry_mode = 'results_only'
         AND attempt_no > 1
         AND retry_of_attempt_id IS NOT NULL
         AND source_attempt_id IS NULL
         AND status = 'succeeded')
    );
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child_attempt
    DROP CONSTRAINT IF EXISTS ck_macb_attempt_execution_kind;
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child_attempt
    ADD CONSTRAINT ck_macb_attempt_execution_kind CHECK (
        execution_kind IN ('remote_execution', 'reference_result', 'derived_result')
    );
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child_attempt
    DROP CONSTRAINT IF EXISTS ck_macb_attempt_result_manifest_hash;
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child_attempt
    ADD CONSTRAINT ck_macb_attempt_result_manifest_hash CHECK (
        result_manifest_hash IS NULL OR result_manifest_hash ~ '^[0-9a-f]{64}$'
    );
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child_attempt
    DROP CONSTRAINT IF EXISTS ck_macb_attempt_execution_remote_fields;
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child_attempt
    ADD CONSTRAINT ck_macb_attempt_execution_remote_fields CHECK (
        (execution_kind = 'remote_execution')
        OR
        (qe_task_id IS NULL
         AND qe_loop_id IS NULL
         AND submission_intent_hash IS NULL
         AND node_id IS NULL
         AND result_manifest_hash IS NOT NULL)
    );

DO $p0_2_attempt_constraints$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_macb_attempt_run_child'
          AND conrelid = 'strategy_pkg.multi_alpha_combine_backtest_child_attempt'::regclass
    ) THEN
        ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child_attempt
            ADD CONSTRAINT fk_macb_attempt_run_child
            FOREIGN KEY (run_id, child_id)
            REFERENCES strategy_pkg.multi_alpha_combine_backtest_child(run_id, child_id)
            ON DELETE CASCADE;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_macb_attempt_source_attempt'
          AND conrelid = 'strategy_pkg.multi_alpha_combine_backtest_child_attempt'::regclass
    ) THEN
        ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child_attempt
            ADD CONSTRAINT fk_macb_attempt_source_attempt
            FOREIGN KEY (source_attempt_id)
            REFERENCES strategy_pkg.multi_alpha_combine_backtest_child_attempt(attempt_id)
            ON DELETE SET NULL;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_macb_attempt_run_child_attempt'
          AND conrelid = 'strategy_pkg.multi_alpha_combine_backtest_child_attempt'::regclass
    ) THEN
        ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child_attempt
            ADD CONSTRAINT uq_macb_attempt_run_child_attempt UNIQUE (run_id, child_id, attempt_id);
    END IF;
END
$p0_2_attempt_constraints$;

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child
    DROP CONSTRAINT IF EXISTS fk_macb_child_selected_attempt;
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child
    ADD CONSTRAINT fk_macb_child_selected_attempt
    FOREIGN KEY (run_id, child_id, selected_attempt_id)
    REFERENCES strategy_pkg.multi_alpha_combine_backtest_child_attempt(run_id, child_id, attempt_id)
    ON DELETE SET NULL (selected_attempt_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_macb_attempt_active_remote_execution
    ON strategy_pkg.multi_alpha_combine_backtest_child_attempt(child_id)
    WHERE execution_kind = 'remote_execution'
      AND status IN ('queued', 'submitting', 'running', 'reconciling');
CREATE INDEX IF NOT EXISTS idx_macb_attempt_source_attempt
    ON strategy_pkg.multi_alpha_combine_backtest_child_attempt(source_attempt_id)
    WHERE source_attempt_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS strategy_pkg.multi_alpha_combine_backtest_command (
    command_id TEXT PRIMARY KEY,
    command_seq BIGSERIAL UNIQUE NOT NULL,
    run_id TEXT NOT NULL REFERENCES strategy_pkg.multi_alpha_combine_backtest_run(id) ON DELETE CASCADE,
    child_id TEXT,
    attempt_id TEXT,
    action TEXT NOT NULL,
    target_key TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    request_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    response_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'accepted',
    requested_by TEXT NOT NULL,
    error_code TEXT,
    error_json JSONB,
    scope_hash TEXT,
    owner_id TEXT,
    row_version BIGINT NOT NULL DEFAULT 1,
    fencing_token BIGINT NOT NULL DEFAULT 0,
    lease_expires_at TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    delivery_attempt_count INTEGER NOT NULL DEFAULT 0,
    next_delivery_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_delivery_at TIMESTAMPTZ,
    staging_manifest_json JSONB,
    staging_manifest_hash TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT ck_macb_command_id CHECK (command_id ~ '^macmd_'),
    CONSTRAINT ck_macb_command_action CHECK (
        action IN ('pause', 'resume', 'cancel', 'reconcile', 'attempt_cancel', 'child_retry')
    ),
    CONSTRAINT ck_macb_command_target CHECK (
        (action IN ('pause', 'resume', 'cancel', 'reconcile') AND child_id IS NULL AND attempt_id IS NULL)
        OR (action = 'attempt_cancel' AND child_id IS NOT NULL AND attempt_id IS NOT NULL)
        OR (action = 'child_retry' AND child_id IS NOT NULL AND attempt_id IS NULL)
    ),
    CONSTRAINT ck_macb_command_target_key CHECK (length(target_key) > 0),
    CONSTRAINT ck_macb_command_payload_hash CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_macb_command_scope_hash CHECK (scope_hash IS NULL OR scope_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_macb_command_status CHECK (
        status IN ('accepted', 'applying', 'reconciling', 'succeeded', 'failed', 'superseded')
    ),
    CONSTRAINT ck_macb_command_request_json CHECK (jsonb_typeof(request_json) = 'object'),
    CONSTRAINT ck_macb_command_response_json CHECK (jsonb_typeof(response_json) = 'object'),
    CONSTRAINT ck_macb_command_error_json CHECK (error_json IS NULL OR jsonb_typeof(error_json) = 'object'),
    CONSTRAINT ck_macb_command_staging_manifest CHECK (
        (staging_manifest_json IS NULL AND staging_manifest_hash IS NULL)
        OR (jsonb_typeof(staging_manifest_json) = 'object'
            AND staging_manifest_hash ~ '^[0-9a-f]{64}$')
    ),
    CONSTRAINT ck_macb_command_row_version CHECK (row_version >= 1),
    CONSTRAINT ck_macb_command_fencing_token CHECK (fencing_token >= 0),
    CONSTRAINT ck_macb_command_delivery_attempt_count CHECK (delivery_attempt_count >= 0),
    CONSTRAINT uq_macb_command_idempotency UNIQUE (run_id, idempotency_key),
    CONSTRAINT fk_macb_command_child
        FOREIGN KEY (run_id, child_id)
        REFERENCES strategy_pkg.multi_alpha_combine_backtest_child(run_id, child_id)
        ON DELETE SET NULL (child_id),
    CONSTRAINT fk_macb_command_attempt
        FOREIGN KEY (run_id, child_id, attempt_id)
        REFERENCES strategy_pkg.multi_alpha_combine_backtest_child_attempt(run_id, child_id, attempt_id)
        ON DELETE SET NULL (child_id, attempt_id)
);

CREATE INDEX IF NOT EXISTS idx_macb_command_claim
    ON strategy_pkg.multi_alpha_combine_backtest_command(status, next_delivery_at, lease_expires_at, updated_at)
    WHERE status IN ('accepted', 'applying', 'reconciling');
CREATE UNIQUE INDEX IF NOT EXISTS uq_macb_command_active_target
    ON strategy_pkg.multi_alpha_combine_backtest_command(
        run_id, action, target_key, COALESCE(scope_hash, '')
    )
    WHERE status IN ('accepted', 'applying', 'reconciling');
CREATE INDEX IF NOT EXISTS idx_macb_command_run_seq
    ON strategy_pkg.multi_alpha_combine_backtest_command(run_id, command_seq);

CREATE TABLE IF NOT EXISTS strategy_pkg.multi_alpha_combine_backtest_cancel_delivery (
    delivery_id TEXT PRIMARY KEY,
    originating_command_id TEXT NOT NULL REFERENCES strategy_pkg.multi_alpha_combine_backtest_command(command_id) ON DELETE CASCADE,
    run_id TEXT NOT NULL REFERENCES strategy_pkg.multi_alpha_combine_backtest_run(id) ON DELETE CASCADE,
    child_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    node_id TEXT NOT NULL,
    qe_task_id TEXT NOT NULL,
    qe_loop_id TEXT NOT NULL,
    submission_intent_hash TEXT NOT NULL,
    kill_target_key TEXT NOT NULL,
    expected_process_identity_json JSONB,
    expected_process_identity_hash TEXT,
    kill_intent_generation INTEGER NOT NULL DEFAULT 1,
    kill_intent_hash TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    owner_id TEXT,
    row_version BIGINT NOT NULL DEFAULT 1,
    fencing_token BIGINT NOT NULL DEFAULT 0,
    lease_expires_at TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    delivery_attempt_count INTEGER NOT NULL DEFAULT 0,
    next_delivery_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_delivery_at TIMESTAMPTZ,
    kill_receipt_json JSONB,
    remote_status TEXT,
    error_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    CONSTRAINT ck_macb_cancel_delivery_id CHECK (delivery_id ~ '^macdl_'),
    CONSTRAINT ck_macb_cancel_delivery_submission_hash CHECK (submission_intent_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_macb_cancel_delivery_target_key CHECK (kill_target_key ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_macb_cancel_delivery_process_identity CHECK (
        (expected_process_identity_json IS NULL AND expected_process_identity_hash IS NULL)
        OR (jsonb_typeof(expected_process_identity_json) = 'object'
            AND expected_process_identity_hash ~ '^[0-9a-f]{64}$')
    ),
    CONSTRAINT ck_macb_cancel_delivery_generation CHECK (kill_intent_generation >= 1),
    CONSTRAINT ck_macb_cancel_delivery_intent_hash CHECK (
        kill_intent_hash IS NULL OR kill_intent_hash ~ '^[0-9a-f]{64}$'
    ),
    CONSTRAINT ck_macb_cancel_delivery_status CHECK (
        status IN ('pending', 'sending', 'reconciling', 'succeeded', 'failed')
    ),
    CONSTRAINT ck_macb_cancel_delivery_row_version CHECK (row_version >= 1),
    CONSTRAINT ck_macb_cancel_delivery_fencing_token CHECK (fencing_token >= 0),
    CONSTRAINT ck_macb_cancel_delivery_attempt_count CHECK (delivery_attempt_count >= 0),
    CONSTRAINT ck_macb_cancel_delivery_receipt_json CHECK (
        kill_receipt_json IS NULL OR jsonb_typeof(kill_receipt_json) = 'object'
    ),
    CONSTRAINT ck_macb_cancel_delivery_error_json CHECK (
        error_json IS NULL OR jsonb_typeof(error_json) = 'object'
    ),
    CONSTRAINT uq_macb_cancel_delivery_target UNIQUE (kill_target_key),
    CONSTRAINT fk_macb_cancel_delivery_child
        FOREIGN KEY (run_id, child_id)
        REFERENCES strategy_pkg.multi_alpha_combine_backtest_child(run_id, child_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_macb_cancel_delivery_attempt
        FOREIGN KEY (run_id, child_id, attempt_id)
        REFERENCES strategy_pkg.multi_alpha_combine_backtest_child_attempt(run_id, child_id, attempt_id)
        ON DELETE RESTRICT
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_macb_cancel_delivery_active_attempt
    ON strategy_pkg.multi_alpha_combine_backtest_cancel_delivery(attempt_id)
    WHERE status IN ('pending', 'sending', 'reconciling');
CREATE INDEX IF NOT EXISTS idx_macb_cancel_delivery_claim
    ON strategy_pkg.multi_alpha_combine_backtest_cancel_delivery(status, next_delivery_at, lease_expires_at, updated_at)
    WHERE status IN ('pending', 'sending', 'reconciling');

CREATE TABLE IF NOT EXISTS strategy_pkg.multi_alpha_combine_backtest_command_delivery (
    command_id TEXT NOT NULL REFERENCES strategy_pkg.multi_alpha_combine_backtest_command(command_id) ON DELETE CASCADE,
    delivery_id TEXT NOT NULL REFERENCES strategy_pkg.multi_alpha_combine_backtest_cancel_delivery(delivery_id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (command_id, delivery_id)
);

COMMENT ON TABLE strategy_pkg.multi_alpha_combine_backtest_command IS
    'QE-only durable P0-2 control/recovery intent ledger. Command status is technical execution state, never approval.';
COMMENT ON TABLE strategy_pkg.multi_alpha_combine_backtest_cancel_delivery IS
    'One durable exact-attempt cancellation delivery. HTTP kill response is evidence only, not a terminal research result.';
COMMENT ON TABLE strategy_pkg.multi_alpha_combine_backtest_command_delivery IS
    'Many commands may observe one exact-attempt cancellation delivery without duplicating remote kill.';
COMMENT ON COLUMN strategy_pkg.multi_alpha_combine_backtest_run.recovery_scope_json IS
    'Frozen child-targeted recovery closure and execution identity; not a research admission or approval payload.';
COMMENT ON COLUMN strategy_pkg.multi_alpha_combine_backtest_child.execution_disposition IS
    'execute/reuse_result/recompute_derived/preserve_unavailable recovery behavior; no research-direction gate.';
COMMENT ON COLUMN strategy_pkg.multi_alpha_combine_backtest_child_attempt.execution_kind IS
    'remote_execution or explicit reference/derived result; reference/derived rows never represent remote training/backtest.';

COMMIT;

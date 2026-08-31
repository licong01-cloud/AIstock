-- Canonical QE execution reservation ledger (F2 P0-1B-0B).
--
-- The table owns only cross-source node slots and remote execution identity.
-- It does not duplicate experiment requests, predictions, metrics, Alpha
-- results, approval state, or research-value decisions.
--
-- Operational rules:
--   * Run the sibling read-only preflight first.
--   * Application code must never auto-apply this migration.
--   * Do not export/backup the database as part of this migration workflow;
--     AIstock production already has its normal backup policy.

BEGIN;

DO $dependency_preflight$
BEGIN
    IF to_regclass('infra.compute_nodes') IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'qe_execution_reservation_compute_nodes_missing';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'infra'
          AND table_name = 'compute_nodes'
          AND column_name = 'node_id'
          AND data_type = 'text'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'qe_execution_reservation_compute_node_identity_invalid';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint AS con
        WHERE con.conrelid = 'infra.compute_nodes'::regclass
          AND con.contype IN ('p', 'u')
          AND con.conkey = ARRAY[
              (
                  SELECT attnum
                  FROM pg_attribute
                  WHERE attrelid = 'infra.compute_nodes'::regclass
                    AND attname = 'node_id'
                    AND NOT attisdropped
              )
          ]::smallint[]
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'qe_execution_reservation_compute_node_identity_not_unique';
    END IF;
    IF to_regclass('infra.qe_execution_reservation') IS NOT NULL
       AND EXISTS (
           SELECT 1
           FROM (VALUES
               ('reservation_id', 'text'),
               ('node_id', 'text'),
               ('source_kind', 'text'),
               ('source_execution_id', 'text'),
               ('qe_task_id', 'text'),
               ('qe_loop_id', 'text'),
               ('submission_intent_hash', 'text'),
               ('status', 'text'),
               ('remote_status', 'text'),
               ('release_reason_code', 'text'),
               ('owner_id', 'text'),
               ('lease_expires_at', 'timestamp with time zone'),
               ('fencing_token', 'bigint'),
               ('row_version', 'bigint'),
               ('reserved_at', 'timestamp with time zone'),
               ('heartbeat_at', 'timestamp with time zone'),
               ('released_at', 'timestamp with time zone'),
               ('created_at', 'timestamp with time zone'),
               ('updated_at', 'timestamp with time zone')
           ) AS expected(column_name, data_type)
           LEFT JOIN information_schema.columns AS actual
             ON actual.table_schema = 'infra'
            AND actual.table_name = 'qe_execution_reservation'
            AND actual.column_name = expected.column_name
           WHERE actual.column_name IS NULL OR actual.data_type <> expected.data_type
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'qe_execution_reservation_existing_table_incompatible';
    END IF;
END
$dependency_preflight$;

CREATE TABLE IF NOT EXISTS infra.qe_execution_reservation (
    reservation_id TEXT PRIMARY KEY,
    node_id TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    source_execution_id TEXT NOT NULL,
    qe_task_id TEXT NOT NULL,
    qe_loop_id TEXT NOT NULL,
    submission_intent_hash TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'reserved',
    remote_status TEXT,
    release_reason_code TEXT,
    owner_id TEXT,
    lease_expires_at TIMESTAMPTZ,
    fencing_token BIGINT NOT NULL DEFAULT 1,
    row_version BIGINT NOT NULL DEFAULT 1,
    reserved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    heartbeat_at TIMESTAMPTZ,
    released_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_qeer_compute_node
        FOREIGN KEY (node_id) REFERENCES infra.compute_nodes(node_id) ON DELETE RESTRICT,
    CONSTRAINT uq_qeer_source_execution UNIQUE (source_kind, source_execution_id),
    CONSTRAINT ck_qeer_reservation_id CHECK (reservation_id ~ '^qer_[0-9a-f]{64}$'),
    CONSTRAINT ck_qeer_source_kind CHECK (
        source_kind IN (
            'multi_alpha_durable_attempt',
            'multi_alpha_pred_backtest',
            'qe_evolution_loop',
            'qe_experiment',
            'qe_multi_alpha_node',
            'qe_dispatch_task',
            'legacy_active_import'
        )
    ),
    CONSTRAINT ck_qeer_nonempty_identity CHECK (
        btrim(node_id) <> ''
        AND btrim(source_execution_id) <> ''
        AND btrim(qe_task_id) <> ''
        AND qe_loop_id ~ '^Loop[1-9][0-9]*$'
    ),
    CONSTRAINT ck_qeer_submission_hash CHECK (submission_intent_hash ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_qeer_status CHECK (
        status IN ('reserved', 'submitting', 'running', 'reconciling',
                   'released', 'failed', 'cancelled')
    ),
    CONSTRAINT ck_qeer_versions CHECK (fencing_token >= 1 AND row_version >= 1),
    CONSTRAINT ck_qeer_ownership CHECK (
        lease_expires_at IS NULL OR owner_id IS NOT NULL
    ),
    CONSTRAINT ck_qeer_release_state CHECK (
        (
            status IN ('reserved', 'submitting', 'running', 'reconciling')
            AND released_at IS NULL
            AND release_reason_code IS NULL
        )
        OR (
            status IN ('released', 'failed', 'cancelled')
            AND released_at IS NOT NULL
            AND release_reason_code IS NOT NULL
            AND btrim(release_reason_code) <> ''
        )
    )
);

ALTER TABLE infra.qe_execution_reservation
    DROP CONSTRAINT IF EXISTS uq_qeer_remote_identity;

CREATE UNIQUE INDEX IF NOT EXISTS uq_qeer_remote_identity_active
    ON infra.qe_execution_reservation(node_id, qe_task_id, qe_loop_id)
    WHERE status IN ('reserved', 'submitting', 'running', 'reconciling');

CREATE INDEX IF NOT EXISTS idx_qeer_node_active
    ON infra.qe_execution_reservation(node_id, reserved_at, reservation_id)
    WHERE status IN ('reserved', 'submitting', 'running', 'reconciling');
CREATE INDEX IF NOT EXISTS idx_qeer_recoverable
    ON infra.qe_execution_reservation(status, lease_expires_at, reserved_at, reservation_id)
    WHERE status IN ('reserved', 'submitting', 'running', 'reconciling');

COMMENT ON TABLE infra.qe_execution_reservation IS
    'Canonical QE-only cross-source execution slot and remote identity ledger; contains no Alpha metrics or approval semantics.';
COMMENT ON COLUMN infra.qe_execution_reservation.reservation_id IS
    'Deterministic qer_ SHA-256 identity derived from source_kind and source_execution_id.';
COMMENT ON COLUMN infra.qe_execution_reservation.node_id IS
    'Compute node whose active capacity slot is reserved.';
COMMENT ON COLUMN infra.qe_execution_reservation.source_kind IS
    'Strongly typed producer family; provenance only, never a research admission or approval state.';
COMMENT ON COLUMN infra.qe_execution_reservation.source_execution_id IS
    'Authoritative business execution identity in the source table or service.';
COMMENT ON COLUMN infra.qe_execution_reservation.qe_task_id IS
    'Expected durable QE Workspace task identity persisted before remote POST.';
COMMENT ON COLUMN infra.qe_execution_reservation.qe_loop_id IS
    'Expected durable QE Workspace loop identity persisted before remote POST.';
COMMENT ON COLUMN infra.qe_execution_reservation.submission_intent_hash IS
    'Lowercase SHA-256 intent bound to the QE Workspace server-side submission receipt.';
COMMENT ON COLUMN infra.qe_execution_reservation.status IS
    'Slot lifecycle: active reserved/submitting/running/reconciling or terminal released/failed/cancelled.';
COMMENT ON COLUMN infra.qe_execution_reservation.remote_status IS
    'Most recent explicit QE Workspace status evidence; NULL means not yet observed, not success or failure.';
COMMENT ON COLUMN infra.qe_execution_reservation.release_reason_code IS
    'Structured terminal evidence explaining why the active slot was released.';
COMMENT ON COLUMN infra.qe_execution_reservation.owner_id IS
    'Current backend worker owner for CAS updates; ownership expiry never releases the capacity slot.';
COMMENT ON COLUMN infra.qe_execution_reservation.lease_expires_at IS
    'Database-clock ownership lease; expiry permits takeover but does not change active capacity.';
COMMENT ON COLUMN infra.qe_execution_reservation.fencing_token IS
    'Monotonic ownership generation preventing stale workers from writing after takeover.';
COMMENT ON COLUMN infra.qe_execution_reservation.row_version IS
    'Optimistic CAS version incremented by each authoritative repository write.';
COMMENT ON COLUMN infra.qe_execution_reservation.reserved_at IS
    'Database time at which the node slot was first reserved.';
COMMENT ON COLUMN infra.qe_execution_reservation.heartbeat_at IS
    'Most recent successful owner heartbeat using the database clock.';
COMMENT ON COLUMN infra.qe_execution_reservation.released_at IS
    'Database time at which an explicit terminal evidence released the slot.';
COMMENT ON COLUMN infra.qe_execution_reservation.created_at IS
    'Immutable row creation time from the database clock.';
COMMENT ON COLUMN infra.qe_execution_reservation.updated_at IS
    'Most recent authoritative repository update time from the database clock.';

DO $postcondition$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM (VALUES
            ('qe_execution_reservation_pkey'),
            ('fk_qeer_compute_node'),
            ('uq_qeer_source_execution'),
            ('ck_qeer_reservation_id'),
            ('ck_qeer_source_kind'),
            ('ck_qeer_nonempty_identity'),
            ('ck_qeer_submission_hash'),
            ('ck_qeer_status'),
            ('ck_qeer_versions'),
            ('ck_qeer_ownership'),
            ('ck_qeer_release_state')
        ) AS expected(conname)
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_constraint AS actual
            WHERE actual.conrelid = 'infra.qe_execution_reservation'::regclass
              AND actual.conname = expected.conname
        )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'qe_execution_reservation_postcondition_constraint_missing';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM (VALUES
            ('uq_qeer_remote_identity_active'),
            ('idx_qeer_node_active'),
            ('idx_qeer_recoverable')
        ) AS expected(indexname)
        WHERE NOT EXISTS (
            SELECT 1
            FROM pg_indexes AS actual
            WHERE actual.schemaname = 'infra'
              AND actual.tablename = 'qe_execution_reservation'
              AND actual.indexname = expected.indexname
        )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'qe_execution_reservation_postcondition_index_missing';
    END IF;
    IF obj_description('infra.qe_execution_reservation'::regclass, 'pg_class') IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'qe_execution_reservation_postcondition_comment_missing';
    END IF;
END
$postcondition$;

COMMIT;

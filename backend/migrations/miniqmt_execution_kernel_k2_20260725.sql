-- MiniQMT Execution Kernel K2-A additive migration.
-- Stage 1: transactional additive columns and tables.
BEGIN;

ALTER TABLE qmt_strategy.execution_runtime_event
    ADD COLUMN IF NOT EXISTS event_contract_version TEXT NOT NULL DEFAULT 'LEGACY_V1',
    ADD COLUMN IF NOT EXISTS event_schema_version TEXT,
    ADD COLUMN IF NOT EXISTS payload_schema_version TEXT,
    ADD COLUMN IF NOT EXISTS event_key_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS payload_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS observed_at_utc TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS logical_at_utc TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS source_identity_json JSONB,
    ADD COLUMN IF NOT EXISTS correlation_json JSONB,
    ADD COLUMN IF NOT EXISTS ingress_receipt_json JSONB,
    ADD COLUMN IF NOT EXISTS ingress_receipt_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS routing_rule_version TEXT,
    ADD COLUMN IF NOT EXISTS transaction_commit_identity TEXT;

ALTER TABLE qmt_strategy.execution_algo_instance
    ADD COLUMN IF NOT EXISTS kernel_contract_version TEXT NOT NULL DEFAULT 'LEGACY_V1',
    ADD COLUMN IF NOT EXISTS traded_quantity INTEGER,
    ADD COLUMN IF NOT EXISTS plugin_id TEXT,
    ADD COLUMN IF NOT EXISTS plugin_version TEXT,
    ADD COLUMN IF NOT EXISTS plugin_manifest_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS plugin_config_json JSONB,
    ADD COLUMN IF NOT EXISTS plugin_config_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS compatibility_receipt_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS state_schema_version TEXT,
    ADD COLUMN IF NOT EXISTS state_json JSONB,
    ADD COLUMN IF NOT EXISTS state_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS transition_sequence BIGINT,
    ADD COLUMN IF NOT EXISTS last_applied_delivery_sequence BIGINT,
    ADD COLUMN IF NOT EXISTS last_applied_delivery_id TEXT,
    ADD COLUMN IF NOT EXISTS last_closed_delivery_sequence BIGINT,
    ADD COLUMN IF NOT EXISTS terminal_delivery_sequence BIGINT,
    ADD COLUMN IF NOT EXISTS failure_receipt_id TEXT,
    ADD COLUMN IF NOT EXISTS active_child_closure_status TEXT,
    ADD COLUMN IF NOT EXISTS active_child_count BIGINT,
    ADD COLUMN IF NOT EXISTS row_version BIGINT,
    ADD COLUMN IF NOT EXISTS terminal_at_utc TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS kernel_carrier_json JSONB;

ALTER TABLE qmt_strategy.execution_child_order
    ADD COLUMN IF NOT EXISTS kernel_contract_version TEXT NOT NULL DEFAULT 'LEGACY_V1',
    ADD COLUMN IF NOT EXISTS mapping_id TEXT,
    ADD COLUMN IF NOT EXISTS command_id TEXT,
    ADD COLUMN IF NOT EXISTS local_vt_orderid TEXT,
    ADD COLUMN IF NOT EXISTS deterministic_client_order_ref TEXT,
    ADD COLUMN IF NOT EXISTS order_remark TEXT,
    ADD COLUMN IF NOT EXISTS mapping_status TEXT,
    ADD COLUMN IF NOT EXISTS mapping_version BIGINT,
    ADD COLUMN IF NOT EXISTS mapping_payload_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS mapping_receipt_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS broker_identity_source_event_id TEXT,
    ADD COLUMN IF NOT EXISTS last_order_event_id TEXT,
    ADD COLUMN IF NOT EXISTS last_trade_event_id TEXT,
    ADD COLUMN IF NOT EXISTS created_transition_id TEXT,
    ADD COLUMN IF NOT EXISTS updated_by_event_id TEXT,
    ADD COLUMN IF NOT EXISTS mapping_created_at_utc TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS mapping_updated_at_utc TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS mapping_json JSONB;

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_kernel_worker_epoch (
    worker_id TEXT NOT NULL,
    process_role TEXT NOT NULL,
    incarnation_sequence BIGINT NOT NULL DEFAULT 0,
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_miniqmt_k2_worker_epoch PRIMARY KEY (worker_id, process_role),
    CONSTRAINT ck_miniqmt_k2_worker_epoch_identity CHECK (btrim(worker_id) <> '' AND btrim(process_role) <> ''),
    CONSTRAINT ck_miniqmt_k2_worker_epoch_sequence CHECK (incarnation_sequence >= 0)
);

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_kernel_worker_incarnation (
    worker_id TEXT NOT NULL,
    process_role TEXT NOT NULL,
    incarnation_sequence BIGINT NOT NULL,
    source_revision TEXT NOT NULL,
    process_incarnation_id TEXT NOT NULL,
    started_at_utc TIMESTAMPTZ NOT NULL,
    startup_transaction_commit_identity TEXT NOT NULL,
    receipt_sha256 TEXT NOT NULL,
    startup_receipt_json JSONB NOT NULL,
    CONSTRAINT pk_miniqmt_k2_worker_incarnation PRIMARY KEY (process_incarnation_id),
    CONSTRAINT uq_miniqmt_k2_worker_incarnation_sequence UNIQUE (worker_id, process_role, incarnation_sequence),
    CONSTRAINT uq_miniqmt_k2_worker_incarnation_owner UNIQUE (worker_id, process_incarnation_id),
    CONSTRAINT fk_miniqmt_k2_worker_incarnation_epoch FOREIGN KEY (worker_id, process_role)
        REFERENCES qmt_strategy.execution_kernel_worker_epoch(worker_id, process_role),
    CONSTRAINT ck_miniqmt_k2_worker_incarnation_sequence CHECK (incarnation_sequence > 0),
    CONSTRAINT ck_miniqmt_k2_worker_incarnation_hash CHECK (receipt_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_algo_event_delivery (
    delivery_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    runtime_id TEXT NOT NULL,
    algo_instance_id TEXT NOT NULL,
    plugin_manifest_sha256 TEXT NOT NULL,
    algo_delivery_sequence BIGINT NOT NULL,
    previous_delivery_sequence BIGINT,
    previous_delivery_id TEXT,
    status TEXT NOT NULL,
    attempt_count BIGINT NOT NULL,
    lease_owner TEXT,
    lease_worker_id TEXT,
    lease_process_incarnation_id TEXT,
    lease_epoch BIGINT NOT NULL DEFAULT 0,
    lease_fence_token TEXT,
    lease_expires_at TIMESTAMPTZ,
    transition_id TEXT,
    last_error_json JSONB,
    next_attempt_at_utc TIMESTAMPTZ,
    failure_receipt_id TEXT,
    skip_receipt_id TEXT,
    row_version BIGINT NOT NULL,
    created_at_utc TIMESTAMPTZ NOT NULL,
    updated_at_utc TIMESTAMPTZ NOT NULL,
    closed_at_utc TIMESTAMPTZ,
    carrier_json JSONB NOT NULL,
    CONSTRAINT uq_miniqmt_k2_delivery_sequence UNIQUE (algo_instance_id, algo_delivery_sequence),
    CONSTRAINT uq_miniqmt_k2_delivery_owner UNIQUE (runtime_id, algo_instance_id, algo_delivery_sequence, delivery_id),
    CONSTRAINT uq_miniqmt_k2_delivery_reference UNIQUE (runtime_id, algo_instance_id, event_id, delivery_id),
    CONSTRAINT uq_miniqmt_k2_delivery_predecessor_target UNIQUE (algo_instance_id, algo_delivery_sequence, delivery_id),
    CONSTRAINT fk_miniqmt_k2_delivery_predecessor FOREIGN KEY (algo_instance_id, previous_delivery_sequence, previous_delivery_id)
        REFERENCES qmt_strategy.execution_algo_event_delivery(algo_instance_id, algo_delivery_sequence, delivery_id),
    CONSTRAINT fk_miniqmt_k2_delivery_incarnation FOREIGN KEY (lease_worker_id, lease_process_incarnation_id)
        REFERENCES qmt_strategy.execution_kernel_worker_incarnation(worker_id, process_incarnation_id),
    CONSTRAINT ck_miniqmt_k2_delivery_sequence CHECK (algo_delivery_sequence > 0 AND attempt_count >= 0 AND row_version > 0),
    CONSTRAINT ck_miniqmt_k2_delivery_status CHECK (
        status IN ('PENDING','CLAIMED','APPLIED','FAILED_RETRYABLE','FAILED_TERMINAL','SKIPPED_TERMINAL')
    ),
    CONSTRAINT ck_miniqmt_k2_delivery_predecessor CHECK (
        (algo_delivery_sequence = 1 AND previous_delivery_sequence IS NULL AND previous_delivery_id IS NULL)
        OR (algo_delivery_sequence > 1 AND previous_delivery_sequence = algo_delivery_sequence - 1 AND previous_delivery_id IS NOT NULL)
    ),
    CONSTRAINT ck_miniqmt_k2_delivery_lease CHECK (
        (lease_owner IS NULL AND lease_worker_id IS NULL AND lease_process_incarnation_id IS NULL AND lease_fence_token IS NULL AND lease_expires_at IS NULL)
        OR (lease_owner = lease_worker_id || ':' || lease_process_incarnation_id AND lease_epoch > 0 AND lease_fence_token IS NOT NULL AND lease_expires_at IS NOT NULL)
    ),
    CONSTRAINT ck_miniqmt_k2_delivery_receipt_presence CHECK (
        (status = 'PENDING' AND transition_id IS NULL AND failure_receipt_id IS NULL AND skip_receipt_id IS NULL AND closed_at_utc IS NULL)
        OR (status = 'CLAIMED' AND lease_owner IS NOT NULL AND closed_at_utc IS NULL)
        OR (status = 'APPLIED' AND transition_id IS NOT NULL AND failure_receipt_id IS NULL AND skip_receipt_id IS NULL AND closed_at_utc IS NOT NULL)
        OR (status = 'FAILED_RETRYABLE' AND last_error_json IS NOT NULL AND next_attempt_at_utc IS NOT NULL AND closed_at_utc IS NULL)
        OR (status = 'FAILED_TERMINAL' AND failure_receipt_id IS NOT NULL AND last_error_json IS NOT NULL AND closed_at_utc IS NOT NULL)
        OR (status = 'SKIPPED_TERMINAL' AND skip_receipt_id IS NOT NULL AND transition_id IS NULL AND closed_at_utc IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_algo_transition (
    transition_id TEXT PRIMARY KEY,
    delivery_id TEXT NOT NULL UNIQUE,
    event_id TEXT NOT NULL,
    runtime_id TEXT NOT NULL,
    algo_instance_id TEXT NOT NULL,
    transition_sequence BIGINT NOT NULL,
    transition_kind TEXT NOT NULL,
    transition_receipt_json JSONB,
    failure_receipt_json JSONB,
    skip_receipt_json JSONB,
    receipt_sha256 TEXT NOT NULL,
    execution_projection_set_json JSONB,
    execution_projection_set_sha256 TEXT,
    after_state_json JSONB,
    after_state_sha256 TEXT,
    transaction_commit_identity TEXT NOT NULL,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_miniqmt_k2_transition_owner UNIQUE (runtime_id, algo_instance_id, transition_sequence, transition_id),
    CONSTRAINT uq_miniqmt_k2_transition_reference UNIQUE (runtime_id, algo_instance_id, transition_id),
    CONSTRAINT uq_miniqmt_k2_projection_owner UNIQUE (runtime_id, algo_instance_id, event_id, delivery_id, execution_projection_set_sha256),
    CONSTRAINT ck_miniqmt_k2_transition_kind CHECK (transition_kind IN ('APPLIED','FAILED_TERMINAL','SKIPPED_TERMINAL')),
    CONSTRAINT ck_miniqmt_k2_transition_hash CHECK (receipt_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_miniqmt_k2_transition_receipt_presence CHECK (
        (transition_kind = 'APPLIED' AND transition_receipt_json IS NOT NULL AND failure_receipt_json IS NULL AND skip_receipt_json IS NULL AND after_state_json IS NOT NULL AND after_state_sha256 IS NOT NULL AND execution_projection_set_json IS NOT NULL AND execution_projection_set_sha256 IS NOT NULL)
        OR (transition_kind = 'FAILED_TERMINAL' AND transition_receipt_json IS NULL AND failure_receipt_json IS NOT NULL AND skip_receipt_json IS NULL)
        OR (transition_kind = 'SKIPPED_TERMINAL' AND transition_receipt_json IS NULL AND failure_receipt_json IS NULL AND skip_receipt_json IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_algo_command_outbox (
    command_id TEXT PRIMARY KEY,
    transition_id TEXT NOT NULL,
    ordinal BIGINT NOT NULL,
    runtime_id TEXT NOT NULL,
    algo_instance_id TEXT NOT NULL,
    parent_intent_id TEXT NOT NULL,
    mapping_id TEXT NOT NULL,
    command_type TEXT NOT NULL,
    local_vt_orderid TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    payload_sha256 TEXT NOT NULL,
    status TEXT NOT NULL,
    attempt_count BIGINT NOT NULL,
    lease_owner TEXT,
    lease_worker_id TEXT,
    lease_process_incarnation_id TEXT,
    lease_epoch BIGINT NOT NULL DEFAULT 0,
    lease_fence_token TEXT,
    lease_expires_at TIMESTAMPTZ,
    dispatch_attempt_id TEXT,
    deterministic_client_order_ref TEXT NOT NULL,
    next_attempt_at_utc TIMESTAMPTZ,
    broker_called BOOLEAN,
    broker_order_id TEXT,
    ack_receipt_json JSONB,
    ack_receipt_sha256 TEXT,
    non_acceptance_receipt_json JSONB,
    unknown_outcome_receipt_json JSONB,
    reconcile_receipt_json JSONB,
    last_error_json JSONB,
    row_version BIGINT NOT NULL,
    created_at_utc TIMESTAMPTZ NOT NULL,
    updated_at_utc TIMESTAMPTZ NOT NULL,
    closed_at_utc TIMESTAMPTZ,
    carrier_json JSONB NOT NULL,
    outbox_row_sha256 TEXT NOT NULL,
    CONSTRAINT uq_miniqmt_k2_outbox_transition_ordinal UNIQUE (transition_id, ordinal),
    CONSTRAINT uq_miniqmt_k2_outbox_mapping UNIQUE (mapping_id, command_id),
    CONSTRAINT fk_miniqmt_k2_outbox_incarnation FOREIGN KEY (lease_worker_id, lease_process_incarnation_id)
        REFERENCES qmt_strategy.execution_kernel_worker_incarnation(worker_id, process_incarnation_id),
    CONSTRAINT ck_miniqmt_k2_outbox_status CHECK (status IN ('PENDING','CLAIMED','DISPATCHING','ACKED','ACKED_REJECTED','FAILED_RETRYABLE','OUTCOME_UNKNOWN','RECONCILING','FAILED_TERMINAL')),
    CONSTRAINT ck_miniqmt_k2_outbox_counter CHECK (ordinal >= 0 AND attempt_count >= 0 AND row_version > 0),
    CONSTRAINT ck_miniqmt_k2_outbox_hash CHECK (payload_sha256 ~ '^[0-9a-f]{64}$' AND outbox_row_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_miniqmt_k2_outbox_lease CHECK (
        (lease_owner IS NULL AND lease_worker_id IS NULL AND lease_process_incarnation_id IS NULL AND lease_fence_token IS NULL AND lease_expires_at IS NULL)
        OR (lease_owner = lease_worker_id || ':' || lease_process_incarnation_id AND lease_epoch > 0 AND lease_fence_token IS NOT NULL AND lease_expires_at IS NOT NULL)
    ),
    CONSTRAINT ck_miniqmt_k2_outbox_broker_called CHECK (
        (status IN ('PENDING','CLAIMED') AND broker_called IS NULL AND ack_receipt_json IS NULL AND ack_receipt_sha256 IS NULL AND non_acceptance_receipt_json IS NULL AND unknown_outcome_receipt_json IS NULL AND reconcile_receipt_json IS NULL AND closed_at_utc IS NULL)
        OR (status = 'DISPATCHING' AND broker_called IS NULL AND dispatch_attempt_id IS NOT NULL AND ack_receipt_json IS NULL AND non_acceptance_receipt_json IS NULL AND unknown_outcome_receipt_json IS NULL AND reconcile_receipt_json IS NULL AND closed_at_utc IS NULL)
        OR (status IN ('ACKED','ACKED_REJECTED') AND broker_called IS TRUE AND ack_receipt_json IS NOT NULL AND ack_receipt_sha256 ~ '^[0-9a-f]{64}$' AND closed_at_utc IS NOT NULL)
        OR (status = 'FAILED_RETRYABLE' AND broker_called IS FALSE AND last_error_json IS NOT NULL AND next_attempt_at_utc IS NOT NULL AND closed_at_utc IS NULL)
        OR (status IN ('OUTCOME_UNKNOWN','RECONCILING') AND broker_called IS NULL AND unknown_outcome_receipt_json IS NOT NULL AND closed_at_utc IS NULL)
        OR (status = 'FAILED_TERMINAL' AND broker_called IS NOT TRUE AND (last_error_json IS NOT NULL OR non_acceptance_receipt_json IS NOT NULL OR unknown_outcome_receipt_json IS NOT NULL OR reconcile_receipt_json IS NOT NULL) AND closed_at_utc IS NOT NULL)
    )
);

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_algo_command_dispatch_attempt (
    dispatch_attempt_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    command_id TEXT NOT NULL,
    attempt_count BIGINT NOT NULL,
    lease_epoch BIGINT NOT NULL,
    lease_fence_token TEXT NOT NULL,
    process_incarnation_id TEXT NOT NULL,
    started_at_utc TIMESTAMPTZ NOT NULL,
    finished_at_utc TIMESTAMPTZ,
    pre_call_complete BOOLEAN NOT NULL,
    broker_called BOOLEAN,
    outcome TEXT,
    error_reason_code TEXT,
    error_context_sha256 TEXT,
    authority_receipt_sha256 TEXT,
    attempt_receipt_sha256 TEXT NOT NULL,
    carrier_json JSONB NOT NULL,
    CONSTRAINT pk_miniqmt_k2_dispatch_attempt PRIMARY KEY (dispatch_attempt_id, stage),
    CONSTRAINT fk_miniqmt_k2_dispatch_attempt_command FOREIGN KEY (command_id)
        REFERENCES qmt_strategy.execution_algo_command_outbox(command_id),
    CONSTRAINT fk_miniqmt_k2_dispatch_attempt_incarnation FOREIGN KEY (process_incarnation_id)
        REFERENCES qmt_strategy.execution_kernel_worker_incarnation(process_incarnation_id),
    CONSTRAINT ck_miniqmt_k2_dispatch_attempt_stage CHECK (stage IN ('CLAIMED','PRE_CALL','DISPATCHING_COMMITTED','GATEWAY_RETURNED','CALLBACK_OBSERVED','COMPLETION_COMMITTED','RECONCILING','CLOSED')),
    CONSTRAINT ck_miniqmt_k2_dispatch_attempt_counter CHECK (attempt_count > 0 AND lease_epoch > 0)
);

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_algo_timer_schedule (
    schedule_id TEXT PRIMARY KEY,
    runtime_id TEXT NOT NULL,
    algo_instance_id TEXT NOT NULL,
    timer_name TEXT NOT NULL,
    schedule_epoch TEXT NOT NULL,
    due_at_exchange_utc TIMESTAMPTZ NOT NULL,
    catch_up_policy TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    payload_sha256 TEXT NOT NULL,
    status TEXT NOT NULL,
    timer_occurrence_id TEXT NOT NULL,
    emitted_event_id TEXT,
    catch_up_receipt_sha256 TEXT,
    lease_owner TEXT,
    lease_worker_id TEXT,
    lease_process_incarnation_id TEXT,
    lease_epoch BIGINT NOT NULL DEFAULT 0,
    lease_fence_token TEXT,
    lease_expires_at_utc TIMESTAMPTZ,
    row_version BIGINT NOT NULL,
    created_at_utc TIMESTAMPTZ NOT NULL,
    updated_at_utc TIMESTAMPTZ NOT NULL,
    closed_at_utc TIMESTAMPTZ,
    schedule_receipt_sha256 TEXT NOT NULL,
    carrier_json JSONB NOT NULL,
    CONSTRAINT uq_miniqmt_k2_timer_schedule_identity UNIQUE (algo_instance_id, timer_name, schedule_epoch),
    CONSTRAINT uq_miniqmt_k2_timer_schedule_occurrence UNIQUE (timer_occurrence_id, schedule_id, runtime_id, algo_instance_id),
    CONSTRAINT fk_miniqmt_k2_timer_schedule_incarnation FOREIGN KEY (lease_worker_id, lease_process_incarnation_id)
        REFERENCES qmt_strategy.execution_kernel_worker_incarnation(worker_id, process_incarnation_id),
    CONSTRAINT ck_miniqmt_k2_timer_schedule_status CHECK (status IN ('SCHEDULED','EMITTING','EMITTED','CANCELLED','EXPIRED')),
    CONSTRAINT ck_miniqmt_k2_timer_schedule_version CHECK (row_version > 0),
    CONSTRAINT ck_miniqmt_k2_timer_schedule_lease CHECK (
        (lease_owner IS NULL AND lease_worker_id IS NULL AND lease_process_incarnation_id IS NULL AND lease_fence_token IS NULL AND lease_expires_at_utc IS NULL)
        OR (lease_owner = lease_worker_id || ':' || lease_process_incarnation_id AND lease_epoch > 0 AND lease_fence_token IS NOT NULL AND lease_expires_at_utc IS NOT NULL)
    ),
    CONSTRAINT ck_miniqmt_k2_timer_schedule_receipt CHECK (schedule_receipt_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_algo_timer_occurrence (
    timer_occurrence_id TEXT PRIMARY KEY,
    schedule_id TEXT NOT NULL,
    runtime_id TEXT NOT NULL,
    algo_instance_id TEXT NOT NULL,
    due_at_exchange_utc TIMESTAMPTZ NOT NULL,
    exchange_session_authority_sha256 TEXT NOT NULL,
    status TEXT NOT NULL,
    emitted_event_id TEXT,
    lease_owner TEXT,
    lease_worker_id TEXT,
    lease_process_incarnation_id TEXT,
    lease_epoch BIGINT NOT NULL DEFAULT 0,
    lease_fence_token TEXT,
    lease_expires_at_utc TIMESTAMPTZ,
    row_version BIGINT NOT NULL,
    created_at_utc TIMESTAMPTZ NOT NULL,
    closed_at_utc TIMESTAMPTZ,
    occurrence_receipt_sha256 TEXT NOT NULL,
    carrier_json JSONB NOT NULL,
    CONSTRAINT fk_miniqmt_k2_timer_occurrence_schedule FOREIGN KEY (timer_occurrence_id, schedule_id, runtime_id, algo_instance_id)
        REFERENCES qmt_strategy.execution_algo_timer_schedule(timer_occurrence_id, schedule_id, runtime_id, algo_instance_id),
    CONSTRAINT fk_miniqmt_k2_timer_occurrence_incarnation FOREIGN KEY (lease_worker_id, lease_process_incarnation_id)
        REFERENCES qmt_strategy.execution_kernel_worker_incarnation(worker_id, process_incarnation_id),
    CONSTRAINT ck_miniqmt_k2_timer_occurrence_status CHECK (status IN ('CLAIMED','EVENT_COMMITTED','SKIPPED','EXPIRED')),
    CONSTRAINT ck_miniqmt_k2_timer_occurrence_version CHECK (row_version > 0),
    CONSTRAINT ck_miniqmt_k2_timer_occurrence_lease CHECK (
        (lease_owner IS NULL AND lease_worker_id IS NULL AND lease_process_incarnation_id IS NULL AND lease_fence_token IS NULL AND lease_expires_at_utc IS NULL)
        OR (lease_owner = lease_worker_id || ':' || lease_process_incarnation_id AND lease_epoch > 0 AND lease_fence_token IS NOT NULL AND lease_expires_at_utc IS NOT NULL)
    ),
    CONSTRAINT ck_miniqmt_k2_timer_occurrence_receipt CHECK (exchange_session_authority_sha256 ~ '^[0-9a-f]{64}$' AND occurrence_receipt_sha256 ~ '^[0-9a-f]{64}$')
);

ALTER TABLE qmt_strategy.execution_algo_command_dispatch_attempt
    ADD COLUMN IF NOT EXISTS pre_call_complete BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS outcome TEXT,
    ADD COLUMN IF NOT EXISTS error_reason_code TEXT,
    ADD COLUMN IF NOT EXISTS error_context_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS authority_receipt_sha256 TEXT;

ALTER TABLE qmt_strategy.execution_algo_command_dispatch_attempt
    ALTER COLUMN pre_call_complete DROP DEFAULT;

ALTER TABLE qmt_strategy.execution_algo_timer_schedule
    ADD COLUMN IF NOT EXISTS catch_up_policy TEXT NOT NULL DEFAULT 'EXPIRE_IF_LATE',
    ADD COLUMN IF NOT EXISTS payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    ADD COLUMN IF NOT EXISTS payload_sha256 TEXT NOT NULL DEFAULT repeat('0', 64);

ALTER TABLE qmt_strategy.execution_algo_timer_schedule
    ALTER COLUMN catch_up_policy DROP DEFAULT,
    ALTER COLUMN payload_json DROP DEFAULT,
    ALTER COLUMN payload_sha256 DROP DEFAULT;

ALTER TABLE qmt_strategy.execution_algo_timer_occurrence
    ADD COLUMN IF NOT EXISTS catch_up_receipt_sha256 TEXT;

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_exchange_session_authority (
    runtime_id TEXT NOT NULL,
    exchange_trade_date DATE NOT NULL,
    calendar_snapshot_set_id TEXT NOT NULL,
    calendar_snapshot_set_sha256 TEXT NOT NULL,
    session_definition_version TEXT NOT NULL,
    authority_sha256 TEXT NOT NULL,
    authority_json JSONB NOT NULL,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT pk_miniqmt_k2_exchange_session_authority PRIMARY KEY (runtime_id, exchange_trade_date),
    CONSTRAINT uq_miniqmt_k2_exchange_session_authority_hash UNIQUE (runtime_id, exchange_trade_date, authority_sha256),
    CONSTRAINT ck_miniqmt_k2_exchange_session_authority_hash CHECK (calendar_snapshot_set_sha256 ~ '^[0-9a-f]{64}$' AND authority_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_algo_diagnostic_observation (
    observation_id TEXT PRIMARY KEY,
    runtime_id TEXT NOT NULL,
    algo_instance_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    transition_id TEXT NOT NULL,
    observation_json JSONB NOT NULL,
    context_sha256 TEXT NOT NULL,
    observed_at_utc TIMESTAMPTZ NOT NULL,
    CONSTRAINT ck_miniqmt_k2_diagnostic_hash CHECK (context_sha256 ~ '^[0-9a-f]{64}$')
);

COMMIT;

-- Stage 2: PostgreSQL requires CONCURRENTLY outside a transaction block.
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_miniqmt_k2_event_key
    ON qmt_strategy.execution_runtime_event(runtime_id, event_key_sha256)
    WHERE event_contract_version = 'KERNEL_V2';

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_miniqmt_k2_event_owner
    ON qmt_strategy.execution_runtime_event(runtime_id, event_id);

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_miniqmt_k2_algo_runtime_identity
    ON qmt_strategy.execution_algo_instance(runtime_id, algo_instance_id);

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_miniqmt_k2_algo_owner
    ON qmt_strategy.execution_algo_instance(runtime_id, algo_instance_id, parent_intent_id);

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_miniqmt_k2_child_mapping
    ON qmt_strategy.execution_child_order(runtime_id, algo_instance_id, parent_intent_id, command_id, local_vt_orderid, child_order_id)
    WHERE kernel_contract_version = 'KERNEL_V2';

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_miniqmt_k2_child_client_ref
    ON qmt_strategy.execution_child_order(deterministic_client_order_ref)
    WHERE kernel_contract_version = 'KERNEL_V2';

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_miniqmt_k2_child_broker_order
    ON qmt_strategy.execution_child_order(broker_order_id)
    WHERE kernel_contract_version = 'KERNEL_V2' AND broker_order_id IS NOT NULL;

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_miniqmt_k2_child_mapping_pair
    ON qmt_strategy.execution_child_order(mapping_id, command_id);

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_miniqmt_k2_child_mapping_id
    ON qmt_strategy.execution_child_order(mapping_id);

-- Stage 3: named checks/FKs, validation, comments, and independent readback.
BEGIN;

ALTER TABLE qmt_strategy.execution_algo_instance DROP CONSTRAINT IF EXISTS ck_miniqmt_algo_status;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_miniqmt_k2_event_contract' AND conrelid = 'qmt_strategy.execution_runtime_event'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_runtime_event ADD CONSTRAINT ck_miniqmt_k2_event_contract CHECK (
            (event_contract_version = 'LEGACY_V1' AND event_schema_version IS NULL AND payload_schema_version IS NULL AND event_key_sha256 IS NULL AND payload_sha256 IS NULL AND observed_at_utc IS NULL AND logical_at_utc IS NULL AND source_identity_json IS NULL AND correlation_json IS NULL AND ingress_receipt_json IS NULL AND ingress_receipt_sha256 IS NULL AND routing_rule_version IS NULL AND transaction_commit_identity IS NULL)
            OR (event_contract_version = 'KERNEL_V2' AND event_schema_version = 'miniqmt_runtime_event_envelope_v2' AND payload_schema_version IS NOT NULL AND event_key_sha256 ~ '^[0-9a-f]{64}$' AND payload_sha256 ~ '^[0-9a-f]{64}$' AND observed_at_utc IS NOT NULL AND logical_at_utc IS NOT NULL AND source_identity_json IS NOT NULL AND correlation_json IS NOT NULL AND ingress_receipt_json IS NOT NULL AND ingress_receipt_sha256 ~ '^[0-9a-f]{64}$' AND routing_rule_version = 'miniqmt_event_routing_v1' AND transaction_commit_identity IS NOT NULL)
        ) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_miniqmt_k2_event_composite' AND conrelid = 'qmt_strategy.execution_runtime_event'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_runtime_event ADD CONSTRAINT ck_miniqmt_k2_event_composite CHECK (
            event_contract_version = 'LEGACY_V1' OR
            (event_type='ALGO_START' AND source='MINIQMT_EXECUTION_KERNEL' AND payload_schema_version='miniqmt_algo_start_v1') OR
            (event_type='TICK' AND source='B0_QUOTE_V2' AND payload_schema_version='miniqmt_market_data_view_v2') OR
            (event_type='TIMER' AND source='EXCHANGE_SESSION_CLOCK' AND payload_schema_version='miniqmt_timer_due_v1') OR
            (event_type='SESSION' AND source='EXCHANGE_SESSION_CLOCK' AND payload_schema_version='miniqmt_session_event_v1') OR
            (event_type='EOD' AND source='EXCHANGE_SESSION_CLOCK' AND payload_schema_version='miniqmt_eod_event_v1') OR
            (event_type='ORDER' AND source='QMT_GATEWAY_CALLBACK' AND payload_schema_version='miniqmt_order_event_v1') OR
            (event_type='TRADE' AND source='QMT_GATEWAY_CALLBACK' AND payload_schema_version='miniqmt_trade_fact_v1') OR
            (event_type='ACCOUNT' AND source='QMT_OMS_PROJECTION' AND payload_schema_version='miniqmt_account_projection_v1') OR
            (event_type='RECONCILE' AND source='QMT_OMS_RECONCILIATION' AND payload_schema_version='miniqmt_reconciliation_receipt_v1') OR
            (event_type='OPERATOR' AND source='SIMULATION_RUNTIME_OPERATOR' AND payload_schema_version='miniqmt_operator_command_v1')
        ) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_miniqmt_k2_algo_status' AND conrelid = 'qmt_strategy.execution_algo_instance'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_algo_instance ADD CONSTRAINT ck_miniqmt_k2_algo_status CHECK (
            (kernel_contract_version = 'LEGACY_V1' AND status IN ('ACTIVE','PAUSED','COMPLETED','CANCELLED','FAILED') AND traded_quantity IS NULL AND plugin_id IS NULL AND plugin_version IS NULL AND plugin_manifest_sha256 IS NULL AND plugin_config_json IS NULL AND plugin_config_sha256 IS NULL AND compatibility_receipt_sha256 IS NULL AND state_schema_version IS NULL AND state_json IS NULL AND state_sha256 IS NULL AND transition_sequence IS NULL AND last_applied_delivery_sequence IS NULL AND last_applied_delivery_id IS NULL AND last_closed_delivery_sequence IS NULL AND terminal_delivery_sequence IS NULL AND failure_receipt_id IS NULL AND active_child_closure_status IS NULL AND active_child_count IS NULL AND row_version IS NULL AND terminal_at_utc IS NULL AND kernel_carrier_json IS NULL)
            OR (
                kernel_contract_version = 'KERNEL_V2'
                AND status IN ('INITIALIZING','ACTIVE','PAUSED','COMPLETED','CANCELLED','FAILED','EXPIRED_WITH_RESIDUAL')
                AND traded_quantity >= 0
                AND remaining_quantity >= 0
                AND traded_quantity + remaining_quantity = target_quantity
                AND plugin_id IS NOT NULL
                AND plugin_version IS NOT NULL
                AND plugin_manifest_sha256 ~ '^[0-9a-f]{64}$'
                AND plugin_config_json IS NOT NULL
                AND plugin_config_sha256 ~ '^[0-9a-f]{64}$'
                AND compatibility_receipt_sha256 ~ '^[0-9a-f]{64}$'
                AND (
                    (state_schema_version IS NOT NULL AND state_json IS NOT NULL AND state_sha256 ~ '^[0-9a-f]{64}$')
                    OR (
                        status = 'FAILED'
                        AND transition_sequence = 0
                        AND state_schema_version IS NULL
                        AND state_json IS NULL
                        AND state_sha256 IS NULL
                    )
                )
                AND transition_sequence >= 0
                AND last_applied_delivery_sequence >= 0
                AND (
                    (status = 'FAILED' AND transition_sequence IN (last_applied_delivery_sequence, last_applied_delivery_sequence + 1))
                    OR (status <> 'FAILED' AND transition_sequence = last_applied_delivery_sequence)
                )
                AND ((last_applied_delivery_sequence = 0) = (last_applied_delivery_id IS NULL))
                AND last_closed_delivery_sequence >= last_applied_delivery_sequence
                AND (
                    (status IN ('COMPLETED','CANCELLED','FAILED','EXPIRED_WITH_RESIDUAL') AND terminal_delivery_sequence IS NOT NULL AND terminal_at_utc IS NOT NULL)
                    OR (status IN ('INITIALIZING','ACTIVE','PAUSED') AND terminal_delivery_sequence IS NULL AND terminal_at_utc IS NULL)
                )
                AND (
                    (status = 'FAILED' AND failure_receipt_id IS NOT NULL)
                    OR (status <> 'FAILED' AND failure_receipt_id IS NULL)
                )
                AND active_child_count >= 0
                AND row_version > 0
                AND kernel_carrier_json IS NOT NULL
            )
        ) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_miniqmt_k2_algo_active_child_closure' AND conrelid = 'qmt_strategy.execution_algo_instance'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_algo_instance ADD CONSTRAINT ck_miniqmt_k2_algo_active_child_closure CHECK (
            kernel_contract_version = 'LEGACY_V1'
            OR (status = 'FAILED' AND active_child_closure_status = 'CLEAN' AND active_child_count = 0)
            OR (status = 'FAILED' AND active_child_closure_status IN ('CANCEL_PENDING','OUTCOME_UNKNOWN') AND active_child_count > 0)
            OR (status IN ('COMPLETED','CANCELLED','EXPIRED_WITH_RESIDUAL') AND active_child_closure_status = 'CLEAN' AND active_child_count = 0)
            OR (status IN ('INITIALIZING','ACTIVE','PAUSED') AND active_child_closure_status = 'NOT_APPLICABLE')
        ) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_miniqmt_k2_child_mapping_contract' AND conrelid = 'qmt_strategy.execution_child_order'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_child_order ADD CONSTRAINT ck_miniqmt_k2_child_mapping_contract CHECK (
            (kernel_contract_version = 'LEGACY_V1' AND mapping_id IS NULL AND command_id IS NULL AND local_vt_orderid IS NULL AND deterministic_client_order_ref IS NULL AND order_remark IS NULL AND mapping_status IS NULL AND mapping_version IS NULL AND mapping_payload_sha256 IS NULL AND mapping_receipt_sha256 IS NULL AND broker_identity_source_event_id IS NULL AND last_order_event_id IS NULL AND last_trade_event_id IS NULL AND created_transition_id IS NULL AND updated_by_event_id IS NULL AND mapping_created_at_utc IS NULL AND mapping_updated_at_utc IS NULL AND mapping_json IS NULL)
            OR (kernel_contract_version = 'KERNEL_V2' AND mapping_id IS NOT NULL AND command_id IS NOT NULL AND local_vt_orderid IS NOT NULL AND deterministic_client_order_ref IS NOT NULL AND order_remark = deterministic_client_order_ref AND mapping_status IN ('RESERVED','DISPATCHING','BROKER_ACCEPTED','BROKER_REJECTED','OUTCOME_UNKNOWN','TERMINAL') AND mapping_version > 0 AND mapping_payload_sha256 ~ '^[0-9a-f]{64}$' AND mapping_receipt_sha256 ~ '^[0-9a-f]{64}$' AND created_transition_id IS NOT NULL AND mapping_created_at_utc IS NOT NULL AND mapping_updated_at_utc >= mapping_created_at_utc AND mapping_json IS NOT NULL)
        ) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_miniqmt_k2_event_owner' AND conrelid = 'qmt_strategy.execution_runtime_event'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_runtime_event
            ADD CONSTRAINT uq_miniqmt_k2_event_owner UNIQUE USING INDEX uq_miniqmt_k2_event_owner;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_miniqmt_k2_algo_runtime_identity' AND conrelid = 'qmt_strategy.execution_algo_instance'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_algo_instance
            ADD CONSTRAINT uq_miniqmt_k2_algo_runtime_identity UNIQUE USING INDEX uq_miniqmt_k2_algo_runtime_identity;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_miniqmt_k2_algo_owner' AND conrelid = 'qmt_strategy.execution_algo_instance'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_algo_instance
            ADD CONSTRAINT uq_miniqmt_k2_algo_owner UNIQUE USING INDEX uq_miniqmt_k2_algo_owner;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_miniqmt_k2_child_mapping_pair' AND conrelid = 'qmt_strategy.execution_child_order'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_child_order
            ADD CONSTRAINT uq_miniqmt_k2_child_mapping_pair UNIQUE USING INDEX uq_miniqmt_k2_child_mapping_pair;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_miniqmt_k2_child_mapping_id' AND conrelid = 'qmt_strategy.execution_child_order'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_child_order
            ADD CONSTRAINT uq_miniqmt_k2_child_mapping_id UNIQUE USING INDEX uq_miniqmt_k2_child_mapping_id;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'uq_miniqmt_k2_runtime_trade_date' AND conrelid = 'qmt_strategy.execution_runtime'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_runtime
            ADD CONSTRAINT uq_miniqmt_k2_runtime_trade_date UNIQUE (runtime_id, trade_date);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_miniqmt_k2_outbox_mapping' AND conrelid = 'qmt_strategy.execution_algo_command_outbox'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_algo_command_outbox
            ADD CONSTRAINT fk_miniqmt_k2_outbox_mapping FOREIGN KEY (mapping_id)
            REFERENCES qmt_strategy.execution_child_order(mapping_id)
            DEFERRABLE INITIALLY DEFERRED NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_miniqmt_k2_delivery_event_owner' AND conrelid = 'qmt_strategy.execution_algo_event_delivery'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_algo_event_delivery ADD CONSTRAINT fk_miniqmt_k2_delivery_event_owner
            FOREIGN KEY (runtime_id,event_id) REFERENCES qmt_strategy.execution_runtime_event(runtime_id,event_id) DEFERRABLE INITIALLY DEFERRED NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_miniqmt_k2_delivery_algo_owner' AND conrelid = 'qmt_strategy.execution_algo_event_delivery'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_algo_event_delivery ADD CONSTRAINT fk_miniqmt_k2_delivery_algo_owner
            FOREIGN KEY (runtime_id,algo_instance_id) REFERENCES qmt_strategy.execution_algo_instance(runtime_id,algo_instance_id) DEFERRABLE INITIALLY DEFERRED NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_miniqmt_k2_transition_delivery_owner' AND conrelid = 'qmt_strategy.execution_algo_transition'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_algo_transition ADD CONSTRAINT fk_miniqmt_k2_transition_delivery_owner
            FOREIGN KEY (runtime_id,algo_instance_id,event_id,delivery_id) REFERENCES qmt_strategy.execution_algo_event_delivery(runtime_id,algo_instance_id,event_id,delivery_id) DEFERRABLE INITIALLY DEFERRED NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_miniqmt_k2_child_algo_owner' AND conrelid = 'qmt_strategy.execution_child_order'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_child_order ADD CONSTRAINT fk_miniqmt_k2_child_algo_owner
            FOREIGN KEY (runtime_id,algo_instance_id,parent_intent_id) REFERENCES qmt_strategy.execution_algo_instance(runtime_id,algo_instance_id,parent_intent_id) DEFERRABLE INITIALLY DEFERRED NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_miniqmt_k2_child_transition_owner' AND conrelid = 'qmt_strategy.execution_child_order'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_child_order ADD CONSTRAINT fk_miniqmt_k2_child_transition_owner
            FOREIGN KEY (runtime_id,algo_instance_id,created_transition_id) REFERENCES qmt_strategy.execution_algo_transition(runtime_id,algo_instance_id,transition_id) DEFERRABLE INITIALLY DEFERRED NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_miniqmt_k2_outbox_algo_owner' AND conrelid = 'qmt_strategy.execution_algo_command_outbox'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_algo_command_outbox ADD CONSTRAINT fk_miniqmt_k2_outbox_algo_owner
            FOREIGN KEY (runtime_id,algo_instance_id,parent_intent_id) REFERENCES qmt_strategy.execution_algo_instance(runtime_id,algo_instance_id,parent_intent_id) DEFERRABLE INITIALLY DEFERRED NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_miniqmt_k2_outbox_transition_owner' AND conrelid = 'qmt_strategy.execution_algo_command_outbox'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_algo_command_outbox ADD CONSTRAINT fk_miniqmt_k2_outbox_transition_owner
            FOREIGN KEY (runtime_id,algo_instance_id,transition_id) REFERENCES qmt_strategy.execution_algo_transition(runtime_id,algo_instance_id,transition_id) DEFERRABLE INITIALLY DEFERRED NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_miniqmt_k2_timer_schedule_algo_owner' AND conrelid = 'qmt_strategy.execution_algo_timer_schedule'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_algo_timer_schedule ADD CONSTRAINT fk_miniqmt_k2_timer_schedule_algo_owner
            FOREIGN KEY (runtime_id,algo_instance_id) REFERENCES qmt_strategy.execution_algo_instance(runtime_id,algo_instance_id) DEFERRABLE INITIALLY DEFERRED NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_miniqmt_k2_exchange_session_runtime' AND conrelid = 'qmt_strategy.execution_exchange_session_authority'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_exchange_session_authority ADD CONSTRAINT fk_miniqmt_k2_exchange_session_runtime
            FOREIGN KEY (runtime_id,exchange_trade_date)
            REFERENCES qmt_strategy.execution_runtime(runtime_id,trade_date) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_miniqmt_k2_diagnostic_event_owner' AND conrelid = 'qmt_strategy.execution_algo_diagnostic_observation'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_algo_diagnostic_observation ADD CONSTRAINT fk_miniqmt_k2_diagnostic_event_owner
            FOREIGN KEY (runtime_id,event_id) REFERENCES qmt_strategy.execution_runtime_event(runtime_id,event_id) DEFERRABLE INITIALLY DEFERRED NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_miniqmt_k2_diagnostic_transition_owner' AND conrelid = 'qmt_strategy.execution_algo_diagnostic_observation'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_algo_diagnostic_observation ADD CONSTRAINT fk_miniqmt_k2_diagnostic_transition_owner
            FOREIGN KEY (runtime_id,algo_instance_id,transition_id) REFERENCES qmt_strategy.execution_algo_transition(runtime_id,algo_instance_id,transition_id) DEFERRABLE INITIALLY DEFERRED NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_miniqmt_k2_delivery_initial' AND conrelid = 'qmt_strategy.execution_algo_event_delivery'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_algo_event_delivery ADD CONSTRAINT ck_miniqmt_k2_delivery_initial CHECK (
            status <> 'PENDING' OR (
                attempt_count=0 AND lease_epoch=0 AND row_version=1
                AND lease_owner IS NULL AND lease_worker_id IS NULL AND lease_process_incarnation_id IS NULL
                AND lease_fence_token IS NULL AND lease_expires_at IS NULL AND transition_id IS NULL
                AND last_error_json IS NULL AND next_attempt_at_utc IS NULL AND failure_receipt_id IS NULL
                AND skip_receipt_id IS NULL AND closed_at_utc IS NULL AND created_at_utc=updated_at_utc
            )
        ) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_miniqmt_k2_child_mapping_initial' AND conrelid = 'qmt_strategy.execution_child_order'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_child_order ADD CONSTRAINT ck_miniqmt_k2_child_mapping_initial CHECK (
            kernel_contract_version <> 'KERNEL_V2' OR mapping_status <> 'RESERVED' OR (
                mapping_version=1 AND broker_order_id IS NULL AND broker_identity_source_event_id IS NULL
                AND last_order_event_id IS NULL AND last_trade_event_id IS NULL AND updated_by_event_id IS NULL
                AND mapping_created_at_utc=mapping_updated_at_utc
            )
        ) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_miniqmt_k2_outbox_initial' AND conrelid = 'qmt_strategy.execution_algo_command_outbox'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_algo_command_outbox ADD CONSTRAINT ck_miniqmt_k2_outbox_initial CHECK (
            status <> 'PENDING' OR (
                attempt_count=0 AND lease_epoch=0 AND row_version=1
                AND lease_owner IS NULL AND lease_worker_id IS NULL AND lease_process_incarnation_id IS NULL
                AND lease_fence_token IS NULL AND lease_expires_at IS NULL AND dispatch_attempt_id IS NULL
                AND next_attempt_at_utc IS NULL AND broker_called IS NULL AND broker_order_id IS NULL
                AND ack_receipt_json IS NULL AND ack_receipt_sha256 IS NULL
                AND non_acceptance_receipt_json IS NULL AND unknown_outcome_receipt_json IS NULL
                AND reconcile_receipt_json IS NULL AND last_error_json IS NULL AND closed_at_utc IS NULL
                AND created_at_utc=updated_at_utc
            )
        ) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_miniqmt_k2_timer_schedule_initial' AND conrelid = 'qmt_strategy.execution_algo_timer_schedule'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_algo_timer_schedule ADD CONSTRAINT ck_miniqmt_k2_timer_schedule_initial CHECK (
            status <> 'SCHEDULED' OR (
                lease_epoch=0 AND row_version=1 AND emitted_event_id IS NULL AND lease_owner IS NULL
                AND lease_worker_id IS NULL AND lease_process_incarnation_id IS NULL
                AND lease_fence_token IS NULL AND lease_expires_at_utc IS NULL AND closed_at_utc IS NULL
                AND created_at_utc=updated_at_utc
            )
        ) NOT VALID;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'ck_miniqmt_k2_timer_occurrence_initial' AND conrelid = 'qmt_strategy.execution_algo_timer_occurrence'::regclass) THEN
        ALTER TABLE qmt_strategy.execution_algo_timer_occurrence ADD CONSTRAINT ck_miniqmt_k2_timer_occurrence_initial CHECK (
            status <> 'CLAIMED' OR (
                lease_epoch=1 AND row_version=1 AND emitted_event_id IS NULL
                AND catch_up_receipt_sha256 IS NULL AND lease_owner IS NOT NULL
                AND lease_worker_id IS NOT NULL AND lease_process_incarnation_id IS NOT NULL
                AND lease_fence_token IS NOT NULL AND lease_expires_at_utc IS NOT NULL AND closed_at_utc IS NULL
            )
        ) NOT VALID;
    END IF;
END $$;

CREATE OR REPLACE FUNCTION qmt_strategy.miniqmt_k2_catalog_fingerprint()
RETURNS TEXT
LANGUAGE SQL
STABLE
AS $fingerprint$
WITH target_tables(relname) AS (
    VALUES
        ('execution_kernel_worker_epoch'),
        ('execution_kernel_worker_incarnation'),
        ('execution_algo_event_delivery'),
        ('execution_algo_transition'),
        ('execution_algo_command_outbox'),
        ('execution_algo_command_dispatch_attempt'),
        ('execution_algo_timer_schedule'),
        ('execution_algo_timer_occurrence'),
        ('execution_exchange_session_authority'),
        ('execution_algo_diagnostic_observation')
), additive_columns(relname,attname) AS (
    VALUES
        ('execution_runtime','runtime_id'),
        ('execution_runtime','trade_date'),
        ('execution_runtime_event','event_contract_version'),
        ('execution_runtime_event','event_schema_version'),
        ('execution_runtime_event','payload_schema_version'),
        ('execution_runtime_event','event_key_sha256'),
        ('execution_runtime_event','payload_sha256'),
        ('execution_runtime_event','observed_at_utc'),
        ('execution_runtime_event','logical_at_utc'),
        ('execution_runtime_event','source_identity_json'),
        ('execution_runtime_event','correlation_json'),
        ('execution_runtime_event','ingress_receipt_json'),
        ('execution_runtime_event','ingress_receipt_sha256'),
        ('execution_runtime_event','routing_rule_version'),
        ('execution_runtime_event','transaction_commit_identity'),
        ('execution_algo_instance','kernel_contract_version'),
        ('execution_algo_instance','traded_quantity'),
        ('execution_algo_instance','plugin_id'),
        ('execution_algo_instance','plugin_version'),
        ('execution_algo_instance','plugin_manifest_sha256'),
        ('execution_algo_instance','plugin_config_json'),
        ('execution_algo_instance','plugin_config_sha256'),
        ('execution_algo_instance','compatibility_receipt_sha256'),
        ('execution_algo_instance','state_schema_version'),
        ('execution_algo_instance','state_json'),
        ('execution_algo_instance','state_sha256'),
        ('execution_algo_instance','transition_sequence'),
        ('execution_algo_instance','last_applied_delivery_sequence'),
        ('execution_algo_instance','last_applied_delivery_id'),
        ('execution_algo_instance','last_closed_delivery_sequence'),
        ('execution_algo_instance','terminal_delivery_sequence'),
        ('execution_algo_instance','failure_receipt_id'),
        ('execution_algo_instance','active_child_closure_status'),
        ('execution_algo_instance','active_child_count'),
        ('execution_algo_instance','row_version'),
        ('execution_algo_instance','terminal_at_utc'),
        ('execution_algo_instance','kernel_carrier_json'),
        ('execution_child_order','kernel_contract_version'),
        ('execution_child_order','mapping_id'),
        ('execution_child_order','command_id'),
        ('execution_child_order','local_vt_orderid'),
        ('execution_child_order','deterministic_client_order_ref'),
        ('execution_child_order','order_remark'),
        ('execution_child_order','mapping_status'),
        ('execution_child_order','mapping_version'),
        ('execution_child_order','mapping_payload_sha256'),
        ('execution_child_order','mapping_receipt_sha256'),
        ('execution_child_order','broker_identity_source_event_id'),
        ('execution_child_order','last_order_event_id'),
        ('execution_child_order','last_trade_event_id'),
        ('execution_child_order','created_transition_id'),
        ('execution_child_order','updated_by_event_id'),
        ('execution_child_order','mapping_created_at_utc'),
        ('execution_child_order','mapping_updated_at_utc'),
        ('execution_child_order','mapping_json')
), catalog_items(sort_key,item) AS (
    SELECT
        format('column:%s:%05s', table_class.relname, attribute.attnum),
        jsonb_build_array(
            'column', table_class.relname, attribute.attname,
            format_type(attribute.atttypid, attribute.atttypmod),
            attribute.attnotnull,
            coalesce(pg_get_expr(attribute_default.adbin, attribute_default.adrelid), '')
        )
    FROM pg_class AS table_class
    JOIN pg_namespace AS table_schema ON table_schema.oid=table_class.relnamespace
    JOIN pg_attribute AS attribute
      ON attribute.attrelid=table_class.oid AND attribute.attnum > 0 AND NOT attribute.attisdropped
    LEFT JOIN pg_attrdef AS attribute_default
      ON attribute_default.adrelid=table_class.oid AND attribute_default.adnum=attribute.attnum
    WHERE table_schema.nspname='qmt_strategy'
      AND (
          table_class.relname IN (SELECT relname FROM target_tables)
          OR (table_class.relname,attribute.attname) IN (SELECT relname,attname FROM additive_columns)
      )

    UNION ALL

    SELECT
        format('constraint:%s:%s', table_class.relname, constraint_record.conname),
        jsonb_build_array(
            'constraint', table_class.relname, constraint_record.conname,
            constraint_record.contype, constraint_record.condeferrable,
            constraint_record.condeferred, constraint_record.convalidated,
            replace(
                pg_get_constraintdef(constraint_record.oid, true),
                table_schema.nspname || '.', '<schema>.'
            )
        )
    FROM pg_constraint AS constraint_record
    JOIN pg_class AS table_class ON table_class.oid=constraint_record.conrelid
    JOIN pg_namespace AS table_schema ON table_schema.oid=table_class.relnamespace
    WHERE table_schema.nspname='qmt_strategy'
      AND (
          table_class.relname IN (SELECT relname FROM target_tables)
          OR constraint_record.conname LIKE '%miniqmt_k2%'
      )

    UNION ALL

    SELECT
        format('index:%s:%s', table_class.relname, index_class.relname),
        jsonb_build_array(
            'index', table_class.relname, index_class.relname,
            index_record.indisunique, index_record.indisprimary,
            index_record.indisvalid, index_record.indisready,
            replace(
                pg_get_indexdef(index_record.indexrelid, 0, true),
                table_schema.nspname || '.', '<schema>.'
            ),
            coalesce(
                replace(
                    pg_get_expr(index_record.indpred, index_record.indrelid, true),
                    table_schema.nspname || '.', '<schema>.'
                ),
                ''
            )
        )
    FROM pg_index AS index_record
    JOIN pg_class AS table_class ON table_class.oid=index_record.indrelid
    JOIN pg_class AS index_class ON index_class.oid=index_record.indexrelid
    JOIN pg_namespace AS table_schema ON table_schema.oid=table_class.relnamespace
    WHERE table_schema.nspname='qmt_strategy'
      AND (
          table_class.relname IN (SELECT relname FROM target_tables)
          OR index_class.relname LIKE '%miniqmt_k2%'
      )
), canonical_catalog AS (
    SELECT coalesce(jsonb_agg(item ORDER BY sort_key), '[]'::jsonb)::TEXT AS payload
    FROM catalog_items
)
SELECT encode(sha256(convert_to(payload, 'UTF8')), 'hex')
FROM canonical_catalog;
$fingerprint$;

ALTER TABLE qmt_strategy.execution_runtime_event VALIDATE CONSTRAINT ck_miniqmt_k2_event_contract;
ALTER TABLE qmt_strategy.execution_runtime_event VALIDATE CONSTRAINT ck_miniqmt_k2_event_composite;
ALTER TABLE qmt_strategy.execution_algo_instance VALIDATE CONSTRAINT ck_miniqmt_k2_algo_status;
ALTER TABLE qmt_strategy.execution_algo_instance VALIDATE CONSTRAINT ck_miniqmt_k2_algo_active_child_closure;
ALTER TABLE qmt_strategy.execution_child_order VALIDATE CONSTRAINT ck_miniqmt_k2_child_mapping_contract;
ALTER TABLE qmt_strategy.execution_algo_command_outbox VALIDATE CONSTRAINT fk_miniqmt_k2_outbox_mapping;
ALTER TABLE qmt_strategy.execution_algo_event_delivery VALIDATE CONSTRAINT fk_miniqmt_k2_delivery_event_owner;
ALTER TABLE qmt_strategy.execution_algo_event_delivery VALIDATE CONSTRAINT fk_miniqmt_k2_delivery_algo_owner;
ALTER TABLE qmt_strategy.execution_algo_transition VALIDATE CONSTRAINT fk_miniqmt_k2_transition_delivery_owner;
ALTER TABLE qmt_strategy.execution_child_order VALIDATE CONSTRAINT fk_miniqmt_k2_child_algo_owner;
ALTER TABLE qmt_strategy.execution_child_order VALIDATE CONSTRAINT fk_miniqmt_k2_child_transition_owner;
ALTER TABLE qmt_strategy.execution_algo_command_outbox VALIDATE CONSTRAINT fk_miniqmt_k2_outbox_algo_owner;
ALTER TABLE qmt_strategy.execution_algo_command_outbox VALIDATE CONSTRAINT fk_miniqmt_k2_outbox_transition_owner;
ALTER TABLE qmt_strategy.execution_algo_timer_schedule VALIDATE CONSTRAINT fk_miniqmt_k2_timer_schedule_algo_owner;
ALTER TABLE qmt_strategy.execution_exchange_session_authority VALIDATE CONSTRAINT fk_miniqmt_k2_exchange_session_runtime;
ALTER TABLE qmt_strategy.execution_algo_diagnostic_observation VALIDATE CONSTRAINT fk_miniqmt_k2_diagnostic_event_owner;
ALTER TABLE qmt_strategy.execution_algo_diagnostic_observation VALIDATE CONSTRAINT fk_miniqmt_k2_diagnostic_transition_owner;
ALTER TABLE qmt_strategy.execution_algo_event_delivery VALIDATE CONSTRAINT ck_miniqmt_k2_delivery_initial;
ALTER TABLE qmt_strategy.execution_child_order VALIDATE CONSTRAINT ck_miniqmt_k2_child_mapping_initial;
ALTER TABLE qmt_strategy.execution_algo_command_outbox VALIDATE CONSTRAINT ck_miniqmt_k2_outbox_initial;
ALTER TABLE qmt_strategy.execution_algo_timer_schedule VALIDATE CONSTRAINT ck_miniqmt_k2_timer_schedule_initial;
ALTER TABLE qmt_strategy.execution_algo_timer_occurrence VALIDATE CONSTRAINT ck_miniqmt_k2_timer_occurrence_initial;

DO $$
DECLARE
    actual_catalog_sha256 TEXT;
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_proc AS function_record
        JOIN pg_namespace AS function_schema ON function_schema.oid=function_record.pronamespace
        JOIN pg_language AS function_language ON function_language.oid=function_record.prolang
        WHERE function_record.oid=to_regprocedure('qmt_strategy.miniqmt_k2_catalog_fingerprint()')
          AND function_language.lanname='sql'
          AND function_record.provolatile='s'
          AND pg_get_function_arguments(function_record.oid)=''
          AND pg_get_function_result(function_record.oid)='text'
          AND encode(
                sha256(
                    convert_to(
                        btrim(
                            replace(function_record.prosrc,function_schema.nspname,'<schema>'),
                            E' \n\r\t;'
                        ),
                        'UTF8'
                    )
                ),
                'hex'
              )='8d9c8b09b5c27a0b0caeeaea3663556b9876b0eea179057d691bbf2fce29c107'
    ) THEN
        RAISE EXCEPTION 'K2 catalog function drift';
    END IF;
    WITH target_tables(relname) AS (
        VALUES
            ('execution_kernel_worker_epoch'),
            ('execution_kernel_worker_incarnation'),
            ('execution_algo_event_delivery'),
            ('execution_algo_transition'),
            ('execution_algo_command_outbox'),
            ('execution_algo_command_dispatch_attempt'),
            ('execution_algo_timer_schedule'),
            ('execution_algo_timer_occurrence'),
            ('execution_exchange_session_authority'),
            ('execution_algo_diagnostic_observation')
    ), additive_columns(relname,attname) AS (
        VALUES
            ('execution_runtime','runtime_id'),
            ('execution_runtime','trade_date'),
            ('execution_runtime_event','event_contract_version'),
            ('execution_runtime_event','event_schema_version'),
            ('execution_runtime_event','payload_schema_version'),
            ('execution_runtime_event','event_key_sha256'),
            ('execution_runtime_event','payload_sha256'),
            ('execution_runtime_event','observed_at_utc'),
            ('execution_runtime_event','logical_at_utc'),
            ('execution_runtime_event','source_identity_json'),
            ('execution_runtime_event','correlation_json'),
            ('execution_runtime_event','ingress_receipt_json'),
            ('execution_runtime_event','ingress_receipt_sha256'),
            ('execution_runtime_event','routing_rule_version'),
            ('execution_runtime_event','transaction_commit_identity'),
            ('execution_algo_instance','kernel_contract_version'),
            ('execution_algo_instance','traded_quantity'),
            ('execution_algo_instance','plugin_id'),
            ('execution_algo_instance','plugin_version'),
            ('execution_algo_instance','plugin_manifest_sha256'),
            ('execution_algo_instance','plugin_config_json'),
            ('execution_algo_instance','plugin_config_sha256'),
            ('execution_algo_instance','compatibility_receipt_sha256'),
            ('execution_algo_instance','state_schema_version'),
            ('execution_algo_instance','state_json'),
            ('execution_algo_instance','state_sha256'),
            ('execution_algo_instance','transition_sequence'),
            ('execution_algo_instance','last_applied_delivery_sequence'),
            ('execution_algo_instance','last_applied_delivery_id'),
            ('execution_algo_instance','last_closed_delivery_sequence'),
            ('execution_algo_instance','terminal_delivery_sequence'),
            ('execution_algo_instance','failure_receipt_id'),
            ('execution_algo_instance','active_child_closure_status'),
            ('execution_algo_instance','active_child_count'),
            ('execution_algo_instance','row_version'),
            ('execution_algo_instance','terminal_at_utc'),
            ('execution_algo_instance','kernel_carrier_json'),
            ('execution_child_order','kernel_contract_version'),
            ('execution_child_order','mapping_id'),
            ('execution_child_order','command_id'),
            ('execution_child_order','local_vt_orderid'),
            ('execution_child_order','deterministic_client_order_ref'),
            ('execution_child_order','order_remark'),
            ('execution_child_order','mapping_status'),
            ('execution_child_order','mapping_version'),
            ('execution_child_order','mapping_payload_sha256'),
            ('execution_child_order','mapping_receipt_sha256'),
            ('execution_child_order','broker_identity_source_event_id'),
            ('execution_child_order','last_order_event_id'),
            ('execution_child_order','last_trade_event_id'),
            ('execution_child_order','created_transition_id'),
            ('execution_child_order','updated_by_event_id'),
            ('execution_child_order','mapping_created_at_utc'),
            ('execution_child_order','mapping_updated_at_utc'),
            ('execution_child_order','mapping_json')
    ), catalog_items(sort_key,item) AS (
        SELECT
            format('column:%s:%05s', table_class.relname, attribute.attnum),
            jsonb_build_array(
                'column', table_class.relname, attribute.attname,
                format_type(attribute.atttypid, attribute.atttypmod),
                attribute.attnotnull,
                coalesce(pg_get_expr(attribute_default.adbin, attribute_default.adrelid), '')
            )
        FROM pg_class AS table_class
        JOIN pg_namespace AS table_schema ON table_schema.oid=table_class.relnamespace
        JOIN pg_attribute AS attribute
          ON attribute.attrelid=table_class.oid AND attribute.attnum > 0 AND NOT attribute.attisdropped
        LEFT JOIN pg_attrdef AS attribute_default
          ON attribute_default.adrelid=table_class.oid AND attribute_default.adnum=attribute.attnum
        WHERE table_schema.nspname='qmt_strategy'
          AND (
              table_class.relname IN (SELECT relname FROM target_tables)
              OR (table_class.relname,attribute.attname) IN (SELECT relname,attname FROM additive_columns)
          )

        UNION ALL

        SELECT
            format('constraint:%s:%s', table_class.relname, constraint_record.conname),
            jsonb_build_array(
                'constraint', table_class.relname, constraint_record.conname,
                constraint_record.contype, constraint_record.condeferrable,
                constraint_record.condeferred, constraint_record.convalidated,
                replace(
                    pg_get_constraintdef(constraint_record.oid, true),
                    table_schema.nspname || '.', '<schema>.'
                )
            )
        FROM pg_constraint AS constraint_record
        JOIN pg_class AS table_class ON table_class.oid=constraint_record.conrelid
        JOIN pg_namespace AS table_schema ON table_schema.oid=table_class.relnamespace
        WHERE table_schema.nspname='qmt_strategy'
          AND (
              table_class.relname IN (SELECT relname FROM target_tables)
              OR constraint_record.conname LIKE '%miniqmt_k2%'
          )

        UNION ALL

        SELECT
            format('index:%s:%s', table_class.relname, index_class.relname),
            jsonb_build_array(
                'index', table_class.relname, index_class.relname,
                index_record.indisunique, index_record.indisprimary,
                index_record.indisvalid, index_record.indisready,
                replace(
                    pg_get_indexdef(index_record.indexrelid, 0, true),
                    table_schema.nspname || '.', '<schema>.'
                ),
                coalesce(
                    replace(
                        pg_get_expr(index_record.indpred, index_record.indrelid, true),
                        table_schema.nspname || '.', '<schema>.'
                    ),
                    ''
                )
            )
        FROM pg_index AS index_record
        JOIN pg_class AS table_class ON table_class.oid=index_record.indrelid
        JOIN pg_class AS index_class ON index_class.oid=index_record.indexrelid
        JOIN pg_namespace AS table_schema ON table_schema.oid=table_class.relnamespace
        WHERE table_schema.nspname='qmt_strategy'
          AND (
              table_class.relname IN (SELECT relname FROM target_tables)
              OR index_class.relname LIKE '%miniqmt_k2%'
          )
    ), canonical_catalog AS (
        SELECT coalesce(jsonb_agg(item ORDER BY sort_key), '[]'::jsonb)::TEXT AS payload
        FROM catalog_items
    )
    SELECT encode(sha256(convert_to(payload, 'UTF8')), 'hex')
    INTO actual_catalog_sha256
    FROM canonical_catalog;
    IF actual_catalog_sha256 <> '6e4fc4ae4c6e403d3316c124da6ae5933eb33184129569fd6bf1cf750e27f762' THEN
        RAISE EXCEPTION 'K2 schema catalog drift: expected %, got %',
            '6e4fc4ae4c6e403d3316c124da6ae5933eb33184129569fd6bf1cf750e27f762',
            actual_catalog_sha256;
    END IF;
END $$;

COMMENT ON TABLE qmt_strategy.execution_algo_event_delivery IS 'K2 durable per-algo delivery latest view with predecessor and worker-fence closure.';
COMMENT ON COLUMN qmt_strategy.execution_algo_event_delivery.carrier_json IS 'Strict AlgoDeliveryPersistenceV1 canonical readback payload.';
COMMENT ON TABLE qmt_strategy.execution_algo_transition IS 'Immutable K2 applied/failure/skip transition receipt journal.';
COMMENT ON TABLE qmt_strategy.execution_algo_command_outbox IS 'K2 broker command latest view committed before any Gateway call.';
COMMENT ON COLUMN qmt_strategy.execution_algo_command_outbox.broker_called IS 'Three-valued broker call authority; NULL is unknown, never default false.';
COMMENT ON TABLE qmt_strategy.execution_algo_command_dispatch_attempt IS 'Append-only dispatch stage history, unique by attempt and stage.';
COMMENT ON TABLE qmt_strategy.execution_kernel_worker_incarnation IS 'Append-only DB-sequenced worker startup receipts used by lease fencing.';
COMMENT ON TABLE qmt_strategy.execution_algo_timer_schedule IS 'K2 durable one-shot timer schedule latest view.';
COMMENT ON TABLE qmt_strategy.execution_algo_timer_occurrence IS 'Append-only K2 timer emission occurrence history.';
COMMENT ON TABLE qmt_strategy.execution_exchange_session_authority IS 'Frozen SH/SZ/BJ calendar/session authority for one runtime and trade date.';

DO $$
DECLARE
    target_table TEXT;
    target_column TEXT;
    column_record RECORD;
BEGIN
    FOREACH target_table IN ARRAY ARRAY[
        'execution_kernel_worker_epoch',
        'execution_kernel_worker_incarnation',
        'execution_algo_event_delivery',
        'execution_algo_transition',
        'execution_algo_command_outbox',
        'execution_algo_command_dispatch_attempt',
        'execution_algo_timer_schedule',
        'execution_algo_timer_occurrence',
        'execution_exchange_session_authority',
        'execution_algo_diagnostic_observation'
    ] LOOP
        FOR target_column IN
            SELECT attribute.attname
            FROM pg_attribute AS attribute
            WHERE attribute.attrelid=format('qmt_strategy.%I', target_table)::regclass
              AND attribute.attnum > 0 AND NOT attribute.attisdropped
        LOOP
            EXECUTE format(
                'COMMENT ON COLUMN qmt_strategy.%I.%I IS %L',
                target_table,
                target_column,
                format('K2 durable %s.%s field; exact type, ownership, nullability and quality semantics are enforced by named constraints and the strict carrier.', target_table, target_column)
            );
        END LOOP;
    END LOOP;

    FOR column_record IN
        SELECT * FROM (VALUES
            ('execution_runtime_event','event_contract_version'),
            ('execution_runtime_event','event_schema_version'),
            ('execution_runtime_event','payload_schema_version'),
            ('execution_runtime_event','event_key_sha256'),
            ('execution_runtime_event','payload_sha256'),
            ('execution_runtime_event','observed_at_utc'),
            ('execution_runtime_event','logical_at_utc'),
            ('execution_runtime_event','source_identity_json'),
            ('execution_runtime_event','correlation_json'),
            ('execution_runtime_event','ingress_receipt_json'),
            ('execution_runtime_event','ingress_receipt_sha256'),
            ('execution_runtime_event','routing_rule_version'),
            ('execution_runtime_event','transaction_commit_identity'),
            ('execution_algo_instance','kernel_contract_version'),
            ('execution_algo_instance','traded_quantity'),
            ('execution_algo_instance','plugin_id'),
            ('execution_algo_instance','plugin_version'),
            ('execution_algo_instance','plugin_manifest_sha256'),
            ('execution_algo_instance','plugin_config_json'),
            ('execution_algo_instance','plugin_config_sha256'),
            ('execution_algo_instance','compatibility_receipt_sha256'),
            ('execution_algo_instance','state_schema_version'),
            ('execution_algo_instance','state_json'),
            ('execution_algo_instance','state_sha256'),
            ('execution_algo_instance','transition_sequence'),
            ('execution_algo_instance','last_applied_delivery_sequence'),
            ('execution_algo_instance','last_applied_delivery_id'),
            ('execution_algo_instance','last_closed_delivery_sequence'),
            ('execution_algo_instance','terminal_delivery_sequence'),
            ('execution_algo_instance','failure_receipt_id'),
            ('execution_algo_instance','active_child_closure_status'),
            ('execution_algo_instance','active_child_count'),
            ('execution_algo_instance','row_version'),
            ('execution_algo_instance','terminal_at_utc'),
            ('execution_algo_instance','kernel_carrier_json'),
            ('execution_child_order','kernel_contract_version'),
            ('execution_child_order','mapping_id'),
            ('execution_child_order','command_id'),
            ('execution_child_order','local_vt_orderid'),
            ('execution_child_order','deterministic_client_order_ref'),
            ('execution_child_order','order_remark'),
            ('execution_child_order','mapping_status'),
            ('execution_child_order','mapping_version'),
            ('execution_child_order','mapping_payload_sha256'),
            ('execution_child_order','mapping_receipt_sha256'),
            ('execution_child_order','broker_identity_source_event_id'),
            ('execution_child_order','last_order_event_id'),
            ('execution_child_order','last_trade_event_id'),
            ('execution_child_order','created_transition_id'),
            ('execution_child_order','updated_by_event_id'),
            ('execution_child_order','mapping_created_at_utc'),
            ('execution_child_order','mapping_updated_at_utc'),
            ('execution_child_order','mapping_json')
        ) AS added_column(table_name,column_name)
    LOOP
        EXECUTE format(
            'COMMENT ON COLUMN qmt_strategy.%I.%I IS %L',
            column_record.table_name,
            column_record.column_name,
            format('K2 additive %s field; LEGACY_V1 must remain empty and KERNEL_V2 semantics are fail-closed.', column_record.column_name)
        );
    END LOOP;
END $$;

COMMIT;

-- Independent readback: callers must verify these rows after every migration stage.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_proc AS function_record
        JOIN pg_namespace AS function_schema ON function_schema.oid=function_record.pronamespace
        JOIN pg_language AS function_language ON function_language.oid=function_record.prolang
        WHERE function_record.oid=to_regprocedure('qmt_strategy.miniqmt_k2_catalog_fingerprint()')
          AND function_language.lanname='sql'
          AND function_record.provolatile='s'
          AND pg_get_function_arguments(function_record.oid)=''
          AND pg_get_function_result(function_record.oid)='text'
          AND encode(
                sha256(
                    convert_to(
                        btrim(
                            replace(function_record.prosrc,function_schema.nspname,'<schema>'),
                            E' \n\r\t;'
                        ),
                        'UTF8'
                    )
                ),
                'hex'
              )='8d9c8b09b5c27a0b0caeeaea3663556b9876b0eea179057d691bbf2fce29c107'
    ) THEN
        RAISE EXCEPTION 'K2 post-commit catalog function drift';
    END IF;
END $$;

SELECT
    function_language.lanname AS independently_read_function_language,
    function_record.provolatile AS independently_read_function_volatility,
    pg_get_function_arguments(function_record.oid) AS independently_read_function_arguments,
    pg_get_function_result(function_record.oid) AS independently_read_function_result,
    encode(
        sha256(
            convert_to(
                btrim(
                    replace(function_record.prosrc,function_schema.nspname,'<schema>'),
                    E' \n\r\t;'
                ),
                'UTF8'
            )
        ),
        'hex'
    ) AS independently_recomputed_catalog_function_body_sha256
FROM pg_proc AS function_record
JOIN pg_namespace AS function_schema ON function_schema.oid=function_record.pronamespace
JOIN pg_language AS function_language ON function_language.oid=function_record.prolang
WHERE function_record.oid=to_regprocedure('qmt_strategy.miniqmt_k2_catalog_fingerprint()');

WITH target_tables(relname) AS (
    VALUES
        ('execution_kernel_worker_epoch'),
        ('execution_kernel_worker_incarnation'),
        ('execution_algo_event_delivery'),
        ('execution_algo_transition'),
        ('execution_algo_command_outbox'),
        ('execution_algo_command_dispatch_attempt'),
        ('execution_algo_timer_schedule'),
        ('execution_algo_timer_occurrence'),
        ('execution_exchange_session_authority'),
        ('execution_algo_diagnostic_observation')
), additive_columns(relname,attname) AS (
    VALUES
        ('execution_runtime','runtime_id'),
        ('execution_runtime','trade_date'),
        ('execution_runtime_event','event_contract_version'),
        ('execution_runtime_event','event_schema_version'),
        ('execution_runtime_event','payload_schema_version'),
        ('execution_runtime_event','event_key_sha256'),
        ('execution_runtime_event','payload_sha256'),
        ('execution_runtime_event','observed_at_utc'),
        ('execution_runtime_event','logical_at_utc'),
        ('execution_runtime_event','source_identity_json'),
        ('execution_runtime_event','correlation_json'),
        ('execution_runtime_event','ingress_receipt_json'),
        ('execution_runtime_event','ingress_receipt_sha256'),
        ('execution_runtime_event','routing_rule_version'),
        ('execution_runtime_event','transaction_commit_identity'),
        ('execution_algo_instance','kernel_contract_version'),
        ('execution_algo_instance','traded_quantity'),
        ('execution_algo_instance','plugin_id'),
        ('execution_algo_instance','plugin_version'),
        ('execution_algo_instance','plugin_manifest_sha256'),
        ('execution_algo_instance','plugin_config_json'),
        ('execution_algo_instance','plugin_config_sha256'),
        ('execution_algo_instance','compatibility_receipt_sha256'),
        ('execution_algo_instance','state_schema_version'),
        ('execution_algo_instance','state_json'),
        ('execution_algo_instance','state_sha256'),
        ('execution_algo_instance','transition_sequence'),
        ('execution_algo_instance','last_applied_delivery_sequence'),
        ('execution_algo_instance','last_applied_delivery_id'),
        ('execution_algo_instance','last_closed_delivery_sequence'),
        ('execution_algo_instance','terminal_delivery_sequence'),
        ('execution_algo_instance','failure_receipt_id'),
        ('execution_algo_instance','active_child_closure_status'),
        ('execution_algo_instance','active_child_count'),
        ('execution_algo_instance','row_version'),
        ('execution_algo_instance','terminal_at_utc'),
        ('execution_algo_instance','kernel_carrier_json'),
        ('execution_child_order','kernel_contract_version'),
        ('execution_child_order','mapping_id'),
        ('execution_child_order','command_id'),
        ('execution_child_order','local_vt_orderid'),
        ('execution_child_order','deterministic_client_order_ref'),
        ('execution_child_order','order_remark'),
        ('execution_child_order','mapping_status'),
        ('execution_child_order','mapping_version'),
        ('execution_child_order','mapping_payload_sha256'),
        ('execution_child_order','mapping_receipt_sha256'),
        ('execution_child_order','broker_identity_source_event_id'),
        ('execution_child_order','last_order_event_id'),
        ('execution_child_order','last_trade_event_id'),
        ('execution_child_order','created_transition_id'),
        ('execution_child_order','updated_by_event_id'),
        ('execution_child_order','mapping_created_at_utc'),
        ('execution_child_order','mapping_updated_at_utc'),
        ('execution_child_order','mapping_json')
), catalog_items(sort_key,item) AS (
    SELECT
        format('column:%s:%05s', table_class.relname, attribute.attnum),
        jsonb_build_array(
            'column', table_class.relname, attribute.attname,
            format_type(attribute.atttypid, attribute.atttypmod),
            attribute.attnotnull,
            coalesce(pg_get_expr(attribute_default.adbin, attribute_default.adrelid), '')
        )
    FROM pg_class AS table_class
    JOIN pg_namespace AS table_schema ON table_schema.oid=table_class.relnamespace
    JOIN pg_attribute AS attribute
      ON attribute.attrelid=table_class.oid AND attribute.attnum > 0 AND NOT attribute.attisdropped
    LEFT JOIN pg_attrdef AS attribute_default
      ON attribute_default.adrelid=table_class.oid AND attribute_default.adnum=attribute.attnum
    WHERE table_schema.nspname='qmt_strategy'
      AND (
          table_class.relname IN (SELECT relname FROM target_tables)
          OR (table_class.relname,attribute.attname) IN (SELECT relname,attname FROM additive_columns)
      )

    UNION ALL

    SELECT
        format('constraint:%s:%s', table_class.relname, constraint_record.conname),
        jsonb_build_array(
            'constraint', table_class.relname, constraint_record.conname,
            constraint_record.contype, constraint_record.condeferrable,
            constraint_record.condeferred, constraint_record.convalidated,
            replace(
                pg_get_constraintdef(constraint_record.oid, true),
                table_schema.nspname || '.', '<schema>.'
            )
        )
    FROM pg_constraint AS constraint_record
    JOIN pg_class AS table_class ON table_class.oid=constraint_record.conrelid
    JOIN pg_namespace AS table_schema ON table_schema.oid=table_class.relnamespace
    WHERE table_schema.nspname='qmt_strategy'
      AND (
          table_class.relname IN (SELECT relname FROM target_tables)
          OR constraint_record.conname LIKE '%miniqmt_k2%'
      )

    UNION ALL

    SELECT
        format('index:%s:%s', table_class.relname, index_class.relname),
        jsonb_build_array(
            'index', table_class.relname, index_class.relname,
            index_record.indisunique, index_record.indisprimary,
            index_record.indisvalid, index_record.indisready,
            replace(
                pg_get_indexdef(index_record.indexrelid, 0, true),
                table_schema.nspname || '.', '<schema>.'
            ),
            coalesce(
                replace(
                    pg_get_expr(index_record.indpred, index_record.indrelid, true),
                    table_schema.nspname || '.', '<schema>.'
                ),
                ''
            )
        )
    FROM pg_index AS index_record
    JOIN pg_class AS table_class ON table_class.oid=index_record.indrelid
    JOIN pg_class AS index_class ON index_class.oid=index_record.indexrelid
    JOIN pg_namespace AS table_schema ON table_schema.oid=table_class.relnamespace
    WHERE table_schema.nspname='qmt_strategy'
      AND (
          table_class.relname IN (SELECT relname FROM target_tables)
          OR index_class.relname LIKE '%miniqmt_k2%'
      )
), canonical_catalog AS (
    SELECT coalesce(jsonb_agg(item ORDER BY sort_key), '[]'::jsonb)::TEXT AS payload
    FROM catalog_items
)
SELECT encode(sha256(convert_to(payload, 'UTF8')), 'hex') AS independently_recomputed_schema_catalog_sha256
FROM canonical_catalog;

SELECT
    to_regclass('qmt_strategy.execution_algo_event_delivery') IS NOT NULL AS delivery_ready,
    to_regclass('qmt_strategy.execution_algo_transition') IS NOT NULL AS transition_ready,
    to_regclass('qmt_strategy.execution_algo_command_outbox') IS NOT NULL AS outbox_ready,
    to_regclass('qmt_strategy.execution_algo_timer_occurrence') IS NOT NULL AS timer_ready,
    to_regclass('qmt_strategy.execution_kernel_worker_incarnation') IS NOT NULL AS worker_ready,
    to_regclass('qmt_strategy.uq_miniqmt_k2_event_key') IS NOT NULL AS event_key_ready,
    to_regclass('qmt_strategy.uq_miniqmt_k2_child_mapping') IS NOT NULL AS child_mapping_ready,
    qmt_strategy.miniqmt_k2_catalog_fingerprint() AS schema_catalog_sha256;

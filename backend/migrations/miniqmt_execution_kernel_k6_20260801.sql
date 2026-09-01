-- MiniQMT K6-A product authority and dependent-BUY durable repository.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SELECT pg_advisory_xact_lock(hashtext('qmt_strategy.miniqmt_execution_kernel_k6_20260801'));

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_dependent_buy_coordination (
    coordination_id TEXT PRIMARY KEY,
    runtime_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    strategy_id TEXT NOT NULL,
    buy_algo_instance_id TEXT NOT NULL,
    buy_parent_intent_id TEXT NOT NULL,
    required_cash TEXT NOT NULL,
    release_command_payload_sha256 TEXT NOT NULL,
    status TEXT NOT NULL,
    decision_sequence BIGINT NOT NULL,
    last_decision_sha256 TEXT,
    released_command_id TEXT,
    released_outbox_id TEXT,
    row_version BIGINT NOT NULL,
    lease_worker_id TEXT,
    lease_process_incarnation_id TEXT,
    lease_epoch BIGINT NOT NULL,
    lease_expires_at_utc TIMESTAMPTZ,
    created_at_utc TIMESTAMPTZ NOT NULL,
    updated_at_utc TIMESTAMPTZ NOT NULL,
    carrier_json JSONB NOT NULL,
    coordination_sha256 TEXT NOT NULL,
    CONSTRAINT uq_miniqmt_k6_coordination_owner UNIQUE (runtime_id,buy_algo_instance_id,buy_parent_intent_id),
    CONSTRAINT uq_miniqmt_k6_coordination_runtime_identity UNIQUE (runtime_id,coordination_id),
    CONSTRAINT fk_miniqmt_k6_coordination_runtime_date FOREIGN KEY (runtime_id,trade_date)
        REFERENCES qmt_strategy.execution_runtime(runtime_id,trade_date),
    CONSTRAINT fk_miniqmt_k6_coordination_buy_algo FOREIGN KEY (runtime_id,buy_algo_instance_id)
        REFERENCES qmt_strategy.execution_algo_instance(runtime_id,algo_instance_id),
    CONSTRAINT fk_miniqmt_k6_coordination_lease FOREIGN KEY (lease_worker_id,lease_process_incarnation_id)
        REFERENCES qmt_strategy.execution_kernel_worker_incarnation(worker_id,process_incarnation_id),
    CONSTRAINT ck_miniqmt_k6_coordination_identity CHECK (
        btrim(coordination_id)<>'' AND btrim(binding_id)<>'' AND btrim(strategy_id)<>''
        AND btrim(buy_parent_intent_id)<>''
    ),
    CONSTRAINT ck_miniqmt_k6_coordination_cash CHECK (
        required_cash ~ '^(0|[1-9][0-9]*)(\.[0-9]+)?$'
    ),
    CONSTRAINT ck_miniqmt_k6_coordination_status CHECK (status IN (
        'DEFERRED_WAITING_SELL_PROCEEDS','RELEASED_TO_K2_OUTBOX',
        'BLOCKED_SELL_PROCEEDS_UNAVAILABLE','EOD_RESIDUAL'
    )),
    CONSTRAINT ck_miniqmt_k6_coordination_decision CHECK (
        decision_sequence>=0 AND row_version>0
        AND ((decision_sequence=0 AND last_decision_sha256 IS NULL)
             OR (decision_sequence>0 AND last_decision_sha256 ~ '^[0-9a-f]{64}$'))
    ),
    CONSTRAINT ck_miniqmt_k6_coordination_release CHECK (
        (status='RELEASED_TO_K2_OUTBOX' AND released_command_id IS NOT NULL AND released_outbox_id IS NOT NULL
            AND released_command_id=released_outbox_id)
        OR (status<>'RELEASED_TO_K2_OUTBOX' AND released_command_id IS NULL AND released_outbox_id IS NULL)
    ),
    CONSTRAINT ck_miniqmt_k6_coordination_lease CHECK (
        (lease_epoch=0 AND lease_worker_id IS NULL AND lease_process_incarnation_id IS NULL AND lease_expires_at_utc IS NULL)
        OR (lease_epoch>0 AND lease_worker_id IS NOT NULL AND lease_process_incarnation_id IS NOT NULL AND lease_expires_at_utc IS NOT NULL)
    ),
    CONSTRAINT ck_miniqmt_k6_coordination_hash CHECK (
        release_command_payload_sha256 ~ '^[0-9a-f]{64}$' AND coordination_sha256 ~ '^[0-9a-f]{64}$'
    )
);

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_dependent_buy_dependency (
    coordination_id TEXT NOT NULL,
    runtime_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    sell_parent_intent_id TEXT NOT NULL,
    sell_algo_instance_id TEXT NOT NULL,
    latest_order_fact_ref TEXT,
    settled_trade_fact_refs JSONB NOT NULL,
    settled_cash_ledger_refs JSONB NOT NULL,
    dependency_status TEXT NOT NULL,
    carrier_json JSONB NOT NULL,
    dependency_sha256 TEXT NOT NULL,
    CONSTRAINT pk_miniqmt_k6_dependency PRIMARY KEY (coordination_id,sell_parent_intent_id),
    CONSTRAINT uq_miniqmt_k6_dependency_hash UNIQUE (coordination_id,dependency_sha256),
    CONSTRAINT fk_miniqmt_k6_dependency_coordination FOREIGN KEY (runtime_id,coordination_id)
        REFERENCES qmt_strategy.execution_dependent_buy_coordination(runtime_id,coordination_id),
    CONSTRAINT fk_miniqmt_k6_dependency_sell_algo FOREIGN KEY (runtime_id,sell_algo_instance_id)
        REFERENCES qmt_strategy.execution_algo_instance(runtime_id,algo_instance_id),
    CONSTRAINT ck_miniqmt_k6_dependency_status CHECK (dependency_status IN (
        'OPEN','PROCEEDS_SETTLED','TERMINAL_WITHOUT_SUFFICIENT_PROCEEDS'
    )),
    CONSTRAINT ck_miniqmt_k6_dependency_json CHECK (
        jsonb_typeof(settled_trade_fact_refs)='array' AND jsonb_typeof(settled_cash_ledger_refs)='array'
        AND jsonb_array_length(settled_trade_fact_refs)=jsonb_array_length(settled_cash_ledger_refs)
    ),
    CONSTRAINT ck_miniqmt_k6_dependency_hash CHECK (
        (latest_order_fact_ref IS NULL OR latest_order_fact_ref ~ '^[0-9a-f]{64}$')
        AND dependency_sha256 ~ '^[0-9a-f]{64}$'
    )
);

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_product_command_authority (
    authority_set_sha256 TEXT PRIMARY KEY,
    transition_id TEXT NOT NULL UNIQUE,
    runtime_id TEXT NOT NULL,
    algo_instance_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    delivery_id TEXT NOT NULL,
    catalog_sha256 TEXT NOT NULL,
    creation_binding_sha256 TEXT NOT NULL,
    facade_conformance_set_sha256 TEXT NOT NULL,
    execution_projection_set_sha256 TEXT NOT NULL,
    transition_receipt_sha256 TEXT NOT NULL,
    materialize_count INTEGER NOT NULL,
    reject_count INTEGER NOT NULL,
    total_count INTEGER NOT NULL,
    aggregate_disposition TEXT NOT NULL,
    carrier_json JSONB NOT NULL,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_miniqmt_k6_authority_transition_owner UNIQUE (runtime_id,algo_instance_id,transition_id),
    CONSTRAINT fk_miniqmt_k6_authority_transition FOREIGN KEY (runtime_id,algo_instance_id,transition_id)
        REFERENCES qmt_strategy.execution_algo_transition(runtime_id,algo_instance_id,transition_id),
    CONSTRAINT ck_miniqmt_k6_authority_counts CHECK (
        materialize_count>=0 AND reject_count>=0 AND total_count BETWEEN 0 AND 256
        AND materialize_count+reject_count=total_count
    ),
    CONSTRAINT ck_miniqmt_k6_authority_disposition CHECK (aggregate_disposition IN (
        'ZERO_COMMAND','ALL_REJECTED','MATERIALIZE_ALL_ACCEPTED_COMMANDS','MIXED_PER_COMMAND'
    )),
    CONSTRAINT ck_miniqmt_k6_authority_hash CHECK (
        authority_set_sha256 ~ '^[0-9a-f]{64}$' AND catalog_sha256 ~ '^[0-9a-f]{64}$'
        AND creation_binding_sha256 ~ '^[0-9a-f]{64}$' AND facade_conformance_set_sha256 ~ '^[0-9a-f]{64}$'
        AND execution_projection_set_sha256 ~ '^[0-9a-f]{64}$' AND transition_receipt_sha256 ~ '^[0-9a-f]{64}$'
    )
);

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_product_command_authority_item (
    authority_set_sha256 TEXT NOT NULL,
    transition_id TEXT NOT NULL,
    effect_ordinal INTEGER NOT NULL,
    command_id TEXT NOT NULL,
    disposition TEXT NOT NULL,
    mapping_id TEXT,
    outbox_id TEXT,
    child_order_id TEXT,
    reject_reason_code TEXT,
    reject_context_sha256 TEXT,
    carrier_json JSONB NOT NULL,
    item_sha256 TEXT NOT NULL,
    CONSTRAINT pk_miniqmt_k6_authority_item PRIMARY KEY (authority_set_sha256,effect_ordinal,command_id),
    CONSTRAINT uq_miniqmt_k6_authority_item_command UNIQUE (command_id),
    CONSTRAINT uq_miniqmt_k6_authority_item_ordinal UNIQUE (transition_id,effect_ordinal),
    CONSTRAINT fk_miniqmt_k6_authority_item_set FOREIGN KEY (authority_set_sha256)
        REFERENCES qmt_strategy.execution_product_command_authority(authority_set_sha256),
    CONSTRAINT fk_miniqmt_k6_authority_item_outbox FOREIGN KEY (outbox_id)
        REFERENCES qmt_strategy.execution_algo_command_outbox(command_id) DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT fk_miniqmt_k6_authority_item_mapping FOREIGN KEY (mapping_id)
        REFERENCES qmt_strategy.execution_child_order(mapping_id) DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT fk_miniqmt_k6_authority_item_child FOREIGN KEY (child_order_id)
        REFERENCES qmt_strategy.execution_child_order(child_order_id) DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT ck_miniqmt_k6_authority_item_ordinal CHECK (effect_ordinal BETWEEN 0 AND 255),
    CONSTRAINT ck_miniqmt_k6_authority_item_disposition CHECK (disposition IN ('MATERIALIZE','REJECT_SYNCHRONOUS')),
    CONSTRAINT ck_miniqmt_k6_authority_item_presence CHECK (
        (disposition='MATERIALIZE' AND mapping_id IS NOT NULL AND outbox_id IS NOT NULL AND child_order_id IS NOT NULL
            AND reject_reason_code IS NULL AND reject_context_sha256 IS NULL)
        OR (disposition='REJECT_SYNCHRONOUS' AND mapping_id IS NULL AND outbox_id IS NULL AND child_order_id IS NULL
            AND reject_reason_code IS NOT NULL AND reject_context_sha256 ~ '^[0-9a-f]{64}$')
    ),
    CONSTRAINT ck_miniqmt_k6_authority_item_hash CHECK (item_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_dependent_buy_decision (
    decision_id TEXT PRIMARY KEY,
    coordination_id TEXT NOT NULL,
    decision_sequence BIGINT NOT NULL,
    previous_decision_sha256 TEXT,
    trigger_ref_sha256 TEXT NOT NULL,
    trigger_event_id TEXT NOT NULL,
    trigger_ref_json JSONB NOT NULL,
    decision TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    ledger_observation_sha256 TEXT NOT NULL,
    ledger_virtual_account_id TEXT NOT NULL,
    ledger_row_version BIGINT NOT NULL,
    ledger_observation_json JSONB NOT NULL,
    ordered_dependency_sha256s JSONB NOT NULL,
    release_event_id TEXT,
    release_transition_id TEXT,
    release_command_authority_set_sha256 TEXT,
    decided_at_utc TIMESTAMPTZ NOT NULL,
    worker_id TEXT NOT NULL,
    process_incarnation_id TEXT NOT NULL,
    lease_epoch BIGINT NOT NULL,
    carrier_json JSONB NOT NULL,
    decision_sha256 TEXT NOT NULL UNIQUE,
    CONSTRAINT uq_miniqmt_k6_decision_sequence UNIQUE (coordination_id,decision_sequence),
    CONSTRAINT fk_miniqmt_k6_decision_coordination FOREIGN KEY (coordination_id)
        REFERENCES qmt_strategy.execution_dependent_buy_coordination(coordination_id),
    CONSTRAINT fk_miniqmt_k6_decision_previous FOREIGN KEY (previous_decision_sha256)
        REFERENCES qmt_strategy.execution_dependent_buy_decision(decision_sha256),
    CONSTRAINT fk_miniqmt_k6_decision_worker FOREIGN KEY (worker_id,process_incarnation_id)
        REFERENCES qmt_strategy.execution_kernel_worker_incarnation(worker_id,process_incarnation_id),
    CONSTRAINT fk_miniqmt_k6_decision_trigger_event FOREIGN KEY (trigger_event_id)
        REFERENCES qmt_strategy.execution_runtime_event(event_id),
    CONSTRAINT fk_miniqmt_k6_decision_release_event FOREIGN KEY (release_event_id)
        REFERENCES qmt_strategy.execution_runtime_event(event_id) DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT fk_miniqmt_k6_decision_release_transition FOREIGN KEY (release_transition_id)
        REFERENCES qmt_strategy.execution_algo_transition(transition_id) DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT fk_miniqmt_k6_decision_release_authority FOREIGN KEY (release_command_authority_set_sha256)
        REFERENCES qmt_strategy.execution_product_command_authority(authority_set_sha256) DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT ck_miniqmt_k6_decision_kind CHECK (decision IN ('WAIT','RELEASE_TO_K2_OUTBOX','BLOCK','EOD_RESIDUAL')),
    CONSTRAINT ck_miniqmt_k6_decision_presence CHECK (
        (decision='RELEASE_TO_K2_OUTBOX' AND release_event_id IS NOT NULL AND release_transition_id IS NOT NULL
            AND release_command_authority_set_sha256 IS NOT NULL)
        OR (decision<>'RELEASE_TO_K2_OUTBOX' AND release_event_id IS NULL AND release_transition_id IS NULL
            AND release_command_authority_set_sha256 IS NULL)
    ),
    CONSTRAINT ck_miniqmt_k6_decision_predecessor CHECK (
        (decision_sequence=1 AND previous_decision_sha256 IS NULL)
        OR (decision_sequence>1 AND previous_decision_sha256 ~ '^[0-9a-f]{64}$')
    ),
    CONSTRAINT ck_miniqmt_k6_decision_hash CHECK (
        decision_id ~ '^[0-9a-f]{64}$' AND trigger_ref_sha256 ~ '^[0-9a-f]{64}$'
        AND ledger_observation_sha256 ~ '^[0-9a-f]{64}$' AND decision_sha256 ~ '^[0-9a-f]{64}$'
        AND lease_epoch>0 AND ledger_row_version>0
        AND jsonb_typeof(ordered_dependency_sha256s)='array'
        AND jsonb_typeof(trigger_ref_json)='object'
        AND jsonb_typeof(ledger_observation_json)='object'
    )
);

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_product_route_cutover (
    runtime_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    route_epoch BIGINT NOT NULL,
    route_owner TEXT NOT NULL,
    effective_new_instance_sequence BIGINT NOT NULL,
    legacy_active_instance_count INTEGER NOT NULL,
    kernel_active_instance_count INTEGER NOT NULL,
    catalog_sha256 TEXT NOT NULL,
    gateway_capability_catalog_sha256 TEXT NOT NULL,
    exchange_session_authority_sha256 TEXT NOT NULL,
    migration_readback_sha256 TEXT NOT NULL,
    product_authority_schema_sha256 TEXT NOT NULL,
    previous_receipt_sha256 TEXT,
    created_at_utc TIMESTAMPTZ NOT NULL,
    carrier_json JSONB NOT NULL,
    receipt_sha256 TEXT NOT NULL,
    CONSTRAINT pk_miniqmt_k6_route_cutover PRIMARY KEY (runtime_id,binding_id,trade_date,route_epoch),
    CONSTRAINT uq_miniqmt_k6_route_receipt UNIQUE (receipt_sha256),
    CONSTRAINT uq_miniqmt_k6_route_receipt_owner UNIQUE (runtime_id,binding_id,trade_date,route_epoch,receipt_sha256),
    CONSTRAINT fk_miniqmt_k6_route_runtime_date FOREIGN KEY (runtime_id,trade_date)
        REFERENCES qmt_strategy.execution_runtime(runtime_id,trade_date),
    CONSTRAINT fk_miniqmt_k6_route_previous FOREIGN KEY (previous_receipt_sha256)
        REFERENCES qmt_strategy.execution_product_route_cutover(receipt_sha256),
    CONSTRAINT ck_miniqmt_k6_route_kind CHECK (route_owner IN ('LEGACY_DRAIN_ONLY','KERNEL_V2')),
    CONSTRAINT ck_miniqmt_k6_route_sequence CHECK (
        route_epoch>0 AND effective_new_instance_sequence>0
        AND legacy_active_instance_count>=0 AND kernel_active_instance_count>=0
        AND ((route_epoch=1 AND previous_receipt_sha256 IS NULL)
             OR (route_epoch>1 AND previous_receipt_sha256 ~ '^[0-9a-f]{64}$'))
    ),
    CONSTRAINT ck_miniqmt_k6_route_hash CHECK (
        receipt_sha256 ~ '^[0-9a-f]{64}$' AND catalog_sha256 ~ '^[0-9a-f]{64}$'
        AND gateway_capability_catalog_sha256 ~ '^[0-9a-f]{64}$'
        AND exchange_session_authority_sha256 ~ '^[0-9a-f]{64}$'
        AND migration_readback_sha256 ~ '^[0-9a-f]{64}$'
        AND product_authority_schema_sha256 ~ '^[0-9a-f]{64}$'
    )
);

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_product_route_owner (
    runtime_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    current_route_epoch BIGINT NOT NULL,
    current_receipt_sha256 TEXT NOT NULL,
    route_owner TEXT NOT NULL,
    effective_new_instance_sequence BIGINT NOT NULL,
    row_version BIGINT NOT NULL,
    carrier_json JSONB NOT NULL,
    owner_sha256 TEXT NOT NULL,
    CONSTRAINT pk_miniqmt_k6_route_owner PRIMARY KEY (runtime_id,binding_id,trade_date),
    CONSTRAINT fk_miniqmt_k6_route_owner_receipt FOREIGN KEY (
        runtime_id,binding_id,trade_date,current_route_epoch,current_receipt_sha256
    ) REFERENCES qmt_strategy.execution_product_route_cutover(
        runtime_id,binding_id,trade_date,route_epoch,receipt_sha256
    ),
    CONSTRAINT ck_miniqmt_k6_route_owner_kind CHECK (route_owner IN ('LEGACY_DRAIN_ONLY','KERNEL_V2')),
    CONSTRAINT ck_miniqmt_k6_route_owner_version CHECK (
        current_route_epoch>0 AND effective_new_instance_sequence>0 AND row_version>0
    ),
    CONSTRAINT ck_miniqmt_k6_route_owner_hash CHECK (
        current_receipt_sha256 ~ '^[0-9a-f]{64}$' AND owner_sha256 ~ '^[0-9a-f]{64}$'
    )
);

CREATE INDEX IF NOT EXISTS ix_miniqmt_k6_coordination_recovery
ON qmt_strategy.execution_dependent_buy_coordination(runtime_id,status,updated_at_utc,coordination_id);
CREATE INDEX IF NOT EXISTS ix_miniqmt_k6_coordination_strategy_date
ON qmt_strategy.execution_dependent_buy_coordination(strategy_id,trade_date,status);
CREATE INDEX IF NOT EXISTS ix_miniqmt_k6_decision_coordination
ON qmt_strategy.execution_dependent_buy_decision(coordination_id,decision_sequence);
CREATE INDEX IF NOT EXISTS ix_miniqmt_k6_authority_runtime_transition
ON qmt_strategy.execution_product_command_authority(runtime_id,transition_id);
CREATE INDEX IF NOT EXISTS ix_miniqmt_k6_route_owner_kind
ON qmt_strategy.execution_product_route_owner(trade_date,route_owner,runtime_id,binding_id);

COMMENT ON TABLE qmt_strategy.execution_dependent_buy_coordination IS 'K6 durable dependent-BUY owner; one exact BUY release state machine.';
COMMENT ON TABLE qmt_strategy.execution_dependent_buy_dependency IS 'K6 exact SELL dependency facts for one dependent BUY.';
COMMENT ON TABLE qmt_strategy.execution_dependent_buy_decision IS 'K6 append-only dependent-BUY trigger/ledger/decision history.';
COMMENT ON TABLE qmt_strategy.execution_product_command_authority IS 'K6 exact transition-level 0..N product command authority aggregate.';
COMMENT ON TABLE qmt_strategy.execution_product_command_authority_item IS 'K6 exact per-command product authority and deterministic K2 lineage.';
COMMENT ON TABLE qmt_strategy.execution_product_route_cutover IS 'K6 immutable route cutover receipt chain; not a manual approval record.';
COMMENT ON TABLE qmt_strategy.execution_product_route_owner IS 'K6 CAS current route pointer; new instances have one product owner.';

COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.coordination_id IS 'Deterministic K6 dependent-BUY owner SHA-256 identity.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.runtime_id IS 'Owning MiniQMT runtime identity from K2.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.binding_id IS 'Frozen simulation binding identity; non-null.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.trade_date IS 'Exchange trade date from K2 runtime authority.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.strategy_id IS 'Frozen strategy identity; not a package admission check.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.buy_algo_instance_id IS 'Owning durable BUY algorithm instance identity.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.buy_parent_intent_id IS 'Frozen BUY parent intent identity.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.required_cash IS 'Canonical non-negative required cash in CNY.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.release_command_payload_sha256 IS 'SHA-256 of the frozen BUY release command payload.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.status IS 'Durable K6 coordination state; transient release readiness cannot be persisted.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.decision_sequence IS 'Monotonic committed decision sequence, starting at zero.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.last_decision_sha256 IS 'Latest append-only decision SHA-256; null before first decision.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.released_command_id IS 'Exact K2 released command identity; RELEASED only.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.released_outbox_id IS 'Exact K2 outbox identity; equal to released command identity.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.row_version IS 'Positive optimistic-CAS row version.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.lease_worker_id IS 'Current K6 coordinator worker identity; null when unleased.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.lease_process_incarnation_id IS 'Current process incarnation identity; null when unleased.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.lease_epoch IS 'Monotonic coordination lease epoch; zero before first claim.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.lease_expires_at_utc IS 'UTC lease expiry; null when unleased.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.created_at_utc IS 'Immutable UTC creation timestamp.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.updated_at_utc IS 'Latest committed UTC state timestamp.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.carrier_json IS 'Strict miniqmt_dependent_buy_coordination_v1 JSON; repository-owned, hash/readback verified.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.coordination_sha256 IS 'Canonical durable business-field SHA-256 excluding lease and row-version facts.';

COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_dependency.coordination_id IS 'Owning dependent-BUY coordination identity.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_dependency.runtime_id IS 'Owning K2 runtime identity.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_dependency.strategy_id IS 'Frozen strategy identity shared with coordination.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_dependency.sell_parent_intent_id IS 'Exact dependent SELL parent intent identity.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_dependency.sell_algo_instance_id IS 'Exact durable SELL algorithm instance identity.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_dependency.latest_order_fact_ref IS 'Optional SHA-256 reference to latest authoritative SELL order fact.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_dependency.settled_trade_fact_refs IS 'Ordered unique JSON array of settled SELL trade SHA-256 refs; schema v1.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_dependency.settled_cash_ledger_refs IS 'Ordered one-to-one JSON array of settled cash-ledger SHA-256 refs; schema v1.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_dependency.dependency_status IS 'OPEN, PROCEEDS_SETTLED, or terminal without sufficient proceeds.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_dependency.carrier_json IS 'Strict miniqmt_dependent_buy_sell_dependency_v1 JSON; repository-owned and readback verified.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_dependency.dependency_sha256 IS 'Canonical dependency SHA-256.';

COMMENT ON COLUMN qmt_strategy.execution_product_command_authority.authority_set_sha256 IS 'Canonical transition-level ProductCommandAuthoritySetV2 SHA-256.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority.transition_id IS 'Exact owning K2 transition identity.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority.runtime_id IS 'Owning K2 runtime identity.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority.algo_instance_id IS 'Owning durable algorithm instance identity.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority.event_id IS 'Exact K2 runtime event identity.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority.delivery_id IS 'Exact K2 delivery identity.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority.catalog_sha256 IS 'Strict plugin catalog SHA-256 used for evaluation.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority.creation_binding_sha256 IS 'Frozen algorithm creation-binding SHA-256.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority.facade_conformance_set_sha256 IS 'Exact vn.py facade conformance-set SHA-256.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority.execution_projection_set_sha256 IS 'Exact K4 execution projection-set SHA-256.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority.transition_receipt_sha256 IS 'Exact K2 transition receipt SHA-256.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority.materialize_count IS 'Number of MATERIALIZE command items.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority.reject_count IS 'Number of synchronous reject command items.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority.total_count IS 'Exact transition command cardinality in range 0..256.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority.aggregate_disposition IS 'ZERO, all rejected, all materialized, or mixed per-command disposition.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority.carrier_json IS 'Strict miniqmt_product_command_authority_set_v2 JSON; repository-owned and readback verified.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority.created_at_utc IS 'Database UTC append timestamp.';

COMMENT ON COLUMN qmt_strategy.execution_product_command_authority_item.authority_set_sha256 IS 'Owning transition authority-set SHA-256.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority_item.transition_id IS 'Exact owning K2 transition identity.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority_item.effect_ordinal IS 'Contiguous zero-based plugin effect ordinal.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority_item.command_id IS 'Exact deterministic K2 command/outbox identity.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority_item.disposition IS 'MATERIALIZE or REJECT_SYNCHRONOUS.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority_item.mapping_id IS 'Exact K2 command-child mapping identity; materialized only.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority_item.outbox_id IS 'Exact K2 outbox identity equal to command_id; materialized only.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority_item.child_order_id IS 'Exact K2 child-order identity; materialized only.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority_item.reject_reason_code IS 'Typed synchronous rejection reason; rejected only.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority_item.reject_context_sha256 IS 'Bounded rejection-context SHA-256; rejected only.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority_item.carrier_json IS 'Strict miniqmt_product_command_authority_item_v2 JSON; repository-owned and readback verified.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority_item.item_sha256 IS 'Canonical per-command authority SHA-256.';

COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.decision_id IS 'Deterministic coordination/sequence/trigger decision identity.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.coordination_id IS 'Owning dependent-BUY coordination identity.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.decision_sequence IS 'Exact positive successor decision sequence.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.previous_decision_sha256 IS 'Previous append-only decision SHA-256; null for sequence one.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.trigger_ref_sha256 IS 'Canonical trigger evidence SHA-256.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.trigger_event_id IS 'Exact existing K2 event identity that triggered evaluation.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.trigger_ref_json IS 'Strict miniqmt_dependent_buy_trigger_event_ref_v1 JSON; complete durable trigger evidence.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.decision IS 'WAIT, RELEASE_TO_K2_OUTBOX, BLOCK, or EOD_RESIDUAL.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.reason_code IS 'Typed K6 coordination decision reason.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.ledger_observation_sha256 IS 'Canonical ledger observation SHA-256.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.ledger_virtual_account_id IS 'Authoritative strategy-ledger virtual-account identity.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.ledger_row_version IS 'Positive observed virtual-account row version.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.ledger_observation_json IS 'Strict miniqmt_dependent_buy_ledger_observation_v1 JSON; complete durable cash evidence.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.ordered_dependency_sha256s IS 'Ordered unique JSON array of all dependency SHA-256 identities.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.release_event_id IS 'K2 release event identity; RELEASE only.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.release_transition_id IS 'K2 release transition identity; RELEASE only.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.release_command_authority_set_sha256 IS 'K6 command-authority set SHA-256; RELEASE only.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.decided_at_utc IS 'UTC decision timestamp.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.worker_id IS 'Current PRODUCT_COORDINATOR worker identity.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.process_incarnation_id IS 'Current worker process incarnation identity.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.lease_epoch IS 'Exact coordination lease epoch used for this decision.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.carrier_json IS 'Strict miniqmt_dependent_buy_release_decision_v1 JSON; repository-owned and readback verified.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.decision_sha256 IS 'Canonical append-only decision SHA-256.';

COMMENT ON COLUMN qmt_strategy.execution_product_route_cutover.runtime_id IS 'Owning K2 runtime identity.';
COMMENT ON COLUMN qmt_strategy.execution_product_route_cutover.binding_id IS 'Frozen simulation binding identity.';
COMMENT ON COLUMN qmt_strategy.execution_product_route_cutover.trade_date IS 'Exchange trade date from K2 runtime authority.';
COMMENT ON COLUMN qmt_strategy.execution_product_route_cutover.route_epoch IS 'Strictly increasing route receipt epoch.';
COMMENT ON COLUMN qmt_strategy.execution_product_route_cutover.route_owner IS 'LEGACY_DRAIN_ONLY or irreversible KERNEL_V2 owner.';
COMMENT ON COLUMN qmt_strategy.execution_product_route_cutover.effective_new_instance_sequence IS 'First new algorithm-instance sequence owned by this route.';
COMMENT ON COLUMN qmt_strategy.execution_product_route_cutover.legacy_active_instance_count IS 'Observed legacy instances still draining.';
COMMENT ON COLUMN qmt_strategy.execution_product_route_cutover.kernel_active_instance_count IS 'Observed KERNEL_V2 active instance count.';
COMMENT ON COLUMN qmt_strategy.execution_product_route_cutover.catalog_sha256 IS 'Exact plugin catalog SHA-256.';
COMMENT ON COLUMN qmt_strategy.execution_product_route_cutover.gateway_capability_catalog_sha256 IS 'Exact gateway capability catalog SHA-256.';
COMMENT ON COLUMN qmt_strategy.execution_product_route_cutover.exchange_session_authority_sha256 IS 'Exact exchange-session authority SHA-256.';
COMMENT ON COLUMN qmt_strategy.execution_product_route_cutover.migration_readback_sha256 IS 'Independent K6 migration readback SHA-256.';
COMMENT ON COLUMN qmt_strategy.execution_product_route_cutover.product_authority_schema_sha256 IS 'Exact K6 product-authority schema SHA-256.';
COMMENT ON COLUMN qmt_strategy.execution_product_route_cutover.previous_receipt_sha256 IS 'Previous route receipt SHA-256; null at epoch one.';
COMMENT ON COLUMN qmt_strategy.execution_product_route_cutover.created_at_utc IS 'Immutable UTC route receipt timestamp.';
COMMENT ON COLUMN qmt_strategy.execution_product_route_cutover.carrier_json IS 'Strict miniqmt_product_route_cutover_receipt_v1 JSON; append-only and readback verified.';
COMMENT ON COLUMN qmt_strategy.execution_product_route_cutover.receipt_sha256 IS 'Canonical route receipt SHA-256.';

COMMENT ON COLUMN qmt_strategy.execution_product_route_owner.runtime_id IS 'Owning K2 runtime identity.';
COMMENT ON COLUMN qmt_strategy.execution_product_route_owner.binding_id IS 'Frozen simulation binding identity.';
COMMENT ON COLUMN qmt_strategy.execution_product_route_owner.trade_date IS 'Exchange trade date.';
COMMENT ON COLUMN qmt_strategy.execution_product_route_owner.current_route_epoch IS 'Current monotonic route epoch.';
COMMENT ON COLUMN qmt_strategy.execution_product_route_owner.current_receipt_sha256 IS 'Exact current immutable route receipt SHA-256.';
COMMENT ON COLUMN qmt_strategy.execution_product_route_owner.route_owner IS 'Current irreversible product route owner.';
COMMENT ON COLUMN qmt_strategy.execution_product_route_owner.effective_new_instance_sequence IS 'Current new-instance cutover sequence.';
COMMENT ON COLUMN qmt_strategy.execution_product_route_owner.row_version IS 'Positive optimistic-CAS owner version.';
COMMENT ON COLUMN qmt_strategy.execution_product_route_owner.carrier_json IS 'Strict miniqmt_product_route_owner_v1 JSON; repository-owned and readback verified.';
COMMENT ON COLUMN qmt_strategy.execution_product_route_owner.owner_sha256 IS 'Canonical current route-owner SHA-256.';

CREATE OR REPLACE FUNCTION qmt_strategy.miniqmt_k6_reject_immutable_mutation()
RETURNS trigger
LANGUAGE plpgsql
VOLATILE
AS $$
BEGIN
    RAISE EXCEPTION 'K6 append-only durable fact cannot be %: table=%', TG_OP, TG_TABLE_NAME;
END
$$;

CREATE OR REPLACE FUNCTION qmt_strategy.miniqmt_k6_validate_route_owner()
RETURNS trigger
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE receipt_record qmt_strategy.execution_product_route_cutover%ROWTYPE;
BEGIN
    SELECT * INTO STRICT receipt_record
    FROM qmt_strategy.execution_product_route_cutover
    WHERE runtime_id=NEW.runtime_id AND binding_id=NEW.binding_id AND trade_date=NEW.trade_date
      AND route_epoch=NEW.current_route_epoch AND receipt_sha256=NEW.current_receipt_sha256;
    IF (NEW.route_owner,NEW.effective_new_instance_sequence)
       IS DISTINCT FROM (receipt_record.route_owner,receipt_record.effective_new_instance_sequence) THEN
        RAISE EXCEPTION 'K6 route owner scalar facts differ from immutable receipt';
    END IF;
    IF TG_OP='INSERT' THEN
        IF NEW.current_route_epoch<>1 OR NEW.row_version<>1 OR receipt_record.previous_receipt_sha256 IS NOT NULL THEN
            RAISE EXCEPTION 'K6 route owner first write requires epoch/version one';
        END IF;
    ELSE
        IF NEW.current_route_epoch<>OLD.current_route_epoch+1 OR NEW.row_version<>OLD.row_version+1
           OR receipt_record.previous_receipt_sha256<>OLD.current_receipt_sha256 THEN
            RAISE EXCEPTION 'K6 route owner update is not the exact receipt/CAS successor';
        END IF;
        IF OLD.route_owner='KERNEL_V2' AND NEW.route_owner<>'KERNEL_V2' THEN
            RAISE EXCEPTION 'K6 product route cannot revert from KERNEL_V2';
        END IF;
    END IF;
    RETURN NEW;
EXCEPTION WHEN NO_DATA_FOUND THEN
    RAISE EXCEPTION 'K6 route owner references an unavailable immutable receipt';
END
$$;

CREATE OR REPLACE FUNCTION qmt_strategy.miniqmt_k6_validate_coordination_update()
RETURNS trigger
LANGUAGE plpgsql
VOLATILE
AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        IF NEW.status<>'DEFERRED_WAITING_SELL_PROCEEDS'
           OR NEW.decision_sequence<>0 OR NEW.last_decision_sha256 IS NOT NULL
           OR NEW.released_command_id IS NOT NULL OR NEW.released_outbox_id IS NOT NULL
           OR NEW.row_version<>1 OR NEW.lease_epoch<>0
           OR NEW.lease_worker_id IS NOT NULL OR NEW.lease_process_incarnation_id IS NOT NULL
           OR NEW.lease_expires_at_utc IS NOT NULL THEN
            RAISE EXCEPTION 'K6 dependent-BUY first write requires exact waiting initial state';
        END IF;
        RETURN NEW;
    END IF;
    IF (NEW.coordination_id,NEW.runtime_id,NEW.binding_id,NEW.trade_date,NEW.strategy_id,
        NEW.buy_algo_instance_id,NEW.buy_parent_intent_id,NEW.required_cash,
        NEW.release_command_payload_sha256,NEW.created_at_utc)
       IS DISTINCT FROM
       (OLD.coordination_id,OLD.runtime_id,OLD.binding_id,OLD.trade_date,OLD.strategy_id,
        OLD.buy_algo_instance_id,OLD.buy_parent_intent_id,OLD.required_cash,
        OLD.release_command_payload_sha256,OLD.created_at_utc) THEN
        RAISE EXCEPTION 'K6 dependent-BUY coordination immutable owner/payload drift';
    END IF;
    IF OLD.status IN ('RELEASED_TO_K2_OUTBOX','BLOCKED_SELL_PROCEEDS_UNAVAILABLE','EOD_RESIDUAL') THEN
        RAISE EXCEPTION 'K6 dependent-BUY terminal coordination cannot be updated';
    END IF;
    IF NEW.row_version<>OLD.row_version+1 OR NEW.decision_sequence<OLD.decision_sequence
       OR NEW.lease_epoch NOT IN (OLD.lease_epoch,OLD.lease_epoch+1)
       OR (NEW.lease_epoch=OLD.lease_epoch AND
           (NEW.lease_worker_id,NEW.lease_process_incarnation_id)
           IS DISTINCT FROM (OLD.lease_worker_id,OLD.lease_process_incarnation_id)) THEN
        RAISE EXCEPTION 'K6 dependent-BUY coordination update is not a monotonic CAS successor';
    END IF;
    RETURN NEW;
END
$$;

CREATE OR REPLACE FUNCTION qmt_strategy.miniqmt_k6_validate_decision_closure()
RETURNS trigger
LANGUAGE plpgsql
VOLATILE
AS $$
DECLARE
    coordination_record qmt_strategy.execution_dependent_buy_coordination%ROWTYPE;
    expected_status TEXT;
BEGIN
    SELECT * INTO STRICT coordination_record
    FROM qmt_strategy.execution_dependent_buy_coordination
    WHERE coordination_id=NEW.coordination_id;
    expected_status := CASE NEW.decision
        WHEN 'WAIT' THEN 'DEFERRED_WAITING_SELL_PROCEEDS'
        WHEN 'RELEASE_TO_K2_OUTBOX' THEN 'RELEASED_TO_K2_OUTBOX'
        WHEN 'BLOCK' THEN 'BLOCKED_SELL_PROCEEDS_UNAVAILABLE'
        WHEN 'EOD_RESIDUAL' THEN 'EOD_RESIDUAL'
    END;
    IF coordination_record.decision_sequence<>NEW.decision_sequence
       OR coordination_record.last_decision_sha256<>NEW.decision_sha256
       OR coordination_record.status<>expected_status
       OR (coordination_record.lease_worker_id,coordination_record.lease_process_incarnation_id,
           coordination_record.lease_epoch)
          IS DISTINCT FROM (NEW.worker_id,NEW.process_incarnation_id,NEW.lease_epoch) THEN
        RAISE EXCEPTION 'K6 dependent-BUY decision does not close to coordination successor';
    END IF;
    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS trg_miniqmt_k6_coordination_cas ON qmt_strategy.execution_dependent_buy_coordination;
CREATE TRIGGER trg_miniqmt_k6_coordination_cas
BEFORE INSERT OR UPDATE ON qmt_strategy.execution_dependent_buy_coordination
FOR EACH ROW EXECUTE FUNCTION qmt_strategy.miniqmt_k6_validate_coordination_update();

DROP TRIGGER IF EXISTS trg_miniqmt_k6_dependency_append_only ON qmt_strategy.execution_dependent_buy_dependency;
CREATE TRIGGER trg_miniqmt_k6_dependency_append_only
BEFORE UPDATE OR DELETE ON qmt_strategy.execution_dependent_buy_dependency
FOR EACH ROW EXECUTE FUNCTION qmt_strategy.miniqmt_k6_reject_immutable_mutation();

DROP TRIGGER IF EXISTS trg_miniqmt_k6_decision_append_only ON qmt_strategy.execution_dependent_buy_decision;
CREATE TRIGGER trg_miniqmt_k6_decision_append_only
BEFORE UPDATE OR DELETE ON qmt_strategy.execution_dependent_buy_decision
FOR EACH ROW EXECUTE FUNCTION qmt_strategy.miniqmt_k6_reject_immutable_mutation();

DROP TRIGGER IF EXISTS trg_miniqmt_k6_decision_closure ON qmt_strategy.execution_dependent_buy_decision;
CREATE CONSTRAINT TRIGGER trg_miniqmt_k6_decision_closure
AFTER INSERT ON qmt_strategy.execution_dependent_buy_decision
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION qmt_strategy.miniqmt_k6_validate_decision_closure();

DROP TRIGGER IF EXISTS trg_miniqmt_k6_authority_append_only ON qmt_strategy.execution_product_command_authority;
CREATE TRIGGER trg_miniqmt_k6_authority_append_only
BEFORE UPDATE OR DELETE ON qmt_strategy.execution_product_command_authority
FOR EACH ROW EXECUTE FUNCTION qmt_strategy.miniqmt_k6_reject_immutable_mutation();

DROP TRIGGER IF EXISTS trg_miniqmt_k6_authority_item_append_only ON qmt_strategy.execution_product_command_authority_item;
CREATE TRIGGER trg_miniqmt_k6_authority_item_append_only
BEFORE UPDATE OR DELETE ON qmt_strategy.execution_product_command_authority_item
FOR EACH ROW EXECUTE FUNCTION qmt_strategy.miniqmt_k6_reject_immutable_mutation();

DROP TRIGGER IF EXISTS trg_miniqmt_k6_route_receipt_append_only ON qmt_strategy.execution_product_route_cutover;
CREATE TRIGGER trg_miniqmt_k6_route_receipt_append_only
BEFORE UPDATE OR DELETE ON qmt_strategy.execution_product_route_cutover
FOR EACH ROW EXECUTE FUNCTION qmt_strategy.miniqmt_k6_reject_immutable_mutation();

DROP TRIGGER IF EXISTS trg_miniqmt_k6_route_owner_closure ON qmt_strategy.execution_product_route_owner;
CREATE TRIGGER trg_miniqmt_k6_route_owner_closure
BEFORE INSERT OR UPDATE ON qmt_strategy.execution_product_route_owner
FOR EACH ROW EXECUTE FUNCTION qmt_strategy.miniqmt_k6_validate_route_owner();

CREATE OR REPLACE FUNCTION qmt_strategy.miniqmt_k6_catalog_fingerprint()
RETURNS TEXT
LANGUAGE sql
STABLE
AS $$
WITH target_tables(relname) AS (
    VALUES
        ('execution_dependent_buy_coordination'),
        ('execution_dependent_buy_dependency'),
        ('execution_dependent_buy_decision'),
        ('execution_product_command_authority'),
        ('execution_product_command_authority_item'),
        ('execution_product_route_cutover'),
        ('execution_product_route_owner')
), catalog_items(sort_key,item) AS (
    SELECT format('column:%s:%05s',c.relname,a.attnum),
           jsonb_build_array('column',c.relname,a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,
                             coalesce(pg_get_expr(d.adbin,d.adrelid),''))
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
    LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
    WHERE n.nspname='qmt_strategy' AND c.relname IN (SELECT relname FROM target_tables)
    UNION ALL
    SELECT format('constraint:%s:%s',c.relname,k.conname),
           jsonb_build_array('constraint',c.relname,k.conname,k.contype,k.condeferrable,k.condeferred,k.convalidated,
                             replace(pg_get_constraintdef(k.oid,true),n.nspname||'.','<schema>.'))
    FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='qmt_strategy' AND c.relname IN (SELECT relname FROM target_tables)
    UNION ALL
    SELECT format('index:%s:%s',c.relname,i.relname),
           jsonb_build_array('index',c.relname,i.relname,x.indisunique,x.indisprimary,x.indisvalid,x.indisready,
                             replace(pg_get_indexdef(x.indexrelid,0,true),n.nspname||'.','<schema>.'),
                             coalesce(replace(pg_get_expr(x.indpred,x.indrelid,true),n.nspname||'.','<schema>.'),''))
    FROM pg_index x JOIN pg_class c ON c.oid=x.indrelid JOIN pg_class i ON i.oid=x.indexrelid
    JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='qmt_strategy' AND c.relname IN (SELECT relname FROM target_tables)
    UNION ALL
    SELECT format('table-comment:%s',c.relname),jsonb_build_array('table-comment',c.relname,coalesce(obj_description(c.oid),'') )
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='qmt_strategy' AND c.relname IN (SELECT relname FROM target_tables)
    UNION ALL
    SELECT format('column-comment:%s:%05s',c.relname,a.attnum),
           jsonb_build_array('column-comment',c.relname,a.attname,coalesce(col_description(c.oid,a.attnum),''))
    FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
    JOIN pg_attribute a ON a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
    WHERE n.nspname='qmt_strategy' AND c.relname IN (SELECT relname FROM target_tables)
    UNION ALL
    SELECT format('trigger:%s:%s',c.relname,t.tgname),
           jsonb_build_array('trigger',c.relname,t.tgname,t.tgenabled,
                             replace(pg_get_triggerdef(t.oid,true),n.nspname||'.','<schema>.'))
    FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='qmt_strategy' AND c.relname IN (SELECT relname FROM target_tables) AND NOT t.tgisinternal
    UNION ALL
    SELECT format('function:%s',p.proname),
           jsonb_build_array('function',p.proname,l.lanname,p.provolatile,
                             replace(p.prosrc,n.nspname||'.','<schema>.'))
    FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang
    WHERE n.nspname='qmt_strategy' AND p.proname IN (
        'miniqmt_k6_reject_immutable_mutation','miniqmt_k6_validate_route_owner',
        'miniqmt_k6_validate_coordination_update','miniqmt_k6_validate_decision_closure'
    )
), canonical_catalog AS (
    SELECT coalesce(jsonb_agg(item ORDER BY sort_key),'[]'::jsonb)::TEXT AS payload FROM catalog_items
)
SELECT encode(sha256(convert_to(payload,'UTF8')),'hex') FROM canonical_catalog
$$;

DO $$
DECLARE actual_catalog_sha256 TEXT;
DECLARE actual_function_body_sha256 TEXT;
DECLARE actual_function_metadata TEXT;
BEGIN
    SELECT qmt_strategy.miniqmt_k6_catalog_fingerprint() INTO actual_catalog_sha256;
    IF actual_catalog_sha256 <> '546a209dc2f8721ccee8b5e905117788486307147dfb4fc6bc396842f5cf84ad' THEN
        RAISE EXCEPTION 'K6-A post-commit catalog drift: expected 546a209dc2f8721ccee8b5e905117788486307147dfb4fc6bc396842f5cf84ad, got %', actual_catalog_sha256;
    END IF;
    SELECT encode(sha256(convert_to(replace(p.prosrc,n.nspname,'<schema>'),'UTF8')),'hex'),
           l.lanname||':'||p.provolatile::TEXT||':'||p.prokind::TEXT||':'||pg_get_function_identity_arguments(p.oid)
    INTO actual_function_body_sha256,actual_function_metadata
    FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang
    WHERE n.nspname='qmt_strategy' AND p.proname='miniqmt_k6_catalog_fingerprint';
    IF actual_function_body_sha256 <> 'bcb0b57b1cb425f4eb3d34b2ce5ca24c9f430986665871384482dfc056f5628a'
       OR actual_function_metadata <> 'sql:s:f:' THEN
        RAISE EXCEPTION 'K6-A catalog fingerprint function definition drift: metadata=%, body=%',
            actual_function_metadata,actual_function_body_sha256;
    END IF;
END $$;

COMMIT;

-- MiniQMT execution runtime production repository.
-- Controlled DDL only: do not run from service startup or automatic migration hooks.

CREATE SCHEMA IF NOT EXISTS qmt_strategy;

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_runtime (
    runtime_id TEXT PRIMARY KEY,
    account_group_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    mode TEXT NOT NULL,
    event_loop_state TEXT NOT NULL,
    gateway_state TEXT NOT NULL,
    oms_state TEXT NOT NULL,
    runtime_config_hash TEXT NOT NULL,
    last_event_sequence INTEGER NOT NULL DEFAULT 0,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    archived_at TIMESTAMPTZ,
    archive_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_miniqmt_runtime_id CHECK (btrim(runtime_id) <> ''),
    CONSTRAINT ck_miniqmt_runtime_account_group CHECK (btrim(account_group_id) <> ''),
    CONSTRAINT ck_miniqmt_runtime_hash CHECK (btrim(runtime_config_hash) <> ''),
    CONSTRAINT ck_miniqmt_runtime_mode CHECK (mode IN ('SIM', 'LIVE_PENDING_APPROVAL', 'LIVE')),
    CONSTRAINT ck_miniqmt_runtime_event_loop_state CHECK (
        event_loop_state IN ('CREATED', 'RECOVERING', 'READY', 'RUNNING', 'RECONCILING', 'PAUSED', 'STOPPED', 'FAILED')
    ),
    CONSTRAINT ck_miniqmt_runtime_gateway_state CHECK (gateway_state IN ('DISCONNECTED', 'CONNECTED', 'DEGRADED')),
    CONSTRAINT ck_miniqmt_runtime_oms_state CHECK (oms_state IN ('EMPTY', 'OPEN', 'RECONCILED', 'FAILED')),
    CONSTRAINT ck_miniqmt_runtime_last_sequence CHECK (last_event_sequence >= 0)
);

CREATE INDEX IF NOT EXISTS ix_miniqmt_runtime_trade_date
    ON qmt_strategy.execution_runtime(trade_date, updated_at DESC)
    WHERE archived_at IS NULL;

CREATE INDEX IF NOT EXISTS ix_miniqmt_runtime_account_date
    ON qmt_strategy.execution_runtime(account_group_id, trade_date)
    WHERE archived_at IS NULL;

COMMENT ON TABLE qmt_strategy.execution_runtime IS 'Production MiniQMT event-loop runtime state; written incrementally per runtime update.';
COMMENT ON COLUMN qmt_strategy.execution_runtime.runtime_id IS 'Stable MiniQMT execution runtime identifier.';
COMMENT ON COLUMN qmt_strategy.execution_runtime.account_group_id IS 'MiniQMT SIM account group or broker account identity.';
COMMENT ON COLUMN qmt_strategy.execution_runtime.trade_date IS 'A-share trade date owned by this runtime.';
COMMENT ON COLUMN qmt_strategy.execution_runtime.metadata IS 'Runtime diagnostics and non-authoritative operator metadata.';
COMMENT ON COLUMN qmt_strategy.execution_runtime.archived_at IS 'Soft-archive marker used by controlled prune jobs.';

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_runtime_event (
    event_id TEXT PRIMARY KEY,
    runtime_id TEXT NOT NULL REFERENCES qmt_strategy.execution_runtime(runtime_id),
    sequence INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    archived_at TIMESTAMPTZ,
    archive_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_miniqmt_event_id CHECK (btrim(event_id) <> ''),
    CONSTRAINT ck_miniqmt_event_sequence CHECK (sequence > 0),
    CONSTRAINT ck_miniqmt_event_type CHECK (
        event_type IN (
            'RUNTIME_CREATED',
            'GATEWAY_CONNECTED',
            'GATEWAY_DISCONNECTED',
            'BROKER_SYNC_STARTED',
            'BROKER_SYNCED',
            'ALGO_INSTANCE_CREATED',
            'TIMER',
            'TICK',
            'ALGO_ACTION_EMITTED',
            'CHILD_ORDER_SUBMITTED',
            'CHILD_ORDER_REJECTED',
            'CHILD_ORDER_CANCEL_REQUESTED',
            'ORDER_EVENT',
            'TRADE_EVENT',
            'ACCOUNT_EVENT',
            'RISK_KILL_SWITCH_TRIGGERED',
            'RECONCILE_STARTED',
            'RECONCILE_COMPLETED',
            'OPERATOR_COMMAND_RECEIVED',
            'OPERATOR_COMMAND_EXECUTED',
            'OPERATOR_COMMAND_REJECTED',
            'RUNTIME_STOPPED'
        )
    ),
    CONSTRAINT ck_miniqmt_event_source CHECK (source IN ('runtime', 'gateway', 'oms', 'algo', 'operator', 'recovery')),
    CONSTRAINT uq_miniqmt_event_runtime_sequence UNIQUE(runtime_id, sequence)
);

CREATE INDEX IF NOT EXISTS ix_miniqmt_event_runtime_sequence
    ON qmt_strategy.execution_runtime_event(runtime_id, sequence)
    WHERE archived_at IS NULL;

CREATE INDEX IF NOT EXISTS ix_miniqmt_event_runtime_type_time
    ON qmt_strategy.execution_runtime_event(runtime_id, event_type, event_time DESC)
    WHERE archived_at IS NULL;

COMMENT ON TABLE qmt_strategy.execution_runtime_event IS 'Append-only MiniQMT runtime event journal; each append writes one event row.';
COMMENT ON COLUMN qmt_strategy.execution_runtime_event.payload IS 'Event payload captured from runtime, gateway, OMS, or operator source.';
COMMENT ON COLUMN qmt_strategy.execution_runtime_event.archived_at IS 'Soft-archive marker used to bound active event scans.';

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_algo_instance (
    algo_instance_id TEXT PRIMARY KEY,
    runtime_id TEXT NOT NULL REFERENCES qmt_strategy.execution_runtime(runtime_id),
    parent_intent_id TEXT NOT NULL,
    strategy_slot_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    target_quantity INTEGER NOT NULL,
    remaining_quantity INTEGER NOT NULL,
    algo_code TEXT NOT NULL,
    status TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    archived_at TIMESTAMPTZ,
    archive_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_miniqmt_algo_id CHECK (btrim(algo_instance_id) <> ''),
    CONSTRAINT ck_miniqmt_algo_parent_intent CHECK (btrim(parent_intent_id) <> ''),
    CONSTRAINT ck_miniqmt_algo_slot CHECK (btrim(strategy_slot_id) <> ''),
    CONSTRAINT ck_miniqmt_algo_symbol CHECK (btrim(symbol) <> ''),
    CONSTRAINT ck_miniqmt_algo_side CHECK (side IN ('BUY', 'SELL')),
    CONSTRAINT ck_miniqmt_algo_quantity CHECK (target_quantity > 0 AND remaining_quantity >= 0 AND remaining_quantity <= target_quantity),
    CONSTRAINT ck_miniqmt_algo_code CHECK (btrim(algo_code) <> ''),
    CONSTRAINT ck_miniqmt_algo_status CHECK (status IN ('ACTIVE', 'PAUSED', 'COMPLETED', 'CANCELLED', 'FAILED'))
);

CREATE INDEX IF NOT EXISTS ix_miniqmt_algo_runtime_status
    ON qmt_strategy.execution_algo_instance(runtime_id, status, updated_at DESC)
    WHERE archived_at IS NULL;

CREATE INDEX IF NOT EXISTS ix_miniqmt_algo_runtime_slot
    ON qmt_strategy.execution_algo_instance(runtime_id, strategy_slot_id)
    WHERE archived_at IS NULL;

COMMENT ON TABLE qmt_strategy.execution_algo_instance IS 'MiniQMT vn.py-style algo instances; each upsert writes one algo row.';
COMMENT ON COLUMN qmt_strategy.execution_algo_instance.metadata IS 'Algo diagnostics, execution policy metadata, and runtime child context.';

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_child_order (
    child_order_id TEXT PRIMARY KEY,
    runtime_id TEXT NOT NULL REFERENCES qmt_strategy.execution_runtime(runtime_id),
    algo_instance_id TEXT NOT NULL REFERENCES qmt_strategy.execution_algo_instance(algo_instance_id),
    parent_intent_id TEXT NOT NULL,
    strategy_slot_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    price NUMERIC(20, 6) NOT NULL,
    price_type INTEGER NOT NULL,
    status TEXT NOT NULL,
    broker_order_id TEXT,
    submitted_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    archived_at TIMESTAMPTZ,
    archive_reason TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_miniqmt_child_id CHECK (btrim(child_order_id) <> ''),
    CONSTRAINT ck_miniqmt_child_parent_intent CHECK (btrim(parent_intent_id) <> ''),
    CONSTRAINT ck_miniqmt_child_slot CHECK (btrim(strategy_slot_id) <> ''),
    CONSTRAINT ck_miniqmt_child_symbol CHECK (btrim(symbol) <> ''),
    CONSTRAINT ck_miniqmt_child_side CHECK (side IN ('BUY', 'SELL')),
    CONSTRAINT ck_miniqmt_child_quantity CHECK (quantity > 0),
    CONSTRAINT ck_miniqmt_child_price CHECK (price >= 0),
    CONSTRAINT ck_miniqmt_child_status CHECK (status IN ('SUBMITTING', 'SUBMITTED', 'PARTIALLY_FILLED', 'FILLED', 'CANCELLED', 'REJECTED'))
);

CREATE INDEX IF NOT EXISTS ix_miniqmt_child_runtime_status
    ON qmt_strategy.execution_child_order(runtime_id, status, updated_at DESC)
    WHERE archived_at IS NULL;

CREATE INDEX IF NOT EXISTS ix_miniqmt_child_runtime_algo
    ON qmt_strategy.execution_child_order(runtime_id, algo_instance_id)
    WHERE archived_at IS NULL;

CREATE INDEX IF NOT EXISTS ix_miniqmt_child_broker_order
    ON qmt_strategy.execution_child_order(broker_order_id)
    WHERE broker_order_id IS NOT NULL AND archived_at IS NULL;

COMMENT ON TABLE qmt_strategy.execution_child_order IS 'MiniQMT child order projection; each upsert writes one child-order row.';
COMMENT ON COLUMN qmt_strategy.execution_child_order.metadata IS 'Broker, quote, and runtime diagnostics for this child order.';

-- MiniQMT multi-strategy virtual ledger schema.
-- This migration is intentionally explicit and is not executed by business
-- service startup code.

CREATE SCHEMA IF NOT EXISTS qmt_strategy;

CREATE TABLE IF NOT EXISTS qmt_strategy.virtual_account (
    strategy_id TEXT PRIMARY KEY,
    strategy_name TEXT NOT NULL,
    display_name TEXT NOT NULL,
    account_id TEXT NOT NULL,
    mode TEXT NOT NULL,
    initial_cash NUMERIC(20, 6) NOT NULL,
    cash NUMERIC(20, 6) NOT NULL,
    frozen_cash NUMERIC(20, 6) NOT NULL DEFAULT 0,
    market_value NUMERIC(20, 6) NOT NULL DEFAULT 0,
    realized_pnl NUMERIC(20, 6) NOT NULL DEFAULT 0,
    unrealized_pnl NUMERIC(20, 6) NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    risk_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_qmt_strategy_virtual_account_strategy_name CHECK (btrim(strategy_name) <> ''),
    CONSTRAINT ck_qmt_strategy_virtual_account_display_name CHECK (btrim(display_name) <> ''),
    CONSTRAINT ck_qmt_strategy_virtual_account_account_id CHECK (btrim(account_id) <> ''),
    CONSTRAINT ck_qmt_strategy_virtual_account_cash CHECK (initial_cash > 0 AND cash >= 0 AND frozen_cash >= 0),
    CONSTRAINT ck_qmt_strategy_virtual_account_mode CHECK (mode IN ('SIM', 'LIVE')),
    CONSTRAINT ck_qmt_strategy_virtual_account_status CHECK (status IN ('DRAFT', 'ENABLED', 'PAUSED', 'DISABLED', 'ARCHIVED')),
    CONSTRAINT uq_qmt_strategy_virtual_account_account_strategy UNIQUE(account_id, strategy_name)
);

COMMENT ON TABLE qmt_strategy.virtual_account IS 'Strategy-scoped virtual cash account mapped to one MiniQMT broker account.';
COMMENT ON COLUMN qmt_strategy.virtual_account.strategy_id IS 'Stable AIstock strategy account identifier.';
COMMENT ON COLUMN qmt_strategy.virtual_account.strategy_name IS 'MiniQMT strategy_name used for broker order attribution; unique within account_id.';
COMMENT ON COLUMN qmt_strategy.virtual_account.display_name IS 'Human-readable virtual strategy account name.';
COMMENT ON COLUMN qmt_strategy.virtual_account.account_id IS 'MiniQMT broker account identifier that owns the real cash and merged positions.';
COMMENT ON COLUMN qmt_strategy.virtual_account.mode IS 'Broker account mode, SIM or LIVE, used to prevent simulation/live routing mismatch.';
COMMENT ON COLUMN qmt_strategy.virtual_account.initial_cash IS 'Initial virtual cash allocation in account currency.';
COMMENT ON COLUMN qmt_strategy.virtual_account.cash IS 'Available virtual strategy cash after fills, freezes, and manual adjustments.';
COMMENT ON COLUMN qmt_strategy.virtual_account.frozen_cash IS 'Virtual cash currently frozen for outstanding buy intents.';
COMMENT ON COLUMN qmt_strategy.virtual_account.market_value IS 'Latest strategy-level market value reconstructed from lots and prices.';
COMMENT ON COLUMN qmt_strategy.virtual_account.realized_pnl IS 'Accumulated realized profit and loss in account currency.';
COMMENT ON COLUMN qmt_strategy.virtual_account.unrealized_pnl IS 'Latest unrealized profit and loss in account currency.';
COMMENT ON COLUMN qmt_strategy.virtual_account.status IS 'Lifecycle status for whether this virtual strategy can submit managed orders.';
COMMENT ON COLUMN qmt_strategy.virtual_account.risk_config IS 'JSON risk limits and account-specific execution controls.';
COMMENT ON COLUMN qmt_strategy.virtual_account.metadata IS 'Operator notes, source evidence, and non-authoritative display metadata.';
COMMENT ON COLUMN qmt_strategy.virtual_account.created_at IS 'UTC timestamp when the virtual account record was created.';
COMMENT ON COLUMN qmt_strategy.virtual_account.updated_at IS 'UTC timestamp when the virtual account record was last updated.';

CREATE TABLE IF NOT EXISTS qmt_strategy.strategy_package_binding (
    binding_id TEXT PRIMARY KEY,
    strategy_id TEXT NOT NULL REFERENCES qmt_strategy.virtual_account(strategy_id),
    package_id TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    selection_run_id TEXT,
    trade_date DATE,
    target_weight NUMERIC(12, 8),
    top_k INTEGER,
    binding_status TEXT NOT NULL,
    runtime_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_qmt_strategy_binding_package CHECK (btrim(package_id) <> ''),
    CONSTRAINT ck_qmt_strategy_binding_manifest CHECK (btrim(manifest_sha256) <> ''),
    CONSTRAINT ck_qmt_strategy_binding_status CHECK (binding_status IN ('ACTIVE', 'PAUSED', 'RETIRED')),
    CONSTRAINT ck_qmt_strategy_binding_weight CHECK (target_weight IS NULL OR target_weight >= 0),
    CONSTRAINT ck_qmt_strategy_binding_top_k CHECK (top_k IS NULL OR top_k > 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_qmt_strategy_active_binding
    ON qmt_strategy.strategy_package_binding(strategy_id)
    WHERE binding_status = 'ACTIVE';

COMMENT ON TABLE qmt_strategy.strategy_package_binding IS 'Evidence linking a virtual strategy account to the StrategyPackage manifest used for intents.';
COMMENT ON COLUMN qmt_strategy.strategy_package_binding.binding_id IS 'Stable package binding identifier.';
COMMENT ON COLUMN qmt_strategy.strategy_package_binding.strategy_id IS 'Virtual account that owns this package binding.';
COMMENT ON COLUMN qmt_strategy.strategy_package_binding.package_id IS 'StrategyPackage identifier selected for this virtual strategy.';
COMMENT ON COLUMN qmt_strategy.strategy_package_binding.manifest_sha256 IS 'Frozen StrategyPackage manifest hash used for audit and reproducibility.';
COMMENT ON COLUMN qmt_strategy.strategy_package_binding.selection_run_id IS 'Optional Selection Center run that produced target symbols or weights.';
COMMENT ON COLUMN qmt_strategy.strategy_package_binding.trade_date IS 'Trade date for the selection evidence when the binding is date-specific.';
COMMENT ON COLUMN qmt_strategy.strategy_package_binding.target_weight IS 'Optional portfolio target weight assigned to this package binding.';
COMMENT ON COLUMN qmt_strategy.strategy_package_binding.top_k IS 'Optional top-k symbol count used by the strategy binding.';
COMMENT ON COLUMN qmt_strategy.strategy_package_binding.binding_status IS 'Binding lifecycle status; only one ACTIVE binding is allowed per strategy.';
COMMENT ON COLUMN qmt_strategy.strategy_package_binding.runtime_config IS 'JSON runtime parameters captured with the binding.';
COMMENT ON COLUMN qmt_strategy.strategy_package_binding.created_at IS 'UTC timestamp when the binding was created.';
COMMENT ON COLUMN qmt_strategy.strategy_package_binding.updated_at IS 'UTC timestamp when the binding was last updated.';

CREATE TABLE IF NOT EXISTS qmt_strategy.order_batch (
    batch_id TEXT PRIMARY KEY,
    strategy_id TEXT REFERENCES qmt_strategy.virtual_account(strategy_id),
    account_id TEXT NOT NULL,
    mode TEXT NOT NULL,
    batch_status TEXT NOT NULL,
    requested_by TEXT,
    request_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    submitted_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    CONSTRAINT ck_qmt_strategy_order_batch_account CHECK (btrim(account_id) <> ''),
    CONSTRAINT ck_qmt_strategy_order_batch_mode CHECK (mode IN ('SIM', 'LIVE')),
    CONSTRAINT ck_qmt_strategy_order_batch_status CHECK (btrim(batch_status) <> '')
);

COMMENT ON TABLE qmt_strategy.order_batch IS 'Managed batch order request envelope; batch success is non-atomic and item status lives in order_intent.';
COMMENT ON COLUMN qmt_strategy.order_batch.batch_id IS 'Stable AIstock managed batch identifier.';
COMMENT ON COLUMN qmt_strategy.order_batch.strategy_id IS 'Optional strategy when all intents in the batch belong to one virtual account.';
COMMENT ON COLUMN qmt_strategy.order_batch.account_id IS 'MiniQMT broker account used for this managed batch.';
COMMENT ON COLUMN qmt_strategy.order_batch.mode IS 'Broker account mode, SIM or LIVE, captured before submission.';
COMMENT ON COLUMN qmt_strategy.order_batch.batch_status IS 'Batch lifecycle status such as CREATED, SUBMITTING, PARTIAL, SUCCEEDED, or FAILED.';
COMMENT ON COLUMN qmt_strategy.order_batch.requested_by IS 'Operator, service, or scheduler identity that requested the batch.';
COMMENT ON COLUMN qmt_strategy.order_batch.request_json IS 'Original normalized batch request payload for audit.';
COMMENT ON COLUMN qmt_strategy.order_batch.result_json IS 'Broker response summary and per-item submission result metadata.';
COMMENT ON COLUMN qmt_strategy.order_batch.metadata IS 'Non-authoritative batch diagnostics and UI metadata.';
COMMENT ON COLUMN qmt_strategy.order_batch.created_at IS 'UTC timestamp when the batch was created.';
COMMENT ON COLUMN qmt_strategy.order_batch.submitted_at IS 'UTC timestamp when the batch was submitted to MiniQMT.';
COMMENT ON COLUMN qmt_strategy.order_batch.completed_at IS 'UTC timestamp when all batch items reached a terminal submission state.';

CREATE TABLE IF NOT EXISTS qmt_strategy.order_intent (
    intent_id TEXT PRIMARY KEY,
    batch_id TEXT REFERENCES qmt_strategy.order_batch(batch_id),
    strategy_id TEXT NOT NULL REFERENCES qmt_strategy.virtual_account(strategy_id),
    strategy_name TEXT NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    order_type INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    price_type INTEGER NOT NULL,
    order_remark TEXT NOT NULL,
    account_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    package_id TEXT,
    selection_run_id TEXT,
    limit_price NUMERIC(20, 6),
    target_weight NUMERIC(12, 8),
    estimated_notional NUMERIC(20, 6),
    estimated_fee NUMERIC(20, 6),
    preflight_status TEXT NOT NULL,
    submit_status TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    submitted_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_qmt_strategy_order_intent_strategy_name CHECK (btrim(strategy_name) <> ''),
    CONSTRAINT ck_qmt_strategy_order_intent_symbol CHECK (btrim(symbol) <> ''),
    CONSTRAINT ck_qmt_strategy_order_intent_order_remark CHECK (btrim(order_remark) <> ''),
    CONSTRAINT ck_qmt_strategy_order_intent_account CHECK (btrim(account_id) <> ''),
    CONSTRAINT ck_qmt_strategy_order_intent_side CHECK (side IN ('BUY', 'SELL')),
    CONSTRAINT ck_qmt_strategy_order_intent_order_type CHECK (order_type IN (23, 24)),
    CONSTRAINT ck_qmt_strategy_order_intent_quantity CHECK (quantity > 0),
    CONSTRAINT ck_qmt_strategy_order_intent_preflight CHECK (preflight_status IN ('PENDING', 'PASSED', 'FAILED')),
    CONSTRAINT ck_qmt_strategy_order_intent_submit CHECK (submit_status IN ('CREATED', 'SUBMITTED', 'ACCEPTED', 'REJECTED', 'CANCELLED')),
    CONSTRAINT uq_qmt_strategy_order_intent_account_remark UNIQUE(account_id, order_remark)
);

CREATE INDEX IF NOT EXISTS ix_qmt_strategy_order_intent_strategy_date
    ON qmt_strategy.order_intent(strategy_id, trade_date);
CREATE INDEX IF NOT EXISTS ix_qmt_strategy_order_intent_batch
    ON qmt_strategy.order_intent(batch_id);

COMMENT ON TABLE qmt_strategy.order_intent IS 'AIstock-authoritative managed order intent before and after MiniQMT submission.';
COMMENT ON COLUMN qmt_strategy.order_intent.intent_id IS 'Stable AIstock order intent identifier.';
COMMENT ON COLUMN qmt_strategy.order_intent.batch_id IS 'Optional managed batch that created this intent.';
COMMENT ON COLUMN qmt_strategy.order_intent.strategy_id IS 'Virtual strategy account that owns this intent.';
COMMENT ON COLUMN qmt_strategy.order_intent.strategy_name IS 'MiniQMT strategy_name written to broker order fields.';
COMMENT ON COLUMN qmt_strategy.order_intent.symbol IS 'A-share symbol such as 300604.SZ.';
COMMENT ON COLUMN qmt_strategy.order_intent.side IS 'Normalized side, BUY or SELL.';
COMMENT ON COLUMN qmt_strategy.order_intent.order_type IS 'MiniQMT order type, 23 for buy or 24 for sell.';
COMMENT ON COLUMN qmt_strategy.order_intent.quantity IS 'Submitted share quantity; A-share lot rules are enforced before insert.';
COMMENT ON COLUMN qmt_strategy.order_intent.price_type IS 'MiniQMT price type used for the intent.';
COMMENT ON COLUMN qmt_strategy.order_intent.order_remark IS 'Unique local order_remark used to match broker order/trade callbacks.';
COMMENT ON COLUMN qmt_strategy.order_intent.account_id IS 'MiniQMT broker account used for the intent.';
COMMENT ON COLUMN qmt_strategy.order_intent.trade_date IS 'Exchange trade date for the intent.';
COMMENT ON COLUMN qmt_strategy.order_intent.package_id IS 'StrategyPackage identifier captured when the intent was created.';
COMMENT ON COLUMN qmt_strategy.order_intent.selection_run_id IS 'Selection Center run identifier captured when the intent was created.';
COMMENT ON COLUMN qmt_strategy.order_intent.limit_price IS 'Optional limit price in account currency.';
COMMENT ON COLUMN qmt_strategy.order_intent.target_weight IS 'Optional target portfolio weight that produced the intent.';
COMMENT ON COLUMN qmt_strategy.order_intent.estimated_notional IS 'Estimated order notional before broker submission.';
COMMENT ON COLUMN qmt_strategy.order_intent.estimated_fee IS 'Estimated fee before broker submission.';
COMMENT ON COLUMN qmt_strategy.order_intent.preflight_status IS 'Risk and broker-readiness preflight result.';
COMMENT ON COLUMN qmt_strategy.order_intent.submit_status IS 'Submission state observed by AIstock before broker fills are reconciled.';
COMMENT ON COLUMN qmt_strategy.order_intent.metadata IS 'Intent diagnostics, source package evidence, and preflight details.';
COMMENT ON COLUMN qmt_strategy.order_intent.created_at IS 'UTC timestamp when the intent was created.';
COMMENT ON COLUMN qmt_strategy.order_intent.submitted_at IS 'UTC timestamp when the intent was submitted to MiniQMT.';
COMMENT ON COLUMN qmt_strategy.order_intent.updated_at IS 'UTC timestamp when the intent state was last updated.';

CREATE TABLE IF NOT EXISTS qmt_strategy.order_ledger (
    ledger_id TEXT PRIMARY KEY,
    intent_id TEXT REFERENCES qmt_strategy.order_intent(intent_id),
    strategy_id TEXT NOT NULL REFERENCES qmt_strategy.virtual_account(strategy_id),
    strategy_name TEXT NOT NULL,
    qmt_order_id TEXT NOT NULL,
    qmt_order_sysid TEXT,
    symbol TEXT NOT NULL,
    order_type INTEGER NOT NULL,
    order_volume INTEGER NOT NULL,
    traded_volume INTEGER NOT NULL,
    order_status INTEGER,
    account_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    price_type INTEGER,
    price NUMERIC(20, 6) NOT NULL DEFAULT 0,
    traded_price NUMERIC(20, 6) NOT NULL DEFAULT 0,
    status_msg TEXT NOT NULL DEFAULT '',
    order_remark TEXT NOT NULL DEFAULT '',
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_qmt_strategy_order_ledger_strategy_name CHECK (btrim(strategy_name) <> ''),
    CONSTRAINT ck_qmt_strategy_order_ledger_qmt_order CHECK (btrim(qmt_order_id) <> ''),
    CONSTRAINT ck_qmt_strategy_order_ledger_symbol CHECK (btrim(symbol) <> ''),
    CONSTRAINT ck_qmt_strategy_order_ledger_account CHECK (btrim(account_id) <> ''),
    CONSTRAINT ck_qmt_strategy_order_ledger_type CHECK (order_type IN (23, 24)),
    CONSTRAINT ck_qmt_strategy_order_ledger_volume CHECK (order_volume >= 0 AND traded_volume >= 0),
    CONSTRAINT uq_qmt_strategy_order_ledger_account_order UNIQUE(account_id, qmt_order_id)
);

CREATE INDEX IF NOT EXISTS ix_qmt_strategy_order_ledger_strategy_date
    ON qmt_strategy.order_ledger(strategy_id, trade_date);

COMMENT ON TABLE qmt_strategy.order_ledger IS 'MiniQMT order mirror linked to local order intents and synchronized idempotently.';
COMMENT ON COLUMN qmt_strategy.order_ledger.ledger_id IS 'Stable AIstock order ledger row identifier.';
COMMENT ON COLUMN qmt_strategy.order_ledger.intent_id IS 'Local managed intent matched to this broker order.';
COMMENT ON COLUMN qmt_strategy.order_ledger.strategy_id IS 'Virtual strategy account attributed to this broker order.';
COMMENT ON COLUMN qmt_strategy.order_ledger.strategy_name IS 'Broker strategy_name observed on the MiniQMT order.';
COMMENT ON COLUMN qmt_strategy.order_ledger.qmt_order_id IS 'MiniQMT order identifier returned by the broker API.';
COMMENT ON COLUMN qmt_strategy.order_ledger.qmt_order_sysid IS 'MiniQMT exchange or system order identifier when available.';
COMMENT ON COLUMN qmt_strategy.order_ledger.symbol IS 'A-share symbol for the broker order.';
COMMENT ON COLUMN qmt_strategy.order_ledger.order_type IS 'MiniQMT order type, 23 for buy or 24 for sell.';
COMMENT ON COLUMN qmt_strategy.order_ledger.order_volume IS 'Original broker order volume in shares.';
COMMENT ON COLUMN qmt_strategy.order_ledger.traded_volume IS 'Cumulative traded volume observed from MiniQMT.';
COMMENT ON COLUMN qmt_strategy.order_ledger.order_status IS 'Raw MiniQMT order status such as 50, 54, 56, or 57.';
COMMENT ON COLUMN qmt_strategy.order_ledger.account_id IS 'MiniQMT broker account that owns this order.';
COMMENT ON COLUMN qmt_strategy.order_ledger.trade_date IS 'Exchange trade date for the broker order.';
COMMENT ON COLUMN qmt_strategy.order_ledger.price_type IS 'MiniQMT price type observed on the order.';
COMMENT ON COLUMN qmt_strategy.order_ledger.price IS 'Submitted price in account currency; zero is valid for market-style orders.';
COMMENT ON COLUMN qmt_strategy.order_ledger.traded_price IS 'Average traded price reported by MiniQMT.';
COMMENT ON COLUMN qmt_strategy.order_ledger.status_msg IS 'Broker status message captured with the latest order snapshot.';
COMMENT ON COLUMN qmt_strategy.order_ledger.order_remark IS 'Broker order_remark used for local attribution.';
COMMENT ON COLUMN qmt_strategy.order_ledger.raw_json IS 'Raw MiniQMT order payload for replay and troubleshooting.';
COMMENT ON COLUMN qmt_strategy.order_ledger.last_synced_at IS 'UTC timestamp when this order mirror was last synchronized.';

CREATE TABLE IF NOT EXISTS qmt_strategy.order_status_event (
    event_id TEXT PRIMARY KEY,
    intent_id TEXT REFERENCES qmt_strategy.order_intent(intent_id),
    qmt_order_id TEXT,
    qmt_order_sysid TEXT,
    event_type TEXT NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    account_id TEXT NOT NULL,
    qmt_order_status INTEGER,
    status_msg TEXT,
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT ck_qmt_strategy_order_status_event_account CHECK (btrim(account_id) <> ''),
    CONSTRAINT ck_qmt_strategy_order_status_event_type CHECK (btrim(event_type) <> '')
);

CREATE INDEX IF NOT EXISTS ix_qmt_strategy_order_status_event_order
    ON qmt_strategy.order_status_event(account_id, qmt_order_id, event_time);

COMMENT ON TABLE qmt_strategy.order_status_event IS 'Append-only order status event history from intent changes and MiniQMT snapshots.';
COMMENT ON COLUMN qmt_strategy.order_status_event.event_id IS 'Stable event identifier.';
COMMENT ON COLUMN qmt_strategy.order_status_event.intent_id IS 'Local order intent associated with this event when known.';
COMMENT ON COLUMN qmt_strategy.order_status_event.qmt_order_id IS 'MiniQMT order identifier associated with this event.';
COMMENT ON COLUMN qmt_strategy.order_status_event.qmt_order_sysid IS 'MiniQMT system order identifier associated with this event.';
COMMENT ON COLUMN qmt_strategy.order_status_event.event_type IS 'Normalized event type such as SUBMITTED, STATUS_SYNC, CANCELLED, or REJECTED.';
COMMENT ON COLUMN qmt_strategy.order_status_event.event_time IS 'UTC timestamp when the event was observed or recorded.';
COMMENT ON COLUMN qmt_strategy.order_status_event.account_id IS 'MiniQMT broker account for this event.';
COMMENT ON COLUMN qmt_strategy.order_status_event.qmt_order_status IS 'Raw MiniQMT order status if the event came from broker state.';
COMMENT ON COLUMN qmt_strategy.order_status_event.status_msg IS 'Broker or AIstock status message for the event.';
COMMENT ON COLUMN qmt_strategy.order_status_event.raw_json IS 'Raw event payload for audit and replay.';

CREATE TABLE IF NOT EXISTS qmt_strategy.trade_ledger (
    trade_id TEXT NOT NULL,
    intent_id TEXT NOT NULL REFERENCES qmt_strategy.order_intent(intent_id),
    strategy_id TEXT NOT NULL REFERENCES qmt_strategy.virtual_account(strategy_id),
    qmt_order_id TEXT NOT NULL,
    qmt_order_sysid TEXT,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    price NUMERIC(20, 6) NOT NULL,
    quantity INTEGER NOT NULL,
    amount NUMERIC(20, 6) NOT NULL,
    commission NUMERIC(20, 6) NOT NULL DEFAULT 0,
    trade_date DATE NOT NULL,
    account_id TEXT NOT NULL,
    trade_time TIMESTAMPTZ,
    order_remark TEXT NOT NULL DEFAULT '',
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT ck_qmt_strategy_trade_ledger_trade_id CHECK (btrim(trade_id) <> ''),
    CONSTRAINT ck_qmt_strategy_trade_ledger_symbol CHECK (btrim(symbol) <> ''),
    CONSTRAINT ck_qmt_strategy_trade_ledger_order CHECK (btrim(qmt_order_id) <> ''),
    CONSTRAINT ck_qmt_strategy_trade_ledger_side CHECK (side IN ('BUY', 'SELL')),
    CONSTRAINT ck_qmt_strategy_trade_ledger_quantity CHECK (quantity > 0),
    CONSTRAINT ck_qmt_strategy_trade_ledger_amount CHECK (amount >= 0),
    CONSTRAINT uq_qmt_strategy_trade_ledger_account_date_trade UNIQUE(account_id, trade_date, trade_id)
);

CREATE INDEX IF NOT EXISTS ix_qmt_strategy_trade_ledger_strategy_date
    ON qmt_strategy.trade_ledger(strategy_id, trade_date);
CREATE INDEX IF NOT EXISTS ix_qmt_strategy_trade_ledger_order
    ON qmt_strategy.trade_ledger(account_id, qmt_order_id);

COMMENT ON TABLE qmt_strategy.trade_ledger IS 'Idempotent MiniQMT trade/fill ledger attributed to virtual strategy accounts.';
COMMENT ON COLUMN qmt_strategy.trade_ledger.trade_id IS 'MiniQMT trade identifier unique per broker account and trade date.';
COMMENT ON COLUMN qmt_strategy.trade_ledger.intent_id IS 'Local managed order intent that owns this trade.';
COMMENT ON COLUMN qmt_strategy.trade_ledger.strategy_id IS 'Virtual strategy account attributed to this trade.';
COMMENT ON COLUMN qmt_strategy.trade_ledger.qmt_order_id IS 'MiniQMT order identifier that produced this trade.';
COMMENT ON COLUMN qmt_strategy.trade_ledger.qmt_order_sysid IS 'MiniQMT system order identifier when available.';
COMMENT ON COLUMN qmt_strategy.trade_ledger.symbol IS 'A-share symbol for the trade.';
COMMENT ON COLUMN qmt_strategy.trade_ledger.side IS 'Normalized side, BUY or SELL.';
COMMENT ON COLUMN qmt_strategy.trade_ledger.price IS 'Execution price in account currency.';
COMMENT ON COLUMN qmt_strategy.trade_ledger.quantity IS 'Executed quantity in shares.';
COMMENT ON COLUMN qmt_strategy.trade_ledger.amount IS 'Execution notional in account currency before or including broker semantics captured in raw_json.';
COMMENT ON COLUMN qmt_strategy.trade_ledger.commission IS 'Broker commission or fee amount in account currency.';
COMMENT ON COLUMN qmt_strategy.trade_ledger.trade_date IS 'Exchange trade date for the trade.';
COMMENT ON COLUMN qmt_strategy.trade_ledger.account_id IS 'MiniQMT broker account that owns this trade.';
COMMENT ON COLUMN qmt_strategy.trade_ledger.trade_time IS 'UTC timestamp for trade execution when provided by MiniQMT.';
COMMENT ON COLUMN qmt_strategy.trade_ledger.order_remark IS 'Broker order_remark copied from the matched order or trade payload.';
COMMENT ON COLUMN qmt_strategy.trade_ledger.raw_json IS 'Raw MiniQMT trade payload for replay and troubleshooting.';

CREATE TABLE IF NOT EXISTS qmt_strategy.position_lot (
    lot_id TEXT PRIMARY KEY,
    strategy_id TEXT NOT NULL REFERENCES qmt_strategy.virtual_account(strategy_id),
    account_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    open_trade_id TEXT NOT NULL,
    open_date DATE NOT NULL,
    open_time TIMESTAMPTZ,
    quantity INTEGER NOT NULL,
    available_quantity INTEGER NOT NULL,
    remaining_quantity INTEGER NOT NULL,
    avg_cost NUMERIC(20, 6) NOT NULL,
    cost_amount NUMERIC(20, 6) NOT NULL,
    realized_pnl NUMERIC(20, 6) NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_qmt_strategy_position_lot_symbol CHECK (btrim(symbol) <> ''),
    CONSTRAINT ck_qmt_strategy_position_lot_account CHECK (btrim(account_id) <> ''),
    CONSTRAINT ck_qmt_strategy_position_lot_quantities CHECK (quantity >= 0 AND available_quantity >= 0 AND remaining_quantity >= 0),
    CONSTRAINT ck_qmt_strategy_position_lot_status CHECK (status IN ('OPEN', 'PARTIALLY_CLOSED', 'CLOSED'))
);

CREATE INDEX IF NOT EXISTS ix_qmt_strategy_position_lot_strategy_symbol
    ON qmt_strategy.position_lot(strategy_id, symbol);

COMMENT ON TABLE qmt_strategy.position_lot IS 'Strategy-level position lots reconstructed from attributed MiniQMT trades.';
COMMENT ON COLUMN qmt_strategy.position_lot.lot_id IS 'Stable strategy position lot identifier.';
COMMENT ON COLUMN qmt_strategy.position_lot.strategy_id IS 'Virtual strategy account that owns this lot.';
COMMENT ON COLUMN qmt_strategy.position_lot.account_id IS 'MiniQMT broker account that holds the merged broker position.';
COMMENT ON COLUMN qmt_strategy.position_lot.symbol IS 'A-share symbol for this position lot.';
COMMENT ON COLUMN qmt_strategy.position_lot.open_trade_id IS 'Trade identifier that opened this lot.';
COMMENT ON COLUMN qmt_strategy.position_lot.open_date IS 'Exchange trade date when this lot was opened.';
COMMENT ON COLUMN qmt_strategy.position_lot.open_time IS 'UTC timestamp when the opening trade was observed.';
COMMENT ON COLUMN qmt_strategy.position_lot.quantity IS 'Original lot quantity in shares.';
COMMENT ON COLUMN qmt_strategy.position_lot.available_quantity IS 'Quantity available for sell after T+1 and pending sells.';
COMMENT ON COLUMN qmt_strategy.position_lot.remaining_quantity IS 'Current unclosed quantity in shares.';
COMMENT ON COLUMN qmt_strategy.position_lot.avg_cost IS 'Average cost per share in account currency.';
COMMENT ON COLUMN qmt_strategy.position_lot.cost_amount IS 'Remaining cost amount in account currency.';
COMMENT ON COLUMN qmt_strategy.position_lot.realized_pnl IS 'Realized profit and loss accumulated from closes against this lot.';
COMMENT ON COLUMN qmt_strategy.position_lot.status IS 'Lot lifecycle status, OPEN, PARTIALLY_CLOSED, or CLOSED.';
COMMENT ON COLUMN qmt_strategy.position_lot.metadata IS 'Lot reconstruction trace and non-authoritative display metadata.';
COMMENT ON COLUMN qmt_strategy.position_lot.updated_at IS 'UTC timestamp when this lot was last updated.';

CREATE TABLE IF NOT EXISTS qmt_strategy.cash_ledger (
    cash_id TEXT PRIMARY KEY,
    strategy_id TEXT NOT NULL REFERENCES qmt_strategy.virtual_account(strategy_id),
    account_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    entry_type TEXT NOT NULL,
    cash_delta NUMERIC(20, 6) NOT NULL,
    cash_after NUMERIC(20, 6) NOT NULL,
    frozen_delta NUMERIC(20, 6) NOT NULL DEFAULT 0,
    frozen_after NUMERIC(20, 6) NOT NULL DEFAULT 0,
    intent_id TEXT REFERENCES qmt_strategy.order_intent(intent_id),
    trade_id TEXT,
    symbol TEXT,
    reason TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_qmt_strategy_cash_ledger_account CHECK (btrim(account_id) <> ''),
    CONSTRAINT ck_qmt_strategy_cash_ledger_type CHECK (entry_type IN ('INITIAL_ALLOCATE', 'FREEZE_BUY', 'UNFREEZE_CANCEL', 'UNFREEZE_REJECT', 'BUY_FILL', 'SELL_FILL', 'FEE', 'MANUAL_ADJUST')),
    CONSTRAINT ck_qmt_strategy_cash_ledger_balances CHECK (cash_after >= 0 AND frozen_after >= 0)
);

CREATE INDEX IF NOT EXISTS ix_qmt_strategy_cash_ledger_strategy_date
    ON qmt_strategy.cash_ledger(strategy_id, trade_date, created_at);

COMMENT ON TABLE qmt_strategy.cash_ledger IS 'Append-only strategy cash and frozen-cash movement ledger.';
COMMENT ON COLUMN qmt_strategy.cash_ledger.cash_id IS 'Stable cash ledger entry identifier.';
COMMENT ON COLUMN qmt_strategy.cash_ledger.strategy_id IS 'Virtual strategy account that owns this cash movement.';
COMMENT ON COLUMN qmt_strategy.cash_ledger.account_id IS 'MiniQMT broker account used for the underlying broker action.';
COMMENT ON COLUMN qmt_strategy.cash_ledger.trade_date IS 'Exchange trade date associated with this cash movement.';
COMMENT ON COLUMN qmt_strategy.cash_ledger.entry_type IS 'Cash movement type such as INITIAL_ALLOCATE, FREEZE_BUY, BUY_FILL, or SELL_FILL.';
COMMENT ON COLUMN qmt_strategy.cash_ledger.cash_delta IS 'Change in available virtual cash in account currency.';
COMMENT ON COLUMN qmt_strategy.cash_ledger.cash_after IS 'Available virtual cash after applying this entry.';
COMMENT ON COLUMN qmt_strategy.cash_ledger.frozen_delta IS 'Change in frozen virtual cash in account currency.';
COMMENT ON COLUMN qmt_strategy.cash_ledger.frozen_after IS 'Frozen virtual cash after applying this entry.';
COMMENT ON COLUMN qmt_strategy.cash_ledger.intent_id IS 'Order intent associated with this cash movement when applicable.';
COMMENT ON COLUMN qmt_strategy.cash_ledger.trade_id IS 'Trade identifier associated with this cash movement when applicable.';
COMMENT ON COLUMN qmt_strategy.cash_ledger.symbol IS 'A-share symbol associated with this cash movement when applicable.';
COMMENT ON COLUMN qmt_strategy.cash_ledger.reason IS 'Human-readable reason for manual or reconciliation-driven entries.';
COMMENT ON COLUMN qmt_strategy.cash_ledger.metadata IS 'Cash movement source payload and reconciliation trace.';
COMMENT ON COLUMN qmt_strategy.cash_ledger.created_at IS 'UTC timestamp when this cash entry was appended.';

CREATE TABLE IF NOT EXISTS qmt_strategy.daily_snapshot (
    snapshot_id TEXT PRIMARY KEY,
    strategy_id TEXT NOT NULL REFERENCES qmt_strategy.virtual_account(strategy_id),
    account_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    cash NUMERIC(20, 6) NOT NULL,
    frozen_cash NUMERIC(20, 6) NOT NULL,
    market_value NUMERIC(20, 6) NOT NULL,
    realized_pnl NUMERIC(20, 6) NOT NULL,
    unrealized_pnl NUMERIC(20, 6) NOT NULL,
    total_equity NUMERIC(20, 6) NOT NULL,
    positions_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_qmt_strategy_daily_snapshot_account CHECK (btrim(account_id) <> ''),
    CONSTRAINT ck_qmt_strategy_daily_snapshot_balances CHECK (cash >= 0 AND frozen_cash >= 0),
    CONSTRAINT uq_qmt_strategy_daily_snapshot_strategy_date UNIQUE(strategy_id, trade_date)
);

COMMENT ON TABLE qmt_strategy.daily_snapshot IS 'Strategy-level end-of-day or intraday valuation snapshot reconstructed from lots and prices.';
COMMENT ON COLUMN qmt_strategy.daily_snapshot.snapshot_id IS 'Stable daily snapshot identifier.';
COMMENT ON COLUMN qmt_strategy.daily_snapshot.strategy_id IS 'Virtual strategy account summarized by this snapshot.';
COMMENT ON COLUMN qmt_strategy.daily_snapshot.account_id IS 'MiniQMT broker account associated with the strategy.';
COMMENT ON COLUMN qmt_strategy.daily_snapshot.trade_date IS 'Exchange trade date represented by this snapshot.';
COMMENT ON COLUMN qmt_strategy.daily_snapshot.cash IS 'Available virtual strategy cash at snapshot time.';
COMMENT ON COLUMN qmt_strategy.daily_snapshot.frozen_cash IS 'Frozen virtual strategy cash at snapshot time.';
COMMENT ON COLUMN qmt_strategy.daily_snapshot.market_value IS 'Strategy lot market value at snapshot time.';
COMMENT ON COLUMN qmt_strategy.daily_snapshot.realized_pnl IS 'Strategy realized profit and loss through the snapshot date.';
COMMENT ON COLUMN qmt_strategy.daily_snapshot.unrealized_pnl IS 'Strategy unrealized profit and loss at snapshot time.';
COMMENT ON COLUMN qmt_strategy.daily_snapshot.total_equity IS 'Cash plus frozen cash plus market value at snapshot time.';
COMMENT ON COLUMN qmt_strategy.daily_snapshot.positions_json IS 'JSON list of strategy positions and lot summaries used to build the snapshot.';
COMMENT ON COLUMN qmt_strategy.daily_snapshot.metadata IS 'Snapshot source prices, valuation mode, and reconciliation notes.';
COMMENT ON COLUMN qmt_strategy.daily_snapshot.created_at IS 'UTC timestamp when this snapshot was created.';

CREATE TABLE IF NOT EXISTS qmt_strategy.reconciliation_run (
    run_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    summary_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT ck_qmt_strategy_reconciliation_run_account CHECK (btrim(account_id) <> ''),
    CONSTRAINT ck_qmt_strategy_reconciliation_run_status CHECK (btrim(status) <> '')
);

COMMENT ON TABLE qmt_strategy.reconciliation_run IS 'One account-level reconciliation pass comparing AIstock strategy ledgers with MiniQMT authority.';
COMMENT ON COLUMN qmt_strategy.reconciliation_run.run_id IS 'Stable reconciliation run identifier.';
COMMENT ON COLUMN qmt_strategy.reconciliation_run.account_id IS 'MiniQMT broker account reconciled by this run.';
COMMENT ON COLUMN qmt_strategy.reconciliation_run.trade_date IS 'Exchange trade date reconciled by this run.';
COMMENT ON COLUMN qmt_strategy.reconciliation_run.status IS 'Run status such as STARTED, SUCCEEDED, WARNING, or FAILED.';
COMMENT ON COLUMN qmt_strategy.reconciliation_run.started_at IS 'UTC timestamp when reconciliation started.';
COMMENT ON COLUMN qmt_strategy.reconciliation_run.completed_at IS 'UTC timestamp when reconciliation completed.';
COMMENT ON COLUMN qmt_strategy.reconciliation_run.summary_json IS 'Machine-readable aggregate reconciliation metrics and totals.';

CREATE TABLE IF NOT EXISTS qmt_strategy.reconciliation_issue (
    issue_id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL REFERENCES qmt_strategy.reconciliation_run(run_id),
    strategy_id TEXT REFERENCES qmt_strategy.virtual_account(strategy_id),
    symbol TEXT,
    qmt_order_id TEXT,
    trade_id TEXT,
    issue_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    message TEXT NOT NULL,
    context JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_qmt_strategy_reconciliation_issue_type CHECK (btrim(issue_type) <> ''),
    CONSTRAINT ck_qmt_strategy_reconciliation_issue_severity CHECK (btrim(severity) <> ''),
    CONSTRAINT ck_qmt_strategy_reconciliation_issue_message CHECK (btrim(message) <> '')
);

CREATE INDEX IF NOT EXISTS ix_qmt_strategy_reconciliation_issue_run
    ON qmt_strategy.reconciliation_issue(run_id, severity);

COMMENT ON TABLE qmt_strategy.reconciliation_issue IS 'Detailed reconciliation exceptions that must be reviewed instead of silently corrected.';
COMMENT ON COLUMN qmt_strategy.reconciliation_issue.issue_id IS 'Stable reconciliation issue identifier.';
COMMENT ON COLUMN qmt_strategy.reconciliation_issue.run_id IS 'Reconciliation run that produced this issue.';
COMMENT ON COLUMN qmt_strategy.reconciliation_issue.strategy_id IS 'Virtual strategy account affected by the issue when attributable.';
COMMENT ON COLUMN qmt_strategy.reconciliation_issue.symbol IS 'A-share symbol affected by the issue when applicable.';
COMMENT ON COLUMN qmt_strategy.reconciliation_issue.qmt_order_id IS 'MiniQMT order identifier involved in the issue when applicable.';
COMMENT ON COLUMN qmt_strategy.reconciliation_issue.trade_id IS 'MiniQMT trade identifier involved in the issue when applicable.';
COMMENT ON COLUMN qmt_strategy.reconciliation_issue.issue_type IS 'Machine-readable issue category such as CASH_MISMATCH or UNATTRIBUTED_TRADE.';
COMMENT ON COLUMN qmt_strategy.reconciliation_issue.severity IS 'Issue severity used for blocking or warning decisions.';
COMMENT ON COLUMN qmt_strategy.reconciliation_issue.message IS 'Human-readable issue description.';
COMMENT ON COLUMN qmt_strategy.reconciliation_issue.context IS 'Machine-readable evidence and numeric deltas for this issue.';
COMMENT ON COLUMN qmt_strategy.reconciliation_issue.created_at IS 'UTC timestamp when this issue was recorded.';

CREATE TABLE IF NOT EXISTS qmt_strategy.unattributed_order (
    unattributed_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    qmt_order_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    reason TEXT NOT NULL,
    order_remark TEXT NOT NULL DEFAULT '',
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_qmt_strategy_unattributed_order_account CHECK (btrim(account_id) <> ''),
    CONSTRAINT ck_qmt_strategy_unattributed_order_qmt_order CHECK (btrim(qmt_order_id) <> ''),
    CONSTRAINT ck_qmt_strategy_unattributed_order_symbol CHECK (btrim(symbol) <> ''),
    CONSTRAINT ck_qmt_strategy_unattributed_order_reason CHECK (btrim(reason) <> ''),
    CONSTRAINT uq_qmt_strategy_unattributed_order_account_date_order UNIQUE(account_id, trade_date, qmt_order_id)
);

COMMENT ON TABLE qmt_strategy.unattributed_order IS 'MiniQMT order snapshots that cannot be safely assigned to a virtual strategy.';
COMMENT ON COLUMN qmt_strategy.unattributed_order.unattributed_id IS 'Stable unattributed order record identifier.';
COMMENT ON COLUMN qmt_strategy.unattributed_order.account_id IS 'MiniQMT broker account where the unattributed order was observed.';
COMMENT ON COLUMN qmt_strategy.unattributed_order.trade_date IS 'Exchange trade date when the unattributed order was observed.';
COMMENT ON COLUMN qmt_strategy.unattributed_order.qmt_order_id IS 'MiniQMT order identifier that could not be attributed.';
COMMENT ON COLUMN qmt_strategy.unattributed_order.symbol IS 'A-share symbol on the unattributed order.';
COMMENT ON COLUMN qmt_strategy.unattributed_order.reason IS 'Machine-readable reason for unattributed classification.';
COMMENT ON COLUMN qmt_strategy.unattributed_order.order_remark IS 'Broker order_remark observed on the unattributed order, possibly blank or duplicated.';
COMMENT ON COLUMN qmt_strategy.unattributed_order.raw_json IS 'Raw MiniQMT order payload for manual attribution review.';
COMMENT ON COLUMN qmt_strategy.unattributed_order.created_at IS 'UTC timestamp when this unattributed order record was created.';

CREATE TABLE IF NOT EXISTS qmt_strategy.unattributed_trade (
    unattributed_id TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    trade_id TEXT NOT NULL,
    qmt_order_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    reason TEXT NOT NULL,
    order_remark TEXT NOT NULL DEFAULT '',
    raw_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT ck_qmt_strategy_unattributed_trade_account CHECK (btrim(account_id) <> ''),
    CONSTRAINT ck_qmt_strategy_unattributed_trade_trade CHECK (btrim(trade_id) <> ''),
    CONSTRAINT ck_qmt_strategy_unattributed_trade_qmt_order CHECK (btrim(qmt_order_id) <> ''),
    CONSTRAINT ck_qmt_strategy_unattributed_trade_symbol CHECK (btrim(symbol) <> ''),
    CONSTRAINT ck_qmt_strategy_unattributed_trade_reason CHECK (btrim(reason) <> ''),
    CONSTRAINT uq_qmt_strategy_unattributed_trade_account_date_trade UNIQUE(account_id, trade_date, trade_id)
);

COMMENT ON TABLE qmt_strategy.unattributed_trade IS 'MiniQMT trade rows that cannot be safely assigned to a virtual strategy lot.';
COMMENT ON COLUMN qmt_strategy.unattributed_trade.unattributed_id IS 'Stable unattributed trade record identifier.';
COMMENT ON COLUMN qmt_strategy.unattributed_trade.account_id IS 'MiniQMT broker account where the unattributed trade was observed.';
COMMENT ON COLUMN qmt_strategy.unattributed_trade.trade_date IS 'Exchange trade date when the unattributed trade was observed.';
COMMENT ON COLUMN qmt_strategy.unattributed_trade.trade_id IS 'MiniQMT trade identifier that could not be attributed.';
COMMENT ON COLUMN qmt_strategy.unattributed_trade.qmt_order_id IS 'MiniQMT order identifier associated with the trade.';
COMMENT ON COLUMN qmt_strategy.unattributed_trade.symbol IS 'A-share symbol on the unattributed trade.';
COMMENT ON COLUMN qmt_strategy.unattributed_trade.reason IS 'Machine-readable reason for unattributed classification.';
COMMENT ON COLUMN qmt_strategy.unattributed_trade.order_remark IS 'Broker order_remark observed on the unattributed trade, possibly blank or duplicated.';
COMMENT ON COLUMN qmt_strategy.unattributed_trade.raw_json IS 'Raw MiniQMT trade payload for manual attribution review.';
COMMENT ON COLUMN qmt_strategy.unattributed_trade.created_at IS 'UTC timestamp when this unattributed trade record was created.';

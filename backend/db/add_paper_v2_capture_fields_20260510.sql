-- 2026-05-10
-- Add DW ETL capture fields to paper_v2.fills, paper_v2.positions and
-- paper_v2.daily_snapshots per T3 + A2 BLOCKING analysis (commits f7c669d
-- + d50d3c5). Without these fields, event-driven DW ETL (D5 design) cannot
-- start because watermark (created_at/updated_at) + slippage attribution
-- (intended_price) + market-context analysis (fill_market_context) are blocked.
-- DO NOT RUN: queued for next D4 batch by user.

ALTER TABLE paper_v2.fills
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS intended_price NUMERIC(18, 4),
    ADD COLUMN IF NOT EXISTS fill_market_context JSONB;

ALTER TABLE paper_v2.positions
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE paper_v2.daily_snapshots
    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

COMMENT ON COLUMN paper_v2.fills.created_at IS
    'Insert-time watermark for event-driven DW ETL; auto-populated, never updated.';
COMMENT ON COLUMN paper_v2.fills.updated_at IS
    'Last-modified watermark for event-driven DW ETL; bumped on every UPDATE.';
COMMENT ON COLUMN paper_v2.fills.intended_price IS
    'Order intent price at submit time; pairs with price for slippage attribution. Source: order_execution_state.algo_state_json.intended_price (T6.1 wiring). NULL = historical row pre-T5 or unwired path.';
COMMENT ON COLUMN paper_v2.fills.fill_market_context IS
    'Market context snapshot at fill time. Expected keys: bid, ask, best_volume, spread. Source: order_execution_state.algo_state_json.market_context (T6.1 wiring). NULL = historical row pre-T5 or unwired path.';

COMMENT ON COLUMN paper_v2.positions.created_at IS
    'Insert-time watermark for event-driven DW ETL.';
COMMENT ON COLUMN paper_v2.positions.updated_at IS
    'Last-modified watermark for event-driven DW ETL; bumped on every UPDATE/upsert.';

COMMENT ON COLUMN paper_v2.daily_snapshots.created_at IS
    'Insert-time watermark for event-driven DW ETL.';
COMMENT ON COLUMN paper_v2.daily_snapshots.updated_at IS
    'Last-modified watermark for event-driven DW ETL; bumped on every UPDATE/upsert.';

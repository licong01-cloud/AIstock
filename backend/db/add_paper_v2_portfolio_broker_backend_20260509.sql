-- Strategy Engine design 2026-05-08 §3.6 (R-Q9 D1/D3): bind paper_v2.portfolio
-- to a concrete BrokerBackend via a new immutable broker_backend column, and
-- extend data_source to allow MINIQMT_REALTIME for minqmt_sim portfolios.
--
-- Defaults: broker_backend = 'local_sim' (LEGACY default — existing rows are
-- LocalSim by definition since MiniQMTSim was not previously implemented).
-- See backend/services/paper_trading_v2/market_data.py::ALLOWED_MARKET_SOURCES
-- for the canonical broker_id <-> minute data source binding.

ALTER TABLE paper_v2.portfolio
    ADD COLUMN IF NOT EXISTS broker_backend VARCHAR(32) NOT NULL DEFAULT 'local_sim';

ALTER TABLE paper_v2.portfolio
    DROP CONSTRAINT IF EXISTS portfolio_broker_backend_check;
ALTER TABLE paper_v2.portfolio
    ADD CONSTRAINT portfolio_broker_backend_check
    CHECK (broker_backend IN ('local_sim', 'minqmt_sim'));

-- Extend data_source CHECK to admit MINIQMT_REALTIME (gated to minqmt_sim
-- broker by application-side assert_broker_market_source_match; the DB-level
-- pairing is enforced by the combined check below).
ALTER TABLE paper_v2.portfolio
    DROP CONSTRAINT IF EXISTS portfolio_data_source_check;

ALTER TABLE paper_v2.portfolio
    ADD CONSTRAINT portfolio_data_source_check
    CHECK (data_source IN ('TDX_REALTIME', 'DB_HISTORICAL', 'MINIQMT_REALTIME'));

ALTER TABLE paper_v2.portfolio
    DROP CONSTRAINT IF EXISTS portfolio_broker_market_source_check;
ALTER TABLE paper_v2.portfolio
    ADD CONSTRAINT portfolio_broker_market_source_check
    CHECK (
        (broker_backend = 'local_sim' AND data_source IN ('TDX_REALTIME', 'DB_HISTORICAL'))
        OR (broker_backend = 'minqmt_sim' AND data_source = 'MINIQMT_REALTIME')
    );

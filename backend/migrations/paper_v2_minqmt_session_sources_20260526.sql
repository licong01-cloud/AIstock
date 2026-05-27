-- Allow Paper v2 MiniQMT live sessions to persist their broker-bound source.
--
-- The MiniQMT backend uses MINIQMT_REALTIME as the authoritative live source.
-- Portfolio constraints already allow that pairing; session and session_day
-- constraints must match the runtime contract or session creation fails before
-- any broker connectivity check can run.

ALTER TABLE paper_v2.trade_session
    DROP CONSTRAINT IF EXISTS trade_session_live_data_source_check;

ALTER TABLE paper_v2.trade_session
    ADD CONSTRAINT trade_session_live_data_source_check
    CHECK (live_data_source IN ('TDX_REALTIME', 'DB_HISTORICAL', 'MINIQMT_REALTIME'));

ALTER TABLE paper_v2.session_day
    DROP CONSTRAINT IF EXISTS session_day_data_source_check;

ALTER TABLE paper_v2.session_day
    ADD CONSTRAINT session_day_data_source_check
    CHECK (data_source IN ('TDX_REALTIME', 'DB_HISTORICAL', 'MINIQMT_REALTIME'));

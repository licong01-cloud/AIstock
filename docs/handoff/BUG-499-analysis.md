# BUG-499 independent analysis

## Conclusions
- backend/services/paper_trading_v2/broker/minqmtsim.py:1118 normalize_miniqmt_quote_row() does not emit limit_up/limit_down; it normalizes L1, pre_close, OHLC, volume/amount/time/raw only.
- MINIQMT_REALTIME.broker_quote pre-trade limits are derived in backend/services/paper_trading_v2/market_data.py:611-614 by quote_tradability_evidence() from pre_close_price * (1 +/- _a_share_daily_limit_pct).
- The degenerate source is not direct use of broker raw limit_up/limit_down. The broker-quote payload can carry stale price_basis/quote_price_basis=raw_li metadata; backend/services/paper_trading_v2/market_data.py:1005-1021 previously trusted that metadata before checking source. That makes yuan prices round on the TDX raw-li tick and can collapse 20.75 to limit_up=20.0 and limit_down=20.0, reproducing L16 REALTIME_QUOTE_LIMIT_RANGE_INVALID.
- For L2 603303.SH pre_close=30.14, the valid broker-quote yuan limits are 33.15/27.13. For L16 000048.SZ pre_close=20.75, valid yuan limits are 22.83/18.68.
- _quote_source_label() now returns MiniQMT for MINIQMT_REALTIME; the old live run message was from previous code. Regression coverage still asserts MiniQMT loud failures do not mention TDX.

## Fix plan
- Force _quote_price_basis() to return yuan for MINIQMT_REALTIME sources, ignoring polluted raw_li metadata and degenerate broker limit metadata. LocalSim/TDX source behavior remains unchanged.
- Make normalize_miniqmt_quote_row() idempotent for already-normalized broker quotes by preserving bid_price_1/ask_price_1/bid_volume_1/ask_volume_1 and avoiding nested raw evidence.
- Add regressions for degenerate MiniQMT raw_li metadata, MiniQMT source-labelled loud failure, ST 5% behavior, and ProductionSimulationRunContextProvider pre-run success.

## Production gates
- production_ddl_gate=noop; production_backend_dependency_gate=noop; production_frontend_dependency_gate=noop; no service was started or restarted.

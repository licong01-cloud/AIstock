-- T10: market.regime_label DDL
-- Per data_warehouse_extension_design_20260510.md §8
-- Status: DRAFT - not applied yet, awaiting user authorization
-- Workspace: market schema (independent of qe_archive, no Codex coordination needed)
--
-- Purpose: classify each trading day into bull / bear / oscillation / high_vol / low_vol
-- based on CSI300 6-month return + 60-day volatility quadrants.
-- Multiple methods may coexist for cross-validation (PRIMARY KEY includes source_method).
--
-- Source data: market.index_daily (assumed external Tushare ingestion - already exists).

CREATE SCHEMA IF NOT EXISTS market;

CREATE TABLE IF NOT EXISTS market.regime_label (
    label_pk             BIGSERIAL,
    trade_date           DATE NOT NULL,
    regime               TEXT NOT NULL CHECK (regime IN (
        'bull', 'bear', 'oscillation', 'high_vol', 'low_vol'
    )),
    regime_confidence    NUMERIC(4,3) CHECK (regime_confidence >= 0 AND regime_confidence <= 1),
    source_method        TEXT NOT NULL CHECK (source_method IN (
        'simple_quadrant', 'hmm_viterbi', 'bbq', 'ensemble'
    )),
    source_signal_json   JSONB,
    labeled_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trade_date, source_method)
);

COMMENT ON TABLE market.regime_label IS
    'Daily market regime classification per source_method. PRIMARY KEY allows '
    'multiple methods to coexist for the same trade_date enabling ensemble / cross-check. '
    'Computed daily after market close from market.index_daily by '
    'scripts/regime_label_daily.py. Consumed by qe_archive.paper_v2_daily_snapshot.regime '
    'at ETL time.';

COMMENT ON COLUMN market.regime_label.regime IS
    'Coarse regime classification. simple_quadrant initial method uses CSI300 6m-return '
    'and 60d-volatility quartiles to assign one of 5 buckets.';

COMMENT ON COLUMN market.regime_label.regime_confidence IS
    'Confidence score 0-1. For simple_quadrant: distance from quadrant boundary. '
    'For hmm_viterbi: posterior probability. NULL allowed if method does not produce score.';

COMMENT ON COLUMN market.regime_label.source_method IS
    'simple_quadrant = CSI300 ret/vol percentile quadrants (default first method). '
    'hmm_viterbi = HMM regime states (reuse hmm_viterbi_forward_filter_fix). '
    'bbq = Bry-Boschan quarterly cycle dating. '
    'ensemble = weighted majority across methods.';

COMMENT ON COLUMN market.regime_label.source_signal_json IS
    'Raw signal values driving the classification, e.g. for simple_quadrant: '
    '{"csi300_6m_ret": 0.15, "csi300_60d_vol": 0.18, "ret_pct_5y": 0.62, "vol_pct_5y": 0.32}';

CREATE INDEX IF NOT EXISTS ix_regime_label_date
    ON market.regime_label (trade_date DESC);

CREATE INDEX IF NOT EXISTS ix_regime_label_regime
    ON market.regime_label (regime, trade_date DESC);

CREATE INDEX IF NOT EXISTS ix_regime_label_method
    ON market.regime_label (source_method, trade_date DESC);

-- Initial backfill stub (commented out, run manually after DDL applied):
-- INSERT INTO market.regime_label (trade_date, regime, regime_confidence, source_method, source_signal_json)
-- SELECT * FROM regime_label_backfill_temp;  -- populated by scripts/regime_label_daily.py --backfill

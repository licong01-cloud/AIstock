-- PriceGuard Stage 1 advisory schema.
-- Production execution is gated; this migration must not be auto-applied by Codex.

CREATE SCHEMA IF NOT EXISTS app;

ALTER TABLE selection.package_result
    ADD COLUMN IF NOT EXISTS suggested_entry_price_band JSONB,
    ADD COLUMN IF NOT EXISTS suggested_stop_loss_zone JSONB,
    ADD COLUMN IF NOT EXISTS guidance_status TEXT,
    ADD COLUMN IF NOT EXISTS price_guard_policy_sha256 TEXT;

COMMENT ON COLUMN selection.package_result.suggested_entry_price_band IS 'Advisory-only green/yellow/red suggested buy interval generated from signal_ref_price; not an order or broker limit price.';
COMMENT ON COLUMN selection.package_result.suggested_stop_loss_zone IS 'Advisory-only soft/hard stop-loss zone generated for display; enforced trading requires later QE validation.';
COMMENT ON COLUMN selection.package_result.guidance_status IS 'Guidance provenance status: rule_default, bucket_calibrated, or qe_validated; Stage 1 must remain rule_default.';
COMMENT ON COLUMN selection.package_result.price_guard_policy_sha256 IS 'Stable SHA-256 of the advisory PriceGuard/ExitGuard policy used to generate display guidance.';

ALTER TABLE app.watchlist_items
    ADD COLUMN IF NOT EXISTS lifecycle_status TEXT DEFAULT 'CANDIDATE',
    ADD COLUMN IF NOT EXISTS planned_entry_price NUMERIC,
    ADD COLUMN IF NOT EXISTS actual_entry_price NUMERIC,
    ADD COLUMN IF NOT EXISTS actual_entry_date DATE,
    ADD COLUMN IF NOT EXISTS exited_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS exit_reason TEXT,
    ADD COLUMN IF NOT EXISTS advisory_enabled BOOLEAN DEFAULT FALSE;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'watchlist_items_lifecycle_status_check'
    ) THEN
        ALTER TABLE app.watchlist_items
            ADD CONSTRAINT watchlist_items_lifecycle_status_check
            CHECK (lifecycle_status IN ('CANDIDATE', 'ENTERED', 'HOLDING', 'EXITED'));
    END IF;
END $$;

COMMENT ON COLUMN app.watchlist_items.lifecycle_status IS 'Advisory lifecycle state: CANDIDATE, ENTERED, HOLDING, or EXITED.';
COMMENT ON COLUMN app.watchlist_items.planned_entry_price IS 'Advisory planned entry price captured when a candidate is accepted into watchlist.';
COMMENT ON COLUMN app.watchlist_items.actual_entry_price IS 'Advisory actual entry cost if known; does not create or imply a broker fill.';
COMMENT ON COLUMN app.watchlist_items.actual_entry_date IS 'Advisory actual entry date used for T+1 stop-loss deferral.';
COMMENT ON COLUMN app.watchlist_items.exited_at IS 'Timestamp when advisory lifecycle entered EXITED.';
COMMENT ON COLUMN app.watchlist_items.exit_reason IS 'Final advisory exit reason code; daily changing details remain in advisory_daily_review.';
COMMENT ON COLUMN app.watchlist_items.advisory_enabled IS 'Whether the item participates in advisory daily review; no OMS, broker, or Paper ledger writes.';

CREATE TABLE IF NOT EXISTS app.advisory_daily_review (
    review_id BIGSERIAL PRIMARY KEY,
    watchlist_item_id BIGINT NOT NULL REFERENCES app.watchlist_items(id),
    code TEXT NOT NULL,
    trade_date DATE NOT NULL,
    evidence_id TEXT NULL REFERENCES selection.daily_selection_evidence(evidence_id),
    score DOUBLE PRECISION,
    rank INTEGER,
    current_price NUMERIC,
    entry_band_json JSONB,
    stop_price NUMERIC,
    take_price NUMERIC,
    action TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    policy_sha256 TEXT NOT NULL,
    guidance_status TEXT NOT NULL,
    price_basis TEXT NOT NULL,
    feature_availability_ts TIMESTAMPTZ NOT NULL,
    t1_note TEXT,
    layer TEXT NOT NULL DEFAULT 'advisory',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(watchlist_item_id, trade_date)
);

COMMENT ON TABLE app.advisory_daily_review IS 'Append-only advisory daily review facts for watchlist items; not an OMS, broker, or Paper ledger table.';
COMMENT ON COLUMN app.advisory_daily_review.review_id IS 'Surrogate primary key for the append-only daily review fact.';
COMMENT ON COLUMN app.advisory_daily_review.watchlist_item_id IS 'Reviewed watchlist item id.';
COMMENT ON COLUMN app.advisory_daily_review.code IS 'A-share symbol code copied for review-time readability.';
COMMENT ON COLUMN app.advisory_daily_review.trade_date IS 'Review trade date; unique per watchlist item for idempotent daily jobs.';
COMMENT ON COLUMN app.advisory_daily_review.evidence_id IS 'Optional daily selection evidence id that supplied score/rank.';
COMMENT ON COLUMN app.advisory_daily_review.score IS 'Selection score snapshot copied from immutable evidence for review.';
COMMENT ON COLUMN app.advisory_daily_review.rank IS 'Selection rank snapshot copied from immutable evidence for review.';
COMMENT ON COLUMN app.advisory_daily_review.current_price IS 'Point-in-time raw current price used by advisory exit evaluator.';
COMMENT ON COLUMN app.advisory_daily_review.entry_band_json IS 'Advisory entry band JSON generated from signal_ref_price and policy.';
COMMENT ON COLUMN app.advisory_daily_review.stop_price IS 'Advisory stop price recomputed for this trade date.';
COMMENT ON COLUMN app.advisory_daily_review.take_price IS 'Optional advisory take-profit price; Stage 1 default keeps take_profit off.';
COMMENT ON COLUMN app.advisory_daily_review.action IS 'Advisory action such as HOLD, STOP_LOSS, ALPHA_RANK_DROP_EXIT, or WAITING.';
COMMENT ON COLUMN app.advisory_daily_review.reason_code IS 'PriceGuard/ExitGuard reason code for this review.';
COMMENT ON COLUMN app.advisory_daily_review.policy_sha256 IS 'Policy hash used by the advisory review.';
COMMENT ON COLUMN app.advisory_daily_review.guidance_status IS 'Guidance provenance status; Stage 1 writes rule_default, not qe_validated.';
COMMENT ON COLUMN app.advisory_daily_review.price_basis IS 'Price basis used by evaluator, currently raw.';
COMMENT ON COLUMN app.advisory_daily_review.feature_availability_ts IS 'Timestamp by which all review inputs were available; used for no-future-function audit.';
COMMENT ON COLUMN app.advisory_daily_review.t1_note IS 'T+1 deferral note such as STOP_LOSS_DEFERRED_T1.';
COMMENT ON COLUMN app.advisory_daily_review.layer IS 'Fixed layer marker: advisory; distinguishes this from Paper v2 ledger or OMS.';
COMMENT ON COLUMN app.advisory_daily_review.created_at IS 'Insert timestamp for the append-only fact row.';

CREATE OR REPLACE FUNCTION app.prevent_advisory_daily_review_update()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'app.advisory_daily_review is append-only; UPDATE is forbidden';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_advisory_daily_review_no_update ON app.advisory_daily_review;
CREATE TRIGGER trg_advisory_daily_review_no_update
    BEFORE UPDATE ON app.advisory_daily_review
    FOR EACH ROW EXECUTE FUNCTION app.prevent_advisory_daily_review_update();

CREATE INDEX IF NOT EXISTS idx_watchlist_items_lifecycle ON app.watchlist_items(lifecycle_status, advisory_enabled);
CREATE INDEX IF NOT EXISTS idx_advisory_daily_review_item_date ON app.advisory_daily_review(watchlist_item_id, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_advisory_daily_review_code_date ON app.advisory_daily_review(code, trade_date DESC);

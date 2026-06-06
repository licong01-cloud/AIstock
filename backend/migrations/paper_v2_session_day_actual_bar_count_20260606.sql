-- BUG-280: persist authoritative Paper v2 session-day processed minute count.
-- Idempotent production DDL; safe to apply after the code path is merged.

ALTER TABLE paper_v2.session_day
    ADD COLUMN IF NOT EXISTS actual_bar_count INTEGER;

ALTER TABLE paper_v2.session_day
    DROP CONSTRAINT IF EXISTS session_day_actual_bar_count_non_negative;

ALTER TABLE paper_v2.session_day
    ADD CONSTRAINT session_day_actual_bar_count_non_negative
    CHECK (actual_bar_count IS NULL OR actual_bar_count >= 0);

COMMENT ON COLUMN paper_v2.session_day.actual_bar_count IS
    'Authoritative count of minute bars actually processed by the Paper v2 session day; NULL means legacy or not yet observed, not a sparse intraday snapshot count.';

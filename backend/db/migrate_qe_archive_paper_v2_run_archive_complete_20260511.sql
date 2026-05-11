-- T24 (Codex T14b/c round 2 BLOCKED) — add SCD2 replay completion marker
-- to qe_archive.paper_v2_run.
--
-- Why: handler `_handle_run_completed` previously short-circuited on
-- paper_v2_run row existence. If the first attempt committed paper_v2_run
-- but failed mid-way through child mirror writes (or a manual partial INSERT
-- happened), every subsequent retry would skip the 17 child mirrors —
-- permanent partial archive.
--
-- archive_complete is set TRUE only after every child mirror succeeds inside
-- the same transaction. Any failure rolls back the flip too, so the next
-- event delivery sees archive_complete=false and re-runs the full mirror.
--
-- Idempotent (IF NOT EXISTS). Safe to apply against a fresh T12-applied DB
-- as well as one that already has the columns from a previous attempt.
--
-- Apply:
--   psql -h ... -p 5433 -d aistock_dev -1 -v ON_ERROR_STOP=1 \
--        -f migrate_qe_archive_paper_v2_run_archive_complete_20260511.sql
--
-- Boundary: dev DB only until prod authorization. ALTER TABLE is non-blocking
-- (no rewrite) since the DEFAULT applies to existing rows lazily on read.

ALTER TABLE qe_archive.paper_v2_run
  ADD COLUMN IF NOT EXISTS archive_complete BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS archive_completed_at TIMESTAMPTZ;

COMMENT ON COLUMN qe_archive.paper_v2_run.archive_complete IS
    'T24 SCD2 replay completion marker. TRUE only after all 17 child mirror '
    'steps succeed inside the same handler transaction. Handler short-circuit '
    'checks this (not row existence) so partial-failure retries land cleanly.';

COMMENT ON COLUMN qe_archive.paper_v2_run.archive_completed_at IS
    'Timestamp when archive_complete flipped to TRUE. NULL while incomplete.';

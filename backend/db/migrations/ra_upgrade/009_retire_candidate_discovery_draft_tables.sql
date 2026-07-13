-- BUG-423 Phase 2: RA no longer owns candidate/discovery draft storage.
-- Run with psql --single-transaction -v ON_ERROR_STOP=1.

DROP INDEX IF EXISTS idx_aic_status_updated;
DROP TABLE IF EXISTS assistant_validation_discovery_reports;
DROP TABLE IF EXISTS assistant_issue_candidates;

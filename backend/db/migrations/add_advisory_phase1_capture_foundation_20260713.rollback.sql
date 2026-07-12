-- Roll back only the additive Phase 1C-1 development migration.
-- This file is for explicit DEV/release rollback workflows, never runtime use.

BEGIN;

DROP TABLE IF EXISTS app.advisory_signal_stage_candidate;
DROP TABLE IF EXISTS app.advisory_signal_stage_evidence;
DROP TABLE IF EXISTS app.advisory_signal_observation_lineage;
DROP TABLE IF EXISTS app.advisory_signal_observation_version;
DROP TABLE IF EXISTS app.advisory_signal_observation;
DROP TABLE IF EXISTS app.advisory_capture_gap;
DROP TABLE IF EXISTS app.advisory_capture_batch_evidence_membership;
DROP TABLE IF EXISTS app.advisory_capture_plan;
DROP TABLE IF EXISTS app.advisory_capture_batch;

DROP FUNCTION IF EXISTS app.verify_advisory_capture_membership();
DROP FUNCTION IF EXISTS app.verify_advisory_capture_batch_transition();
DROP FUNCTION IF EXISTS app.verify_advisory_signal_calendar_adjacency();
DROP FUNCTION IF EXISTS app.verify_advisory_observation_revision_chain();
DROP FUNCTION IF EXISTS app.reject_advisory_phase1_capture_mutation();

COMMIT;

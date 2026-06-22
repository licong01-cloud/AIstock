-- BUG-473 manual stop-bleeding cleanup for paper_v2 daemon telemetry outbox rows.
--
-- DO NOT run from Codex. This is a production-gated manual DML script for the operator.
-- DO NOT paste and execute this entire file. Run it statement-by-statement only:
--   (a) run pre-count/pre-breakdown/safety checks;
--   (b) type BEGIN manually;
--   (c) run the DELETE ... RETURNING SELECT and stop;
--   (d) verify deleted_count/time bounds;
--   (e) manually type either COMMIT or ROLLBACK.
-- Scope: qe_archive.outbox_event rows only; no qe.* research events and no paper_v2 source tables.
-- Incident predicate is intentionally narrow:
--   source_system = 'paper_v2.daemon'
--   payload->>'routing_class' = 'telemetry'
--   status = 'pending'
--   event_type LIKE 'paper.daemon.%'
--
-- Rollback policy:
--   - Before COMMIT: use ROLLBACK.
--   - After COMMIT: no row reconstruction is planned because these telemetry rows are throwaway.
--     Keep the deleted_count and min/max created_at returned by the DELETE as the audit record.

-- 1) Pre-count: operator must capture this before running the DELETE.
SELECT
    COUNT(*) AS pending_paper_daemon_telemetry_count,
    MIN(created_at) AS oldest_created_at,
    MAX(created_at) AS newest_created_at
FROM qe_archive.outbox_event
WHERE source_system = 'paper_v2.daemon'
  AND payload->>'routing_class' = 'telemetry'
  AND status = 'pending'
  AND event_type LIKE 'paper.daemon.%';

-- 2) Pre-breakdown: verify only expected telemetry event types are in scope.
SELECT
    event_type,
    COUNT(*) AS count,
    MIN(created_at) AS oldest_created_at,
    MAX(created_at) AS newest_created_at
FROM qe_archive.outbox_event
WHERE source_system = 'paper_v2.daemon'
  AND payload->>'routing_class' = 'telemetry'
  AND status = 'pending'
  AND event_type LIKE 'paper.daemon.%'
GROUP BY event_type
ORDER BY count DESC, event_type ASC;

-- 3) Safety guard: this must return zero before cleanup. If it returns rows,
-- the predicate has drifted away from paper daemon telemetry and the operator
-- should stop.
SELECT
    event_type,
    source_system,
    payload->>'routing_class' AS routing_class,
    status,
    COUNT(*) AS count
FROM qe_archive.outbox_event
WHERE status = 'pending'
  AND source_system = 'paper_v2.daemon'
  AND payload->>'routing_class' = 'telemetry'
  AND event_type NOT LIKE 'paper.daemon.%'
GROUP BY event_type, source_system, payload->>'routing_class', status;

-- 4) Forward cleanup. Manual transaction only. Type BEGIN yourself, then run the
-- DELETE statement below and STOP before manually typing COMMIT or ROLLBACK.
-- BEGIN;

WITH deleted AS (
    DELETE FROM qe_archive.outbox_event AS outbox
    WHERE outbox.source_system = 'paper_v2.daemon'
      AND outbox.payload->>'routing_class' = 'telemetry'
      AND outbox.status = 'pending'
      AND outbox.event_type LIKE 'paper.daemon.%'
    RETURNING
        outbox.event_id,
        outbox.event_type,
        outbox.source_id,
        outbox.source_sub_id,
        outbox.created_at
)
SELECT
    COUNT(*) AS deleted_count,
    MIN(created_at) AS oldest_deleted_created_at,
    MAX(created_at) AS newest_deleted_created_at
FROM deleted;

-- STOP HERE. If the deleted_count is unexpected, manually type ROLLBACK.
-- If the deleted_count is expected, manually type COMMIT.
-- ROLLBACK;
-- COMMIT;

-- 5) Post-verify: must be zero after COMMIT.
SELECT
    COUNT(*) AS remaining_pending_paper_daemon_telemetry_count
FROM qe_archive.outbox_event
WHERE source_system = 'paper_v2.daemon'
  AND payload->>'routing_class' = 'telemetry'
  AND status = 'pending'
  AND event_type LIKE 'paper.daemon.%';

-- 6) Post-health helper: archive pending should not include paper telemetry.
SELECT
    source_system,
    COALESCE(payload->>'routing_class', 'unknown') AS routing_class,
    status,
    COUNT(*) AS count,
    MIN(created_at) AS oldest_created_at
FROM qe_archive.outbox_event
WHERE status IN ('pending', 'processing')
GROUP BY source_system, COALESCE(payload->>'routing_class', 'unknown'), status
ORDER BY count DESC, source_system ASC, routing_class ASC, status ASC;

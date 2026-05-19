CREATE TABLE IF NOT EXISTS market.data_sync_targets (
    target_id UUID PRIMARY KEY,
    dataset TEXT NOT NULL,
    target_date DATE NOT NULL,
    status TEXT NOT NULL,
    source TEXT NOT NULL,
    reason TEXT NOT NULL,
    failure_category TEXT,
    next_retry_at TIMESTAMPTZ,
    final_deadline_at TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    fingerprint TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_data_sync_targets_dataset_date
    ON market.data_sync_targets(dataset, target_date);
CREATE INDEX IF NOT EXISTS idx_data_sync_targets_retry
    ON market.data_sync_targets(status, next_retry_at)
    WHERE status IN ('queued', 'retry_waiting', 'waiting_release', 'running');
CREATE INDEX IF NOT EXISTS idx_data_sync_targets_final
    ON market.data_sync_targets(status, final_deadline_at)
    WHERE status IN ('final_blocked', 'db_unavailable', 'provider_contract_error');
CREATE UNIQUE INDEX IF NOT EXISTS uq_data_sync_targets_dataset_date
    ON market.data_sync_targets(dataset, target_date);

ALTER TABLE market.data_alerts
    DROP CONSTRAINT IF EXISTS data_alerts_alert_type_check;
ALTER TABLE market.data_alerts
    ADD CONSTRAINT data_alerts_alert_type_check
    CHECK (alert_type IN (
        'stale','low_coverage','gap','zero_rows','api_failure','retry_exhausted','final_blocked'
    ));

COMMENT ON TABLE market.data_sync_targets IS 'Autonomous local-data sync target queue. One row represents one dataset/date readiness gap or retry target; dataset_date_refresh_audit remains the readiness authority.';
COMMENT ON COLUMN market.data_sync_targets.target_id IS 'Stable UUID for the autonomous sync target.';
COMMENT ON COLUMN market.data_sync_targets.dataset IS 'Logical dataset key, for example cyq_perf, stk_limit, suspend_d, or margin_detail.';
COMMENT ON COLUMN market.data_sync_targets.target_date IS 'Trading date or effective dataset date that must become ready in dataset_date_refresh_audit.';
COMMENT ON COLUMN market.data_sync_targets.status IS 'Target lifecycle state such as planned, waiting_release, queued, running, retry_waiting, success, empty_valid, final_blocked, db_unavailable, or provider_contract_error.';
COMMENT ON COLUMN market.data_sync_targets.source IS 'Producer of the target, for example freshness_check, reconciliation, scheduler, ui_manual_fill, or retry_worker.';
COMMENT ON COLUMN market.data_sync_targets.reason IS 'Machine-readable reason for this target, for example audit_missing, audit_stale, job_success_audit_missing, or success_without_data_update.';
COMMENT ON COLUMN market.data_sync_targets.failure_category IS 'Final or latest failure category used by retry policy and alert gate, for example empty_invalid, provider_unavailable, provider_contract_error, db_unavailable, or retry_exhausted.';
COMMENT ON COLUMN market.data_sync_targets.next_retry_at IS 'Next timestamp when retry worker may attempt this target; NULL means not scheduled.';
COMMENT ON COLUMN market.data_sync_targets.final_deadline_at IS 'Timestamp after which recoverable gaps become final_blocked and are eligible for Alert Gate notifications.';
COMMENT ON COLUMN market.data_sync_targets.metadata IS 'Additional JSON context such as schedule_id, job_id, row counts, release policy, retry count, or reconciliation evidence.';
COMMENT ON COLUMN market.data_sync_targets.fingerprint IS 'SHA-256 idempotency key for dataset/date to merge duplicate scheduler/UI/reconciliation triggers; source and reason are mutable evidence.';
COMMENT ON COLUMN market.data_sync_targets.created_at IS 'Timestamp when the target was first created.';
COMMENT ON COLUMN market.data_sync_targets.updated_at IS 'Timestamp when the target was last merged or changed.';

UPDATE market.data_stats_config
   SET extra_info = COALESCE(extra_info, '{}'::jsonb)
                    || jsonb_build_object(
                        'cursor_source', 'refresh_audit',
                        'bootstrap_start_date', '2018-01-01',
                        'cursor_bootstrap_policy', 'physical_audit_seed_before_bootstrap',
                        'source_api', 'tushare.cyq_perf'
                    )
 WHERE data_kind = 'cyq_perf';

CREATE TABLE IF NOT EXISTS market.data_sync_attempts (
    attempt_id UUID PRIMARY KEY,
    target_id UUID NOT NULL REFERENCES market.data_sync_targets(target_id) ON DELETE CASCADE,
    job_id UUID,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    inserted_rows BIGINT,
    error_message TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_data_sync_attempts_target
    ON market.data_sync_attempts(target_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_data_sync_attempts_job
    ON market.data_sync_attempts(job_id)
    WHERE job_id IS NOT NULL;

COMMENT ON TABLE market.data_sync_attempts IS 'Attempt history for autonomous local-data sync targets. Attempts are evidence only and never replace dataset_date_refresh_audit readiness.';
COMMENT ON COLUMN market.data_sync_attempts.attempt_id IS 'Stable UUID for one retry or ingestion attempt against a sync target.';
COMMENT ON COLUMN market.data_sync_attempts.target_id IS 'Foreign key to market.data_sync_targets.target_id.';
COMMENT ON COLUMN market.data_sync_attempts.job_id IS 'Optional market.ingestion_jobs.job_id associated with this attempt.';
COMMENT ON COLUMN market.data_sync_attempts.status IS 'Attempt status such as queued, running, success, failed, duplicate_recent, delayed, or skipped.';
COMMENT ON COLUMN market.data_sync_attempts.started_at IS 'Timestamp when the attempt started or was queued.';
COMMENT ON COLUMN market.data_sync_attempts.finished_at IS 'Timestamp when the attempt finished, if known.';
COMMENT ON COLUMN market.data_sync_attempts.inserted_rows IS 'Rows inserted or updated by this attempt; NULL when unknown.';
COMMENT ON COLUMN market.data_sync_attempts.error_message IS 'Provider, validation, database, or scheduler error text for failed attempts.';
COMMENT ON COLUMN market.data_sync_attempts.metadata IS 'Additional JSON evidence such as command, retry number, audit status, physical row count, or source payload hash.';
COMMENT ON COLUMN market.data_sync_attempts.created_at IS 'Timestamp when the attempt row was created.';

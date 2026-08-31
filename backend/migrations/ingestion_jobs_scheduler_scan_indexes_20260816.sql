DO $$
BEGIN
    IF to_regclass('market.ingestion_jobs') IS NULL THEN
        RAISE EXCEPTION 'market.ingestion_jobs is required for BUG-1106 index migration';
    END IF;
END
$$;

CREATE INDEX IF NOT EXISTS ix_ingestion_jobs_stale_running_started_at
    ON market.ingestion_jobs (started_at, job_id)
    WHERE status = 'running' AND started_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_ingestion_jobs_stale_queued_created_at
    ON market.ingestion_jobs (created_at, job_id)
    WHERE status IN ('queued', 'pending')
      AND started_at IS NULL
      AND COALESCE(summary->>'triggered_by', '') IN ('schedule', 'data_sync_target_due');

CREATE INDEX IF NOT EXISTS ix_ingestion_jobs_recent_dataset_mode_created_at
    ON market.ingestion_jobs (
        lower(summary->>'dataset'),
        lower(COALESCE(summary->>'mode', '')),
        created_at DESC,
        job_id
    )
    WHERE status IN ('queued', 'pending', 'running', 'success');

CREATE INDEX IF NOT EXISTS ix_ingestion_jobs_go_init_success_finished_at
    ON market.ingestion_jobs (finished_at DESC, job_id)
    WHERE status = 'success' AND summary->>'via' = 'go_init';

COMMENT ON INDEX market.ix_ingestion_jobs_stale_running_started_at IS
    'BUG-1106: bounded stale-running reconciliation for the 30-second TDX scheduler refresh.';
COMMENT ON INDEX market.ix_ingestion_jobs_stale_queued_created_at IS
    'BUG-1106: bounded scheduler/data-sync-target queued reconciliation.';
COMMENT ON INDEX market.ix_ingestion_jobs_recent_dataset_mode_created_at IS
    'BUG-1106: bounded recent dataset/mode submission deduplication.';
COMMENT ON INDEX market.ix_ingestion_jobs_go_init_success_finished_at IS
    'BUG-1106: bounded successful Go-ingestion audit reconciliation.';

ANALYZE market.ingestion_jobs;

SELECT indexname, indexdef
  FROM pg_indexes
 WHERE schemaname = 'market'
   AND tablename = 'ingestion_jobs'
   AND indexname IN (
       'ix_ingestion_jobs_stale_running_started_at',
       'ix_ingestion_jobs_stale_queued_created_at',
       'ix_ingestion_jobs_recent_dataset_mode_created_at',
       'ix_ingestion_jobs_go_init_success_finished_at'
   )
 ORDER BY indexname;

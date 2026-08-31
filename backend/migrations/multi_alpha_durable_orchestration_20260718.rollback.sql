-- Guarded rollback for multi_alpha_durable_orchestration_20260718.sql.
--
-- Normal code rollback should KEEP the additive schema and stop creating new
-- durable tasks. Run this destructive schema rollback only when no durable
-- task/run/child/attempt/event data has ever been activated.

BEGIN;

DO $rollback_precondition$
BEGIN
    IF to_regclass('strategy_pkg.multi_alpha_combine_task') IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'multi_alpha_durable_rollback_schema_missing';
    END IF;
    IF EXISTS (SELECT 1 FROM strategy_pkg.multi_alpha_combine_task)
       OR EXISTS (SELECT 1 FROM strategy_pkg.multi_alpha_combine_backtest_child)
       OR EXISTS (SELECT 1 FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt)
       OR EXISTS (SELECT 1 FROM strategy_pkg.multi_alpha_combine_backtest_event) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'multi_alpha_durable_rollback_data_present',
            DETAIL = 'Keep the additive schema; destructive rollback is forbidden after durable data exists.';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM strategy_pkg.multi_alpha_combine_backtest_run
        WHERE task_id IS NOT NULL
           OR request_hash IS NOT NULL
           OR retry_of_run_id IS NOT NULL
           OR owner_id IS NOT NULL
           OR lease_expires_at IS NOT NULL
           OR heartbeat_at IS NOT NULL
           OR pause_requested_at IS NOT NULL
           OR cancel_requested_at IS NOT NULL
           OR error_code IS NOT NULL
           OR error_json IS NOT NULL
           OR status NOT IN ('running', 'succeeded', 'failed')
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'multi_alpha_durable_rollback_run_data_present',
            DETAIL = 'A run uses durable fields or a durable-only status; preserve the schema.';
    END IF;
END
$rollback_precondition$;

DROP TABLE strategy_pkg.multi_alpha_combine_backtest_event;

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child
    DROP CONSTRAINT IF EXISTS fk_macb_child_selected_attempt;
DROP TABLE strategy_pkg.multi_alpha_combine_backtest_child_attempt;
DROP TABLE strategy_pkg.multi_alpha_combine_backtest_child;

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    DROP CONSTRAINT IF EXISTS fk_macb_run_task,
    DROP CONSTRAINT IF EXISTS fk_macb_run_retry_of,
    DROP CONSTRAINT IF EXISTS ck_macb_run_progress_json,
    DROP CONSTRAINT IF EXISTS ck_macb_run_parallelism_json,
    DROP CONSTRAINT IF EXISTS ck_macb_run_error_json,
    DROP CONSTRAINT IF EXISTS ck_macb_run_row_version,
    DROP CONSTRAINT IF EXISTS ck_macb_run_fencing_token,
    DROP CONSTRAINT IF EXISTS ck_macb_run_request_hash,
    DROP CONSTRAINT IF EXISTS ck_macb_run_status;

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    ADD CONSTRAINT ck_macb_run_status CHECK (status IN ('running', 'succeeded', 'failed'));

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    DROP COLUMN IF EXISTS task_id,
    DROP COLUMN IF EXISTS request_hash,
    DROP COLUMN IF EXISTS retry_of_run_id,
    DROP COLUMN IF EXISTS phase,
    DROP COLUMN IF EXISTS progress_json,
    DROP COLUMN IF EXISTS row_version,
    DROP COLUMN IF EXISTS owner_id,
    DROP COLUMN IF EXISTS fencing_token,
    DROP COLUMN IF EXISTS lease_expires_at,
    DROP COLUMN IF EXISTS heartbeat_at,
    DROP COLUMN IF EXISTS pause_requested_at,
    DROP COLUMN IF EXISTS pause_requested_by,
    DROP COLUMN IF EXISTS cancel_requested_at,
    DROP COLUMN IF EXISTS cancel_requested_by,
    DROP COLUMN IF EXISTS node_parallelism_json,
    DROP COLUMN IF EXISTS started_at,
    DROP COLUMN IF EXISTS finished_at,
    DROP COLUMN IF EXISTS error_code,
    DROP COLUMN IF EXISTS error_json;

-- updated_at is intentionally retained because production deployments may
-- already have the compatible column independently of this migration, and the
-- existing repository detects and uses it.

DROP TABLE strategy_pkg.multi_alpha_combine_task;

COMMIT;

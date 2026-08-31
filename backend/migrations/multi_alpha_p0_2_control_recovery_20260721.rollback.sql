-- Guarded destructive schema rollback for multi_alpha_p0_2_control_recovery_20260721.sql.
--
-- Normal application rollback keeps this additive schema and simply stops
-- creating P0-2 commands or recovery successors. Run this script only when
-- P0-2 has never recorded a command, cancellation delivery, recovery lineage,
-- or P0-2-only terminal status. It intentionally does not create a DB export.

BEGIN;

DO $p0_2_rollback_precondition$
BEGIN
    IF to_regclass('strategy_pkg.multi_alpha_combine_backtest_command') IS NULL
       OR to_regclass('strategy_pkg.multi_alpha_combine_backtest_cancel_delivery') IS NULL
       OR to_regclass('strategy_pkg.multi_alpha_combine_backtest_command_delivery') IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'multi_alpha_p0_2_rollback_schema_missing',
            DETAIL = 'The P0-2 schema is not complete; repair that state explicitly rather than dropping unknown objects.';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM (VALUES
            ('multi_alpha_combine_backtest_run', 'recovery_kind'),
            ('multi_alpha_combine_backtest_run', 'recovery_scope_json'),
            ('multi_alpha_combine_backtest_run', 'recovery_scope_hash'),
            ('multi_alpha_combine_backtest_run', 'execution_identity_json'),
            ('multi_alpha_combine_backtest_run', 'execution_identity_hash'),
            ('multi_alpha_combine_backtest_run', 'execution_identity_evidence_json'),
            ('multi_alpha_combine_backtest_child', 'source_child_id'),
            ('multi_alpha_combine_backtest_child', 'execution_disposition'),
            ('multi_alpha_combine_backtest_child', 'source_lineage_json'),
            ('multi_alpha_combine_backtest_child', 'source_lineage_hash'),
            ('multi_alpha_combine_backtest_child_attempt', 'run_id'),
            ('multi_alpha_combine_backtest_child_attempt', 'source_attempt_id'),
            ('multi_alpha_combine_backtest_child_attempt', 'execution_kind'),
            ('multi_alpha_combine_backtest_child_attempt', 'result_manifest_hash')
        ) AS expected(table_name, column_name)
        WHERE NOT EXISTS (
            SELECT 1
            FROM information_schema.columns AS actual
            WHERE actual.table_schema = 'strategy_pkg'
              AND actual.table_name = expected.table_name
              AND actual.column_name = expected.column_name
        )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'multi_alpha_p0_2_rollback_column_missing',
            DETAIL = 'The P0-2 schema is partial; repair it explicitly instead of executing destructive rollback.';
    END IF;

    IF EXISTS (SELECT 1 FROM strategy_pkg.multi_alpha_combine_backtest_command)
       OR EXISTS (SELECT 1 FROM strategy_pkg.multi_alpha_combine_backtest_cancel_delivery)
       OR EXISTS (SELECT 1 FROM strategy_pkg.multi_alpha_combine_backtest_command_delivery) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'multi_alpha_p0_2_rollback_control_data_present',
            DETAIL = 'Keep additive P0-2 schema; durable control/delivery audit data must not be deleted.';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM strategy_pkg.multi_alpha_combine_backtest_run
        WHERE recovery_kind IS NOT NULL
           OR recovery_scope_json <> '{}'::jsonb
           OR recovery_scope_hash IS NOT NULL
           OR execution_identity_json IS NOT NULL
           OR execution_identity_hash IS NOT NULL
           OR execution_identity_evidence_json IS NOT NULL
           OR status = 'partial_recovered'
    ) OR EXISTS (
        SELECT 1
        FROM strategy_pkg.multi_alpha_combine_backtest_child
        WHERE source_child_id IS NOT NULL
           OR execution_disposition <> 'execute'
           OR source_lineage_json IS NOT NULL
           OR source_lineage_hash IS NOT NULL
           OR source_kind = 'recovery_reference'
           OR status = 'not_recovered'
    ) OR EXISTS (
        SELECT 1
        FROM strategy_pkg.multi_alpha_combine_backtest_child_attempt
        WHERE source_attempt_id IS NOT NULL
           OR execution_kind <> 'remote_execution'
           OR result_manifest_hash IS NOT NULL
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'multi_alpha_p0_2_rollback_recovery_data_present',
            DETAIL = 'Keep additive P0-2 schema; recovery provenance or result-reference data exists.';
    END IF;
END
$p0_2_rollback_precondition$;

DROP TABLE strategy_pkg.multi_alpha_combine_backtest_command_delivery;
DROP TABLE strategy_pkg.multi_alpha_combine_backtest_cancel_delivery;
DROP TABLE strategy_pkg.multi_alpha_combine_backtest_command;

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child
    DROP CONSTRAINT IF EXISTS fk_macb_child_selected_attempt;
ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child_attempt
    DROP CONSTRAINT IF EXISTS fk_macb_attempt_run_child,
    DROP CONSTRAINT IF EXISTS fk_macb_attempt_source_attempt,
    DROP CONSTRAINT IF EXISTS uq_macb_attempt_run_child_attempt,
    DROP CONSTRAINT IF EXISTS ck_macb_attempt_execution_remote_fields,
    DROP CONSTRAINT IF EXISTS ck_macb_attempt_result_manifest_hash,
    DROP CONSTRAINT IF EXISTS ck_macb_attempt_execution_kind,
    DROP CONSTRAINT IF EXISTS ck_macb_attempt_lineage;

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child
    DROP CONSTRAINT IF EXISTS fk_macb_child_source_child,
    DROP CONSTRAINT IF EXISTS uq_macb_child_run_child,
    DROP CONSTRAINT IF EXISTS ck_macb_child_not_recovered_disposition,
    DROP CONSTRAINT IF EXISTS ck_macb_child_source_lineage,
    DROP CONSTRAINT IF EXISTS ck_macb_child_execution_disposition,
    DROP CONSTRAINT IF EXISTS ck_macb_child_source,
    DROP CONSTRAINT IF EXISTS ck_macb_child_status;

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    DROP CONSTRAINT IF EXISTS ck_macb_run_partial_recovered_kind,
    DROP CONSTRAINT IF EXISTS ck_macb_run_execution_identity_evidence_alignment,
    DROP CONSTRAINT IF EXISTS ck_macb_run_execution_identity_evidence,
    DROP CONSTRAINT IF EXISTS ck_macb_run_execution_identity,
    DROP CONSTRAINT IF EXISTS ck_macb_run_recovery_tuple,
    DROP CONSTRAINT IF EXISTS ck_macb_run_recovery_scope_hash,
    DROP CONSTRAINT IF EXISTS ck_macb_run_recovery_scope_json,
    DROP CONSTRAINT IF EXISTS ck_macb_run_recovery_kind,
    DROP CONSTRAINT IF EXISTS ck_macb_run_status;

DROP INDEX IF EXISTS strategy_pkg.idx_macb_run_recovery_source;

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    ADD CONSTRAINT ck_macb_run_status CHECK (
        status IN ('queued', 'preparing', 'running', 'pause_requested', 'paused',
                   'cancel_requested', 'cancelling', 'succeeded', 'partial_failed',
                   'failed', 'cancelled')
    );

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child
    ADD CONSTRAINT ck_macb_child_status CHECK (
        status IN ('pending', 'materializing', 'queued', 'running', 'reconciling',
                   'cancel_requested', 'cancelling', 'succeeded', 'not_computable',
                   'failed', 'cancelled')
    ),
    ADD CONSTRAINT ck_macb_child_source CHECK (
        source_kind IN ('runtime', 'legacy_result_backfill')
    );

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child_attempt
    ADD CONSTRAINT ck_macb_attempt_lineage CHECK (
        (retry_mode = 'initial' AND attempt_no = 1 AND retry_of_attempt_id IS NULL)
        OR (retry_mode <> 'initial' AND attempt_no > 1 AND retry_of_attempt_id IS NOT NULL)
    );

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child
    ADD CONSTRAINT fk_macb_child_selected_attempt
    FOREIGN KEY (selected_attempt_id)
    REFERENCES strategy_pkg.multi_alpha_combine_backtest_child_attempt(attempt_id)
    ON DELETE SET NULL;

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child_attempt
    DROP COLUMN IF EXISTS run_id,
    DROP COLUMN IF EXISTS source_attempt_id,
    DROP COLUMN IF EXISTS execution_kind,
    DROP COLUMN IF EXISTS result_manifest_hash;

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_child
    DROP COLUMN IF EXISTS source_child_id,
    DROP COLUMN IF EXISTS execution_disposition,
    DROP COLUMN IF EXISTS source_lineage_json,
    DROP COLUMN IF EXISTS source_lineage_hash;

ALTER TABLE strategy_pkg.multi_alpha_combine_backtest_run
    DROP COLUMN IF EXISTS recovery_kind,
    DROP COLUMN IF EXISTS recovery_scope_json,
    DROP COLUMN IF EXISTS recovery_scope_hash,
    DROP COLUMN IF EXISTS execution_identity_json,
    DROP COLUMN IF EXISTS execution_identity_hash,
    DROP COLUMN IF EXISTS execution_identity_evidence_json;

COMMIT;

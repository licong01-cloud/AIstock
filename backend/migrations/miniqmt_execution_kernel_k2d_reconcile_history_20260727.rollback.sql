-- MiniQMT K2-D append-only reconciliation history guarded rollback.
BEGIN;

DO $$
DECLARE
    durable_fact_count BIGINT;
BEGIN
    IF to_regclass('qmt_strategy.execution_broker_reconciliation_attempt') IS NOT NULL THEN
        SELECT count(*) INTO durable_fact_count
        FROM qmt_strategy.execution_broker_reconciliation_attempt;
        IF durable_fact_count <> 0 THEN
            RAISE EXCEPTION 'K2-D destructive rollback refused: durable_fact_count=%', durable_fact_count;
        END IF;
    END IF;
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='qmt_strategy' AND table_name='execution_algo_command_outbox'
          AND column_name='callback_watermark_before_call'
    ) THEN
        SELECT count(*) INTO durable_fact_count
        FROM qmt_strategy.execution_algo_command_outbox
        WHERE callback_watermark_before_call IS NOT NULL;
        IF durable_fact_count <> 0 THEN
            RAISE EXCEPTION 'K2-D destructive rollback refused: callback_watermark_fact_count=%', durable_fact_count;
        END IF;
    END IF;
END $$;

DROP TABLE IF EXISTS qmt_strategy.execution_broker_reconciliation_attempt;
DROP FUNCTION IF EXISTS qmt_strategy.miniqmt_k2d_catalog_fingerprint();
ALTER TABLE qmt_strategy.execution_algo_command_outbox
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k2d_outbox_callback_watermark;
ALTER TABLE qmt_strategy.execution_algo_command_outbox
    DROP CONSTRAINT IF EXISTS uq_miniqmt_k2d_outbox_command_runtime;
ALTER TABLE qmt_strategy.execution_algo_command_outbox
    DROP COLUMN IF EXISTS callback_watermark_before_call;

COMMIT;

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
END $$;

DROP TABLE IF EXISTS qmt_strategy.execution_broker_reconciliation_attempt;

COMMIT;

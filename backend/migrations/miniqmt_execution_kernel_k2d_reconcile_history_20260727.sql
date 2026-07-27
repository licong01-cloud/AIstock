-- MiniQMT K2-D append-only broker reconciliation history.
BEGIN;

CREATE TABLE IF NOT EXISTS qmt_strategy.execution_broker_reconciliation_attempt (
    receipt_sha256 TEXT PRIMARY KEY,
    command_id TEXT NOT NULL,
    runtime_id TEXT NOT NULL,
    reconcile_attempt INTEGER NOT NULL,
    callback_watermark TEXT NOT NULL,
    outcome TEXT NOT NULL,
    observed_at_utc TIMESTAMPTZ NOT NULL,
    receipt_json JSONB NOT NULL,
    CONSTRAINT uq_miniqmt_k2d_reconcile_command_attempt UNIQUE (command_id,reconcile_attempt),
    CONSTRAINT ck_miniqmt_k2d_reconcile_receipt_sha CHECK (receipt_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_miniqmt_k2d_reconcile_attempt CHECK (reconcile_attempt BETWEEN 1 AND 10),
    CONSTRAINT ck_miniqmt_k2d_reconcile_outcome CHECK (
        outcome IN ('NOT_FOUND','UNIQUE_ACCEPTED','UNIQUE_REJECTED','CONFLICT')
    ),
    CONSTRAINT fk_miniqmt_k2d_reconcile_command FOREIGN KEY (command_id)
        REFERENCES qmt_strategy.execution_algo_command_outbox(command_id),
    CONSTRAINT fk_miniqmt_k2d_reconcile_runtime FOREIGN KEY (runtime_id)
        REFERENCES qmt_strategy.execution_runtime(runtime_id)
);

CREATE INDEX IF NOT EXISTS ix_miniqmt_k2d_reconcile_runtime_observed
ON qmt_strategy.execution_broker_reconciliation_attempt(runtime_id,observed_at_utc,receipt_sha256);

COMMENT ON TABLE qmt_strategy.execution_broker_reconciliation_attempt IS
'K2-D immutable broker/OMS snapshot reconciliation history; one exact receipt per command attempt.';

DO $$
DECLARE
    column_count INTEGER;
    constraint_count INTEGER;
    actual_catalog_sha256 TEXT;
BEGIN
    SELECT count(*) INTO column_count
    FROM information_schema.columns
    WHERE table_schema='qmt_strategy' AND table_name='execution_broker_reconciliation_attempt'
      AND column_name IN (
        'receipt_sha256','command_id','runtime_id','reconcile_attempt','callback_watermark',
        'outcome','observed_at_utc','receipt_json'
      );
    IF column_count <> 8 THEN
        RAISE EXCEPTION 'K2-D post-commit readback drift: column_count=%', column_count;
    END IF;
    SELECT count(*) INTO constraint_count
    FROM pg_constraint
    WHERE conrelid='qmt_strategy.execution_broker_reconciliation_attempt'::regclass
      AND conname IN (
        'execution_broker_reconciliation_attempt_pkey','uq_miniqmt_k2d_reconcile_command_attempt',
        'ck_miniqmt_k2d_reconcile_receipt_sha','ck_miniqmt_k2d_reconcile_attempt',
        'ck_miniqmt_k2d_reconcile_outcome','fk_miniqmt_k2d_reconcile_command',
        'fk_miniqmt_k2d_reconcile_runtime'
      );
    IF constraint_count <> 7 THEN
        RAISE EXCEPTION 'K2-D post-commit readback drift: constraint_count=%', constraint_count;
    END IF;
    SELECT qmt_strategy.miniqmt_k2_catalog_fingerprint() INTO actual_catalog_sha256;
    IF actual_catalog_sha256 <> '8f1534aae7b0362de2061fafec2f16e056f52fd66251c0d67698ef33a2915d9d' THEN
        RAISE EXCEPTION 'K2-D post-commit catalog drift: expected 8f1534aae7b0362de2061fafec2f16e056f52fd66251c0d67698ef33a2915d9d, got %', actual_catalog_sha256;
    END IF;
END $$;

COMMIT;

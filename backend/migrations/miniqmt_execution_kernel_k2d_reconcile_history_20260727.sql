-- MiniQMT K2-D append-only broker reconciliation history.
BEGIN;

ALTER TABLE qmt_strategy.execution_algo_command_outbox
    ADD COLUMN IF NOT EXISTS callback_watermark_before_call TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname='uq_miniqmt_k2d_outbox_command_runtime'
          AND conrelid='qmt_strategy.execution_algo_command_outbox'::regclass
    ) THEN
        ALTER TABLE qmt_strategy.execution_algo_command_outbox
            ADD CONSTRAINT uq_miniqmt_k2d_outbox_command_runtime UNIQUE (command_id,runtime_id);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname='ck_miniqmt_k2d_outbox_callback_watermark'
          AND conrelid='qmt_strategy.execution_algo_command_outbox'::regclass
    ) THEN
        ALTER TABLE qmt_strategy.execution_algo_command_outbox
            ADD CONSTRAINT ck_miniqmt_k2d_outbox_callback_watermark CHECK (
                (status IN ('DISPATCHING','OUTCOME_UNKNOWN','RECONCILING','ACKED','ACKED_REJECTED')
                    AND callback_watermark_before_call IS NOT NULL)
                OR (status IN ('PENDING','CLAIMED')
                    AND callback_watermark_before_call IS NULL)
                OR status IN ('FAILED_RETRYABLE','FAILED_TERMINAL')
            ) NOT VALID;
    END IF;
END $$;

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
    CONSTRAINT fk_miniqmt_k2d_reconcile_command_runtime FOREIGN KEY (command_id,runtime_id)
        REFERENCES qmt_strategy.execution_algo_command_outbox(command_id,runtime_id)
);

ALTER TABLE qmt_strategy.execution_algo_command_outbox
    VALIDATE CONSTRAINT ck_miniqmt_k2d_outbox_callback_watermark;

CREATE INDEX IF NOT EXISTS ix_miniqmt_k2d_reconcile_runtime_observed
ON qmt_strategy.execution_broker_reconciliation_attempt(runtime_id,observed_at_utc,receipt_sha256);

COMMENT ON TABLE qmt_strategy.execution_broker_reconciliation_attempt IS
'K2-D immutable broker/OMS snapshot reconciliation history; one exact receipt per command attempt.';
COMMENT ON COLUMN qmt_strategy.execution_algo_command_outbox.callback_watermark_before_call IS
'Durable runtime event watermark committed before entering the broker-call boundary.';
COMMENT ON COLUMN qmt_strategy.execution_broker_reconciliation_attempt.receipt_sha256 IS 'Immutable receipt identity and payload hash.';
COMMENT ON COLUMN qmt_strategy.execution_broker_reconciliation_attempt.command_id IS 'Owning durable broker command identity.';
COMMENT ON COLUMN qmt_strategy.execution_broker_reconciliation_attempt.runtime_id IS 'Owning runtime identity, closed jointly with command_id.';
COMMENT ON COLUMN qmt_strategy.execution_broker_reconciliation_attempt.reconcile_attempt IS 'Strict one-based bounded reconciliation attempt.';
COMMENT ON COLUMN qmt_strategy.execution_broker_reconciliation_attempt.callback_watermark IS 'Durable runtime event watermark after the broker snapshot read.';
COMMENT ON COLUMN qmt_strategy.execution_broker_reconciliation_attempt.outcome IS 'Exact broker outcome classification.';
COMMENT ON COLUMN qmt_strategy.execution_broker_reconciliation_attempt.observed_at_utc IS 'Canonical observation time for this immutable receipt.';
COMMENT ON COLUMN qmt_strategy.execution_broker_reconciliation_attempt.receipt_json IS 'Strict BrokerOutcomeReconciliationReceiptV1 carrier.';

CREATE OR REPLACE FUNCTION qmt_strategy.miniqmt_k2d_catalog_fingerprint()
RETURNS TEXT
LANGUAGE sql
STABLE
AS $$
WITH catalog_items(sort_key,item) AS (
    SELECT
        format('column:%s:%05s', table_class.relname, attribute.attnum),
        jsonb_build_array(
            'column',table_class.relname,attribute.attname,
            format_type(attribute.atttypid,attribute.atttypmod),attribute.attnotnull,
            coalesce(pg_get_expr(attribute_default.adbin,attribute_default.adrelid),'')
        )
    FROM pg_class AS table_class
    JOIN pg_namespace AS table_schema ON table_schema.oid=table_class.relnamespace
    JOIN pg_attribute AS attribute
      ON attribute.attrelid=table_class.oid AND attribute.attnum>0 AND NOT attribute.attisdropped
    LEFT JOIN pg_attrdef AS attribute_default
      ON attribute_default.adrelid=table_class.oid AND attribute_default.adnum=attribute.attnum
    WHERE table_schema.nspname='qmt_strategy'
      AND (
        table_class.relname='execution_broker_reconciliation_attempt'
        OR (table_class.relname='execution_algo_command_outbox'
            AND attribute.attname='callback_watermark_before_call')
      )

    UNION ALL

    SELECT
        format('constraint:%s:%s',table_class.relname,constraint_record.conname),
        jsonb_build_array(
            'constraint',table_class.relname,constraint_record.conname,
            constraint_record.contype,constraint_record.convalidated,
            replace(pg_get_constraintdef(constraint_record.oid,true),table_schema.nspname||'.','<schema>.')
        )
    FROM pg_constraint AS constraint_record
    JOIN pg_class AS table_class ON table_class.oid=constraint_record.conrelid
    JOIN pg_namespace AS table_schema ON table_schema.oid=table_class.relnamespace
    WHERE table_schema.nspname='qmt_strategy'
      AND (
        table_class.relname='execution_broker_reconciliation_attempt'
        OR constraint_record.conname IN (
            'uq_miniqmt_k2d_outbox_command_runtime','ck_miniqmt_k2d_outbox_callback_watermark'
        )
      )

    UNION ALL

    SELECT
        format('index:%s:%s',table_class.relname,index_class.relname),
        jsonb_build_array(
            'index',table_class.relname,index_class.relname,
            index_record.indisunique,index_record.indisprimary,index_record.indisvalid,index_record.indisready,
            replace(pg_get_indexdef(index_record.indexrelid,0,true),table_schema.nspname||'.','<schema>.'),
            coalesce(replace(pg_get_expr(index_record.indpred,index_record.indrelid,true),
                             table_schema.nspname||'.','<schema>.'),'')
        )
    FROM pg_index AS index_record
    JOIN pg_class AS table_class ON table_class.oid=index_record.indrelid
    JOIN pg_class AS index_class ON index_class.oid=index_record.indexrelid
    JOIN pg_namespace AS table_schema ON table_schema.oid=table_class.relnamespace
    WHERE table_schema.nspname='qmt_strategy'
      AND (
        table_class.relname='execution_broker_reconciliation_attempt'
        OR index_class.relname='uq_miniqmt_k2d_outbox_command_runtime'
      )
), canonical_catalog AS (
    SELECT coalesce(jsonb_agg(item ORDER BY sort_key),'[]'::jsonb)::TEXT AS payload
    FROM catalog_items
)
SELECT encode(sha256(convert_to(payload,'UTF8')),'hex') FROM canonical_catalog
$$;

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
        'ck_miniqmt_k2d_reconcile_outcome','fk_miniqmt_k2d_reconcile_command_runtime'
      );
    IF constraint_count <> 6 THEN
        RAISE EXCEPTION 'K2-D post-commit readback drift: constraint_count=%', constraint_count;
    END IF;
    SELECT qmt_strategy.miniqmt_k2_catalog_fingerprint() INTO actual_catalog_sha256;
    IF actual_catalog_sha256 <> '2ae93a1e637f4232ea01fc80f7f7a4680679956cc428b12c56adb01f16efea6a' THEN
        RAISE EXCEPTION 'K2-D post-commit base catalog drift: expected 2ae93a1e637f4232ea01fc80f7f7a4680679956cc428b12c56adb01f16efea6a, got %', actual_catalog_sha256;
    END IF;
    SELECT qmt_strategy.miniqmt_k2d_catalog_fingerprint() INTO actual_catalog_sha256;
    IF actual_catalog_sha256 <> 'f9034e9e9680a12e335c5bdc0ac06e10dda73d34c8a65128df08c26b0f93725d' THEN
        RAISE EXCEPTION 'K2-D post-commit catalog drift: expected f9034e9e9680a12e335c5bdc0ac06e10dda73d34c8a65128df08c26b0f93725d, got %', actual_catalog_sha256;
    END IF;
END $$;

COMMIT;

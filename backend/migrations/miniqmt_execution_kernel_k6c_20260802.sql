-- MiniQMT K6-C0 successor: strict product V3 and dependent-BUY V2 schema.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
SELECT pg_advisory_xact_lock(hashtext('qmt_strategy.miniqmt_execution_kernel_k6c_20260802'));

DO $$
DECLARE table_name TEXT;
DECLARE durable_rows BIGINT;
BEGIN
    IF to_regprocedure('qmt_strategy.miniqmt_k6c_catalog_fingerprint()') IS NULL THEN
        FOREACH table_name IN ARRAY ARRAY[
            'execution_dependent_buy_coordination','execution_dependent_buy_dependency',
            'execution_dependent_buy_decision','execution_product_command_authority',
            'execution_product_command_authority_item','execution_product_route_cutover',
            'execution_product_route_owner'
        ] LOOP
            EXECUTE format('SELECT count(*) FROM qmt_strategy.%I',table_name) INTO durable_rows;
            IF durable_rows<>0 THEN
                RAISE EXCEPTION 'K6-C0 successor requires zero K6-A durable rows: table=%, rows=%',table_name,durable_rows;
            END IF;
        END LOOP;
    END IF;
END $$;

ALTER TABLE qmt_strategy.execution_dependent_buy_coordination
    ADD COLUMN IF NOT EXISTS release_command_id TEXT,
    ADD COLUMN IF NOT EXISTS release_transition_id TEXT,
    ADD COLUMN IF NOT EXISTS release_command_authority_item_sha256 TEXT;

ALTER TABLE qmt_strategy.execution_dependent_buy_dependency
    ADD COLUMN IF NOT EXISTS latest_order_fact_id TEXT,
    ADD COLUMN IF NOT EXISTS latest_order_fact_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS ordered_settled_proceeds_refs JSONB;

ALTER TABLE qmt_strategy.execution_dependent_buy_decision
    ADD COLUMN IF NOT EXISTS ledger_virtual_account_updated_at_utc TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS ledger_latest_cash_sequence BIGINT,
    ADD COLUMN IF NOT EXISTS ledger_revision_sha256 TEXT,
    ALTER COLUMN ledger_row_version DROP NOT NULL;

ALTER TABLE qmt_strategy.execution_product_command_authority
    ADD COLUMN IF NOT EXISTS defer_count INTEGER;

ALTER TABLE qmt_strategy.execution_product_command_authority_item
    ADD COLUMN IF NOT EXISTS command_json JSONB,
    ADD COLUMN IF NOT EXISTS evaluation_evidence_json JSONB,
    ADD COLUMN IF NOT EXISTS evaluation_evidence_sha256 TEXT,
    ADD COLUMN IF NOT EXISTS coordination_id TEXT;

ALTER TABLE qmt_strategy.execution_dependent_buy_coordination
    ALTER COLUMN release_command_id SET NOT NULL,
    ALTER COLUMN release_transition_id SET NOT NULL,
    ALTER COLUMN release_command_authority_item_sha256 SET NOT NULL;
ALTER TABLE qmt_strategy.execution_dependent_buy_dependency
    ALTER COLUMN ordered_settled_proceeds_refs SET NOT NULL;
ALTER TABLE qmt_strategy.execution_product_command_authority
    ALTER COLUMN defer_count SET NOT NULL;
ALTER TABLE qmt_strategy.execution_product_command_authority_item
    ALTER COLUMN command_json SET NOT NULL,
    ALTER COLUMN evaluation_evidence_json SET NOT NULL,
    ALTER COLUMN evaluation_evidence_sha256 SET NOT NULL;

ALTER TABLE qmt_strategy.execution_child_order DROP CONSTRAINT IF EXISTS ck_miniqmt_k2_child_mapping_contract;
ALTER TABLE qmt_strategy.execution_child_order ADD CONSTRAINT ck_miniqmt_k2_child_mapping_contract CHECK (
    (kernel_contract_version = 'LEGACY_V1' AND mapping_id IS NULL AND command_id IS NULL AND local_vt_orderid IS NULL AND deterministic_client_order_ref IS NULL AND order_remark IS NULL AND mapping_status IS NULL AND mapping_version IS NULL AND mapping_payload_sha256 IS NULL AND mapping_receipt_sha256 IS NULL AND broker_identity_source_event_id IS NULL AND last_order_event_id IS NULL AND last_trade_event_id IS NULL AND created_transition_id IS NULL AND updated_by_event_id IS NULL AND mapping_created_at_utc IS NULL AND mapping_updated_at_utc IS NULL AND mapping_json IS NULL)
    OR (kernel_contract_version = 'KERNEL_V2' AND mapping_id IS NOT NULL AND command_id IS NOT NULL AND local_vt_orderid IS NOT NULL AND deterministic_client_order_ref IS NOT NULL AND order_remark = deterministic_client_order_ref AND mapping_status IN ('DEFERRED_DEPENDENT_BUY','RESERVED','DISPATCHING','BROKER_ACCEPTED','BROKER_REJECTED','OUTCOME_UNKNOWN','TERMINAL') AND mapping_version > 0 AND mapping_payload_sha256 ~ '^[0-9a-f]{64}$' AND mapping_receipt_sha256 ~ '^[0-9a-f]{64}$' AND created_transition_id IS NOT NULL AND mapping_created_at_utc IS NOT NULL AND mapping_updated_at_utc >= mapping_created_at_utc AND mapping_json IS NOT NULL)
) NOT VALID;
ALTER TABLE qmt_strategy.execution_child_order VALIDATE CONSTRAINT ck_miniqmt_k2_child_mapping_contract;

ALTER TABLE qmt_strategy.execution_child_order
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k2_child_mapping_initial,
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k6_product_mapping_state;
ALTER TABLE qmt_strategy.execution_child_order
    ADD CONSTRAINT ck_miniqmt_k2_child_mapping_initial CHECK (
        kernel_contract_version <> 'KERNEL_V2' OR mapping_status <> 'RESERVED' OR (
            (mapping_version=1 AND broker_order_id IS NULL AND broker_identity_source_event_id IS NULL
                AND last_order_event_id IS NULL AND last_trade_event_id IS NULL AND updated_by_event_id IS NULL
                AND mapping_created_at_utc=mapping_updated_at_utc)
            OR
            (mapping_version=2 AND broker_order_id IS NULL AND broker_identity_source_event_id IS NULL
                AND last_order_event_id IS NULL AND last_trade_event_id IS NULL AND updated_by_event_id IS NOT NULL
                AND mapping_updated_at_utc>mapping_created_at_utc
                AND mapping_json->>'schema_version'='miniqmt_product_command_child_mapping_v1')
        )
    ) NOT VALID,
    ADD CONSTRAINT ck_miniqmt_k6_product_mapping_state CHECK (
        CASE WHEN mapping_json->>'schema_version'='miniqmt_product_command_child_mapping_v1' THEN (
            jsonb_typeof(mapping_json)='object'
            AND mapping_json=jsonb_build_object(
                'schema_version','miniqmt_product_command_child_mapping_v1',
                'mapping_id',mapping_id,
                'authority_item_sha256',mapping_json->'authority_item_sha256',
                'coordination_id',mapping_json->'coordination_id',
                'command_id',command_id,
                'runtime_id',runtime_id,
                'algo_instance_id',algo_instance_id,
                'parent_intent_id',parent_intent_id,
                'strategy_slot_id',strategy_slot_id,
                'local_vt_orderid',local_vt_orderid,
                'child_order_id',child_order_id,
                'deterministic_client_order_ref',deterministic_client_order_ref,
                'order_remark',order_remark,
                'symbol',symbol,
                'side',side,
                'requested_price_decimal',mapping_json->'requested_price_decimal',
                'requested_quantity',quantity,
                'broker_order_id',broker_order_id,
                'broker_identity_source_event_id',broker_identity_source_event_id,
                'mapping_status',mapping_status,
                'mapping_version',mapping_version,
                'payload_sha256',mapping_payload_sha256,
                'last_order_event_id',last_order_event_id,
                'last_trade_event_id',last_trade_event_id,
                'created_transition_id',created_transition_id,
                'updated_by_event_id',updated_by_event_id,
                'created_at_utc',mapping_json->'created_at_utc',
                'updated_at_utc',mapping_json->'updated_at_utc',
                'mapping_receipt_sha256',mapping_receipt_sha256
            )
            AND mapping_json->>'schema_version'='miniqmt_product_command_child_mapping_v1'
            AND mapping_json->>'mapping_id'=mapping_id
            AND mapping_json->>'command_id'=command_id
            AND mapping_json->>'runtime_id'=runtime_id
            AND mapping_json->>'algo_instance_id'=algo_instance_id
            AND mapping_json->>'parent_intent_id'=parent_intent_id
            AND mapping_json->>'strategy_slot_id'=strategy_slot_id
            AND mapping_json->>'local_vt_orderid'=local_vt_orderid
            AND mapping_json->>'child_order_id'=child_order_id
            AND mapping_json->>'deterministic_client_order_ref'=deterministic_client_order_ref
            AND mapping_json->>'order_remark'=order_remark
            AND mapping_json->>'symbol'=symbol
            AND mapping_json->>'side'=side
            AND (mapping_json->>'requested_price_decimal')::NUMERIC=price
            AND (mapping_json->>'requested_quantity')::BIGINT=quantity
            AND (mapping_json->>'broker_order_id') IS NOT DISTINCT FROM broker_order_id
            AND (mapping_json->>'broker_identity_source_event_id') IS NOT DISTINCT FROM broker_identity_source_event_id
            AND mapping_json->>'mapping_status'=mapping_status
            AND mapping_json->>'mapping_version'=mapping_version::TEXT
            AND mapping_json->>'payload_sha256'=mapping_payload_sha256
            AND mapping_json->>'mapping_receipt_sha256'=mapping_receipt_sha256
            AND jsonb_typeof(mapping_json->'authority_item_sha256')='string'
            AND mapping_json->>'authority_item_sha256' ~ '^[0-9a-f]{64}$'
            AND jsonb_typeof(mapping_json->'coordination_id')='string'
            AND mapping_json->>'coordination_id' ~ '^[0-9a-f]{64}$'
            AND jsonb_typeof(mapping_json->'requested_price_decimal')='string'
            AND jsonb_typeof(mapping_json->'created_at_utc')='string'
            AND jsonb_typeof(mapping_json->'updated_at_utc')='string'
            AND (mapping_json->>'last_order_event_id') IS NOT DISTINCT FROM last_order_event_id
            AND (mapping_json->>'last_trade_event_id') IS NOT DISTINCT FROM last_trade_event_id
            AND mapping_json->>'created_transition_id'=created_transition_id
            AND (mapping_json->>'updated_by_event_id') IS NOT DISTINCT FROM updated_by_event_id
            AND (mapping_json->>'created_at_utc')::TIMESTAMPTZ=mapping_created_at_utc
            AND (mapping_json->>'updated_at_utc')::TIMESTAMPTZ=mapping_updated_at_utc
            AND (
                (mapping_status='DEFERRED_DEPENDENT_BUY' AND mapping_version=1
                    AND broker_order_id IS NULL AND broker_identity_source_event_id IS NULL
                    AND last_order_event_id IS NULL AND last_trade_event_id IS NULL AND updated_by_event_id IS NULL
                    AND mapping_created_at_utc=mapping_updated_at_utc)
                OR
                (mapping_status IN ('RESERVED','TERMINAL') AND mapping_version=2
                    AND broker_order_id IS NULL AND broker_identity_source_event_id IS NULL
                    AND last_order_event_id IS NULL AND last_trade_event_id IS NULL AND updated_by_event_id IS NOT NULL
                    AND mapping_updated_at_utc>mapping_created_at_utc)
            )
        ) ELSE mapping_status<>'DEFERRED_DEPENDENT_BUY' END
    ) NOT VALID;
ALTER TABLE qmt_strategy.execution_child_order
    VALIDATE CONSTRAINT ck_miniqmt_k2_child_mapping_initial;
ALTER TABLE qmt_strategy.execution_child_order
    VALIDATE CONSTRAINT ck_miniqmt_k6_product_mapping_state;

ALTER TABLE qmt_strategy.execution_dependent_buy_coordination
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k6_coordination_v2_release;
ALTER TABLE qmt_strategy.execution_dependent_buy_coordination
    ADD CONSTRAINT ck_miniqmt_k6_coordination_v2_release CHECK (
        btrim(release_command_id)<>'' AND btrim(release_transition_id)<>''
        AND release_command_authority_item_sha256 ~ '^[0-9a-f]{64}$'
        AND ((status='RELEASED_TO_K2_OUTBOX' AND released_command_id=release_command_id
              AND released_outbox_id=release_command_id)
             OR (status<>'RELEASED_TO_K2_OUTBOX' AND released_command_id IS NULL AND released_outbox_id IS NULL))
    );

ALTER TABLE qmt_strategy.execution_dependent_buy_dependency
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k6_dependency_v2_json;
ALTER TABLE qmt_strategy.execution_dependent_buy_dependency
    ADD CONSTRAINT ck_miniqmt_k6_dependency_v2_json CHECK (
        jsonb_typeof(ordered_settled_proceeds_refs)='array'
        AND jsonb_array_length(ordered_settled_proceeds_refs)<=4096
        AND ((latest_order_fact_id IS NULL AND latest_order_fact_sha256 IS NULL)
             OR (btrim(latest_order_fact_id)<>'' AND latest_order_fact_sha256 ~ '^[0-9a-f]{64}$'))
    );

ALTER TABLE qmt_strategy.execution_dependent_buy_decision
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k6_decision_v2_ledger,
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k6_decision_hash;
ALTER TABLE qmt_strategy.execution_dependent_buy_decision
    ADD CONSTRAINT ck_miniqmt_k6_decision_hash CHECK (
        decision_id ~ '^[0-9a-f]{64}$' AND trigger_ref_sha256 ~ '^[0-9a-f]{64}$'
        AND ledger_observation_sha256 ~ '^[0-9a-f]{64}$' AND decision_sha256 ~ '^[0-9a-f]{64}$'
        AND lease_epoch>0 AND (ledger_row_version IS NULL OR ledger_row_version>0)
        AND jsonb_typeof(ordered_dependency_sha256s)='array'
        AND jsonb_typeof(trigger_ref_json)='object'
        AND jsonb_typeof(ledger_observation_json)='object'
    ),
    ADD CONSTRAINT ck_miniqmt_k6_decision_v2_ledger CHECK (
        ledger_row_version IS NULL
        AND ledger_virtual_account_updated_at_utc IS NOT NULL
        AND ledger_latest_cash_sequence>=0
        AND ledger_revision_sha256 ~ '^[0-9a-f]{64}$'
    );

ALTER TABLE qmt_strategy.execution_product_command_authority
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k6_authority_counts,
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k6_authority_disposition;
ALTER TABLE qmt_strategy.execution_product_command_authority
    ADD CONSTRAINT ck_miniqmt_k6_authority_counts CHECK (
        materialize_count>=0 AND reject_count>=0 AND defer_count>=0 AND total_count BETWEEN 0 AND 256
        AND materialize_count+reject_count+defer_count=total_count
    ),
    ADD CONSTRAINT ck_miniqmt_k6_authority_disposition CHECK (aggregate_disposition IN (
        'ZERO_COMMAND','ALL_REJECTED','ALL_DEFERRED','MATERIALIZE_ALL_ACCEPTED_COMMANDS','MIXED_PER_COMMAND'
    ));

ALTER TABLE qmt_strategy.execution_product_command_authority_item
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k6_authority_item_disposition,
    DROP CONSTRAINT IF EXISTS ck_miniqmt_k6_authority_item_presence;
ALTER TABLE qmt_strategy.execution_product_command_authority_item
    ADD CONSTRAINT ck_miniqmt_k6_authority_item_disposition CHECK (
        disposition IN ('MATERIALIZE','REJECT_SYNCHRONOUS','DEFER_DEPENDENT_BUY')
    ),
    ADD CONSTRAINT ck_miniqmt_k6_authority_item_presence CHECK (
        jsonb_typeof(command_json)='object' AND jsonb_typeof(evaluation_evidence_json)='object'
        AND evaluation_evidence_sha256 ~ '^[0-9a-f]{64}$'
        AND (
            (disposition='MATERIALIZE' AND mapping_id IS NOT NULL AND outbox_id=command_id
                AND child_order_id IS NOT NULL AND reject_reason_code IS NULL
                AND reject_context_sha256 IS NULL AND coordination_id IS NULL)
            OR (disposition='REJECT_SYNCHRONOUS' AND mapping_id IS NOT NULL AND outbox_id=command_id
                AND child_order_id IS NOT NULL AND reject_reason_code IS NOT NULL
                AND reject_context_sha256 ~ '^[0-9a-f]{64}$' AND coordination_id IS NULL)
            OR (disposition='DEFER_DEPENDENT_BUY' AND mapping_id IS NOT NULL AND outbox_id IS NULL
                AND child_order_id IS NOT NULL AND reject_reason_code IS NULL
                AND reject_context_sha256 IS NULL AND coordination_id IS NOT NULL)
        )
    );

ALTER TABLE qmt_strategy.execution_dependent_buy_coordination
    DROP CONSTRAINT IF EXISTS fk_miniqmt_k6_coordination_release_item;
ALTER TABLE qmt_strategy.execution_product_command_authority_item
    DROP CONSTRAINT IF EXISTS fk_miniqmt_k6_authority_item_coordination,
    DROP CONSTRAINT IF EXISTS uq_miniqmt_k6_authority_item_sha256;
ALTER TABLE qmt_strategy.execution_product_command_authority_item
    ADD CONSTRAINT uq_miniqmt_k6_authority_item_sha256 UNIQUE (item_sha256),
    ADD CONSTRAINT fk_miniqmt_k6_authority_item_coordination FOREIGN KEY (coordination_id)
        REFERENCES qmt_strategy.execution_dependent_buy_coordination(coordination_id)
        DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE qmt_strategy.execution_dependent_buy_coordination
    DROP CONSTRAINT IF EXISTS fk_miniqmt_k6_coordination_release_item;
ALTER TABLE qmt_strategy.execution_dependent_buy_coordination
    ADD CONSTRAINT fk_miniqmt_k6_coordination_release_item
        FOREIGN KEY (release_command_authority_item_sha256)
        REFERENCES qmt_strategy.execution_product_command_authority_item(item_sha256)
        DEFERRABLE INITIALLY DEFERRED;

CREATE OR REPLACE FUNCTION qmt_strategy.miniqmt_k6_validate_coordination_update()
RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        IF NEW.status<>'DEFERRED_WAITING_SELL_PROCEEDS'
           OR NEW.decision_sequence<>0 OR NEW.last_decision_sha256 IS NOT NULL
           OR NEW.released_command_id IS NOT NULL OR NEW.released_outbox_id IS NOT NULL
           OR NEW.row_version<>1 OR NEW.lease_epoch<>0
           OR NEW.lease_worker_id IS NOT NULL OR NEW.lease_process_incarnation_id IS NOT NULL
           OR NEW.lease_expires_at_utc IS NOT NULL THEN
            RAISE EXCEPTION 'K6 dependent-BUY V2 first write requires exact waiting initial state';
        END IF;
        RETURN NEW;
    END IF;
    IF (NEW.coordination_id,NEW.runtime_id,NEW.binding_id,NEW.trade_date,NEW.strategy_id,
        NEW.buy_algo_instance_id,NEW.buy_parent_intent_id,NEW.required_cash,
        NEW.release_command_id,NEW.release_transition_id,NEW.release_command_authority_item_sha256,
        NEW.release_command_payload_sha256,NEW.created_at_utc)
       IS DISTINCT FROM
       (OLD.coordination_id,OLD.runtime_id,OLD.binding_id,OLD.trade_date,OLD.strategy_id,
        OLD.buy_algo_instance_id,OLD.buy_parent_intent_id,OLD.required_cash,
        OLD.release_command_id,OLD.release_transition_id,OLD.release_command_authority_item_sha256,
        OLD.release_command_payload_sha256,OLD.created_at_utc) THEN
        RAISE EXCEPTION 'K6 dependent-BUY V2 coordination immutable owner/payload drift';
    END IF;
    IF OLD.status IN ('RELEASED_TO_K2_OUTBOX','BLOCKED_SELL_PROCEEDS_UNAVAILABLE','EOD_RESIDUAL') THEN
        RAISE EXCEPTION 'K6 dependent-BUY terminal coordination cannot be updated';
    END IF;
    IF NEW.row_version<>OLD.row_version+1 OR NEW.decision_sequence<OLD.decision_sequence
       OR NEW.lease_epoch NOT IN (OLD.lease_epoch,OLD.lease_epoch+1)
       OR (NEW.lease_epoch=OLD.lease_epoch AND
           (NEW.lease_worker_id,NEW.lease_process_incarnation_id)
           IS DISTINCT FROM (OLD.lease_worker_id,OLD.lease_process_incarnation_id)) THEN
        RAISE EXCEPTION 'K6 dependent-BUY V2 coordination update is not a monotonic CAS successor';
    END IF;
    RETURN NEW;
END $$;

COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.release_command_id IS 'K6-C0 V2 exact original BUY command identity; immutable and reused by release.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.release_transition_id IS 'K6-C0 V2 original transition identity; release never creates a parallel transition.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_coordination.release_command_authority_item_sha256 IS 'K6-C0 V3 exact DEFER authority-item SHA-256; deferrable closed to item row.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_dependency.latest_order_fact_id IS 'K6-C0 V2 latest authoritative SELL order fact identity; nullable with paired hash.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_dependency.latest_order_fact_sha256 IS 'K6-C0 V2 latest SELL order fact SHA-256; nullable with paired identity.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_dependency.ordered_settled_proceeds_refs IS 'K6-C0 strict miniqmt_dependent_buy_settled_proceeds_ref_v2 array, canonical and max 4096.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.ledger_row_version IS 'Deprecated K6-A field; K6-C0 V2 rows require NULL and use ledger_revision_sha256.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.ledger_virtual_account_updated_at_utc IS 'K6-C0 locked virtual-account updated_at UTC included in ledger revision.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.ledger_latest_cash_sequence IS 'K6-C0 latest committed strategy cash-ledger sequence, zero when absent.';
COMMENT ON COLUMN qmt_strategy.execution_dependent_buy_decision.ledger_revision_sha256 IS 'K6-C0 V2 hash of locked account cash/update, cash sequence and settled proceeds refs.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority.defer_count IS 'K6-C0 number of DEFER_DEPENDENT_BUY V3 items.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority_item.command_json IS 'Strict miniqmt_broker_command_v2 full command JSON; max 16KiB enforced by writer/readback.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority_item.evaluation_evidence_json IS 'Strict miniqmt_product_command_evaluation_evidence_v3 JSON; max 64KiB enforced by writer/readback.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority_item.evaluation_evidence_sha256 IS 'Canonical V3 evaluation-evidence SHA-256.';
COMMENT ON COLUMN qmt_strategy.execution_product_command_authority_item.coordination_id IS 'Only DEFER_DEPENDENT_BUY: exact V2 coordination identity; otherwise NULL.';
COMMENT ON COLUMN qmt_strategy.execution_child_order.mapping_status IS 'K2 mapping lifecycle including K6-C0 DEFERRED_DEPENDENT_BUY -> RESERVED exact successor.';

CREATE OR REPLACE FUNCTION qmt_strategy.miniqmt_k6c_catalog_fingerprint()
RETURNS TEXT LANGUAGE sql STABLE AS $$
WITH target_columns(relname,attname) AS (
    VALUES
        ('execution_child_order','mapping_status'),
        ('execution_dependent_buy_coordination','release_command_id'),
        ('execution_dependent_buy_coordination','release_transition_id'),
        ('execution_dependent_buy_coordination','release_command_authority_item_sha256'),
        ('execution_dependent_buy_dependency','latest_order_fact_id'),
        ('execution_dependent_buy_dependency','latest_order_fact_sha256'),
        ('execution_dependent_buy_dependency','ordered_settled_proceeds_refs'),
        ('execution_dependent_buy_decision','ledger_row_version'),
        ('execution_dependent_buy_decision','ledger_virtual_account_updated_at_utc'),
        ('execution_dependent_buy_decision','ledger_latest_cash_sequence'),
        ('execution_dependent_buy_decision','ledger_revision_sha256'),
        ('execution_product_command_authority','defer_count'),
        ('execution_product_command_authority_item','command_json'),
        ('execution_product_command_authority_item','evaluation_evidence_json'),
        ('execution_product_command_authority_item','evaluation_evidence_sha256'),
        ('execution_product_command_authority_item','coordination_id')
), target_constraints(relname,conname) AS (
    VALUES
        ('execution_child_order','ck_miniqmt_k2_child_mapping_contract'),
        ('execution_child_order','ck_miniqmt_k2_child_mapping_initial'),
        ('execution_child_order','ck_miniqmt_k6_product_mapping_state'),
        ('execution_dependent_buy_coordination','ck_miniqmt_k6_coordination_v2_release'),
        ('execution_dependent_buy_coordination','fk_miniqmt_k6_coordination_release_item'),
        ('execution_dependent_buy_dependency','ck_miniqmt_k6_dependency_v2_json'),
        ('execution_dependent_buy_decision','ck_miniqmt_k6_decision_v2_ledger'),
        ('execution_product_command_authority','ck_miniqmt_k6_authority_counts'),
        ('execution_product_command_authority','ck_miniqmt_k6_authority_disposition'),
        ('execution_product_command_authority_item','ck_miniqmt_k6_authority_item_disposition'),
        ('execution_product_command_authority_item','ck_miniqmt_k6_authority_item_presence'),
        ('execution_product_command_authority_item','uq_miniqmt_k6_authority_item_sha256'),
        ('execution_product_command_authority_item','fk_miniqmt_k6_authority_item_coordination')
), catalog_items(sort_key,item) AS (
    SELECT format('column:%s:%s',c.relname,a.attname),
           jsonb_build_array('column',c.relname,a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,
                             coalesce(pg_get_expr(d.adbin,d.adrelid),''),coalesce(col_description(c.oid,a.attnum),''))
    FROM target_columns tc JOIN pg_class c ON c.relname=tc.relname
    JOIN pg_namespace n ON n.oid=c.relnamespace AND n.nspname='qmt_strategy'
    JOIN pg_attribute a ON a.attrelid=c.oid AND a.attname=tc.attname AND a.attnum>0 AND NOT a.attisdropped
    LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
    UNION ALL
    SELECT format('constraint:%s:%s',c.relname,k.conname),
           jsonb_build_array('constraint',c.relname,k.conname,k.contype,k.condeferrable,k.condeferred,k.convalidated,
                             replace(pg_get_constraintdef(k.oid,true),n.nspname||'.','<schema>.'))
    FROM target_constraints tc JOIN pg_class c ON c.relname=tc.relname
    JOIN pg_namespace n ON n.oid=c.relnamespace AND n.nspname='qmt_strategy'
    JOIN pg_constraint k ON k.conrelid=c.oid AND k.conname=tc.conname
    UNION ALL
    SELECT 'function:miniqmt_k6_validate_coordination_update',
           jsonb_build_array('function',p.proname,l.lanname,p.provolatile,
                             replace(p.prosrc,n.nspname||'.','<schema>.'))
    FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace JOIN pg_language l ON l.oid=p.prolang
    WHERE n.nspname='qmt_strategy' AND p.proname='miniqmt_k6_validate_coordination_update'
), canonical_catalog AS (
    SELECT coalesce(jsonb_agg(item ORDER BY sort_key),'[]'::jsonb)::TEXT AS payload FROM catalog_items
)
SELECT encode(sha256(convert_to(payload,'UTF8')),'hex') FROM canonical_catalog
$$;

DO $$
DECLARE actual TEXT;
BEGIN
    SELECT qmt_strategy.miniqmt_k6c_catalog_fingerprint() INTO actual;
    IF actual <> 'f4fc093c83642577009dc5ce8c03550bbb75e00f09ada7bf2489272ddd67bd7d' THEN
        RAISE EXCEPTION 'K6-C0 post-commit catalog drift: expected f4fc093c83642577009dc5ce8c03550bbb75e00f09ada7bf2489272ddd67bd7d, got %',actual;
    END IF;
END $$;

COMMIT;

CREATE TABLE IF NOT EXISTS app.advisory_source_observer_cursor (
    observer_config_hash TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    data_source TEXT NOT NULL,
    source_role TEXT NOT NULL,
    last_audit_refreshed_at TIMESTAMPTZ NOT NULL,
    last_trade_date DATE,
    last_audit_row_hash TEXT,
    row_version INTEGER NOT NULL DEFAULT 1 CHECK (row_version >= 1),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (observer_config_hash, dataset_name, data_source, source_role),
    CHECK (observer_config_hash ~ '^[0-9a-f]{64}$'),
    CHECK (last_audit_row_hash IS NULL OR last_audit_row_hash ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS app.advisory_source_observation_receipt (
    observation_receipt_id TEXT PRIMARY KEY,
    observation_receipt_hash TEXT NOT NULL UNIQUE,
    observer_config_id TEXT NOT NULL,
    observer_config_version TEXT NOT NULL,
    observer_config_hash TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    data_source TEXT NOT NULL,
    source_role TEXT NOT NULL,
    trade_date DATE NOT NULL,
    partition_key JSONB NOT NULL,
    partition_key_hash TEXT NOT NULL,
    audit_refreshed_at TIMESTAMPTZ NOT NULL,
    audit_row_hash TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('EVENT_APPENDED', 'UNCHANGED', 'NOT_ELIGIBLE')),
    availability_event_id TEXT,
    availability_event_hash TEXT REFERENCES app.advisory_source_availability_event(event_content_hash),
    observed_schema_fingerprint TEXT,
    observed_row_count BIGINT CHECK (observed_row_count >= 0),
    observed_partition_content_hash TEXT,
    reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
    observed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (observation_receipt_hash ~ '^[0-9a-f]{64}$'),
    CHECK (observer_config_hash ~ '^[0-9a-f]{64}$'),
    CHECK (partition_key_hash ~ '^[0-9a-f]{64}$'),
    CHECK (audit_row_hash ~ '^[0-9a-f]{64}$'),
    CHECK (availability_event_hash IS NULL OR availability_event_hash ~ '^[0-9a-f]{64}$'),
    CHECK (observed_schema_fingerprint IS NULL OR observed_schema_fingerprint ~ '^[0-9a-f]{64}$'),
    CHECK (observed_partition_content_hash IS NULL OR observed_partition_content_hash ~ '^[0-9a-f]{64}$'),
    CHECK (jsonb_typeof(reason_codes) = 'array'),
    CHECK (
        (
            outcome IN ('EVENT_APPENDED', 'UNCHANGED')
            AND availability_event_id IS NOT NULL
            AND availability_event_hash IS NOT NULL
            AND observed_schema_fingerprint IS NOT NULL
            AND observed_row_count IS NOT NULL
            AND observed_partition_content_hash IS NOT NULL
            AND jsonb_array_length(reason_codes) = 0
        )
        OR (
            outcome = 'NOT_ELIGIBLE'
            AND availability_event_id IS NULL
            AND availability_event_hash IS NULL
            AND observed_schema_fingerprint IS NULL
            AND observed_row_count IS NULL
            AND observed_partition_content_hash IS NULL
            AND jsonb_array_length(reason_codes) > 0
        )
    ),
    UNIQUE (observer_config_hash, audit_row_hash, source_role)
);

CREATE INDEX IF NOT EXISTS ix_advisory_source_observer_cursor_updated
    ON app.advisory_source_observer_cursor(observer_config_hash, dataset_name, data_source, source_role, updated_at DESC);

CREATE INDEX IF NOT EXISTS ix_advisory_source_observation_receipt_audit
    ON app.advisory_source_observation_receipt(observer_config_hash, dataset_name, data_source, source_role, audit_refreshed_at DESC);

CREATE OR REPLACE FUNCTION app.verify_advisory_source_observer_cursor_update()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.row_version <> OLD.row_version + 1 THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1_SOURCE_OBSERVER_CURSOR_CONFLICT';
    END IF;
    IF NEW.last_audit_refreshed_at < OLD.last_audit_refreshed_at THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1_SOURCE_OBSERVER_CURSOR_CONFLICT';
    END IF;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_source_observer_cursor_update
    ON app.advisory_source_observer_cursor;
CREATE TRIGGER trg_verify_advisory_source_observer_cursor_update
    BEFORE UPDATE ON app.advisory_source_observer_cursor
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_source_observer_cursor_update();

CREATE OR REPLACE FUNCTION app.verify_advisory_source_observation_receipt()
RETURNS TRIGGER AS $$
DECLARE
    observed_now TIMESTAMPTZ := clock_timestamp();
    source_event app.advisory_source_availability_event%ROWTYPE;
BEGIN
    IF NEW.observed_at < observed_now - INTERVAL '5 seconds'
       OR NEW.observed_at > observed_now + INTERVAL '5 seconds' THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1_SOURCE_EVENT_TIME_INVALID';
    END IF;
    NEW.created_at := observed_now;

    IF NEW.outcome = 'NOT_ELIGIBLE' THEN
        RETURN NEW;
    END IF;

    SELECT * INTO source_event
    FROM app.advisory_source_availability_event
    WHERE event_content_hash = NEW.availability_event_hash
    FOR KEY SHARE;

    IF NOT FOUND
       OR source_event.availability_event_id <> NEW.availability_event_id
       OR source_event.dataset_name <> NEW.dataset_name
       OR source_event.source_role <> NEW.source_role
       OR source_event.partition_key_hash <> NEW.partition_key_hash
       OR source_event.schema_fingerprint <> NEW.observed_schema_fingerprint
       OR source_event.row_count <> NEW.observed_row_count
       OR source_event.partition_content_hash <> NEW.observed_partition_content_hash THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1_SOURCE_OBSERVER_RECEIPT_CONFLICT';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_source_observation_receipt
    ON app.advisory_source_observation_receipt;
CREATE TRIGGER trg_verify_advisory_source_observation_receipt
    BEFORE INSERT ON app.advisory_source_observation_receipt
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_source_observation_receipt();

CREATE OR REPLACE FUNCTION app.reject_advisory_source_observation_receipt_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'ADVISORY_PHASE1_SOURCE_OBSERVATION_RECEIPT_IMMUTABLE';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_reject_advisory_source_observation_receipt_mutation
    ON app.advisory_source_observation_receipt;
CREATE TRIGGER trg_reject_advisory_source_observation_receipt_mutation
    BEFORE UPDATE OR DELETE ON app.advisory_source_observation_receipt
    FOR EACH ROW EXECUTE FUNCTION app.reject_advisory_source_observation_receipt_mutation();

COMMENT ON TABLE app.advisory_source_observer_cursor IS
    'Mutable Phase 1D standalone worker checkpoint. It is not source availability authority and does not participate in selection, paper, simulation, or trading.';
COMMENT ON TABLE app.advisory_source_observation_receipt IS
    'Append-only Phase 1D processing evidence for one mutable ingestion-audit revision. Source availability authority remains app.advisory_source_availability_event.';
COMMENT ON COLUMN app.advisory_source_observation_receipt.audit_row_hash IS
    'Canonical hash of the observed mutable market.dataset_date_refresh_audit row, used only for exact retry and receipt identity.';

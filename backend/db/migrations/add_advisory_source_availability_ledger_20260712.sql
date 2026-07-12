CREATE TABLE IF NOT EXISTS app.advisory_source_availability_event (
    availability_event_id TEXT PRIMARY KEY,
    append_request_hash TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    source_role TEXT NOT NULL,
    partition_key JSONB NOT NULL,
    partition_key_hash TEXT NOT NULL,
    partition_chain_key TEXT NOT NULL,
    revision_id TEXT NOT NULL,
    event_revision_no INTEGER NOT NULL CHECK (event_revision_no >= 1),
    event_type TEXT NOT NULL CHECK (event_type IN ('INGESTED', 'CORRECTED', 'INVALIDATED', 'REVALIDATED')),
    predecessor_event_hash TEXT REFERENCES app.advisory_source_availability_event(event_content_hash),
    provider_job_id TEXT,
    refresh_job_id TEXT,
    provider_published_at TIMESTAMPTZ,
    first_observed_at TIMESTAMPTZ NOT NULL,
    formal_available_at TIMESTAMPTZ NOT NULL,
    schema_fingerprint TEXT NOT NULL,
    row_count BIGINT NOT NULL CHECK (row_count >= 0),
    partition_content_hash TEXT NOT NULL,
    quality_status TEXT NOT NULL,
    reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
    event_content_hash TEXT NOT NULL UNIQUE,
    created_by_service_principal TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_advisory_source_availability_formal_time
        CHECK (formal_available_at = GREATEST(first_observed_at, COALESCE(provider_published_at, first_observed_at))),
    CHECK (event_content_hash <> predecessor_event_hash),
    CHECK (
        (event_revision_no = 1 AND event_type = 'INGESTED' AND predecessor_event_hash IS NULL)
        OR (event_revision_no > 1 AND predecessor_event_hash IS NOT NULL)
    ),
    UNIQUE (partition_chain_key, event_revision_no)
);

ALTER TABLE app.advisory_source_availability_event
    ADD COLUMN IF NOT EXISTS append_request_hash TEXT;
ALTER TABLE app.advisory_source_availability_event
    ALTER COLUMN append_request_hash SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS ux_advisory_source_availability_request_hash
    ON app.advisory_source_availability_event(append_request_hash);

CREATE UNIQUE INDEX IF NOT EXISTS ux_advisory_source_availability_natural_revision_idx
    ON app.advisory_source_availability_event(dataset_name, source_role, partition_key_hash, event_revision_no);

ALTER TABLE app.advisory_source_availability_event
    DROP CONSTRAINT IF EXISTS ck_advisory_source_availability_formal_time;
ALTER TABLE app.advisory_source_availability_event
    ADD CONSTRAINT ck_advisory_source_availability_formal_time
    CHECK (formal_available_at = GREATEST(first_observed_at, COALESCE(provider_published_at, first_observed_at)));

CREATE UNIQUE INDEX IF NOT EXISTS ux_advisory_source_availability_one_successor
    ON app.advisory_source_availability_event(predecessor_event_hash)
    WHERE predecessor_event_hash IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_advisory_source_availability_as_of
    ON app.advisory_source_availability_event(partition_chain_key, formal_available_at DESC, event_revision_no DESC);

CREATE OR REPLACE FUNCTION app.verify_advisory_source_availability_successor()
RETURNS TRIGGER AS $$
DECLARE
    predecessor app.advisory_source_availability_event%ROWTYPE;
    observed_now TIMESTAMPTZ := clock_timestamp();
BEGIN
    IF NEW.first_observed_at < observed_now - INTERVAL '5 seconds'
       OR NEW.first_observed_at > observed_now + INTERVAL '5 seconds' THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1_SOURCE_EVENT_TIME_INVALID';
    END IF;
    NEW.created_at := observed_now;
    IF NEW.event_revision_no = 1 THEN
        RETURN NEW;
    END IF;

    SELECT * INTO predecessor
    FROM app.advisory_source_availability_event
    WHERE event_content_hash = NEW.predecessor_event_hash
    FOR KEY SHARE;

    IF NOT FOUND
       OR predecessor.partition_chain_key <> NEW.partition_chain_key
       OR predecessor.dataset_name <> NEW.dataset_name
       OR predecessor.source_role <> NEW.source_role
       OR predecessor.partition_key_hash <> NEW.partition_key_hash
       OR predecessor.event_revision_no <> NEW.event_revision_no - 1 THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1_SOURCE_EVENT_CHAIN_INVALID';
    END IF;
    IF (NEW.event_type = 'REVALIDATED' AND predecessor.event_type <> 'INVALIDATED')
       OR (NEW.event_type <> 'REVALIDATED' AND predecessor.event_type = 'INVALIDATED') THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1_SOURCE_EVENT_CHAIN_INVALID';
    END IF;
    IF NEW.event_type IN ('CORRECTED', 'REVALIDATED')
       AND (NEW.revision_id = predecessor.revision_id OR NEW.partition_content_hash = predecessor.partition_content_hash) THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1_SOURCE_EVENT_CHAIN_INVALID';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_source_availability_successor
    ON app.advisory_source_availability_event;
CREATE TRIGGER trg_verify_advisory_source_availability_successor
    BEFORE INSERT ON app.advisory_source_availability_event
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_source_availability_successor();

CREATE OR REPLACE FUNCTION app.reject_advisory_source_availability_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'ADVISORY_PHASE1_SOURCE_EVENT_IMMUTABLE';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_reject_advisory_source_availability_mutation
    ON app.advisory_source_availability_event;
CREATE TRIGGER trg_reject_advisory_source_availability_mutation
    BEFORE UPDATE OR DELETE ON app.advisory_source_availability_event
    FOR EACH ROW EXECUTE FUNCTION app.reject_advisory_source_availability_mutation();

COMMENT ON TABLE app.advisory_source_availability_event IS
    'Append-only Phase 1 source-availability evidence. It records only post-ingestion observed availability and never updates market source tables, backfills historical availability, starts an observer, or represents a trading action.';
COMMENT ON COLUMN app.advisory_source_availability_event.formal_available_at IS
    'Derived as max(provider_published_at when proven, repository-controlled database observation time). The insert trigger rejects caller-backdated observation timestamps.';
COMMENT ON COLUMN app.advisory_source_availability_event.predecessor_event_hash IS
    'Exact prior event in the same partition chain. A unique partial index and trigger prohibit forks, cross-partition links and skipped revisions.';

CREATE TABLE IF NOT EXISTS app.advisory_source_revision_set (
    source_revision_set_id TEXT PRIMARY KEY,
    source_revision_set_hash TEXT NOT NULL UNIQUE,
    query_registry_hash TEXT NOT NULL,
    requested_source_cutoff TIMESTAMPTZ NOT NULL,
    label_as_of_ts TIMESTAMPTZ NOT NULL,
    research_only BOOLEAN NOT NULL,
    member_count INTEGER NOT NULL CHECK (member_count > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE app.advisory_source_revision_set
    DROP CONSTRAINT IF EXISTS ck_advisory_source_revision_set_research_only;
ALTER TABLE app.advisory_source_revision_set
    ADD CONSTRAINT ck_advisory_source_revision_set_research_only
    CHECK (research_only IS TRUE);

CREATE TABLE IF NOT EXISTS app.advisory_source_revision_member (
    source_revision_set_id TEXT NOT NULL REFERENCES app.advisory_source_revision_set(source_revision_set_id),
    member_key TEXT NOT NULL,
    source_role TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    query_template_id TEXT NOT NULL,
    query_template_version TEXT NOT NULL,
    query_template_hash TEXT NOT NULL,
    bound_parameter_hash TEXT NOT NULL,
    partition_key JSONB NOT NULL,
    partition_key_hash TEXT NOT NULL,
    revision_kind TEXT NOT NULL CHECK (revision_kind IN ('IMMUTABLE_INGESTION', 'PARTITION_CONTENT_HASH', 'DURABLE_DB_SNAPSHOT', 'WATERMARK_ONLY')),
    revision_id TEXT NOT NULL,
    availability_event_hash TEXT REFERENCES app.advisory_source_availability_event(event_content_hash),
    availability_requirement TEXT NOT NULL,
    business_min_date DATE NOT NULL,
    business_max_date DATE NOT NULL,
    available_at_min TIMESTAMPTZ NOT NULL,
    available_at_max TIMESTAMPTZ NOT NULL,
    schema_fingerprint TEXT NOT NULL,
    row_count BIGINT NOT NULL CHECK (row_count >= 0),
    partition_content_hash TEXT NOT NULL,
    quality_status TEXT NOT NULL,
    reason_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
    research_only BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_revision_set_id, member_key),
    CHECK (business_min_date <= business_max_date),
    CHECK (available_at_min <= available_at_max),
    CHECK (availability_event_hash IS NOT NULL OR availability_requirement <> 'DECISION_CUTOFF'),
    CHECK (NOT (revision_kind = 'WATERMARK_ONLY' AND availability_requirement = 'DECISION_CUTOFF'))
);

ALTER TABLE app.advisory_source_revision_member
    DROP CONSTRAINT IF EXISTS ck_advisory_source_revision_member_research_only;
ALTER TABLE app.advisory_source_revision_member
    ADD CONSTRAINT ck_advisory_source_revision_member_research_only
    CHECK (research_only IS TRUE);

ALTER TABLE app.advisory_source_revision_member
    DROP CONSTRAINT IF EXISTS advisory_source_revision_member_availability_requirement_check;
ALTER TABLE app.advisory_source_revision_member
    DROP CONSTRAINT IF EXISTS ck_advisory_source_revision_member_requirement;
ALTER TABLE app.advisory_source_revision_member
    ADD CONSTRAINT ck_advisory_source_revision_member_requirement
    CHECK (availability_requirement IN ('DECISION_CUTOFF', 'LABEL_AS_OF', 'POLICY_FROZEN'));

CREATE INDEX IF NOT EXISTS idx_advisory_source_revision_member_event
    ON app.advisory_source_revision_member(availability_event_hash)
    WHERE availability_event_hash IS NOT NULL;

CREATE OR REPLACE FUNCTION app.verify_advisory_source_revision_member_event()
RETURNS TRIGGER AS $$
DECLARE
    source_event app.advisory_source_availability_event%ROWTYPE;
    source_set app.advisory_source_revision_set%ROWTYPE;
BEGIN
    SELECT * INTO source_set
    FROM app.advisory_source_revision_set
    WHERE source_revision_set_id = NEW.source_revision_set_id;
    IF NOT FOUND OR source_set.research_only <> NEW.research_only THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1_SOURCE_REVISION_MEMBER_INVALID';
    END IF;
    IF NEW.availability_event_hash IS NULL THEN
        RETURN NEW;
    END IF;
    SELECT * INTO source_event
    FROM app.advisory_source_availability_event
    WHERE event_content_hash = NEW.availability_event_hash;
    IF NOT FOUND
       OR source_event.dataset_name <> NEW.dataset_name
       OR source_event.source_role <> NEW.source_role
       OR source_event.partition_key_hash <> NEW.partition_key_hash
       OR source_event.revision_id <> NEW.revision_id
       OR source_event.partition_content_hash <> NEW.partition_content_hash
       OR source_event.schema_fingerprint <> NEW.schema_fingerprint
       OR source_event.row_count <> NEW.row_count
       OR source_event.formal_available_at <> NEW.available_at_min
       OR source_event.formal_available_at <> NEW.available_at_max
       OR source_event.quality_status <> 'PASS'
       OR source_event.event_type = 'INVALIDATED' THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1_SOURCE_REVISION_MEMBER_INVALID';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_verify_advisory_source_revision_member_event
    ON app.advisory_source_revision_member;
CREATE TRIGGER trg_verify_advisory_source_revision_member_event
    BEFORE INSERT ON app.advisory_source_revision_member
    FOR EACH ROW EXECUTE FUNCTION app.verify_advisory_source_revision_member_event();

CREATE OR REPLACE FUNCTION app.reject_advisory_source_revision_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'ADVISORY_PHASE1_SOURCE_REVISION_IMMUTABLE';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_reject_advisory_source_revision_set_mutation
    ON app.advisory_source_revision_set;
CREATE TRIGGER trg_reject_advisory_source_revision_set_mutation
    BEFORE UPDATE OR DELETE ON app.advisory_source_revision_set
    FOR EACH ROW EXECUTE FUNCTION app.reject_advisory_source_revision_mutation();

DROP TRIGGER IF EXISTS trg_reject_advisory_source_revision_member_mutation
    ON app.advisory_source_revision_member;
CREATE TRIGGER trg_reject_advisory_source_revision_member_mutation
    BEFORE UPDATE OR DELETE ON app.advisory_source_revision_member
    FOR EACH ROW EXECUTE FUNCTION app.reject_advisory_source_revision_mutation();

COMMENT ON TABLE app.advisory_source_revision_set IS
    'Immutable set of exact source members for an Advisory capture, label or dataset build. Runtime database snapshots and current source-table watermarks are intentionally excluded from its stable hash.';
COMMENT ON TABLE app.advisory_source_revision_member IS
    'Append-only exact source member. Formal members reference a matching immutable availability event; event-free members are explicitly research-only and cannot satisfy a decision-cutoff requirement with WATERMARK_ONLY evidence.';

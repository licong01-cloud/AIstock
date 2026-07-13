-- Phase 1C-2: schema-v2 source revision members carry the exact PIT cutoff predicate.
-- Existing v1 evidence remains immutable and is never backfilled or rewritten.

ALTER TABLE app.advisory_source_revision_set
    ADD COLUMN IF NOT EXISTS schema_version TEXT NOT NULL
    DEFAULT 'advisory_phase1_source_revision_set_v1';

ALTER TABLE app.advisory_source_revision_set
    ALTER COLUMN schema_version SET DEFAULT 'advisory_phase1_source_revision_set_v2';

ALTER TABLE app.advisory_source_revision_set
    DROP CONSTRAINT IF EXISTS ck_advisory_source_revision_set_schema_version;
ALTER TABLE app.advisory_source_revision_set
    ADD CONSTRAINT ck_advisory_source_revision_set_schema_version
    CHECK (schema_version IN (
        'advisory_phase1_source_revision_set_v1',
        'advisory_phase1_source_revision_set_v2'
    ));

ALTER TABLE app.advisory_source_revision_member
    ADD COLUMN IF NOT EXISTS enforced_cutoff_predicate_hash TEXT;

CREATE OR REPLACE FUNCTION app.require_advisory_source_revision_set_v2_on_insert()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.schema_version <> 'advisory_phase1_source_revision_set_v2' THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1_SOURCE_REVISION_SET_SCHEMA_INVALID';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_require_advisory_source_revision_set_v2_on_insert
    ON app.advisory_source_revision_set;
CREATE TRIGGER trg_require_advisory_source_revision_set_v2_on_insert
BEFORE INSERT ON app.advisory_source_revision_set
FOR EACH ROW EXECUTE FUNCTION app.require_advisory_source_revision_set_v2_on_insert();

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
    IF source_set.schema_version = 'advisory_phase1_source_revision_set_v2'
       AND (
            NEW.enforced_cutoff_predicate_hash IS NULL
            OR NEW.enforced_cutoff_predicate_hash !~ '^[0-9a-f]{64}$'
       ) THEN
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

COMMENT ON COLUMN app.advisory_source_revision_set.schema_version IS
    'Stable source revision contract version. Legacy v1 evidence remains readable; new repository writes use v2.';
COMMENT ON COLUMN app.advisory_source_revision_member.enforced_cutoff_predicate_hash IS
    'Canonical hash of the frozen cutoff predicate. It is mandatory for v2 members and is never inferred from a current query.';

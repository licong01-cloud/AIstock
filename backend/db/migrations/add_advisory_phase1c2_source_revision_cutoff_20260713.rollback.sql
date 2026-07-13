-- Development/test rollback only. It must not erase immutable v2 evidence.

DO $$
DECLARE
    has_v2_evidence BOOLEAN := FALSE;
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'app'
          AND table_name = 'advisory_source_revision_set'
          AND column_name = 'schema_version'
    ) THEN
        EXECUTE
            'SELECT EXISTS (
                SELECT 1
                FROM app.advisory_source_revision_set
                WHERE schema_version = ''advisory_phase1_source_revision_set_v2''
            )'
            INTO has_v2_evidence;
    END IF;
    IF has_v2_evidence THEN
        RAISE EXCEPTION 'ADVISORY_PHASE1C2_ROLLBACK_REQUIRES_NO_V2_SOURCE_REVISION_EVIDENCE';
    END IF;
END;
$$;

DROP TRIGGER IF EXISTS trg_require_advisory_source_revision_set_v2_on_insert
    ON app.advisory_source_revision_set;
DROP FUNCTION IF EXISTS app.require_advisory_source_revision_set_v2_on_insert();

ALTER TABLE app.advisory_source_revision_member
    DROP COLUMN IF EXISTS enforced_cutoff_predicate_hash;

ALTER TABLE app.advisory_source_revision_set
    DROP CONSTRAINT IF EXISTS ck_advisory_source_revision_set_schema_version;
ALTER TABLE app.advisory_source_revision_set
    DROP COLUMN IF EXISTS schema_version;

-- Restore the pre-Phase-1C-2 validation body after the new column is removed.
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

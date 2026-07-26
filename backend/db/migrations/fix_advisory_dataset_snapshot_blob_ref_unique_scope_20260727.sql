-- BUG-879: scope app.advisory_dataset_snapshot_blob_ref uniqueness per snapshot.
--
-- Rows in app.advisory_dataset_snapshot_blob_ref are per-snapshot references:
-- the primary key, foreign keys, and exact-retry readback are snapshot-scoped.
-- Multiple snapshots may therefore reference the same immutable content. The
-- legacy global UNIQUE (ref_content_hash) incorrectly treated content identity
-- as reference-row identity. The correct uniqueness contract is
-- UNIQUE (snapshot_id, ref_content_hash).

BEGIN;

-- Fail visibly instead of waiting indefinitely behind a concurrent snapshot
-- write or another schema change.
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '60s';

LOCK TABLE app.advisory_dataset_snapshot_blob_ref IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM app.advisory_dataset_snapshot_blob_ref
         GROUP BY snapshot_id, ref_content_hash
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'ADVISORY_SNAPSHOT_BLOB_REF_DUPLICATE_WITHIN_SNAPSHOT';
    END IF;
END;
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'advisory_dataset_snapshot_blob_ref_ref_content_hash_key'
           AND conrelid = 'app.advisory_dataset_snapshot_blob_ref'::regclass
    ) THEN
        ALTER TABLE app.advisory_dataset_snapshot_blob_ref
            DROP CONSTRAINT advisory_dataset_snapshot_blob_ref_ref_content_hash_key;
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'advisory_dataset_snapshot_blob_ref_snapshot_scoped_ref_hash_key'
           AND conrelid = 'app.advisory_dataset_snapshot_blob_ref'::regclass
    ) THEN
        ALTER TABLE app.advisory_dataset_snapshot_blob_ref
            ADD CONSTRAINT advisory_dataset_snapshot_blob_ref_snapshot_scoped_ref_hash_key
            UNIQUE (snapshot_id, ref_content_hash);
    END IF;
END;
$$;

DO $$
DECLARE
    constraint_def TEXT;
BEGIN
    SELECT pg_get_constraintdef(oid)
      INTO constraint_def
      FROM pg_constraint
     WHERE conname = 'advisory_dataset_snapshot_blob_ref_snapshot_scoped_ref_hash_key'
       AND conrelid = 'app.advisory_dataset_snapshot_blob_ref'::regclass;

    IF constraint_def IS DISTINCT FROM 'UNIQUE (snapshot_id, ref_content_hash)' THEN
        RAISE EXCEPTION 'ADVISORY_SNAPSHOT_BLOB_REF_UNIQUE_SCOPE_MISMATCH: %',
            coalesce(constraint_def, '<missing>');
    END IF;

    IF EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'advisory_dataset_snapshot_blob_ref_ref_content_hash_key'
           AND conrelid = 'app.advisory_dataset_snapshot_blob_ref'::regclass
    ) THEN
        RAISE EXCEPTION 'ADVISORY_SNAPSHOT_BLOB_REF_LEGACY_UNIQUE_STILL_PRESENT';
    END IF;
END;
$$;

COMMIT;

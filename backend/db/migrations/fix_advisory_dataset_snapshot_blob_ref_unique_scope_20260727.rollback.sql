-- Rollback for fix_advisory_dataset_snapshot_blob_ref_unique_scope_20260727.sql.
--
-- Restores the legacy global UNIQUE (ref_content_hash). Rollback is refused
-- once multiple snapshots share a content hash because the old constraint can
-- no longer represent the persisted reference set without deleting data.

BEGIN;

SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '60s';

LOCK TABLE app.advisory_dataset_snapshot_blob_ref IN SHARE ROW EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM app.advisory_dataset_snapshot_blob_ref
         GROUP BY ref_content_hash
        HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'ADVISORY_SNAPSHOT_BLOB_REF_SHARED_ACROSS_SNAPSHOTS';
    END IF;
END;
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'advisory_dataset_snapshot_blob_ref_snapshot_scoped_ref_hash_key'
           AND conrelid = 'app.advisory_dataset_snapshot_blob_ref'::regclass
    ) THEN
        ALTER TABLE app.advisory_dataset_snapshot_blob_ref
            DROP CONSTRAINT advisory_dataset_snapshot_blob_ref_snapshot_scoped_ref_hash_key;
    END IF;

    IF NOT EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'advisory_dataset_snapshot_blob_ref_ref_content_hash_key'
           AND conrelid = 'app.advisory_dataset_snapshot_blob_ref'::regclass
    ) THEN
        ALTER TABLE app.advisory_dataset_snapshot_blob_ref
            ADD CONSTRAINT advisory_dataset_snapshot_blob_ref_ref_content_hash_key
            UNIQUE (ref_content_hash);
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
     WHERE conname = 'advisory_dataset_snapshot_blob_ref_ref_content_hash_key'
       AND conrelid = 'app.advisory_dataset_snapshot_blob_ref'::regclass;

    IF constraint_def IS DISTINCT FROM 'UNIQUE (ref_content_hash)' THEN
        RAISE EXCEPTION 'ADVISORY_SNAPSHOT_BLOB_REF_ROLLBACK_MISMATCH: %',
            coalesce(constraint_def, '<missing>');
    END IF;

    IF EXISTS (
        SELECT 1
          FROM pg_constraint
         WHERE conname = 'advisory_dataset_snapshot_blob_ref_snapshot_scoped_ref_hash_key'
           AND conrelid = 'app.advisory_dataset_snapshot_blob_ref'::regclass
    ) THEN
        RAISE EXCEPTION 'ADVISORY_SNAPSHOT_BLOB_REF_SNAPSHOT_UNIQUE_STILL_PRESENT';
    END IF;
END;
$$;

COMMIT;

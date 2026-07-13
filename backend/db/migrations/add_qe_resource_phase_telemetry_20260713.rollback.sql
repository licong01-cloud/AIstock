BEGIN;

DROP TABLE IF EXISTS qe_archive.run_resource_phase;
DROP TABLE IF EXISTS qe_archive.run_resource_session;
DELETE FROM qe_archive.schema_version WHERE version = 'qe_archive_v4_20260713';

COMMIT;

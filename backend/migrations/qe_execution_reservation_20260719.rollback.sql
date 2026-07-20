-- Guarded rollback for qe_execution_reservation_20260719.sql.
--
-- Normal code rollback keeps the additive table. Run this destructive schema
-- rollback only before any reservation row has ever been created.

BEGIN;

DO $rollback_precondition$
BEGIN
    IF to_regclass('infra.qe_execution_reservation') IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'qe_execution_reservation_rollback_schema_missing';
    END IF;
    IF EXISTS (SELECT 1 FROM infra.qe_execution_reservation) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0001',
            MESSAGE = 'qe_execution_reservation_rollback_data_present',
            DETAIL = 'Keep the additive ledger; destructive rollback is forbidden after any execution reservation exists.';
    END IF;
END
$rollback_precondition$;

DROP TABLE infra.qe_execution_reservation;

COMMIT;

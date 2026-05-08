-- Manual rollback plan for backend/migrations/model_registry_phase5_20260509.sql.
--
-- WARNING: This is destructive. It removes every model_registry table, view,
-- index, comment, and row. Do not run against production. Use only on a dev
-- database after exporting any rows that must be preserved.
--
-- Required operator steps:
-- 1. Verify DB target is a dev/test/sandbox database, not production.
-- 2. Run inside an explicit transaction.
-- 3. Set the confirmation token in the same transaction:
--      SET LOCAL aistock.model_registry_rollback_confirm = 'DROP_MODEL_REGISTRY_PHASE5_DEV_ONLY';
-- 4. Review the objects that will be dropped before committing.

DO $$
BEGIN
    IF current_setting('aistock.model_registry_rollback_confirm', true) IS DISTINCT FROM 'DROP_MODEL_REGISTRY_PHASE5_DEV_ONLY' THEN
        RAISE EXCEPTION
            'Refusing model_registry rollback without SET LOCAL aistock.model_registry_rollback_confirm = DROP_MODEL_REGISTRY_PHASE5_DEV_ONLY';
    END IF;
END $$;

DROP SCHEMA IF EXISTS model_registry CASCADE;

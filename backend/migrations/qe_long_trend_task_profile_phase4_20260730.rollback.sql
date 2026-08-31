BEGIN;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'qe_evolution_tasks'
          AND column_name = 'long_trend_profile_id'
    ) THEN
        LOCK TABLE qe_evolution_tasks IN ACCESS EXCLUSIVE MODE;
        IF EXISTS (
            SELECT 1 FROM qe_evolution_tasks
            WHERE long_trend_profile_id IS NOT NULL
            LIMIT 1
        ) THEN
            RAISE EXCEPTION 'guarded rollback refused: long_trend_profile_id contains data';
        END IF;
    END IF;
END
$$;

ALTER TABLE qe_evolution_tasks
    DROP COLUMN IF EXISTS long_trend_profile_id;

COMMIT;

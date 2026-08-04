BEGIN;

ALTER TABLE qe_evolution_tasks
    ADD COLUMN IF NOT EXISTS long_trend_profile_id TEXT;

COMMENT ON COLUMN qe_evolution_tasks.long_trend_profile_id IS
    'Immutable registered QE-only F-014 task profile. NULL keeps long-trend evaluation disabled.';

COMMIT;

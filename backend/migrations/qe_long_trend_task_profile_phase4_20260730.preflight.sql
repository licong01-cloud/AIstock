DO $$
BEGIN
    IF to_regclass('public.qe_evolution_tasks') IS NULL THEN
        RAISE EXCEPTION 'qe_evolution_tasks is missing';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'qe_evolution_tasks'
          AND column_name = 'long_trend_profile_id'
          AND (udt_name <> 'text' OR is_nullable <> 'YES')
    ) THEN
        RAISE EXCEPTION 'existing long_trend_profile_id column contract differs';
    END IF;
END
$$;

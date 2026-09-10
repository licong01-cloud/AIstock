-- Only after explicit authorization, both tables empty, and no consumer dependency.
BEGIN;
LOCK TABLE public.factor_research_tasks, public.factor_research_records IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM public.factor_research_tasks)
       OR EXISTS (SELECT 1 FROM public.factor_research_records) THEN
        RAISE EXCEPTION 'Research history exists: use a compatible forward repair';
    END IF;
END $$;
DROP TABLE public.factor_research_records;
DROP TABLE public.factor_research_tasks;
COMMIT;

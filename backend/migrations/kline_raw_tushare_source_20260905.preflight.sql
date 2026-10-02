-- Read-only preflight for allowing truthful Tushare fallback provenance.
DO $$
DECLARE
    table_name TEXT;
    constraint_name TEXT;
    definition TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY['kline_daily_raw', 'kline_minute_raw']
    LOOP
        IF to_regclass(format('market.%I', table_name)) IS NULL THEN
            RAISE EXCEPTION 'required table is missing: market.%', table_name;
        END IF;
        constraint_name := table_name || '_source_check';
        SELECT pg_get_constraintdef(oid)
          INTO definition
          FROM pg_constraint
         WHERE conrelid = format('market.%I', table_name)::regclass
           AND conname = constraint_name
           AND contype = 'c';
        IF definition IS NULL THEN
            RAISE EXCEPTION 'required source constraint is missing: %', constraint_name;
        END IF;
        IF position('tdx_api' IN definition) = 0 OR position('tdx_vipdoc' IN definition) = 0 THEN
            RAISE EXCEPTION 'source constraint has an unknown predecessor: %', constraint_name;
        END IF;
    END LOOP;
END
$$;

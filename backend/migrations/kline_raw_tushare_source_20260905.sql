BEGIN;

ALTER TABLE market.kline_daily_raw
    DROP CONSTRAINT IF EXISTS kline_daily_raw_source_check;
ALTER TABLE market.kline_daily_raw
    ADD CONSTRAINT kline_daily_raw_source_check
    CHECK (source IN ('tdx_api', 'tdx_vipdoc', 'tushare_api')) NOT VALID;

ALTER TABLE market.kline_minute_raw
    DROP CONSTRAINT IF EXISTS kline_minute_raw_source_check;
ALTER TABLE market.kline_minute_raw
    ADD CONSTRAINT kline_minute_raw_source_check
    CHECK (source IN ('tdx_api', 'tdx_vipdoc', 'tushare_api')) NOT VALID;

DO $$
DECLARE
    table_name TEXT;
    constraint_name TEXT;
    definition TEXT;
    validated BOOLEAN;
BEGIN
    FOREACH table_name IN ARRAY ARRAY['kline_daily_raw', 'kline_minute_raw']
    LOOP
        constraint_name := table_name || '_source_check';
        SELECT pg_get_constraintdef(oid), convalidated
          INTO definition, validated
          FROM pg_constraint
         WHERE conrelid = format('market.%I', table_name)::regclass
           AND conname = constraint_name
           AND contype = 'c';
        IF definition IS NULL
           OR position('tdx_api' IN definition) = 0
           OR position('tdx_vipdoc' IN definition) = 0
           OR position('tushare_api' IN definition) = 0
           OR validated THEN
            RAISE EXCEPTION 'source constraint readback failed: %', constraint_name;
        END IF;
    END LOOP;
END
$$;

COMMIT;

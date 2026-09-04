BEGIN;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM market.kline_daily_raw WHERE source = 'tushare_api')
       OR EXISTS (SELECT 1 FROM market.kline_minute_raw WHERE source = 'tushare_api') THEN
        RAISE EXCEPTION 'rollback refused while tushare_api rows exist';
    END IF;
END
$$;

ALTER TABLE market.kline_daily_raw
    DROP CONSTRAINT IF EXISTS kline_daily_raw_source_check;
ALTER TABLE market.kline_daily_raw
    ADD CONSTRAINT kline_daily_raw_source_check
    CHECK (source IN ('tdx_api', 'tdx_vipdoc')) NOT VALID;

ALTER TABLE market.kline_minute_raw
    DROP CONSTRAINT IF EXISTS kline_minute_raw_source_check;
ALTER TABLE market.kline_minute_raw
    ADD CONSTRAINT kline_minute_raw_source_check
    CHECK (source IN ('tdx_api', 'tdx_vipdoc')) NOT VALID;

COMMIT;

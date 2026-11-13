-- Explicit DEV-first migration. Never executed by backend startup.
BEGIN;
ALTER TABLE market.daily_basic ADD COLUMN IF NOT EXISTS limit_status SMALLINT;
COMMENT ON COLUMN market.daily_basic.limit_status IS '收盘涨跌停状态0..6，NULL为未知；非盘中可成交判断';

CREATE TABLE IF NOT EXISTS market.vix_index (
    trade_date DATE PRIMARY KEY,
    close NUMERIC NOT NULL CHECK (close>0 AND close::text NOT IN ('NaN','Infinity','-Infinity')),
    pct_change NUMERIC NOT NULL CHECK (pct_change::text NOT IN ('NaN','Infinity','-Infinity')),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
COMMENT ON TABLE market.vix_index IS 'Tushare vix_index 美国VIX日频事实，非历史PIT可得证明';
COMMENT ON COLUMN market.vix_index.trade_date IS '美国session日期，不使用中国交易日历';
COMMENT ON COLUMN market.vix_index.close IS 'VIX收盘指数点，有限正数';
COMMENT ON COLUMN market.vix_index.pct_change IS '涨跌幅百分比，可为负';
COMMENT ON COLUMN market.vix_index.updated_at IS '本地当前事实实际更新时间';

CREATE TABLE IF NOT EXISTS market.stock_daily_afterhours (
    trade_date DATE NOT NULL CHECK (trade_date >= DATE '2026-07-06'),
    ts_code TEXT NOT NULL CHECK (ts_code ~ '^(00[0-9]{4}[.]SZ|30[0-9]{4}[.]SZ|6[0-9]{5}[.]SH)$'),
    ah_vol NUMERIC CHECK (ah_vol>=0 AND ah_vol::text NOT IN ('NaN','Infinity','-Infinity')),
    ah_amount NUMERIC CHECK (ah_amount>=0 AND ah_amount::text NOT IN ('NaN','Infinity','-Infinity')),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (trade_date, ts_code)
);
COMMENT ON TABLE market.stock_daily_afterhours IS 'Tushare daily 盘后固定价格成交事实，不改写TDX量额';
COMMENT ON COLUMN market.stock_daily_afterhours.trade_date IS '交易日，来源覆盖起点2026-07-06';
COMMENT ON COLUMN market.stock_daily_afterhours.ts_code IS '沪深A股代码，不按买入PIT裁剪';
COMMENT ON COLUMN market.stock_daily_afterhours.ah_vol IS '盘后成交量（手=100股），NULL未知，0为真实零';
COMMENT ON COLUMN market.stock_daily_afterhours.ah_amount IS '盘后成交额（千元人民币），NULL未知';
COMMENT ON COLUMN market.stock_daily_afterhours.updated_at IS '本地当前事实实际更新时间';

CREATE TABLE IF NOT EXISTS market.tushare_supplemental_raw (
    dataset TEXT NOT NULL CHECK (dataset IN ('vix_index','stock_daily_afterhours')),
    source_api TEXT NOT NULL,
    trade_date DATE NOT NULL,
    ts_code TEXT,
    source_row_sha256 TEXT NOT NULL CHECK (source_row_sha256 ~ '^[0-9a-f]{64}$'),
    raw_payload JSONB NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    last_observed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    published_at TIMESTAMPTZ,
    publication_time_quality TEXT NOT NULL DEFAULT 'UNKNOWN' CHECK (publication_time_quality='UNKNOWN'),
    vintage_quality TEXT NOT NULL DEFAULT 'NON_VINTAGE' CHECK (vintage_quality='NON_VINTAGE'),
    PRIMARY KEY (dataset, source_row_sha256)
);
CREATE INDEX IF NOT EXISTS tushare_supplemental_raw_date_idx
    ON market.tushare_supplemental_raw(dataset, trade_date, ts_code);
COMMENT ON TABLE market.tushare_supplemental_raw IS '本批VIX及盘后量额原始观察版本；同内容幂等，不伪称历史PIT';
COMMENT ON COLUMN market.tushare_supplemental_raw.dataset IS '正式DatasetSpec名称';
COMMENT ON COLUMN market.tushare_supplemental_raw.source_api IS '真实Tushare接口名称';
COMMENT ON COLUMN market.tushare_supplemental_raw.trade_date IS '上游事实日期；非捕获或发布时间';
COMMENT ON COLUMN market.tushare_supplemental_raw.ts_code IS '股票来源代码；VIX不适用保留NULL';
COMMENT ON COLUMN market.tushare_supplemental_raw.source_row_sha256 IS '原始JSON字段排序紧凑UTF8内容SHA256，非供应商记录ID';
COMMENT ON COLUMN market.tushare_supplemental_raw.raw_payload IS '请求字段原文，保留数值字符串及显式NULL';
COMMENT ON COLUMN market.tushare_supplemental_raw.captured_at IS '该内容首次实际抓取时间，绝不倒填';
COMMENT ON COLUMN market.tushare_supplemental_raw.last_observed_at IS '该内容最近实际观察时间';
COMMENT ON COLUMN market.tushare_supplemental_raw.published_at IS '上游未提供，保留NULL';
COMMENT ON COLUMN market.tushare_supplemental_raw.publication_time_quality IS 'UNKNOWN，不由日期或capture推断';
COMMENT ON COLUMN market.tushare_supplemental_raw.vintage_quality IS 'NON_VINTAGE，当前供应商重建历史';

CREATE OR REPLACE FUNCTION market.touch_tushare_phase1_updated_at() RETURNS trigger AS $$
BEGIN NEW.updated_at=clock_timestamp(); RETURN NEW; END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS touch_tushare_phase1 ON market.vix_index;
CREATE TRIGGER touch_tushare_phase1 BEFORE UPDATE ON market.vix_index
    FOR EACH ROW EXECUTE FUNCTION market.touch_tushare_phase1_updated_at();
DROP TRIGGER IF EXISTS touch_tushare_phase1 ON market.stock_daily_afterhours;
CREATE TRIGGER touch_tushare_phase1 BEFORE UPDATE ON market.stock_daily_afterhours
    FOR EACH ROW EXECUTE FUNCTION market.touch_tushare_phase1_updated_at();

INSERT INTO market.data_stats_config
  (data_kind, table_name, date_column, updated_column, enabled, extra_info)
VALUES
 ('vix_index','market.vix_index','trade_date','updated_at',TRUE,
  '{"desc":"VIX美国日频指数","source_api":"vix_index","date_sequence":"calendar","cursor_source":"refresh_audit","session_calendar_completeness":"NOT_EVALUATED","required_for_daily_complete":false}'::jsonb),
 ('stock_daily_afterhours','market.stock_daily_afterhours','trade_date','updated_at',TRUE,
  '{"desc":"股票盘后固定价格成交量额（手/千元）","source_api":"daily","date_sequence":"trading","cursor_source":"refresh_audit","required_for_daily_complete":false}'::jsonb)
ON CONFLICT(data_kind) DO UPDATE SET table_name=EXCLUDED.table_name,
  date_column=EXCLUDED.date_column, updated_column=EXCLUDED.updated_column,
  extra_info=EXCLUDED.extra_info;

INSERT INTO market.data_stats(data_kind,table_name,extra_info)
SELECT data_kind,table_name,extra_info FROM market.data_stats_config
WHERE data_kind IN ('vix_index','stock_daily_afterhours')
ON CONFLICT(data_kind) DO NOTHING;

-- New defaults only: do not overwrite existing operator schedule customizations.
INSERT INTO market.ingestion_schedules
 (schedule_id,dataset,mode,frequency,enabled,options,created_at,updated_at)
VALUES
 ('7cbdc784-6824-47c0-aee3-6207335f9785','vix_index','incremental','daily',TRUE,
  '{"at":"08:00","skip_auto_range":true}'::jsonb,NOW(),NOW()),
 ('b290d553-51b2-44f3-b1e8-a0d0732ebf41','_vix_index_midday','incremental','daily',TRUE,
  '{"at":"12:00","skip_auto_range":true}'::jsonb,NOW(),NOW()),
 ('c6c0c550-45e4-4d42-9977-b1089537cdf2','stock_daily_afterhours','incremental','daily',TRUE,
  '{"at":"20:35"}'::jsonb,NOW(),NOW())
ON CONFLICT (dataset,mode) DO NOTHING;
COMMIT;

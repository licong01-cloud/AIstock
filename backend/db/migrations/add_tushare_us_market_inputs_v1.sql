-- Existing DEV first. Two small source tables; no equity/PIT/business writes.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
CREATE TABLE IF NOT EXISTS market.index_global (
    trade_date DATE NOT NULL,
    ts_code TEXT NOT NULL CHECK (ts_code IN ('SPX','IXIC','DJI')),
    open NUMERIC NOT NULL CHECK (open>0 AND open::text NOT IN ('NaN','Infinity','-Infinity')),
    close NUMERIC NOT NULL CHECK (close>0 AND close::text NOT IN ('NaN','Infinity','-Infinity')),
    high NUMERIC NOT NULL CHECK (high>0 AND high::text NOT IN ('NaN','Infinity','-Infinity')),
    low NUMERIC NOT NULL CHECK (low>0 AND low::text NOT IN ('NaN','Infinity','-Infinity')),
    pre_close NUMERIC NOT NULL CHECK (pre_close>0 AND pre_close::text NOT IN ('NaN','Infinity','-Infinity')),
    change NUMERIC, pct_chg NUMERIC, swing NUMERIC, vol NUMERIC, amount NUMERIC,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(trade_date, ts_code),
    CHECK (low<=open AND low<=close AND high>=open AND high>=close)
);
COMMENT ON TABLE market.index_global IS 'Tushare index_global 美股三大指数日线，纽约session date';
COMMENT ON COLUMN market.index_global.trade_date IS '美国交易日，非北京时间日期';
COMMENT ON COLUMN market.index_global.ts_code IS '官方文本代码SPX/IXIC/DJI，不使用A股或PIT代码';
COMMENT ON COLUMN market.index_global.open IS '开盘点位';
COMMENT ON COLUMN market.index_global.close IS '收盘点位';
COMMENT ON COLUMN market.index_global.high IS '最高点位';
COMMENT ON COLUMN market.index_global.low IS '最低点位';
COMMENT ON COLUMN market.index_global.pre_close IS '上个交易日收盘点位';
COMMENT ON COLUMN market.index_global.change IS '涨跌点位';
COMMENT ON COLUMN market.index_global.pct_chg IS '涨跌幅（百分点，不除以100存储）';
COMMENT ON COLUMN market.index_global.swing IS '振幅（百分点）';
COMMENT ON COLUMN market.index_global.vol IS '供应商原始成交量，官方未明确单位，通常NULL，不猜测转换';
COMMENT ON COLUMN market.index_global.amount IS '供应商原始成交额，官方未明确币种/单位，通常NULL';
COMMENT ON COLUMN market.index_global.updated_at IS '本地事实实际更新时间，不代表历史发布时间';
CREATE TABLE IF NOT EXISTS market.us_tradecal (
    cal_date DATE PRIMARY KEY,
    is_open SMALLINT NOT NULL CHECK (is_open IN (0,1)),
    pretrade_date DATE CHECK (pretrade_date<cal_date),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
COMMENT ON TABLE market.us_tradecal IS 'Tushare us_tradecal 美股开休市日历，不提供半日收市时刻';
COMMENT ON COLUMN market.us_tradecal.cal_date IS '美国本地日历日期，包含休市日';
COMMENT ON COLUMN market.us_tradecal.is_open IS '供应商开市标记：0休市、1开市';
COMMENT ON COLUMN market.us_tradecal.pretrade_date IS '供应商上一交易日；缺失保留NULL';
COMMENT ON COLUMN market.us_tradecal.updated_at IS '本地日历事实实际更新时间';

-- Extend only this feed allow-list; all existing observations remain intact.
ALTER TABLE market.tushare_supplemental_raw DROP CONSTRAINT IF EXISTS tushare_supplemental_raw_dataset_check;
ALTER TABLE market.tushare_supplemental_raw ADD CONSTRAINT tushare_supplemental_raw_dataset_check
    CHECK (dataset IN ('vix_index','stock_daily_afterhours','index_global','us_tradecal'));
COMMENT ON TABLE market.tushare_supplemental_raw IS 'Tushare共享日频原始内容版本，真实捕获时间，不冒充历史PIT';
DROP TRIGGER IF EXISTS touch_tushare_us ON market.index_global;
CREATE TRIGGER touch_tushare_us BEFORE UPDATE ON market.index_global
 FOR EACH ROW EXECUTE FUNCTION market.touch_tushare_phase1_updated_at();
DROP TRIGGER IF EXISTS touch_tushare_us ON market.us_tradecal;
CREATE TRIGGER touch_tushare_us BEFORE UPDATE ON market.us_tradecal
 FOR EACH ROW EXECUTE FUNCTION market.touch_tushare_phase1_updated_at();

INSERT INTO market.data_stats_config(data_kind,table_name,date_column,updated_column,enabled,extra_info)
VALUES
 ('index_global','market.index_global','trade_date','updated_at',TRUE,
  '{"desc":"美股三大指数（标普500/纳斯达克/道琼斯）","source_api":"index_global","date_sequence":"calendar","session_calendar":"market.us_tradecal","required_for_daily_complete":false}'::jsonb),
 ('us_tradecal','market.us_tradecal','cal_date','updated_at',TRUE,
  '{"desc":"美股交易日历","source_api":"us_tradecal","date_sequence":"calendar","required_for_daily_complete":false}'::jsonb)
ON CONFLICT(data_kind) DO UPDATE SET table_name=EXCLUDED.table_name,date_column=EXCLUDED.date_column,
 updated_column=EXCLUDED.updated_column,extra_info=EXCLUDED.extra_info;
INSERT INTO market.data_stats(data_kind,table_name,extra_info)
SELECT data_kind,table_name,extra_info FROM market.data_stats_config
 WHERE data_kind IN ('index_global','us_tradecal') ON CONFLICT(data_kind) DO NOTHING;
INSERT INTO market.ingestion_schedules(schedule_id,dataset,mode,frequency,enabled,options,created_at,updated_at)
VALUES
 -- New schedules stay disabled until the backend has loaded this producer.
 ('d69ac4db-5c08-4a11-aac3-37a217b5ef25','us_tradecal','incremental','daily',FALSE,'{"at":"07:30","skip_auto_range":true}',NOW(),NOW()),
 ('dbeb178a-5a48-49c6-b205-ecac8f6b0dc5','index_global','incremental','daily',FALSE,'{"at":"08:05","skip_auto_range":true}',NOW(),NOW()),
 ('47d5420c-9cca-411c-a0f6-7247ae51cb79','_index_global_midday','incremental','daily',FALSE,'{"at":"12:05","skip_auto_range":true}',NOW(),NOW())
ON CONFLICT(dataset,mode) DO NOTHING;
COMMIT;

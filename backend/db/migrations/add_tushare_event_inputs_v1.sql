-- Operator-run DEV-first migration; backend startup never creates these tables.
BEGIN;
DO $migration$
DECLARE relation_name text;
BEGIN
  FOREACH relation_name IN ARRAY ARRAY['tushare_risk_event_raw','index_announcements_raw','stock_seasoned_raw'] LOOP
    EXECUTE format('CREATE TABLE IF NOT EXISTS market.%I (
      source_api text NOT NULL,
      request_date date NOT NULL,
      event_date date NOT NULL,
      ts_code text,
      source_row_sha256 text NOT NULL CHECK(source_row_sha256 ~ ''^[0-9a-f]{64}$''),
      raw_payload jsonb NOT NULL,
      captured_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      last_observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
      batch_id uuid NOT NULL,
      source_record_key text NOT NULL DEFAULT ''UNKNOWN'',
      published_at timestamptz CHECK(published_at IS NULL),
      publication_time_quality text NOT NULL DEFAULT ''UNKNOWN'' CHECK(publication_time_quality=''UNKNOWN''),
      vintage_quality text NOT NULL DEFAULT ''NON_VINTAGE'' CHECK(vintage_quality=''NON_VINTAGE''),
      PRIMARY KEY(source_api,source_row_sha256)
    )', relation_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON market.%I(source_api,request_date,event_date)',
                   relation_name||'_date_idx', relation_name);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON market.%I(source_api,ts_code)',
                   relation_name||'_code_idx', relation_name);
    EXECUTE format('COMMENT ON TABLE market.%I IS %L', relation_name,
                   'Tushare原始事件观察版本，不是历史PIT；仅限本地数据采集');
    EXECUTE format('COMMENT ON COLUMN market.%I.source_api IS %L', relation_name,'真实Tushare接口名称');
    EXECUTE format('COMMENT ON COLUMN market.%I.request_date IS %L', relation_name,'查询事件日期分桶；不等于实际捕获时间');
    EXECUTE format('COMMENT ON COLUMN market.%I.event_date IS %L', relation_name,'上游公告日期或提示开始日，不推断发布时刻/生效日');
    EXECUTE format('COMMENT ON COLUMN market.%I.ts_code IS %L', relation_name,'来源沪深A股代码；指数公告不适用为NULL');
    EXECUTE format('COMMENT ON COLUMN market.%I.source_row_sha256 IS %L', relation_name,'原始排序紧凑UTF8 JSON内容SHA256，不是供应商项目ID');
    EXECUTE format('COMMENT ON COLUMN market.%I.raw_payload IS %L', relation_name,'aistock_tushare_event_raw_v1，字段见source_api的DatasetSpec；NON_VINTAGE/UNKNOWN；保留NULL及股数/人民币元/比例原单位，不做业务转换');
    EXECUTE format('COMMENT ON COLUMN market.%I.captured_at IS %L', relation_name,'该内容首次实际抓取时间，不倒填');
    EXECUTE format('COMMENT ON COLUMN market.%I.last_observed_at IS %L', relation_name,'该内容最近实际观察时间');
    EXECUTE format('COMMENT ON COLUMN market.%I.batch_id IS %L', relation_name,'首次内容捕获的原生ingestion job身份');
    EXECUTE format('COMMENT ON COLUMN market.%I.source_record_key IS %L', relation_name,'UNKNOWN；不拼造稳定项目/事件身份');
    EXECUTE format('COMMENT ON COLUMN market.%I.published_at IS %L', relation_name,'上游未给可靠发布时间，保留NULL');
    EXECUTE format('COMMENT ON COLUMN market.%I.publication_time_quality IS %L', relation_name,'UNKNOWN，日期不能代替时刻');
    EXECUTE format('COMMENT ON COLUMN market.%I.vintage_quality IS %L', relation_name,'NON_VINTAGE，供应商当前重建历史');
  END LOOP;
END;
$migration$;
ALTER TABLE market.stock_seasoned_raw ADD COLUMN IF NOT EXISTS last_project_review_at timestamptz;
COMMENT ON COLUMN market.stock_seasoned_raw.last_project_review_at IS '已知代码+首次公告查询范围的实际月度复查时间，不是项目实体ID';
CREATE OR REPLACE VIEW market.stk_shock_raw_stats AS
  SELECT * FROM market.tushare_risk_event_raw WHERE source_api='stk_shock';
CREATE OR REPLACE VIEW market.stk_high_shock_raw_stats AS
  SELECT * FROM market.tushare_risk_event_raw WHERE source_api='stk_high_shock';
CREATE OR REPLACE VIEW market.stk_alert_raw_stats AS
  SELECT * FROM market.tushare_risk_event_raw WHERE source_api='stk_alert';

INSERT INTO market.data_stats_config(data_kind,table_name,date_column,updated_column,enabled,extra_info)
SELECT name,relation_name,'request_date','last_observed_at',TRUE,
  jsonb_build_object('desc',description,'source_api',name,'date_sequence','calendar',
    'cursor_source','refresh_audit','required_for_daily_complete',false,
    'coverage_basis','completed_provider_query_not_historical_PIT','vintage_quality','NON_VINTAGE')
FROM (VALUES
 ('stk_shock','market.stk_shock_raw_stats','个股异常波动原文'),
 ('stk_high_shock','market.stk_high_shock_raw_stats','个股严重异常波动原文'),
 ('stk_alert','market.stk_alert_raw_stats','个股重点提示原文'),
 ('idx_anns','market.index_announcements_raw','指数公司公告原文'),
 ('stk_seasoned','market.stock_seasoned_raw','增发进程完整原文及修订')
) AS inputs(name,relation_name,description)
ON CONFLICT(data_kind) DO UPDATE SET table_name=EXCLUDED.table_name,date_column=EXCLUDED.date_column,
  updated_column=EXCLUDED.updated_column,extra_info=EXCLUDED.extra_info;
INSERT INTO market.data_stats(data_kind,table_name,extra_info)
SELECT data_kind,table_name,extra_info FROM market.data_stats_config
WHERE data_kind IN ('stk_shock','stk_high_shock','stk_alert','idx_anns','stk_seasoned')
ON CONFLICT(data_kind) DO NOTHING;

-- Stage new defaults disabled until the restarted backend advertises the new
-- DatasetSpec catalogue. Enable these exact rows through the existing local
-- management API after source readback; never expose unknown names to an old
-- running integrity catalogue. Preserve existing operator customizations.
INSERT INTO market.ingestion_schedules(schedule_id,dataset,mode,frequency,enabled,options,created_at,updated_at)
SELECT id::uuid,name,'incremental',frequency,FALSE,options::jsonb,NOW(),NOW()
FROM (VALUES
 ('5d77f7a5-bbe6-4ec6-a151-cafc4e078e01','stk_shock','daily','{"at":"21:10","skip_auto_range":true}'),
 ('5d77f7a5-bbe6-4ec6-a151-cafc4e078e02','stk_high_shock','daily','{"at":"21:15","skip_auto_range":true}'),
 ('5d77f7a5-bbe6-4ec6-a151-cafc4e078e03','stk_alert','daily','{"at":"21:20","skip_auto_range":true}'),
 ('5d77f7a5-bbe6-4ec6-a151-cafc4e078e04','idx_anns','daily','{"at":"21:30","skip_auto_range":true}'),
 ('5d77f7a5-bbe6-4ec6-a151-cafc4e078e05','stk_seasoned','daily','{"at":"21:40","skip_auto_range":true}'),
 ('5d77f7a5-bbe6-4ec6-a151-cafc4e078e06','_stk_shock_morning','daily','{"at":"08:10","skip_auto_range":true}'),
 ('5d77f7a5-bbe6-4ec6-a151-cafc4e078e07','_stk_high_shock_morning','daily','{"at":"08:12","skip_auto_range":true}'),
 ('5d77f7a5-bbe6-4ec6-a151-cafc4e078e08','_stk_alert_morning','daily','{"at":"08:14","skip_auto_range":true}'),
 ('5d77f7a5-bbe6-4ec6-a151-cafc4e078e09','_idx_anns_morning','daily','{"at":"08:20","skip_auto_range":true}'),
 ('5d77f7a5-bbe6-4ec6-a151-cafc4e078e10','_stk_seasoned_morning','daily','{"at":"09:20","skip_auto_range":true}'),
 ('5d77f7a5-bbe6-4ec6-a151-cafc4e078e11','_stk_seasoned_project_review','1h','{"skip_auto_range":true}')
) AS defaults(id,name,frequency,options) ON CONFLICT(dataset,mode) DO NOTHING;
COMMIT;

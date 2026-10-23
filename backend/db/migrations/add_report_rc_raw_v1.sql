-- Source-only, append-only report_rc capture. No historical PIT availability claim.
CREATE TABLE IF NOT EXISTS market.tushare_report_rc_capture_v1 (
    capture_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    report_date DATE NOT NULL,
    fetch_params JSONB NOT NULL CHECK(jsonb_typeof(fetch_params)='object'),
    source_api TEXT NOT NULL DEFAULT 'report_rc' CHECK(source_api='report_rc'),
    source_rows_sha256 TEXT NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL,
    row_count INTEGER NOT NULL CHECK(row_count>=0),
    packet_sha256 TEXT NOT NULL,
    vintage_status TEXT NOT NULL DEFAULT 'NON_VINTAGE' CHECK(vintage_status='NON_VINTAGE')
);
CREATE TABLE IF NOT EXISTS market.tushare_report_rc_raw_v1 (
    capture_id TEXT NOT NULL REFERENCES market.tushare_report_rc_capture_v1(capture_id),
    row_ordinal INTEGER NOT NULL CHECK(row_ordinal>=0),
    ts_code TEXT,
    report_date DATE NOT NULL,
    org_name TEXT,
    quarter TEXT,
    report_title TEXT,
    source_row_sha256 TEXT NOT NULL,
    raw_payload JSONB NOT NULL CHECK(jsonb_typeof(raw_payload)='object'),
    PRIMARY KEY(capture_id,row_ordinal)
);
CREATE INDEX IF NOT EXISTS report_rc_raw_pair_v1 ON market.tushare_report_rc_raw_v1(ts_code,org_name,quarter,report_date);
COMMENT ON TABLE market.tushare_report_rc_capture_v1 IS 'Tushare report_rc真实抓取页；当前重建历史NON_VINTAGE，captured_at不是历史公开时间';
COMMENT ON TABLE market.tushare_report_rc_raw_v1 IS 'Tushare report_rc原始逐次出现；保留重复、冲突、不同预测期及晚修；EPS元/股，股本口径未知';
COMMENT ON COLUMN market.tushare_report_rc_raw_v1.quarter IS '供应商预测期原文，不得跨期去重';
COMMENT ON COLUMN market.tushare_report_rc_raw_v1.raw_payload IS '全部供应商字段，eps元/股、create_time为供应商更新时间；均不代表历史首次公开';
COMMENT ON COLUMN market.tushare_report_rc_capture_v1.captured_at IS '实际响应采集时间UTC，不倒填';
COMMENT ON COLUMN market.tushare_report_rc_capture_v1.capture_id IS '内部抓取页身份，不是供应商报告ID';
COMMENT ON COLUMN market.tushare_report_rc_capture_v1.batch_id IS '采集批次身份';
COMMENT ON COLUMN market.tushare_report_rc_capture_v1.report_date IS '请求的供应商报告自然日，不是可得时间';
COMMENT ON COLUMN market.tushare_report_rc_capture_v1.fetch_params IS '无密钥的源请求参数，含自然日及分页';
COMMENT ON COLUMN market.tushare_report_rc_capture_v1.source_api IS 'Tushare源接口report_rc';
COMMENT ON COLUMN market.tushare_report_rc_capture_v1.source_rows_sha256 IS '该页有序原始行规范JSON摘要';
COMMENT ON COLUMN market.tushare_report_rc_capture_v1.row_count IS '源端成功返回行数，零行也保留成功页';
COMMENT ON COLUMN market.tushare_report_rc_capture_v1.packet_sha256 IS '包含抓取时间和原始行的不可变页文件字节摘要';
COMMENT ON COLUMN market.tushare_report_rc_capture_v1.vintage_status IS '当前供应商重建历史NON_VINTAGE，不声称历史PIT';
COMMENT ON COLUMN market.tushare_report_rc_raw_v1.capture_id IS '抓取页外键；实际captured_at和批次通过此键追溯';
COMMENT ON COLUMN market.tushare_report_rc_raw_v1.row_ordinal IS '源页中的行位置，重复源行仍保留';
COMMENT ON COLUMN market.tushare_report_rc_raw_v1.ts_code IS '供应商原始证券代码，不按当前股票池裁切';
COMMENT ON COLUMN market.tushare_report_rc_raw_v1.report_date IS '供应商报告自然日，不等于收盘可得';
COMMENT ON COLUMN market.tushare_report_rc_raw_v1.org_name IS '机构原文，不静默合并别名';
COMMENT ON COLUMN market.tushare_report_rc_raw_v1.report_title IS '供应商原始报告标题';
COMMENT ON COLUMN market.tushare_report_rc_raw_v1.source_row_sha256 IS '原始行规范JSON摘要，不作为重复删除依据';

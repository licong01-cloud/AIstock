BEGIN;

CREATE TABLE IF NOT EXISTS market.etf_share_size (
    trade_date DATE NOT NULL,
    ts_code TEXT NOT NULL,
    total_share NUMERIC,
    total_size NUMERIC,
    nav NUMERIC,
    close NUMERIC,
    PRIMARY KEY (trade_date, ts_code)
);

COMMENT ON TABLE market.etf_share_size IS 'Tushare etf_share_size ETF daily shares and size';
COMMENT ON COLUMN market.etf_share_size.trade_date IS '交易日期';
COMMENT ON COLUMN market.etf_share_size.ts_code IS 'ETF Tushare 代码';
COMMENT ON COLUMN market.etf_share_size.total_share IS 'ETF 总份额（万份）';
COMMENT ON COLUMN market.etf_share_size.total_size IS 'ETF 总规模（万元）';
COMMENT ON COLUMN market.etf_share_size.nav IS '基金份额净值（元）';
COMMENT ON COLUMN market.etf_share_size.close IS '收盘价（元）';

SELECT create_hypertable(
    'market.etf_share_size',
    'trade_date',
    if_not_exists => TRUE
);

CREATE TABLE IF NOT EXISTS market.etf_basic_snapshots (
    snapshot_date DATE NOT NULL,
    ts_code TEXT NOT NULL,
    csname TEXT,
    extname TEXT,
    cname TEXT,
    index_code TEXT,
    index_name TEXT,
    setup_date DATE,
    list_date DATE,
    list_status TEXT,
    exchange TEXT,
    mgr_name TEXT,
    custod_name TEXT,
    mgt_fee NUMERIC,
    etf_type TEXT,
    PRIMARY KEY (snapshot_date, ts_code)
);

COMMENT ON TABLE market.etf_basic_snapshots IS 'Tushare etf_basic daily observation snapshots from integration date';
COMMENT ON COLUMN market.etf_basic_snapshots.snapshot_date IS '本地观察日期，不代表历史生效日期';
COMMENT ON COLUMN market.etf_basic_snapshots.ts_code IS 'ETF Tushare 代码';
COMMENT ON COLUMN market.etf_basic_snapshots.csname IS 'ETF 中文简称';
COMMENT ON COLUMN market.etf_basic_snapshots.extname IS 'ETF 扩位简称';
COMMENT ON COLUMN market.etf_basic_snapshots.cname IS '基金中文全称';
COMMENT ON COLUMN market.etf_basic_snapshots.index_code IS '当前观察到的基准指数代码';
COMMENT ON COLUMN market.etf_basic_snapshots.index_name IS '当前观察到的基准指数名称';
COMMENT ON COLUMN market.etf_basic_snapshots.setup_date IS '基金设立日期';
COMMENT ON COLUMN market.etf_basic_snapshots.list_date IS '上市日期';
COMMENT ON COLUMN market.etf_basic_snapshots.list_status IS '观察日上市状态（L/D/P）';
COMMENT ON COLUMN market.etf_basic_snapshots.exchange IS '交易所';
COMMENT ON COLUMN market.etf_basic_snapshots.mgr_name IS '基金管理人简称';
COMMENT ON COLUMN market.etf_basic_snapshots.custod_name IS '基金托管人名称';
COMMENT ON COLUMN market.etf_basic_snapshots.mgt_fee IS '基金管理费率';
COMMENT ON COLUMN market.etf_basic_snapshots.etf_type IS 'ETF 投资通道类型';

SELECT create_hypertable(
    'market.etf_basic_snapshots',
    'snapshot_date',
    if_not_exists => TRUE
);

INSERT INTO market.data_stats_config
    (data_kind, table_name, date_column, updated_column, enabled, extra_info)
VALUES
    (
        'etf_share_size',
        'market.etf_share_size',
        'trade_date',
        NULL,
        TRUE,
        jsonb_build_object(
            'desc', 'Tushare ETF 每日份额和规模',
            'source_api', 'etf_share_size',
            'date_sequence', 'trading',
            'cursor_source', 'refresh_audit',
            'availability', 'next_trading_day_for_research'
        )
    ),
    (
        'etf_basic_snapshots',
        'market.etf_basic_snapshots',
        'snapshot_date',
        NULL,
        TRUE,
        jsonb_build_object(
            'desc', 'Tushare ETF 基础信息每日观察快照',
            'source_api', 'etf_basic',
            'date_sequence', 'calendar',
            'historical_backfill', false
        )
    )
ON CONFLICT (data_kind) DO UPDATE
    SET table_name = EXCLUDED.table_name,
        date_column = EXCLUDED.date_column,
        updated_column = EXCLUDED.updated_column,
        enabled = EXCLUDED.enabled,
        extra_info = EXCLUDED.extra_info;

COMMIT;

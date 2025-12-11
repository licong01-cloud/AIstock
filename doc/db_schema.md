# 数据库结构文档

- 生成时间: 2025-12-11 23:12:58 中国标准时间
- 数据库名: aistock

> 本文档由 scripts/export_db_schema_docs.py 自动生成，请勿手工编辑。

## Schema `market`

### Table `market.adj_factor`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `ts_code` | `text` | NO |  | YES |  |
| 2 | `trade_date` | `date` | NO |  | YES |  |
| 3 | `adj_factor` | `double precision` | NO |  |  |  |

**主键约束**

- 名称: `adj_factor_pkey`
- 字段: `ts_code`, `trade_date`

---

### Table `market.anns`

> Tushare anns_d 上市公司公告（含本地PDF元数据）

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `id` | `bigint` | NO | `nextval('market.anns_id_seq'::regclass)` | YES | 本地自增ID |
| 2 | `ann_date` | `date` | NO |  |  | 公告日期，对应Tushare anns_d中的ann_date (YYYYMMDD) |
| 3 | `ts_code` | `text` | NO |  |  | TS代码，例如 600000.SH |
| 4 | `name` | `text` | NO |  |  | 股票名称，对应Tushare anns_d中的name |
| 5 | `title` | `text` | NO |  |  | 公告标题，对应Tushare anns_d中的title |
| 6 | `url` | `text` | NO |  |  | 公告原文URL，对应Tushare anns_d中的url（可能为PDF或HTML链接） |
| 7 | `rec_time` | `timestamp with time zone` | YES |  |  | 公告接收或发布时间，对应Tushare anns_d中的rec_time |
| 8 | `local_path` | `text` | YES |  |  | 公告本地相对存储路径，相对于 ANNOUNCE_PDF_ROOT，例如 2025-01-15/600000.SH_123.pdf |
| 9 | `file_ext` | `text` | YES |  |  | 本地文件扩展名，如 pdf、html |
| 10 | `file_size` | `bigint` | YES |  |  | 本地文件大小，单位字节 |
| 11 | `file_hash` | `text` | YES |  |  | 本地文件内容哈希值，例如md5/sha256，用于校验 |
| 12 | `download_status` | `text` | NO | `'pending'::text` |  | PDF下载状态：pending/success/failed |
| 13 | `created_at` | `timestamp with time zone` | NO | `now()` |  | 记录创建时间 |
| 14 | `updated_at` | `timestamp with time zone` | NO | `now()` |  | 记录最近更新时间 |

**主键约束**

- 名称: `anns_pkey`
- 字段: `id`

---

### Table `market.bak_basic`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `trade_date` | `date` | NO |  | YES |  |
| 2 | `ts_code` | `text` | NO |  | YES |  |
| 3 | `name` | `text` | YES |  |  |  |
| 4 | `industry` | `text` | YES |  |  |  |
| 5 | `area` | `text` | YES |  |  |  |
| 6 | `pe` | `numeric` | YES |  |  |  |
| 7 | `pb` | `numeric` | YES |  |  |  |
| 8 | `total_share` | `numeric` | YES |  |  |  |
| 9 | `float_share` | `numeric` | YES |  |  |  |
| 10 | `free_share` | `numeric` | YES |  |  |  |
| 11 | `total_mv` | `numeric` | YES |  |  |  |
| 12 | `circ_mv` | `numeric` | YES |  |  |  |

**主键约束**

- 名称: `bak_basic_pkey`
- 字段: `trade_date`, `ts_code`

---

### Table `market.daily_basic`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `trade_date` | `date` | NO |  | YES |  |
| 2 | `ts_code` | `text` | NO |  | YES |  |
| 3 | `close` | `numeric` | YES |  |  |  |
| 4 | `turnover_rate` | `numeric` | YES |  |  |  |
| 5 | `turnover_rate_f` | `numeric` | YES |  |  |  |
| 6 | `volume_ratio` | `numeric` | YES |  |  |  |
| 7 | `pe` | `numeric` | YES |  |  |  |
| 8 | `pe_ttm` | `numeric` | YES |  |  |  |
| 9 | `pb` | `numeric` | YES |  |  |  |
| 10 | `ps` | `numeric` | YES |  |  |  |
| 11 | `ps_ttm` | `numeric` | YES |  |  |  |
| 12 | `dv_ratio` | `numeric` | YES |  |  |  |
| 13 | `dv_ttm` | `numeric` | YES |  |  |  |
| 14 | `total_share` | `numeric` | YES |  |  |  |
| 15 | `float_share` | `numeric` | YES |  |  |  |
| 16 | `free_share` | `numeric` | YES |  |  |  |
| 17 | `total_mv` | `numeric` | YES |  |  |  |
| 18 | `circ_mv` | `numeric` | YES |  |  |  |

**主键约束**

- 名称: `daily_basic_pkey`
- 字段: `trade_date`, `ts_code`

---

### Table `market.data_stats`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `id` | `bigint` | NO | `nextval('market.data_stats_id_seq'::regclass)` | YES |  |
| 2 | `data_kind` | `text` | NO |  |  |  |
| 3 | `table_name` | `text` | NO |  |  |  |
| 4 | `min_date` | `date` | YES |  |  |  |
| 5 | `max_date` | `date` | YES |  |  |  |
| 6 | `row_count` | `bigint` | YES |  |  |  |
| 7 | `table_bytes` | `bigint` | YES |  |  |  |
| 8 | `index_bytes` | `bigint` | YES |  |  |  |
| 9 | `last_updated_at` | `timestamp with time zone` | YES |  |  |  |
| 10 | `stat_generated_at` | `timestamp with time zone` | NO | `now()` |  |  |
| 11 | `extra_info` | `jsonb` | YES |  |  |  |
| 12 | `last_check_result` | `jsonb` | YES |  |  |  |
| 13 | `last_check_at` | `timestamp with time zone` | YES |  |  |  |

**主键约束**

- 名称: `data_stats_pkey`
- 字段: `id`

---

### Table `market.data_stats_config`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `data_kind` | `text` | NO |  | YES |  |
| 2 | `table_name` | `text` | NO |  |  |  |
| 3 | `date_column` | `text` | NO |  |  |  |
| 4 | `updated_column` | `text` | YES |  |  |  |
| 5 | `enabled` | `boolean` | NO | `true` |  |  |
| 6 | `extra_info` | `jsonb` | YES |  |  |  |

**主键约束**

- 名称: `data_stats_config_pkey`
- 字段: `data_kind`

---

### Table `market.hotboard_config`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `id` | `smallint` | NO | `1` | YES |  |
| 2 | `enabled` | `boolean` | NO | `true` |  |  |
| 3 | `frequency_seconds` | `integer` | NO | `5` |  |  |
| 4 | `trading_windows` | `jsonb` | YES |  |  |  |
| 5 | `last_run_at` | `timestamp with time zone` | YES |  |  |  |
| 6 | `updated_at` | `timestamp with time zone` | YES | `now()` |  |  |

**主键约束**

- 名称: `hotboard_config_pkey`
- 字段: `id`

---

### Table `market.index_basic`

> Tushare index_basic 指数基础信息

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `ts_code` | `character varying(32)` | NO |  | YES | TS指数代码 |
| 2 | `name` | `character varying(100)` | YES |  |  | 指数简称 |
| 3 | `fullname` | `character varying(200)` | YES |  |  | 指数全称 |
| 4 | `market` | `character varying(32)` | YES |  |  | 市场（如沪深等） |
| 5 | `publisher` | `character varying(64)` | YES |  |  | 发布方 |
| 6 | `index_type` | `character varying(64)` | YES |  |  | 指数风格（规模/行业/策略等） |
| 7 | `category` | `character varying(64)` | YES |  |  | 指数类别 |
| 8 | `base_date` | `date` | YES |  |  | 基期日期 |
| 9 | `base_point` | `numeric(20,4)` | YES |  |  | 基点 |
| 10 | `list_date` | `date` | YES |  |  | 发布日期 |
| 11 | `weight_rule` | `character varying(128)` | YES |  |  | 加权方式 |
| 12 | `desc` | `text` | YES |  |  | 指数简介 |
| 13 | `exp_date` | `date` | YES |  |  | 终止日期 |

**主键约束**

- 名称: `index_basic_pkey`
- 字段: `ts_code`

---

### Table `market.index_daily`

> Tushare index_daily 指数日线行情

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `ts_code` | `character varying(32)` | NO |  | YES | TS 指数代码 |
| 2 | `trade_date` | `date` | NO |  | YES | 交易日 |
| 3 | `close` | `numeric(20,4)` | YES |  |  | 收盘点位 |
| 4 | `open` | `numeric(20,4)` | YES |  |  | 开盘点位 |
| 5 | `high` | `numeric(20,4)` | YES |  |  | 最高点位 |
| 6 | `low` | `numeric(20,4)` | YES |  |  | 最低点位 |
| 7 | `pre_close` | `numeric(20,4)` | YES |  |  | 昨日收盘点 |
| 8 | `change` | `numeric(20,4)` | YES |  |  | 涨跌点 |
| 9 | `pct_chg` | `numeric(20,4)` | YES |  |  | 涨跌幅(%) |
| 10 | `vol` | `numeric(24,4)` | YES |  |  | 成交量(手) |
| 11 | `amount` | `numeric(24,4)` | YES |  |  | 成交额(千元) |

**主键约束**

- 名称: `index_daily_pkey`
- 字段: `ts_code`, `trade_date`

---

### Table `market.index_daily_tdx`

> TDX 指数日线原始数据（未复权，价格/金额单位：厘，成交量单位：手）

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `trade_date` | `date` | NO |  | YES | 交易日期，对应 TDX 返回 Time 字段的本地日期 |
| 2 | `index_code` | `text` | NO |  | YES | 指数代码（TDX 风格，例如 sh000300、sz399001），由请求参数填入 |
| 3 | `open_li` | `bigint` | NO |  |  | 开盘价，单位：厘（1 元 = 1000 厘），对应 Open |
| 4 | `high_li` | `bigint` | NO |  |  | 最高价，单位：厘，对应 High |
| 5 | `low_li` | `bigint` | NO |  |  | 最低价，单位：厘，对应 Low |
| 6 | `close_li` | `bigint` | NO |  |  | 收盘价，单位：厘，对应 Close |
| 7 | `last_close_li` | `bigint` | NO |  |  | 昨收价，单位：厘，对应 Last |
| 8 | `volume_hand` | `bigint` | NO |  |  | 成交量，单位：手（1 手 = 100 股），对应 Volume |
| 9 | `amount_li` | `bigint` | NO |  |  | 成交额，单位：厘，对应 Amount |
| 10 | `source` | `text` | NO | `'tdx'::text` |  | 数据来源标识，例如 tdx、ths 等 |
| 11 | `created_at` | `timestamp with time zone` | NO | `now()` |  | 入库时间戳 |

**主键约束**

- 名称: `index_daily_tdx_pkey`
- 字段: `index_code`, `trade_date`

---

### Table `market.index_kline_daily_qfq`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `trade_date` | `date` | NO |  | YES |  |
| 2 | `code` | `character varying(16)` | NO |  | YES |  |
| 3 | `open_li` | `integer` | NO |  |  |  |
| 4 | `high_li` | `integer` | NO |  |  |  |
| 5 | `low_li` | `integer` | NO |  |  |  |
| 6 | `close_li` | `integer` | NO |  |  |  |
| 7 | `volume_hand` | `bigint` | YES |  |  |  |
| 8 | `amount_li` | `bigint` | YES |  |  |  |
| 9 | `up_count` | `integer` | YES |  |  |  |
| 10 | `down_count` | `integer` | YES |  |  |  |
| 11 | `adjust_type` | `character(3)` | NO | `'qfq'::bpchar` |  |  |
| 12 | `source` | `character varying(16)` | NO |  |  |  |

**主键约束**

- 名称: `index_kline_daily_qfq_pkey`
- 字段: `code`, `trade_date`

---

### Table `market.ingestion_checkpoints`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `run_id` | `uuid` | NO |  | YES |  |
| 2 | `dataset` | `character varying(64)` | NO |  | YES |  |
| 3 | `ts_code` | `character(9)` | NO |  | YES |  |
| 4 | `cursor_date` | `date` | YES |  |  |  |
| 5 | `cursor_time` | `timestamp with time zone` | YES |  |  |  |
| 6 | `extra` | `jsonb` | YES |  |  |  |

**主键约束**

- 名称: `ingestion_checkpoints_pkey`
- 字段: `run_id`, `dataset`, `ts_code`

**外键约束**

- `ingestion_checkpoints_run_id_fkey`: (`run_id`) → `market.ingestion_runs` (`run_id`)

---

### Table `market.ingestion_errors`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `error_id` | `bigint` | NO | `nextval('market.ingestion_errors_error_id_seq'::regclass)` | YES |  |
| 2 | `run_id` | `uuid` | YES |  |  |  |
| 3 | `dataset` | `character varying(64)` | YES |  |  |  |
| 4 | `ts_code` | `character(9)` | YES |  |  |  |
| 5 | `error_at` | `timestamp with time zone` | YES | `now()` |  |  |
| 6 | `message` | `text` | YES |  |  |  |
| 7 | `detail` | `jsonb` | YES |  |  |  |

**主键约束**

- 名称: `ingestion_errors_pkey`
- 字段: `error_id`

**外键约束**

- `ingestion_errors_run_id_fkey`: (`run_id`) → `market.ingestion_runs` (`run_id`)

---

### Table `market.ingestion_job_tasks`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `task_id` | `uuid` | NO |  | YES |  |
| 2 | `job_id` | `uuid` | NO |  |  |  |
| 3 | `dataset` | `character varying(64)` | NO |  |  |  |
| 4 | `ts_code` | `character(9)` | YES |  |  |  |
| 5 | `date_from` | `date` | YES |  |  |  |
| 6 | `date_to` | `date` | YES |  |  |  |
| 7 | `status` | `character varying(16)` | NO |  |  |  |
| 8 | `progress` | `numeric(5,2)` | YES | `0` |  |  |
| 9 | `retries` | `integer` | YES | `0` |  |  |
| 10 | `last_error` | `text` | YES |  |  |  |
| 11 | `updated_at` | `timestamp with time zone` | YES | `now()` |  |  |

**主键约束**

- 名称: `ingestion_job_tasks_pkey`
- 字段: `task_id`

**外键约束**

- `ingestion_job_tasks_job_id_fkey`: (`job_id`) → `market.ingestion_jobs` (`job_id`)

---

### Table `market.ingestion_jobs`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `job_id` | `uuid` | NO |  | YES |  |
| 2 | `job_type` | `character varying(16)` | NO |  |  |  |
| 3 | `status` | `character varying(16)` | NO |  |  |  |
| 4 | `created_at` | `timestamp with time zone` | YES | `now()` |  |  |
| 5 | `started_at` | `timestamp with time zone` | YES |  |  |  |
| 6 | `finished_at` | `timestamp with time zone` | YES |  |  |  |
| 7 | `summary` | `jsonb` | YES |  |  |  |

**主键约束**

- 名称: `ingestion_jobs_pkey`
- 字段: `job_id`

---

### Table `market.ingestion_logs`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `job_id` | `uuid` | NO |  |  |  |
| 2 | `ts` | `timestamp with time zone` | YES | `now()` |  |  |
| 3 | `level` | `character varying(8)` | NO |  |  |  |
| 4 | `message` | `text` | YES |  |  |  |

---

### Table `market.ingestion_runs`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `run_id` | `uuid` | NO |  | YES |  |
| 2 | `mode` | `character varying(16)` | NO |  |  |  |
| 3 | `dataset` | `character varying(64)` | YES |  |  |  |
| 4 | `status` | `character varying(16)` | NO |  |  |  |
| 5 | `created_at` | `timestamp with time zone` | YES | `now()` |  |  |
| 6 | `started_at` | `timestamp with time zone` | YES |  |  |  |
| 7 | `finished_at` | `timestamp with time zone` | YES |  |  |  |
| 8 | `params` | `jsonb` | YES |  |  |  |
| 9 | `summary` | `jsonb` | YES |  |  |  |

**主键约束**

- 名称: `ingestion_runs_pkey`
- 字段: `run_id`

---

### Table `market.ingestion_schedules`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `schedule_id` | `uuid` | NO |  | YES |  |
| 2 | `dataset` | `character varying(64)` | NO |  |  |  |
| 3 | `mode` | `character varying(16)` | NO |  |  |  |
| 4 | `frequency` | `text` | NO |  |  |  |
| 5 | `enabled` | `boolean` | NO | `true` |  |  |
| 6 | `options` | `jsonb` | YES |  |  |  |
| 7 | `last_run_at` | `timestamp with time zone` | YES |  |  |  |
| 8 | `next_run_at` | `timestamp with time zone` | YES |  |  |  |
| 9 | `last_status` | `character varying(16)` | YES |  |  |  |
| 10 | `last_error` | `text` | YES |  |  |  |
| 11 | `created_at` | `timestamp with time zone` | YES | `now()` |  |  |
| 12 | `updated_at` | `timestamp with time zone` | YES | `now()` |  |  |

**主键约束**

- 名称: `ingestion_schedules_pkey`
- 字段: `schedule_id`

---

### Table `market.ingestion_state`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `dataset` | `character varying(64)` | NO |  | YES |  |
| 2 | `ts_code` | `character(9)` | NO |  | YES |  |
| 3 | `last_success_date` | `date` | YES |  |  |  |
| 4 | `last_success_time` | `timestamp with time zone` | YES |  |  |  |
| 5 | `extra` | `jsonb` | YES |  |  |  |

**主键约束**

- 名称: `ingestion_state_pkey`
- 字段: `dataset`, `ts_code`

---

### View `market.kline_15m`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `ts_code` | `character(9)` | YES |  |  |  |
| 2 | `bucket` | `timestamp with time zone` | YES |  |  |  |
| 3 | `freq` | `character varying(8)` | YES |  |  |  |
| 4 | `open_li` | `integer` | YES |  |  |  |
| 5 | `high_li` | `integer` | YES |  |  |  |
| 6 | `low_li` | `integer` | YES |  |  |  |
| 7 | `close_li` | `integer` | YES |  |  |  |
| 8 | `volume_hand` | `numeric` | YES |  |  |  |
| 9 | `amount_li` | `numeric` | YES |  |  |  |

---

### View `market.kline_5m`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `ts_code` | `character(9)` | YES |  |  |  |
| 2 | `bucket` | `timestamp with time zone` | YES |  |  |  |
| 3 | `freq` | `character varying(8)` | YES |  |  |  |
| 4 | `open_li` | `integer` | YES |  |  |  |
| 5 | `high_li` | `integer` | YES |  |  |  |
| 6 | `low_li` | `integer` | YES |  |  |  |
| 7 | `close_li` | `integer` | YES |  |  |  |
| 8 | `volume_hand` | `numeric` | YES |  |  |  |
| 9 | `amount_li` | `numeric` | YES |  |  |  |

---

### View `market.kline_60m`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `ts_code` | `character(9)` | YES |  |  |  |
| 2 | `bucket` | `timestamp with time zone` | YES |  |  |  |
| 3 | `freq` | `character varying(8)` | YES |  |  |  |
| 4 | `open_li` | `integer` | YES |  |  |  |
| 5 | `high_li` | `integer` | YES |  |  |  |
| 6 | `low_li` | `integer` | YES |  |  |  |
| 7 | `close_li` | `integer` | YES |  |  |  |
| 8 | `volume_hand` | `numeric` | YES |  |  |  |
| 9 | `amount_li` | `numeric` | YES |  |  |  |

---

### Table `market.kline_daily_hfq`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `trade_date` | `date` | NO |  | YES |  |
| 2 | `ts_code` | `character(9)` | NO |  | YES |  |
| 3 | `open_li` | `integer` | NO |  |  |  |
| 4 | `high_li` | `integer` | NO |  |  |  |
| 5 | `low_li` | `integer` | NO |  |  |  |
| 6 | `close_li` | `integer` | NO |  |  |  |
| 7 | `volume_hand` | `bigint` | NO |  |  |  |
| 8 | `amount_li` | `bigint` | NO |  |  |  |
| 9 | `adjust_type` | `character(3)` | NO | `'hfq'::bpchar` |  |  |
| 10 | `source` | `character varying(16)` | NO |  |  |  |

**主键约束**

- 名称: `kline_daily_hfq_pkey`
- 字段: `ts_code`, `trade_date`

---

### Table `market.kline_daily_qfq`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `trade_date` | `date` | NO |  | YES |  |
| 2 | `ts_code` | `character(9)` | NO |  | YES |  |
| 3 | `open_li` | `integer` | NO |  |  |  |
| 4 | `high_li` | `integer` | NO |  |  |  |
| 5 | `low_li` | `integer` | NO |  |  |  |
| 6 | `close_li` | `integer` | NO |  |  |  |
| 7 | `volume_hand` | `bigint` | NO |  |  |  |
| 8 | `amount_li` | `bigint` | NO |  |  |  |
| 9 | `adjust_type` | `character(3)` | NO | `'qfq'::bpchar` |  |  |
| 10 | `source` | `character varying(16)` | NO |  |  |  |

**主键约束**

- 名称: `kline_daily_qfq_pkey`
- 字段: `ts_code`, `trade_date`

---

### Table `market.kline_daily_raw`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `trade_date` | `date` | NO |  | YES |  |
| 2 | `ts_code` | `character(9)` | NO |  | YES |  |
| 3 | `open_li` | `integer` | NO |  |  |  |
| 4 | `high_li` | `integer` | NO |  |  |  |
| 5 | `low_li` | `integer` | NO |  |  |  |
| 6 | `close_li` | `integer` | NO |  |  |  |
| 7 | `volume_hand` | `bigint` | NO |  |  |  |
| 8 | `amount_li` | `bigint` | NO |  |  |  |
| 9 | `adjust_type` | `character(4)` | NO | `'none'::bpchar` |  |  |
| 10 | `source` | `character varying(16)` | NO |  |  |  |

**主键约束**

- 名称: `kline_daily_raw_pkey`
- 字段: `ts_code`, `trade_date`

---

### Table `market.kline_minute_raw`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `trade_time` | `timestamp with time zone` | NO |  | YES |  |
| 2 | `ts_code` | `character(9)` | NO |  | YES |  |
| 3 | `freq` | `character varying(8)` | NO |  | YES |  |
| 4 | `open_li` | `integer` | YES |  |  |  |
| 5 | `high_li` | `integer` | YES |  |  |  |
| 6 | `low_li` | `integer` | YES |  |  |  |
| 7 | `close_li` | `integer` | NO |  |  |  |
| 8 | `volume_hand` | `bigint` | YES |  |  |  |
| 9 | `amount_li` | `bigint` | YES |  |  |  |
| 10 | `adjust_type` | `character(4)` | NO | `'none'::bpchar` |  |  |
| 11 | `source` | `character varying(16)` | NO |  |  |  |

**主键约束**

- 名称: `kline_minute_raw_pkey`
- 字段: `ts_code`, `trade_time`, `freq`

---

### Table `market.kline_monthly_qfq`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `month_end_date` | `date` | NO |  | YES |  |
| 2 | `ts_code` | `character(9)` | NO |  | YES |  |
| 3 | `open_li` | `integer` | NO |  |  |  |
| 4 | `high_li` | `integer` | NO |  |  |  |
| 5 | `low_li` | `integer` | NO |  |  |  |
| 6 | `close_li` | `integer` | NO |  |  |  |
| 7 | `volume_hand` | `bigint` | NO |  |  |  |
| 8 | `amount_li` | `bigint` | NO |  |  |  |
| 9 | `adjust_type` | `character(3)` | NO | `'qfq'::bpchar` |  |  |
| 10 | `source` | `character varying(16)` | NO |  |  |  |

**主键约束**

- 名称: `kline_monthly_qfq_pkey`
- 字段: `ts_code`, `month_end_date`

---

### Table `market.kline_weekly_qfq`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `week_end_date` | `date` | NO |  | YES |  |
| 2 | `ts_code` | `character(9)` | NO |  | YES |  |
| 3 | `open_li` | `integer` | NO |  |  |  |
| 4 | `high_li` | `integer` | NO |  |  |  |
| 5 | `low_li` | `integer` | NO |  |  |  |
| 6 | `close_li` | `integer` | NO |  |  |  |
| 7 | `volume_hand` | `bigint` | NO |  |  |  |
| 8 | `amount_li` | `bigint` | NO |  |  |  |
| 9 | `adjust_type` | `character(3)` | NO | `'qfq'::bpchar` |  |  |
| 10 | `source` | `character varying(16)` | NO |  |  |  |

**主键约束**

- 名称: `kline_weekly_qfq_pkey`
- 字段: `ts_code`, `week_end_date`

---

### Table `market.market_stats_snapshot`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `snapshot_time` | `timestamp with time zone` | NO |  | YES |  |
| 2 | `stats` | `jsonb` | NO |  |  |  |

**主键约束**

- 名称: `market_stats_snapshot_pkey`
- 字段: `snapshot_time`

---

### Table `market.moneyflow_ind_dc`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `trade_date` | `date` | NO |  | YES |  |
| 2 | `ts_code` | `character(9)` | NO |  | YES |  |
| 3 | `buy_elg_vol` | `numeric(20,4)` | YES |  |  |  |
| 4 | `buy_elg_amount` | `numeric(28,2)` | YES |  |  |  |
| 5 | `sell_elg_vol` | `numeric(20,4)` | YES |  |  |  |
| 6 | `sell_elg_amount` | `numeric(28,2)` | YES |  |  |  |
| 7 | `net_elg_amount` | `numeric(28,2)` | YES |  |  |  |
| 8 | `buy_lg_vol` | `numeric(20,4)` | YES |  |  |  |
| 9 | `buy_lg_amount` | `numeric(28,2)` | YES |  |  |  |
| 10 | `sell_lg_vol` | `numeric(20,4)` | YES |  |  |  |
| 11 | `sell_lg_amount` | `numeric(28,2)` | YES |  |  |  |
| 12 | `net_lg_amount` | `numeric(28,2)` | YES |  |  |  |
| 13 | `buy_md_vol` | `numeric(20,4)` | YES |  |  |  |
| 14 | `buy_md_amount` | `numeric(28,2)` | YES |  |  |  |
| 15 | `sell_md_vol` | `numeric(20,4)` | YES |  |  |  |
| 16 | `sell_md_amount` | `numeric(28,2)` | YES |  |  |  |
| 17 | `net_md_amount` | `numeric(28,2)` | YES |  |  |  |
| 18 | `buy_sm_vol` | `numeric(20,4)` | YES |  |  |  |
| 19 | `buy_sm_amount` | `numeric(28,2)` | YES |  |  |  |
| 20 | `sell_sm_vol` | `numeric(20,4)` | YES |  |  |  |
| 21 | `sell_sm_amount` | `numeric(28,2)` | YES |  |  |  |
| 22 | `net_sm_amount` | `numeric(28,2)` | YES |  |  |  |
| 23 | `total_value_traded` | `numeric(28,2)` | YES |  |  |  |

**主键约束**

- 名称: `moneyflow_ind_dc_pkey`
- 字段: `ts_code`, `trade_date`

---

### Table `market.moneyflow_ts`

> Tushare moneyflow 个股资金流（按交易日）

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `trade_date` | `date` | NO |  | YES | 交易日期 |
| 2 | `ts_code` | `text` | NO |  | YES | TS代码 |
| 3 | `buy_sm_vol` | `numeric` | YES |  |  | 小单买入量（手） |
| 4 | `buy_sm_amount` | `numeric` | YES |  |  | 小单买入金额（万元） |
| 5 | `sell_sm_vol` | `numeric` | YES |  |  | 小单卖出量（手） |
| 6 | `sell_sm_amount` | `numeric` | YES |  |  | 小单卖出金额（万元） |
| 7 | `buy_md_vol` | `numeric` | YES |  |  | 中单买入量（手） |
| 8 | `buy_md_amount` | `numeric` | YES |  |  | 中单买入金额（万元） |
| 9 | `sell_md_vol` | `numeric` | YES |  |  | 中单卖出量（手） |
| 10 | `sell_md_amount` | `numeric` | YES |  |  | 中单卖出金额（万元） |
| 11 | `buy_lg_vol` | `numeric` | YES |  |  | 大单买入量（手） |
| 12 | `buy_lg_amount` | `numeric` | YES |  |  | 大单买入金额（万元） |
| 13 | `sell_lg_vol` | `numeric` | YES |  |  | 大单卖出量（手） |
| 14 | `sell_lg_amount` | `numeric` | YES |  |  | 大单卖出金额（万元） |
| 15 | `buy_elg_vol` | `numeric` | YES |  |  | 特大单买入量（手） |
| 16 | `buy_elg_amount` | `numeric` | YES |  |  | 特大单买入金额（万元） |
| 17 | `sell_elg_vol` | `numeric` | YES |  |  | 特大单卖出量（手） |
| 18 | `sell_elg_amount` | `numeric` | YES |  |  | 特大单卖出金额（万元） |
| 19 | `net_mf_vol` | `numeric` | YES |  |  | 净流入量（手） |
| 20 | `net_mf_amount` | `numeric` | YES |  |  | 净流入额（万元） |

**主键约束**

- 名称: `moneyflow_ts_pkey`
- 字段: `trade_date`, `ts_code`

---

### Table `market.qlib_export_meta`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `snapshot_id` | `text` | NO |  | YES |  |
| 2 | `data_type` | `text` | NO |  | YES |  |
| 3 | `last_datetime` | `timestamp with time zone` | NO |  |  |  |
| 4 | `updated_at` | `timestamp with time zone` | YES | `now()` |  |  |

**主键约束**

- 名称: `qlib_export_meta_pkey`
- 字段: `snapshot_id`, `data_type`

---

### Table `market.quote_snapshot`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `snapshot_time` | `timestamp with time zone` | NO |  | YES |  |
| 2 | `ts_code` | `character(9)` | NO |  | YES |  |
| 3 | `last_li` | `integer` | YES |  |  |  |
| 4 | `open_li` | `integer` | YES |  |  |  |
| 5 | `high_li` | `integer` | YES |  |  |  |
| 6 | `low_li` | `integer` | YES |  |  |  |
| 7 | `close_li` | `integer` | YES |  |  |  |
| 8 | `total_hand` | `bigint` | YES |  |  |  |
| 9 | `amount_li` | `bigint` | YES |  |  |  |
| 10 | `inside_dish` | `bigint` | YES |  |  |  |
| 11 | `outer_disc` | `bigint` | YES |  |  |  |
| 12 | `intuition` | `bigint` | YES |  |  |  |
| 13 | `server_time_ts` | `timestamp with time zone` | YES |  |  |  |
| 14 | `buy_levels` | `jsonb` | YES |  |  |  |
| 15 | `sell_levels` | `jsonb` | YES |  |  |  |

**主键约束**

- 名称: `quote_snapshot_pkey`
- 字段: `ts_code`, `snapshot_time`

---

### Table `market.sina_board_daily`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `trade_date` | `date` | NO |  | YES |  |
| 2 | `cate_type` | `smallint` | NO |  | YES |  |
| 3 | `board_code` | `text` | NO |  | YES |  |
| 4 | `board_name` | `text` | YES |  |  |  |
| 5 | `pct_chg` | `numeric(10,4)` | YES |  |  |  |
| 6 | `amount` | `numeric(28,2)` | YES |  |  |  |
| 7 | `net_inflow` | `numeric(28,2)` | YES |  |  |  |
| 8 | `turnover` | `numeric(18,4)` | YES |  |  |  |
| 9 | `ratioamount` | `numeric(18,6)` | YES |  |  |  |
| 10 | `meta` | `jsonb` | YES |  |  |  |

**主键约束**

- 名称: `sina_board_daily_pkey`
- 字段: `trade_date`, `cate_type`, `board_code`

---

### Table `market.sina_board_intraday`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `ts` | `timestamp with time zone` | NO |  | YES |  |
| 2 | `cate_type` | `smallint` | NO |  | YES |  |
| 3 | `board_code` | `text` | NO |  | YES |  |
| 4 | `board_name` | `text` | YES |  |  |  |
| 5 | `pct_chg` | `numeric(10,4)` | YES |  |  |  |
| 6 | `amount` | `numeric(28,2)` | YES |  |  |  |
| 7 | `net_inflow` | `numeric(28,2)` | YES |  |  |  |
| 8 | `turnover` | `numeric(18,4)` | YES |  |  |  |
| 9 | `ratioamount` | `numeric(18,6)` | YES |  |  |  |
| 10 | `meta` | `jsonb` | YES |  |  |  |

**主键约束**

- 名称: `sina_board_intraday_pkey`
- 字段: `ts`, `cate_type`, `board_code`

---

### Table `market.stock_basic`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `ts_code` | `text` | NO |  | YES |  |
| 2 | `symbol` | `text` | YES |  |  |  |
| 3 | `name` | `text` | YES |  |  |  |
| 4 | `area` | `text` | YES |  |  |  |
| 5 | `industry` | `text` | YES |  |  |  |
| 6 | `fullname` | `text` | YES |  |  |  |
| 7 | `enname` | `text` | YES |  |  |  |
| 8 | `market` | `text` | YES |  |  |  |
| 9 | `exchange` | `text` | YES |  |  |  |
| 10 | `curr_type` | `text` | YES |  |  |  |
| 11 | `list_status` | `text` | YES |  |  |  |
| 12 | `list_date` | `date` | YES |  |  |  |
| 13 | `delist_date` | `date` | YES |  |  |  |
| 14 | `is_hs` | `text` | YES |  |  |  |

**主键约束**

- 名称: `stock_basic_pkey`
- 字段: `ts_code`

---

### Table `market.stock_info`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `ts_code` | `character(9)` | NO |  | YES |  |
| 2 | `name` | `character varying(64)` | YES |  |  |  |
| 3 | `industry` | `character varying(64)` | YES |  |  |  |
| 4 | `market` | `character varying(16)` | YES |  |  |  |
| 5 | `area` | `character varying(32)` | YES |  |  |  |
| 6 | `list_date` | `date` | YES |  |  |  |
| 7 | `ext_json` | `jsonb` | YES |  |  |  |
| 8 | `updated_at` | `timestamp with time zone` | YES | `now()` |  |  |

**主键约束**

- 名称: `stock_info_pkey`
- 字段: `ts_code`

---

### Table `market.stock_st`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `ts_code` | `text` | NO |  | YES |  |
| 2 | `ann_date` | `date` | NO |  | YES |  |
| 3 | `start_date` | `date` | YES |  |  |  |
| 4 | `end_date` | `date` | YES |  |  |  |
| 5 | `market` | `text` | YES |  |  |  |
| 6 | `exchange` | `text` | YES |  |  |  |

**主键约束**

- 名称: `stock_st_pkey`
- 字段: `ts_code`, `ann_date`

---

### Table `market.symbol_dim`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `ts_code` | `character(9)` | NO |  | YES |  |
| 2 | `symbol` | `character(6)` | NO |  |  |  |
| 3 | `exchange` | `character(2)` | NO |  |  |  |
| 4 | `name` | `character varying(64)` | YES |  |  |  |
| 5 | `industry` | `character varying(64)` | YES |  |  |  |
| 6 | `list_date` | `date` | YES |  |  |  |

**主键约束**

- 名称: `symbol_dim_pkey`
- 字段: `ts_code`

---

### Table `market.tdx_board_daily`

> 通达信板块行情（Tushare tdx_daily）

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `trade_date` | `date` | NO |  | YES | 交易日（YYYY-MM-DD），接口入参 trade_date |
| 2 | `ts_code` | `character varying(16)` | NO |  | YES | 板块代码，如 880728.TDX |
| 3 | `open` | `numeric(20,6)` | YES |  |  | 开盘价 |
| 4 | `high` | `numeric(20,6)` | YES |  |  | 最高价 |
| 5 | `low` | `numeric(20,6)` | YES |  |  | 最低价 |
| 6 | `close` | `numeric(20,6)` | YES |  |  | 收盘价 |
| 7 | `pre_close` | `numeric(20,6)` | YES |  |  | 前收盘价 |
| 8 | `change` | `numeric(20,6)` | YES |  |  | 涨跌额 |
| 9 | `pct_chg` | `numeric(10,4)` | YES |  |  | 涨跌幅（%） |
| 10 | `vol` | `numeric(20,2)` | YES |  |  | 成交量（手） |
| 11 | `amount` | `numeric(28,2)` | YES |  |  | 成交额（元） |

**主键约束**

- 名称: `tdx_board_daily_pkey`
- 字段: `trade_date`, `ts_code`

---

### Table `market.tdx_board_index`

> 通达信板块基础信息（Tushare tdx_index）

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `trade_date` | `date` | NO |  | YES | 数据日期（YYYY-MM-DD），接口入参 trade_date |
| 2 | `ts_code` | `character varying(16)` | NO |  | YES | 板块代码，如 880728.TDX |
| 3 | `name` | `character varying(128)` | YES |  |  | 板块名称 |
| 4 | `idx_type` | `character varying(32)` | YES |  |  | 板块类型：概念/行业/风格/地域等 |
| 5 | `idx_count` | `integer` | YES |  |  | 板块成分数量 |

**主键约束**

- 名称: `tdx_board_index_pkey`
- 字段: `trade_date`, `ts_code`

---

### Table `market.tdx_board_member`

> 通达信板块成分（Tushare tdx_member）

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `trade_date` | `date` | NO |  | YES | 数据日期（YYYY-MM-DD），接口入参 trade_date |
| 2 | `ts_code` | `character varying(16)` | NO |  | YES | 板块代码，如 880728.TDX |
| 3 | `con_code` | `character varying(16)` | NO |  | YES | 成分证券代码（TS 标准代码） |
| 4 | `con_name` | `character varying(128)` | YES |  |  | 成分证券名称 |

**主键约束**

- 名称: `tdx_board_member_pkey`
- 字段: `trade_date`, `ts_code`, `con_code`

---

### Table `market.testing_runs`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `run_id` | `uuid` | NO |  | YES |  |
| 2 | `schedule_id` | `uuid` | YES |  |  |  |
| 3 | `triggered_by` | `character varying(16)` | NO |  |  |  |
| 4 | `status` | `character varying(16)` | NO |  |  |  |
| 5 | `started_at` | `timestamp with time zone` | YES | `now()` |  |  |
| 6 | `finished_at` | `timestamp with time zone` | YES |  |  |  |
| 7 | `summary` | `jsonb` | YES |  |  |  |
| 8 | `detail` | `jsonb` | YES |  |  |  |
| 9 | `log` | `text` | YES |  |  |  |

**主键约束**

- 名称: `testing_runs_pkey`
- 字段: `run_id`

**外键约束**

- `testing_runs_schedule_id_fkey`: (`schedule_id`) → `market.testing_schedules` (`schedule_id`)

---

### Table `market.testing_schedules`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `schedule_id` | `uuid` | NO |  | YES |  |
| 2 | `enabled` | `boolean` | NO | `true` |  |  |
| 3 | `frequency` | `text` | NO |  |  |  |
| 4 | `options` | `jsonb` | YES |  |  |  |
| 5 | `last_run_at` | `timestamp with time zone` | YES |  |  |  |
| 6 | `next_run_at` | `timestamp with time zone` | YES |  |  |  |
| 7 | `last_status` | `character varying(16)` | YES |  |  |  |
| 8 | `last_error` | `text` | YES |  |  |  |
| 9 | `created_at` | `timestamp with time zone` | YES | `now()` |  |  |
| 10 | `updated_at` | `timestamp with time zone` | YES | `now()` |  |  |

**主键约束**

- 名称: `testing_schedules_pkey`
- 字段: `schedule_id`

---

### Table `market.tick_trade_raw`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `trade_time` | `timestamp with time zone` | NO |  | YES |  |
| 2 | `ts_code` | `character(9)` | NO |  | YES |  |
| 3 | `price_li` | `integer` | NO |  | YES |  |
| 4 | `volume_hand` | `integer` | NO |  | YES |  |
| 5 | `status` | `smallint` | NO | `'-1'::integer` | YES |  |
| 6 | `source` | `character varying(16)` | NO | `'tdx_api'::character varying` |  |  |

**主键约束**

- 名称: `tick_trade_raw_pkey`
- 字段: `ts_code`, `trade_time`, `price_li`, `volume_hand`, `status`

---

### Table `market.trading_calendar`

| # | 字段名 | 类型 | 允许为空 | 默认值 | 是否主键 | 备注 |
|---|--------|------|----------|--------|----------|------|
| 1 | `cal_date` | `date` | NO |  | YES |  |
| 2 | `is_trading` | `boolean` | NO |  |  |  |

**主键约束**

- 名称: `trading_calendar_pkey`
- 字段: `cal_date`

---

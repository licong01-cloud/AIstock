# Paper v2 数据刷新审计表交接说明

创建日期：2026-05-05
适用对象：本地数据管理、数据同步任务、Paper Trading v2 / Selection Center 运行链路开发
相关表：`market.dataset_date_refresh_audit`

## 1. 背景

`market.dataset_date_refresh_audit` 是 AIstock 用于记录“某个逻辑数据集在某个业务日期是否已完成刷新并可被运行链路使用”的 readiness 台账。

它不是物理行情表，也不是自动从物理表 `MAX(trade_date)` 推导出来的缓存表。只有同步任务、seed 工具或专门的 readiness 发布逻辑显式写入时，它才会更新。

当前 Paper v2 / Selection Center 的部分 readiness 逻辑会调用该表做 fail-fast 检查。如果真实物理表已经更新，但该审计表没有同步写入成功记录，就会出现：

```text
真实数据表已到最新交易日
审计表仍停留在旧交易日
Paper v2 / Selection readiness 被误杀
```

因此，本地数据同步任务如果负责更新 Paper v2 依赖的数据集，需要在数据集刷新成功或失败后同步写入这张审计表。

## 2. 表结构

DDL 来源：

- `backend/migrations/trading_core_v2_schema.sql`
- `backend/db/init_trading_core_v2_schema.py`

字段如下：

| 字段 | 类型 | 是否为空 | 默认值 | 说明 |
|---|---:|---:|---|---|
| `dataset` | `text` | 否 | 无 | 逻辑数据集名称，不一定等于物理表名。例如 `stock_moneyflow_ts` 对应物理表 `market.moneyflow_ts`。 |
| `trade_date` | `date` | 否 | 无 | 该条审计对应的业务日期。对交易日数据通常是交易日；对可预告/未来日数据可由数据集自身语义决定。 |
| `data_source` | `text` | 否 | 无 | 数据来源或审计来源，例如 `tushare`、`tdx`、`seed_existing_rows`、`derived_builder`。 |
| `job_id` | `uuid` | 是 | `NULL` | 对应 `market.ingestion_jobs.job_id`，如果本次刷新由 ingestion job 驱动，应写入。 |
| `status` | `text` | 否 | 无 | 审计状态，只允许 `success` 或 `failed`。 |
| `row_count` | `integer` | 否 | `0` | 该 dataset/date 本次成功写入、替换或确认可用的行数；必须 `>= 0`。 |
| `refreshed_at` | `timestamptz` | 否 | `now()` | 审计记录写入或更新的时间。注意它是“审计确认时间”，不一定是物理表最后写入时间。 |
| `error_message` | `text` | 是 | `NULL` | `failed` 状态下的失败原因；`success` 一般为空。 |
| `metadata` | `jsonb` | 否 | `{}` | 扩展上下文，例如源 API、同步模式、物理表名、日期列、覆盖率、上游依赖、脚本名等。 |
| `data_max_at` | `timestamptz` | 是 | `NULL` | 时间戳型/分钟级数据集覆盖到的最大源数据时间；纯日期数据集可为空。 |
| `written_rows` | `bigint` | 是 | `NULL` | 本次刷新实际写入、替换或触达的行数；未知时为空。 |
| `expected_rows` | `bigint` | 是 | `NULL` | 该 dataset/date 按策略预期应有的可用行数，用于覆盖率检查。 |
| `coverage_ratio` | `numeric(12,8)` | 是 | `NULL` | 覆盖率，通常为 `row_count / expected_rows`；低于策略阈值时应配合 `quality_status=low_coverage`。 |
| `quality_status` | `text` | 否 | `unknown` | 质量分类，例如 `ok`、`empty_valid`、`empty_invalid`、`low_coverage`、`provider_unavailable`、`error`。 |
| `failure_category` | `text` | 是 | `NULL` | 机器可读失败分类，例如 `audit_stale`、`empty_invalid`、`provider_unavailable`，供重试/自愈逻辑使用。 |

主键：

```sql
PRIMARY KEY (dataset, trade_date, data_source)
```

约束：

```sql
CHECK (status IN ('success', 'failed'))
CHECK (row_count >= 0)
CHECK (written_rows IS NULL OR written_rows >= 0)
CHECK (expected_rows IS NULL OR expected_rows >= 0)
CHECK (coverage_ratio IS NULL OR (coverage_ratio >= 0 AND coverage_ratio <= 1.5))
```

索引：

```sql
CREATE INDEX idx_dataset_refresh_audit_date
ON market.dataset_date_refresh_audit(dataset, trade_date, status);
```

## 3. 字段语义重点

### 3.1 `dataset` 是逻辑数据集名

`dataset` 不是物理表名。物理表映射应从 `market.data_stats_config` 查询。

已知示例：

| 逻辑数据集 `dataset` / `data_kind` | 物理表 | 日期列 | 说明 |
|---|---|---|---|
| `kline_daily_raw` | `market.kline_daily_raw` | `trade_date` | TDX 未复权日线原始行情。 |
| `daily_basic` | `market.daily_basic` | `trade_date` | Tushare 每日基本面指标。 |
| `stock_moneyflow_ts` | `market.moneyflow_ts` | `trade_date` | Tushare 个股资金流。注意不存在 `market.stock_moneyflow_ts` 物理表。 |
| `sector_data` | `market.sector_data` | `trade_date` | 申万行业数据展开到个股级别的派生表。 |
| `index_daily` | `market.index_daily` | `trade_date` | Tushare 指数日线。 |
| `stk_limit` | `market.stk_limit` | `trade_date` | Tushare 每日涨跌停价格。 |
| `suspend_d` | `market.suspend_d` | `trade_date` | Tushare 停复牌数据。 |

### 3.2 `refreshed_at` 不是物理表更新时间

`refreshed_at` 表示“该审计记录被写入或更新的时间”。

如果记录来自实时同步任务成功后立即写入，则它可以近似代表“该 dataset/date 被系统确认刷新完成的时间”。

如果记录来自 `scripts/seed_dataset_refresh_audit.py`，则它只是“从已有表补写审计的时间”，不代表物理数据真实入库时间。

需要区分以下四个概念：

| 概念 | 来源 | 含义 |
|---|---|---|
| 最新成功覆盖日期 | `max(trade_date)` from audit where `status='success'` | 审计台账认定最新可用到哪天。 |
| 最后审计更新时间 | `max(refreshed_at)` | 审计记录最近一次写入/更新的时间。 |
| 物理表最新日期 | `select max(date_column) from physical_table` | 真实表数据覆盖到哪天。 |
| 物理表最后写入时间 | 物理表 `updated_at` / `ingested_at` 或 ingestion job | 真实数据写入发生的时间；当前并非所有表都有该字段。 |

## 4. Paper v2 对审计表的使用逻辑

### 4.1 通用读取入口

Paper v2 使用 `backend/services/data_refresh_audit.py` 中的 `DataRefreshAuditRepository.require_success()` 读取审计表。

核心语义：

```text
输入 dataset + trade_date + 可选 data_source
查询 market.dataset_date_refresh_audit
要求最新一条记录 status = success
否则抛出 DataUnavailableError
```

如果传入 `max_age_minutes`，还会检查 `refreshed_at` 是否过期；目前 Paper v2 主链路主要使用 dataset/date 成功状态。

### 4.2 Paper v2 day run 的数据 readiness

`backend/services/paper_trading_v2/day_runner.py` 中的 `_require_data_ready()` 会根据执行策略和运行配置检查：

| 条件 | 审计数据集 | 要求日期 |
|---|---|---|
| 执行策略要求停牌状态，或运行配置启用停牌过滤 | `suspend_d` | 当前 `trade_date` |
| 执行策略要求涨跌停价格 | `stk_limit` | 当前 `trade_date` |

这些检查用于确保日运行前的关键交易数据已经刷新成功。

### 4.3 前收盘价 fallback 的审计

`backend/services/paper_trading_v2/market_data.py` 中的 `DbPreviousCloseProvider.get_previous_close()` 在需要从日线表补充前收盘价时，会检查：

| 用途 | 审计数据集 | 要求日期 | 实际查询表 |
|---|---|---|---|
| 计算 `pre_close` fallback | `kline_daily_raw` | 当前交易日前一个交易日 | `market.kline_daily_raw` |

如果 `kline_daily_raw` 对前一交易日没有 success 审计，即使物理表有行，也会 fail-fast。

### 4.4 V25 day features 的审计

`backend/services/paper_trading_v2/day_features.py` 中的 `DbV25DayFeatureProvider` 用于构建 V25 执行算法需要的 10 维日级特征。

对运行日 `trade_date`：

```text
feature_date = trade_date 的前一个交易日
previous_feature_date = feature_date 的前一个交易日
```

它会先检查以下审计数据集在 `feature_date` 上是否 success：

| 审计数据集 | 实际查询表 | 用途 |
|---|---|---|
| `kline_daily_raw` | `market.kline_daily_raw` | 股票日线 OHLCV、当日收益、振幅、成交量、成交额。 |
| `daily_basic` | `market.daily_basic` | 换手率、自由流通换手率、PB 等基本面指标。 |
| `stock_moneyflow_ts` | `market.moneyflow_ts` | 个股资金流，计算 `moneyflow_net_ratio`。 |
| `sector_data` | `market.sector_data` | 行业涨跌幅等行业特征。 |
| `index_daily` | `market.index_daily` | 沪深 300 等市场指数收益。 |

此外，它还会检查：

| 审计数据集 | 要求日期 | 用途 |
|---|---|---|
| `kline_daily_raw` | `previous_feature_date` | 计算股票前一日收盘价，用于 `stock_ret_1d`。 |

注意：`stock_moneyflow_ts` 是逻辑数据集名；实际查询的是 `market.moneyflow_ts`。

### 4.5 Selection Center 相关使用

Selection Center 在启用停牌过滤时也会检查 `suspend_d` 审计，确保选股时使用的是已确认的停牌数据，而不是过期或缺失数据。

此外，运行配置中如果显式声明某些 required datasets，也会按 dataset/date 调用 `require_success()`。

### 4.6 `paper_v2_data_quality` 的使用

`nox -s paper_v2_data_quality` 会运行：

```text
scripts/aistock_data_quality_smoke.py
```

该脚本当前检查的是审计表最新 success 日期，而不是直接检查 H5/Bin，也不是检查 Qlib 回测数据。

当前要求大致为：

| 数据集 | 要求日期 |
|---|---|
| `suspend_d` | 最新已完成交易日 |
| `stk_limit` | 最新已完成交易日 |
| `kline_daily_raw` | 最新已完成交易日前一交易日 |
| `daily_basic` | 最新已完成交易日前一交易日 |
| `stock_moneyflow_ts` | 最新已完成交易日前一交易日 |
| `sector_data` | 最新已完成交易日前一交易日 |
| `index_daily` | 最新已完成交易日前一交易日 |

这里的“最新已完成交易日”受脚本中的本地盘后就绪时间控制：18:00 前不会要求当天盘后数据必须已存在。

## 5. 本地数据同步任务的写入建议

### 5.1 什么时候写 success

建议在“某个 dataset/date 的数据已刷新完成，并通过该 dataset 的完整性/覆盖率检查后”写入 `success`。

不要在任务开始前写 success。不要仅因为物理表 `MAX(date_column)` 到了某天就无条件写 success。

推荐基本原则：

| 数据集 | 写 success 的推荐条件 |
|---|---|
| `kline_daily_raw` | 当日 TDX 日线写入完成，并确认行数/覆盖率达到预期。 |
| `daily_basic` | Tushare `daily_basic` 按日期拉取完成并写入成功。 |
| `stock_moneyflow_ts` | Tushare `moneyflow` 按日期拉取完成并写入 `market.moneyflow_ts`；覆盖率达到预期。 |
| `index_daily` | 目标指数代码集合处理完成；失败数为 0 或满足明确业务阈值。 |
| `sector_data` | 派生构建完成，且上游 `sw_daily`、`moneyflow_ts`、PIT 行业映射满足该日期构建要求。 |
| `stk_limit` | Tushare `stk_limit` 按日期拉取完成。若交易所/上游返回合法 0 行，也应记录 success + `row_count=0`。 |
| `suspend_d` | Tushare `suspend_d` 按日期刷新完成。合法无停牌时可以 success + `row_count=0`。 |

### 5.2 什么时候写 failed

以下情况建议写 `failed`：

- API 调用失败。
- 写入物理表失败。
- 只写入部分数据且不满足覆盖率标准。
- 派生表上游依赖不完整。
- 任务被中断或超过重试上限。

失败记录应包含 `error_message` 和可定位上下文。

### 5.3 推荐写入 SQL

成功：

```sql
INSERT INTO market.dataset_date_refresh_audit (
    dataset,
    trade_date,
    data_source,
    job_id,
    status,
    row_count,
    refreshed_at,
    error_message,
    metadata,
    data_max_at,
    written_rows,
    expected_rows,
    coverage_ratio,
    quality_status,
    failure_category
) VALUES (
    %(dataset)s,
    %(trade_date)s,
    %(data_source)s,
    %(job_id)s,
    'success',
    %(row_count)s,
    NOW(),
    NULL,
    %(metadata)s::jsonb,
    %(data_max_at)s,
    %(written_rows)s,
    %(expected_rows)s,
    %(coverage_ratio)s,
    COALESCE(%(quality_status)s, 'ok'),
    NULL
)
ON CONFLICT (dataset, trade_date, data_source) DO UPDATE SET
    job_id = EXCLUDED.job_id,
    status = EXCLUDED.status,
    row_count = EXCLUDED.row_count,
    refreshed_at = EXCLUDED.refreshed_at,
    error_message = NULL,
    metadata = EXCLUDED.metadata,
    data_max_at = EXCLUDED.data_max_at,
    written_rows = EXCLUDED.written_rows,
    expected_rows = EXCLUDED.expected_rows,
    coverage_ratio = EXCLUDED.coverage_ratio,
    quality_status = EXCLUDED.quality_status,
    failure_category = NULL;
```

失败：

```sql
INSERT INTO market.dataset_date_refresh_audit (
    dataset,
    trade_date,
    data_source,
    job_id,
    status,
    row_count,
    refreshed_at,
    error_message,
    metadata,
    data_max_at,
    written_rows,
    expected_rows,
    coverage_ratio,
    quality_status,
    failure_category
) VALUES (
    %(dataset)s,
    %(trade_date)s,
    %(data_source)s,
    %(job_id)s,
    'failed',
    0,
    NOW(),
    %(error_message)s,
    %(metadata)s::jsonb,
    %(data_max_at)s,
    %(written_rows)s,
    %(expected_rows)s,
    %(coverage_ratio)s,
    COALESCE(%(quality_status)s, 'error'),
    %(failure_category)s
)
ON CONFLICT (dataset, trade_date, data_source) DO UPDATE SET
    job_id = EXCLUDED.job_id,
    status = EXCLUDED.status,
    row_count = 0,
    refreshed_at = EXCLUDED.refreshed_at,
    error_message = EXCLUDED.error_message,
    metadata = EXCLUDED.metadata,
    data_max_at = EXCLUDED.data_max_at,
    written_rows = EXCLUDED.written_rows,
    expected_rows = EXCLUDED.expected_rows,
    coverage_ratio = EXCLUDED.coverage_ratio,
    quality_status = EXCLUDED.quality_status,
    failure_category = EXCLUDED.failure_category;
```

Python 侧已有封装：

```text
backend/services/data_refresh_audit.py
```

可使用：

```python
DataRefreshAuditRepository.record_success(...)
DataRefreshAuditRepository.record_failure(...)
```

如果同步任务运行在独立脚本中，也可以直接复用该 repository；若因依赖隔离不方便复用，则至少应保持 SQL 写入语义一致。

## 6. metadata 推荐字段

建议不同数据集统一写入以下 metadata：

```json
{
  "physical_table": "market.moneyflow_ts",
  "date_column": "trade_date",
  "source_api": "tushare.moneyflow",
  "mode": "incremental",
  "producer": "scripts/ingest_tushare_moneyflow_ts.py",
  "coverage": {
    "expected_count": 5500,
    "actual_count": 5151,
    "coverage_ratio": 0.9365
  }
}
```

派生表建议额外记录上游依赖：

```json
{
  "physical_table": "market.sector_data",
  "date_column": "trade_date",
  "producer": "SectorDataBuilder",
  "upstreams": {
    "market.sw_daily": "2026-04-30",
    "market.moneyflow_ts": "2026-04-30",
    "market.sw_index_member": "PIT mapping active"
  },
  "builder_version": "sector_data_builder_v1"
}
```

Seed 工具写入的 metadata 应明确标记：

```json
{
  "seeded_from_existing_rows": true,
  "table": "market.moneyflow_ts",
  "script": "scripts/seed_dataset_refresh_audit.py"
}
```

这类记录只能证明“seed 当时物理表已有该日期数据”，不能证明真实同步任务在该时间完成。

## 7. 常用诊断 SQL

查看某数据集最新 success 日期：

```sql
SELECT dataset, max(trade_date) AS latest_success_date
FROM market.dataset_date_refresh_audit
WHERE dataset = 'stock_moneyflow_ts'
  AND status = 'success'
GROUP BY dataset;
```

查看某数据集最近审计更新时间：

```sql
SELECT dataset, max(refreshed_at) AS latest_audit_time
FROM market.dataset_date_refresh_audit
WHERE dataset = 'stock_moneyflow_ts'
GROUP BY dataset;
```

查看最新审计记录详情：

```sql
SELECT dataset, trade_date, data_source, status, row_count,
       refreshed_at, job_id, error_message, metadata
FROM market.dataset_date_refresh_audit
WHERE dataset = 'stock_moneyflow_ts'
ORDER BY trade_date DESC, refreshed_at DESC
LIMIT 5;
```

查看逻辑数据集到物理表的映射：

```sql
SELECT data_kind, table_name, date_column, enabled, extra_info
FROM market.data_stats_config
WHERE data_kind = 'stock_moneyflow_ts';
```

对比审计日期和物理表日期：

```sql
WITH cfg AS (
    SELECT data_kind, table_name, date_column
    FROM market.data_stats_config
    WHERE data_kind = 'stock_moneyflow_ts'
),
audit AS (
    SELECT dataset, max(trade_date) AS audit_latest_success
    FROM market.dataset_date_refresh_audit
    WHERE dataset = 'stock_moneyflow_ts'
      AND status = 'success'
    GROUP BY dataset
)
SELECT cfg.data_kind,
       cfg.table_name,
       cfg.date_column,
       audit.audit_latest_success
FROM cfg
LEFT JOIN audit ON audit.dataset = cfg.data_kind;
```

物理表最大日期需要动态 SQL，示例：

```sql
SELECT max(trade_date) AS physical_max_date
FROM market.moneyflow_ts;
```

## 8. 与 H5/Bin 回测快照审计的边界

`market.dataset_date_refresh_audit` 是 DB 数据集按日期的 readiness 台账，主要服务于 Paper v2 / Selection / 本地 DB 权威数据服务。

它不等同于 H5/Bin/Qlib 回测快照审计。

回测 H5/Bin 应单独维护 snapshot / artifact manifest，记录：

- 文件路径
- 文件 hash
- 数据最大日期
- 数据最小日期
- 字段列表
- 股票池范围
- 源表及源表日期
- 导出任务 ID
- 导出时间

Paper v2 不应因为某个 Qlib H5/Bin 只到旧日期而失败；Paper v2 应只检查它实际使用的 DB/TDX 数据源契约。

## 9. 当前已知风险

当前本地环境曾出现以下状态：

```text
真实物理表已到 2026-04-30：
  market.kline_daily_raw
  market.moneyflow_ts
  market.sector_data
  market.index_daily

审计表最新 success 仍停在 2026-04-28：
  kline_daily_raw
  stock_moneyflow_ts
  sector_data
  index_daily
```

根因是部分入库路径只写了物理表和 ingestion job/log，没有写 `market.dataset_date_refresh_audit`。

这会导致 Paper v2 readiness 误判数据不可用。

## 10. 建议交付项

给本地数据管理同步任务的建议改造项：

1. 对所有 Paper v2 / Selection 运行契约依赖的数据集，统一在同步成功后写 `market.dataset_date_refresh_audit`。
2. 对失败日期写 `failed`，不要只在日志中记录失败。
3. 使用 `market.data_stats_config` 作为逻辑 dataset 到物理表的映射来源，避免把 `stock_moneyflow_ts` 误当物理表。
4. `paper_v2_data_quality` 后续应同时展示：
   - 审计最新 success 日期；
   - 审计最新更新时间 `refreshed_at`；
   - 物理表最大日期；
   - 两者是否一致。
5. 对派生表 `sector_data` 增加上游依赖 metadata，避免只看派生表最大日期。
6. 对 H5/Bin 建立独立 snapshot 审计，不与 Paper v2 DB readiness 混用。

## 11. 2026-05-05 验证更新

本次验证结论：

- 审计表结构已包含增强字段 `data_max_at`、`written_rows`、`expected_rows`、`coverage_ratio`、`quality_status`、`failure_category`，字段注释齐全。
- Paper v2 当前数据质量 smoke 需要的审计行已经满足：`suspend_d`、`stk_limit`、`kline_daily_raw`、`daily_basic`、`stock_moneyflow_ts`、`sector_data`、`index_daily` 均通过 freshness gate。
- `stock_moneyflow_ts` 仍是逻辑 dataset 名，实际物理表为 `market.moneyflow_ts`。
- `kline_minute_raw` 目前没有进入 `paper_v2_data_quality` 的审计 freshness gate；Paper v2 执行时仍会直接按 symbol/date 加载 `market.kline_minute_raw` 分钟线并 fail-fast。
- 审计 freshness 只能证明 dataset/date 层面的可用性，不能完全证明 symbol 级覆盖率。2026-04-30 实际 live selection 验证仍暴露 `adj_factor` 对新股 `301599.SZ` 的历史窗口覆盖不足，因此后续本地数据管理还需要增加 symbol-level coverage gate。

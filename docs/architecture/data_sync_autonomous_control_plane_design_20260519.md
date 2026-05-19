# AIstock 每日数据同步自治控制平面设计方案

> 日期：2026-05-19
> 类型：详细设计方案
> 范围：本地数据管理模块、Tushare/TDX 数据同步、数据看板、数据健康检查、Paper v2 / Selection Center readiness
> 目标：除 Tushare 数据集本身缺失、Tushare API 不可用、数据库不可用、接口返回结构错误等不可控故障外，所有每日必要数据集都能自动追赶、自动校验、自动补齐，并且只在最终无法自动恢复时报警。

## 1. 背景和结论

2026-05-18 晚间 `cyq_perf` 调度任务显示执行成功：进度 100%，完成 6/6，新增 32961 条，失败 0。但数据看板仍显示未更新，系统检查仍报警。

本次分析确认这不是单纯的 UI 缓存问题，而是本地数据管理模块内部同时存在三套状态来源：

1. 物理表：例如 `market.cyq_perf`，代表真实入库的数据行。
2. 作业表：`market.ingestion_jobs` / `market.ingestion_logs`，代表脚本执行过程是否成功。
3. 权威 readiness 表：`market.dataset_date_refresh_audit`，代表某个 dataset/date 是否已经满足“可被业务消费”的条件。
4. 看板缓存表：`market.data_stats`，代表 UI 快速加载和 gap 查询的缓存结果。

长期方案必须把 `market.dataset_date_refresh_audit` 固定为每日 readiness 的唯一权威，把 `market.data_stats` 明确为可失效、可重建、可标记 stale 的缓存视图；任何任务的 `success` 只能表示“作业进程完成”，不能直接等价为“业务数据已更新”。

## 2. `cyq_perf` 实际情况和根因

### 2.1 现象复盘

| 项目 | 观察值 |
|---|---|
| 数据集 | `cyq_perf`，筹码分布绩效，Tushare `cyq_perf` |
| 类型 | 增量 |
| 开始时间 | 2026-05-18 23:19:52 |
| 作业状态 | `success` |
| 作业进度 | 100%，完成 6/6 |
| 新增行数 | 32961 |
| 失败数 | 0 |

这说明旧脚本确实把一批数据写入了物理表，但并不保证：

- `market.dataset_date_refresh_audit` 中存在 `cyq_perf` 对应日期的 `success` readiness。
- `market.data_stats` 已经刷新到物理表最新日期。
- UI 读取的 gap 缓存已经失效。
- 系统健康检查看到的是同一个权威状态。

### 2.2 代码路径差异

| 位置 | 当前职责 | 问题 |
|---|---|---|
| `backend/ingestion/tdx_scheduler.py:1293` | `cyq_perf` / `cyq_chips` 仍路由到 `scripts/ingest_tushare_cyq.py` | 绕过统一 `TushareSyncEngine` |
| `backend/ingestion/tdx_scheduler.py:1428` | 为 `cyq_perf` 构造 `--mode incremental --dataset cyq_perf` CLI | 调度层认为脚本成功即可成功 |
| `scripts/ingest_tushare_cyq.py:220` | `run_cyq_perf_ingestion()` 分页拉取、upsert、记录 job/log/progress | 未调用 `DataRefreshAuditRepository` 写入 readiness |
| `backend/services/tushare_sync_engine.py:337` | 统一同步引擎按日期处理并调用 `record_success()` / `record_failure()` | 统一路径才会写 audit |
| `backend/services/data_refresh_audit.py:46` | `record_success()` 写入 `market.dataset_date_refresh_audit` | readiness 权威入口 |
| `backend/services/audit_backed_data_health.py:248` | 根据 audit 计算 `ok/stale/error` | 健康检查不以 job success 为准 |
| `backend/routers/ingestion.py:2067` | `/api/data-stats` 读取 `market.data_stats` | 看板读取缓存，不一定实时反映 audit |
| `frontend/src/app/local-data/page.tsx:3948` | 首屏读取 `/api/data-stats` | UI 初始状态依赖缓存结果 |

因此 `cyq_perf` 的核心根因是：旧脚本路径写了物理表和 job 状态，却没有写入 readiness 权威表；看板又依赖 `data_stats` 缓存，所以出现“任务成功、看板未更新、系统检查报警”的状态分裂。

### 2.3 是否只有 `cyq_perf` 使用缓存表

不是。`market.data_stats` 是本地数据看板的全局缓存表，不是 `cyq_perf` 专属表。它的价值是：

- 首屏快速显示所有数据集的行数、最大日期、更新时间、gap 摘要，避免每次打开 UI 都扫描大表。
- 支持 gap 查询 TTL，降低本地 PostgreSQL/TimescaleDB 的重复查询压力。
- 为前端提供统一分类、状态、摘要字段，避免每个页面直接理解不同表结构。
- 作为“展示缓存”和“异常线索”有价值，但不能作为 readiness 权威。

长期方案必须保留 `data_stats`，但要改变语义：它只能缓存由 `dataset_date_refresh_audit` 和物理表 reconciliation 计算出的结果；当缓存落后于 audit 或物理表时，必须显式显示 `cache_state='stale'`，并触发后台重建，而不是误导用户。

## 3. 目标状态

### 3.1 权威状态模型

每日数据同步必须形成以下闭环：

1. 生成目标：根据交易日历、数据集 release policy、依赖 DAG 生成 `dataset/date` 级 target。
2. 执行同步：统一 ingestion adapter 拉取数据并写物理表。
3. 写 readiness：只有通过校验后才写 `market.dataset_date_refresh_audit(status='success')`。
4. 重建缓存：以 audit 为主、物理表为校验来源刷新 `market.data_stats`。
5. 对账修复：定期比较 target、job、audit、physical、data_stats。
6. 自动补齐：所有可恢复缺口进入 retry queue，直到成功或 final deadline。
7. 报警门控：只有不可控故障或 deadline 后仍无法恢复的 final blocked 才报警。

### 3.2 成功判定

一个 dataset/date 只有同时满足以下条件，才可以对业务声明 ready：

- 已到该数据集 release window，且不早于 provider 合理发布时间。
- 物理表存在该日期数据，或 catalog 明确允许该日期空结果为合法。
- 数据行通过 schema、主键、日期、关键字段、合理行数、重复行校验。
- `market.dataset_date_refresh_audit` 写入 `status='success'`，并记录 `row_count`、`written_rows`、`expected_rows`、`coverage_ratio`、`quality_status`、`failure_category`、`job_id`。
- `market.data_stats` 已刷新或被标记为 stale 并安排重建。

## 4. 数据集分层和 deadline policy

### 4.1 优先级

| 优先级 | 数据集示例 | 业务影响 |
|---|---|---|
| P0 盘前关键 | `stk_limit`、`suspend_d`、`kline_minute_raw`、`kline_daily_raw` | Paper v2 / Selection Center 盘前 readiness |
| P1 日终核心 | `daily_basic`、`adj_factor`、`stock_moneyflow_ts`、`index_daily`、`sector_data` | 因子、选股、回测和健康检查 |
| P1 延迟发布 | `cyq_perf`、`bak_basic`、`sw_daily`、`margin_detail` | 筹码、申万、融资融券等辅助或扩展数据 |
| P2 基础维表 | `stock_basic`、`stock_st`、`stock_st_events`、`sw_index_classify`、`sw_index_member` | universe、行业、ST/PIT |
| P2 稀疏公告 | `tushare_forecast_raw`、`tushare_express_raw`、`tushare_fina_indicator_raw` | 财务公告，可合法 0 行 |

### 4.2 release window

| 数据集 | 日期策略 | 首次尝试 | soft deadline | final deadline | 说明 |
|---|---|---:|---:|---:|---|
| `stk_limit` | 上一交易日，盘前补齐 | 09:01 | 09:14 | 09:15 | 明早获取涨跌停板数据，final 前不报警 |
| `suspend_d` | 当前或下一交易日 | 07:30、08:50、09:05、12:40、16:10、17:30 | 09:15 / 17:30 | 18:00 | 允许盘前和盘后多次补齐 |
| `cyq_perf` | 上一交易日至最新可用交易日 | 18:10 | 22:00 | 23:30 | Tushare 可能延迟，0 行不直接成功 |
| `sw_daily` | 上一交易日至最新可用交易日 | 18:20 | 22:00 | 23:30 | 行业行情延迟发布 |
| `margin_detail` | T+1 | 19:00 | 次日 09:00 | 次日 12:00 | 上一交易日数据通常次日才完整 |
| `bak_basic` | 上一交易日至最新可用交易日 | 18:30 | 22:30 | 23:30 | 延迟发布或部分日期空结果 |
| 稀疏公告类 | 自然日或公告日 | 20:30 | 23:00 | 次日 08:00 | 0 行可能合法，但必须写 empty_valid audit |

### 4.3 报警原则

不再对“暂未发布、正在重试、可自动补齐”的状态报警。报警只允许以下情况：

- provider API 不可用且超过 final deadline。
- provider 返回字段缺失、类型错误、日期错位、重复主键等 contract error。
- 数据库不可用、写入失败、事务失败。
- 重试次数耗尽且 deadline 已过。
- reconciliation 发现 job/audit/physical/data_stats 无法自动修复。

## 5. 控制平面架构

### 5.1 核心组件

| 组件 | 职责 |
|---|---|
| Dataset Catalog | 描述数据集日期策略、主键、源接口、发布窗口、空结果策略、依赖关系、质量阈值 |
| Target Generator | 生成每日必须完成的 `dataset/date` target |
| Policy Engine | 判断是否可尝试、是否 pending_publish、是否 final_blocked |
| Retry Queue | 持久化待补齐目标、attempt、next_retry_at、last_error、failure_category |
| Ingestion Adapter | 统一封装 Tushare/TDX 调用、字段校验、分页、限流、幂等写入 |
| Audit Writer | 写入 `market.dataset_date_refresh_audit`，作为 readiness 权威 |
| Reconciliation Worker | 对账 job、audit、physical、data_stats，并自动修复可修复差异 |
| Data Stats Builder | 重建 `market.data_stats` 和 gap 缓存，标记 cache_state |
| Alert Gate | 只对 final blocked 和不可控故障创建 `market.data_alerts` |
| Readiness API | 向 UI、Paper v2、Selection Center 提供统一 readiness |

### 5.2 状态机

`dataset/date` target 状态：

```text
planned
  -> waiting_release
  -> queued
  -> running
  -> success
  -> reconciled
```

异常状态：

```text
running
  -> retry_waiting
  -> queued
  -> final_blocked
```

合法空结果状态：

```text
running
  -> empty_valid
  -> success
```

可自动修复状态：

```text
job_success_audit_missing
physical_success_audit_missing
audit_success_stats_stale
stats_success_physical_missing
```

这些状态不能直接报警，必须先进入 reconciliation 和 retry queue。

## 6. `cyq_perf` 专项修复设计

### 6.1 短期补齐

当前立即事项可以不改程序代码完成：用只读核验 SQL 确认物理表最新日期、audit 最新日期、data_stats 最新日期，再通过现有受控脚本或已有 API 重建 `data_stats`，必要时补写 audit 只能走明确审批或后续代码修复，不应手工随意改库。

### 6.2 长期代码修复

长期修复必须在独立开发分支完成，不直接在 main 改 runtime：

1. 将 `cyq_perf` 从旧 `scripts/ingest_tushare_cyq.py` 路由迁移到统一 `TushareSyncEngine`，或在旧脚本中接入同一套 `DataRefreshAuditRepository` 和质量校验。
2. 对每个 trade_date 写入 audit，而不是仅记录 job summary。
3. `inserted_rows=0` 不能默认 success，必须根据 `zero_row_policy` 和 deadline 判断 `pending_publish`、`empty_valid` 或 `provider_data_missing`。
4. 同步完成后触发 `data_stats` 针对性刷新或标记 cache stale。
5. Reconciliation worker 能识别历史“物理表有数据但 audit 缺失”的日期，并自动补写 audit 或创建受控 backfill target。

## 7. 数据看板改造

### 7.1 UI 展示字段

看板必须同时展示：

- `physical_max_date`：物理表最大日期。
- `ready_date`：audit 权威 ready 日期。
- `stats_max_date`：data_stats 缓存日期。
- `cache_state`：`fresh` / `stale` / `rebuilding` / `error`。
- `next_retry_at`：下一次自动补齐时间。
- `release_state`：`waiting_release` / `pending_publish` / `ready` / `final_blocked`。
- `operator_action_required`：是否需要人工介入。
- `failure_category`：失败分类。

### 7.2 false-success 防护

UI 不得只显示 job success。若 job success 但 audit 缺失，应显示：

```text
作业已完成，但 readiness 尚未确认；系统正在执行对账和自动补齐。
```

若 audit ready 但 data_stats stale，应显示：

```text
数据已入库并通过 readiness，缓存正在刷新；业务可按 audit ready 状态判断。
```

## 8. 数据库和 API 设计

### 8.1 新增或扩展表

建议新增：

- `market.data_sync_targets`：每日目标和状态。
- `market.data_sync_attempts`：每次尝试的执行记录。
- `market.data_sync_retry_queue`：持久化重试队列。
- `market.dataset_release_policy`：数据集发布时间、deadline、空结果策略。

建议扩展：

- `market.dataset_date_refresh_audit` 增加 `quality_status`、`failure_category`、`expected_rows`、`coverage_ratio`、`source_payload_hash`、`reconciled_at`。
- `market.data_stats` 增加 `cache_state`、`audit_ready_date`、`physical_max_date`、`stats_source`、`stale_reason`。
- `market.data_alerts` 增加 `target_id`、`failure_category`、`operator_action_required`、`auto_retry_exhausted`。

所有新增表和字段必须有 PostgreSQL comment。

### 8.2 API

| API | 职责 |
|---|---|
| `GET /api/data-readiness` | 返回权威 readiness、release_state、retry、alert gate 状态 |
| `POST /api/data-sync/reconcile` | 受控触发对账，不直接伪造 success |
| `POST /api/data-sync/retry` | 受控重试单个 dataset/date |
| `GET /api/data-stats` | 保留看板缓存，但返回 cache_state 和 audit_ready_date |
| `GET /api/data-alerts` | 只展示 final blocked 或不可控故障 |

## 9. 严格测试用例和测试方案

### 9.1 测试分层

| 等级 | 范围 | 退出标准 |
|---|---|---|
| L0 静态/契约 | YAML、schema、guardrail、DB comment、路径归属 | YAML 可解析，规则引用有效，无新增 P0/P1 |
| L1 单元 | policy engine、target generator、alert gate、zero-row 判定 | release/T+1/deadline/failure_category 全覆盖 |
| L2 集成 | 临时 DB + mocked source + scheduler | job/audit/physical/data_stats/retry_queue 一致 |
| L3 API/UI | 本地 dev backend/frontend | 看板状态、readiness API、Paper v2 readiness 一致 |
| L4 跨模块 | 数据同步 + Selection/Paper/QE 只读链路 | 真实业务依赖按 audit ready 判定 |
| L5 长跑/夜间 | 市场日自动同步全链路 | 次日无需人工干预，只有 final blocked 报警 |

### 9.2 必须落地的测试用例

| ID | 场景 | 输入/前置 | 期望结果 | 报警 |
|---|---|---|---|---|
| DS-AUTO-001 | `cyq_perf` job success 但 audit 缺失 | mock 旧脚本 job success，物理表有数据 | reconciliation 自动补 audit 或生成 retry target | 否 |
| DS-AUTO-002 | `cyq_perf` 首次同步失败后自动补齐 | audit failed + retry target | 下次成功后 target/audit/data_stats 全部 ready | 否 |
| DS-AUTO-003 | `cyq_perf` deadline 前 0 行 | Tushare 返回空 DataFrame | 状态为 `pending_publish`，安排 backoff | 否 |
| DS-AUTO-004 | `cyq_perf` deadline 后仍 0 行 | 仍为空 | `final_blocked` + `provider_data_missing` | 是 |
| DS-AUTO-005 | data_stats 落后 audit | audit ready_date > stats_max_date | API 返回 `cache_state='stale'` 并触发重建 | 否 |
| DS-AUTO-006 | audit success 但物理表缺行 | mock 人工删除物理行 | reconciliation 标记 audit invalid 并创建 retry | final 后是 |
| DS-AUTO-007 | `margin_detail` T+1 发布 | 最新交易日 T，数据最大 T-1 | T 日不误报，次日 deadline 前自动补齐 | 否 |
| DS-AUTO-008 | `stk_limit` 09:05 未发布 | 盘前 mock 空结果 | 进入 pre-open retry | 否 |
| DS-AUTO-009 | `stk_limit` 09:15 仍缺 | 盘前 final 已过 | final blocked，阻断 Paper v2 readiness | 是 |
| DS-AUTO-010 | `suspend_d` 多时段刷新 | release policy 多窗口 | 每个窗口幂等尝试，最新 ready 写 audit | 否 |
| DS-AUTO-011 | `sector_data` 依赖缺失 | `sw_daily` 或 `moneyflow_ts` audit missing | 依赖 target 优先补齐，sector 不伪成功 | 否 |
| DS-AUTO-012 | Tushare 500/timeout | mocked adapter 抛 timeout | retry 到 deadline，分类 `provider_unavailable` | final 后是 |
| DS-AUTO-013 | Tushare 字段错误 | mocked DataFrame 缺字段 | fail-fast，分类 `provider_contract_error` | 是 |
| DS-AUTO-014 | DB 不可用 | mocked psycopg2 failure | job failed，分类 `db_unavailable` | 是 |
| DS-AUTO-015 | scheduler 中断 | worker 进程被杀 | target 保留，watchdog 重新入队 | 否 |
| DS-AUTO-016 | job success 但 `inserted_rows=0` | job summary success/0 | 不得写 readiness success，按 policy 判定 | 否 |
| DS-AUTO-017 | 稀疏公告合法 0 行 | `zero_row_policy=valid_sparse` | audit success + `quality_status='empty_valid'` | 否 |
| DS-AUTO-018 | gap cache 过期 | `last_check_at` 超 TTL | 自动失效并重算 gap | 否 |
| DS-AUTO-019 | 报警恢复闭环 | stale alert 存在且 retry 成功 | 自动标记 resolved/ack source | 否 |
| DS-AUTO-020 | 全天 provider outage | API 持续不可用 | 只在 final blocked 后产生一条去重报警 | 是 |

### 9.3 结果数据验证方式

每次 L2-L5 验证必须同时核对四类证据：

```sql
-- 1. 权威 readiness
SELECT dataset,
       MAX(trade_date) FILTER (WHERE status = 'success') AS ready_date,
       MAX(refreshed_at) AS last_audit_at
FROM market.dataset_date_refresh_audit
WHERE dataset = ANY(%(required_datasets)s)
GROUP BY dataset;
```

```sql
-- 2. cyq_perf 物理表和 audit 对账
WITH physical AS (
  SELECT trade_date, COUNT(*)::bigint AS physical_rows
  FROM market.cyq_perf
  WHERE trade_date BETWEEN %(start_date)s AND %(end_date)s
  GROUP BY trade_date
), audit AS (
  SELECT trade_date, status, row_count AS audit_rows, quality_status
  FROM market.dataset_date_refresh_audit
  WHERE dataset = 'cyq_perf'
    AND trade_date BETWEEN %(start_date)s AND %(end_date)s
)
SELECT COALESCE(p.trade_date, a.trade_date) AS trade_date,
       p.physical_rows,
       a.audit_rows,
       a.status,
       a.quality_status
FROM physical p
FULL OUTER JOIN audit a USING (trade_date)
ORDER BY trade_date;
```

```sql
-- 3. data_stats 缓存和 audit 对账
SELECT s.data_kind,
       s.max_date AS stats_max_date,
       s.stat_generated_at,
       a.ready_date,
       a.last_audit_at
FROM market.data_stats s
LEFT JOIN (
  SELECT dataset, MAX(trade_date) AS ready_date, MAX(refreshed_at) AS last_audit_at
  FROM market.dataset_date_refresh_audit
  WHERE status = 'success'
  GROUP BY dataset
) a ON a.dataset = s.data_kind
WHERE s.data_kind = ANY(%(required_datasets)s);
```

```sql
-- 4. 当日有效报警
SELECT dataset, severity, alert_type, title, created_at
FROM market.data_alerts
WHERE acknowledged = FALSE
  AND created_at >= CURRENT_DATE
ORDER BY severity DESC, dataset;
```

验证记录必须保存到：

`tests/aistock_validation/history/local_data_management/<YYYYMMDD>_data_sync_autonomy_<level>.md`

记录内容必须包含：命令、端口、mock 数据、SQL 摘要、API 响应、UI 截图/trace、失败和修复、rerun 结果、是否触碰生产服务。

## 10. Main 合入验收标准

### 10.1 本设计文档合入 Main 标准

本次文档和规范变更可合入 Main 的条件：

1. 设计文档位于 `docs/architecture/data_sync_autonomous_control_plane_design_20260519.md`。
2. 开发规范升级到 v1.2，并同步更新机器 YAML。
3. 旧 v1.1 标准移入 `docs/standards/archive/`。
4. guardrail scanner 默认读取 v1.2 YAML。
5. YAML 可解析，规则引用有效。
6. 目标 pytest 通过。
7. 只提交本任务相关文件，不提交其它窗口的 `.codex_tmp`、运行记录或无关文档。
8. 不触碰生产 backend `8001` 和 frontend `3000`。

### 10.2 长期代码实现合入 Main 标准

后续独立开发分支实现后，必须满足：

1. L0-L5 测试矩阵全部通过并保存验证记录。
2. `cyq_perf` 覆盖 job success + audit missing + data_stats stale 的自动恢复路径。
3. 首次同步失败后 retry queue 能自动补齐并去重。
4. `stk_limit`、`suspend_d`、`margin_detail` 覆盖 release/T+1/deadline 策略。
5. 只有 final blocked 或不可控故障产生 `market.data_alerts`。
6. API/UI 同时展示 `ready_date`、`physical_max_date`、`stats_max_date`、`cache_state`、`next_retry_at`、`operator_action_required`。
7. Paper v2 / Selection Center 只消费 audit-backed readiness。
8. DB schema comment、migration、回滚策略、run evidence 完整。
9. 自动化流水线通过后，由用户确认是否合入 Main。

## 11. 分阶段实施计划

### Phase 0：设计和治理固化

- 落地本设计文档。
- 升级开发规范 v1.2。
- 在项目记忆中记录 `cyq_perf` 根因和设计交付自动提交 Main 的治理规则。

### Phase 1：Catalog + Policy Engine

- 增加 release policy、zero-row policy、dependency policy。
- 覆盖 `margin_detail` T+1、`stk_limit` 盘前 deadline、`suspend_d` 多窗口刷新。
- 完成 L1 policy 单元测试。

### Phase 2：`cyq_perf` 接入统一 readiness

- 统一写入 `dataset_date_refresh_audit`。
- 同步后刷新或标记 `data_stats`。
- 支持物理表/audit/data_stats reconciliation。

### Phase 3：持久化 retry queue + watchdog

- 增加 retry queue 和 attempts。
- scheduler 重启后恢复未完成 target。
- job success 但 readiness 缺失时自动补齐。

### Phase 4：Alert Gate

- 重写 `_data_freshness_check` 或其上层聚合逻辑。
- 只有 deadline + retry exhausted 才产生报警。
- 实现报警恢复、去重和 ack 语义。

### Phase 5：看板和 readiness API

- 增加 audit-backed readiness API。
- 看板展示 cache_state、next_retry_at、operator_action_required。
- UI 区分 job success、readiness success、cache stale。

### Phase 6：验证中心和夜间自动化

- 将 L0-L5 测试接入 nox / Validation Center。
- 每个市场日保存 L5 自动同步验证记录。
- 代码分支通过流水线后再请求用户确认合入 Main。

## 12. 不做事项

- 不把 `market.data_stats` 当作 readiness 权威。
- 不把 job `success` 当作业务成功。
- 不在 release deadline 前报警。
- 不把 T+1 数据强行要求 T 日 ready。
- 不以单表 `MAX(date)` 代替 per-date audit。
- 不手工长期补库替代程序化 reconciliation。
- 不把长期代码修复直接混入本次 Main 文档提交。

## 13. 核心结论

| 问题 | 结论 |
|---|---|
| readiness 权威 | `market.dataset_date_refresh_audit` |
| 缓存表价值 | `market.data_stats` 用于 UI 快速加载和 gap 摘要，但可 stale、可重建 |
| `cyq_perf` 根因 | 旧脚本路径写物理表和 job，不写 audit |
| 首次失败自动补齐 | 通过持久化 target/retry queue + watchdog + reconciliation |
| 准确更新情况 | 同时返回 physical/audit/stats/retry/alert 状态 |
| 报警策略 | 只对 final blocked 和不可控故障报警 |
| 合入 Main | 文档和规范可本次合入；代码实现必须独立分支、流水线通过、用户确认 |

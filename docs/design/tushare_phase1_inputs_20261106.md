# Tushare 第一批共享输入接入（F1）

## 背景

用户批准现有 Tushare 兼容整改后的新增输入实施。本批仅接入 VIX、
daily_basic.limit_status、股票盘后成交量额，复用现有同步引擎、调度、统计和 UI。
BUG-2187 已合入并通过重启验收；不重复实现 HTTP 兼容修复。
跟踪 Issue：#7082；基线 main：75f93692d。

## 范围

- VIX：`market.vix_index`，上游 `vix_index` 的 trade_date、close、pct_change。
- daily_basic：新增可空 SMALLINT limit_status（0..6），复用原每日请求。
- 盘后：`market.stock_daily_afterhours`，上游 daily 的 ah_vol（手）、ah_amount（千元）。
- 两个新输入保存带真实捕获时间及 SHA256 的原始观察版本；相同内容不产生事实 revision。
- 现有 DEV 迁移、短窗口真实同步、独立读回及幂等验证。

## 非目标

不修改 QE/HMM/Advisory、PIT 规则、旧数据集、账户、因子和训练。风险事件、指数公告、
增发及可选因子库属于后续批次。本批不采全历史、不执行生产迁移或重启服务。
不新增全局完整率阈值、审批、队列或调度器，不把新增可选输入变成全市场每日硬依赖。

## Design Acceptance Index

- F-001：准确字段、单位、边界、空值及股票范围。
- F-002：原始观察版本、真实捕获时间、修订和幂等。
- F-003：现有同步、有限重试、滚动尾窗及失败状态。
- F-004：本地管理 API、看板、手工补齐及默认自动调度。
- F-005：可选输入与 A 股全局每日完成标识独立；VIX 不冒充美国日历完整。
- F-006：DEV 迁移及真实来源写入、独立读回；生产操作独立授权。

## 数据合同

VIX 仅承诺官方当前真实返回的三字段，不根据示例制造 OHLC/pre_close。
close 必须有限且大于零，pct_change 有限、可负；原始字符串保留在原始观察 JSON。
日期是美国 session date；请求分段最多 90 自然日，明确拒绝达到 300 行上限。
默认每日北京时间 08:00 运行；12:00 观察通过现有调度器的同一数据集补查入口运行。
未取得正式美国交易日历前，只报告成功查询覆盖与实际事实，不证明未知空日为休市，
`session_calendar_completeness=NOT_EVALUATED`。不实施消费者 as-of 连接或宣称历史 PIT。

盘后数据起点 2026-07-06，之前为不适用；保留上游 NULL、真实零及有限非负值。
不修改 TDX 日线或分钟量额，也不从它们扣除盘后值。全沪深 A 股事实独立于买入 PIT，
排除北交所、B 股；ST、新上市及退市前真实事实不因不可选而删除。

limit_status 可空，非整数或 0..6 之外拒绝入库；不新增必需字段覆盖率要求。
初次历史回补应另指定窗口并仅补该列 NULL，不随本批启动全历史作业。

原始观察表以 dataset + 原始内容 SHA256 保存版本，captured_at/last_observed_at 为实际时间，
published_at=NULL、publication_time_quality=UNKNOWN、vintage_quality=NON_VINTAGE。
原始观察保留供应商 NULL 修订；当前事实投影不被 NULL 擦除，不能把合并投影冒充单条来源原文。
原始观察和当前事实在同一事务中写入；上游错误不得写成零记录成功。

## 实施方案

1. 先写直接回归，验证新 spec、字段、范围、非法值、上限及调度独立性。
2. 新增显式 SQL 迁移及对应 DEV 验证脚本；不隐式建表或加载生产默认连接。
3. 在现有引擎增加本批原始观察写入与 VIX 范围获取；新增 dataset 注册。
4. 接通现有默认调度、统计配置、API 和 UI 全部操作入口。
5. 最小定向回归、DEV 真实短窗、独立进程读回、重复执行与多轮审核。

## 验证方案

- `backend/tests/test_tushare_phase1_inputs.py`：直接合同、边界、失败、幂等、目录/调度/API/UI 接入。
- `backend/tests/test_tushare_sync_engine.py`：共享引擎原有最小回归。
- DEV 真实数据：选少量已结束日期；检查数值/原文/SHA、NULL 不擦除、修订和日期。
- 在既存 DEV 验证迁移两次；无新测试数据库、不要求生产备份。
- 变更文件 Ruff、编译、差异空白和 F1 设计符合性校验；广域 UI/模块检查交现有 CI。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | backend/services/tushare_dataset_specs.py | backend/tests/test_tushare_phase1_inputs.py | verified | - |
| F-002 | backend/services/tushare_supplemental_inputs.py | artifact:X:/AIstock_temp/DATA_PREPARATION/tushare-phase1-inputs-20261106/dev-atomic-and-API.json | verified | - |
| F-003 | backend/services/tushare_sync_engine.py | backend/tests/test_tushare_phase1_inputs.py | verified | - |
| F-004 | backend/db/init_tushare_schedules.py; backend/routers/ingestion.py; frontend/src/app/local-data/page.tsx | artifact:X:/AIstock_temp/DATA_PREPARATION/tushare-phase1-inputs-20261106/dev-atomic-and-API.json | verified | - |
| F-005 | backend/services/data_integrity/catalog.py; backend/services/data_integrity/worker.py; backend/services/data_refresh_audit.py | backend/tests/test_tushare_phase1_inputs.py | verified | - |
| F-006 | backend/db/migrations/add_tushare_phase1_inputs_v1.sql; scripts/validate_tushare_phase1_inputs.py | artifact:X:/AIstock_temp/DATA_PREPARATION/tushare-phase1-inputs-20261106/dev-independent-readback.json | verified | - |

## 本轮验收与审核

- RED：新增合同初始 18 项失败（缺少 spec/实现）；GREEN：最终相关回归 394 passed、1 skipped。
- DEV 现有库迁移连续两次通过；2026-11-04..05 原生 public sync 每个输入运行两次。
- VIX 两行；盘后 10,435 行；limit_status 10,435 个非空值，取值均在 0..6。
- 原始观察 10,437 条独立 SHA256 校验无不一致；四个可选输入分区只读 PhysicalValidator 均 VALID，
  VIX 明确附带 session_calendar_completeness=NOT_EVALUATED，不宣称美国日历完整。
- 两次真实同步后分区 revision 均为 1；DEV 原生故障注入证明原文/当前事实/分区 revision 整体回滚。
- DEV 事务内测试 NULL 保留有效事实、有效修订增加版本及 revision、同内容不再递增；测试修改均已回滚。
- 现有统计单分区入口只刷新两个新小表；原生数据列表及 auto-range 独立读回通过。
- 本地前端 TSX 语法通过；广域 UI 类型/交互及跨模块回归交现有 CI/Validation Center，不启动用户服务。
- 三轮复审：①字段、单位、原文及 NULL；②可选完整性目录与美国空日，不新增无效重试；
  ③DEV 原生事务与 API、幂等和生产权限边界。无模型/PIT/账户/发布数据集修改。
- `noxfile.py` 仅给现有 local_data 回归清单增加本批直接测试，不改变工作流触发、依赖或执行机制。

## 风险

供应商可修订历史、延迟发布或拒绝权限；保留失败与 UNKNOWN，不伪报历史可得。
VIX 没有正式日历时不能宣称预期 session 全覆盖；这不影响 A 股原有完成标识。
17:00 America/New_York 只是晚于常规计算结束的保守查询上界，不是供应商发布承诺，
依据：https://www.cboe.com/tradable_products/vix/faqs 。正式 Cboe 节假日日历尚未登记。
生产 daily_basic 新列必须先显式迁移，再部署请求新增字段的代码；不能先重启造成入库失败。
默认调度启用必须与对应迁移同次部署；新表缺失沿用清晰报错，不在启动阶段创建。

## Production gates（既有生产权限边界）

DEV 验证、源码交付、生产授权、生产迁移及读回、重启调度验收分别报告。
本批未获特定生产 DDL/DML 授权，不自动调用生产同步、不安装依赖、不重启服务。

生产迁移精确文件：`backend/db/migrations/add_tushare_phase1_inputs_v1.sql`，
SHA256=`1703f53a89ddaff50dfa6921074c83d644855df38254659b6cdfdffbb2af678c`。
对象仅为 daily_basic 新增一列、两个新事实表、一个原始观察表、对应元数据和三条默认调度。

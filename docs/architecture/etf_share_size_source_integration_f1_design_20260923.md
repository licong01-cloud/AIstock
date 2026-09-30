# ETF 份额规模原始源接入 F1 详细设计

## 1. 背景与目标

因子研究已经完成 `etf_share_size` 的只读可行性研究：Tushare 能提供沪深 ETF 的每日份额、规模、净值和收盘价，数据量可由现有按交易日同步引擎承载；但现有数据库没有该原始数据源，也没有可审计的 ETF 当期基础信息快照。

本功能只补齐共享数据准备能力，不直接研发因子、不改变 QE/HMM/荐股/模拟盘消费者，也不修改、重导或激活 R8 数据集。目标是：

1. 将 ETF 每日份额规模作为普通 Tushare 原始数据集写入本地数据管理；
2. 从功能启用日起保存 ETF 基础信息的每日观察快照，禁止用当前映射回填历史；
3. 复用现有调度、审计、告警和自动重试链；
4. 支持显式历史补录，并在补录完成后运行一次只读完整校验；
5. 为后续研究提供稳定原始表，但不把本次接入冒充为可用因子或指数成分 PIT authority。

功能等级为 `F1`：单一 `local_data` 数据准备边界内的数据库、同步引擎、调度、API/本地数据管理和定向测试变更。

## 2. 范围与来源合同 / Scope

### 2.1 `etf_share_size`

- Tushare API：`etf_share_size`；要求 8000 积分；单次最多 5000 行。
- 可按 `trade_date` 查询；使用现有 `BY_DATE` 路径，按交易日迭代。
- 官方说明数据分批入库，建议每日 19:00 后提取；海外 ETF 可能更晚。
- 落库字段限定为：`trade_date`、`ts_code`、`total_share`、`total_size`、`nav`、`close`。
- 单位保持源语义：`total_share` 为万份，`total_size` 为万元，`nav/close` 为元。
- 主键为 `(trade_date, ts_code)`；重复执行仅 upsert，不删除其他日期。
- 当日返回 0 行，或整日 `total_share`/`total_size` 任一核心源字段完全没有有限值，视为上游尚未完成发布；由既有审计和重试链处理。不得设置基于当前 ETF 数量或非空比例的固定门禁。

### 2.2 `etf_basic_snapshots`

- Tushare API：`etf_basic`；单次最多 5000 行，当前规模低于上限。
- 每日分别请求 `L`、`D`、`P` 三种上市状态。
- 保存源字段及本地 `snapshot_date`；主键为 `(snapshot_date, ts_code)`。
- `snapshot_date` 是本次观察日期，不是源披露的历史生效日期。
- 只从功能启用日起积累快照；禁止将今天的 `index_code/index_name` 反向填充到历史日期。

## 3. 因果时间和消费者边界

- `etf_share_size.trade_date = T` 的研究可用时点固定为下一交易日；本 PR 只保存原始事实，不实现消费者 join。
- `etf_basic_snapshots.snapshot_date = D` 只证明 D 日同步时观察到该元数据。
- 后续若构造 ETF 到指数或行业的历史映射，必须另行获得有效日期证据；本次快照不是有效期 PIT 表。
- 不生成 ETF 流量因子、不晋升因子库、不修改 R8、不增加月度数据集组件、不启动 QE/HMM 实验。

## 4. 数据库设计

### 4.1 `market.etf_share_size`

| 字段 | 类型 | 约束/语义 |
| --- | --- | --- |
| `trade_date` | `DATE` | 非空，交易日 |
| `ts_code` | `TEXT` | 非空，ETF 代码 |
| `total_share` | `NUMERIC` | 万份，可空 |
| `total_size` | `NUMERIC` | 万元，可空 |
| `nav` | `NUMERIC` | 元，可空 |
| `close` | `NUMERIC` | 元，可空 |

主键 `(trade_date, ts_code)`，按 `trade_date` 建 Timescale hypertable。

### 4.2 `market.etf_basic_snapshots`

除 `snapshot_date` 外保存 `etf_basic` 的 `ts_code`、名称、基准指数、设立/上市日期、状态、交易所、管理人、托管人、费率与 ETF 类型字段。主键 `(snapshot_date, ts_code)`，按 `snapshot_date` 建 Timescale hypertable。

迁移同时幂等注册 `market.data_stats_config`。迁移必须先在既有 DEV 数据库执行并回读；生产迁移保持独立授权，本功能合入不代表生产 DDL 已获授权。

## 5. 实施方案 / Implementation Plan

1. `DatasetSpec` 注册两个数据集；`etf_share_size` 支持 `init/incremental`，`etf_basic_snapshots` 仅支持 `init`。
2. `DatasetSpec.snapshot_date_column` 仅用于 SINGLE_CALL 快照：请求字段时排除该本地列，写入前注入执行日；不建立第二套同步服务。
3. 默认调度：
   - `etf_share_size`：每日 20:00 incremental；
   - `etf_basic_snapshots`：每日 20:05 init；
   - 22:00 复用 `_data_freshness_check`，23:00 复用 `_auto_retry_stale`。
4. `etf_share_size` 纳入既有轻量健康检查和交易日自动补齐；不增加资源门禁、覆盖率阈值或新审批。
5. 本地数据管理展示两个数据集；只有 `etf_share_size` 提供按日期 init/补齐，快照表不提供历史 auto-range。
6. 历史补录使用现有 `/api/ingestion/run` 的 `init` 日期范围，按交易日串行 upsert。不得复制研究产物代替正式源同步。
7. 历史补录完成后，运行一次只读校验脚本，报告日期范围、行数、主键重复、非有限数值、每日行数和源字段空值；该校验不是今后每次导出或月更的附加门禁。

## 6. 失败语义

- API/协议错误、日期错配、主键缺失、单日 0 行、单次达到 5000 行上限或写入失败：任务失败并保留既有审计记录，等待既有重试。
- 若份额行已发布但 `total_share` 或 `total_size` 整列尚未发布，已写原始行不删除，审计标记为 `low_coverage/required_source_field_unpublished` 并复用 23:00 重试；不因少量 ETF 合法空值设置百分比阻断。
- `nav/close/total_share/total_size` 可按源数据为空；不得补零、前填或伪造。
- 海外 ETF 晚到只导致当日重试/次日补齐，不删除已写数据。
- 快照同步失败不得回用昨日快照冒充当天观察。

## 7. 明确非目标

- 不接入季度 ETF 持仓；不把季度披露当日频持仓。
- 不构建 ETF-index 历史 PIT 映射，不用文本匹配结果作为 authority。
- 不修改 R8 candidate/profile，不导出 H5/Qlib/Parquet，不触发数据集发布。
- 不修改因子研究、QE、HMM、Selection、Paper 或 Advisory 代码。
- 不新增冻结、内容哈希、资源门禁或重复校验平台。

## Risks / Failure Modes / 风险

- Tushare 可能先发布当日份额、稍后才发布规模/净值/收盘价。整列核心字段未发布时保留原始行并标记既有 `low_coverage`，由现有链重试；不按 ETF 总数或非空比例建立新门禁。
- 少量跨境、新上市或特殊 ETF 可能长期缺少 `nav/total_size`；这些源空值在一次性校验中披露，不补零、不前填，也不阻断其它已发布记录。
- 基础信息快照只代表观察日状态，不能证明历史有效期；任何历史映射消费者必须另行取得 PIT 证据。
- 源码合入不代表生产 schema、历史数据或调度已经启用；生产 DDL、DML、后端重启分别验收。

## 8. Design Acceptance Index

| design_item | requirement |
| --- | --- |
| F-001 | DEV 验证的幂等迁移创建两张表、完整注释和 `data_stats_config`，生产 DDL 独立待授权。 |
| F-002 | `etf_share_size` 使用 BY_DATE、交易日序列、六个源字段和 `(trade_date, ts_code)` upsert；0 行/截断/日期错配显式失败。 |
| F-003 | `etf_basic_snapshots` 使用 SINGLE_CALL 的 L/D/P 参数集，并由通用最小扩展注入执行日；请求不得把本地 `snapshot_date` 发给 Tushare。 |
| F-004 | 20:00/20:05 默认调度、22:00 健康检查和 23:00 既有重试链可观测；只对核心字段整列未发布进行重试，不新增固定数量或百分比阈值。 |
| F-005 | API 与本地数据管理可发起正式同步；`etf_share_size` 支持显式历史 init 和补齐，快照不伪造历史 auto-range。 |
| F-006 | 只读历史校验工具检查范围、主键、非有限值、每日规模与空值，不写数据且不成为月度导出的新门禁。 |
| F-007 | 定向测试覆盖 DatasetSpec、快照注入、调度目录、路由参数、健康检查和迁移合同；通过 F1 feature validation 与 DESIGN-COMPLIANCE-001。 |

## 9. 验证方案 / Verification Plan

- Python AST/`py_compile`、Ruff、`git diff --check`。
- `backend/tests/test_tushare_sync_engine.py` 的 ETF 定向合同测试。
- 调度目录、ingestion API 和健康检查的定向测试。
- 迁移静态合同测试；随后在既有 DEV 数据库 apply/readback。
- 小范围 DEV 同步烟测先于任何历史补录；本轮不执行生产补录。
- `python scripts/aistock_feature_workflow.py validate --design <本文件> --tier F1`。
- `python -m nox -s validation_module_registry_l0` 与 `python -m nox -s l0`。
- 两轮正式审核：第一轮查设计/实现一致性、因果时间与越界；第二轮查失败语义、重复实现、测试价值和生产边界。

## 10. Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
| --- | --- | --- | --- | --- |
| F-001 | `backend/db/migrations/add_etf_share_sources_20260923.sql` 与 rollback | `backend/tests/test_etf_share_size_source.py::test_etf_migration_is_idempotent_and_registers_both_stats_rows`；DEV `aistock_dev` migration apply×2 及 schema/hypertable/comment/config readback | IMPLEMENTED_VERIFIED | none |
| F-002 | `backend/services/tushare_dataset_specs.py::ETF_SHARE_SIZE`、`TushareSyncEngine._sync_by_date` | `backend/tests/test_etf_share_size_source.py`；DEV 2026-09-18/22 各 1,674 行真实同步 | IMPLEMENTED_VERIFIED | none |
| F-003 | `DatasetSpec.snapshot_date_column`、`TushareSyncEngine._sync_single_call` | `backend/tests/test_etf_share_size_source.py::test_snapshot_date_is_local_only_and_not_requested_from_tushare`、`::test_single_call_injects_same_observation_date_before_upsert`；DEV L/D/P 共 1,829 行 | IMPLEMENTED_VERIFIED | none |
| F-004 | `backend/db/init_tushare_schedules.py`、`audit_backed_data_health.py`、`_etf_share_size_publication_quality` | `backend/tests/test_etf_share_size_source.py`；DEV 9 月 18 日 `ok`、9 月 22 日 `low_coverage/required_source_field_unpublished` readback | IMPLEMENTED_VERIFIED | none |
| F-005 | `backend/routers/ingestion.py`、`frontend/src/app/local-data/page.tsx` | 路由日期/模式回归；`python -m nox -s data_sync_autonomy_backend` | IMPLEMENTED_VERIFIED | none |
| F-006 | `scripts/validate_etf_share_size_source.py` | `backend/tests/scripts/test_validate_etf_share_size_source.py`；DEV read-only smoke | IMPLEMENTED_VERIFIED | none |
| F-007 | ownership/test-plan 登记及本文 | `python -m nox -s validation_module_registry_l0`；`python -m nox -s l0`；Ruff、py_compile、`git diff --check` | IMPLEMENTED_VERIFIED | none |

## 11. Production Gates

- `production_ddl_gate`: pending explicit authorization for the two named tables and config rows.
- `production_dml_gate`: pending explicit authorization for historical backfill.
- `backend_restart_required`: true after source merge; owner is user.
- `frontend_restart_required`: follows existing deployment process; no process control in this task.
- `dependency_gate`: noop.
- `dataset_release_activation`: noop.

## 12. DESIGN-COMPLIANCE-001

- [x] F-001～F-007 均有实现与可复核证据；没有 POC、placeholder 或手工旁路。
- [x] 交易日、源字段单位、次交易日研究可用语义和快照观察日语义保持一致，无未来数据回填。
- [x] 真实 DEV 迁移和 Tushare 小样本验证已执行；测试替身只覆盖错误边界，不冒充生产数据已完成。
- [x] 复用现有同步、审计、健康检查和重试链；没有第二套 writer、资源门禁、冻结、哈希或消费者逻辑。
- [x] R8、QE/HMM/Selection/Paper/Advisory、生产库和运行时均未修改；生产操作仍受独立授权约束。

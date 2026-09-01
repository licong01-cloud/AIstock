# 数据集低条数（low_coverage）自动重试门禁 F1 设计

日期：2026-08-22
分支：`feature/low-coverage-retry-gate-20260822`
模式：设计 + 实现同轮交付，本地门禁 + CI 通过后合入。

## Background / 背景

### 现状核实（只读核实，未重启服务、未写生产 DB、未执行 DDL/DML）

1. 2026-08 实测发现 cyq_perf 多个交易日只抓到半成品（49–3886 行，全天应有 5522–5543 行），
   同步引擎仍把当日审计写为 `quality_status='ok'`，健康检查判为 ok，不触发任何重试。
2. 重试消费链路已完整存在，无需新建：
   - `market.dataset_date_refresh_audit` 已有 `expected_rows / coverage_ratio / quality_status / failure_category` 列；
   - `backend/services/data_refresh_audit.py:record_success()` 已接受 `expected_rows / coverage_ratio / quality_status / failure_category` 参数；
   - `backend/services/audit_backed_data_health.py:299` 已把 `quality_status='low_coverage'` 映射为 `status='low_coverage'`（非 ok）；
   - `backend/ingestion/tdx_scheduler.py:_record_freshness_retry_targets()` 对非 ok 结果登记重试目标，23:00 `_run_auto_retry_stale` 按增量区间 upsert 重试；
   - `backend/services/data_completeness.py` 已有 `T_PLUS_1_TABLES={"margin_detail"}`（周一检查对象为周五数据）。
3. 唯一缺口：`backend/services/tushare_sync_engine.py` 在所有审计写入路径只会写
   `ok / empty_valid / empty_invalid / error`，**从不写 `low_coverage`**；
   `backend/services/tushare_dataset_specs.py:DatasetSpec` 也没有 per-dataset 最小条数配置。
4. 参考基线（2026-08 实测）：cyq_perf 完整日 5522–5543 行；margin_detail 完整日 4353–4394 行、
   仅上交所部分发布时 1998 行；sw_daily 完整日 131 行。

### 用户诉求

在现有重试基础上，对"今日有数据但条数不合理"的数据集判定为不完整，并以 upsert 方式自动重试
（示例阈值：筹码数据 < 5000 条触发重试）。

## Scope / 范围

- `backend/services/tushare_dataset_specs.py`：DatasetSpec 新增可选字段，并为三个数据集配置阈值。
- `backend/services/tushare_sync_engine.py`：BY_DATE 与 BY_CODE 两条同步路径的审计写入判定。
- `backend/tests/test_tushare_sync_engine.py`：新增单元测试。
- 本设计文档与验收矩阵。

## Non-Goals / 非目标

- 不新建重试/告警机制（复用现有 18:30 freshness check → retry target → 23:00 auto retry 链路）。
- 不修改 `_sync_by_period`（稀疏事件类数据集无条数阈值需求）。
- 不修改 physical audit seed 路径（历史种子数据不追溯打标，避免意外批量重试）。
- 不为 index_daily 配置阈值（其日行数随指数池口径变化，`data_completeness._expected_rows` 已显式跳过其覆盖率检查）。
- 不调整任何调度时刻、不删除任何数据（重试全程 upsert）。
- 不做 DDL（audit 表现有列已够用）。

## Design Acceptance Index / 设计验收索引

- F-001：`DatasetSpec` 新增 `min_expected_rows: Optional[int] = None`，默认 None 时行为与现状完全一致。
- F-002：BY_DATE 路径（`_sync_by_date`）审计写入：配置了阈值且 `0 < written_rows < min_expected_rows` 时，
  写 `quality_status='low_coverage'`、`failure_category='low_coverage'`、`expected_rows=min_expected_rows`、
  `coverage_ratio=written_rows/min_expected_rows`；`written_rows >= min_expected_rows` 仍写 `ok`；0 行逻辑不变。
- F-003：BY_CODE 路径（`_record_by_code_audit`）成功分支同样判定：`result.ok and row_count > 0` 且低于阈值时
  写 `low_coverage`（仍走 `record_success`，因为同步本身成功，只是条数不足）。
- F-004：阈值配置：cyq_perf=5000、margin_detail=4000、sw_daily=100；index_daily 及其余数据集不配置。
- F-005：单元测试覆盖：未配置阈值不触发；等于阈值不触发；低于阈值触发（BY_DATE 与 BY_CODE 各一）；
  0 行仍为空值语义（empty_valid/异常分支不被改变）。

## Implementation Plan / 实施方案

1. `DatasetSpec` 增加字段 `min_expected_rows: Optional[int] = None`（带注释说明语义）。
2. `tushare_sync_engine.py` 新增私有辅助 `_audit_quality_for_rows(spec, rows)`：
   返回 `(quality_status, failure_category, expected_rows, coverage_ratio)` 四元组；
   未配置阈值或 rows>=阈值时返回 `("ok", None, None, None)`；rows==0 由调用方既有分支处理，辅助函数不介入。
3. `_sync_by_date` 成功写入处与 `_record_by_code_audit` 成功分支接入该辅助函数。
4. 三个 DatasetSpec 配置阈值。
5. 单元测试：mock `_refresh_audit.record_success` 断言 low_coverage 写参。

## Verification Plan / 验证方案

- 变更文件 Ruff lint + py_compile。
- `python -m pytest backend/tests/test_tushare_sync_engine.py -q`（新增用例 + 既有用例回归）。
- `nox -s data_sync_autonomy_backend`（local_data 模块相关小矩阵）。
- `git diff --check`。
- 运行时生效依赖用户重启 8001 后端；重启后验证方式为只读观察：后续交易日 cyq_perf 若半成品落库，
  audit 应出现 `quality_status='low_coverage'` 且 23:00 自动重试（本 PR 不含运行时验证，标 pending）。

## Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|-------------|---------------------|------------------|--------|------------------|
| F-001 | backend/services/tushare_dataset_specs.py (DatasetSpec.min_expected_rows) | pytest backend/tests/test_tushare_sync_engine.py (46 passed) | 已验证 | - |
| F-002 | backend/services/tushare_sync_engine.py (_sync_by_date + _audit_quality_for_rows) | pytest backend/tests/test_tushare_sync_engine.py -k min_expected_rows (4 passed) | 已验证 | - |
| F-003 | backend/services/tushare_sync_engine.py (_record_by_code_audit) | pytest backend/tests/test_tushare_sync_engine.py -k record_by_code_audit (2 passed) | 已验证 | - |
| F-004 | backend/services/tushare_dataset_specs.py (CYQ_PERF=5000/MARGIN_DETAIL=4000/SW_DAILY=100) | pytest backend/tests/test_tushare_sync_engine.py::test_chip_and_margin_specs_carry_min_expected_rows (passed) | 已验证 | - |
| F-005 | backend/tests/test_tushare_sync_engine.py (6 新增用例) | pytest backend/tests/test_tushare_sync_engine.py + nox -s data_sync_autonomy_backend (180 passed, 1 skipped) | 已验证 | - |

## Risks / 风险与失败模式

- 阈值随上市公司数量增长而过低失效：接受，阈值为静态配置，可后续调整；metadata 中记录阈值便于排查。
- 上游当日只发布部分数据（如 SZSE 周五 margin_detail 不发布）：23:00 重试仍只得部分数据，
  audit 继续 low_coverage，次日 freshness check 对 margin_detail 因 T+1 语义再次判定并重试，可自愈；不重试风暴（每日至多每数据集一次自动重试）。
- 误判 ok→low_coverage 仅影响重试调度，不影响已落库数据，无删除风险。
- BY_CODE 数据集一次任务覆盖多日时，按日落库行数逐日判定，部分日 low_coverage 不影响其他日 ok。

## Production Gates / 生产门禁

- production_ddl_gate：noop（无 schema 变更，audit 表列已存在）。
- backend_dependency：需要用户重启 8001 后端后生效（引擎代码由调度进程在启动时加载）。
- frontend_dependency：noop。
- 数据写入：仅审计表新增行的 quality_status 取值变化 + 既有自动重试链路的 upsert，无删除操作。

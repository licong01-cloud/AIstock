# StrategyPackage 因子库分级保护批4设计（2026-07-01）

## Background

已合入的资产固化链路让新建 StrategyPackage 拥有包自有 `factor_code` / model 资产，运行时不再依赖 QE 源。批4要补齐因子库侧保护：硬删因子是破坏性操作，若该因子仍被非 `RETIRED` StrategyPackage 引用，删除会破坏审计与可追溯；`deprecate` 只是停止新实验选择，不抹除代码或包资产，应继续放行。

现状取证（来自当前代码与战略 session）：
- `backend/routers/quantevolver.py::delete_factor` 在查到 `aistock_factor_catalog.id` 后直接删除指标、分类、QE 记录、catalog 与缓存，没有 StrategyPackage 引用检查。
- `backend/routers/factor_library.py::get_usage_summary` 只展示指标版本/批次数，不展示 StrategyPackage 引用。
- `backend/routers/factor_library.py::deprecate_confirmed` 仅更新 `is_available=FALSE`，语义为非破坏性退选路径，必须继续允许被引用因子 deprecate。

## Scope

本批只实现因子库分级保护：
1. 硬删 guard：`DELETE /quantevolver/factors` 在任何 DELETE 前阻止删除被非 `RETIRED` StrategyPackage 引用的因子。
2. usage-summary 可见性：`GET /factor-library/factors/{factor_name}/usage-summary` 增加 StrategyPackage 引用摘要。
3. deprecate 放行：`/factor-library/deprecate-confirmed` 不新增引用 guard，并用回归测试固化该语义。

## Non-Goals

- 不改 StrategyPackage freeze/self-check/runtime/manifest 代码。
- 不做批5候选退役、批6删源 guard。
- 不新增 DDL，不写生产 DB，不启/重启服务。
- 不改变因子硬删既有级联删除顺序；仅在删除前新增保护检查。

## Architecture

新增共享只读模块 `backend/services/strategy_package/factor_reference_guard.py`，由因子库 router 与 quantevolver router 共同调用，避免两处 SQL 漂移。

引用判定取两来源并集：
- `strategy_pkg.package_asset`：`lower(asset_type)='factor_code'` 且 `metadata->>'logical_name' = factor_name`。
- `strategy_pkg.package.manifest_json->'factor_set'`：数组中任一元素 `->>'factor_name' = factor_name`。

返回结构按 `package_id + package_status` 去重，并合并 `reference_sources`（`package_asset` / `manifest`）。硬删 guard 默认 `include_retired=False`，只阻止非 `RETIRED` 包；usage-summary 使用同一默认口径展示会阻止硬删的活跃保护引用。

`RETIRED` 决策：不阻止硬删。理由：`RETIRED` 包已处于淘汰链路，不应继续保护因子库 catalog；仍保留包自有资产与 status/event 审计。若未来需要查看 retired 引用，可在只读查询扩展参数，但本批不改变硬删策略。

## Contracts

### API: `DELETE /api/v1/quantevolver/factors`

注入点：`quantevolver.py::delete_factor` 查到 `catalog_id` 后、任何 DELETE/UPDATE 之前。

若存在非 `RETIRED` 引用，返回 HTTP 409，body：

```json
{
  "reason_code": "factor_delete_blocked_referenced_by_strategy_package",
  "message": "factor is referenced by non-retired StrategyPackage packages",
  "factor_name": "factor_x",
  "source": "qe",
  "referenced_packages": [
    {
      "package_id": "pkg_...",
      "package_status": "SELECTION_ENABLED",
      "reference_sources": ["package_asset", "manifest"]
    }
  ]
}
```

DB 异常不得吞掉或 fail-open；异常传播到现有 500 路径并 rollback。日志记录 `factor_name`、`source`、引用包清单。

### API: `GET /api/v1/factor-library/factors/{factor_name}/usage-summary`

保留现有 summary envelope，新增 `strategy_package_references`：

```json
{
  "referenced": true,
  "count": 1,
  "packages": [
    {"package_id": "pkg_...", "package_status": "BACKTEST_APPROVED", "reference_sources": ["manifest"]}
  ],
  "blocking_policy": "non_retired_packages_block_hard_delete",
  "reason_code": "factor_delete_blocked_referenced_by_strategy_package"
}
```

### Deprecate Contract

`/factor-library/deprecate-confirmed` 不调用 guard；被引用因子可成功 `is_available=FALSE`。

## Design Acceptance Index

| 设计项 | 标题 | 章节 |
|---|---|---|
| F-001 | 共享 StrategyPackage 因子引用查询，两来源取并集 | Architecture |
| F-002 | 硬删被非 RETIRED 包引用的因子返回 409 且不改数据 | API: delete |
| F-003 | deprecate 被引用因子继续放行 | Deprecate Contract |
| F-004 | usage-summary 展示 StrategyPackage 引用摘要 | API: usage-summary |
| F-005 | fail-closed/no-silent：查询失败不得放行硬删 | Risks / Verification |

## Implementation Plan

1. 新增 `backend/services/strategy_package/factor_reference_guard.py`：
   - `FACTOR_DELETE_BLOCKED_REASON_CODE` 常量。
   - `StrategyPackageFactorReference` dataclass。
   - `find_strategy_packages_referencing_factor(conn, factor_name, include_retired=False)` 使用调用方传入连接查询 `package_asset` 和 `manifest_json.factor_set`，合并来源并排序。
   - `strategy_package_references_summary(...)` 供 usage-summary 序列化。
2. 修改 `backend/routers/quantevolver.py`：
   - 在 `catalog_id` 获取后调用共享查询。
   - 有引用时 `conn.rollback()` 并抛 HTTP 409；不进入任何 DELETE/缓存清理。
3. 修改 `backend/routers/factor_library.py`：
   - usage-summary 读取共享查询并加入 `extra.strategy_package_references`。
   - 不改 `deprecate_confirmed` 控制流。
4. 增加 pytest 覆盖三杀手与两来源并集。

## Verification Plan

- L0 static：`python -m compileall backend/services/strategy_package/factor_reference_guard.py backend/routers/factor_library.py backend/routers/quantevolver.py`；`python -m ruff check <changed files>`；`git diff --check`。
- L1 unit/router：
  1. 硬删被非 `RETIRED` 包引用的因子 -> HTTP 409，body 含 reason_code/package/source，DELETE 语句未执行，factor catalog 未变。
  2. 同一被引用因子走 deprecate -> 成功返回，执行 `UPDATE aistock_factor_catalog SET is_available=FALSE`。
  3. 硬删未被任何包引用 -> 保持既有级联删除成功路径。
  4. package_asset-only + manifest-only 引用都出现在 usage-summary，且 retired-only 不阻止硬删。
- Workflow：`python scripts/aistock_feature_workflow.py validate --design docs/analysis/strategy_package_factor_library_protection_batch4_design_20260701.md --tier F2`。

## Design Acceptance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/strategy_package/factor_reference_guard.py` | `backend/tests/strategy_package/test_factor_library_protection_batch4.py::test_reference_query_merges_package_asset_and_manifest_sources` | done | - |
| F-002 | `backend/routers/quantevolver.py::delete_factor` | `test_delete_factor_referenced_by_non_retired_package_returns_409_and_does_not_delete` | done | - |
| F-003 | `backend/routers/factor_library.py::deprecate_confirmed` unchanged guard-free path | `test_deprecate_referenced_factor_is_allowed` | done | - |
| F-004 | `backend/routers/factor_library.py::get_usage_summary` | `test_usage_summary_includes_strategy_package_references` | done | - |
| F-005 | guard query called before deletes; exceptions not caught as allow; existing rollback 500 path preserved | `test_reference_query_failure_propagates_before_delete` | done | - |

## Rollout / Rollback

- Rollout：纯代码/API 行为变更；合并后用户按需重启 backend 生效，本任务不启服务。
- Rollback：回滚本 PR 即恢复硬删无 StrategyPackage guard 与旧 usage-summary。无 DDL/DML 回滚项。

## Risks

| 风险 | 影响 | 缓解 |
|---|---|---|
| guard 查询失败被误当无引用 | 误删仍被包引用的因子 | 不捕获为成功；异常传播并 rollback，fail-closed |
| 只查 package_asset 或只查 manifest | 漏阻止历史/过渡包 | 共享函数固定两来源并集合并 |
| RETIRED 包引用长期阻塞清理 | 退役资产拖累因子库治理 | 硬删只按非 RETIRED 阻止；usage-summary 声明 blocking_policy |
| manifest JSON 查询性能 | 删除/usage 变慢 | 先按 `package_status <> 'RETIRED'` 过滤；StrategyPackage 包量小，删除/deprecate 非高频；不扫描因子 catalog |

## Production Gates

- `production_ddl_gate=noop`：不新增表/列/约束。
- `production_dml_gate=noop`：不写生产 DB；测试使用 fake connection。
- `production_frontend_dependency_gate=noop`。
- `production_backend_dependency_gate=noop`。
- 服务重启：本任务不启/重启服务；合并后由用户决定运行时重启窗口。

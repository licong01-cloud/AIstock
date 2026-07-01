# StrategyPackage Asset Freeze Batch 4 F2 子设计（2026-06-30）

## Background / 背景

总设计 `docs/analysis/strategy_package_asset_freeze_and_candidate_retirement_f2_design_20260630.md` 已把因子库分级保护列为 F-005：StrategyPackage 包一旦引用某个因子，该因子代码需要作为包运行时资产保留；因子库仍可通过 `is_available=FALSE` 退役，阻止新实验继续选用，但硬删必须拒绝，避免破坏已建包的可审计性与 Batch 1/2/3 的自包含资产链路。

本批次只交付 `[4]` 因子库分级保护，独立于 Batch 1/2/3；不触碰 `qe_archive`、不启停服务、不执行生产 DDL/DML。

## Scope / 范围

- 扩展 `factor_library_get_usage_summary`：返回该因子是否被 StrategyPackage 引用、引用包数量、引用来源和样例包引用。
- 在 `backend/routers/quantevolver.py` 的硬删路径前增加 StrategyPackage 引用 guard。
- 保持 `factor_library.py` 的 deprecate 语义：被包引用时仍允许 `is_available=FALSE`，并在 plan 响应中提示引用情况。
- 查询来源覆盖 `strategy_pkg.package_asset.asset_type='factor_code'` 与 `strategy_pkg.package.manifest_json.factor_set`，兼容 Batch 1 之前的 manifest-only 包引用。

## Non-Goals / 边界

- 不改变 StrategyPackage 建包、运行时读取、回填或 candidate 退役逻辑。
- 不新增 DB 表、列、索引或 migration。
- 不删除任何历史因子或包资产；不执行生产写入。
- 不阻止 deprecate；deprecate 仅影响未来因子选择，不抹除已有包引用。

## Architecture / 架构

### 查询与保护链路

1. 新增只读查询模块 `backend/services/strategy_package/factor_usage.py`。
2. `find_strategy_package_factor_usage(factor_name)` 同时扫描：
   - `strategy_pkg.package_asset` 中 `asset_type='factor_code'`，按 metadata `logical_name/factor_name/factor_id`、`asset_ref` logical_name 与 `source_uri` 匹配。
   - `strategy_pkg.package.manifest_json->'factor_set'` 中的 `factor_name/factor_id`。
3. usage summary 端点把查询结果作为 `strategy_package_usage` 返回。
4. hard delete 在开始级联删除前调用同一查询：
   - `protected=true`：返回 HTTP 409，`reason_code=factor_referenced_by_strategy_package`，包含 `factor_name/source/package_count/reference_count/sample_references/allowed_action`。
   - usage 查询异常：fail-closed 返回 HTTP 500，`reason_code=strategy_package_factor_usage_check_failed`，不执行任何 delete。

## Contracts / API/DB/UI/MCP 契约

- `GET /api/v1/factor-library/factors/{factor_name}/usage-summary` 增加 `strategy_package_usage` 字段；原有 metrics summary 仍保留。
- `POST /api/v1/factor-library/deprecate-plan` 增加 `strategy_package_usage` 与 `deprecate_policy=allowed_even_when_referenced_by_strategy_package`。
- `POST /api/v1/factor-library/deprecate-confirmed` 行为不变，仍只写 `is_available=FALSE`。
- `DELETE /api/v1/quantevolver/factors` 对被包引用因子返回 409；对 usage 查询失败返回 500 fail-closed。
- DB 契约只读既有表：`strategy_pkg.package_asset`、`strategy_pkg.package`、`aistock_factor_catalog`；无 DDL。

## Design Acceptance Index / 设计验收索引

| id | requirement | refs |
|---|---|---|
| F-001 | usage summary 接入 StrategyPackage 引用查询，覆盖 package_asset 与 manifest factor_set | 总设计 F-005 / §4.6 |
| F-002 | hard delete 被 StrategyPackage 引用因子时拒绝并返回显式 reason/context | 总设计 F-005 / §4.6 |
| F-003 | deprecate 被引用因子仍允许，plan 明示引用但不阻断退役 | 总设计 F-005 / §4.6 |
| F-004 | usage 查询失败时 fail-closed，不能 silent 允许删除 | 总设计 §8 |
| F-005 | 本批不新增 DDL/DML、不启停服务、不触碰 qe_archive | 总设计 §9 |

## Implementation Plan / 实施方案

1. 增加 `strategy_package.factor_usage` 只读查询与错误类型。
2. 更新 `factor_library.get_usage_summary` 与 `plan_deprecate` 响应。
3. 更新 `quantevolver.delete_factor`，在任何级联 delete 前执行引用 guard。
4. 新增 L1 单元测试覆盖 package_asset/manifest 查询、usage summary、deprecate plan、hard delete blocked、usage 查询 fail-closed。
5. 运行 F2 validator、compileall、目标 pytest、相关 mcp/factor tests、diff check。

## Verification Plan / 验证方案

- F2：`rtk python scripts/aistock_feature_workflow.py validate --design docs/architecture/strategy_package_factor_library_protection_batch4_f2_design_20260630.md --tier F2`。
- L0：`rtk python -m compileall -q backend/services/strategy_package backend/routers backend/tests/quantevolver/test_factor_library_strategy_package_protection.py`。
- L1：`rtk python -m pytest backend/tests/quantevolver/test_factor_library_strategy_package_protection.py -q`。
- 回归：`rtk python -m pytest backend/tests/mcp/test_domain_modules.py backend/tests/quantevolver/test_payload_summary.py -q`。
- 静态：changed-file ruff、`git diff --check`、grep 确认无 DDL/migration 与 qe_archive 改动。

## Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/strategy_package/factor_usage.py`; `backend/routers/factor_library.py` | `test_strategy_package_factor_usage_includes_ledger_and_manifest_refs`; `test_usage_summary_exposes_strategy_package_references` | verified | - |
| F-002 | `backend/routers/quantevolver.py`; `backend/services/strategy_package/factor_usage.py` | `test_hard_delete_referenced_factor_is_blocked_before_delete` 断言 409、reason_code、未执行 `DELETE FROM aistock_factor_catalog` | verified | - |
| F-003 | `backend/routers/factor_library.py` | `test_plan_deprecate_allows_strategy_package_referenced_factor` 断言 referenced factor 仍 `will_write=True` 且 policy 明示允许 | verified | - |
| F-004 | `backend/routers/quantevolver.py`; `backend/services/strategy_package/factor_usage.py` | `test_hard_delete_fails_closed_when_usage_check_fails` 断言 500 fail-closed、无 delete、rollback；targeted pytest 7 passed | verified | - |
| F-005 | no migration files; no qe_archive files | F2 validate PASS；`git diff --name-only` 仅本批 backend/test/doc/validation 证据；`production_ddl_gate=noop` / `production_dml_gate=noop` | verified | - |

## Rollout / Rollback

- Rollout：合并后立即保护 hard delete；deprecate 和查询端点仍可用。
- Rollback：回滚本批代码即可恢复旧行为；无 DB 迁移、无生产数据变更需要回滚。
- 运行时：需要用户按常规窗口重启后端才会生效，本批不启停服务。

## Risks / 风险

- manifest-only 匹配可能因同名因子 source 不明确而偏保守；设计选择 fail-closed，避免误删运行时需要的因子。
- package_asset `logical_name` 缺失时依赖 URI/source_uri 辅助匹配；同时扫描 manifest factor_set 兜底。
- 删除 guard 增加一次只读查询；硬删是低频治理操作，性能影响可接受。

## Production Gates / 生产门禁

- `production_ddl_gate=noop`：无 schema/migration 变更。
- `production_dml_gate=noop`：不执行生产写入，不删除因子。
- `production_backend_dependency_gate=noop`。
- `production_frontend_dependency_gate=noop`。


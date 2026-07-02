# 多Alpha LocalSim 准入放宽与 UI 打通 F2 设计

日期：2026-07-02  
分支：`feature/multi-alpha-localsim-admission-ui-20260702`  
模式：设计先行，本轮不写实现代码，等待 Tier2 评审后再进入实现。

## Background / 背景

### 现状核实

本轮只读核实现有代码，未启动/重启服务，未写生产 DB，未执行 DDL/DML。

1. 多Alpha 父包 promotion 仍在 frozen manifest 中固化 `source_evidence.multi_alpha.paper_admission.blocking=["multi_alpha_runtime_not_validated_until_dry_run"]`：
   - `backend/services/strategy_package/multi_alpha_promotion.py` 中 `_paper_admission()` 返回 `eligible=false + blocking`。
   - 相关回归测试 `backend/tests/strategy_package/test_multi_alpha_promotion.py` 断言该 blocker 会进入 manifest 与 API payload。
2. 当前准入硬门在 `backend/services/strategy_package/asset_eligibility.py`：
   - `MULTI_ALPHA_PAPER_ADMISSION_BLOCKER = "multi_alpha_runtime_not_validated_until_dry_run"`。
   - `_multi_alpha_runtime_blockers()` 只要发现该 blocker，就按 `(package_id, manifest_sha256, broker_backend, runtime_variant)` 查询 `MultiAlphaPaperAdmissionRepository.get_eligible()`。
   - 查到 admission 才 `PASS`；查不到则 `FAIL/hard`，导致 `summarize().eligible=False`。
   - admission 查询异常目前 fail-closed 并在 context 写 `admission_lookup_error`，不是 silent。
3. 选股与模拟盘入口都受该 eligibility 影响：
   - Selection full path：`backend/services/selection_center/service.py:list_selectable_packages()` 对每个 package 调 `asset_eligibility_service.summarize(record)`，`eligible=False` 直接过滤。
   - Selection summary path：`backend/services/strategy_package/repository.py:list_summaries()` 用 SQL 判断 `summary_asset_eligible`，当前对 `local_sim` 也要求 admission，否则把 blocker 写入 `summary_asset_blockers`。
   - Paper create path：`backend/services/paper_trading_v2/service.py:create_portfolio()` 调 `self.asset_eligibility_service.require_eligible(record, broker_backend=broker_backend)`。
4. dry-run 能力已经存在但仅 LocalSim：
   - `backend/routers/strategy_packages.py` 暴露 `POST /strategy-packages/{package_id}/paper-runtime-dry-run`。
   - `backend/services/strategy_package/multi_alpha_paper_dry_run.py` 要求 confirm token `MULTI_ALPHA_LOCALSIM_DRY_RUN`，且 `broker_backend != "local_sim"` 时 fail-loud：`multi_alpha_dry_run_unsupported_broker`。
   - 该 validator 复用 selection artifact、runtime snapshot、target engine、rebalance engine，并做 deterministic replay。
5. UI 断链已确认：
   - `frontend/src/app/paper-v2/packages/page.tsx` 仅在建包后与 NoticePanel 文案中提示用户手动 POST dry-run。
   - `frontend/src/lib/paper-v2/api.ts` 还没有 `paper-runtime-dry-run` API helper。
   - packages 页只有跳转 `/paper-v2/portfolios?package_id=...` 的入口；当前 `portfolios/page.tsx` 自身可创建 portfolio，但 packages 页没有对 multi_alpha LocalSim/MiniQMT 准入差异给出真实操作入口。
6. 已合入的自包含 build gate 可作为 LocalSim 放宽后的安全兜底之一：
   - `backend/services/strategy_package/frozen_runtime_self_check.py` 已在单包、组件包、多Alpha promotion 路径中校验 self-contained runtime。
   - 设计文档 `docs/analysis/strategy_package_freeze_completeness_and_build_gate_f2_design_20260701.md` 记录该门禁，避免把缺 runtime asset 的 package 视为可运行。

### 决策变更

旧设计 `docs/analysis/qe_to_paper_chain_closure_gap_and_multi_alpha_paper_admission_design_20260628.md` 把 LocalSim dry-run admission 设计为硬门；本轮根据战略 session 新判定调整为：

- LocalSim 是纯模拟撮合，无真钱/真实券商副作用。缺少 dry-run admission 不再是阻断项；LocalSim 的真实验证由“运行即验证”承担，且建包自包含 self-check 已兜住 frozen runtime asset。
- MiniQMT / live 仍必须 fail-closed。当前 dry-run 端点也只支持 `local_sim`，因此 MiniQMT 缺 admission 或不支持 dry-run 必须继续 loud 阻断，不得借 LocalSim 放宽旁路。
- 放宽只针对 reason_code `multi_alpha_runtime_not_validated_until_dry_run` 且 `broker_backend=local_sim` 的缺 admission 情况；manifest hash、retired、asset/self-check、未知 paper admission blocker 等硬门不放宽。

## Scope / 范围

本设计覆盖：

1. 后端 eligibility 语义：LocalSim 缺 dry-run admission 从 `FAIL/hard` 改为显式 `WARN/warning` 或等价非阻断 check；MiniQMT/live 维持 `FAIL/hard`。
2. Selection summary SQL 语义：summary path 与 full path 对 LocalSim 缺 dry-run admission 保持一致，不再把该 blocker 写成 ineligible。
3. Paper v2 packages UI：给 multi_alpha 父包提供真实 dry-run 按钮、结果展示与 LocalSim 建组合入口；MiniQMT 保留 dry-run/准入说明并 fail-loud 展示后端 reason_code。
4. 前端 API helper 与类型：新增 `strategyPackageApi.paperRuntimeDryRun()`，不新增依赖。
5. 测试与验证矩阵：覆盖 LocalSim 放行、MiniQMT 阻断、其它 blocker 不放宽、单Alpha 零回归、UI 真实按钮调用。

## Non-goals / 边界

1. 不伪造或写入 `strategy_pkg.multi_alpha_paper_admission`；LocalSim 放宽不是插入 admission，而是 eligibility 对缺 admission 的 venue-aware 判定。
2. 不放宽 MiniQMT / live；不实现 MiniQMT dry-run validator；不触碰 MiniQMT 路线 A 执行层。
3. 不改 `PaperPortfolio.package_id` 单 package 主契约。
4. 不改 multi_alpha promotion 的 frozen manifest 契约，不重写既有父包 manifest。
5. 不改 single_alpha 流程；`alpha_mode != multi_alpha` 仍提前 return。
6. 不改已合固化相关设计/实现（如 #1792）与因子库保护（如 #1799）。
7. 不新增 DDL/DML；不写生产 DB；不启/重启服务。
8. 不触碰 `research-assistant`。

## Architecture / 架构

### 后端准入语义

`StrategyPackageAssetEligibilityService.summarize(record, broker_backend="local_sim", runtime_variant=None)` 继续是统一入口。调整只发生在 `_multi_alpha_runtime_blockers()` 内：

1. `alpha_mode != "multi_alpha"`：维持现状，直接 `return []`。
2. `blocking` 中非 `multi_alpha_runtime_not_validated_until_dry_run` 的其它 reason：维持 `FAIL/hard`，不放宽。
3. `reason == multi_alpha_runtime_not_validated_until_dry_run`：
   - 如果 admission 存在：维持 `PASS/hard`，context 保留 `admission_id/dry_run_run_id/validated_at`。
   - 如果 admission 不存在且 `broker_backend == "local_sim"`：返回显式非阻断 check，例如：
     - `name = "multi_alpha_runtime_not_validated_until_dry_run"`
     - `status = "WARN"`
     - `severity = "warning"`
     - `message = "MULTI_ALPHA LocalSim does not require blocking dry-run admission; runtime will validate via LocalSim run and frozen self-check"`
     - `context.reason_code = "multi_alpha_localsim_dry_run_not_required"`
     - `context.original_blocker = "multi_alpha_runtime_not_validated_until_dry_run"`
     - `context.broker_backend = "local_sim"`
     - `context.runtime_variant = resolved_variant`
   - 如果 admission 不存在且 `broker_backend != "local_sim"`：维持 `FAIL/hard`，context 写原 reason、broker、runtime variant，MiniQMT 不放宽。
   - 如果 admission lookup 抛异常：
     - `broker_backend == "local_sim"` 时也不得 silent：建议仍产生 WARN，并在 context 带 `admission_lookup_error` 与 `reason_code=multi_alpha_localsim_dry_run_not_required`。原因是 LocalSim 不依赖 admission 表即可放行；错误被可见记录但不阻断。
     - `broker_backend != "local_sim"` 时维持 fail-closed hard FAIL，并带 `admission_lookup_error`。

### Selection summary SQL 对齐

`StrategyPackageRepository.list_summaries()` 当前用 SQL 直接把缺 `local_sim` admission 的 multi_alpha 标成 ineligible。实现阶段必须同步调整：

- `summary_asset_eligible` 对 LocalSim 视角不再因 `paper_admission_blocking ? 'multi_alpha_runtime_not_validated_until_dry_run'` 且无 admission 而 false。
- `summary_asset_blockers` 对 LocalSim 不再返回该 blocker；可选在 `asset_eligibility.warnings` 或新增 summary 字段里提示 `multi_alpha_localsim_dry_run_not_required`，但不得影响 `eligible`。
- 若未来 summary 支持 `broker_backend` 参数，再按 broker 透传；本轮最小实现保持 summary path 的默认 LocalSim 语义与 full path 一致。

### Paper create path

`PaperTradingV2PortfolioService.create_portfolio()` 已把 `broker_backend` 传给 `require_eligible()`。实现阶段只需确保：

- `broker_backend="local_sim"`：缺 dry-run admission 不阻断 create_portfolio；其它硬门仍阻断。
- `broker_backend="minqmt_sim"`：缺 dry-run admission 仍阻断。
- 不改变 `PAPER_V2_CREATABLE_BROKER_BACKENDS`、不改 broker/data_source 校验、不改 execution policy 校验。

### UI 通路

在 `frontend/src/app/paper-v2/packages/page.tsx` 保持现有 Paper v2 页面风格与组件，不引入新视觉体系。新增一个 multi_alpha 准入操作区：

1. 识别 multi_alpha：
   - `selected.alpha_mode === "multi_alpha"` 或 `selected.asset_eligibility.checks/context` 中存在 multi_alpha 信息。
2. LocalSim 默认提示：
   - 显示“LocalSim 不再要求阻断式 dry-run；可直接创建 LocalSim 模拟盘；dry-run 仍可作为可选留证”。
   - “用此包创建 LocalSim 模拟盘”按钮跳转 `/paper-v2/portfolios?package_id=<id>&broker_backend=local_sim&top_k=<variant>`，或沿用现有 package_id 跳转并在 portfolios 页读取 query 参数。
3. dry-run 真实按钮：
   - 表单字段：`broker_backend`（本轮选项 `local_sim`、`minqmt_sim`）、`runtime_variant`（`top_k=25/50`）、`trade_date`、`initial_cash`。
   - 点击调用 `strategyPackageApi.paperRuntimeDryRun(package_id, payload)`。
   - `local_sim` 成功时展示 `dry_run_run_id/admission_id/target_count/order_intent_count/artifact_shas` 摘要。
   - `minqmt_sim` 当前后端会 fail-loud（unsupported broker 或缺 admission），UI 要展示 `errorCode` 与 `context`，不得吞掉或只显示“失败”。
4. 建包后的 next_step 文案更新：
   - 不再说 LocalSim 必须手工 POST 清门。
   - 改为：LocalSim 可直接进入模拟盘；如需留证可在当前页点击 dry-run；MiniQMT/真实 paper 仍需专门准入。
5. portfolios 页 query 串联：
   - `frontend/src/app/paper-v2/portfolios/page.tsx` 已有 create portfolio 表单和 `paperV2Api.createPortfolio()`。
   - 实现阶段建议读取 `package_id` query 参数自动选中 package；如同时传 `broker_backend=local_sim`，显式固定/展示 LocalSim。
   - `paperV2Api.createPortfolio()` 已支持 `broker_backend?: "local_sim" | "minqmt_sim"`，但当前 UI 未传；若要支持 MiniQMT 选择，需要单独显式字段并避免默认误触。

## Contracts / 契约

### Backend checks

| 名称 | status/severity | 适用条件 | 说明 |
|---|---|---|---|
| `multi_alpha_runtime_not_validated_until_dry_run` | `PASS/hard` | 任意 broker，admission 命中 | 维持现状 |
| `multi_alpha_runtime_not_validated_until_dry_run` | `WARN/warning` | `broker_backend=local_sim` 且缺 admission | 新语义；`eligible=True`，context 必须显式含 `reason_code=multi_alpha_localsim_dry_run_not_required` |
| `multi_alpha_runtime_not_validated_until_dry_run` | `FAIL/hard` | `broker_backend=minqmt_sim` 或 future live 且缺 admission | 维持 fail-closed |
| 其它 manifest blocker | `FAIL/hard` | 任意 broker | 不放宽 |

新增 reason_code：

- `multi_alpha_localsim_dry_run_not_required`：LocalSim 缺 dry-run admission 已被显式非阻断处理。

保留 reason_code：

- `multi_alpha_runtime_not_validated_until_dry_run`：MiniQMT/live 缺 admission 的阻断 reason。
- `multi_alpha_dry_run_unsupported_broker`：dry-run API 对非 LocalSim 请求的 fail-loud reason。

### API

复用既有端点，不新增后端路由：

```http
POST /api/v1/strategy-packages/{package_id}/paper-runtime-dry-run
```

前端新增 helper：

```ts
strategyPackageApi.paperRuntimeDryRun(packageId: string, payload: {
  broker_backend: "local_sim" | "minqmt_sim";
  trade_date: string;
  runtime_variant: "top_k=25" | "top_k=50";
  confirmation: "MULTI_ALPHA_LOCALSIM_DRY_RUN";
  validated_by?: string;
  runtime_config?: JsonObject;
  initial_cash?: number;
}): Promise<JsonObject>
```

失败必须通过现有 `PaperV2ApiError` 透传 `errorCode/context`。

### UI

- 只使用现有 Paper v2 页面组件：`SectionCard`、`NoticePanel`、`MetricCard`、`PaperTable`、`StatusBadge`、`ErrorPanel`、`pv2-*` class。
- 不新增设计系统、不新增依赖、不改全局 CSS 视觉方向。
- 不以 raw JSON 作为主视图；artifact/context 可作为调试摘要展示。

## Design Acceptance Index / 设计验收索引

| design_item | 标题 |
|---|---|
| F-001 | LocalSim 缺 dry-run admission 不再阻断 multi_alpha eligibility |
| F-002 | MiniQMT/live 缺 dry-run admission 继续 hard FAIL |
| F-003 | 其它硬门不放宽：retired、manifest identity/hash、asset/self-check、未知 blocker |
| F-004 | Selection full 与 summary path 对 LocalSim eligibility 语义一致 |
| F-005 | Paper create_portfolio 对 LocalSim multi_alpha 可建组合，对 MiniQMT 仍 fail-closed |
| F-006 | UI 提供真实 paper-runtime-dry-run 按钮、broker/top_k/trade_date 输入、结果/错误展示 |
| F-007 | UI 串通 LocalSim create portfolio 入口，不再只提示手工 POST |
| F-008 | 单Alpha create/list/selection/paper 路径零回归 |
| F-009 | 无 silent error：所有失败展示 reason_code/context，LocalSim 放宽也以 WARN/context 显式可见 |
| F-010 | 无 DDL/DML、无服务启停、无 research-assistant、无 protected asset/factor library 改动 |

## Implementation Plan / 实施方案

### Phase 1：后端 eligibility 放宽

1. 修改 `backend/services/strategy_package/asset_eligibility.py`：
   - 增加本设计 reason_code 常量。
   - 在 `_multi_alpha_runtime_blockers()` 中对 `broker_backend == "local_sim"` 且缺 admission 的 paper admission blocker 生成 `WARN/warning`。
   - MiniQMT 分支维持当前 hard FAIL。
2. 修改 `backend/services/strategy_package/repository.py:list_summaries()`：
   - 默认 summary 视角为 LocalSim，缺 dry-run admission 不再让 multi_alpha summary ineligible。
   - 如保留 warnings，需要与 `_summary_from_row()` 输出结构对齐。
3. 更新/新增后端测试：
   - `backend/tests/strategy_package/test_multi_alpha_promotion.py` 中原 `test_asset_eligibility_blocks_multi_alpha_until_dry_run` 改为断言 LocalSim eligible + warning，同时新增 MiniQMT blocked。
   - `backend/tests/strategy_package/test_multi_alpha_paper_admission.py` 新增“无 admission LocalSim create_portfolio succeeds / MiniQMT blocked”。
   - `backend/tests/selection_center/test_runtime_selection.py` 覆盖 summary path 不再过滤 LocalSim multi_alpha。

### Phase 2：前端 API 与 packages UI

1. `frontend/src/lib/paper-v2/api.ts`：
   - 新增 `paperRuntimeDryRun()`。
2. `frontend/src/lib/paper-v2/types.ts`：
   - 如有必要新增 dry-run payload/result 类型；也可用 `JsonObject` 保持轻量。
3. `frontend/src/app/paper-v2/packages/page.tsx`：
   - 新增 multi_alpha 准入卡片/按钮。
   - 更新建包 `next_step` 与 NoticePanel 文案。
   - 成功 dry-run 展示摘要；失败交给 `ErrorPanel` 显示 `PaperV2ApiError`。
   - LocalSim create portfolio 链接带 query 参数。
4. `frontend/src/app/paper-v2/portfolios/page.tsx`：
   - 读取 `package_id` query 并自动选中 package。
   - 若传入 `broker_backend=local_sim`，显示 LocalSim 语义；如要传给 API，确保 create payload 明确包含 `broker_backend`，不靠默认隐藏行为。

### Phase 3：回归与自审

1. 运行后端 targeted pytest 与前端 type/lint/build。
2. 跑 `git diff --name-only` scope 自检，确认无 `research-assistant`、无 execution layer、无 migrations、无 protected asset/factor library。
3. PR body 填写 Design Acceptance Matrix、production gates、UI 截图/测试证据。

## Verification Plan / 验证方案

后端：

```powershell
rtk python -m pytest backend/tests/strategy_package/test_multi_alpha_promotion.py -q
rtk python -m pytest backend/tests/strategy_package/test_multi_alpha_paper_admission.py -q
rtk python -m pytest backend/tests/selection_center/test_runtime_selection.py -q
rtk python -m pytest backend/tests/paper_trading_v2/test_service_backend.py -q
rtk python -m compileall backend/services/strategy_package backend/services/paper_trading_v2 backend/routers
```

前端：

```powershell
cd frontend
rtk npm run lint
rtk npm run build
```

Workflow / guard：

```powershell
rtk python scripts/aistock_feature_workflow.py validate --design docs/analysis/multi_alpha_localsim_admission_relaxation_ui_design_20260702.md --tier F2
rtk git diff --check
rtk git diff --name-only
```

验收用例：

1. 无 `multi_alpha_paper_admission` 记录时，multi_alpha 父包 `summarize(record, broker_backend="local_sim").eligible is True`，warnings 包含 `multi_alpha_runtime_not_validated_until_dry_run`，context reason_code 为 `multi_alpha_localsim_dry_run_not_required`。
2. 同一包 `summarize(record, broker_backend="minqmt_sim").eligible is False`，blockers 包含 `multi_alpha_runtime_not_validated_until_dry_run`。
3. 人为制造 manifest hash mismatch/retired/未知 blocker 时，LocalSim 仍 hard FAIL。
4. Selection full path 和 summary path 都能列出未 dry-run 的 LocalSim multi_alpha 包。
5. `PaperTradingV2PortfolioService.create_portfolio(... broker_backend="local_sim")` 对未 dry-run multi_alpha 包成功；`broker_backend="minqmt_sim"` 仍失败。
6. UI dry-run 按钮对 local_sim 发真实 POST，成功展示 admission/dry-run 摘要；对 minqmt_sim 展示后端 reason_code/context。
7. UI LocalSim create portfolio 链接/预选能把用户带到可创建组合的路径。
8. 单Alpha package create/list/selection/paper 既有测试通过。

## Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/strategy_package/asset_eligibility.py::_multi_alpha_runtime_blockers` | `pytest backend/tests/strategy_package/test_multi_alpha_promotion.py backend/tests/strategy_package/test_multi_alpha_paper_admission.py ... -q`：LocalSim 无 admission `eligible=True` 且 warning/context reason_code=`multi_alpha_localsim_dry_run_not_required` | verified | 无 |
| F-002 | `backend/services/strategy_package/asset_eligibility.py::_multi_alpha_runtime_blockers` | 同上：`broker_backend=minqmt_sim` 无 admission 仍 `eligible=False`，blocker=`multi_alpha_runtime_not_validated_until_dry_run` | verified | 无 |
| F-003 | `backend/services/strategy_package/asset_eligibility.py`; `backend/services/strategy_package/repository.py` | `test_unknown_multi_alpha_paper_admission_blocker_still_blocks_localsim` 与 summary unknown-blocker 测试：未知 blocker 仍 hard FAIL；既有 retired/hash 回归全绿 | verified | 无 |
| F-004 | `backend/services/selection_center/service.py`; `backend/services/strategy_package/repository.py:list_summaries` | `test_selection_full_path_lists_multi_alpha_without_localsim_dry_run_admission` + `test_selectable_packages_summary_path_preserves_localsim_multi_alpha_warning_rows`：full/summary 均保留 LocalSim multi_alpha warning rows | verified | 无 |
| F-005 | `backend/services/paper_trading_v2/service.py:create_portfolio` 既有 broker_backend 透传；eligibility 修改 | `test_local_sim_portfolio_create_succeeds_without_admission_and_minqmt_stays_closed`：LocalSim create 成功，MiniQMT create blocked | verified | 无 |
| F-006 | `frontend/src/lib/paper-v2/api.ts`; `frontend/src/app/paper-v2/packages/page.tsx` | `npm run lint`、`npm run build` 通过；packages UI 新增真实 `paperRuntimeDryRun()` 按钮、broker/topK/date/cash 输入与 admission/artifact 摘要 | verified | 无 |
| F-007 | `frontend/src/app/paper-v2/packages/page.tsx`; `frontend/src/app/paper-v2/portfolios/page.tsx` | `npm run build` 通过；LocalSim 链接带 `package_id&broker_backend=local_sim&top_k=`，portfolios 页面读取 query 并预选 package/topK | verified | 无 |
| F-008 | single_alpha 旁路不变；`alpha_mode!=multi_alpha return []` | `pytest backend/tests/paper_trading_v2/test_portfolio_broker_backend.py backend/tests/strategy_package/test_enable_paper_invariants.py -q` 通过；144 条相关回归通过 | verified | 无 |
| F-009 | backend reason_code context；frontend `PaperV2ApiError` + `ErrorPanel` | `ruff check` 通过；失败路径测试断言 reason_code/context；grep 未发现新增 silent catch/fallback | verified | 无 |
| F-010 | 本 PR diff scope；无 migrations/DB init；无 RA；无服务命令 | `git diff --name-only` scope 自检无 research-assistant/migrations/执行层；`git diff --check` 通过；`production_ddl_gate=noop` | verified | 无 |

## Rollout / Rollback / 发布回滚

- 发布：后端与前端代码合入后，需要用户按既有流程重启后端/前端运行时；本任务不启动/重启服务。
- DB：无 DDL/DML，无生产迁移，无 backfill。
- 回滚：回滚代码即可恢复旧语义。因为本设计不写 admission、不改 manifest、不改 DB schema，不存在数据回滚。
- 兼容：存量 multi_alpha 父包 manifest 中的 blocker 保留不变；新 eligibility 在 LocalSim 上把该 blocker 解释为 warning，MiniQMT 仍解释为 blocker。

## Risks / Failure Modes / 风险

1. **Selection summary SQL 与 full path 不一致**：如果只改 Python eligibility，不改 `list_summaries()` SQL，UI summary 仍会把 multi_alpha 标 blocked。验收 F-004 必须覆盖。
2. **LocalSim 放宽误扩散到 MiniQMT**：必须用 broker_backend 分支和测试锁住；不得用“删除 blocker”或“忽略 admission 表”这种全局放宽。
3. **其它硬门被误伤**：manifest hash、retired、validator、unknown blocker 必须保留 hard FAIL。
4. **UI 继续文案式成功**：按钮必须调用真实 API；失败展示 reason_code/context，不能只显示“请手动 POST”。
5. **dry-run API 对 MiniQMT 当前不支持**：UI 需要展示这是后端明确拒绝，不要吞错误或伪造 admission。

## Implementation Evidence / 实现证据（2026-07-02）

- 后端：LocalSim 缺 `multi_alpha_runtime_not_validated_until_dry_run` admission 时返回 `WARN/warning`，不再生成 hard blocker；MiniQMT 仍 hard FAIL；未知 blocker、retired、manifest/hash/self-check 等硬门不放宽。
- Selection：full path 与 summary SQL / in-memory summary 均保留 LocalSim multi_alpha package，并把旧 dry-run blocker 暴露为 warning。
- UI：packages 页提供真实 `paper-runtime-dry-run` 按钮与 LocalSim 建组合入口；portfolios 页读取 `package_id/top_k` query 并显式以 `broker_backend=local_sim` 创建组合。
- PR 依赖说明：本任务只放宽 admission eligibility 门，不修冷启动 preflight。多Alpha 父包（2 模型）选股 artifact 生成仍可能依赖 PR #1810 修复 `live_inference.py::_single_model_asset_for_runtime("requires exactly one model asset")`；本 PR 只声明 LocalSim selectable/eligibility/create_portfolio 不被 admission 阻断。

## Production Gates / 生产门禁

- `production_ddl_gate=noop`：本任务不新增/修改 migration、schema、COMMENT、constraint，不执行 DDL。
- `production_dml_gate=noop`：不写生产 DB，不插入/修改 admission。
- `production_frontend_dependency_gate=noop`：不改 `package.json` / lockfile，不新增前端依赖。
- `production_backend_dependency_gate=noop`：不改 Python/Conda 依赖。
- `service_restart=not_performed`：本任务不启动/重启后端、前端、TDX、MiniQMT；合入后的运行时生效由用户重启。
- `research_assistant_scope=not_touched`。
